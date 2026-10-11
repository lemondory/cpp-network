#include <cpp_network/connection.hpp>
#include <asio.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using cpp_network::Connection;
using cpp_network::SendResult;
using asio::ip::tcp;

void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

struct Pair {
    asio::io_context io;
    tcp::acceptor listener{io, {asio::ip::address_v4::loopback(), 0}};
    tcp::socket first{io};
    tcp::socket second{io};
    Pair() {
        first.connect(listener.local_endpoint());
        listener.accept(second);
        listener.close();
    }
};

// 조건 기반 기한 대기다. 임의 sleep으로 성공을 추측하지 않는다.
template<class Predicate>
void until(asio::io_context& io, Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        if (io.stopped()) io.restart();
        io.run_for(1ms);
    }
    require(predicate(), "completion deadline exceeded");
}

void bounds() {
    Pair pair;
    int closed = 0;
    Connection connection(std::move(pair.first), {8, 2, 4}, {
        [](auto) { return true; }, [&](auto) { ++closed; }, {}});
    std::array<std::byte, 4> data{};
    require(connection.send({}) == SendResult::empty, "empty send");
    std::array<std::byte, 9> large{};
    require(connection.send(large) == SendResult::too_large, "oversized send");
    require(connection.send(data) == SendResult::accepted, "first reservation");
    require(connection.send(data) == SendResult::accepted, "second reservation");
    require(connection.send(data) == SendResult::would_block, "byte/message cap");
    require(connection.pending().bytes == 8 && connection.pending().messages == 2, "rejection changed counters");
    connection.close();
    connection.close();
    require(connection.send(data) == SendResult::closed, "accept after close");
    until(pair.io, [&] { return closed == 1; });
    require(connection.pending().bytes == 0 && connection.pending().messages == 0, "close leaked reservation");
    // 메시지 수와 바이트 수 상한을 각각 독립적으로 확인한다.
    Pair count_pair;
    Connection count(std::move(count_pair.first), {100, 1, 4}, {[](auto) { return true; }, [](auto) {}, {}});
    require(count.send(data) == SendResult::accepted && count.send(data) == SendResult::would_block, "message cap alone");
    count.close(); count_pair.io.run();
    Pair bytes_pair;
    Connection bytes(std::move(bytes_pair.first), {5, 100, 4}, {[](auto) { return true; }, [](auto) {}, {}});
    require(bytes.send(data) == SendResult::accepted && bytes.send(data) == SendResult::would_block, "byte cap alone");
    bytes.close(); bytes_pair.io.run();
    bool rejected = false;
    try {
        Pair invalid;
        Connection bad(std::move(invalid.first), {0, 1, 1}, {[](auto) { return true; }, [](auto) {}, {}});
    } catch (const std::invalid_argument&) { rejected = true; }
    require(rejected, "zero limit accepted");
}

void roundtrip() {
    Pair pair;
    int closed = 0;
    int writable = 0;
    std::vector<std::byte> received;
    Connection sender(std::move(pair.first), {8, 2, 3}, {
        [](auto) { return true; }, [&](auto) { ++closed; }, [&] { ++writable; }});
    Connection receiver(std::move(pair.second), {8, 2, 3}, {
        [&](auto bytes) { received.insert(received.end(), bytes.begin(), bytes.end()); return true; },
        [&](auto) { ++closed; }, {}});
    std::array<std::byte, 4> data{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    const auto original = data;
    require(sender.send(data) == SendResult::accepted && sender.send(data) == SendResult::accepted, "send");
    require(sender.send(data) == SendResult::would_block, "must block");
    data.fill(std::byte{99});
    sender.start(); receiver.start();
    until(pair.io, [&] { return received.size() == 8 && writable > 0; });
    require(std::equal(original.begin(), original.end(), received.begin()), "caller mutation changed wire data");
    require(std::equal(original.begin(), original.end(), received.begin() + 4), "stream order");
    require(sender.send(data) == SendResult::accepted, "capacity not restored");
    until(pair.io, [&] { return received.size() == 12; });
    sender.close(); receiver.close();
    until(pair.io, [&] { return closed == 2; });
}

void receive_pause() {
    Pair pair;
    int closed = 0;
    std::size_t bytes = 0;
    Connection receiver(std::move(pair.second), {16, 4, 1}, {
        [&](auto input) { bytes += input.size(); return false; }, [&](auto) { ++closed; }, {}});
    receiver.start();
    const std::array<std::byte, 3> input{};
    asio::write(pair.first, asio::buffer(input));
    until(pair.io, [&] { return bytes == 1; });
    pair.io.restart(); pair.io.poll();
    require(bytes == 1, "read continued while paused");
    receiver.resume();
    until(pair.io, [&] { return bytes == 2; });
    pair.io.restart(); pair.io.poll();
    require(bytes == 2, "resume must stop again on false");
    receiver.close();
    until(pair.io, [&] { return closed == 1; });
}

void concurrent(bool race_close) {
    Pair pair;
    constexpr std::size_t producers = 6;
    constexpr std::size_t per_producer = 300;
    std::atomic<int> closed{0};
    std::atomic<int> callbacks_running{0};
    std::atomic<bool> overlap{false};
    std::atomic<std::size_t> received_size{0};
    std::atomic<std::size_t> accepted{0};
    std::atomic<bool> unexpected{false};
    std::vector<std::byte> received;
    Connection sender(std::move(pair.first), {producers * per_producer * 4, producers * per_producer, 31}, {
        [](auto) { return true; }, [&](auto) { ++closed; }, {}});
    Connection receiver(std::move(pair.second), {}, {
        [&](auto input) {
            if (callbacks_running.fetch_add(1) != 0) overlap = true;
            received.insert(received.end(), input.begin(), input.end());
            received_size += input.size();
            callbacks_running.fetch_sub(1);
            return true;
        }, [&](auto) { ++closed; }, {}});
    sender.start(); receiver.start();
    auto work = asio::make_work_guard(pair.io);
    std::vector<std::jthread> workers;
    for (int i = 0; i < 4; ++i) workers.emplace_back([&] { pair.io.run(); });
    std::vector<std::jthread> writers;
    for (std::size_t producer = 0; producer < producers; ++producer) {
        writers.emplace_back([&, producer] {
            for (std::size_t index = 0; index < per_producer; ++index) {
                const auto id = producer * per_producer + index;
                const std::array<std::byte, 4> data{std::byte(id & 255), std::byte((id >> 8) & 255), std::byte{17}, std::byte{29}};
                const auto result = sender.send(data);
                if (result == SendResult::accepted) ++accepted;
                else if (!race_close || result != SendResult::closed) unexpected = true;
                if (race_close && index == per_producer / 2) sender.close();
            }
        });
    }
    writers.clear(); // 작성자 종료를 기다린다.
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    if (!race_close) {
        while (received_size != producers * per_producer * 4 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
    }
    sender.close(); receiver.close();
    while (closed != 2 && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    work.reset();
    pair.io.stop();
    workers.clear();
    require(closed == 2 && !overlap && !unexpected, "concurrent callbacks/close/send failed");
    require(sender.pending().bytes == 0 && sender.pending().messages == 0, "reservation leak");
    if (race_close) return; // 취소 시 일부 전달은 허용한다.
    require(accepted == producers * per_producer && received.size() == accepted * 4, "lost bytes");
    std::vector<bool> seen(producers * per_producer);
    std::array<std::size_t, producers> next{};
    for (std::size_t offset = 0; offset < received.size(); offset += 4) {
        const auto id = std::to_integer<unsigned>(received[offset]) + (std::to_integer<unsigned>(received[offset + 1]) << 8);
        require(id < seen.size() && !seen[id], "interleaving or duplicate");
        require(received[offset + 2] == std::byte{17} && received[offset + 3] == std::byte{29}, "payload corrupted");
        require(id % per_producer == next[id / per_producer]++, "producer order changed");
        seen[id] = true;
    }
}

void stalled_peer() {
    Pair pair;
    pair.first.set_option(asio::socket_base::send_buffer_size(1024));
    pair.second.set_option(asio::socket_base::receive_buffer_size(1024));
    constexpr std::size_t capacity = 8 * 1024 * 1024;
    int closed = 0;
    Connection sender(std::move(pair.first), {capacity, 2, 64}, {
        [](auto) { return true; }, [&](auto) { ++closed; }, {}});
    std::vector<std::byte> data(capacity, std::byte{37});
    require(sender.send(data) == SendResult::accepted, "large send refused");
    sender.start();
    pair.io.poll(); // 상대가 읽지 않으므로 커널 버퍼가 찬 뒤 비동기 쓰기가 대기한다.
    require(sender.pending().bytes == capacity, "in-flight write not included in cap");
    require(sender.send(std::span(data).first(1)) == SendResult::would_block, "in-flight cap bypassed");
    sender.close();
    until(pair.io, [&] { return closed == 1; });
    require(sender.pending().bytes == 0, "cancelled write retained reservation");
}

void peer_close(bool throw_callback) {
    Pair pair;
    int closed = 0;
    cpp_network::CloseReason reason{};
    Connection receiver(std::move(pair.second), {}, {
        [=](auto) { if (throw_callback) throw std::runtime_error("test"); return true; },
        [&](auto info) { reason = info.reason; ++closed; }, {}});
    receiver.start();
    if (throw_callback) {
        const std::array<std::byte, 1> data{};
        asio::write(pair.first, asio::buffer(data));
    } else pair.first.close();
    until(pair.io, [&] { return closed == 1; });
    require(reason == (throw_callback ? cpp_network::CloseReason::callback_error : cpp_network::CloseReason::peer), "wrong close reason");
    receiver.close();
    pair.io.restart(); pair.io.poll();
    require(closed == 1, "duplicate close callback");
}

void lifetime() {
    Pair pair;
    int closed = 0;
    {
        Connection sender(std::move(pair.first), {}, {[](auto) { return true; }, [&](auto) { ++closed; }, {}});
        sender.start();
        pair.io.poll(); // 읽기가 진행 중인 상태에서 공개 객체를 파괴한다.
    }
    until(pair.io, [&] { return closed == 1; });
}

int main(int argc, char** argv) {
    try {
        const std::string name = argc == 2 ? argv[1] : "";
        if (name == "bounds") bounds();
        else if (name == "roundtrip") roundtrip();
        else if (name == "pause") receive_pause();
        else if (name == "concurrent") concurrent(false);
        else if (name == "close_race") concurrent(true);
        else if (name == "stalled_peer") stalled_peer();
        else if (name == "peer_close") peer_close(false);
        else if (name == "callback_error") peer_close(true);
        else if (name == "lifetime") lifetime();
        else throw std::runtime_error("unknown case");
        std::cout << "PASS " << name << '\n';
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}

// ID(Identifier): 송신 항목의 식별자. io(Input/Output): 입출력 실행 문맥 변수.
// producer: 송신을 요청하는 스레드. loopback: 같은 컴퓨터 안에서 통신하는 주소.
