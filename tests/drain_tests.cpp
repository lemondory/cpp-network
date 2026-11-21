#include <cpp_network/connection.hpp>
#include <asio.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace n = cpp_network;
using asio::ip::tcp;
using namespace std::chrono_literals;
using Clock = n::Connection::Clock;
void require(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
struct Pair {
    asio::io_context io;
    tcp::acceptor acceptor{io, {asio::ip::address_v4::loopback(), 0}};
    tcp::socket first{io}, second{io};
    Pair() { first.connect(acceptor.local_endpoint()); acceptor.accept(second); acceptor.close(); }
};
void run(asio::io_context& io) { if (io.stopped()) io.restart(); io.run(); }
const std::array<std::byte, 4> payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};

void cancelled() {
    Pair pair;
    std::vector<n::WriteInfo> writes;
    int closed = 0;
    bool complete_before_close = false;
    n::Connection connection(std::move(pair.first), {8, 2, 64}, {
        [](auto) { return true; }, [&](auto) { ++closed; complete_before_close = writes.size() == 2; }, {},
        [&](auto info) { writes.push_back(info); }});
    require(connection.send(payload, 1) == n::SendResult::accepted, "first send");
    require(connection.send(payload, 2) == n::SendResult::accepted, "second send");
    require(connection.send(payload, 3) == n::SendResult::would_block, "third should block");
    require(connection.send({}, 4) == n::SendResult::empty, "empty send");
    connection.close(); connection.close(); run(pair.io);
    require(closed == 1 && complete_before_close && writes.size() == 2, "completion count/order");
    for (std::size_t i = 0; i < writes.size(); ++i)
        require(writes[i].tag == i + 1 && writes[i].status == n::WriteStatus::cancelled && !writes[i].bytes &&
                writes[i].error == asio::error::operation_aborted, "queued cancellation metadata");
    require(connection.pending().bytes == 0 && connection.pending().messages == 0, "reservation leak");
}

void graceful() {
    for (bool started : {false, true}) {
        Pair pair;
        std::vector<n::WriteInfo> writes;
        std::optional<n::CloseInfo> result;
        int reads = 0, closes = 0;
        n::Connection connection(std::move(pair.first), {}, {
            [&](auto) { ++reads; return true; }, [&](auto info) { result = info; ++closes; }, {},
            [&](auto info) { writes.push_back(info); }});
        if (started) { connection.start(); pair.io.poll_one(); }
        for (std::uint64_t i = 0; i < 3; ++i)
            require(connection.send(payload, i) == n::SendResult::accepted, "send failed");
        std::array<std::byte, 12> received{};
        bool received_ok = false;
        asio::async_read(pair.second, asio::buffer(received), [&](auto error, auto size) {
            received_ok = !error && size == received.size();
        });
        connection.close_after_flush(Clock::now() + 5s);
        connection.close_after_flush(Clock::now()); // 첫 기한을 바꾸지 않는다.
        connection.resume(); connection.start();
        require(connection.send(payload, 99) == n::SendResult::closed, "send accepted after drain request");
        run(pair.io);
        require(result && result->reason == n::CloseReason::drained && !result->error && closes == 1,
                "graceful close failed");
        require(received_ok && writes.size() == 3 && reads == 0, "missing bytes or unexpected read callback");
        for (std::size_t i = 0; i < writes.size(); ++i)
            require(writes[i].tag == i && writes[i].status == n::WriteStatus::written && writes[i].bytes == 4 &&
                    !writes[i].error && std::equal(payload.begin(), payload.end(), received.begin() + i * 4),
                    "write completion/order/payload");
        std::error_code error;
        pair.second.read_some(asio::buffer(received), error);
        require(error == asio::error::eof, "graceful close did not deliver EOF");
        require(connection.pending().messages == 0 && connection.pending().bytes == 0, "drain reservation leak");
    }
    Pair empty;
    int closes = 0;
    n::Connection connection(std::move(empty.first), {}, {[](auto) { return true; }, [&](auto info) {
        if (info.reason == n::CloseReason::drained) ++closes;
    }, {}});
    connection.close_after_flush(Clock::now() + 1h); run(empty.io);
    require(closes == 1, "empty drain waited for deadline");
}

void timeout() {
    for (bool expired : {true, false}) {
        Pair pair;
        pair.first.set_option(asio::socket_base::send_buffer_size(1024));
        pair.second.set_option(asio::socket_base::receive_buffer_size(1024));
        constexpr std::size_t capacity = 8 * 1024 * 1024;
        std::vector<std::byte> bytes(capacity, std::byte{7});
        std::vector<n::WriteInfo> writes;
        std::optional<n::CloseInfo> result;
        n::Connection connection(std::move(pair.first), {capacity + 4, 2, 64}, {
            [](auto) { return true; }, [&](auto info) { result = info; }, {}, [&](auto info) { writes.push_back(info); }});
        require(connection.send(bytes, 1) == n::SendResult::accepted &&
                connection.send(payload, 2) == n::SendResult::accepted, "send failed");
        connection.close_after_flush(Clock::now() + (expired ? 0ms : 40ms));
        connection.close_after_flush(Clock::now() + 1h);
        run(pair.io);
        require(result && result->reason == n::CloseReason::drain_timeout && result->error == asio::error::timed_out,
                "drain deadline lost or extended");
        require(writes.size() == 2 && writes[0].tag == 1 && writes[1].tag == 2 &&
                writes[0].status == n::WriteStatus::cancelled && writes[0].bytes <= capacity &&
                writes[1].status == n::WriteStatus::cancelled && writes[1].bytes == 0, "cancel completion order");
        if (expired) require(writes[0].bytes == 0, "started write past deadline");
        require(connection.pending().bytes == 0 && connection.pending().messages == 0, "timeout reservation leak");
    }
}

void abort_drain(bool destroy) {
    Pair pair;
    pair.first.set_option(asio::socket_base::send_buffer_size(1024));
    pair.second.set_option(asio::socket_base::receive_buffer_size(1024));
    std::vector<std::byte> bytes(8 * 1024 * 1024);
    std::optional<n::CloseInfo> result;
    int writes = 0, closes = 0;
    auto connection = std::make_unique<n::Connection>(std::move(pair.first), n::Limits{bytes.size(), 1, 64}, n::Callbacks{
        [](auto) { return true; }, [&](auto info) { result = info; ++closes; }, {}, [&](auto) { ++writes; }});
    require(connection->send(bytes) == n::SendResult::accepted, "send failed");
    connection->close_after_flush(Clock::now() + 1h);
    pair.io.poll_one(); // 기한과 쓰기가 대기 중일 때 중단한다.
    if (destroy) connection.reset();
    else { connection->close(); connection->close(); }
    run(pair.io);
    require(result && result->reason == n::CloseReason::local && writes == 1 && closes == 1,
            "abort/destruction did not finish drain");
}

void reentrant() {
    Pair pair;
    n::Connection* handle = nullptr;
    std::vector<n::WriteInfo> writes;
    std::optional<n::CloseInfo> result;
    bool send_ok = false;
    n::Connection connection(std::move(pair.first), {8, 2, 64}, {
        [](auto) { return true; }, [&](auto info) { result = info; }, {}, [&](auto info) {
            writes.push_back(info);
            if (info.tag == 1) {
                send_ok = handle->send(payload, 2) == n::SendResult::accepted;
                handle->close_after_flush(Clock::now() + 5s);
            }
        }});
    handle = &connection;
    require(connection.send(payload, 1) == n::SendResult::accepted, "send failed");
    connection.start(); run(pair.io);
    require(send_ok && writes.size() == 2 && writes[1].tag == 2 && writes[1].status == n::WriteStatus::written &&
            result && result->reason == n::CloseReason::drained, "reentrant send/drain failed");
}

void callback_error() {
    Pair pair;
    std::vector<n::WriteInfo> writes;
    std::optional<n::CloseInfo> result;
    n::Connection connection(std::move(pair.first), {}, {
        [](auto) { return true; }, [&](auto info) { result = info; throw std::runtime_error("closed"); }, {},
        [&](auto info) { writes.push_back(info); throw std::runtime_error("write"); }});
    for (int i = 0; i < 3; ++i) require(connection.send(payload, i) == n::SendResult::accepted, "send failed");
    connection.start(); run(pair.io);
    require(result && result->reason == n::CloseReason::callback_error && writes.size() == 3 &&
            writes[0].status == n::WriteStatus::written && writes[1].status == n::WriteStatus::cancelled &&
            writes[2].status == n::WriteStatus::cancelled, "callback exception lost completion");
}

void write_error() {
    Pair pair;
    pair.first.shutdown(tcp::socket::shutdown_send);
    std::optional<n::CloseInfo> result;
    std::vector<n::WriteInfo> writes;
    n::Connection connection(std::move(pair.first), {}, {
        [](auto) { return true; }, [&](auto info) { result = info; }, {}, [&](auto info) { writes.push_back(info); }});
    require(connection.send(payload, 1) == n::SendResult::accepted &&
            connection.send(payload, 2) == n::SendResult::accepted, "send failed");
    connection.close_after_flush(Clock::now() + 5s); run(pair.io);
    require(result && result->reason == n::CloseReason::io_error && writes.size() == 2 &&
            writes[0].status == n::WriteStatus::failed && writes[0].error &&
            writes[1].status == n::WriteStatus::cancelled, "write error lost completion");
}

void concurrent(bool race) {
    Pair pair;
    constexpr std::size_t producers = 6, count = 64, total = producers * count;
    std::atomic<int> closed{0}, active{0};
    std::atomic<bool> invalid{false};
    std::array<std::atomic<bool>, total> accepted{};
    std::atomic<std::size_t> accepted_count{0};
    std::vector<n::WriteInfo> writes;
    n::Connection connection(std::move(pair.first), {total * 4, total, 64}, {
        [](auto) { return true; }, [&](auto info) {
            if (active != 0 || (info.reason != n::CloseReason::drained && !(race && info.reason == n::CloseReason::local)))
                invalid = true;
            ++closed;
        }, {}, [&](auto info) {
            if (active.fetch_add(1) != 0) invalid = true;
            writes.push_back(info);
            active.fetch_sub(1);
        }});
    connection.start();
    auto work = asio::make_work_guard(pair.io);
    std::vector<std::jthread> workers, callers;
    for (int i = 0; i < 4; ++i) workers.emplace_back([&] { pair.io.run(); });
    for (std::size_t p = 0; p < producers; ++p) callers.emplace_back([&, p] {
        for (std::size_t j = 0; j < count; ++j) {
            const auto tag = p * count + j;
            const auto result = connection.send(payload, tag);
            if (result == n::SendResult::accepted) { accepted[tag] = true; ++accepted_count; }
            else if (!race || result != n::SendResult::closed) invalid = true;
            if (race && j == count / 2) {
                connection.close_after_flush(Clock::now() + 5s);
                connection.close();
            }
        }
    });
    callers.clear();
    for (int i = 0; i < 6; ++i) callers.emplace_back([&] { connection.close_after_flush(Clock::now() + 5s); });
    callers.clear();
    const auto deadline = Clock::now() + 6s;
    while (!closed && Clock::now() < deadline) std::this_thread::yield();
    work.reset(); pair.io.stop(); workers.clear();
    require(closed == 1 && !invalid && writes.size() == accepted_count, "concurrent drain failed");
    std::array<std::size_t, producers> next{};
    for (const auto& info : writes) {
        require(info.tag < total && accepted[info.tag], "completion without acceptance");
        if (!race) require(info.status == n::WriteStatus::written && info.bytes == 4 && !info.error,
                           "write completion metadata");
        require(info.tag % count == next[info.tag / count]++, "duplicate/lost/reordered completion");
    }
    require(connection.pending().bytes == 0 && connection.pending().messages == 0, "concurrent reservation leak");
    connection.close(); run(pair.io);
    require(closed == 1 && writes.size() == accepted_count, "late duplicate completion");
}

int main(int argc, char** argv) {
    try {
        const std::string name = argc == 2 ? argv[1] : "";
        if (name == "cancelled") cancelled();
        else if (name == "graceful") graceful();
        else if (name == "timeout") timeout();
        else if (name == "abort") abort_drain(false);
        else if (name == "lifetime") abort_drain(true);
        else if (name == "reentrant") reentrant();
        else if (name == "callback_error") callback_error();
        else if (name == "write_error") write_error();
        else if (name == "concurrent") concurrent(false);
        else if (name == "close_race") concurrent(true);
        else throw std::runtime_error("unknown case");
        std::cout << "PASS " << name << '\n';
    } catch (const std::exception& error) { std::cerr << "FAIL " << error.what() << '\n'; return 1; }
}
