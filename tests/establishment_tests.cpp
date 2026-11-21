#include <cpp_network/establishment.hpp>
#include <asio.hpp>
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace n = cpp_network;
using asio::ip::tcp;
using namespace std::chrono_literals;
using Clock = n::Connector::Clock;

void require(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
tcp::endpoint loopback() { return {asio::ip::address_v4::loopback(), 0}; }
template<class Predicate> void until(asio::io_context& io, Predicate done) {
    const auto deadline = Clock::now() + 5s;
    while (!done() && Clock::now() < deadline) {
        if (io.stopped()) io.restart();
        io.run_for(1ms);
    }
    require(done(), "completion deadline exceeded");
}
void drain(asio::io_context& io) {
    if (io.stopped()) io.restart();
    io.run(); // 타이머나 취소된 I/O가 남으면 CTest 기한 안에 종료하지 못한다.
}

void validation() {
    asio::io_context io;
    int rejected = 0;
    try { n::Listener bad(io.get_executor(), loopback(), {0}, {[](auto) { return true; }, [](auto) {}}); }
    catch (const std::invalid_argument&) { ++rejected; }
    try { n::Listener bad(io.get_executor(), loopback(), {}, {{}, [](auto) {}}); }
    catch (const std::invalid_argument&) { ++rejected; }
    try { n::Listener bad(io.get_executor(), loopback(), {}, {[](auto) { return true; }, {}}); }
    catch (const std::invalid_argument&) { ++rejected; }
    try { n::Connector bad(io.get_executor(), loopback(), Clock::now(), [](auto) {}); }
    catch (const std::invalid_argument&) { ++rejected; }
    try { n::Connector bad(io.get_executor(), {asio::ip::address_v4::loopback(), 1}, Clock::now(), {}); }
    catch (const std::invalid_argument&) { ++rejected; }
    require(rejected == 5, "invalid settings accepted");
    n::Listener listener(io.get_executor(), loopback(), {}, {[](auto) { return true; }, [](auto) {}});
    bool bind_failed = false;
    try { n::Listener duplicate(io.get_executor(), listener.local_endpoint(), {}, {[](auto) { return true; }, [](auto) {}}); }
    catch (const std::system_error&) { bind_failed = true; }
    require(bind_failed, "duplicate bind succeeded");
    listener.close(); drain(io);
}

void exchange() {
    asio::io_context io;
    std::optional<n::SerialSocket> accepted;
    std::optional<n::ConnectResult> result;
    int accepts = 0, completed = 0, closed = 0;
    n::Listener listener(io.get_executor(), loopback(), {}, {
        [&](auto socket) { ++accepts; accepted.emplace(std::move(socket)); return true; },
        [&](auto) { ++closed; }});
    n::Connector connector(io.get_executor(), listener.local_endpoint(), Clock::now() + 5s,
        [&](auto value) { ++completed; result.emplace(std::move(value)); });
    listener.start(); listener.start(); connector.start(); connector.start();
    until(io, [&] { return accepted && result; });
    require(result->status == n::ConnectStatus::connected && !result->error && result->socket,
            "connect did not transfer socket");
    require(result->socket->remote_endpoint() == listener.local_endpoint(), "wrong remote");
    listener.close(); connector.cancel();
    drain(io);
    require(accepts == 1 && completed == 1 && closed == 1, "duplicate completion");
    // Listener/Connector 종료 후에도 전달된 소켓은 호출자가 소유한다.
    const std::string data = "independent ownership";
    asio::write(*result->socket, asio::buffer(data));
    std::string received(data.size(), '\0');
    asio::read(*accepted, asio::buffer(received));
    require(received == data, "establishment shutdown closed handed-off sockets");
}

void refusal() {
    asio::io_context io;
    // 사용 가능한 로컬 포트를 얻은 뒤 리스너를 닫아 접속 거절을 재현한다.
    tcp::acceptor reserved(io, loopback());
    const auto refused_endpoint = reserved.local_endpoint();
    reserved.close();
    int completed = 0;
    std::optional<n::ConnectResult> result;
    n::Connector connector(io.get_executor(), refused_endpoint, Clock::now() + 1h,
        [&](auto value) { ++completed; result.emplace(std::move(value)); });
    connector.start(); drain(io);
    require(completed == 1 && result->status == n::ConnectStatus::io_error && result->error && !result->socket,
            "refused connect did not clean up");
    // 실패 이후 새 시도는 정상적으로 진행할 수 있어야 한다.
    tcp::acceptor retry_listener(io, loopback());
    n::Connector retry(io.get_executor(), retry_listener.local_endpoint(), Clock::now() + 1h,
        [&](auto value) { ++completed; result.emplace(std::move(value)); });
    retry.start(); drain(io);
    require(completed == 2 && result->status == n::ConnectStatus::connected && result->socket,
            "fresh attempt after failure did not connect");
}

void timeout() {
    for (bool already_expired : {true, false}) {
        asio::io_context io;
        tcp::acceptor listener(io, loopback());
        int completed = 0;
        std::optional<n::ConnectResult> result;
        const auto deadline = Clock::now() + (already_expired ? 0ms : 50ms);
        n::Connector connector(io.get_executor(), listener.local_endpoint(), deadline,
            [&](auto value) { ++completed; result.emplace(std::move(value)); });
        connector.start();
        if (!already_expired) {
            require(io.poll_one() == 1, "start was not queued");
            // 커널 접속이 성공해도 완료 처리가 절대 기한 뒤라면 성공으로 전달하지 않는다.
            std::this_thread::sleep_until(deadline);
        }
        drain(io);
        require(completed == 1 && result->status == n::ConnectStatus::timed_out &&
                result->error == asio::error::timed_out && !result->socket, "deadline bypassed");
        connector.cancel(); drain(io);
        require(completed == 1, "late cancel repeated timeout completion");
    }
}

void cancellation() {
    // 시작 전 취소, 예약 후 취소, connect 개시 후 완료 처리 전 취소.
    for (int stage = 0; stage != 3; ++stage) {
        asio::io_context io;
        tcp::acceptor listener(io, loopback());
        int completed = 0;
        std::optional<n::ConnectResult> result;
        n::Connector connector(io.get_executor(), listener.local_endpoint(), Clock::now() + 1h,
            [&](auto value) { ++completed; result.emplace(std::move(value)); });
        if (stage > 0) connector.start();
        if (stage == 2) require(io.poll_one() == 1, "start was not queued");
        connector.cancel(); connector.cancel(); connector.start();
        drain(io);
        require(completed == 1 && result->status == n::ConnectStatus::cancelled &&
                result->error == asio::error::operation_aborted && !result->socket, "cancellation lost");
    }
}

void pause_resume() {
    asio::io_context io;
    int accepts = 0, closed = 0;
    n::Listener* handle = nullptr;
    std::vector<n::SerialSocket> sockets;
    n::Listener listener(io.get_executor(), loopback(), {}, {
        [&](auto socket) {
            sockets.push_back(std::move(socket));
            // 콜백 안에서 요청한 resume이 false 반환 뒤에도 유실되지 않아야 한다.
            if (++accepts == 2) { handle->resume(); handle->resume(); }
            return false;
        }, [&](auto) { ++closed; }});
    handle = &listener;
    listener.start();
    std::vector<tcp::socket> clients;
    for (int i = 0; i < 3; ++i) { clients.emplace_back(io); clients.back().connect(listener.local_endpoint()); }
    until(io, [&] { return accepts == 1; });
    io.restart(); io.poll();
    require(accepts == 1, "accept continued while paused");
    listener.resume();
    until(io, [&] { return accepts == 3; });
    listener.close(); drain(io);
    require(closed == 1, "paused listener did not close");
}

void listener_close() {
    for (bool start : {false, true}) {
        asio::io_context io;
        int accepted = 0, closed = 0;
        n::ListenerResult result{};
        n::Listener listener(io.get_executor(), loopback(), {}, {
            [&](auto) { ++accepted; return true; }, [&](auto value) { ++closed; result = value; }});
        if (start) { listener.start(); io.poll_one(); }
        listener.close(); listener.close(); listener.start(); listener.resume();
        drain(io);
        require(accepted == 0 && closed == 1 && result.reason == n::ListenerCloseReason::local && !result.error,
                "listener close leaked accept or repeated notification");
    }
    // accept 완료가 준비된 상태에서도 공개 close 요청 뒤 새 소켓을 전달하지 않는다.
    asio::io_context io;
    int accepted = 0, closed = 0;
    n::Listener listener(io.get_executor(), loopback(), {}, {
        [&](auto) { ++accepted; return true; }, [&](auto) { ++closed; }});
    listener.start(); io.poll_one();
    tcp::socket client(io); client.connect(listener.local_endpoint());
    listener.close(); drain(io);
    require(accepted == 0 && closed == 1, "socket delivered after close request");
}

void lifetime() {
    for (bool start : {false, true}) {
        asio::io_context io;
        int listener_closed = 0, connected = 0;
        bool cancelled = false;
        auto listener = std::make_unique<n::Listener>(io.get_executor(), loopback(), n::ListenerConfig{},
            n::ListenerCallbacks{[](auto) { return true; }, [&](auto) { ++listener_closed; }});
        auto connector = std::make_unique<n::Connector>(io.get_executor(), listener->local_endpoint(), Clock::now() + 1h,
            [&](auto result) { ++connected; cancelled = result.status == n::ConnectStatus::cancelled && !result.socket; });
        if (start) { connector->start(); io.poll_one(); listener->start(); }
        connector.reset(); listener.reset(); drain(io);
        require(listener_closed == 1 && connected == 1 && cancelled, "destruction lost cancellation");
    }
    // 실행하지 않은 문맥을 파괴해도 핸들러의 소유권 순환을 만들지 않는다(ASan 검사).
    asio::io_context io;
    n::Listener listener(io.get_executor(), loopback(), {}, {[](auto) { return true; }, [](auto) {}});
    n::Connector connector(io.get_executor(), listener.local_endpoint(), Clock::now() + 1h, [](auto) {});
    listener.start(); connector.start();
}

void callback_errors() {
    asio::io_context io;
    int closed = 0, completed = 0;
    n::ListenerResult result{};
    n::Listener listener(io.get_executor(), loopback(), {}, {
        [](auto) -> bool { throw std::runtime_error("accept callback"); },
        [&](auto value) { ++closed; result = value; throw std::runtime_error("close callback"); }});
    n::Connector connector(io.get_executor(), listener.local_endpoint(), Clock::now() + 1h,
        [&](auto) { ++completed; throw std::runtime_error("completion callback"); });
    listener.start(); connector.start(); drain(io);
    require(closed == 1 && completed == 1 && result.reason == n::ListenerCloseReason::callback_error,
            "callback error escaped or did not close listener");
}

void concurrent() {
    asio::io_context io;
    constexpr int count = 48;
    std::atomic<int> completed{0}, closed{0}, active{0};
    std::atomic<bool> invalid{false}, overlap{false};
    std::atomic<int> calls[count]{};
    n::Listener listener(io.get_executor(), loopback(), {}, {
        [&](auto) {
            if (active.fetch_add(1) != 0) overlap = true;
            active.fetch_sub(1);
            return true;
        }, [&](auto) { if (active.load() != 0) overlap = true; ++closed; }});
    std::vector<std::unique_ptr<n::Connector>> connectors;
    for (int i = 0; i != count; ++i) {
        connectors.push_back(std::make_unique<n::Connector>(io.get_executor(), listener.local_endpoint(), Clock::now() + 10s,
            [&, i](auto value) {
                if (++calls[i] != 1 || (value.status != n::ConnectStatus::connected && value.status != n::ConnectStatus::cancelled) ||
                    (value.status == n::ConnectStatus::connected) != value.socket.has_value()) invalid = true;
                ++completed;
            }));
    }
    auto work = asio::make_work_guard(io);
    std::vector<std::jthread> workers;
    for (int i = 0; i < 4; ++i) workers.emplace_back([&] { io.run(); });
    std::vector<std::jthread> callers;
    for (int i = 0; i < 6; ++i) callers.emplace_back([&] {
        listener.start();
        for (int j = 0; j != count; ++j) {
            connectors[j]->start();
            if (j % 2) connectors[j]->cancel();
            listener.resume();
        }
    });
    callers.clear();
    const auto deadline = Clock::now() + 5s;
    while (completed != count && Clock::now() < deadline) std::this_thread::yield();
    for (int i = 0; i < 6; ++i) callers.emplace_back([&] { listener.close(); });
    callers.clear();
    while (closed != 1 && Clock::now() < deadline) std::this_thread::yield();
    work.reset(); io.stop(); workers.clear();
    require(completed == count && closed == 1 && !invalid && !overlap, "concurrent establishment contract failed");
    connectors.clear(); drain(io);
    require(completed == count && closed == 1, "late completion after cancellation");
}

int main(int argc, char** argv) {
    try {
        const std::string name = argc == 2 ? argv[1] : "";
        if (name == "validation") validation();
        else if (name == "exchange") exchange();
        else if (name == "refusal") refusal();
        else if (name == "timeout") timeout();
        else if (name == "cancel") cancellation();
        else if (name == "pause") pause_resume();
        else if (name == "close") listener_close();
        else if (name == "lifetime") lifetime();
        else if (name == "callbacks") callback_errors();
        else if (name == "concurrent") concurrent();
        else throw std::runtime_error("unknown case");
        std::cout << "PASS " << name << '\n';
    } catch (const std::exception& error) { std::cerr << "FAIL " << error.what() << '\n'; return 1; }
}
