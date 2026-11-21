#pragma once

#include <asio/ip/tcp.hpp>
#include <asio/strand.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <system_error>

namespace cpp_network {

using SerialExecutor = asio::strand<asio::any_io_executor>;
using SerialSocket = asio::basic_stream_socket<asio::ip::tcp, SerialExecutor>;

enum class ConnectStatus { connected, cancelled, timed_out, io_error };
struct ConnectResult {
    ConnectStatus status;
    std::error_code error;
    // connected일 때만 소켓 소유권을 전달한다. Connection에 이동해서 사용할 수 있다.
    std::optional<SerialSocket> socket;
};

// 한 객체가 접속 한 번을 담당한다. 재시도는 새 객체로 명시적으로 시작한다.
// start/cancel은 여러 스레드에서 호출 가능하고 중복 요청은 합쳐진다.
// 파괴는 cancel을 요청한다. 완료 통지까지 실행 문맥을 유지해야 한다.
class Connector {
public:
    using Clock = std::chrono::steady_clock;
    using OnComplete = std::function<void(ConnectResult)>;
    Connector(asio::any_io_executor executor, asio::ip::tcp::endpoint remote,
              Clock::time_point deadline, OnComplete on_complete);
    ~Connector();
    Connector(const Connector&) = delete;
    Connector& operator=(const Connector&) = delete;
    void start();
    void cancel();
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

enum class ListenerCloseReason { local, io_error, callback_error };
struct ListenerResult {
    ListenerCloseReason reason;
    std::error_code error;
};
struct ListenerConfig {
    int backlog = 128;
};
struct ListenerCallbacks {
    // 소켓 소유권을 전달한다. false는 다음 accept를 멈추며 resume으로 재개한다.
    std::function<bool(SerialSocket)> on_accept;
    std::function<void(ListenerResult)> on_closed;
};

// 생성 시 bind/listen한다. start 이후 accept는 한 개씩 비동기로 실행한다.
// start/resume/close는 여러 스레드에서 호출 가능하며 객체 이동·파괴와 겹치면 안 된다.
// 각 수락 소켓은 별도의 strand를 가진다. Listener 종료는 이미 전달한 소켓을 닫지 않는다.
class Listener {
public:
    Listener(asio::any_io_executor executor, asio::ip::tcp::endpoint endpoint,
             ListenerConfig config, ListenerCallbacks callbacks);
    ~Listener();
    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;
    void start();
    void resume();
    void close();
    // 생성 시 확정한 주소의 사본이며 종료 뒤에도 조회할 수 있다.
    [[nodiscard]] asio::ip::tcp::endpoint local_endpoint() const;
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

} // cpp_network 네임스페이스 끝
