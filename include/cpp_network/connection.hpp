#pragma once

#include <asio/ip/tcp.hpp>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <system_error>

namespace cpp_network {

struct Limits {
    std::size_t send_bytes = 256 * 1024;
    std::size_t send_messages = 256;
    std::size_t receive_chunk = 16 * 1024;
};

enum class SendResult { accepted, would_block, too_large, empty, closed };
enum class CloseReason { local, peer, io_error, callback_error };
struct CloseInfo {
    CloseReason reason;
    std::error_code error;
};
struct Pending {
    std::size_t bytes;
    std::size_t messages;
};
struct Callbacks {
    // 데이터는 호출 동안만 유효하다. false를 반환하면 다음 읽기를 중단한다.
    std::function<bool(std::span<const std::byte>)> on_bytes;
    // 같은 연결의 모든 통지는 직렬 실행된다. 예외를 던지지 않는 것이 계약이다.
    std::function<void(CloseInfo)> on_closed;
    // would_block 이후 공간이 줄어들면 통지한다. 재시도 성공을 보장하지 않는다.
    std::function<void()> on_writable;
};

// 이미 연결된 소켓의 소유권을 받는다. 게임 규약·프레임 경계는 다루지 않는다.
// start 이후 호출 가능: send/resume/close/pending. 서로 다른 스레드에서 사용 가능하다.
// 객체 이동·파괴는 다른 공개 함수 호출과 동시에 수행하면 안 된다.
// 실행 문맥은 이 객체보다 오래 살아야 한다. 종료 통지까지 실행을 계속해야 한다.
class Connection {
public:
    Connection(asio::ip::tcp::socket socket, Limits limits, Callbacks callbacks);
    ~Connection();
    Connection(Connection&&) noexcept;
    Connection& operator=(Connection&&) noexcept;
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void start();
    // start 전에도 대기열 수락은 가능하다. 수락은 원격 수신 성공을 뜻하지 않는다.
    [[nodiscard]] SendResult send(std::span<const std::byte> bytes);
    void resume();
    // 즉시 취소: 대기 송신을 버린다. 이미 전송한 바이트는 회수할 수 없다.
    void close();
    [[nodiscard]] Pending pending() const;

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

} // cpp_network 네임스페이스 끝

// TCP(Transmission Control Protocol): 순서 있는 신뢰성 바이트 스트림 전송 규약.
// I/O(Input/Output): 입출력. callback: 비동기 결과를 전달받는 함수.
// span: 데이터를 소유하지 않는 연속 메모리 범위. pending: 아직 완료되지 않은 송신.
