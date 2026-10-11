#include <cpp_network/connection.hpp>

#include <asio/bind_executor.hpp>
#include <asio/post.hpp>
#include <asio/strand.hpp>
#include <asio/write.hpp>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace cpp_network {

struct Connection::Impl : std::enable_shared_from_this<Impl> {
    asio::ip::tcp::socket socket;
    asio::strand<asio::any_io_executor> strand;
    const Limits limits;
    Callbacks callbacks;
    std::vector<std::byte> receive_buffer;

    // 외부 호출과 실행 흐름 사이의 대기열만 잠근다. 사용자 함수는 잠금 밖에서 실행한다.
    mutable std::mutex mutex;
    std::deque<std::shared_ptr<std::vector<std::byte>>> queue;
    Pending reserved{}; // 실제 쓰기 중인 버퍼도 포함한다.
    bool wake_posted = false;
    bool started = false;
    bool close_requested = false;
    bool resume_requested = false;
    bool blocked = false;
    CloseInfo close_info{CloseReason::local, {}};

    // 아래 상태와 소켓 접근은 strand에서만 수행한다.
    bool reading = false;
    bool writing = false;
    bool paused = false;
    bool closing = false;
    bool notified = false;

    Impl(asio::ip::tcp::socket input, Limits bounds, Callbacks handlers)
        : socket(std::move(input)), strand(asio::make_strand(socket.get_executor())),
          limits(bounds), callbacks(std::move(handlers)) {
        if (!limits.send_bytes || !limits.send_messages || !limits.receive_chunk ||
            !callbacks.on_bytes || !callbacks.on_closed)
            throw std::invalid_argument("positive limits and receive/close callbacks required");
        // 열려 있기만 한 미연결 소켓도 거절한다.
        std::error_code error;
        socket.remote_endpoint(error);
        if (error) throw std::system_error(error, "connected socket required");
        receive_buffer.resize(limits.receive_chunk);
    }

    void wake_locked() {
        if (wake_posted) return;
        // 송신마다 무제한 post하지 않는다. 대기 중인 깨우기는 연결당 최대 하나다.
        asio::post(strand, [self = shared_from_this()] { self->wake(); });
        wake_posted = true;
    }

    void request_close(CloseReason reason, std::error_code error = {}) {
        std::lock_guard lock(mutex);
        if (!close_requested) {
            close_requested = true;
            close_info = {reason, error};
            wake_locked();
        }
    }

    bool is_close_requested() const {
        std::lock_guard lock(mutex);
        return close_requested;
    }

    SendResult send(std::span<const std::byte> bytes) {
        std::lock_guard lock(mutex);
        if (close_requested) return SendResult::closed;
        if (bytes.empty()) return SendResult::empty;
        if (bytes.size() > limits.send_bytes) return SendResult::too_large;
        if (reserved.messages == limits.send_messages || bytes.size() > limits.send_bytes - reserved.bytes) {
            blocked = true;
            return SendResult::would_block;
        }
        // 상한 확인 후 소유 복사한다. 호출자가 원본을 바꿔도 송신 데이터가 변하지 않는다.
        auto data = std::make_shared<std::vector<std::byte>>(bytes.begin(), bytes.end());
        queue.push_back(std::move(data));
        reserved.bytes += bytes.size();
        ++reserved.messages;
        wake_locked();
        return SendResult::accepted;
    }

    void wake() {
        bool ready;
        bool stop;
        {
            std::lock_guard lock(mutex);
            wake_posted = false;
            ready = started;
            stop = close_requested;
            if (resume_requested) {
                paused = false;
                resume_requested = false;
            }
        }
        if (stop) {
            begin_close();
            return;
        }
        if (!ready) return;
        write_next();
        read_next();
    }

    void begin_close() {
        if (!closing) {
            closing = true;
            {
                std::lock_guard lock(mutex);
                for (const auto& data : queue) {
                    reserved.bytes -= data->size();
                    --reserved.messages;
                }
                queue.clear();
            }
            std::error_code ignored;
            socket.close(ignored);
        }
        notify_closed();
    }

    void notify_closed() {
        if (!closing || reading || writing || notified) return;
        notified = true;
        auto closed = std::move(callbacks.on_closed);
        callbacks.on_bytes = {};
        callbacks.on_writable = {};
        // close_info는 최초 종료 요청 뒤 변하지 않는다.
        CloseInfo info;
        { std::lock_guard lock(mutex); info = close_info; }
        try { closed(info); } catch (...) { /* 종료 통지 예외는 실행 풀로 전파하지 않는다. */ }
    }

    void read_next() {
        if (reading || paused || closing || is_close_requested()) return;
        reading = true;
        socket.async_read_some(asio::buffer(receive_buffer), asio::bind_executor(strand,
            [self = shared_from_this()](std::error_code error, std::size_t size) {
                self->reading = false;
                if (self->is_close_requested()) { self->begin_close(); return; }
                // 오류와 함께 반환된 마지막 데이터도 먼저 전달한다.
                if (size) {
                    try {
                        self->paused = !self->callbacks.on_bytes({self->receive_buffer.data(), size});
                    } catch (...) {
                        self->request_close(CloseReason::callback_error);
                    }
                }
                if (error) self->request_close(error == asio::error::eof ? CloseReason::peer : CloseReason::io_error, error);
                if (self->is_close_requested()) self->begin_close();
                else self->read_next();
            }));
    }

    void write_next() {
        if (writing || closing) return;
        std::shared_ptr<std::vector<std::byte>> data;
        {
            std::lock_guard lock(mutex);
            if (close_requested || queue.empty()) return;
            data = std::move(queue.front());
            queue.pop_front();
        }
        writing = true;
        // 부분 쓰기는 async_write가 이어서 처리한다. 완료 전까지 버퍼를 유지한다.
        const auto buffer = asio::buffer(*data);
        asio::async_write(socket, buffer, asio::bind_executor(strand,
            [self = shared_from_this(), data = std::move(data)](std::error_code error, std::size_t) mutable {
                self->writing = false;
                bool writable;
                {
                    std::lock_guard lock(self->mutex);
                    const auto size = data->size();
                    data.reset(); // 새 송신을 받기 전에 완료된 버퍼를 해제한다.
                    self->reserved.bytes -= size;
                    --self->reserved.messages;
                    writable = std::exchange(self->blocked, false);
                }
                if (error) self->request_close(CloseReason::io_error, error);
                if (!self->is_close_requested() && writable && self->callbacks.on_writable) {
                    try { self->callbacks.on_writable(); }
                    catch (...) { self->request_close(CloseReason::callback_error); }
                }
                if (self->is_close_requested()) self->begin_close();
                else self->write_next();
            }));
    }
};

Connection::Connection(asio::ip::tcp::socket socket, Limits limits, Callbacks callbacks)
    : impl_(std::make_shared<Impl>(std::move(socket), limits, std::move(callbacks))) {}
Connection::~Connection() { close(); }
Connection::Connection(Connection&&) noexcept = default;
Connection& Connection::operator=(Connection&& other) noexcept {
    if (this != &other) { close(); impl_ = std::move(other.impl_); }
    return *this;
}
void Connection::start() {
    if (!impl_) return;
    std::lock_guard lock(impl_->mutex);
    if (impl_->started || impl_->close_requested) return;
    impl_->started = true;
    impl_->wake_locked();
}
SendResult Connection::send(std::span<const std::byte> bytes) {
    return impl_ ? impl_->send(bytes) : SendResult::closed;
}
void Connection::resume() {
    if (!impl_) return;
    std::lock_guard lock(impl_->mutex);
    if (impl_->close_requested) return;
    impl_->resume_requested = true;
    impl_->wake_locked();
}
void Connection::close() { if (impl_) impl_->request_close(CloseReason::local); }
Pending Connection::pending() const {
    if (!impl_) return {};
    std::lock_guard lock(impl_->mutex);
    return impl_->reserved;
}

} // cpp_network 네임스페이스 끝

// strand: 지정된 완료 처리를 한 번에 하나만 실행하는 Asio 실행 수단.
// post: 실행기에 작업을 예약하는 함수. EOF(End Of File): 상대의 송신 종료.
