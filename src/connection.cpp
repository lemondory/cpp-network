#include <cpp_network/connection.hpp>

#include <asio/bind_cancellation_slot.hpp>
#include <asio/bind_executor.hpp>
#include <asio/cancellation_signal.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#include <asio/strand.hpp>
#include <asio/write.hpp>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace cpp_network {

struct Connection::Impl : std::enable_shared_from_this<Impl> {
    struct Write { std::vector<std::byte> bytes; std::uint64_t tag; };
    asio::ip::tcp::socket socket;
    asio::strand<asio::any_io_executor> strand;
    asio::steady_timer drain_timer;
    asio::cancellation_signal read_cancel;
    const Limits limits;
    Callbacks callbacks;
    std::vector<std::byte> receive_buffer;

    // 공개 호출과 실행 측 사이의 예약량만 보호한다. 사용자 함수는 잠금 밖에서 실행한다.
    mutable std::mutex mutex;
    std::deque<std::shared_ptr<Write>> queue;
    Pending reserved{};
    bool wake_posted = false;
    bool started = false;
    bool close_requested = false;
    bool drain_requested = false;
    Clock::time_point drain_deadline{};
    bool resume_requested = false;
    bool blocked = false;
    CloseInfo close_info{CloseReason::local, {}};

    // 아래 상태는 strand에서만 변경한다.
    bool reading = false;
    bool writing = false;
    bool paused = false;
    bool draining = false;
    bool timer_waiting = false;
    bool closing = false;
    bool notified = false;

    Impl(asio::ip::tcp::socket input, Limits bounds, Callbacks handlers)
        : socket(std::move(input)), strand(asio::make_strand(socket.get_executor())),
          drain_timer(strand), limits(bounds), callbacks(std::move(handlers)) {
        if (!limits.send_bytes || !limits.send_messages || !limits.receive_chunk ||
            !callbacks.on_bytes || !callbacks.on_closed)
            throw std::invalid_argument("positive limits and receive/close callbacks required");
        std::error_code error;
        socket.remote_endpoint(error);
        if (error) throw std::system_error(error, "connected socket required");
        receive_buffer.resize(limits.receive_chunk);
    }

    void wake_locked() {
        if (wake_posted) return;
        asio::post(strand, [self = shared_from_this()] {
            { std::lock_guard lock(self->mutex); self->wake_posted = false; }
            self->drive();
        });
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
    bool is_close_requested() const { std::lock_guard lock(mutex); return close_requested; }
    bool is_draining() const { std::lock_guard lock(mutex); return drain_requested; }

    SendResult send(std::span<const std::byte> bytes, std::uint64_t tag) {
        std::lock_guard lock(mutex);
        if (close_requested || drain_requested) return SendResult::closed;
        if (bytes.empty()) return SendResult::empty;
        if (bytes.size() > limits.send_bytes) return SendResult::too_large;
        if (reserved.messages == limits.send_messages || bytes.size() > limits.send_bytes - reserved.bytes) {
            blocked = true;
            return SendResult::would_block;
        }
        auto data = std::make_shared<Write>(Write{{bytes.begin(), bytes.end()}, tag});
        queue.push_back(std::move(data));
        reserved.bytes += bytes.size();
        ++reserved.messages;
        wake_locked();
        return SendResult::accepted;
    }
    void notify_write(WriteInfo info) {
        if (!callbacks.on_write) return;
        try { callbacks.on_write(info); }
        catch (...) { request_close(CloseReason::callback_error); }
    }

    void drive() {
        bool ready, stop, flush;
        Clock::time_point deadline;
        std::size_t pending;
        {
            std::lock_guard lock(mutex);
            ready = started;
            stop = close_requested;
            flush = drain_requested;
            deadline = drain_deadline;
            pending = reserved.messages;
            if (resume_requested) { paused = false; resume_requested = false; }
        }
        if (stop) { begin_close(); return; }
        if (flush) {
            if (Clock::now() >= deadline) {
                request_close(CloseReason::drain_timeout, asio::error::timed_out);
                begin_close();
                return;
            }
            if (!draining) {
                draining = true;
                // 쓰기는 계속하고 진행 중인 읽기만 취소한다.
                read_cancel.emit(asio::cancellation_type::terminal);
                drain_timer.expires_at(deadline);
                timer_waiting = true;
                drain_timer.async_wait([self = shared_from_this()](std::error_code error) {
                    self->timer_waiting = false;
                    if (!error) self->request_close(CloseReason::drain_timeout, asio::error::timed_out);
                    self->drive();
                });
            }
            // 읽기 취소 완료까지 회수한 뒤 FIN/close한다.
            if (!pending && !reading) {
                request_close(CloseReason::drained);
                begin_close();
                return;
            }
        }
        if (!ready) return;
        write_next();
        read_next();
    }

    void begin_close() {
        if (!closing) {
            closing = true;
            drain_timer.cancel();
            CloseInfo info;
            { std::lock_guard lock(mutex); info = close_info; }
            std::error_code ignored;
            if (info.reason == CloseReason::drained) socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignored);
            socket.close(ignored);
        }
        notify_closed();
    }
    void notify_closed() {
        if (!closing || reading || writing || timer_waiting || notified) return;
        notified = true;
        // 진행 중 쓰기의 통지 뒤에, 시작하지 않은 송신을 수락 순서대로 취소 통지한다.
        for (;;) {
            std::uint64_t tag;
            {
                std::lock_guard lock(mutex);
                if (queue.empty()) break;
                tag = queue.front()->tag;
                reserved.bytes -= queue.front()->bytes.size();
                --reserved.messages;
                queue.pop_front();
            }
            notify_write({tag, WriteStatus::cancelled, 0, asio::error::operation_aborted});
        }
        auto closed = std::move(callbacks.on_closed);
        callbacks.on_bytes = {};
        callbacks.on_writable = {};
        callbacks.on_write = {};
        CloseInfo info;
        { std::lock_guard lock(mutex); info = close_info; }
        try { closed(info); } catch (...) {}
    }

    void read_next() {
        if (reading || paused || closing || is_close_requested() || is_draining()) return;
        reading = true;
        socket.async_read_some(asio::buffer(receive_buffer), asio::bind_cancellation_slot(read_cancel.slot(),
            asio::bind_executor(strand, [self = shared_from_this()](std::error_code error, std::size_t size) {
                self->reading = false;
                if (self->is_close_requested()) { self->begin_close(); return; }
                if (self->is_draining()) {
                    if (error && error != asio::error::operation_aborted && error != asio::error::eof)
                        self->request_close(CloseReason::io_error, error);
                    self->drive();
                    return;
                }
                if (size) {
                    try { self->paused = !self->callbacks.on_bytes({self->receive_buffer.data(), size}); }
                    catch (...) { self->request_close(CloseReason::callback_error); }
                }
                if (error) self->request_close(error == asio::error::eof ? CloseReason::peer : CloseReason::io_error, error);
                self->drive();
            })));
    }
    void write_next() {
        if (writing || closing) return;
        std::shared_ptr<Write> data;
        {
            std::lock_guard lock(mutex);
            if (close_requested || queue.empty()) return;
            data = std::move(queue.front());
            queue.pop_front();
        }
        writing = true;
        const auto buffer = asio::buffer(data->bytes);
        asio::async_write(socket, buffer, asio::bind_executor(strand,
            [self = shared_from_this(), data = std::move(data)](std::error_code error, std::size_t bytes) mutable {
                self->writing = false;
                const auto tag = data->tag;
                bool writable;
                {
                    std::lock_guard lock(self->mutex);
                    const auto size = data->bytes.size();
                    data.reset();
                    self->reserved.bytes -= size;
                    --self->reserved.messages;
                    writable = std::exchange(self->blocked, false);
                }
                if (error) self->request_close(CloseReason::io_error, error);
                const auto status = !error ? WriteStatus::written :
                    error == asio::error::operation_aborted ? WriteStatus::cancelled : WriteStatus::failed;
                self->notify_write({tag, status, bytes, error});
                if (!self->is_close_requested() && !self->is_draining() && writable && self->callbacks.on_writable) {
                    try { self->callbacks.on_writable(); }
                    catch (...) { self->request_close(CloseReason::callback_error); }
                }
                self->drive();
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
    if (impl_->started || impl_->close_requested || impl_->drain_requested) return;
    impl_->started = true;
    impl_->wake_locked();
}
SendResult Connection::send(std::span<const std::byte> bytes, std::uint64_t tag) {
    return impl_ ? impl_->send(bytes, tag) : SendResult::closed;
}
void Connection::resume() {
    if (!impl_) return;
    std::lock_guard lock(impl_->mutex);
    if (impl_->close_requested || impl_->drain_requested) return;
    impl_->resume_requested = true;
    impl_->wake_locked();
}
void Connection::close() { if (impl_) impl_->request_close(CloseReason::local); }
void Connection::close_after_flush(Clock::time_point deadline) {
    if (!impl_) return;
    std::lock_guard lock(impl_->mutex);
    if (impl_->close_requested || impl_->drain_requested) return;
    impl_->drain_requested = true;
    impl_->drain_deadline = deadline;
    impl_->started = true;
    impl_->wake_locked();
}
Pending Connection::pending() const {
    if (!impl_) return {};
    std::lock_guard lock(impl_->mutex);
    return impl_->reserved;
}

} // cpp_network 네임스페이스 끝
