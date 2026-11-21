#include <cpp_network/establishment.hpp>

#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#include <atomic>
#include <stdexcept>
#include <utility>

namespace cpp_network {

struct Connector::Impl : std::enable_shared_from_this<Impl> {
    SerialExecutor serial;
    SerialSocket socket;
    asio::steady_timer timer;
    const asio::ip::tcp::endpoint remote;
    const Clock::time_point deadline;
    OnComplete on_complete;
    std::atomic<bool> start_requested{false};
    std::atomic<bool> cancel_requested{false};
    bool decided = false;
    bool connecting = false;
    bool timing = false;
    bool notified = false;
    ConnectStatus status = ConnectStatus::io_error;
    std::error_code error;

    Impl(asio::any_io_executor executor, asio::ip::tcp::endpoint endpoint,
         Clock::time_point due, OnComplete callback)
        : serial(asio::make_strand(std::move(executor))), socket(serial), timer(serial),
          remote(endpoint), deadline(due), on_complete(std::move(callback)) {
        if (!remote.port() || !on_complete)
            throw std::invalid_argument("remote port and completion callback required");
    }

    void notify_complete() {
        if (!decided || connecting || timing || notified) return;
        notified = true;
        ConnectResult result{status, error, std::nullopt};
        if (status == ConnectStatus::connected) result.socket.emplace(std::move(socket));
        auto callback = std::move(on_complete);
        // 완료 콜백 예외는 실행 풀로 전파하지 않으며 두 번째 완료를 만들지 않는다.
        try { callback(std::move(result)); } catch (...) {}
    }

    void decide(ConnectStatus value, std::error_code failure = {}) {
        if (decided) return;
        decided = true;
        status = value;
        error = failure;
        timer.cancel();
        if (status != ConnectStatus::connected) {
            std::error_code ignored;
            socket.close(ignored);
        }
        // 취소된 timer/connect 핸들러까지 회수한 뒤 소켓을 전달한다.
        notify_complete();
    }

    void begin() {
        if (decided) return;
        if (cancel_requested) { decide(ConnectStatus::cancelled, asio::error::operation_aborted); return; }
        if (Clock::now() >= deadline) { decide(ConnectStatus::timed_out, asio::error::timed_out); return; }
        timer.expires_at(deadline);
        timing = true;
        timer.async_wait([self = shared_from_this()](std::error_code failure) {
            self->timing = false;
            if (!failure && !self->decided) {
                if (self->cancel_requested)
                    self->decide(ConnectStatus::cancelled, asio::error::operation_aborted);
                else self->decide(ConnectStatus::timed_out, asio::error::timed_out);
            }
            self->notify_complete();
        });
        connecting = true;
        socket.async_connect(remote, [self = shared_from_this()](std::error_code failure) {
            self->connecting = false;
            if (!self->decided) {
                if (self->cancel_requested)
                    self->decide(ConnectStatus::cancelled, asio::error::operation_aborted);
                else if (Clock::now() >= self->deadline)
                    self->decide(ConnectStatus::timed_out, asio::error::timed_out);
                else self->decide(failure ? ConnectStatus::io_error : ConnectStatus::connected, failure);
            }
            self->notify_complete();
        });
    }
};

Connector::Connector(asio::any_io_executor executor, asio::ip::tcp::endpoint endpoint,
                     Clock::time_point deadline, OnComplete callback)
    : impl_(std::make_shared<Impl>(std::move(executor), endpoint, deadline, std::move(callback))) {}
Connector::~Connector() { cancel(); }
void Connector::start() {
    if (impl_->start_requested.exchange(true)) return;
    asio::post(impl_->serial, [self = impl_] { self->begin(); });
}
void Connector::cancel() {
    if (impl_->cancel_requested.exchange(true)) return;
    asio::post(impl_->serial, [self = impl_] {
        self->decide(ConnectStatus::cancelled, asio::error::operation_aborted);
    });
}

struct Listener::Impl : std::enable_shared_from_this<Impl> {
    const asio::any_io_executor executor;
    SerialExecutor serial;
    asio::ip::tcp::acceptor acceptor;
    asio::ip::tcp::endpoint endpoint;
    ListenerCallbacks callbacks;
    std::optional<SerialSocket> incoming;
    std::atomic<bool> start_requested{false};
    std::atomic<bool> close_requested{false};
    std::atomic<bool> resume_requested{false};
    bool started = false;
    bool paused = false;
    bool accepting = false;
    bool closing = false;
    bool notified = false;
    ListenerResult result{ListenerCloseReason::local, {}};

    Impl(asio::any_io_executor execution, asio::ip::tcp::endpoint bind_to,
         ListenerConfig config, ListenerCallbacks handlers)
        : executor(std::move(execution)), serial(asio::make_strand(executor)),
          acceptor(serial), callbacks(std::move(handlers)) {
        if (config.backlog <= 0 || !callbacks.on_accept || !callbacks.on_closed)
            throw std::invalid_argument("positive backlog and accept/close callbacks required");
        acceptor.open(bind_to.protocol());
        acceptor.bind(bind_to);
        acceptor.listen(config.backlog);
        endpoint = acceptor.local_endpoint();
    }

    void notify_closed() {
        if (!closing || accepting || notified) return;
        notified = true;
        incoming.reset();
        callbacks.on_accept = {};
        auto callback = std::move(callbacks.on_closed);
        try { callback(result); } catch (...) {}
    }

    void stop(ListenerCloseReason reason, std::error_code failure = {}) {
        if (!closing) {
            closing = true;
            result = {reason, failure};
            std::error_code ignored;
            acceptor.close(ignored);
        }
        notify_closed();
    }

    void accept_next() {
        if (closing) return;
        if (close_requested) { stop(ListenerCloseReason::local); return; }
        if (!started || paused || accepting) return;
        incoming.emplace(asio::make_strand(executor));
        accepting = true;
        acceptor.async_accept(*incoming, [self = shared_from_this()](std::error_code failure) {
            self->accepting = false;
            if (self->closing || self->close_requested) {
                self->stop(ListenerCloseReason::local);
                return;
            }
            if (failure) {
                self->stop(ListenerCloseReason::io_error, failure);
                return;
            }
            auto socket = std::move(*self->incoming);
            self->incoming.reset();
            try { self->paused = !self->callbacks.on_accept(std::move(socket)); }
            catch (...) { self->stop(ListenerCloseReason::callback_error); return; }
            self->accept_next();
        });
    }
};

Listener::Listener(asio::any_io_executor executor, asio::ip::tcp::endpoint endpoint,
                   ListenerConfig config, ListenerCallbacks callbacks)
    : impl_(std::make_shared<Impl>(std::move(executor), endpoint, config, std::move(callbacks))) {}
Listener::~Listener() { close(); }
void Listener::start() {
    if (impl_->start_requested.exchange(true)) return;
    asio::post(impl_->serial, [self = impl_] { self->started = true; self->accept_next(); });
}
void Listener::resume() {
    if (impl_->close_requested || impl_->resume_requested.exchange(true)) return;
    asio::post(impl_->serial, [self = impl_] {
        self->resume_requested = false;
        self->paused = false;
        self->accept_next();
    });
}
void Listener::close() {
    if (impl_->close_requested.exchange(true)) return;
    asio::post(impl_->serial, [self = impl_] { self->stop(ListenerCloseReason::local); });
}
asio::ip::tcp::endpoint Listener::local_endpoint() const { return impl_->endpoint; }

} // cpp_network 네임스페이스 끝
