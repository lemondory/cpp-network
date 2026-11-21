#include <cpp_network/connection.hpp>
#include <cpp_network/establishment.hpp>
#include <asio.hpp>
#include <iostream>
#include <string>

int main() {
    try {
        namespace n = cpp_network;
        asio::io_context io;
        const std::string message = "asynchronous accept/connect roundtrip";
        std::string received;
        std::unique_ptr<n::Connection> server, client;
        n::Listener* listener_handle = nullptr;
        bool failed = false, listener_closed = false, connected = false;
        int closed = 0;
        asio::steady_timer deadline(io, std::chrono::seconds(5));
        auto on_closed = [&](auto) { if (++closed == 2) deadline.cancel(); };
        n::Listener listener(io.get_executor(), {asio::ip::address_v4::loopback(), 0}, {}, {
            [&](auto socket) {
                server = std::make_unique<n::Connection>(asio::ip::tcp::socket(std::move(socket)), n::Limits{}, n::Callbacks{
                    [&](auto bytes) {
                        if (server->send(bytes) != n::SendResult::accepted) { failed = true; server->close(); }
                        return true;
                    }, on_closed, {}});
                server->start();
                listener_handle->close(); // 한 연결만 받는다. 전달한 소켓은 계속 살아 있다.
                return false;
            }, [&](auto result) {
                listener_closed = true;
                if (result.reason != n::ListenerCloseReason::local) failed = true;
            }});
        listener_handle = &listener;
        n::Connector connector(io.get_executor(), listener.local_endpoint(), n::Connector::Clock::now() + std::chrono::seconds(3),
            [&](auto result) {
                connected = result.status == n::ConnectStatus::connected;
                if (!connected) { failed = true; listener.close(); deadline.cancel(); return; }
                client = std::make_unique<n::Connection>(asio::ip::tcp::socket(std::move(*result.socket)), n::Limits{}, n::Callbacks{
                    [&](auto bytes) {
                        received.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                        if (received.size() >= message.size()) { client->close(); server->close(); }
                        return true;
                    }, on_closed, {}});
                client->start();
                if (client->send(std::as_bytes(std::span(message))) != n::SendResult::accepted) {
                    failed = true; client->close();
                }
            });
        deadline.async_wait([&](std::error_code error) {
            if (error) return;
            failed = true;
            connector.cancel(); listener.close();
            if (server) server->close();
            if (client) client->close();
        });
        listener.start(); connector.start();
        io.run();
        if (failed || !connected || !listener_closed || closed != 2 || received != message) return 1;
        std::cout << received << '\n';
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
