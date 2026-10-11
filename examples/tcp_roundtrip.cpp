#include <cpp_network/connection.hpp>
#include <asio.hpp>
#include <chrono>
#include <iostream>
#include <string>

// 한 프로세스 안의 두 실제 소켓으로 왕복을 확인한다. 접속 준비는 예제의 책임이다.
int main() {
    try {
        using asio::ip::tcp;
        asio::io_context io;
        tcp::acceptor listener(io, {asio::ip::address_v4::loopback(), 0});
        tcp::socket client_socket(io);
        client_socket.connect(listener.local_endpoint());
        auto server_socket = listener.accept();
        listener.close();
        std::string received;
        const std::string message = "cpp-network independent TCP roundtrip";
        int closed = 0;
        bool failed = false;
        asio::steady_timer deadline(io, std::chrono::seconds(5));
        auto on_closed = [&](auto) { if (++closed == 2) deadline.cancel(); };
        cpp_network::Connection* server_handle = nullptr;
        cpp_network::Connection server(std::move(server_socket), {}, {
            [&](auto bytes) {
                if (server_handle->send(bytes) != cpp_network::SendResult::accepted) {
                    failed = true; server_handle->close();
                }
                return true;
            }, on_closed, {}});
        server_handle = &server;
        cpp_network::Connection* client_handle = nullptr;
        cpp_network::Connection client(std::move(client_socket), {}, {
            [&](auto bytes) {
                received.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                if (received.size() >= message.size()) { client_handle->close(); server.close(); }
                return true;
            }, on_closed, {}});
        client_handle = &client;
        deadline.async_wait([&](std::error_code error) {
            if (!error) { failed = true; client.close(); server.close(); }
        });
        server.start(); client.start();
        if (client.send(std::as_bytes(std::span(message))) != cpp_network::SendResult::accepted) return 1;
        io.run();
        if (failed || received != message || closed != 2) return 1;
        std::cout << received << '\n';
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

// TCP(Transmission Control Protocol): 순서 있는 신뢰성 바이트 스트림 전송 규약.
// io(Input/Output): 입출력 실행 문맥. echo: 받은 바이트를 그대로 돌려주는 처리.
