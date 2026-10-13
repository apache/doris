// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/client_cache.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "common/check.h"
#include "runtime/exec_env.h"
#include "util/dns_cache.h"
#include "util/thrift_client.h"

namespace doris {

// A server on the loopback that accepts connections and does nothing else: enough to see whether
// a client connected anew, and to close a connection under the client as a server that restarts
// does.
class AcceptingServer {
public:
    AcceptingServer() {
        _listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        DORIS_CHECK(_listen_fd >= 0);
        sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        DORIS_CHECK(::bind(_listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        DORIS_CHECK(::listen(_listen_fd, 16) == 0);
        socklen_t len = sizeof(addr);
        DORIS_CHECK(::getsockname(_listen_fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
        _port = ntohs(addr.sin_port);
    }

    ~AcceptingServer() {
        for (int fd : _accepted) {
            ::close(fd);
        }
        ::close(_listen_fd);
    }

    int port() const { return _port; }

    // Accepts the next connection, waiting at most timeout_ms. Returns its fd, or -1 if none came.
    int accept_within(int timeout_ms) {
        pollfd pfd {_listen_fd, POLLIN, 0};
        if (::poll(&pfd, 1, timeout_ms) <= 0) {
            return -1;
        }
        int fd = ::accept(_listen_fd, nullptr, nullptr);
        DORIS_CHECK(fd >= 0);
        _accepted.push_back(fd);
        return fd;
    }

    // Closes the server's end of an accepted connection.
    void close_connection(int fd) {
        std::erase(_accepted, fd);
        ::close(fd);
    }

private:
    int _listen_fd = -1;
    int _port = 0;
    std::vector<int> _accepted;
};

class ClientCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // The cache resolves the host of every client it hands out; the test servers are on an IP
        // already.
        _saved_dns_cache = ExecEnv::GetInstance()->_dns_cache;
        ExecEnv::GetInstance()->_dns_cache = &_dns_cache;
    }

    void TearDown() override { ExecEnv::GetInstance()->_dns_cache = _saved_dns_cache; }

    static TNetworkAddress address_of(const AcceptingServer& server) {
        TNetworkAddress address;
        address.hostname = "127.0.0.1";
        address.port = server.port();
        return address;
    }

private:
    DNSCache _dns_cache {[](const std::string& hostname, std::string& ip, bool, int*) {
        ip = hostname;
        return Status::OK();
    }};
    DNSCache* _saved_dns_cache = nullptr;
};

TEST_F(ClientCacheTest, peer_closed_tells_a_connection_the_server_closed_from_an_idle_one) {
    AcceptingServer server;
    ThriftClient<FrontendServiceClient> client("127.0.0.1", server.port());
    ASSERT_TRUE(client.open().ok());
    int accepted = server.accept_within(5000);
    ASSERT_GE(accepted, 0);

    // Open and idle: nothing to read, and the check does not wait for anything.
    EXPECT_FALSE(client.peer_closed());

    // The server goes away (it restarted): its end of stream reaches the client.
    server.close_connection(accepted);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!client.peer_closed() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(client.peer_closed());
}

TEST_F(ClientCacheTest, an_idle_cached_client_is_handed_out_again) {
    AcceptingServer server;
    FrontendServiceClientCache cache;
    FrontendServiceClient* cached = nullptr;
    {
        Status status;
        FrontendServiceConnection connection(&cache, address_of(server), &status);
        ASSERT_TRUE(status.ok()) << status;
        cached = connection.operator->();
    }
    ASSERT_GE(server.accept_within(5000), 0);

    Status status;
    FrontendServiceConnection connection(&cache, address_of(server), &status);
    ASSERT_TRUE(status.ok()) << status;
    // The same client, on the same connection: nothing connected anew.
    EXPECT_EQ(cached, connection.operator->());
    EXPECT_LT(server.accept_within(200), 0);
}

TEST_F(ClientCacheTest, a_cached_client_whose_connection_the_server_closed_is_not_handed_out) {
    AcceptingServer server;
    FrontendServiceClientCache cache;
    {
        Status status;
        FrontendServiceConnection connection(&cache, address_of(server), &status);
        ASSERT_TRUE(status.ok()) << status;
    }
    int accepted = server.accept_within(5000);
    ASSERT_GE(accepted, 0);

    // The server restarts while the client sits in the cache. A call on that client would fail -
    // and a fetchSplitBatch cannot be sent again once it may have been read - so the cache
    // connects anew instead of handing it out.
    server.close_connection(accepted);
    bool connected_anew = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!connected_anew && std::chrono::steady_clock::now() < deadline) {
        Status status;
        FrontendServiceConnection connection(&cache, address_of(server), &status);
        ASSERT_TRUE(status.ok()) << status;
        connected_anew = server.accept_within(100) >= 0;
    }
    EXPECT_TRUE(connected_anew);
}

} // namespace doris
