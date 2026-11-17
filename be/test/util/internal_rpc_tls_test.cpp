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

#include <brpc/channel.h>
#include <brpc/server.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <spawn.h>
#include <sys/wait.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSSLServerSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "gen_cpp/function_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/exec_env.h"
#include "testutil/scoped_temp_dir.h"
#include "util/brpc_client_cache.h"
#include "util/client_cache.h"
#include "util/client_connection_provider.h"
#include "util/defer_op.h"
#include "util/dns_cache.h"
#include "util/private_rpc_tls.h"
#include "util/thrift_server.h"

namespace doris {
namespace {

class InternalRpcTlsTest : public testing::Test {
protected:
    static inline std::unique_ptr<test::ScopedTempDirectory> certificates;

    static void SetUpTestSuite() {
        certificates = std::make_unique<test::ScopedTempDirectory>("private-rpc-certificates");
        const char* root = std::getenv("ROOT");
        ASSERT_NE(root, nullptr);
        std::string script =
                std::string(root) +
                "/docker/thirdparties/docker-compose/iceberg/encryption/generate-tls-fixture.sh";
        std::string directory = certificates->path();
        std::string log = directory + "/generation.log";
        std::vector<char*> arguments {const_cast<char*>("bash"), script.data(), directory.data(),
                                      nullptr};
        posix_spawn_file_actions_t actions;
        ASSERT_EQ(posix_spawn_file_actions_init(&actions), 0);
        Defer release_actions {[&] { posix_spawn_file_actions_destroy(&actions); }};
        ASSERT_EQ(posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log.c_str(),
                                                   O_WRONLY | O_CREAT | O_TRUNC, 0600),
                  0);
        ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO), 0);
        pid_t child;
        ASSERT_EQ(posix_spawnp(&child, "bash", &actions, nullptr, arguments.data(), environ), 0);
        int status = 0;
        ASSERT_EQ(waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0) << "ephemeral TLS fixture generation failed";
    }

    static void TearDownTestSuite() { certificates.reset(); }

    void SetUp() override {
        _enabled = config::enable_tls;
        _excluded = config::tls_excluded_protocols;
        _mode = config::tls_verify_mode;
        _certificate = config::tls_certificate_path;
        _key = config::tls_private_key_path;
        _ca = config::tls_ca_certificate_path;
        _refresh = config::tls_cert_refresh_interval_seconds;
        _connect_timeout = config::thrift_connect_timeout_seconds;
        config::tls_cert_refresh_interval_seconds = 0;
        config::enable_tls = true;
        config::tls_excluded_protocols = "http,arrowflight";
        config::tls_verify_mode = "verify_fail_if_no_peer_cert";
        config::tls_certificate_path = path("be.pem");
        config::tls_private_key_path = path("be.key");
        config::tls_ca_certificate_path = path("ca.pem");
    }

    void TearDown() override {
        config::enable_tls = _enabled;
        config::tls_excluded_protocols = _excluded;
        config::tls_verify_mode = _mode;
        config::tls_certificate_path = _certificate;
        config::tls_private_key_path = _key;
        config::tls_ca_certificate_path = _ca;
        config::tls_cert_refresh_interval_seconds = _refresh;
        config::thrift_connect_timeout_seconds = _connect_timeout;
    }

    static std::string path(const std::string& name) {
        return (certificates->path() / name).string();
    }

    static void expect_successful_split(FrontendServiceConnection& connection) {
        TFetchSplitBatchResult result;
        connection->fetchSplitBatch(result, TFetchSplitBatchRequest());
        EXPECT_TRUE(result.__isset.status);
        EXPECT_EQ(TStatusCode::OK, result.status.status_code);
    }

    static void configure_test_channel(brpc::ChannelOptions* options, const std::string& mode) {
        options->protocol = "h2";
        options->timeout_ms = 1000;
        options->max_retry = 0;
        if (mode == "plaintext") {
            return;
        }
        ASSERT_TRUE(client::configure_brpc_channel_options(options).ok());
        if (mode == "missing") {
            options->mutable_ssl_options()->client_cert.certificate.clear();
            options->mutable_ssl_options()->client_cert.private_key.clear();
        } else if (mode == "untrusted") {
            options->mutable_ssl_options()->client_cert.certificate = path("untrusted.pem");
            options->mutable_ssl_options()->client_cert.private_key = path("untrusted.key");
        }
    }

    static bool exchange(const std::string& identity) {
        using apache::thrift::transport::TSSLServerSocket;
        using apache::thrift::transport::TSSLSocketFactory;
        using apache::thrift::transport::TSocket;
        auto factory = std::make_shared<TSSLSocketFactory>();
        factory->server(true);
        factory->authenticate(true);
        factory->loadCertificate(path(identity + ".pem").c_str());
        factory->loadPrivateKey(path(identity + ".key").c_str());
        factory->loadTrustedCertificates(path("ca.pem").c_str());
        TSSLServerSocket server("127.0.0.1", 0, factory);
        server.setAcceptTimeout(5000);
        server.listen();
        auto worker = std::async(std::launch::async, [&] {
            try {
                auto peer = server.accept();
                auto socket = std::static_pointer_cast<TSocket>(peer);
                socket->setRecvTimeout(3000);
                socket->setSendTimeout(3000);
                uint8_t value = 0;
                peer->readAll(&value, 1);
                peer->write(&value, 1);
                peer->flush();
                peer->close();
                return value == 42;
            } catch (const std::exception&) {
                return false;
            }
        });
        bool received = false;
        try {
            auto peer = client::create_thrift_client_socket("127.0.0.1", server.getPort());
            peer->setConnTimeout(3000);
            peer->setRecvTimeout(3000);
            peer->setSendTimeout(3000);
            peer->open();
            uint8_t value = 42;
            peer->write(&value, 1);
            peer->flush();
            peer->readAll(&value, 1);
            received = value == 42;
            peer->close();
        } catch (const std::exception&) {
            // Every rejected identity or plaintext downgrade must fail the complete exchange.
        }
        bool accepted = worker.get();
        server.close();
        return received && accepted;
    }

private:
    bool _enabled;
    int _refresh;
    int _connect_timeout;
    std::string _excluded;
    std::string _mode;
    std::string _certificate;
    std::string _key;
    std::string _ca;
};

class TlsEchoService final : public PBackendService {
public:
    std::atomic<int> calls {0};

    void exec_plan_fragment(google::protobuf::RpcController*, const PExecPlanFragmentRequest*,
                            PExecPlanFragmentResult* response,
                            google::protobuf::Closure* done) override {
        calls.fetch_add(1);
        response->mutable_status()->set_status_code(0);
        done->Run();
    }
};

class TlsHeartbeatService final : public HeartbeatServiceIf {
public:
    void heartbeat(THeartbeatResult& result, const TMasterInfo&) override {
        result.status.__set_status_code(TStatusCode::OK);
        result.backend_info.__set_be_port(1);
        result.backend_info.__set_http_port(1);
    }
};

class TlsSplitService final : public FrontendServiceNull {
public:
    void fetchSplitBatch(TFetchSplitBatchResult& result, const TFetchSplitBatchRequest&) override {
        TStatus status;
        status.__set_status_code(TStatusCode::OK);
        result.__set_status(status);
    }
};

class PlainBrokerService final : public TPaloBrokerServiceNull {
public:
    void ping(TBrokerOperationStatus& result, const TBrokerPingBrokerRequest&) override {
        result.__set_statusCode(TBrokerOperationStatusCode::OK);
    }
};

class PlainFunctionService final : public PFunctionService {
public:
    void check_fn(google::protobuf::RpcController*, const PCheckFunctionRequest*,
                  PCheckFunctionResponse* result, google::protobuf::Closure* done) override {
        result->mutable_status()->set_status_code(0);
        done->Run();
    }

    void fn_call(google::protobuf::RpcController*, const PFunctionCallRequest*,
                 PFunctionCallResponse* result, google::protobuf::Closure* done) override {
        result->mutable_status()->set_status_code(0);
        done->Run();
    }
};

TEST_F(InternalRpcTlsTest, LazySplitClientAuthenticatesBothPeers) {
    EXPECT_TRUE(exchange("fe"));
}

TEST_F(InternalRpcTlsTest, LazySplitClientRejectsAnUntrustedServer) {
    EXPECT_FALSE(exchange("untrusted"));
}

TEST_F(InternalRpcTlsTest, LazySplitClientRejectsAnotherHostOnTheTrustedCa) {
    EXPECT_FALSE(exchange("wrong-name"));
}

TEST_F(InternalRpcTlsTest, LazySplitServerRejectsAnUntrustedClient) {
    config::tls_certificate_path = path("untrusted.pem");
    config::tls_private_key_path = path("untrusted.key");
    EXPECT_FALSE(exchange("fe"));
}

TEST_F(InternalRpcTlsTest, BrpcChannelsAuthenticateWithTheConfiguredIdentity) {
    brpc::ChannelOptions options;
    ASSERT_TRUE(client::configure_brpc_channel_options(&options).ok());
    ASSERT_TRUE(options.has_ssl_options());
    EXPECT_GT(options.ssl_options().verify.verify_depth, 0);
    EXPECT_EQ(options.ssl_options().verify.ca_file_path, path("ca.pem"));
    EXPECT_EQ(options.ssl_options().client_cert.certificate, path("be.pem"));
    EXPECT_EQ(options.ssl_options().client_cert.private_key, path("be.key"));
    EXPECT_EQ(options.ssl_options().protocols, "TLSv1.2");
}

TEST_F(InternalRpcTlsTest, BrpcListenerRejectsPlaintextAndMissingOrUntrustedClientIdentity) {
    TlsEchoService service;
    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    brpc::ServerOptions server_options;
    ASSERT_TRUE(rpc_tls::configure_server(&server_options).ok());
    ASSERT_EQ(server.Start("127.0.0.1:0", &server_options), 0);
    Defer shutdown {[&] {
        server.Stop(0);
        server.Join();
    }};
    for (const std::string mode : {"trusted", "plaintext", "missing", "untrusted"}) {
        brpc::ChannelOptions options;
        configure_test_channel(&options, mode);
        brpc::Channel channel;
        ASSERT_EQ(channel.Init(server.listen_address(), &options), 0);
        PBackendService_Stub stub(&channel);
        PExecPlanFragmentRequest request;
        PExecPlanFragmentResult result;
        brpc::Controller controller;
        stub.exec_plan_fragment(&controller, &request, &result, nullptr);
        EXPECT_EQ(controller.Failed(), mode != "trusted") << mode << ": " << controller.ErrorText();
        if (!controller.Failed()) {
            EXPECT_EQ(result.status().status_code(), 0);
        }
    }
    EXPECT_EQ(service.calls.load(), 1);
}

TEST_F(InternalRpcTlsTest, ProviderThriftListenerProtectsRealHeartbeatRpc) {
    using apache::thrift::protocol::TBinaryProtocol;
    using apache::thrift::transport::TBufferedTransport;
    using apache::thrift::transport::TSocket;
    using apache::thrift::transport::TTransportException;
    auto handler = std::make_shared<TlsHeartbeatService>();
    auto processor = std::make_shared<HeartbeatServiceProcessor>(handler);
    ThriftServer server("private-tls-test", processor, 0);
    ASSERT_TRUE(rpc_tls::configure_thrift_server(&server, "127.0.0.1", 0).ok());
    ASSERT_TRUE(server.start().ok());
    Defer shutdown {[&] { server.stop_for_testing(); }};
    const int port = server._server_socket->getPort();
    {
        auto socket = client::create_thrift_client_socket("127.0.0.1", port);
        socket->setRecvTimeout(3000);
        auto transport = std::make_shared<TBufferedTransport>(socket);
        HeartbeatServiceClient client(std::make_shared<TBinaryProtocol>(transport));
        for (int attempt = 0; attempt < 2; ++attempt) {
            transport->open();
            THeartbeatResult result;
            client.heartbeat(result, TMasterInfo());
            EXPECT_EQ(result.status.status_code, TStatusCode::OK);
            transport->close();
            EXPECT_FALSE(socket->peek());
            socket->flush();
        }
    }
    {
        auto socket = std::make_shared<TSocket>("127.0.0.1", port);
        socket->setRecvTimeout(3000);
        socket->open();
        auto transport = std::make_shared<TBufferedTransport>(socket);
        HeartbeatServiceClient client(std::make_shared<TBinaryProtocol>(transport));
        THeartbeatResult result;
        EXPECT_THROW(client.heartbeat(result, TMasterInfo()), TTransportException);
        transport->close();
    }
}

TEST_F(InternalRpcTlsTest, InvalidTlsConfigurationCanDestroyAnUnstartedServer) {
    auto handler = std::make_shared<TlsHeartbeatService>();
    auto processor = std::make_shared<HeartbeatServiceProcessor>(handler);
    ThriftServer server("invalid-private-tls-test", processor, 0);
    config::tls_certificate_path.clear();
    EXPECT_FALSE(rpc_tls::configure_thrift_server(&server, "127.0.0.1", 0).ok());
}

TEST_F(InternalRpcTlsTest, UnsupportedListenerModeCanDestroyAnUnstartedServer) {
    auto handler = std::make_shared<TlsHeartbeatService>();
    auto processor = std::make_shared<HeartbeatServiceProcessor>(handler);
    ThriftServer server("unsupported-private-tls-test", processor, 0, 1,
                        ThriftServer::NON_BLOCKING);
    ASSERT_TRUE(rpc_tls::configure_thrift_server(&server, "127.0.0.1", 0).ok());
    EXPECT_FALSE(server.start().ok());
    EXPECT_FALSE(server._started);
    EXPECT_EQ(server._server_thread, nullptr);
}

TEST_F(InternalRpcTlsTest, IdleThriftPeerCannotKeepAnUnauthenticatedWorker) {
    using apache::thrift::protocol::TBinaryProtocol;
    using apache::thrift::transport::TBufferedTransport;
    using apache::thrift::transport::TSocket;
    config::thrift_connect_timeout_seconds = 1;
    auto handler = std::make_shared<TlsHeartbeatService>();
    auto processor = std::make_shared<HeartbeatServiceProcessor>(handler);
    ThriftServer server("idle-private-tls-test", processor, 0);
    ASSERT_TRUE(rpc_tls::configure_thrift_server(&server, "127.0.0.1", 0).ok());
    ASSERT_TRUE(server.start().ok());
    Defer shutdown {[&] { server.stop_for_testing(); }};
    const int port = server._server_socket->getPort();
    {
        TSocket peer("127.0.0.1", port);
        peer.setRecvTimeout(4500);
        peer.open();
        uint8_t value = 0;
        EXPECT_EQ(peer.read(&value, 1), 0);
        peer.close();
    }
    auto socket = client::create_thrift_client_socket("127.0.0.1", port);
    socket->setRecvTimeout(3000);
    auto transport = std::make_shared<TBufferedTransport>(socket);
    transport->open();
    HeartbeatServiceClient client(std::make_shared<TBinaryProtocol>(transport));
    THeartbeatResult result;
    client.heartbeat(result, TMasterInfo());
    EXPECT_EQ(result.status.status_code, TStatusCode::OK);
    transport->close();
}

TEST_F(InternalRpcTlsTest, ClientAuthenticatesBeforeOpenReturnsWithUnlimitedRpcReads) {
    using apache::thrift::transport::TServerSocket;
    using apache::thrift::transport::TTransportException;
    config::thrift_connect_timeout_seconds = 1;
    TServerSocket peer("127.0.0.1", 0);
    peer.setAcceptTimeout(5000);
    peer.listen();
    std::promise<void> release;
    auto released = release.get_future();
    auto accepted = std::async(std::launch::async, [&] {
        auto connection = peer.accept();
        released.wait();
        connection->close();
    });
    Defer shutdown {[&] {
        release.set_value();
        accepted.get();
        peer.close();
    }};
    auto socket = client::create_thrift_client_socket("127.0.0.1", peer.getPort());
    socket->setRecvTimeout(0);
    try {
        socket->open();
        ADD_FAILURE() << "TLS open must authenticate the peer before returning";
    } catch (const TTransportException& failure) {
        EXPECT_EQ(failure.getType(), TTransportException::TIMED_OUT);
    }
    socket->close();
}

TEST_F(InternalRpcTlsTest, FrontendCacheKeepsDnsIdentityAcrossResolvedConnectionsAndReopen) {
    config::tls_certificate_path = path("dns-fe.pem");
    config::tls_private_key_path = path("dns-fe.key");
    auto processor =
            std::make_shared<FrontendServiceProcessor>(std::make_shared<TlsSplitService>());
    ThriftServer server("private-tls-dns-test", processor, 0);
    ASSERT_TRUE(rpc_tls::configure_thrift_server(&server, "127.0.0.1", 0).ok());
    ASSERT_TRUE(server.start().ok());
    Defer shutdown {[&] { server.stop_for_testing(); }};
    config::tls_certificate_path = path("be.pem");
    config::tls_private_key_path = path("be.key");

    DNSCache dns([](const std::string&, std::string& ip, bool, int* error) {
        ip = "127.0.0.1";
        *error = 0;
        return Status::OK();
    });
    auto* previous_dns = std::exchange(ExecEnv::GetInstance()->_dns_cache, &dns);
    Defer restore_dns {[&] { ExecEnv::GetInstance()->_dns_cache = previous_dns; }};
    ClientCache<FrontendServiceClient> cache;
    TNetworkAddress address;
    address.__set_hostname("localhost");
    address.__set_port(server._server_socket->getPort());
    Status status;
    {
        ClientConnection<FrontendServiceClient> connection(&cache, address, 3000, &status, 0);
        ASSERT_TRUE(status.ok()) << status;
        for (int attempt = 0; attempt < 2; ++attempt) {
            expect_successful_split(connection);
            if (attempt == 0) {
                ASSERT_TRUE(connection.reopen(3000).ok());
            }
        }
    }
    {
        ClientConnection<FrontendServiceClient> cached(&cache, address, 3000, &status, 0);
        ASSERT_TRUE(status.ok()) << status;
        expect_successful_split(cached);
    }
    // The resolver deliberately maps both names to one IP; the certificate must still distinguish them.
    address.__set_hostname("other.invalid");
    ClientConnection<FrontendServiceClient> wrong_host(&cache, address, 3000, &status, 0);
    EXPECT_FALSE(status.ok());
}

// Gtest assertion/exception macros inflate the complexity of this acquire/reopen fault matrix.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(InternalRpcTlsTest, ClientCacheReturnsStatusForTlsConstructionFailuresAndRecovers) {
    // Match the initialized BE/BRPC signal policy when exercising Thrift in isolation.
    struct sigaction previous_pipe_action {};
    ASSERT_EQ(sigaction(SIGPIPE, nullptr, &previous_pipe_action), 0);
    struct sigaction ignored_pipe_action {};
    ignored_pipe_action.sa_handler = SIG_IGN;
    sigemptyset(&ignored_pipe_action.sa_mask);
    ASSERT_EQ(sigaction(SIGPIPE, &ignored_pipe_action, nullptr), 0);
    Defer restore_pipe_action {[&] { sigaction(SIGPIPE, &previous_pipe_action, nullptr); }};
    auto processor =
            std::make_shared<FrontendServiceProcessor>(std::make_shared<TlsSplitService>());
    ThriftServer server("private-tls-construction-test", processor, 0);
    ASSERT_TRUE(rpc_tls::configure_thrift_server(&server, "127.0.0.1", 0).ok());
    ASSERT_TRUE(server.start().ok());
    Defer shutdown {[&] { server.stop_for_testing(); }};
    DNSCache dns([](const std::string&, std::string& ip, bool, int* error) {
        ip = "127.0.0.1";
        *error = 0;
        return Status::OK();
    });
    auto* previous_dns = std::exchange(ExecEnv::GetInstance()->_dns_cache, &dns);
    Defer restore_dns {[&] { ExecEnv::GetInstance()->_dns_cache = previous_dns; }};
    TNetworkAddress address;
    address.__set_hostname("localhost");
    address.__set_port(server._server_socket->getPort());
    std::ofstream(path("malformed.pem")) << "not a PEM identity";
    auto restore = [&] {
        config::thrift_connect_timeout_seconds = 3;
        config::tls_certificate_path = path("be.pem");
        config::tls_private_key_path = path("be.key");
    };
    Defer restore_configuration {restore};
    const std::vector<std::pair<std::string, std::function<void()>>> faults {
            {"zero timeout", [] { config::thrift_connect_timeout_seconds = 0; }},
            {"missing certificate", [] { config::tls_certificate_path = path("missing.pem"); }},
            {"malformed certificate", [] { config::tls_certificate_path = path("malformed.pem"); }},
            {"missing key", [] { config::tls_private_key_path = path("missing.key"); }},
            {"malformed key", [] { config::tls_private_key_path = path("malformed.pem"); }}};
    for (const auto& [name, fault] : faults) {
        for (bool reopen_existing : {false, true}) {
            SCOPED_TRACE(name + (reopen_existing ? " reopen" : " acquire"));
            restore();
            ClientCache<FrontendServiceClient> cache;
            Status status;
            if (reopen_existing) {
                FrontendServiceConnection connection(&cache, address, 3000, &status, 0);
                ASSERT_TRUE(status.ok()) << status;
                fault();
                EXPECT_NO_THROW(status = connection.reopen(3000));
                EXPECT_FALSE(connection.is_alive());
            } else {
                fault();
                EXPECT_NO_THROW({
                    FrontendServiceConnection connection(&cache, address, 3000, &status, 0);
                    EXPECT_FALSE(connection.is_alive());
                });
            }
            EXPECT_FALSE(status.ok());
            EXPECT_EQ(std::string::npos, status.to_string().find(certificates->path().string()));
            EXPECT_TRUE(cache._client_cache_helper._client_map.empty());
            restore();
            FrontendServiceConnection recovered(&cache, address, 3000, &status, 0);
            ASSERT_TRUE(status.ok()) << status;
            expect_successful_split(recovered);
        }
    }
}

TEST_F(InternalRpcTlsTest, ExternalFunctionCacheRetainsPlaintextWhilePrivateTlsIsEnabled) {
    PlainFunctionService service;
    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start("127.0.0.1:0", nullptr));
    Defer shutdown {[&] {
        server.Stop(0);
        server.Join();
    }};
    const std::string endpoint = "127.0.0.1:" + std::to_string(server.listen_address().port);
    BrpcClientCache<PFunctionService_Stub> cache("h2");
    auto cached = cache.get_client(endpoint);
    auto fresh = cache.get_new_client_no_cache(endpoint, "h2");
    for (const auto& stub : {cached, fresh}) {
        ASSERT_NE(nullptr, stub);
        brpc::Controller validation;
        validation.set_timeout_ms(1000);
        validation.set_max_retry(0);
        PCheckFunctionRequest check;
        PCheckFunctionResponse checked;
        stub->check_fn(&validation, &check, &checked, nullptr);
        ASSERT_FALSE(validation.Failed()) << validation.ErrorText();
        EXPECT_EQ(0, checked.status().status_code());
        brpc::Controller execution;
        execution.set_timeout_ms(1000);
        execution.set_max_retry(0);
        PFunctionCallRequest call;
        PFunctionCallResponse called;
        stub->fn_call(&execution, &call, &called, nullptr);
        ASSERT_FALSE(execution.Failed()) << execution.ErrorText();
        EXPECT_EQ(0, called.status().status_code());
    }
}

TEST_F(InternalRpcTlsTest, BackendBrokerCacheRetainsTheExternalFilesystemTransport) {
    auto processor =
            std::make_shared<TPaloBrokerServiceProcessor>(std::make_shared<PlainBrokerService>());
    ThriftServer server("plain-broker-test", processor, 0);
    server.set_server_socket(
            std::make_shared<apache::thrift::transport::TServerSocket>("127.0.0.1", 0));
    ASSERT_TRUE(server.start().ok());
    Defer shutdown {[&] { server.stop_for_testing(); }};
    DNSCache dns([](const std::string&, std::string& ip, bool, int* error) {
        ip = "127.0.0.1";
        *error = 0;
        return Status::OK();
    });
    auto* previous_dns = std::exchange(ExecEnv::GetInstance()->_dns_cache, &dns);
    Defer restore_dns {[&] { ExecEnv::GetInstance()->_dns_cache = previous_dns; }};
    BrokerServiceClientCache cache;
    TNetworkAddress address;
    address.__set_hostname("127.0.0.1");
    address.__set_port(server._server_socket->getPort());
    Status status;
    BrokerServiceConnection connection(&cache, address, 3000, &status, 0);
    ASSERT_TRUE(status.ok()) << status;
    for (int attempt = 0; attempt < 2; ++attempt) {
        TBrokerPingBrokerRequest request;
        request.__set_version(TBrokerVersion::VERSION_ONE);
        request.__set_clientId("fixture-backend");
        TBrokerOperationStatus result;
        connection->ping(result, request);
        EXPECT_EQ(TBrokerOperationStatusCode::OK, result.statusCode);
        if (attempt == 0) {
            ASSERT_TRUE(connection.reopen(3000).ok());
        }
    }
}

} // namespace
} // namespace doris
