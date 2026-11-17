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

#include <arrow/util/config.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>
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
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "exec/scan/file_scan_range_utils.h"
#include "exec/sink/load_stream_stub.h"
#include "exec/sink/writer/maxcompute/vmc_table_writer.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "gen_cpp/function_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/scoped_temp_dir.h"
#include "util/brpc_client_cache.h"
#include "util/client_cache.h"
#include "util/client_connection_provider.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/defer_op.h"
#include "util/dns_cache.h"
#include "util/private_rpc_tls.h"
#include "util/thrift_rpc_helper.h"
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
        TFetchSplitBatchRequest request;
        request.__set_native_file_encryption_version(1);
        connection->fetchSplitBatch(result, request);
        EXPECT_TRUE(result.__isset.status);
        EXPECT_EQ(TStatusCode::OK, result.status.status_code);
        ASSERT_TRUE(result.__isset.splits);
        ASSERT_EQ(1, result.splits.size());
        const auto& ranges = result.splits[0].scan_range.ext_scan_range.file_scan_range.ranges;
        ASSERT_EQ(1, ranges.size());
        EXPECT_EQ(TFileFormatType::FORMAT_PARQUET_PME, ranges[0].format_type);
        const auto& iceberg = ranges[0].table_format_params.iceberg_params;
        EXPECT_EQ(std::string(16, 'd'), iceberg.encryption_info.key);
        EXPECT_EQ(std::string("\0\xff", 2), iceberg.encryption_info.aad_prefix);
        ASSERT_EQ(2, iceberg.delete_files.size());
        EXPECT_EQ(std::string(16, 'p'), iceberg.delete_files[0].encryption_info.key);
        EXPECT_EQ(std::string(16, 'e'), iceberg.delete_files[1].encryption_info.key);
        EXPECT_EQ(std::string("\0\xfe", 2), iceberg.delete_files[0].encryption_info.aad_prefix);
        EXPECT_EQ(std::string("\0\xfd", 2), iceberg.delete_files[1].encryption_info.aad_prefix);
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
    void fetchSplitBatch(TFetchSplitBatchResult& result,
                         const TFetchSplitBatchRequest& request) override {
        EXPECT_TRUE(request.__isset.native_file_encryption_version);
        EXPECT_EQ(1, request.native_file_encryption_version);
        TStatus status;
        status.__set_status_code(TStatusCode::OK);
        result.__set_status(status);
        // Real TLS RPCs must preserve independent binary file payloads on cached and reopened sockets.
        auto key = [](char value, const std::string& aad) {
            TFileEncryptionInfo info;
            info.__set_version(1);
            info.__set_key(std::string(16, value));
            info.__set_aad_prefix(aad);
            return info;
        };
        TIcebergDeleteFileDesc position;
        position.__set_encryption_info(key('p', std::string("\0\xfe", 2)));
        TIcebergDeleteFileDesc equality;
        equality.__set_encryption_info(key('e', std::string("\0\xfd", 2)));
        TIcebergFileDesc iceberg;
        iceberg.__set_encryption_info(key('d', std::string("\0\xff", 2)));
        iceberg.__set_delete_files({position, equality});
        TTableFormatFileDesc table;
        table.__set_table_format_type("iceberg");
        table.__set_iceberg_params(iceberg);
        TFileRangeDesc range;
        range.__set_format_type(TFileFormatType::FORMAT_PARQUET_PME);
        range.__set_table_format_params(table);
        TFileScanRange file_scan;
        file_scan.__set_ranges({range});
        TExternalScanRange external;
        external.__set_file_scan_range(file_scan);
        TScanRange scan;
        scan.__set_ext_scan_range(external);
        TScanRangeLocations location;
        location.__set_scan_range(scan);
        result.__set_splits({location});
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

TEST_F(InternalRpcTlsTest, JavaWriterCallbacksUseLocalPolicyInsteadOfSinkProperties) {
    auto* env = ExecEnv::GetInstance();
    ClusterInfo cluster;
    auto* previous = env->cluster_info();
    env->set_cluster_info(&cluster);
    Defer restore {[&] { env->set_cluster_info(previous); }};
    RuntimeState state;
    state.set_exec_env(env);
    TMaxComputeTableSink sink;
    sink.__set_properties({{"fe_rpc_tls_enabled", "false"},
                           {"fe_rpc_tls_certificate_path", "untrusted-property"}});
    TDataSink descriptor;
    descriptor.__set_max_compute_table_sink(sink);
    VMCTableWriter writer(descriptor, {}, nullptr, nullptr);
    writer._state = &state;
    auto options = writer._build_base_writer_params();
    ASSERT_EQ(options.at("fe_rpc_tls_enabled"), "true");
    EXPECT_EQ(options.at("fe_rpc_tls_certificate_path"), config::tls_certificate_path);
    EXPECT_EQ(options.at("fe_rpc_tls_private_key_path"), config::tls_private_key_path);
    EXPECT_EQ(options.at("fe_rpc_tls_ca_certificate_path"), config::tls_ca_certificate_path);
    EXPECT_EQ(options.at("fe_rpc_connect_timeout_ms"),
              std::to_string(static_cast<int64_t>(config::thrift_connect_timeout_seconds) * 1000));
    config::tls_excluded_protocols += ",thrift";
    EXPECT_EQ(writer._build_base_writer_params().at("fe_rpc_tls_enabled"), "false");
}

TEST_F(InternalRpcTlsTest, LazySplitClientAuthenticatesBothPeers) {
    EXPECT_TRUE(exchange("fe"));
}

TEST_F(InternalRpcTlsTest, NativeFileKeysRequireBothProtectedProtocolsAndAnExplicitReaderFormat) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET_PME);
    for (int content : {0, 1, 2}) {
        TFileEncryptionInfo info;
        info.__set_version(1);
        info.__set_key(std::string(16, 'k'));
        info.__set_aad_prefix("");
        TIcebergFileDesc iceberg;
        if (content == 0) {
            iceberg.__set_encryption_info(info);
        } else {
            TIcebergDeleteFileDesc file;
            file.__set_content(content);
            file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
            file.__set_encryption_info(info);
            iceberg.__set_delete_files({file});
        }
        TTableFormatFileDesc table;
        table.__set_table_format_type("iceberg");
        table.__set_iceberg_params(iceberg);
        TFileRangeDesc range;
        range.__set_table_format_params(table);
        // Protected transports cannot grant reader support when the installed Parquet bundle lacks PME.
#if defined(PARQUET_REQUIRE_ENCRYPTION) && __has_include(<parquet/encryption/encryption_internal.h>)
        ASSERT_TRUE(validate_file_scan_support(params, &range).ok());
#else
        EXPECT_FALSE(validate_file_scan_support(params, &range).ok());
#endif

        for (const auto* excluded : {"brpc", "thrift"}) {
            config::tls_excluded_protocols = excluded;
            EXPECT_FALSE(validate_file_scan_support(params, &range).ok());
        }
        config::tls_excluded_protocols = "http,arrowflight";
        config::enable_tls = false;
        EXPECT_FALSE(validate_file_scan_support(params, &range).ok());
        config::enable_tls = true;
        config::tls_verify_mode = "verify_peer";
        EXPECT_FALSE(validate_file_scan_support(params, &range).ok());
        config::tls_verify_mode = "verify_fail_if_no_peer_cert";

        // A markerless optional key could be silently ignored by a downgraded process.
        range.__set_format_type(TFileFormatType::FORMAT_PARQUET);
        EXPECT_FALSE(validate_file_scan_support(params, &range).ok());
    }
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

// Gtest assertion/exception macros inflate the complexity of this cache failure/recovery matrix.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(InternalRpcTlsTest, BrpcCacheReturnsNullForTlsConfigurationFailuresAndRecovers) {
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
    const std::string endpoint = butil::endpoint2str(server.listen_address()).c_str();
    auto restore = [&] {
        for (const std::string name : {"be.pem", "be.key", "ca.pem"}) {
            std::filesystem::copy_file(path(name), path("brpc-client-" + name),
                                       std::filesystem::copy_options::overwrite_existing);
        }
        config::tls_certificate_path = path("brpc-client-be.pem");
        config::tls_private_key_path = path("brpc-client-be.key");
        config::tls_ca_certificate_path = path("brpc-client-ca.pem");
        config::tls_verify_mode = "verify_fail_if_no_peer_cert";
    };
    Defer restore_configuration {restore};
    auto probe = [](const std::shared_ptr<PBackendService_Stub>& stub) {
        ASSERT_NE(nullptr, stub);
        brpc::Controller controller;
        controller.set_timeout_ms(3000);
        controller.set_max_retry(0);
        PExecPlanFragmentRequest request;
        PExecPlanFragmentResult response;
        stub->exec_plan_fragment(&controller, &request, &response, nullptr);
        ASSERT_FALSE(controller.Failed()) << controller.ErrorText();
        EXPECT_EQ(0, response.status().status_code());
    };
    const std::vector<std::pair<std::string, std::function<void()>>> faults {
            {"missing certificate",
             [] { EXPECT_TRUE(std::filesystem::remove(path("brpc-client-be.pem"))); }},
            {"missing key",
             [] { EXPECT_TRUE(std::filesystem::remove(path("brpc-client-be.key"))); }},
            {"missing CA",
             [] { EXPECT_TRUE(std::filesystem::remove(path("brpc-client-ca.pem"))); }},
            {"unsupported verification", [] { config::tls_verify_mode = "verify_none"; }}};
    for (const auto& [name, fault] : faults) {
        SCOPED_TRACE(name);
        restore();
        BrpcClientCache<PBackendService_Stub> cache;
        auto healthy = cache.get_client(endpoint);
        probe(healthy);
        fault();
        // Static identity files need not remain readable for an already authenticated connection.
        EXPECT_EQ(healthy, cache.get_client(endpoint));
        probe(healthy);
        EXPECT_EQ(1, cache.erase(endpoint));
        healthy.reset();
        std::shared_ptr<PBackendService_Stub> failed;
        EXPECT_NO_THROW(failed = cache.get_client(endpoint));
        EXPECT_EQ(nullptr, failed);
        EXPECT_EQ(0, cache.size());
        BrpcClientCache<PBackendService_Stub> fresh;
        EXPECT_NO_THROW(failed = fresh.get_client(endpoint));
        EXPECT_EQ(nullptr, failed);
        EXPECT_EQ(0, fresh.size());
        EXPECT_NO_THROW(failed = fresh.get_new_client_no_cache(endpoint));
        EXPECT_EQ(nullptr, failed);
        EXPECT_EQ(0, fresh.size());
        restore();
        probe(cache.get_client(endpoint));
        probe(fresh.get_client(endpoint));
    }
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

TEST_F(InternalRpcTlsTest, RemoteFrontendCallbacksRetainPlaintextAcrossReopenAndMasterChange) {
    // The real client cache resolves even numeric endpoints through ExecEnv's DNS cache.
    DNSCache dns;
    auto* previous_dns = std::exchange(ExecEnv::GetInstance()->_dns_cache, &dns);
    Defer restore_dns {[&] { ExecEnv::GetInstance()->_dns_cache = previous_dns; }};
    auto processor =
            std::make_shared<FrontendServiceProcessor>(std::make_shared<TlsSplitService>());
    config::enable_tls = false;
    ThriftServer first("remote-frontend-first", processor, 0);
    ThriftServer second("remote-frontend-second", processor, 0);
    first.set_server_socket(
            std::make_shared<apache::thrift::transport::TServerSocket>("127.0.0.1", 0));
    second.set_server_socket(
            std::make_shared<apache::thrift::transport::TServerSocket>("127.0.0.1", 0));
    ASSERT_TRUE(first.start().ok());
    Defer stop_first {[&] { first.stop_for_testing(); }};
    ASSERT_TRUE(second.start().ok());
    Defer stop_second {[&] { second.stop_for_testing(); }};
    config::enable_tls = true;
    ClientCache<FrontendServiceClient> remote_cache(2, false);
    TNetworkAddress address;
    address.__set_hostname("127.0.0.1");
    address.__set_port(first._server_socket->getPort());
    Status status;
    {
        FrontendServiceConnection connection(&remote_cache, address, 1000, &status, 0);
        ASSERT_TRUE(status.ok()) << status;
        expect_successful_split(connection);
        ASSERT_TRUE(connection.reopen(1000).ok());
        expect_successful_split(connection);
    }
    // The address-provider retry must keep the remote cache when a new master is returned.
    int attempt = 0;
    auto call = [&](FrontendServiceConnection& connection) {
        expect_successful_split(connection);
        if (attempt++ == 0) {
            address.__set_port(second._server_socket->getPort());
            throw apache::thrift::transport::TTransportException("fixture master changed");
        }
    };
    auto result = ThriftRpcHelper::rpc<FrontendServiceClient>([&] { return address; }, call, 1000,
                                                              &remote_cache);
    EXPECT_TRUE(result.ok()) << result;
    EXPECT_EQ(2, attempt);
}

class RemoteStreamService final : public PBackendService {
public:
    ~RemoteStreamService() override {
        for (auto stream : streams) {
            brpc::StreamClose(stream);
        }
    }

    void open_load_stream(google::protobuf::RpcController* controller,
                          const POpenLoadStreamRequest*, POpenLoadStreamResponse* response,
                          google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        brpc::StreamId stream;
        if (brpc::StreamAccept(&stream, *static_cast<brpc::Controller*>(controller), nullptr) !=
            0) {
            controller->SetFailed("Fixture stream acceptance failed");
            return;
        }
        streams.push_back(stream);
        response->mutable_status()->set_status_code(0);
    }

    std::vector<brpc::StreamId> streams;
};

TEST_F(InternalRpcTlsTest, RemoteDorisStreamingWritesOpenPlaintextStreamsWithPrivateTlsEnabled) {
    RemoteStreamService service;
    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
    {
        // BRPC 1.4 retains its bthread keytable pool after Join; keep stream/client allocations checked.
        debug::ScopedLeakCheckDisabler disable_lsan;
        ASSERT_EQ(0, server.Start("127.0.0.1:0", nullptr));
    }
    Defer shutdown {[&] {
        server.Stop(0);
        server.Join();
    }};
    InternalServiceClientCache cache("baidu_std", "", "streaming");
    NodeInfo node {};
    node.id = 1;
    node.host = "127.0.0.1";
    node.brpc_port = server.listen_address().port;
    OlapTableSchemaParam schema;
    ASSERT_TRUE(schema.init(TOlapTableSchemaParam()).ok());
    LoadStreamStubs streams(2, UniqueId(1, 2), 1, std::make_shared<IndexToTabletSchema>(),
                            std::make_shared<IndexToEnableMoW>());
    auto result = streams.open(&cache, node, 1, schema, {}, 2, 1000, false, true);
    EXPECT_TRUE(result.ok()) << result;
    EXPECT_EQ(2, service.streams.size());
    streams.cancel(Status::Cancelled("Fixture complete"));
}

// Gtest assertions inflate the complexity of this two-connection-group policy matrix.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(InternalRpcTlsTest, RemoteDorisBackendDoesNotUseTheLocalPrivateTlsPolicy) {
    TlsEchoService service;
    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start("127.0.0.1:0", nullptr));
    Defer shutdown {[&] {
        server.Stop(0);
        server.Join();
    }};
    const std::string host = "127.0.0.1";
    const int port = server.listen_address().port;
    const std::string endpoint = host + ":" + std::to_string(port);
    for (const auto& group : {std::string(), std::string("streaming")}) {
        InternalServiceClientCache cache("baidu_std", "", group);
        auto remote = cache.get_client(host, port, client::RpcPurpose::EXTERNAL_SERVICE);
        auto fresh = cache.get_new_client_no_cache(endpoint, "", "", "", 500, 0,
                                                   client::RpcPurpose::EXTERNAL_SERVICE);
        auto internal = cache.get_client(host, port);
        ASSERT_NE(nullptr, remote);
        ASSERT_NE(nullptr, fresh);
        ASSERT_NE(nullptr, internal);
        EXPECT_NE(remote, internal);
        EXPECT_EQ(remote, cache.get_client(host, port, client::RpcPurpose::EXTERNAL_SERVICE));
        auto call = [&](const auto& stub) {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            cntl.set_max_retry(0);
            PExecPlanFragmentRequest request;
            PExecPlanFragmentResult response;
            stub->exec_plan_fragment(&cntl, &request, &response, nullptr);
            return !cntl.Failed() && response.status().status_code() == 0;
        };
        EXPECT_TRUE(call(remote));
        EXPECT_TRUE(call(fresh));
        auto http = cache.get_new_client_no_cache(endpoint, "http", "", "", 500, 0,
                                                  client::RpcPurpose::EXTERNAL_SERVICE);
        ASSERT_NE(nullptr, http);
        EXPECT_TRUE(call(http));
        // A warm external connection at the same address must not bypass internal TLS.
        EXPECT_FALSE(call(internal));
        EXPECT_TRUE(call(remote));
        EXPECT_EQ(1, cache.erase(host, port, client::RpcPurpose::EXTERNAL_SERVICE));
        EXPECT_EQ(internal, cache.get_client(host, port));
        EXPECT_TRUE(call(cache.get_client(host, port, client::RpcPurpose::EXTERNAL_SERVICE)));
        std::vector<std::string> endpoints;
        cache.get_all(&endpoints);
        EXPECT_EQ(std::vector<std::string> {endpoint}, endpoints);
        EXPECT_TRUE(cache.exist(endpoint));
        EXPECT_EQ(2, cache.erase(endpoint));
        EXPECT_EQ(0, cache.size());
        EXPECT_NE(nullptr, cache.get_client(host, port, client::RpcPurpose::EXTERNAL_SERVICE));
        cache.clear();
        EXPECT_EQ(0, cache.size());
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
