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

#include "util/thrift_rpc_helper.h"

#include <gen_cpp/FrontendService.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportException.h>

#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "util/client_cache.h"
#include "util/defer_op.h"
#include "util/dns_cache.h"

namespace doris {

TEST(ThriftRpcHelperTest, DisabledModeTransportRetryUsesLiveMasterProvider) {
    ClusterInfo cluster_info;
    ClientCache<FrontendServiceClient> cache;
    DNSCache dns([](const std::string&, std::string& ip, bool, int* error) {
        ip = "127.0.0.1";
        *error = 0;
        return Status::OK();
    });
    auto* env = ExecEnv::GetInstance();
    auto* old_cluster_info = env->_cluster_info;
    auto* old_cache = env->_frontend_client_cache;
    auto* old_dns = env->_dns_cache;
    auto* old_helper_env = ThriftRpcHelper::get_exec_env();
    auto old_retry_interval = config::thrift_client_retry_interval_ms;
    Defer restore {[&] {
        env->_cluster_info = old_cluster_info;
        env->_frontend_client_cache = old_cache;
        env->_dns_cache = old_dns;
        ThriftRpcHelper::setup(old_helper_env);
        config::thrift_client_retry_interval_ms = old_retry_interval;
    }};
    env->_cluster_info = &cluster_info;
    env->_frontend_client_cache = &cache;
    env->_dns_cache = &dns;
    ThriftRpcHelper::setup(env);
    config::thrift_client_retry_interval_ms = 0;
    // Exercise the real client-cache and transport retry path without a network server.
    cache._client_factory = [](const TNetworkAddress& address, void** key) -> ThriftClientImpl* {
        auto* client = new ThriftClient<FrontendServiceClient>(address.hostname, address.port);
        client->_transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        *key = client->iface();
        return client;
    };
    TNetworkAddress master;
    master.__set_hostname("old-master");
    master.__set_port(9020);
    int calls = 0;
    auto status = ThriftRpcHelper::rpc_fe_with_master_refresh(
            [&] { return master; },
            [&](ClientConnection<FrontendServiceClient>& client) {
                if (calls++ == 0) {
                    master.__set_hostname("new-master");
                    throw apache::thrift::transport::TTransportException(
                            "old master closed socket");
                }
                EXPECT_EQ(cache._client_cache_helper._client_hostport_map.at(client._client),
                          master);
            },
            [] { return TStatus(); }, []() -> const TNetworkAddress* { return nullptr; });
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(calls, 2);
    EXPECT_FALSE(cluster_info.in_graceful_shutdown());
}

TEST(ThriftRpcHelperTest, NotMasterHintDoesNotOverwriteHeartbeatAddress) {
    TNetworkAddress master;
    master.__set_hostname("old-master");
    master.__set_port(9020);
    TNetworkAddress hint = master;
    hint.__set_hostname("new-master");
    TStatus response;
    int calls = 0;
    auto status = ThriftRpcHelper::_retry_fe_rpc(
            [&]() { return master; },
            [&](const TNetworkAddress& target) {
                EXPECT_EQ(target.hostname, calls == 0 ? "old-master" : "new-master");
                response.__set_status_code(calls++ == 0 ? TStatusCode::NOT_MASTER
                                                        : TStatusCode::OK);
                return Status::OK();
            },
            [&]() { return response; }, [&]() { return &hint; }, true, 0);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(master.hostname, "old-master");
}

TEST(ThriftRpcHelperTest, TransportFailureUsesFreshHeartbeat) {
    TNetworkAddress master;
    master.__set_hostname("old-master");
    master.__set_port(9020);
    TStatus response;
    response.__set_status_code(TStatusCode::OK);
    int calls = 0;
    auto status = ThriftRpcHelper::_retry_fe_rpc(
            [&]() { return master; },
            [&](const TNetworkAddress& target) {
                if (calls++ == 0) {
                    master.__set_hostname("new-master");
                    return Status::RpcError("connection closed during election");
                }
                EXPECT_EQ(target.hostname, "new-master");
                return Status::OK();
            },
            [&]() { return response; }, []() -> const TNetworkAddress* { return nullptr; }, true,
            0);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(calls, 2);
}

TEST(ThriftRpcHelperTest, ApplicationFailureAndDisabledModeDoNotRetry) {
    TNetworkAddress master;
    TStatus response;
    response.__set_status_code(TStatusCode::ANALYSIS_ERROR);
    int calls = 0;
    auto attempt = [&](const TNetworkAddress&) {
        ++calls;
        return Status::OK();
    };
    auto hint = []() -> const TNetworkAddress* { return nullptr; };
    EXPECT_TRUE(ThriftRpcHelper::_retry_fe_rpc([&]() { return master; }, attempt,
                                               [&]() { return response; }, hint, true, 0)
                        .ok());
    EXPECT_EQ(calls, 1);
    response.__set_status_code(TStatusCode::NOT_MASTER);
    EXPECT_TRUE(ThriftRpcHelper::_retry_fe_rpc([&]() { return master; }, attempt,
                                               [&]() { return response; }, hint, false, 0)
                        .ok());
    EXPECT_EQ(calls, 2);
}

TEST(ThriftRpcHelperTest, ElectionWithoutMasterIsBounded) {
    TNetworkAddress master;
    TStatus response;
    response.__set_status_code(TStatusCode::NOT_MASTER);
    int calls = 0;
    auto status = ThriftRpcHelper::_retry_fe_rpc([&]() { return master; },
                                                 [&](const TNetworkAddress&) {
                                                     ++calls;
                                                     return Status::OK();
                                                 },
                                                 [&]() { return response; },
                                                 []() -> const TNetworkAddress* { return nullptr; },
                                                 true, 0);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.status_code, TStatusCode::NOT_MASTER);
    EXPECT_EQ(calls, 6);
}

} // namespace doris
