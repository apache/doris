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

#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

namespace doris {

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
