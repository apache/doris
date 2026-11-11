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

#include "agent/heartbeat_server.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "storage/storage_engine.h"
#include "util/defer_op.h"

namespace doris {

class HeartbeatLogSink : public google::LogSink {
public:
    void send(google::LogSeverity /*severity*/, const char* /*full_filename*/,
              const char* /*base_filename*/, int /*line*/, const google::LogMessageTime& /*time*/,
              const char* message, std::size_t message_len) override {
        messages.append(message, message_len);
    }

    std::string messages;
};

TEST(HeartbeatServerTest, DoesNotExposeTokens) {
    ExecEnv::GetInstance()->set_storage_engine(std::make_unique<StorageEngine>(EngineOptions {}));
    Defer reset_engine([] { ExecEnv::GetInstance()->set_storage_engine(nullptr); });
    ClusterInfo cluster;
    cluster.cluster_id = 7;
    HeartbeatServer server(&cluster);
    TMasterInfo master;
    master.__set_cluster_id(7);
    master.__set_epoch(10);
    TNetworkAddress address;
    address.__set_hostname("master-a");
    address.__set_port(9020);
    master.__set_network_address(address);
    master.__set_token("heartbeat_cluster_secret");

    HeartbeatLogSink log_sink;
    google::AddLogSink(&log_sink);
    Defer remove_log_sink([&] { google::RemoveLogSink(&log_sink); });

    THeartbeatResult result;
    for (const auto* token :
         {"heartbeat_auth_secret_a", "heartbeat_auth_secret_b", "heartbeat_auth_secret_c"}) {
        master.__set_auth_token(token);
        server.heartbeat(result, master);
        ASSERT_EQ(TStatusCode::OK, result.status.status_code);
    }

    server.heartbeat(result, master);
    ASSERT_EQ(TStatusCode::OK, result.status.status_code);
    EXPECT_EQ("heartbeat_auth_secret_c", cluster.curr_auth_token);
    EXPECT_EQ("heartbeat_auth_secret_b", cluster.last_auth_token);

    master.__set_token("heartbeat_mismatched_secret");
    server.heartbeat(result, master);
    ASSERT_EQ(TStatusCode::INTERNAL_ERROR, result.status.status_code);
    ASSERT_EQ(1, result.status.error_msgs.size());
    EXPECT_NE(std::string::npos, result.status.error_msgs.front().find("invalid token"));
    EXPECT_EQ("heartbeat_cluster_secret", cluster.token);

    for (const auto* secret :
         {"heartbeat_cluster_secret", "heartbeat_mismatched_secret", "heartbeat_auth_secret_a",
          "heartbeat_auth_secret_b", "heartbeat_auth_secret_c"}) {
        EXPECT_EQ(std::string::npos, log_sink.messages.find(secret));
        EXPECT_EQ(std::string::npos, result.status.error_msgs.front().find(secret));
    }
}

} // namespace doris
