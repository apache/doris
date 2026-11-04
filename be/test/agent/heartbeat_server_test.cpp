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
#include <vector>

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
    std::vector<std::string> auth_tokens;
    int32_t cluster_id;
    cluster.get_auth_tokens(&auth_tokens, &cluster_id);
    EXPECT_EQ((std::vector<std::string> {"heartbeat_auth_secret_b", "heartbeat_auth_secret_c"}),
              auth_tokens);
    EXPECT_EQ(7, cluster_id);

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

TEST(HeartbeatServerTest, PublishesCompleteAuthQueueToNewBackend) {
    ExecEnv::GetInstance()->set_storage_engine(std::make_unique<StorageEngine>(EngineOptions {}));
    Defer reset_engine([] { ExecEnv::GetInstance()->set_storage_engine(nullptr); });
    ClusterInfo cluster;
    cluster.cluster_id = 7;
    HeartbeatServer server(&cluster);
    TMasterInfo master;
    master.__set_cluster_id(7);
    master.__set_epoch(10);
    master.__set_network_address(TNetworkAddress());
    master.network_address.__set_hostname("master-a");
    master.network_address.__set_port(9020);
    master.__set_token("cluster-secret");
    master.__set_auth_token("newest-token");
    master.__set_auth_tokens({"oldest-token", "signing-token", "newest-token"});

    THeartbeatResult result;
    server.heartbeat(result, master);
    ASSERT_EQ(TStatusCode::OK, result.status.status_code);
    std::vector<std::string> tokens;
    int32_t cluster_id;
    cluster.get_auth_tokens(&tokens, &cluster_id);
    EXPECT_EQ(master.auth_tokens, tokens);
    EXPECT_EQ(7, cluster_id);
    EXPECT_EQ("newest-token", cluster.current_auth_token());
    EXPECT_TRUE(cluster.check_auth_token("signing-token"));
    EXPECT_TRUE(cluster.check_auth_token("oldest-token"));
    EXPECT_FALSE(cluster.check_auth_token(""));

    // Rotating the full queue must remove evicted keys, including on an existing BE.
    master.__set_auth_token("rotated-token");
    master.__set_auth_tokens({"signing-token", "newest-token", "rotated-token"});
    server.heartbeat(result, master);
    ASSERT_EQ(TStatusCode::OK, result.status.status_code);
    EXPECT_FALSE(cluster.check_auth_token("oldest-token"));
    EXPECT_TRUE(cluster.check_auth_token("signing-token"));
    EXPECT_EQ("rotated-token", cluster.current_auth_token());

    // Rejected heartbeats cannot replace the authentication queue.
    master.__set_token("wrong-cluster-secret");
    master.__set_auth_tokens({"rejected-token"});
    server.heartbeat(result, master);
    ASSERT_EQ(TStatusCode::INTERNAL_ERROR, result.status.status_code);
    EXPECT_FALSE(cluster.check_auth_token("rejected-token"));
    EXPECT_EQ("rotated-token", cluster.current_auth_token());

    // An old master only sends auth_token after a rollback or mixed-version election.
    master.__set_token("cluster-secret");
    master.__isset.auth_tokens = false;
    master.__set_auth_token("legacy-token");
    server.heartbeat(result, master);
    ASSERT_EQ(TStatusCode::OK, result.status.status_code);
    cluster.get_auth_tokens(&tokens, &cluster_id);
    EXPECT_EQ((std::vector<std::string> {"rotated-token", "legacy-token"}), tokens);
    EXPECT_EQ(7, cluster_id);
    EXPECT_EQ("legacy-token", cluster.current_auth_token());
}

TEST(ClusterInfoTest, SigningTokenHandlesEmptyAndLegacyQueues) {
    ClusterInfo cluster;
    EXPECT_TRUE(cluster.get_sign_token().empty());

    cluster.update_auth_token("first-token", 7);
    EXPECT_EQ("first-token", cluster.get_sign_token());
    EXPECT_EQ("first-token", cluster.current_auth_token());

    cluster.update_auth_token("second-token", 7);
    EXPECT_EQ("first-token", cluster.get_sign_token());
    EXPECT_EQ("second-token", cluster.current_auth_token());
    EXPECT_TRUE(cluster.check_auth_token(cluster.get_sign_token()));
}

TEST(ClusterInfoTest, SigningTokenToleratesAdjacentHeartbeatGenerations) {
    ClusterInfo sender;
    ClusterInfo receiver;
    sender.update_auth_tokens({"B", "C", "D", "E"}, 7);
    receiver.update_auth_tokens({"A", "B", "C", "D"}, 7);

    // Either side can send requests while the receiver has not received the rotation.
    EXPECT_EQ("D", sender.get_sign_token());
    EXPECT_EQ("E", sender.current_auth_token());
    EXPECT_TRUE(receiver.check_auth_token(sender.get_sign_token()));
    EXPECT_TRUE(sender.check_auth_token(receiver.get_sign_token()));
    EXPECT_FALSE(receiver.check_auth_token(sender.current_auth_token()));

    receiver.update_auth_tokens({"B", "C", "D", "E"}, 7);
    EXPECT_FALSE(receiver.check_auth_token("A"));
    const auto request_token = sender.get_sign_token();
    // A timed-out heartbeat can finish after its retry has published the newer queue.
    receiver.update_auth_tokens({"A", "B", "C", "D"}, 7);
    EXPECT_TRUE(receiver.check_auth_token(request_token));
    EXPECT_TRUE(sender.check_auth_token(receiver.get_sign_token()));
}

} // namespace doris
