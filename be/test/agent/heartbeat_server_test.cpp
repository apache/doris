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

#include <ctime>

#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/Types_types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/logging.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;
using std::vector;

namespace doris {

TEST(HeartbeatTest, TestHeartbeat) {
    setenv("DORIS_HOME", "./", 1);
    THeartbeatResult heartbeat_result;
    TMasterInfo ori_master_info;
    ori_master_info.cluster_id = -1;
    ori_master_info.epoch = 0;
    ori_master_info.network_address.hostname = "";
    ori_master_info.network_address.port = 0;
    HeartbeatServer heartbeat_server(&ori_master_info);

    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::OK, heartbeat_result.status.status_code);
    EXPECT_EQ(master_info.epoch, heartbeat_server._epoch);
    EXPECT_EQ(master_info.cluster_id, heartbeat_server._master_info->cluster_id);
    EXPECT_EQ(master_info.network_address.hostname,
              heartbeat_server._master_info->network_address.hostname);
    EXPECT_EQ(master_info.network_address.port,
              heartbeat_server._master_info->network_address.port);

    // Diff cluster heartbeat
    master_info.cluster_id = 2;
    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::RUNTIME_ERROR, heartbeat_result.status.status_code);

    // New master but epoch small
    master_info.cluster_id = 1;
    master_info.epoch = 9;
    master_info.network_address.hostname = "new_host";
    master_info.network_address.port = 54321;
    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::RUNTIME_ERROR, heartbeat_result.status.status_code);

    // New master and epoch bigger
    master_info.epoch = 11;
    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::OK, heartbeat_result.status.status_code);
    EXPECT_EQ(master_info.epoch, heartbeat_server._epoch);
    EXPECT_EQ(master_info.network_address.hostname,
              heartbeat_server._master_info->network_address.hostname);
    EXPECT_EQ(master_info.network_address.port,
              heartbeat_server._master_info->network_address.port);

    heartbeat_server._olap_rootpath_instance = ori_olap_rootpath;
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
