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
#include "olap/storage_engine.h"
#include "util/logging.h"
#include "runtime/exec_env.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;
using std::vector;

namespace doris {

static StorageEngine* k_engine = nullptr;

TEST(HeartbeatTest, TestHeartbeat) {
    ExecEnv* env = ExecEnv::GetInstance();
    env->set_last_heartbeat(new DateTimeValue());
    EngineOptions options;
    // won't open engine, options.path is needless
    options.backend_uid = UniqueId::gen_uid();
    if (k_engine == nullptr) {
        k_engine = new StorageEngine(options);
    }

    THeartbeatResult heartbeat_result;
    TMasterInfo ori_master_info;
    ori_master_info.cluster_id = -1;
    ori_master_info.epoch = 0;
    ori_master_info.network_address.hostname = "";
    ori_master_info.network_address.port = 0;
    HeartbeatServer heartbeat_server(env, &ori_master_info);

    TMasterInfo master_info;
    master_info.cluster_id = 1;
    master_info.epoch = 10;
    master_info.network_address.hostname = "0.9.1.9";
    master_info.network_address.port = 1994;

    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::OK, heartbeat_result.status.status_code);
    EXPECT_EQ(master_info.epoch, heartbeat_server._fe_epoch);
    EXPECT_EQ(master_info.cluster_id, heartbeat_server._master_info->cluster_id);
    EXPECT_EQ(master_info.network_address.hostname,
              heartbeat_server._master_info->network_address.hostname);
    EXPECT_EQ(master_info.network_address.port,
              heartbeat_server._master_info->network_address.port);

    // Diff cluster heartbeat
    master_info.cluster_id = 2;
    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::INTERNAL_ERROR, heartbeat_result.status.status_code);

    // New master but epoch small
    master_info.cluster_id = 1;
    master_info.epoch = 9;
    master_info.network_address.hostname = "new_host";
    master_info.network_address.port = 54321;
    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::INTERNAL_ERROR, heartbeat_result.status.status_code);

    // New master and epoch bigger
    master_info.epoch = 11;
    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::OK, heartbeat_result.status.status_code);
    EXPECT_EQ(master_info.epoch, heartbeat_server._fe_epoch);
    EXPECT_EQ(master_info.network_address.hostname,
              heartbeat_server._master_info->network_address.hostname);
    EXPECT_EQ(master_info.network_address.port,
              heartbeat_server._master_info->network_address.port);

    int64_t current_time = time(NULL);
    TFrontendInfo frontend_info;
    frontend_info.network_address.hostname = "0.8.1.4";
    frontend_info.network_address.port = 1994;
    frontend_info.is_alive = true;
    frontend_info.fe_start_time = current_time * 1000;
    master_info.frontends_info.push_back(frontend_info);
    master_info.__isset.frontends_info = true;

    std::string coord_addr_str = frontend_info.network_address.hostname + ":" +
                            std::to_string(frontend_info.network_address.port);
    heartbeat_server.heartbeat(heartbeat_result, master_info);
    EXPECT_EQ(TStatusCode::OK, heartbeat_result.status.status_code);
    EXPECT_TRUE(env->frontends_start_time().find(coord_addr_str) != env->frontends_start_time().end());
    EXPECT_EQ(env->frontends_start_time()[coord_addr_str]->is_alive, frontend_info.is_alive);
    EXPECT_EQ(env->frontends_start_time()[coord_addr_str]->start_time, frontend_info.fe_start_time);
    EXPECT_TRUE(env->frontends_start_time()[coord_addr_str]->last_heartbeat > 0);

    DateTimeValue current_date;
    current_date.from_unixtime(current_time, TimezoneUtils::default_time_zone);
    env->last_heartbeat()->from_unixtime(current_time, TimezoneUtils::default_time_zone);
    env->frontends_start_time()[coord_addr_str]
        ->last_heartbeat->from_unixtime(current_time, TimezoneUtils::default_time_zone);
    
    // frontend not exist
    TNetworkAddress coord_addr;
    coord_addr.__set_hostname("0.0.0.0");
    coord_addr.__set_port(1994);
    EXPECT_TRUE(HeartbeatServer::is_fe_restart(env, coord_addr, DateTimeValue::local_time()));
    // last_heartbeat < fe_msg_time
    coord_addr.__set_hostname("0.8.1.4");
    EXPECT_FALSE(HeartbeatServer::is_fe_restart(env, coord_addr, DateTimeValue(current_date.to_int64() + 100)));
    env->last_heartbeat()->from_unixtime(current_time + 200, TimezoneUtils::default_time_zone);
    // fe_last_heartbeat < fe_msg_time
    EXPECT_FALSE(HeartbeatServer::is_fe_restart(env, coord_addr, DateTimeValue(current_date.to_int64() + 100)));
    // fe_start_time > fe_msg_time
    EXPECT_TRUE(HeartbeatServer::is_fe_restart(env, coord_addr, DateTimeValue(current_date.to_int64() - 100)));
    env->frontends_start_time()[coord_addr_str]->start_time = (current_time - 200) * 1000;
    // query is executed normally, last_heartbeat > fe_msg_time, fe_start_time < fe_msg_time, is_alive == true
    EXPECT_FALSE(HeartbeatServer::is_fe_restart(env, coord_addr, DateTimeValue(current_date.to_int64() - 100)));
    env->frontends_start_time()[coord_addr_str]->is_alive = false;
    // frontend is not alive
    EXPECT_TRUE(HeartbeatServer::is_fe_restart(env, coord_addr, DateTimeValue(current_date.to_int64() - 100)));
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
