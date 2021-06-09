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

#include "agent/agent_server.h"

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/Types_types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "olap/mock_command_executor.h"
#include "util/logging.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;
using std::vector;

namespace doris {

TEST(SubmitTasksTest, TestSubmitTasks) {
    TAgentResult return_value;
    std::vector<TAgentTaskRequest> tasks;

    ExecEnv env;
    TMasterInfo master_info;
    TNetworkAddress network_address;
    AgentServer agent_server(&env, master_info);

    // Master info not init
    agent_server.submit_tasks(return_value, tasks);
    EXPECT_EQ(TStatusCode::CANCELLED, return_value.status.status_code);
    EXPECT_STREQ("Not get master heartbeat yet.", return_value.status.error_msgs[0].c_str());

    // Master info inited, type invalid
    master_info.network_address.hostname = "host name";
    master_info.network_address.port = 1234;
    TAgentTaskRequest task;
    task.task_type = TTaskType::CREATE;
    tasks.push_back(task);
    TAgentResult return_value1;
    agent_server.submit_tasks(return_value1, tasks);
    EXPECT_EQ(TStatusCode::ANALYSIS_ERROR, return_value1.status.status_code);

    // Master info inited, submit task
    tasks.clear();
    TAgentTaskRequest create_tablet_task;
    TCreateTabletReq create_tablet_req;
    create_tablet_task.task_type = TTaskType::CREATE;
    create_tablet_task.__set_create_tablet_req(create_tablet_req);
    tasks.push_back(create_tablet_task);
    TAgentTaskRequest drop_tablet_task;
    TDropTabletReq drop_tablet_req;
    drop_tablet_task.task_type = TTaskType::DROP;
    drop_tablet_task.__set_drop_tablet_req(drop_tablet_req);
    tasks.push_back(drop_tablet_task);
    TAgentTaskRequest alter_tablet_task;
    TAlterTabletReq alter_tablet_req;
    alter_tablet_task.task_type = TTaskType::ROLLUP;
    alter_tablet_task.__set_alter_tablet_req(alter_tablet_req);
    tasks.push_back(alter_tablet_task);
    TAgentTaskRequest clone_task;
    TCloneReq clone_req;
    clone_task.task_type = TTaskType::CLONE;
    clone_task.__set_clone_req(clone_req);
    tasks.push_back(clone_task);
    TAgentTaskRequest push_task;
    TPushReq push_req;
    push_task.task_type = TTaskType::PUSH;
    push_task.__set_push_req(push_req);
    tasks.push_back(push_task);
    TAgentTaskRequest cancel_delete_task;
    TCancelDeleteDataReq cancel_delete_data_req;
    cancel_delete_task.task_type = TTaskType::CANCEL_DELETE;
    cancel_delete_task.__set_cancel_delete_data_req(cancel_delete_data_req);
    tasks.push_back(cancel_delete_task);
    TAgentTaskRequest upload_task;
    TUploadReq upload_req;
    upload_task.task_type = TTaskType::UPLOAD;
    upload_task.__set_upload_req(upload_req);
    tasks.push_back(upload_task);
    TAgentTaskRequest make_snapshot_task;
    TSnapshotRequest snapshot_req;
    make_snapshot_task.task_type = TTaskType::MAKE_SNAPSHOT;
    make_snapshot_task.__set_snapshot_req(snapshot_req);
    tasks.push_back(make_snapshot_task);
    TAgentTaskRequest release_snapshot_task;
    TReleaseSnapshotRequest release_snapshot_req;
    release_snapshot_task.task_type = TTaskType::RELEASE_SNAPSHOT;
    release_snapshot_task.__set_release_snapshot_req(release_snapshot_req);
    tasks.push_back(release_snapshot_task);
    TAgentResult return_value2;
    agent_server.submit_tasks(return_value2, tasks);
    EXPECT_EQ(TStatusCode::OK, return_value2.status.status_code);
    EXPECT_EQ(0, return_value2.status.error_msgs.size());
}

TEST(MakeSnapshotTest, TestMakeSnapshot) {
    TAgentResult return_value;
    TSnapshotRequest snapshot_request;
    snapshot_request.tablet_id = 1;
    snapshot_request.schema_hash = 12345678;
    string snapshot_path;
    TMasterInfo master_info;

    ExecEnv env;
    CommandExecutor* tmp;
    MockCommandExecutor mock_command_executor;
    AgentServer agent_server(&env, master_info);
    tmp = agent_server._command_executor;
    agent_server._command_executor = &mock_command_executor;

    EXPECT_CALL(mock_command_executor, make_snapshot(_, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>("snapshot path"), Return(OLAP_SUCCESS)));
    agent_server.make_snapshot(return_value, snapshot_request);

    EXPECT_EQ(TStatusCode::OK, return_value.status.status_code);
    EXPECT_STREQ("snapshot path", return_value.snapshot_path.c_str());

    TAgentResult return_value2;
    EXPECT_CALL(mock_command_executor, make_snapshot(_, _))
            .Times(1)
            .WillOnce(Return(OLAP_ERR_VERSION_NOT_EXIST));
    agent_server.make_snapshot(return_value2, snapshot_request);

    EXPECT_EQ(TStatusCode::RUNTIME_ERROR, return_value2.status.status_code);

    agent_server._command_executor = tmp;
}

TEST(ReleaseSnapshotTest, TestReleaseSnapshot) {
    TAgentResult return_value;
    string snapshot_path = "snapshot path";
    TMasterInfo master_info;

    CommandExecutor* tmp;
    MockCommandExecutor mock_command_executor;
    ExecEnv env;
    AgentServer agent_server(&env, master_info);
    tmp = agent_server._command_executor;
    agent_server._command_executor = &mock_command_executor;

    EXPECT_CALL(mock_command_executor, release_snapshot(snapshot_path))
            .Times(1)
            .WillOnce(Return(OLAP_SUCCESS));
    agent_server.release_snapshot(return_value, snapshot_path);

    EXPECT_EQ(TStatusCode::OK, return_value.status.status_code);
    EXPECT_EQ(0, return_value.status.error_msgs.size());

    TAgentResult return_value2;
    EXPECT_CALL(mock_command_executor, release_snapshot(snapshot_path))
            .Times(1)
            .WillOnce(Return(OLAP_ERR_VERSION_NOT_EXIST));
    agent_server.release_snapshot(return_value2, snapshot_path);

    EXPECT_EQ(TStatusCode::RUNTIME_ERROR, return_value2.status.status_code);
    EXPECT_EQ(1, return_value2.status.error_msgs.size());

    agent_server._command_executor = tmp;
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
