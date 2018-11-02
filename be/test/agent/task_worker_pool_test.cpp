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

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "agent/file_downloader.h"
#include "agent/mock_file_downloader.h"
#include "agent/mock_pusher.h"
#include "agent/mock_utils.h"
#include "agent/mock_task_worker_pool.h"
#include "agent/task_worker_pool.h"
#include "agent/utils.h"
#include "olap/mock_command_executor.h"
#include "util/logging.h"
#include "runtime/exec_env.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;
using std::deque;

namespace doris {

MockFileDownloader::MockFileDownloader(const FileDownloaderParam& param):FileDownloader(param) {
}

MockPusher::MockPusher(const TPushReq& push_req) : Pusher(push_req) {
};

MockAgentServerClient::MockAgentServerClient(const TBackend backend)
        : AgentServerClient(backend) {
} 

MockMasterServerClient::MockMasterServerClient(
        const TMasterInfo& master_info,
        FrontendServiceClientCache* client_cache) : MasterServerClient(master_info, client_cache) {
}

TEST(TaskWorkerPoolTest, TestStart) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool_create_table(
            TaskWorkerPool::TaskWorkerType::CREATE_TABLE,
            &env,
            master_info);
    task_worker_pool_create_table.start();
    EXPECT_EQ(task_worker_pool_create_table._worker_count, config::create_table_worker_count);

    TaskWorkerPool task_worker_pool_drop_table(
            TaskWorkerPool::TaskWorkerType::DROP_TABLE,
            &env,
            master_info);
    task_worker_pool_drop_table.start();
    EXPECT_EQ(task_worker_pool_create_table._worker_count, config::drop_table_worker_count);

    TaskWorkerPool task_worker_pool_push(
            TaskWorkerPool::TaskWorkerType::PUSH,
            &env,
            master_info);
    task_worker_pool_push.start();
    EXPECT_EQ(task_worker_pool_push._worker_count, config::push_worker_count_normal_priority
            + config::push_worker_count_high_priority);

    TaskWorkerPool task_worker_pool_publish_version(
            TaskWorkerPool::TaskWorkerType::PUBLISH_VERSION,
            &env,
            master_info);
    task_worker_pool_publish_version.start();
    EXPECT_EQ(task_worker_pool_publish_version._worker_count, config::publish_version_worker_count);

    TaskWorkerPool task_worker_pool_alter_table(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);
    task_worker_pool_alter_table.start();
    EXPECT_EQ(task_worker_pool_alter_table._worker_count, config::alter_table_worker_count);

    TaskWorkerPool task_worker_pool_clone(
            TaskWorkerPool::TaskWorkerType::CLONE,
            &env,
            master_info);
    task_worker_pool_clone.start();
    EXPECT_EQ(task_worker_pool_clone._worker_count, config::clone_worker_count);

    TaskWorkerPool task_worker_pool_cancel_delete_data(
            TaskWorkerPool::TaskWorkerType::CANCEL_DELETE_DATA,
            &env,
            master_info);
    task_worker_pool_cancel_delete_data.start();
    EXPECT_EQ(
            task_worker_pool_cancel_delete_data._worker_count,
            config::cancel_delete_data_worker_count);

    TaskWorkerPool task_worker_pool_report_task(
            TaskWorkerPool::TaskWorkerType::REPORT_TASK,
            &env,
            master_info);
    task_worker_pool_report_task.start();
    EXPECT_EQ(task_worker_pool_report_task._worker_count, REPORT_TASK_WORKER_COUNT);

    TaskWorkerPool task_worker_pool_report_disk_state(
            TaskWorkerPool::TaskWorkerType::REPORT_DISK_STATE,
            &env,
            master_info);
    task_worker_pool_report_disk_state.start();
    EXPECT_EQ(task_worker_pool_report_disk_state._worker_count, REPORT_DISK_STATE_WORKER_COUNT);

    TaskWorkerPool task_worker_pool_report_olap_table(
            TaskWorkerPool::TaskWorkerType::REPORT_OLAP_TABLE,
            &env,
            master_info);
    task_worker_pool_report_olap_table.start();
    EXPECT_EQ(task_worker_pool_report_olap_table._worker_count, REPORT_OLAP_TABLE_WORKER_COUNT);
    
    TaskWorkerPool task_worker_pool_upload(
            TaskWorkerPool::TaskWorkerType::UPLOAD,
            &env,
            master_info);
    task_worker_pool_upload.start();
    EXPECT_EQ(task_worker_pool_upload._worker_count, config::upload_worker_count);

    TaskWorkerPool task_worker_pool_make_snapshot(
            TaskWorkerPool::TaskWorkerType::MAKE_SNAPSHOT,
            &env,
            master_info);
    task_worker_pool_make_snapshot.start();
    EXPECT_EQ(task_worker_pool_make_snapshot._worker_count, config::make_snapshot_worker_count);

    TaskWorkerPool task_worker_pool_release_snapshot(
            TaskWorkerPool::TaskWorkerType::RELEASE_SNAPSHOT,
            &env,
            master_info);
    task_worker_pool_release_snapshot.start();
    EXPECT_EQ(task_worker_pool_release_snapshot._worker_count,
            config::release_snapshot_worker_count);
}

TEST(TaskWorkerPoolTest, TestSubmitTask) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    // Record signature success
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::ROLLUP;
    agent_task_request.signature = 123456;
    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._tasks.size());

    // Record same signature
    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._tasks.size());

    task_worker_pool._s_task_signatures[agent_task_request.task_type].clear();
}

TEST(TaskWorkerPoolTest, TestRecordTaskInfo) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    TTaskType::type task_type = TTaskType::ROLLUP;
    // Record signature success
    bool ret = task_worker_pool._record_task_info(task_type, 123456, "root");
    EXPECT_TRUE(ret);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[task_type].size());

    // Record same signature
    ret = task_worker_pool._record_task_info(task_type, 123456, "root");
    EXPECT_FALSE(ret);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[task_type].size());

    // Record different signature
    ret = task_worker_pool._record_task_info(task_type, 123457, "");
    EXPECT_TRUE(ret);
    EXPECT_EQ(2, task_worker_pool._s_task_signatures[task_type].size());

    TMasterInfo master_info2;
    TaskWorkerPool task_worker_pool2(
            TaskWorkerPool::TaskWorkerType::PUSH,
            &env,
            master_info2);
    TTaskType::type task_type2 = TTaskType::PUSH;
    
    // Record push task info
    ret = task_worker_pool._record_task_info(task_type2, 223456, "root");
    EXPECT_TRUE(ret);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[task_type2].size());
    EXPECT_EQ(1, task_worker_pool._s_total_task_user_count[task_type2]["root"]);
    EXPECT_EQ(1, task_worker_pool._s_total_task_count[task_type2]);

    // Record same signature push task
    ret = task_worker_pool._record_task_info(task_type2, 223456, "user");
    EXPECT_FALSE(ret);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[task_type2].size());
    EXPECT_EQ(1, task_worker_pool._s_total_task_user_count[task_type2]["root"]);
    EXPECT_EQ(1, task_worker_pool._s_total_task_count[task_type2]);

    // Record diff signature same user
    ret = task_worker_pool._record_task_info(task_type2, 223457, "root");
    EXPECT_TRUE(ret);
    EXPECT_EQ(2, task_worker_pool._s_task_signatures[task_type2].size());
    EXPECT_EQ(2, task_worker_pool._s_total_task_user_count[task_type2]["root"]);
    EXPECT_EQ(2, task_worker_pool._s_total_task_count[task_type2]);

    // Record diff signature diff user
    ret = task_worker_pool._record_task_info(task_type2, 223458, "user");
    EXPECT_TRUE(ret);
    EXPECT_EQ(3, task_worker_pool._s_task_signatures[task_type2].size());
    EXPECT_EQ(1, task_worker_pool._s_total_task_user_count[task_type2]["user"]);
    EXPECT_EQ(3, task_worker_pool._s_total_task_count[task_type2]);
}

TEST(TaskWorkerPoolTest, TestRemoveTaskInfo) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    TTaskType::type task_type = TTaskType::ROLLUP;
    EXPECT_EQ(2, task_worker_pool._s_task_signatures[task_type].size());
    task_worker_pool._remove_task_info(task_type, 123456, "root");
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[task_type].size());
    task_worker_pool._remove_task_info(task_type, 123457, "root");
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[task_type].size());

    TTaskType::type task_type_push = TTaskType::PUSH;
    task_worker_pool._s_running_task_user_count[task_type_push]["root"] = 2;
    task_worker_pool._s_running_task_user_count[task_type_push]["user"] = 1;
    
    EXPECT_EQ(3, task_worker_pool._s_task_signatures[task_type_push].size());
    EXPECT_EQ(2, task_worker_pool._s_total_task_user_count[task_type_push]["root"]);
    EXPECT_EQ(1, task_worker_pool._s_total_task_user_count[task_type_push]["user"]);
    EXPECT_EQ(3, task_worker_pool._s_total_task_count[task_type_push]);
    task_worker_pool._remove_task_info(task_type_push, 223456, "root");
    EXPECT_EQ(2, task_worker_pool._s_task_signatures[task_type_push].size());
    EXPECT_EQ(1, task_worker_pool._s_total_task_user_count[task_type_push]["root"]);
    EXPECT_EQ(2, task_worker_pool._s_total_task_count[task_type_push]);
    EXPECT_EQ(1, task_worker_pool._s_running_task_user_count[task_type_push]["root"]);
    task_worker_pool._remove_task_info(task_type_push, 223457, "root");
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[task_type_push].size());
    EXPECT_EQ(0, task_worker_pool._s_total_task_user_count[task_type_push]["root"]);
    EXPECT_EQ(1, task_worker_pool._s_total_task_count[task_type_push]);
    EXPECT_EQ(0, task_worker_pool._s_running_task_user_count[task_type_push]["root"]);
    task_worker_pool._remove_task_info(task_type_push, 223458, "user");
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[task_type_push].size());
    EXPECT_EQ(0, task_worker_pool._s_total_task_user_count[task_type_push]["user"]);
    EXPECT_EQ(0, task_worker_pool._s_total_task_count[task_type_push]);
    EXPECT_EQ(0, task_worker_pool._s_running_task_user_count[task_type_push]["user"]);
}

TEST(TaskWorkerPoolTest, TestGetNextTask) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
        TaskWorkerPool::TaskWorkerType::PUSH,
            &env,
        master_info);

    // Add 1 task
    int32_t thread_count = 3;
    deque<TAgentTaskRequest> tasks;
    TAgentTaskRequest task1;
    task1.resource_info.user = "root";
    task1.__isset.resource_info = true;
    task1.task_type = TTaskType::PUSH;
    tasks.push_back(task1);
    task_worker_pool._s_total_task_user_count[TTaskType::PUSH]["root"] = 1;
    task_worker_pool._s_total_task_count[TTaskType::PUSH] = 1;
    task_worker_pool._s_running_task_user_count[TTaskType::PUSH]["root"] = 0;
    uint32_t ret = task_worker_pool._get_next_task_index(thread_count, tasks, TPriority::NORMAL);
    EXPECT_EQ(0, ret);
    tasks.erase(tasks.begin() + 0);

    // Add 3 task
    TAgentTaskRequest task2;
    task2.resource_info.user = "root";
    task2.__isset.resource_info = true;
    task2.task_type = TTaskType::PUSH;
    tasks.push_back(task2);
    TAgentTaskRequest task3;
    task3.resource_info.user = "root";
    task3.__isset.resource_info = true;
    task3.task_type = TTaskType::PUSH;
    tasks.push_back(task3);
    TAgentTaskRequest task4;
    task4.resource_info.user = "user1";
    task4.__isset.resource_info = true;
    task4.task_type = TTaskType::PUSH;
    tasks.push_back(task4);
    task_worker_pool._s_total_task_user_count[TTaskType::PUSH]["root"] = 3;
    task_worker_pool._s_total_task_user_count[TTaskType::PUSH]["user1"] = 1;
    task_worker_pool._s_total_task_count[TTaskType::PUSH] = 4;
    task_worker_pool._s_running_task_user_count[TTaskType::PUSH]["root"] = 1;
    ret = task_worker_pool._get_next_task_index(thread_count, tasks, TPriority::NORMAL);
    EXPECT_EQ(0, ret);
    tasks.erase(tasks.begin() + 0);

    // Go on
    task_worker_pool._s_running_task_user_count[TTaskType::PUSH]["root"] = 2;
    ret = task_worker_pool._get_next_task_index(thread_count, tasks, TPriority::NORMAL);
    EXPECT_EQ(1, ret);
    tasks.erase(tasks.begin() + 1);

    // Add 2 task, 1 root task finished
    TAgentTaskRequest task5;
    task5.resource_info.user = "user1";
    task5.__isset.resource_info = true;
    task5.task_type = TTaskType::PUSH;
    tasks.push_back(task5);
    TAgentTaskRequest task6;
    task6.resource_info.user = "user2";
    task6.__isset.resource_info = true;
    task6.task_type = TTaskType::PUSH;
    tasks.push_back(task6);
    task_worker_pool._s_total_task_user_count[TTaskType::PUSH]["root"] = 2;
    task_worker_pool._s_total_task_user_count[TTaskType::PUSH]["user1"] = 2;
    task_worker_pool._s_total_task_user_count[TTaskType::PUSH]["user2"] = 1;
    task_worker_pool._s_total_task_count[TTaskType::PUSH] = 5;
    task_worker_pool._s_running_task_user_count[TTaskType::PUSH]["root"] = 1;
    task_worker_pool._s_running_task_user_count[TTaskType::PUSH]["user1"] = 1;
    ret = task_worker_pool._get_next_task_index(thread_count, tasks, TPriority::NORMAL);
    EXPECT_EQ(2, ret);
    tasks.erase(tasks.begin() + 2);

    // User2 task finished, no one task was fit, choose first one
    task_worker_pool._s_total_task_user_count[TTaskType::PUSH]["user2"] = 0;
    task_worker_pool._s_total_task_count[TTaskType::PUSH] = 4;
    ret = task_worker_pool._get_next_task_index(thread_count, tasks, TPriority::NORMAL);
    EXPECT_EQ(0, ret);
}

TEST(TaskWorkerPoolTest, TestFinishTask) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    // Finish task failed
    TFinishTaskRequest finish_task_request;
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(TASK_FINISH_MAX_RETRY)
            .WillRepeatedly(Return(DORIS_ERROR));
    task_worker_pool._finish_task(finish_task_request);

    // Finish task success
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    task_worker_pool._finish_task(finish_task_request);

    task_worker_pool._master_client = original_master_server_client;
}

#if 0
TEST(TaskWorkerPoolTest, TestCreateTable) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::CREATE;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::CREATE_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    // Create table failed
    EXPECT_CALL(mock_command_executor, create_table(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._create_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Create table success
    EXPECT_CALL(mock_command_executor, create_table(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._create_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}
#endif

TEST(TaskWorkerPoolTest, TestDropTableTask) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::DROP;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::DROP_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    // Drop table failed
    EXPECT_CALL(mock_command_executor, drop_table(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._drop_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Drop table success
    EXPECT_CALL(mock_command_executor, drop_table(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._drop_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestSchemaChange) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::SCHEMA_CHANGE;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;
    
    // New tablet size ok, last schema change status is failed
    // Delete failed alter table tablet file failed
    TCreateTabletReq create_tablet_req1;
    agent_task_request.alter_tablet_req.base_tablet_id = 12345;
    agent_task_request.alter_tablet_req.base_schema_hash = 56789;
    agent_task_request.alter_tablet_req.__set_new_tablet_req(create_tablet_req1);

    EXPECT_CALL(mock_command_executor, show_alter_table_status(
            agent_task_request.alter_tablet_req.base_tablet_id,
            agent_task_request.alter_tablet_req.base_schema_hash))
            .Times(1)
            .WillOnce(Return(ALTER_TABLE_FAILED));
    EXPECT_CALL(mock_command_executor, drop_table(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_command_executor, schema_change(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._alter_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());
    
    // New tablet size ok, last schema change status is failed
    // Delete failed alter table tablet file success
    // Do schema change failed
    EXPECT_CALL(mock_command_executor, show_alter_table_status(
            agent_task_request.alter_tablet_req.base_tablet_id,
            agent_task_request.alter_tablet_req.base_schema_hash))
            .Times(1)
            .WillOnce(Return(ALTER_TABLE_FAILED));
    EXPECT_CALL(mock_command_executor, drop_table(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_command_executor, schema_change(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._alter_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // New tablet size ok, last schema change status is failed
    // Delete failed alter table tablet file success
    // Do schema change success, check status failed
    EXPECT_CALL(mock_command_executor, show_alter_table_status(
            agent_task_request.alter_tablet_req.base_tablet_id,
            agent_task_request.alter_tablet_req.base_schema_hash))
            .Times(1)
            .WillOnce(Return(ALTER_TABLE_FAILED));
    EXPECT_CALL(mock_command_executor, drop_table(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_command_executor, schema_change(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._alter_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());
    
    // New tablet size ok, last schema change status is ok
    // Do schema change success, check status running then success
    EXPECT_CALL(mock_command_executor, show_alter_table_status(
            agent_task_request.alter_tablet_req.base_tablet_id,
            agent_task_request.alter_tablet_req.base_schema_hash))
            .Times(1)
            .WillOnce(Return(ALTER_TABLE_FINISHED));
    EXPECT_CALL(mock_command_executor, drop_table(_))
            .Times(0);
    EXPECT_CALL(mock_command_executor, schema_change(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._alter_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestRollup) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::ROLLUP;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    // New tablet size ok, last rollup status is ok
    // Do rollup success, check status running then success
    TCreateTabletReq create_tablet_req1;
    agent_task_request.alter_tablet_req.base_tablet_id = 12345;
    agent_task_request.alter_tablet_req.base_schema_hash = 56789;
    agent_task_request.alter_tablet_req.__set_new_tablet_req(create_tablet_req1);
    EXPECT_CALL(mock_command_executor, show_alter_table_status(
            agent_task_request.alter_tablet_req.base_tablet_id,
            agent_task_request.alter_tablet_req.base_schema_hash))
            .Times(1)
            .WillOnce(Return(ALTER_TABLE_FINISHED));
    EXPECT_CALL(mock_command_executor, drop_table(_))
            .Times(0);
    EXPECT_CALL(mock_command_executor, create_rollup_table(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._alter_table_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestPush) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::PUSH;
    agent_task_request.signature = 123456;
    agent_task_request.__set_priority(TPriority::HIGH);
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::PUSH,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;
    TPushReq push_req;
    MockPusher mock_pusher(push_req);
    Pusher* original_pusher = task_worker_pool._pusher;
    task_worker_pool._pusher = &mock_pusher;

    // Push type load, push init failed
    agent_task_request.push_req.push_type = TPushType::LOAD;
    EXPECT_CALL(mock_pusher, init())
            .Times(1)
            .WillOnce(Return(DORIS_ERROR));
    EXPECT_CALL(mock_pusher, process(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._push_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Push type load, push init success, push failed
    EXPECT_CALL(mock_pusher, init())
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_pusher, process(_))
            .Times(PUSH_MAX_RETRY)
            .WillRepeatedly(Return(DORIS_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._push_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());
    
    // Push type load, push init success, push success
    EXPECT_CALL(mock_pusher, init())
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_pusher, process(_))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._push_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Push type delete, delete failed
    agent_task_request.push_req.push_type = TPushType::DELETE;
    EXPECT_CALL(mock_command_executor, delete_data(_, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._push_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Push type delete, delete success
    EXPECT_CALL(mock_command_executor, delete_data(_, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._push_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
    task_worker_pool._pusher = original_pusher;
}

TEST(TaskWorkerPoolTest, TestPublishVersionTask) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::PUBLISH_VERSION;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::PUBLISH_VERSION,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    // publish version failed
    EXPECT_CALL(mock_command_executor, publish_version(_, _))
            .Times(3)
            .WillRepeatedly(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._publish_version_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // publish version success
    EXPECT_CALL(mock_command_executor, publish_version(_, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._publish_version_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestClone) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::CLONE;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::CLONE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;
    TBackend backend;
    MockAgentServerClient mock_agent_server_client(backend);
    AgentServerClient* original_agent_server_client;
    original_agent_server_client = task_worker_pool._agent_client;
    task_worker_pool._agent_client = &mock_agent_server_client;
    FileDownloader::FileDownloaderParam param;
    MockFileDownloader mock_file_downloader(param);
    FileDownloader* original_file_downloader_ptr;
    original_file_downloader_ptr = task_worker_pool._file_downloader_ptr;
    task_worker_pool._file_downloader_ptr = &mock_file_downloader;
    MockAgentUtils mock_agent_utils;
    AgentUtils* original_agent_utils;
    original_agent_utils = task_worker_pool._agent_utils;
    task_worker_pool._agent_utils = &mock_agent_utils;

    // Tablet has exist
    // incremental clone's make snapshot failed
    // full clone's make snapshot failed
    TCloneReq clone_req;
    TBackend backend1;
    TBackend backend2;
    TBackend backend3;
    clone_req.src_backends.push_back(backend1);
    clone_req.src_backends.push_back(backend2);
    clone_req.src_backends.push_back(backend3);
    clone_req.tablet_id = 123;
    clone_req.schema_hash = 456;

    TAgentResult agent_result;
    agent_result.status.status_code = TStatusCode::INTERNAL_ERROR;
    agent_task_request.__set_clone_req(clone_req);

    TSnapshotRequest snapshot_request;
    snapshot_request.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request.__set_schema_hash(agent_task_request.clone_req.schema_hash);

    TSnapshotRequest snapshot_request2;
    snapshot_request2.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request2.__set_schema_hash(agent_task_request.clone_req.schema_hash);
    std::vector<int64_t> missing_versions;
    snapshot_request2.__set_missing_version(missing_versions);

    std::shared_ptr<OLAPTable> olap_table_ok(new OLAPTable(NULL, nullptr));
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_ok));
    EXPECT_CALL(mock_command_executor, get_info_before_incremental_clone(_, _, _))
            .Times(1);
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request2, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(0);
    EXPECT_CALL(mock_command_executor, finish_clone(_, _, _, _))
            .Times(0);
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet has exist
    // incremental clone's make snapshot success
    // incremental clone failed
    TAgentResult agent_result2;
    agent_result2.__set_snapshot_path("path");
    agent_result2.status.status_code = TStatusCode::OK;

    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_ok));
    EXPECT_CALL(mock_command_executor, get_info_before_incremental_clone(_, _, _))
            .Times(1)
            .WillOnce(Return("./test_data/5/6"));
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request2, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    uint64_t file_size = 4;
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<0>(file_size), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(3)
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, finish_clone(_, _, _, _))
            .Times(1)
            .WillOnce(Return(OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet has exist
    // incremental clone success
    // get tablet info failed
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_ok));
    EXPECT_CALL(mock_command_executor, get_info_before_incremental_clone(_, _, _))
            .Times(1)
            .WillOnce(Return("./test_data/5/6"));
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request2, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<0>(file_size), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(3)
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, finish_clone(_, _, _, _))
            .Times(1)
            .WillOnce(Return(OLAP_SUCCESS));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet has exist
    // incremental clone's make snapshot failed
    // full clone's make snapshot success
    // full clone failed
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_ok));
    EXPECT_CALL(mock_command_executor, get_info_before_incremental_clone(_, _, _))
            .Times(1)
            .WillOnce(Return("./test_data/5/6"));
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request2, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<0>(file_size), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(3)
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, finish_clone(_, _, _, _))
            .Times(1)
            .WillOnce(Return(OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet has exist
    // incremental clone's make snapshot failed
    // full clone's make snapshot success
    // full clone success
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_ok));
    EXPECT_CALL(mock_command_executor, get_info_before_incremental_clone(_, _, _))
            .Times(1)
            .WillOnce(Return("./test_data/5/6"));
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request2, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<0>(file_size), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(3)
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, finish_clone(_, _, _, _))
            .Times(1)
            .WillOnce(Return(OLAP_SUCCESS));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path failed, do not get tablet info
    std::shared_ptr<OLAPTable> olap_table_null(NULL);
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path success, make snapshot failed
    agent_result2.__isset.snapshot_path = false;
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(clone_req.src_backends.size())
            .WillOnce(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)))
            .WillOnce(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)))
            .WillOnce(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(0);
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path success, make snapshot success
    // List remote dir failed
    clone_req.tablet_id = 5;
    clone_req.schema_hash = 6;
    agent_task_request.__set_clone_req(clone_req);
    agent_result2.__set_snapshot_path("path");
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<1>("./test_data"),
                    Return(OLAPStatus::OLAP_SUCCESS)));
    snapshot_request.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request.__set_schema_hash(agent_task_request.clone_req.schema_hash);
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(clone_req.src_backends.size() * DOWNLOAD_FILE_MAX_RETRY)
            .WillRepeatedly(Return(DORIS_ERROR));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path success, make snapshot success
    // List remote dir success, get remote file length failed
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<1>("./test_data"),
                    Return(OLAPStatus::OLAP_SUCCESS)));
    snapshot_request.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request.__set_schema_hash(agent_task_request.clone_req.schema_hash);
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(clone_req.src_backends.size() * DOWNLOAD_FILE_MAX_RETRY)
            .WillRepeatedly(Return(DORIS_ERROR));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path success, make snapshot success
    // List remote dir success, get remote file length success
    // Download file failed
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<1>("./test_data"),
                    Return(OLAPStatus::OLAP_SUCCESS)));
    snapshot_request.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request.__set_schema_hash(agent_task_request.clone_req.schema_hash);
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(clone_req.src_backends.size() * DOWNLOAD_FILE_MAX_RETRY)
            .WillRepeatedly(Return(DORIS_ERROR));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path success, make snapshot success
    // List remote dir success, get remote file length success
    // Download file success, but file size is wrong
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<1>("./test_data"),
                    Return(OLAPStatus::OLAP_SUCCESS)));
    snapshot_request.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request.__set_schema_hash(agent_task_request.clone_req.schema_hash);
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    file_size = 5;
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<0>(file_size), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(clone_req.src_backends.size() * DOWNLOAD_FILE_MAX_RETRY)
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(clone_req.src_backends.size())
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path success, make snapshot success
    // List remote dir success, get remote file length success
    // Download file success, load header failed
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<1>("./test_data"),
                    Return(OLAPStatus::OLAP_SUCCESS)));
    snapshot_request.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request.__set_schema_hash(agent_task_request.clone_req.schema_hash);
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(1)
            .WillRepeatedly(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    file_size = 4;
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<0>(file_size), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(3)
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_command_executor, load_header(_, _, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(0);
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    
    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path success, make snapshot success
    // List remote dir success, get remote file length success
    // Download file success, load header success
    // Release snapshot failed, get tablet info failed
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<1>("./test_data"),
                    Return(OLAPStatus::OLAP_SUCCESS)));
    snapshot_request.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request.__set_schema_hash(agent_task_request.clone_req.schema_hash);
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(1)
            .WillRepeatedly(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    file_size = 4;
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<0>(file_size), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(3)
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_command_executor, load_header(_, _, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_ERROR)));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Tablet not exist, obtain root path success, make snapshot success
    // List remote dir success, get remote file length success
    // Download file success, load header success
    // Release snapshot success, get tablet info success
    EXPECT_CALL(mock_command_executor, get_table(
            agent_task_request.clone_req.tablet_id,
            agent_task_request.clone_req.schema_hash))
            .Times(1)
            .WillOnce(Return(olap_table_null));
    EXPECT_CALL(mock_command_executor, obtain_shard_path(_, _))
            .Times(1)
            .WillOnce(
                    DoAll(SetArgPointee<1>("./test_data"),
                    Return(OLAPStatus::OLAP_SUCCESS)));
    snapshot_request.__set_tablet_id(agent_task_request.clone_req.tablet_id);
    snapshot_request.__set_schema_hash(agent_task_request.clone_req.schema_hash);
    EXPECT_CALL(mock_agent_server_client, make_snapshot(snapshot_request, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result2), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, list_file_dir(_))
            .Times(1)
            .WillRepeatedly(
                    DoAll(SetArgPointee<0>("1.hdr\n1.idx\n1.dat"), Return(DORIS_SUCCESS)));
    file_size = 4;
    EXPECT_CALL(mock_file_downloader, get_length(_))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<0>(file_size), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(3)
            .WillRepeatedly(Return(DORIS_SUCCESS));
    EXPECT_CALL(mock_command_executor, load_header(_, _, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_agent_server_client, release_snapshot(_, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(agent_result), Return(DORIS_SUCCESS)));
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._clone_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
    task_worker_pool._agent_client = original_agent_server_client;
    task_worker_pool._agent_utils = original_agent_utils;
    task_worker_pool._file_downloader_ptr = original_file_downloader_ptr;
}

TEST(TaskWorkerPoolTest, TestCancelDeleteData) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::CANCEL_DELETE;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::CANCEL_DELETE_DATA,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;
    
    // Cancel delete failed
    EXPECT_CALL(mock_command_executor, cancel_delete(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._cancel_delete_data_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    // Cancel delete success
    EXPECT_CALL(mock_command_executor, cancel_delete(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));

    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    task_worker_pool._cancel_delete_data_worker_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestReportTask) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::SCHEMA_CHANGE;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    // Report failed
    EXPECT_CALL(mock_master_server_client, report(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_ERROR));
    task_worker_pool._report_task_worker_thread_callback(&task_worker_pool);

    // Report success
    EXPECT_CALL(mock_master_server_client, report(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    task_worker_pool._report_task_worker_thread_callback(&task_worker_pool);

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestReportDiskState) {
    TMasterInfo master_info;
    ExecEnv env;
    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::SCHEMA_CHANGE;
    agent_task_request.signature = 123456;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    // Get root path failed, report failed
#if 0
    EXPECT_CALL(mock_command_executor, get_all_root_path_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, report(_, _))
            .Times(0);
    task_worker_pool._report_disk_state_worker_thread_callback(&task_worker_pool);
#endif

    // Get root path success, report failed
    EXPECT_CALL(mock_command_executor, get_all_root_path_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, report(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_ERROR));
    task_worker_pool._report_disk_state_worker_thread_callback(&task_worker_pool);

    // Get root path success, report success
    EXPECT_CALL(mock_command_executor, get_all_root_path_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, report(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    task_worker_pool._report_disk_state_worker_thread_callback(&task_worker_pool);

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestReportOlapTable) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    // Get tablet info failed, report failed
    EXPECT_CALL(mock_command_executor, report_all_tablets_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, report(_, _))
            .Times(0);
    task_worker_pool._report_olap_table_worker_thread_callback(&task_worker_pool);

    // Get tablet info success, report failed
    EXPECT_CALL(mock_command_executor, report_all_tablets_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, report(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_ERROR));
    task_worker_pool._report_olap_table_worker_thread_callback(&task_worker_pool);

    // Get tablet info success, report success
    EXPECT_CALL(mock_command_executor, report_all_tablets_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, report(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    task_worker_pool._report_olap_table_worker_thread_callback(&task_worker_pool);

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestMakeSnapshot) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::MAKE_SNAPSHOT,
            &env,
            master_info);
    
    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::MAKE_SNAPSHOT;
    agent_task_request.signature = 123456;
    
    // make snapshot failed
    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    EXPECT_CALL(mock_command_executor, make_snapshot(_, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    task_worker_pool._make_snapshot_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());    
    
    // make snapshot success
    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    EXPECT_CALL(mock_command_executor, make_snapshot(_, _))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    task_worker_pool._make_snapshot_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestReleaseSnapshot) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::RELEASE_SNAPSHOT,
            &env,
            master_info);
    
    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;
    FrontendServiceClientCache* client_cache = new FrontendServiceClientCache();
    MockMasterServerClient mock_master_server_client(master_info, client_cache);
    MasterServerClient* original_master_server_client;
    original_master_server_client = task_worker_pool._master_client;
    task_worker_pool._master_client = &mock_master_server_client;

    TAgentTaskRequest agent_task_request;
    agent_task_request.task_type = TTaskType::RELEASE_SNAPSHOT;
    agent_task_request.signature = 123456;
    
    // make snapshot failed
    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    EXPECT_CALL(mock_command_executor, release_snapshot(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    task_worker_pool._release_snapshot_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(0, task_worker_pool._tasks.size());    
    
    // make snapshot success
    task_worker_pool.submit_task(agent_task_request);
    EXPECT_EQ(1, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());
    EXPECT_EQ(1, task_worker_pool._tasks.size());
    EXPECT_CALL(mock_command_executor, release_snapshot(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    EXPECT_CALL(mock_master_server_client, finish_task(_, _))
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    task_worker_pool._release_snapshot_thread_callback(&task_worker_pool);
    EXPECT_EQ(0, task_worker_pool._s_task_signatures[agent_task_request.task_type].size());

    task_worker_pool._command_executor = original_command_executor;
    task_worker_pool._master_client = original_master_server_client;
}

TEST(TaskWorkerPoolTest, TestShowAlterTableStatus) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;

    // Get tablet info failed
    TTabletInfo tablet_info;
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    AgentStatus status = task_worker_pool._get_tablet_info(1, 2, 123456, &tablet_info);
    EXPECT_EQ(DORIS_ERROR, status);

    // Get tablet info success
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    status = task_worker_pool._get_tablet_info(1, 2, 123456, &tablet_info);
    EXPECT_EQ(DORIS_SUCCESS, status);

    task_worker_pool._command_executor = original_command_executor;
}

TEST(TaskWorkerPoolTest, TestDropTable) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;

    TTabletId tablet_id = 123;
    TSchemaHash schema_hash = 456;
    EXPECT_CALL(mock_command_executor, show_alter_table_status(tablet_id, schema_hash))
            .Times(1)
            .WillOnce(Return(ALTER_TABLE_RUNNING));
    AlterTableStatus status = task_worker_pool._show_alter_table_status(tablet_id, schema_hash);
    EXPECT_EQ(ALTER_TABLE_RUNNING, status);

    task_worker_pool._command_executor = original_command_executor;
}

TEST(TaskWorkerPoolTest, TestGetTabletInfo) {
    TMasterInfo master_info;
    ExecEnv env;
    TaskWorkerPool task_worker_pool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            &env,
            master_info);

    MockCommandExecutor mock_command_executor;
    CommandExecutor* original_command_executor;
    original_command_executor = task_worker_pool._command_executor;
    task_worker_pool._command_executor = &mock_command_executor;

    // Report tablet info failed
    TTabletInfo tablet_info;
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    AgentStatus status = task_worker_pool._get_tablet_info(1, 2, 123456, &tablet_info);
    EXPECT_EQ(DORIS_ERROR, status);
    
    // Report tablet info success
    EXPECT_CALL(mock_command_executor, report_tablet_info(_))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    status = task_worker_pool._get_tablet_info(1, 2, 123456, &tablet_info);
    EXPECT_EQ(DORIS_SUCCESS, status);

    task_worker_pool._command_executor = original_command_executor;
}

}

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
