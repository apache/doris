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

#include "runtime/export_task_mgr.h"

#include <gtest/gtest.h>

#include "gen_cpp/BackendService.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"

namespace doris {

// Mock fragment mgr
Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params, FinishCallback cb) {
    return Status::OK();
}

FragmentMgr::FragmentMgr(ExecEnv* exec_env) : _thread_pool(10, 128) {}

FragmentMgr::~FragmentMgr() {}

void FragmentMgr::debug(std::stringstream& ss) {}

class ExportTaskMgrTest : public testing::Test {
public:
    ExportTaskMgrTest() {}

private:
    ExecEnv _exec_env;
};

TEST_F(ExportTaskMgrTest, NormalCase) {
    ExportTaskMgr mgr(&_exec_env);
    TUniqueId id;
    id.hi = 1;
    id.lo = 2;

    TExportStatusResult res;
    TExportTaskRequest req;
    req.params.params.fragment_instance_id = id;

    // make it running
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::RUNNING, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    ExportTaskResult task_result;
    task_result.files.push_back("path/file1");
    EXPECT_TRUE(mgr.finish_task(id, Status::OK(), task_result).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::FINISHED, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);
    EXPECT_EQ(1, res.files.size());

    // erase it
    EXPECT_TRUE(mgr.erase_task(id).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::CANCELLED, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(ExportTaskMgrTest, DuplicateCase) {
    ExportTaskMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TExportStatusResult res;
    TExportTaskRequest req;
    req.params.params.fragment_instance_id = id;

    // make it running
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::RUNNING, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::RUNNING, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(ExportTaskMgrTest, RunAfterSuccess) {
    ExportTaskMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TExportStatusResult res;
    TExportTaskRequest req;
    req.params.params.fragment_instance_id = id;

    // make it running
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::RUNNING, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    ExportTaskResult task_result;
    task_result.files.push_back("path/file1");
    EXPECT_TRUE(mgr.finish_task(id, Status::OK(), task_result).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::FINISHED, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);
    EXPECT_EQ(1, res.files.size());
    EXPECT_EQ("path/file1", res.files[0]);

    // Put it twice
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::FINISHED, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);
    EXPECT_EQ(1, res.files.size());
    EXPECT_EQ("path/file1", res.files[0]);
}

TEST_F(ExportTaskMgrTest, RunAfterFail) {
    ExportTaskMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TExportStatusResult res;
    TExportTaskRequest req;
    req.params.params.fragment_instance_id = id;

    // make it running
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::RUNNING, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    ExportTaskResult task_result;
    EXPECT_TRUE(mgr.finish_task(id, Status::ThriftRpcError("Thrift rpc error"), task_result).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::CANCELLED, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::RUNNING, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(ExportTaskMgrTest, CancelJob) {
    ExportTaskMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TExportStatusResult res;
    TExportTaskRequest req;
    req.params.params.fragment_instance_id = id;

    // make it running
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::RUNNING, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    ExportTaskResult task_result;
    EXPECT_TRUE(mgr.cancel_task(id).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::CANCELLED, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    EXPECT_TRUE(mgr.start_task(req).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::RUNNING, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(ExportTaskMgrTest, FinishUnknownJob) {
    ExportTaskMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TExportStatusResult res;

    // make it finishing
    ExportTaskResult task_result;
    EXPECT_FALSE(mgr.finish_task(id, Status::ThriftRpcError("Thrift rpc error"), task_result).ok());
    EXPECT_TRUE(mgr.get_task_state(id, &res).ok());
    EXPECT_EQ(TExportState::CANCELLED, res.state);
    EXPECT_EQ(TStatusCode::OK, res.status.status_code);
}

} // namespace doris
