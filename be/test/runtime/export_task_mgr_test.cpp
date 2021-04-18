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
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::RUNNING, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    ExportTaskResult task_result;
    task_result.files.push_back("path/file1");
    ASSERT_TRUE(mgr.finish_task(id, Status::OK(), task_result).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::FINISHED, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
    ASSERT_EQ(1, res.files.size());

    // erase it
    ASSERT_TRUE(mgr.erase_task(id).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::CANCELLED, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
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
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::RUNNING, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::RUNNING, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
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
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::RUNNING, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    ExportTaskResult task_result;
    task_result.files.push_back("path/file1");
    ASSERT_TRUE(mgr.finish_task(id, Status::OK(), task_result).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::FINISHED, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
    ASSERT_EQ(1, res.files.size());
    ASSERT_EQ("path/file1", res.files[0]);

    // Put it twice
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::FINISHED, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
    ASSERT_EQ(1, res.files.size());
    ASSERT_EQ("path/file1", res.files[0]);
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
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::RUNNING, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    ExportTaskResult task_result;
    ASSERT_TRUE(mgr.finish_task(id, Status::ThriftRpcError("Thrift rpc error"), task_result).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::CANCELLED, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::RUNNING, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
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
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::RUNNING, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    ExportTaskResult task_result;
    ASSERT_TRUE(mgr.cancel_task(id).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::CANCELLED, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    ASSERT_TRUE(mgr.start_task(req).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::RUNNING, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(ExportTaskMgrTest, FinishUnknownJob) {
    ExportTaskMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TExportStatusResult res;

    // make it finishing
    ExportTaskResult task_result;
    ASSERT_FALSE(mgr.finish_task(id, Status::ThriftRpcError("Thrift rpc error"), task_result).ok());
    ASSERT_TRUE(mgr.get_task_state(id, &res).ok());
    ASSERT_EQ(TExportState::CANCELLED, res.state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
}

} // namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }

    doris::config::read_size = 8388608;
    doris::config::min_buffer_size = 1024;
    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::config::pull_load_task_dir = "/tmp";

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
