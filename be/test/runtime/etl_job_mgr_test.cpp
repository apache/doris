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

#include "runtime/etl_job_mgr.h"

#include "gen_cpp/Types_types.h"
#include <gtest/gtest.h>
#include "runtime/fragment_mgr.h"
#include "runtime/exec_env.h"
#include "util/cpu_info.h"

namespace doris {
// Mock fragment mgr
Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params,
                                       FinishCallback cb) {
    return Status::OK();
}

FragmentMgr::FragmentMgr(ExecEnv* exec_env) :
        _thread_pool(10, 128) {
}

FragmentMgr::~FragmentMgr() {
}

void FragmentMgr::debug(std::stringstream& ss) {
}

class EtlJobMgrTest : public testing::Test {
public:
    EtlJobMgrTest() {
    }
private:
    ExecEnv _exec_env;
};

TEST_F(EtlJobMgrTest, NormalCase) {
    EtlJobMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TMiniLoadEtlStatusResult res;
    TMiniLoadEtlTaskRequest req;
    TDeleteEtlFilesRequest del_req;
    del_req.mini_load_id = id;
    req.params.params.fragment_instance_id = id;

    // make it running
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::RUNNING, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    EtlJobResult job_result;
    job_result.file_map["abc"] = 100L;
    ASSERT_TRUE(mgr.finish_job(id, Status::OK(), job_result).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::FINISHED, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
    ASSERT_EQ(1, res.file_map.size());
    ASSERT_EQ(100, res.file_map["abc"]);

    // erase it
    ASSERT_TRUE(mgr.erase_job(del_req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::CANCELLED, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(EtlJobMgrTest, DuplicateCase) {
    EtlJobMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TMiniLoadEtlStatusResult res;
    TMiniLoadEtlTaskRequest req;
    req.params.params.fragment_instance_id = id;

    // make it running
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::RUNNING, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::RUNNING, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(EtlJobMgrTest, RunAfterSuccess) {
    EtlJobMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TMiniLoadEtlStatusResult res;
    TMiniLoadEtlTaskRequest req;
    TDeleteEtlFilesRequest del_req;
    del_req.mini_load_id = id;
    req.params.params.fragment_instance_id = id;

    // make it running
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::RUNNING, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    EtlJobResult job_result;
    job_result.file_map["abc"] = 100L;
    ASSERT_TRUE(mgr.finish_job(id, Status::OK(), job_result).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::FINISHED, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
    ASSERT_EQ(1, res.file_map.size());
    ASSERT_EQ(100, res.file_map["abc"]);

    // Put it twice
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::FINISHED, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
    ASSERT_EQ(1, res.file_map.size());
    ASSERT_EQ(100, res.file_map["abc"]);
}

TEST_F(EtlJobMgrTest, RunAfterFail) {
    EtlJobMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TMiniLoadEtlStatusResult res;
    TMiniLoadEtlTaskRequest req;
    req.params.params.fragment_instance_id = id;

    // make it running
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::RUNNING, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    EtlJobResult job_result;
    job_result.debug_path = "abc";
    ASSERT_TRUE(mgr.finish_job(id, Status::ThriftRpcError("Thrift rpc error"), job_result).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::CANCELLED, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::RUNNING, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(EtlJobMgrTest, CancelJob) {
    EtlJobMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TMiniLoadEtlStatusResult res;
    TMiniLoadEtlTaskRequest req;
    req.params.params.fragment_instance_id = id;

    // make it running
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::RUNNING, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // make it finishing
    EtlJobResult job_result;
    job_result.debug_path = "abc";
    ASSERT_TRUE(mgr.cancel_job(id).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::CANCELLED, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);

    // Put it twice
    ASSERT_TRUE(mgr.start_job(req).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::RUNNING, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
}

TEST_F(EtlJobMgrTest, FinishUnknowJob) {
    EtlJobMgr mgr(&_exec_env);
    TUniqueId id;
    id.lo = 1;
    id.hi = 1;

    TMiniLoadEtlStatusResult res;

    // make it finishing
    EtlJobResult job_result;
    job_result.debug_path = "abc";
    ASSERT_FALSE(mgr.finish_job(id, Status::ThriftRpcError("Thrift rpc error"), job_result).ok());
    ASSERT_TRUE(mgr.get_job_state(id, &res).ok());
    ASSERT_EQ(TEtlState::CANCELLED, res.etl_state);
    ASSERT_EQ(TStatusCode::OK, res.status.status_code);
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
