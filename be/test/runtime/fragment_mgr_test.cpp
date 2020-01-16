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

#include <gtest/gtest.h>
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/row_batch.h"
#include "exec/data_sink.h"
#include "common/config.h"

namespace doris {

static Status s_prepare_status;
static Status s_open_status;
// Mock used for this unittest
PlanFragmentExecutor::PlanFragmentExecutor(ExecEnv* exec_env, 
                                           const report_status_callback& report_status_cb) : 
        _exec_env(exec_env),
        _report_status_cb(report_status_cb) {
}

PlanFragmentExecutor::~PlanFragmentExecutor() {
}

Status PlanFragmentExecutor::prepare(const TExecPlanFragmentParams& request) {
    return s_prepare_status;
}

Status PlanFragmentExecutor::open() {
    usleep(50000);
    return s_open_status;
}

void PlanFragmentExecutor::cancel() {
}

void PlanFragmentExecutor::close() {
}

class FragmentMgrTest : public testing::Test {
public:
    FragmentMgrTest() {
    }

protected:
    virtual void SetUp() {
        s_prepare_status = Status::OK();
        s_open_status = Status::OK();
        LOG(INFO) << "fragment_pool_thread_num=" << config::fragment_pool_thread_num << ", pool_size=" << config::fragment_pool_queue_size;
        config::fragment_pool_thread_num = 32;
        config::fragment_pool_queue_size = 1024;
    }
    virtual void TearDown() {
    }
};

TEST_F(FragmentMgrTest, Normal) {
    FragmentMgr mgr(nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(100);
    params.params.fragment_instance_id.__set_lo(200);
    ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());
    // Duplicated
    ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());
}

TEST_F(FragmentMgrTest, AddNormal) {
    FragmentMgr mgr(nullptr);
    for (int i = 0; i < 8; ++i) {
        TExecPlanFragmentParams params;
        params.params.fragment_instance_id = TUniqueId();
        params.params.fragment_instance_id.__set_hi(100 + i);
        params.params.fragment_instance_id.__set_lo(200);
        ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());
    }
}

TEST_F(FragmentMgrTest, CancelNormal) {
    FragmentMgr mgr(nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(100);
    params.params.fragment_instance_id.__set_lo(200);
    ASSERT_TRUE(mgr.exec_plan_fragment(params).ok());
    // Cancel after add
    ASSERT_TRUE(mgr.cancel(params.params.fragment_instance_id).ok());
}

TEST_F(FragmentMgrTest, CancelWithoutAdd) {
    FragmentMgr mgr(nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(100);
    params.params.fragment_instance_id.__set_lo(200);
    ASSERT_TRUE(mgr.cancel(params.params.fragment_instance_id).ok());
}

TEST_F(FragmentMgrTest, PrepareFailed) {
    s_prepare_status = Status::InternalError("Prepare failed.");
    FragmentMgr mgr(nullptr);
    TExecPlanFragmentParams params;
    params.params.fragment_instance_id = TUniqueId();
    params.params.fragment_instance_id.__set_hi(100);
    params.params.fragment_instance_id.__set_lo(200);
    ASSERT_FALSE(mgr.exec_plan_fragment(params).ok());
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
