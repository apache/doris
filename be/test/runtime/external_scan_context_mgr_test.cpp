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

#include "runtime/external_scan_context_mgr.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/thread_resource_mgr.h"

namespace doris {

class ExternalScanContextMgrTest : public testing::Test {
public:
    ExternalScanContextMgrTest() {
        FragmentMgr* fragment_mgr = new FragmentMgr(&_exec_env);
        ThreadResourceMgr* thread_mgr = new ThreadResourceMgr();
        ResultQueueMgr* result_queue_mgr = new ResultQueueMgr();
        _exec_env._fragment_mgr = fragment_mgr;
        _exec_env._thread_mgr = thread_mgr;
        _exec_env._result_queue_mgr = result_queue_mgr;
    }
    virtual ~ExternalScanContextMgrTest() {
        delete _exec_env._fragment_mgr;
        delete _exec_env._thread_mgr;
        delete _exec_env._result_queue_mgr;
    }

protected:
    virtual void SetUp() {}

private:
    ExecEnv _exec_env;
};

TEST_F(ExternalScanContextMgrTest, create_normal) {
    std::shared_ptr<ScanContext> context;
    ExternalScanContextMgr context_mgr(&_exec_env);
    Status st = context_mgr.create_scan_context(&context);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(context != nullptr);
}

TEST_F(ExternalScanContextMgrTest, get_normal) {
    std::shared_ptr<ScanContext> context;
    ExternalScanContextMgr context_mgr(&_exec_env);
    Status st = context_mgr.create_scan_context(&context);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(context != nullptr);

    std::string context_id = context->context_id;
    std::shared_ptr<ScanContext> result;
    st = context_mgr.get_scan_context(context_id, &result);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(context != nullptr);
}

TEST_F(ExternalScanContextMgrTest, get_abnormal) {
    std::string context_id = "not_exist";
    std::shared_ptr<ScanContext> result;
    ExternalScanContextMgr context_mgr(&_exec_env);
    Status st = context_mgr.get_scan_context(context_id, &result);
    ASSERT_TRUE(!st.ok());
    ASSERT_TRUE(result == nullptr);
}

TEST_F(ExternalScanContextMgrTest, clear_context) {
    std::shared_ptr<ScanContext> context;
    ExternalScanContextMgr context_mgr(&_exec_env);
    Status st = context_mgr.create_scan_context(&context);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(context != nullptr);

    std::string context_id = context->context_id;
    st = context_mgr.clear_scan_context(context_id);
    ASSERT_TRUE(st.ok());

    std::shared_ptr<ScanContext> result;
    st = context_mgr.get_scan_context(context_id, &result);
    ASSERT_TRUE(!st.ok());
    ASSERT_TRUE(result == nullptr);
}
} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
