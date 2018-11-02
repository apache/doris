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

#include "runtime/pull_load_task_mgr.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/Types_types.h"
#include "util/cpu_info.h"

namespace doris {

class PullLoadTaskMgrTest : public testing::Test {
public:
    PullLoadTaskMgrTest() {
    }

protected:
    virtual void SetUp() {
    }
    virtual void TearDown() {
    }
};

TEST_F(PullLoadTaskMgrTest, Normal) {
    PullLoadTaskMgr mgr("./test/var/pull_task");
    auto st = mgr.init();
    ASSERT_TRUE(st.ok());

    // register one task
    TUniqueId id;
    id.__set_hi(101);
    id.__set_lo(102);
    st = mgr.deregister_task(id);
    ASSERT_TRUE(st.ok());
    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::CANCELLED, result.task_info.etl_state);
    }

    st = mgr.register_task(id, 2);
    ASSERT_TRUE(st.ok());
    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::RUNNING, result.task_info.etl_state);
    }

    // report sub-task info
    {
        TPullLoadSubTaskInfo sub_task_info;
        sub_task_info.id = id;
        sub_task_info.sub_task_id = 1;
        sub_task_info.file_map.emplace("http://abc.com/1", 100);
        st = mgr.report_sub_task_info(sub_task_info);
        ASSERT_TRUE(st.ok());
    }
    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::RUNNING, result.task_info.etl_state);
    }
    {
        TPullLoadSubTaskInfo sub_task_info;
        sub_task_info.id = id;
        sub_task_info.sub_task_id = 2;
        sub_task_info.file_map.emplace("http://abc.com/2", 200);
        st = mgr.report_sub_task_info(sub_task_info);
        ASSERT_TRUE(st.ok());
    }
    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::FINISHED, result.task_info.etl_state);
    }
}

TEST_F(PullLoadTaskMgrTest, LoadTask) {
    PullLoadTaskMgr mgr("./test/var/pull_task");
    auto st = mgr.init();
    ASSERT_TRUE(st.ok());

    // register one task
    TUniqueId id;
    id.__set_hi(101);
    id.__set_lo(102);
    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::FINISHED, result.task_info.etl_state);
    }
}

TEST_F(PullLoadTaskMgrTest, Deregister) {
    PullLoadTaskMgr mgr("./test/var/pull_task");
    auto st = mgr.init();
    ASSERT_TRUE(st.ok());

    // register one task
    TUniqueId id;
    id.__set_hi(102);
    id.__set_lo(103);

    st = mgr.register_task(id, 2);
    ASSERT_TRUE(st.ok());
    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::RUNNING, result.task_info.etl_state);
    }

    // report sub-task info
    {
        TPullLoadSubTaskInfo sub_task_info;
        sub_task_info.id = id;
        sub_task_info.sub_task_id = 1;
        sub_task_info.file_map.emplace("http://abc.com/1", 100);
        st = mgr.report_sub_task_info(sub_task_info);
        ASSERT_TRUE(st.ok());
    }

    {
        st = mgr.deregister_task(id);
        ASSERT_TRUE(st.ok());
    }

    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::CANCELLED, result.task_info.etl_state);
    }
}

TEST_F(PullLoadTaskMgrTest, Deregister2) {
    PullLoadTaskMgr mgr("./test/var/pull_task");
    auto st = mgr.init();
    ASSERT_TRUE(st.ok());

    // register one task
    TUniqueId id;
    id.__set_hi(103);
    id.__set_lo(104);

    st = mgr.register_task(id, 1);
    ASSERT_TRUE(st.ok());

    // report sub-task info
    {
        TPullLoadSubTaskInfo sub_task_info;
        sub_task_info.id = id;
        sub_task_info.sub_task_id = 1;
        sub_task_info.file_map.emplace("http://abc.com/1", 100);
        st = mgr.report_sub_task_info(sub_task_info);
        ASSERT_TRUE(st.ok());
    }
    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::FINISHED, result.task_info.etl_state);
    }

    {
        st = mgr.deregister_task(id);
        ASSERT_TRUE(st.ok());
    }

    // Fetch
    {
        TFetchPullLoadTaskInfoResult result;
        st = mgr.fetch_task_info(id, &result);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TEtlState::CANCELLED, result.task_info.etl_state);
    }
}

}

int main(int argc, char** argv) {
    // init_glog("be-test");
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

