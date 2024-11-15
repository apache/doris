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

#include "runtime/result_buffer_mgr.h"

#include <gtest/gtest.h>

#include "gen_cpp/PaloInternalService_types.h"
#include "runtime/buffer_control_block.h"
#include "util/cpu_info.h"

namespace doris {

class ResultBufferMgrTest : public testing::Test {
public:
    ResultBufferMgrTest() {}
    virtual ~ResultBufferMgrTest() {}

protected:
    virtual void SetUp() {}

private:
    RuntimeState _state;
};

TEST_F(ResultBufferMgrTest, create_normal) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<BufferControlBlock> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state).ok());
}

TEST_F(ResultBufferMgrTest, create_same_buffer) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<BufferControlBlock> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state).ok());
    std::shared_ptr<BufferControlBlock> control_block2;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block2, &_state).ok());

    EXPECT_EQ(control_block1.get(), control_block1.get());
}

TEST_F(ResultBufferMgrTest, fetch_data_normal) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<BufferControlBlock> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state).ok());

    TFetchDataResult* result = new TFetchDataResult();
    result->result_batch.rows.push_back("hello test");
    control_block1->add_batch(result);
    TFetchDataResult get_result;
    EXPECT_TRUE(buffer_mgr.fetch_data(query_id, &get_result).ok());
    EXPECT_EQ(1U, get_result.result_batch.rows.size());
    EXPECT_STREQ("hello test", get_result.result_batch.rows[0].c_str());
}

TEST_F(ResultBufferMgrTest, fetch_data_no_block) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<BufferControlBlock> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state).ok());

    TFetchDataResult* result = new TFetchDataResult();
    query_id.lo = 11;
    query_id.hi = 100;
    EXPECT_FALSE(buffer_mgr.fetch_data(query_id, result).ok());
    delete result;
}

TEST_F(ResultBufferMgrTest, normal_cancel) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<BufferControlBlock> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state).ok());

    EXPECT_TRUE(buffer_mgr.cancel(query_id).ok());
}

TEST_F(ResultBufferMgrTest, cancel_no_block) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    EXPECT_TRUE(buffer_mgr.cancel(query_id).ok());
}
} // namespace doris
