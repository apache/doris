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
#include "runtime/result_block_buffer.h"
#include "util/cpu_info.h"
#include "util/thread.h"
#include "vec/core/block.h"
#include "vec/sink/varrow_flight_result_writer.h"
#include "vec/sink/vmysql_result_writer.h"

namespace doris {

class ResultBufferMgrTest : public testing::Test {
public:
    ResultBufferMgrTest() {}
    ~ResultBufferMgrTest() override {}

private:
    RuntimeState _state;
};

TEST_F(ResultBufferMgrTest, create_normal) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false).ok());
    EXPECT_NE(control_block1, nullptr);
    control_block1.reset();

    EXPECT_FALSE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false).ok());
}

TEST_F(ResultBufferMgrTest, create_arrow) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    auto schema = std::make_shared<arrow::Schema>(std::move(fields));
    EXPECT_TRUE(
            buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, true, schema).ok());
}

TEST_F(ResultBufferMgrTest, create_same_buffer) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false).ok());
    std::shared_ptr<ResultBlockBufferBase> control_block2;
    EXPECT_FALSE(buffer_mgr.create_sender(query_id, 1024, &control_block2, &_state, false).ok());
}

TEST_F(ResultBufferMgrTest, find_buffer) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    auto schema = std::make_shared<arrow::Schema>(std::move(fields));
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false, nullptr)
                        .ok());

    std::shared_ptr<vectorized::MySQLResultBlockBuffer> buffer;
    EXPECT_TRUE(buffer_mgr.find_buffer(query_id, buffer).ok());
    EXPECT_TRUE(buffer != nullptr);
}

TEST_F(ResultBufferMgrTest, normal_cancel) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false).ok());

    EXPECT_TRUE(buffer_mgr.cancel(query_id, Status::InternalError("")));
}

TEST_F(ResultBufferMgrTest, cancel_no_block) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    EXPECT_FALSE(buffer_mgr.cancel(query_id, Status::InternalError("")));
}

} // namespace doris
