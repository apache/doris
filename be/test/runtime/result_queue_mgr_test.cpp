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

#include "runtime/result_queue_mgr.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <memory>

#include "gen_cpp/DorisExternalService_types.h"
#include "testutil/test_util.h"
#include "util/blocking_queue.hpp"

namespace doris {

class ResultQueueMgrTest : public testing::Test {};

TEST_F(ResultQueueMgrTest, create_normal) {
    BlockQueueSharedPtr block_queue_t;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;
    ResultQueueMgr queue_mgr;
    queue_mgr.create_queue(query_id, &block_queue_t);
    EXPECT_TRUE(block_queue_t != nullptr);
}

TEST_F(ResultQueueMgrTest, create_same_queue) {
    ResultQueueMgr queue_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    BlockQueueSharedPtr block_queue_t_1;
    queue_mgr.create_queue(query_id, &block_queue_t_1);
    EXPECT_TRUE(block_queue_t_1 != nullptr);

    BlockQueueSharedPtr block_queue_t_2;
    queue_mgr.create_queue(query_id, &block_queue_t_2);
    EXPECT_TRUE(block_queue_t_2 != nullptr);

    EXPECT_EQ(block_queue_t_1.get(), block_queue_t_2.get());
}

TEST_F(ResultQueueMgrTest, fetch_result_normal) {
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;
    ResultQueueMgr queue_mgr;

    BlockQueueSharedPtr block_queue_t;
    queue_mgr.create_queue(query_id, &block_queue_t);
    EXPECT_TRUE(block_queue_t != nullptr);

    std::shared_ptr<arrow::Field> field = arrow::field("k1", arrow::int32(), true);
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.push_back(field);
    std::shared_ptr<arrow::Schema> schema = arrow::schema(std::move(fields));

    std::shared_ptr<arrow::Array> k1_col;
    arrow::NumericBuilder<arrow::Int32Type> builder;

    auto st = builder.Reserve(1);
    if (!st.ok()) {
        LOG(WARNING) << "Reserve error";
    }
    st = builder.Append(20);
    if (!st.ok()) {
        LOG(WARNING) << "Append error";
    }
    st = builder.Finish(&k1_col);
    if (!st.ok()) {
        LOG(WARNING) << "Finish error";
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.push_back(k1_col);
    std::shared_ptr<arrow::RecordBatch> record_batch =
            arrow::RecordBatch::Make(schema, 1, std::move(arrays));
    block_queue_t->blocking_put(record_batch);
    // sentinel
    block_queue_t->blocking_put(nullptr);

    std::shared_ptr<arrow::RecordBatch> result;
    bool eos;
    EXPECT_TRUE(queue_mgr.fetch_result(query_id, &result, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, result->num_rows());
    EXPECT_EQ(1, result->num_columns());
}

TEST_F(ResultQueueMgrTest, fetch_result_end) {
    ResultQueueMgr queue_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    BlockQueueSharedPtr block_queue_t;
    queue_mgr.create_queue(query_id, &block_queue_t);
    EXPECT_TRUE(block_queue_t != nullptr);
    block_queue_t->blocking_put(nullptr);

    std::shared_ptr<arrow::RecordBatch> result;
    bool eos;
    EXPECT_TRUE(queue_mgr.fetch_result(query_id, &result, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_TRUE(result == nullptr);
}

TEST_F(ResultQueueMgrTest, normal_cancel) {
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;
    ResultQueueMgr queue_mgr;
    BlockQueueSharedPtr block_queue_t;
    queue_mgr.create_queue(query_id, &block_queue_t);
    EXPECT_TRUE(block_queue_t != nullptr);
    EXPECT_TRUE(queue_mgr.cancel(query_id).ok());
}

TEST_F(ResultQueueMgrTest, cancel_no_block) {
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;
    ResultQueueMgr queue_mgr;
    BlockQueueSharedPtr block_queue_t;
    queue_mgr.create_queue(query_id, &block_queue_t);
    EXPECT_TRUE(block_queue_t != nullptr);
    EXPECT_TRUE(queue_mgr.cancel(query_id).ok());
}
} // namespace doris
