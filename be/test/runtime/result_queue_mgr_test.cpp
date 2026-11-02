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

#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <chrono>
#include <memory>
#include <ostream>
#include <thread>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/record_batch_queue.h"

namespace arrow {
class Array;
} // namespace arrow

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

// MemoryScratchSink puts the sentinel when its task closes, also after a failure. A successful
// close records OK, which must leave the sentinel a normal end of the scan.
TEST_F(ResultQueueMgrTest, fetch_result_end_after_ok_status) {
    ResultQueueMgr queue_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    BlockQueueSharedPtr block_queue_t;
    queue_mgr.create_queue(query_id, &block_queue_t);
    block_queue_t->update_status(Status::OK());
    block_queue_t->blocking_put(nullptr);

    std::shared_ptr<arrow::RecordBatch> result;
    bool eos = false;
    EXPECT_TRUE(queue_mgr.fetch_result(query_id, &result, &eos).ok());
    EXPECT_TRUE(eos);
}

// A failed scan (for example a batch the Arrow Flight convertor rejects) also ends with the
// sentinel. The failure is recorded before it, and must reach the external scanner instead of a
// successful end with the rows fetched so far, also when its fetch already waits for data: that
// fetch passed the status check before the failure happened.
TEST_F(ResultQueueMgrTest, fetch_result_reports_failure_recorded_before_sentinel) {
    ResultQueueMgr queue_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    BlockQueueSharedPtr block_queue_t;
    queue_mgr.create_queue(query_id, &block_queue_t);

    std::shared_ptr<arrow::RecordBatch> waiting_result;
    bool waiting_eos = false;
    Status waiting_status;
    std::thread waiting_fetch([&]() {
        waiting_status = queue_mgr.fetch_result(query_id, &waiting_result, &waiting_eos);
    });
    // Let the fetch pass its first status check and block on the empty queue.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    block_queue_t->update_status(Status::InvalidArgument("rejected Arrow batch"));
    block_queue_t->blocking_put(nullptr);
    waiting_fetch.join();
    EXPECT_TRUE(waiting_status.is<ErrorCode::INVALID_ARGUMENT>()) << waiting_status;

    // Later fetches keep reporting the failure instead of the end of the scan.
    std::shared_ptr<arrow::RecordBatch> result;
    bool eos = false;
    const auto status = queue_mgr.fetch_result(query_id, &result, &eos);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
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
