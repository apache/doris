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

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <butil/iobuf.h>
#include <gen_cpp/Data_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include "pipeline/dependency.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/sink/varrow_flight_result_writer.h"

namespace doris::vectorized {

class ArrowResultBlockBufferTest : public ::testing::Test {
public:
    ArrowResultBlockBufferTest() = default;
    ~ArrowResultBlockBufferTest() = default;
};

class MockGetArrowResultBatchCtx : public GetArrowResultBatchCtx {
public:
    ENABLE_FACTORY_CREATOR(MockGetArrowResultBatchCtx)
    MockGetArrowResultBatchCtx(std::function<void()> fail_cb, std::function<void()> close_cb,
                               std::function<void()> data_cb, PFetchArrowDataResult* result)
            : GetArrowResultBatchCtx(result),
              _fail_cb(fail_cb),
              _close_cb(close_cb),
              _data_cb(data_cb) {}
    ~MockGetArrowResultBatchCtx() override = default;

    void on_failure(const Status& status) override { _fail_cb(); }
    void on_close(int64_t packet_seq, int64_t returned_rows = 0) override { _close_cb(); }
    Status on_data(const std::shared_ptr<vectorized::Block>& t_result, int64_t packet_seq,
                   ResultBlockBufferBase* buffer) override {
        _data_cb();
        return GetArrowResultBatchCtx::on_data(t_result, packet_seq, buffer);
    }

private:
    std::function<void()> _fail_cb;
    std::function<void()> _close_cb;
    std::function<void()> _data_cb;
};

TEST_F(ArrowResultBlockBufferTest, TestArrowResultBlockBuffer) {
    MockRuntimeState state;
    state.batsh_size = 1;
    int buffer_size = 16;
    auto dep = pipeline::Dependency::create_shared(0, 0, "Test", true);
    auto ins_id = TUniqueId();
    bool fail = false;
    bool close = false;
    bool data = false;
    std::shared_ptr<arrow::Schema> schema;
    ArrowFlightResultBlockBuffer buffer(TUniqueId(), &state, schema, buffer_size);
    buffer.set_dependency(ins_id, dep);
    PFetchArrowDataResult presult;
    std::shared_ptr<GetArrowResultBatchCtx> ctx = MockGetArrowResultBatchCtx::create_shared(
            [&]() -> void { fail = true; }, [&]() -> void { close = true; },
            [&]() -> void { data = true; }, &presult);

    {
        auto num_rows = 2;
        auto in_block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
        EXPECT_TRUE(buffer.add_batch(&state, in_block).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], num_rows);
        EXPECT_EQ(buffer._instance_rows_in_queue.back()[ins_id], num_rows);
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 1);
        EXPECT_FALSE(dep->ready());

        in_block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
        EXPECT_TRUE(buffer.add_batch(&state, in_block).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], num_rows * 2);
        EXPECT_EQ(buffer._instance_rows_in_queue.back()[ins_id], num_rows * 2);
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 1);
        EXPECT_FALSE(dep->ready());
    }
    {
        EXPECT_TRUE(buffer.get_batch(ctx).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 1);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_TRUE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
        EXPECT_EQ(presult.empty_batch(), false);
        EXPECT_EQ(presult.packet_seq(), 0);
        EXPECT_EQ(presult.eos(), false);
        EXPECT_EQ(presult.status().status_code(), ErrorCode::OK);
        data = false;
    }
    {
        EXPECT_TRUE(buffer.get_batch(ctx).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(buffer._waiting_rpc.size(), 1);
        EXPECT_EQ(buffer._packet_num, 1);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
    }
    {
        auto in_block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
        EXPECT_TRUE(buffer.add_batch(&state, in_block).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 2);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
        EXPECT_TRUE(dep->ready());
        EXPECT_EQ(presult.empty_batch(), false);
        EXPECT_EQ(presult.packet_seq(), 1);
        EXPECT_EQ(presult.eos(), false);
        EXPECT_EQ(presult.status().status_code(), ErrorCode::OK);
        data = false;
    }
    {
        EXPECT_TRUE(buffer.get_batch(ctx).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(buffer._waiting_rpc.size(), 1);
        EXPECT_EQ(buffer._packet_num, 2);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
    }
    {
        EXPECT_TRUE(buffer.close(ins_id, Status::OK(), 0).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(buffer._waiting_rpc.size(), 0);
        EXPECT_EQ(buffer._packet_num, 2);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_FALSE(data);
        EXPECT_TRUE(close);
        EXPECT_FALSE(fail);
        close = false;
    }
    {
        EXPECT_TRUE(buffer.get_batch(ctx).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(buffer._waiting_rpc.size(), 0);
        EXPECT_EQ(buffer._packet_num, 2);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_FALSE(data);
        EXPECT_TRUE(close);
        EXPECT_FALSE(fail);
    }
}

TEST_F(ArrowResultBlockBufferTest, TestCancelArrowResultBlockBuffer) {
    MockRuntimeState state;
    state.batsh_size = 1;
    int buffer_size = 16;
    auto dep = pipeline::Dependency::create_shared(0, 0, "Test", true);
    auto ins_id = TUniqueId();
    bool fail = false;
    bool close = false;
    bool data = false;
    std::shared_ptr<arrow::Schema> schema;
    ArrowFlightResultBlockBuffer buffer(TUniqueId(), &state, schema, buffer_size);
    buffer.set_dependency(ins_id, dep);
    PFetchArrowDataResult presult;
    std::shared_ptr<GetArrowResultBatchCtx> ctx = MockGetArrowResultBatchCtx::create_shared(
            [&]() -> void { fail = true; }, [&]() -> void { close = true; },
            [&]() -> void { data = true; }, &presult);

    {
        EXPECT_TRUE(buffer.get_batch(ctx).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(buffer._waiting_rpc.size(), 1);
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
    }
    {
        auto cancel_status = Status::InternalError("");
        buffer.cancel(cancel_status);
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(buffer._waiting_rpc.size(), 0);
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_EQ(buffer._status.code(), ErrorCode::INTERNAL_ERROR);
        EXPECT_TRUE(dep->ready());
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_TRUE(fail);
        fail = false;
    }
    {
        EXPECT_EQ(buffer.get_batch(ctx).code(), ErrorCode::INTERNAL_ERROR);
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_TRUE(fail);
        EXPECT_TRUE(dep->ready());
        fail = false;
    }
    {
        auto in_block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
        EXPECT_FALSE(buffer.add_batch(&state, in_block).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
        EXPECT_TRUE(dep->ready());
    }
}

TEST_F(ArrowResultBlockBufferTest, TestErrorClose) {
    MockRuntimeState state;
    state.batsh_size = 1;
    int buffer_size = 16;
    auto dep = pipeline::Dependency::create_shared(0, 0, "Test", true);
    auto ins_id = TUniqueId();
    bool fail = false;
    bool close = false;
    bool data = false;
    std::shared_ptr<arrow::Schema> schema;
    ArrowFlightResultBlockBuffer buffer(TUniqueId(), &state, schema, buffer_size);
    buffer.set_dependency(ins_id, dep);
    PFetchArrowDataResult presult;
    std::shared_ptr<GetArrowResultBatchCtx> ctx = MockGetArrowResultBatchCtx::create_shared(
            [&]() -> void { fail = true; }, [&]() -> void { close = true; },
            [&]() -> void { data = true; }, &presult);

    {
        EXPECT_TRUE(buffer.get_batch(ctx).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(buffer._waiting_rpc.size(), 1);
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
    }
    {
        EXPECT_EQ(buffer.close(ins_id, Status::InternalError(""), 0).code(),
                  ErrorCode::INTERNAL_ERROR);
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(buffer._waiting_rpc.size(), 0);
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_TRUE(buffer._result_sink_dependencies.empty());
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_TRUE(fail);
        fail = false;
    }
    {
        auto new_ins_id = TUniqueId();
        new_ins_id.lo = 1;
        auto new_dep = pipeline::Dependency::create_shared(0, 0, "Test", true);
        buffer.set_dependency(new_ins_id, new_dep);
        EXPECT_EQ(buffer.close(ins_id, Status::InternalError(""), 0).code(),
                  ErrorCode::INTERNAL_ERROR);
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
    }
}

TEST_F(ArrowResultBlockBufferTest, TestArrowResultSerializeFailure) {
    MockRuntimeState state;
    state.batsh_size = 1;
    int buffer_size = 16;
    auto dep = pipeline::Dependency::create_shared(0, 0, "Test", true);
    auto ins_id = TUniqueId();
    bool fail = false;
    bool close = false;
    bool data = false;
    std::shared_ptr<arrow::Schema> schema;
    ArrowFlightResultBlockBuffer buffer(TUniqueId(), &state, schema, buffer_size);
    buffer.set_dependency(ins_id, dep);
    PFetchArrowDataResult presult;
    std::shared_ptr<GetArrowResultBatchCtx> ctx = MockGetArrowResultBatchCtx::create_shared(
            [&]() -> void { fail = true; }, [&]() -> void { close = true; },
            [&]() -> void { data = true; }, &presult);

    {
        auto num_rows = 2;
        auto in_block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
        EXPECT_TRUE(buffer.add_batch(&state, in_block).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], num_rows);
        EXPECT_EQ(buffer._instance_rows_in_queue.back()[ins_id], num_rows);
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 1);
        EXPECT_FALSE(dep->ready());

        in_block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
        EXPECT_TRUE(buffer.add_batch(&state, in_block).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], num_rows * 2);
        EXPECT_EQ(buffer._instance_rows_in_queue.back()[ins_id], num_rows * 2);
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 1);
        EXPECT_FALSE(dep->ready());
    }
    {
        ctx->_max_msg_size = 0;
        EXPECT_TRUE(buffer.get_batch(ctx).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 1);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(dep->ready());
        EXPECT_TRUE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
        EXPECT_EQ(presult.empty_batch(), false);
        EXPECT_EQ(presult.packet_seq(), 0);
        EXPECT_EQ(presult.eos(), false);
        EXPECT_EQ(presult.status().status_code(), ErrorCode::INTERNAL_ERROR);
        data = false;
    }
}

} // namespace doris::vectorized
