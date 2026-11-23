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

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "exec/pipeline/dependency.h"
#include "exec/sink/writer/vmysql_result_writer.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class MysqlResultBlockBufferTest : public ::testing::Test {
public:
    MysqlResultBlockBufferTest() = default;
    ~MysqlResultBlockBufferTest() = default;

protected:
    static std::shared_ptr<TFetchDataResult> make_batch(
            const std::vector<std::string>& rows,
            const std::map<std::string, std::string>& infos = {}) {
        auto result = std::make_shared<TFetchDataResult>();
        result->result_batch.__set_rows(rows);
        result->result_batch.__set_attached_infos(infos);
        return result;
    }
};

class MockGetResultBatchCtx : public GetResultBatchCtx {
public:
    ENABLE_FACTORY_CREATOR(MockGetResultBatchCtx)
    MockGetResultBatchCtx(std::function<void()> fail_cb, std::function<void()> close_cb,
                          std::function<void()> data_cb)
            : _fail_cb(fail_cb), _close_cb(close_cb), _data_cb(data_cb) {}
    ~MockGetResultBatchCtx() override = default;

    void on_failure(const Status& status) override { _fail_cb(); }
    void on_close(int64_t packet_seq, int64_t returned_rows = 0) override { _close_cb(); }
    Status on_data(const std::shared_ptr<TFetchDataResult>& t_result, int64_t packet_seq,
                   ResultBlockBufferBase* buffer) override {
        data_result = t_result;
        data_packet_seq = packet_seq;
        _data_cb();
        return Status::OK();
    }

    std::shared_ptr<TFetchDataResult> data_result;
    int64_t data_packet_seq = -1;

private:
    std::function<void()> _fail_cb;
    std::function<void()> _close_cb;
    std::function<void()> _data_cb;
};

// GTest assertions verify row accounting and backpressure after each enqueue and fetch.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(MysqlResultBlockBufferTest, MetadataBatchesStaySeparate) {
    MockRuntimeState first_state;
    MockRuntimeState second_state;
    first_state._batch_size = 1;
    second_state._fragment_instance_id.lo = 1;
    const auto first_id = first_state.fragment_instance_id();
    const auto second_id = second_state.fragment_instance_id();
    auto first_dep = Dependency::create_shared(0, 0, "First", true);
    auto second_dep = Dependency::create_shared(0, 0, "Second", true);
    MySQLResultBlockBuffer buffer(TUniqueId(), &first_state, 16);
    buffer.set_dependency(first_id, first_dep);
    buffer.set_dependency(second_id, second_dep);

    const std::vector<MockRuntimeState*> states = {&first_state, &second_state, &first_state};
    const std::vector<std::vector<std::string>> rows = {
            {"aa", "bbb"}, {"cccc", "ddddd"}, {"eeeeee"}};
    const std::vector<std::map<std::string, std::string>> infos = {
            {{"SingleFileURL", "s3://bucket/first.csv"}, {"TotalRows", "2"}},
            {{"SingleFileURL", "s3://bucket/second.csv"}, {"TotalRows", "3"}},
            {{"SingleFileURL", "s3://bucket/third.csv"}, {"TotalRows", "4"}}};
    const size_t batch_bytes[] = {5, 9, 6};
    const size_t first_queued[] = {2, 2, 3};
    const size_t second_queued[] = {0, 2, 2};
    for (size_t i = 0; i < rows.size(); ++i) {
        auto result = make_batch(rows[i], infos[i]);
        ASSERT_TRUE(buffer.add_batch(states[i], result).ok());
        ASSERT_EQ(i + 1, buffer._result_batch_queue.size());
        ASSERT_EQ(i + 1, buffer._instance_rows_in_queue.size());
        EXPECT_EQ(rows[i], buffer._result_batch_queue.back()->result_batch.rows);
        EXPECT_EQ(infos[i], buffer._result_batch_queue.back()->result_batch.attached_infos);
        EXPECT_EQ(batch_bytes[i], buffer._last_batch_bytes);
        ASSERT_EQ(1, buffer._instance_rows_in_queue.back().size());
        EXPECT_EQ(rows[i].size(),
                  buffer._instance_rows_in_queue.back().at(states[i]->fragment_instance_id()));
        EXPECT_EQ(first_queued[i], buffer._instance_rows[first_id]);
        EXPECT_EQ(second_queued[i], buffer._instance_rows[second_id]);
        EXPECT_EQ(first_queued[i] <= 1, first_dep->ready());
        EXPECT_EQ(second_queued[i] <= 1, second_dep->ready());
    }

    auto ctx = MockGetResultBatchCtx::create_shared([] { ADD_FAILURE() << "Unexpected failure"; },
                                                    [] { ADD_FAILURE() << "Unexpected close"; },
                                                    [] {});
    const size_t first_remaining[] = {1, 1, 0};
    const size_t second_remaining[] = {2, 0, 0};
    for (size_t i = 0; i < rows.size(); ++i) {
        ASSERT_TRUE(buffer.get_batch(ctx).ok());
        ASSERT_NE(nullptr, ctx->data_result);
        EXPECT_EQ(i, ctx->data_packet_seq);
        EXPECT_EQ(rows[i], ctx->data_result->result_batch.rows);
        EXPECT_TRUE(ctx->data_result->result_batch.__isset.attached_infos);
        EXPECT_EQ(infos[i], ctx->data_result->result_batch.attached_infos);
        EXPECT_EQ(rows.size() - i - 1, buffer._result_batch_queue.size());
        EXPECT_EQ(buffer._result_batch_queue.size(), buffer._instance_rows_in_queue.size());
        EXPECT_EQ(first_remaining[i], buffer._instance_rows[first_id]);
        EXPECT_EQ(second_remaining[i], buffer._instance_rows[second_id]);
        EXPECT_EQ(first_remaining[i] <= 1, first_dep->ready());
        EXPECT_EQ(second_remaining[i] <= 1, second_dep->ready());
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(i + 1, buffer._packet_num);
    }
}

// Keep the GTest assertions for each stage of the same mixed queue together.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(MysqlResultBlockBufferTest, PlainBatchesMergeOnlyWithPlainBatches) {
    MockRuntimeState first_state;
    MockRuntimeState second_state;
    first_state._batch_size = 1;
    second_state._fragment_instance_id.lo = 1;
    const auto first_id = first_state.fragment_instance_id();
    const auto second_id = second_state.fragment_instance_id();
    auto first_dep = Dependency::create_shared(0, 0, "First", true);
    auto second_dep = Dependency::create_shared(0, 0, "Second", true);
    MySQLResultBlockBuffer buffer(TUniqueId(), &first_state, 16);
    buffer.set_dependency(first_id, first_dep);
    buffer.set_dependency(second_id, second_dep);
    const std::map<std::string, std::string> first_infos = {{"SingleFileURL", "first.csv"}};
    const std::map<std::string, std::string> second_infos = {{"SingleFileURL", "second.csv"}};
    auto enqueue = [&](MockRuntimeState* state, const std::string& row,
                       const std::map<std::string, std::string>& infos, size_t queue_size,
                       size_t last_bytes) {
        auto result = make_batch({row}, infos);
        ASSERT_TRUE(buffer.add_batch(state, result).ok());
        EXPECT_EQ(queue_size, buffer._result_batch_queue.size());
        EXPECT_EQ(queue_size, buffer._instance_rows_in_queue.size());
        EXPECT_EQ(last_bytes, buffer._last_batch_bytes);
    };

    // Empty attached_infos, including an explicitly set empty map, still permit merging.
    auto plain = make_batch({"aa"});
    plain->result_batch.__isset.attached_infos = false;
    ASSERT_TRUE(buffer.add_batch(&first_state, plain).ok());
    enqueue(&second_state, "bbb", {}, 1, 5);
    enqueue(&first_state, "ccccccc", first_infos, 2, 7);
    enqueue(&second_state, "dd", {}, 3, 2);
    enqueue(&first_state, "eee", {}, 3, 5);
    enqueue(&second_state, "fffffffff", second_infos, 4, 9);
    EXPECT_EQ(3, buffer._instance_rows[first_id]);
    EXPECT_EQ(3, buffer._instance_rows[second_id]);
    EXPECT_FALSE(first_dep->ready());
    EXPECT_FALSE(second_dep->ready());

    const std::vector<std::vector<std::string>> expected_rows = {
            {"aa", "bbb"}, {"ccccccc"}, {"dd", "eee"}, {"fffffffff"}};
    const std::vector<std::map<std::string, std::string>> expected_infos = {
            {}, first_infos, {}, second_infos};
    const std::vector<std::unordered_map<TUniqueId, size_t>> expected_instance_rows = {
            {{first_id, 1}, {second_id, 1}},
            {{first_id, 1}},
            {{first_id, 1}, {second_id, 1}},
            {{second_id, 1}}};
    auto ctx = MockGetResultBatchCtx::create_shared([] { ADD_FAILURE() << "Unexpected failure"; },
                                                    [] { ADD_FAILURE() << "Unexpected close"; },
                                                    [] {});
    const size_t first_remaining[] = {2, 1, 0, 0};
    const size_t second_remaining[] = {2, 2, 1, 0};
    for (size_t i = 0; i < expected_rows.size(); ++i) {
        ASSERT_FALSE(buffer._instance_rows_in_queue.empty());
        EXPECT_EQ(expected_instance_rows[i], buffer._instance_rows_in_queue.front());
        ASSERT_TRUE(buffer.get_batch(ctx).ok());
        ASSERT_NE(nullptr, ctx->data_result);
        EXPECT_EQ(i, ctx->data_packet_seq);
        EXPECT_EQ(expected_rows[i], ctx->data_result->result_batch.rows);
        EXPECT_EQ(expected_infos[i], ctx->data_result->result_batch.attached_infos);
        EXPECT_EQ(expected_rows.size() - i - 1, buffer._result_batch_queue.size());
        EXPECT_EQ(buffer._result_batch_queue.size(), buffer._instance_rows_in_queue.size());
        EXPECT_EQ(first_remaining[i], buffer._instance_rows[first_id]);
        EXPECT_EQ(second_remaining[i], buffer._instance_rows[second_id]);
        EXPECT_EQ(first_remaining[i] <= 1, first_dep->ready());
        EXPECT_EQ(second_remaining[i] <= 1, second_dep->ready());
    }
}

TEST_F(MysqlResultBlockBufferTest, WaitingRpcPreservesMetadata) {
    MockRuntimeState state;
    MySQLResultBlockBuffer buffer(TUniqueId(), &state, 16);
    auto ctx = MockGetResultBatchCtx::create_shared([] { ADD_FAILURE() << "Unexpected failure"; },
                                                    [] { ADD_FAILURE() << "Unexpected close"; },
                                                    [] {});
    ASSERT_TRUE(buffer.get_batch(ctx).ok());
    ASSERT_EQ(1, buffer._waiting_rpc.size());
    auto result = make_batch({"row"}, {{"SingleFileURL", "s3://bucket/part_0.csv"}});
    ASSERT_TRUE(buffer.add_batch(&state, result).ok());
    ASSERT_EQ(result, ctx->data_result);
    EXPECT_EQ(0, ctx->data_packet_seq);
    EXPECT_EQ(1, buffer._packet_num);
    EXPECT_TRUE(ctx->data_result->result_batch.__isset.attached_infos);
    EXPECT_EQ("s3://bucket/part_0.csv",
              ctx->data_result->result_batch.attached_infos.at("SingleFileURL"));
    EXPECT_TRUE(buffer._waiting_rpc.empty());
    EXPECT_TRUE(buffer._result_batch_queue.empty());
    EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
    EXPECT_TRUE(buffer._instance_rows.empty());
}

TEST_F(MysqlResultBlockBufferTest, TestMySQLResultBlockBuffer) {
    MockRuntimeState state;
    state._batch_size = 1;
    int buffer_size = 16;
    auto dep = Dependency::create_shared(0, 0, "Test", true);
    auto ins_id = TUniqueId();
    bool fail = false;
    bool close = false;
    bool data = false;
    MySQLResultBlockBuffer buffer(TUniqueId(), &state, buffer_size);
    buffer.set_dependency(ins_id, dep);
    std::shared_ptr<GetResultBatchCtx> ctx = MockGetResultBatchCtx::create_shared(
            [&]() -> void { fail = true; }, [&]() -> void { close = true; },
            [&]() -> void { data = true; });

    {
        uint8_t test_byte = 1;
        auto num_rows = 2;
        auto tresult = std::make_shared<TFetchDataResult>();
        tresult->result_batch.rows.resize(num_rows);
        tresult->result_batch.rows[0].append((const char*)&test_byte, sizeof(test_byte));
        tresult->result_batch.rows[1].append((const char*)&test_byte, sizeof(test_byte));
        EXPECT_TRUE(buffer.add_batch(&state, tresult).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], num_rows);
        EXPECT_EQ(buffer._instance_rows_in_queue.back()[ins_id], num_rows);
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 0);
        EXPECT_EQ(buffer._result_batch_queue.size(), 1);
        EXPECT_FALSE(dep->ready());

        tresult = std::make_shared<TFetchDataResult>();
        tresult->result_batch.rows.resize(num_rows);
        tresult->result_batch.rows[0].append((const char*)&test_byte, sizeof(test_byte));
        tresult->result_batch.rows[1].append((const char*)&test_byte, sizeof(test_byte));
        EXPECT_TRUE(buffer.add_batch(&state, tresult).ok());
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
        uint8_t test_byte = 1;
        auto num_rows = 2;
        auto tresult = std::make_shared<TFetchDataResult>();
        tresult->result_batch.rows.resize(num_rows);
        tresult->result_batch.rows[0].append((const char*)&test_byte, sizeof(test_byte));
        tresult->result_batch.rows[1].append((const char*)&test_byte, sizeof(test_byte));
        EXPECT_TRUE(buffer.add_batch(&state, tresult).ok());
        EXPECT_EQ(buffer._instance_rows[ins_id], 0);
        EXPECT_TRUE(buffer._instance_rows_in_queue.empty());
        EXPECT_TRUE(buffer._waiting_rpc.empty());
        EXPECT_EQ(buffer._packet_num, 2);
        EXPECT_EQ(buffer._result_batch_queue.size(), 0);
        EXPECT_TRUE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
        EXPECT_TRUE(dep->ready());
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
        bool is_fully_closed = false;
        EXPECT_TRUE(buffer.close(ins_id, Status::OK(), 0, is_fully_closed).ok());
        EXPECT_TRUE(is_fully_closed);
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

TEST_F(MysqlResultBlockBufferTest, TestCancelMySQLResultBlockBuffer) {
    MockRuntimeState state;
    state._batch_size = 1;
    int buffer_size = 16;
    auto dep = Dependency::create_shared(0, 0, "Test", true);
    auto ins_id = TUniqueId();
    bool fail = false;
    bool close = false;
    bool data = false;
    MySQLResultBlockBuffer buffer(TUniqueId(), &state, buffer_size);
    buffer.set_dependency(ins_id, dep);
    std::shared_ptr<GetResultBatchCtx> ctx = MockGetResultBatchCtx::create_shared(
            [&]() -> void { fail = true; }, [&]() -> void { close = true; },
            [&]() -> void { data = true; });

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
        uint8_t test_byte = 1;
        auto num_rows = 2;
        auto tresult = std::make_shared<TFetchDataResult>();
        tresult->result_batch.rows.resize(num_rows);
        tresult->result_batch.rows[0].append((const char*)&test_byte, sizeof(test_byte));
        tresult->result_batch.rows[1].append((const char*)&test_byte, sizeof(test_byte));
        EXPECT_FALSE(buffer.add_batch(&state, tresult).ok());
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

TEST_F(MysqlResultBlockBufferTest, TestErrorClose) {
    MockRuntimeState state;
    state._batch_size = 1;
    int buffer_size = 16;
    auto dep = Dependency::create_shared(0, 0, "Test", true);
    auto ins_id = TUniqueId();
    bool fail = false;
    bool close = false;
    bool data = false;
    MySQLResultBlockBuffer buffer(TUniqueId(), &state, buffer_size);
    buffer.set_dependency(ins_id, dep);
    std::shared_ptr<GetResultBatchCtx> ctx = MockGetResultBatchCtx::create_shared(
            [&]() -> void { fail = true; }, [&]() -> void { close = true; },
            [&]() -> void { data = true; });

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
        bool is_fully_closed = false;
        EXPECT_EQ(buffer.close(ins_id, Status::InternalError(""), 0, is_fully_closed).code(),
                  ErrorCode::INTERNAL_ERROR);
        EXPECT_TRUE(is_fully_closed);
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
        auto new_dep = Dependency::create_shared(0, 0, "Test", true);
        buffer.set_dependency(new_ins_id, new_dep);
        bool is_fully_closed = true; // will be set to false since new_dep remains
        EXPECT_EQ(buffer.close(ins_id, Status::InternalError(""), 0, is_fully_closed).code(),
                  ErrorCode::INTERNAL_ERROR);
        EXPECT_FALSE(is_fully_closed);
        EXPECT_FALSE(data);
        EXPECT_FALSE(close);
        EXPECT_FALSE(fail);
    }
}

} // namespace doris
