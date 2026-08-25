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

#include "exec/operator/iceberg_table_sink_operator.h"

#include <gtest/gtest.h>

#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/spill_iceberg_table_sink_operator.h"
#include "exec/sink/writer/async_result_writer.h"
#include "exec/sink/writer/iceberg/viceberg_sort_writer.h"
#include "runtime/runtime_profile.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

static_assert(!std::is_base_of_v<AsyncResultWriter, VIcebergTableWriter>);

namespace {

TDataSink make_iceberg_table_sink() {
    TIcebergTableSink iceberg_sink;
    TDataSink sink;
    sink.__set_type(TDataSinkType::ICEBERG_TABLE_SINK);
    sink.__set_iceberg_table_sink(iceberg_sink);
    return sink;
}

struct FakeWriterState {
    Status open_result;
    Status write_result;
    Status close_result;
    int open_count = 0;
    int write_count = 0;
    int close_count = 0;
    int cleanup_count = 0;
    size_t written_rows = 0;
    std::vector<Status> close_inputs;
    Status cleanup_status;
    std::function<void()> close_hook;
};

class FakeIcebergTableWriter final : public VIcebergTableWriter {
public:
    FakeIcebergTableWriter(const VExprContextSPtrs& output_exprs,
                           std::shared_ptr<FakeWriterState> state)
            : VIcebergTableWriter(make_iceberg_table_sink(), output_exprs),
              _fake_state(std::move(state)) {}

    Status open(RuntimeState*, RuntimeProfile*) override {
        ++_fake_state->open_count;
        return _fake_state->open_result;
    }

    Status write(RuntimeState*, Block& block) override {
        ++_fake_state->write_count;
        _fake_state->written_rows += block.rows();
        return _fake_state->write_result;
    }

    Status close(Status status) override {
        ++_fake_state->close_count;
        _fake_state->close_inputs.emplace_back(status);
        if (_fake_state->close_hook) {
            _fake_state->close_hook();
        }
        return _fake_state->close_result;
    }

    void finish_deferred_file_cleanup(Status status) override {
        ++_fake_state->cleanup_count;
        _fake_state->cleanup_status = status;
    }

private:
    std::shared_ptr<FakeWriterState> _fake_state;
};

struct FakeSortWriterState {
    size_t data_size = 0;
    int spill_count = 0;
};

class FakeRevocableIcebergSortWriter final : public VIcebergSortWriter {
public:
    explicit FakeRevocableIcebergSortWriter(std::shared_ptr<FakeSortWriterState> state)
            : VIcebergSortWriter(nullptr, TSortInfo {}, 0), _fake_state(std::move(state)) {}

    size_t data_size() const override { return _fake_state->data_size; }

    Status trigger_spill() override {
        ++_fake_state->spill_count;
        _fake_state->data_size = 0;
        return Status::OK();
    }

private:
    std::shared_ptr<FakeSortWriterState> _fake_state;
};

} // namespace

class IcebergTableSinkOperatorTest : public testing::Test {
protected:
    template <typename Parent, typename LocalState>
    void initialize_local_state(Parent* parent, LocalState* local_state, MockRuntimeState* state,
                                std::shared_ptr<FakeWriterState>* fake_state) {
        TDataSink sink = make_iceberg_table_sink();
        ASSERT_TRUE(parent->init(sink).ok());

        auto shared_state = parent->create_shared_state();
        LocalSinkStateInfo info {.task_idx = 0,
                                 .parent_profile = &_parent_profile,
                                 .sender_id = 0,
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = {},
                                 .tsink = sink};
        ASSERT_TRUE(local_state->init(state, info).ok());

        *fake_state = std::make_shared<FakeWriterState>();
        auto writer = std::make_unique<FakeIcebergTableWriter>(_output_exprs, *fake_state);
        local_state->_writer = std::move(writer);
        ASSERT_TRUE(local_state->open(state).ok());
    }

    void set_partition_writers(SpillIcebergTableSinkLocalState* local_state,
                               std::shared_ptr<VIcebergSortWriter> first,
                               std::shared_ptr<VIcebergSortWriter> second) {
        auto current_writer = second;
        local_state->_writer->_partitions_to_writers.emplace("first", std::move(first));
        local_state->_writer->_partitions_to_writers.emplace("second", std::move(second));
        local_state->_writer->_current_writer.store(std::move(current_writer));
    }

    void mark_spilled(VIcebergSortWriter* writer, size_t count = 1) {
        for (size_t i = 0; i < count; ++i) {
            writer->_sorted_spill_files.emplace_back(nullptr);
        }
    }

    void initialize_sort_writer_for_write(VIcebergSortWriter* writer, RuntimeState* state,
                                          const DataTypePtr& data_type) {
        _sort_row_desc =
                std::make_unique<MockRowDescriptor>(std::vector<DataTypePtr> {data_type}, &_pool);
        writer->_runtime_state = state;
        writer->_ordering_expr_ctxs = MockSlotRef::create_mock_contexts(0, data_type);
        writer->_sort_info.is_asc_order = {true};
        writer->_sort_info.nulls_first = {false};
        writer->_sorter = FullSorter::create_unique(
                writer->_ordering_expr_ctxs, -1, 0, &writer->_pool, writer->_sort_info.is_asc_order,
                writer->_sort_info.nulls_first, *_sort_row_desc, state, nullptr);
        writer->_sorter->set_enable_spill();
        writer->_target_file_size_bytes = std::numeric_limits<int64_t>::max();
    }

    std::pair<size_t, size_t> spill_batch_state(const VIcebergSortWriter& writer) const {
        return {writer._avg_row_bytes, writer._spill_block_batch_row_count};
    }

    ObjectPool _pool;
    RowDescriptor _row_desc;
    std::unique_ptr<MockRowDescriptor> _sort_row_desc;
    VExprContextSPtrs _output_exprs;
    RuntimeProfile _parent_profile {"IcebergTableSinkOperatorTest"};
};

TEST_F(IcebergTableSinkOperatorTest, SyncWritersUseBlockingSchedulerWithoutDependencies) {
    IcebergTableSinkLocalState table_sink(nullptr, nullptr);
    SpillIcebergTableSinkLocalState spill_sink(nullptr, nullptr);

    EXPECT_TRUE(table_sink.is_blockable());
    EXPECT_TRUE(spill_sink.is_blockable());
    EXPECT_TRUE(table_sink.dependencies().empty());
    EXPECT_TRUE(spill_sink.dependencies().empty());
    EXPECT_EQ(table_sink.finishdependency(), nullptr);
    EXPECT_EQ(spill_sink.finishdependency(), nullptr);
}

TEST_F(IcebergTableSinkOperatorTest, NormalSinkWritesDataAndClosesAfterEmptyEos) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    IcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    IcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);

    Block data = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    Block empty = ColumnHelper::create_block<DataTypeInt32>({});
    ASSERT_TRUE(local_state.sink(&state, &data, false).ok());
    ASSERT_TRUE(local_state.sink(&state, &empty, true).ok());
    EXPECT_EQ(fake_state->write_count, 1);
    EXPECT_EQ(fake_state->written_rows, 3);
    EXPECT_EQ(fake_state->close_count, 0);

    ASSERT_TRUE(local_state.close(&state, Status::OK()).ok());
    EXPECT_EQ(fake_state->close_count, 1);
    ASSERT_EQ(fake_state->close_inputs.size(), 1);
    EXPECT_TRUE(fake_state->close_inputs.front().ok());
}

TEST_F(IcebergTableSinkOperatorTest, NormalSinkConvertsOkCloseToCancellation) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    IcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    IcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);

    state.cancel(Status::Cancelled("cancel normal Iceberg sink"));
    Status close_status = local_state.close(&state, Status::OK());
    EXPECT_TRUE(close_status.is<ErrorCode::CANCELLED>()) << close_status.to_string();
    ASSERT_EQ(fake_state->close_inputs.size(), 1);
    EXPECT_TRUE(fake_state->close_inputs.front().is<ErrorCode::CANCELLED>());
}

TEST_F(IcebergTableSinkOperatorTest, NormalSinkObservesCancellationDuringWriterClose) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    IcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    IcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);
    fake_state->close_hook = [&state]() {
        state.cancel(Status::Cancelled("cancel during normal writer close"));
    };

    Status close_status = local_state.close(&state, Status::OK());
    EXPECT_TRUE(close_status.is<ErrorCode::CANCELLED>()) << close_status.to_string();
    ASSERT_EQ(fake_state->close_inputs.size(), 1);
    EXPECT_TRUE(fake_state->close_inputs.front().ok());
    EXPECT_EQ(fake_state->cleanup_count, 1);
    EXPECT_TRUE(fake_state->cleanup_status.is<ErrorCode::CANCELLED>());
}

TEST_F(IcebergTableSinkOperatorTest, SpillSinkFinalizesNonEmptyEos) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    SpillIcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    SpillIcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);

    Block data = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    ASSERT_TRUE(local_state.sink(&state, &data, true).ok());
    EXPECT_EQ(fake_state->write_count, 1);
    EXPECT_EQ(fake_state->close_count, 1);
    ASSERT_TRUE(local_state.close(&state, Status::OK()).ok());
    EXPECT_EQ(fake_state->close_count, 1);
    EXPECT_EQ(fake_state->cleanup_count, 1);
    EXPECT_TRUE(fake_state->cleanup_status.ok());
}

TEST_F(IcebergTableSinkOperatorTest, SpillSinkFinalizesEmptyEosAfterData) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    SpillIcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    SpillIcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);

    Block data = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    Block empty = ColumnHelper::create_block<DataTypeInt32>({});
    ASSERT_TRUE(local_state.sink(&state, &data, false).ok());
    EXPECT_EQ(fake_state->close_count, 0);
    ASSERT_TRUE(local_state.sink(&state, &empty, true).ok());
    EXPECT_EQ(fake_state->close_count, 1);

    ASSERT_TRUE(local_state.close(&state, Status::OK()).ok());
    EXPECT_EQ(fake_state->close_count, 1);
    EXPECT_EQ(fake_state->cleanup_count, 1);
}

TEST_F(IcebergTableSinkOperatorTest, SpillSinkReservesEosAdmissionAndLargestPartitionMerge) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    SpillIcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    SpillIcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);

    EXPECT_EQ(local_state.get_reserve_mem_size(&state, true),
              state.minimum_operator_memory_required_bytes());

    auto empty_writer = std::make_shared<VIcebergSortWriter>(nullptr, TSortInfo {}, 0);
    auto spilled_writer = std::make_shared<VIcebergSortWriter>(nullptr, TSortInfo {}, 0);
    auto* spilled_writer_ptr = spilled_writer.get();
    mark_spilled(spilled_writer.get());
    EXPECT_EQ(spilled_writer->get_reserve_mem_size(&state, true),
              static_cast<size_t>(state.spill_sort_merge_mem_limit_bytes()));
    set_partition_writers(&local_state, std::move(empty_writer), std::move(spilled_writer));
    EXPECT_EQ(local_state.get_reserve_mem_size(&state, true),
              static_cast<size_t>(state.spill_sort_merge_mem_limit_bytes()));

    mark_spilled(spilled_writer_ptr, 7);
    EXPECT_EQ(local_state.get_reserve_mem_size(&state, true), 72 * 1024 * 1024);

    TQueryOptions query_options = state.query_options();
    query_options.__set_spill_buffer_size_bytes(256 * 1024 * 1024);
    query_options.__set_spill_sort_merge_mem_limit_bytes(1024 * 1024);
    state.set_query_options(query_options);
    EXPECT_EQ(local_state.get_reserve_mem_size(&state, true), 768 * 1024 * 1024);
    ASSERT_TRUE(local_state.close(&state, Status::OK()).ok());
}

TEST_F(IcebergTableSinkOperatorTest, SortWriterSamplesSpillBatchBeforeConsumingInput) {
    MockRuntimeState state;
    TQueryOptions query_options = state.query_options();
    query_options.__set_spill_buffer_size_bytes(1024 * 1024);
    state.set_query_options(query_options);

    auto data_type = std::make_shared<DataTypeString>();
    VIcebergSortWriter writer(nullptr, TSortInfo {}, std::numeric_limits<int64_t>::max());
    initialize_sort_writer_for_write(&writer, &state, data_type);

    std::string wide_value(64 * 1024, 'x');
    Block block = ColumnHelper::create_block<DataTypeString>(
            {wide_value, wide_value, wide_value, wide_value});
    const size_t expected_avg_row_bytes = block.bytes() / block.rows();
    const size_t expected_batch_rows =
            (state.spill_buffer_size_bytes() + expected_avg_row_bytes - 1) / expected_avg_row_bytes;

    ASSERT_TRUE(writer.write(block).ok());
    EXPECT_EQ(block.rows(), 0);
    EXPECT_EQ(spill_batch_state(writer),
              std::make_pair(expected_avg_row_bytes, expected_batch_rows));
}

TEST_F(IcebergTableSinkOperatorTest, SpillSinkAccountsAndRevokesAllEosPartitions) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    SpillIcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    SpillIcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);

    auto large_state = std::make_shared<FakeSortWriterState>();
    large_state->data_size = 1024 * 1024;
    auto small_state = std::make_shared<FakeSortWriterState>();
    small_state->data_size = 128 * 1024;
    set_partition_writers(&local_state,
                          std::make_shared<FakeRevocableIcebergSortWriter>(large_state),
                          std::make_shared<FakeRevocableIcebergSortWriter>(small_state));
    static_cast<void>(local_state.get_reserve_mem_size(&state, true));

    EXPECT_EQ(local_state.get_revocable_mem_size(&state), large_state->data_size);
    ASSERT_TRUE(local_state.revoke_memory(&state).ok());
    EXPECT_EQ(large_state->spill_count, 1);
    EXPECT_EQ(small_state->spill_count, 0);
    EXPECT_EQ(local_state.get_revocable_mem_size(&state), 0);
    ASSERT_TRUE(local_state.close(&state, Status::OK()).ok());
}

TEST_F(IcebergTableSinkOperatorTest, SpillSinkWriteErrorClosesWithError) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    SpillIcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    SpillIcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);
    fake_state->write_result = Status::InternalError("injected write failure");

    Block data = ColumnHelper::create_block<DataTypeInt32>({1});
    Status sink_status = local_state.sink(&state, &data, true);
    EXPECT_TRUE(sink_status.is<ErrorCode::INTERNAL_ERROR>()) << sink_status.to_string();
    EXPECT_EQ(fake_state->close_count, 0);

    Status close_status = local_state.close(&state, sink_status);
    EXPECT_TRUE(close_status.is<ErrorCode::INTERNAL_ERROR>()) << close_status.to_string();
    EXPECT_EQ(fake_state->close_count, 1);
    ASSERT_EQ(fake_state->close_inputs.size(), 1);
    EXPECT_TRUE(fake_state->close_inputs.front().is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_EQ(fake_state->cleanup_count, 1);
    EXPECT_TRUE(fake_state->cleanup_status.is<ErrorCode::INTERNAL_ERROR>());
}

TEST_F(IcebergTableSinkOperatorTest, SpillSinkCloseErrorPropagatesAndCleansUp) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    SpillIcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    SpillIcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);
    fake_state->close_result = Status::InternalError("injected close failure");

    Block empty = ColumnHelper::create_block<DataTypeInt32>({});
    Status sink_status = local_state.sink(&state, &empty, true);
    EXPECT_TRUE(sink_status.is<ErrorCode::INTERNAL_ERROR>()) << sink_status.to_string();
    EXPECT_EQ(fake_state->close_count, 1);

    Status close_status = local_state.close(&state, sink_status);
    EXPECT_TRUE(close_status.is<ErrorCode::INTERNAL_ERROR>()) << close_status.to_string();
    EXPECT_EQ(fake_state->close_count, 1);
    EXPECT_EQ(fake_state->cleanup_count, 1);
    EXPECT_TRUE(fake_state->cleanup_status.is<ErrorCode::INTERNAL_ERROR>());
}

TEST_F(IcebergTableSinkOperatorTest, SpillSinkCancellationAfterEosDeletesDeferredFiles) {
    MockRuntimeState state;
    std::vector<TExpr> thrift_exprs;
    SpillIcebergTableSinkOperatorX parent(&_pool, 1, _row_desc, thrift_exprs);
    SpillIcebergTableSinkLocalState local_state(&parent, &state);
    std::shared_ptr<FakeWriterState> fake_state;
    initialize_local_state(&parent, &local_state, &state, &fake_state);

    Block empty = ColumnHelper::create_block<DataTypeInt32>({});
    ASSERT_TRUE(local_state.sink(&state, &empty, true).ok());
    EXPECT_EQ(fake_state->close_count, 1);

    state.cancel(Status::Cancelled("cancel spill Iceberg sink after EOS"));
    Status close_status = local_state.close(&state, Status::OK());
    EXPECT_TRUE(close_status.is<ErrorCode::CANCELLED>()) << close_status.to_string();
    EXPECT_EQ(fake_state->close_count, 1);
    EXPECT_EQ(fake_state->cleanup_count, 1);
    EXPECT_TRUE(fake_state->cleanup_status.is<ErrorCode::CANCELLED>());
}

} // namespace doris
