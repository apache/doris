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

#include <string>
#include <thread>

#include "common/config.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable.h"
#include "runtime/workload_management/resource_context.h"
#include "testutil/creators.h"

namespace doris {

class MemTableMemoryTrackingTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() override {
        _old_inaccurate_detect = config::crash_in_memory_tracker_inaccurate;
        _old_stack_trace = config::enable_address_sanitizers_with_stack_trace;
        config::crash_in_memory_tracker_inaccurate = true;
        config::enable_address_sanitizers_with_stack_trace = true;
        _thread_context = std::make_unique<ScopedInitThreadContext>();
        auto resource_ctx = ResourceContext::create_shared();
        resource_ctx->memory_context()->set_mem_tracker(MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, "MemTableMemoryTrackingTest"));

        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(UNIQUE_KEYS);
        testutil::add_column_pb(&schema_pb, 0, "k1", "INT", true, false);
        testutil::add_column_pb(&schema_pb, 1, "k2", "INT", true, false);
        testutil::add_column_pb(&schema_pb, 2, "v", "STRING", false, false)
                ->set_aggregation("REPLACE");
        schema_pb.add_cluster_key_uids(1);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);

        auto tdesc = testutil::create_descriptor_table(
                {{.type = TYPE_INT, .column_name = "k1", .nullable = false},
                 {.type = TYPE_INT, .column_name = "k2", .nullable = false},
                 {.type = TYPE_STRING, .column_name = "v", .nullable = false}});
        DescriptorTbl* desc_tbl = nullptr;
        ASSERT_TRUE(DescriptorTbl::create(&_pool, tdesc, &desc_tbl).ok());
        auto* tuple_desc = desc_tbl->get_tuple_descriptor(0);
        _memtable = std::make_unique<MemTable>(1, schema, &tuple_desc->slots(), tuple_desc, true,
                                               nullptr, resource_ctx, GetParam());
        for (const auto* slot : tuple_desc->slots()) {
            _input.insert(ColumnWithTypeAndName(slot->get_empty_mutable_column(), slot->type(),
                                                slot->col_name()));
        }
        auto columns = _input.mutate_columns_scoped();
        for (uint32_t i = 0; i < NUM_ROWS; ++i) {
            int32_t k1 = i;
            int32_t k2 = NUM_ROWS - i;
            columns.mutable_columns()[0]->insert_data(reinterpret_cast<const char*>(&k1), 0);
            columns.mutable_columns()[1]->insert_data(reinterpret_cast<const char*>(&k2), 0);
            auto value = std::string(64, 'v') + std::to_string(i);
            columns.mutable_columns()[2]->insert_data(value.data(), value.size());
            _rows.row_idxs.push_back(i);
            if (GetParam()) {
                _rows.allocated_lsns.push_back(1000 + i);
            }
        }
    }

    void TearDown() override {
        auto tracker = _memtable->mem_tracker();
        auto write_tracker =
                _memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker();
        // Memtables can be destroyed by a flush worker instead of the inserting thread.
        std::thread destroyer([memtable = std::move(_memtable)]() mutable { memtable.reset(); });
        destroyer.join();
        EXPECT_EQ(tracker->consumption(), 0);
        EXPECT_TRUE(write_tracker->_address_sanitizers.empty());
        EXPECT_TRUE(write_tracker->_error_address_sanitizers.empty());
        config::crash_in_memory_tracker_inaccurate = _old_inaccurate_detect;
        config::enable_address_sanitizers_with_stack_trace = _old_stack_trace;
    }

    void check_sorted_output(const IColumn& primary_key, const IColumn& cluster_key) {
        for (uint32_t i = 0; i < NUM_ROWS; ++i) {
            EXPECT_EQ(primary_key.get_int(i), NUM_ROWS - 1 - i);
            EXPECT_EQ(cluster_key.get_int(i), i + 1);
            if (GetParam()) {
                EXPECT_EQ((*_memtable->_output_allocated_lsns)[i], 1000 + NUM_ROWS - 1 - i);
            }
        }
    }

    static constexpr uint32_t NUM_ROWS = 1024;
    bool _old_inaccurate_detect = false;
    bool _old_stack_trace = false;
    std::unique_ptr<ScopedInitThreadContext> _thread_context;
    ObjectPool _pool;
    Block _input;
    TabletAddRowsPayload _rows;
    std::unique_ptr<MemTable> _memtable;
};

TEST_P(MemTableMemoryTrackingTest, BatchedAllocationDiagnostics) {
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
    auto write_tracker =
            _memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker();
    size_t stack_trace_bytes = 0;
    for (const auto& [address, allocation] : write_tracker->_address_sanitizers) {
        stack_trace_bytes += allocation.stack_trace.capacity();
    }
    RecordProperty("allocation_records", write_tracker->_address_sanitizers.size());
    RecordProperty("stack_trace_bytes", stack_trace_bytes);
    // cloud_p0 records an address and a stack trace for every Doris allocation.
    // Row storage must use a bounded number of allocations for a batch of 1024 rows.
    EXPECT_LT(write_tracker->_address_sanitizers.size(), 32);
}

TEST_P(MemTableMemoryTrackingTest, InsertAndReleaseRows) {
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
    const auto num_rows = _memtable->_row_in_blocks->size();
    ASSERT_EQ(num_rows, 2 * NUM_ROWS);
    const auto column_and_reference_bytes =
            _memtable->_input_mutable_block.allocated_bytes() +
            _memtable->_row_in_blocks->capacity() * sizeof(std::shared_ptr<RowInBlock>);
    // Do not assume a standard library's control block layout or allocator size class.
    EXPECT_GT(_memtable->memory_usage(),
              column_and_reference_bytes + num_rows * sizeof(RowInBlock));

    const auto old_adaptive = config::enable_adaptive_write_buffer_size;
    const auto old_buffer_size = config::write_buffer_size;
    Defer restore_config {[&] {
        config::enable_adaptive_write_buffer_size = old_adaptive;
        config::write_buffer_size = old_buffer_size;
    }};
    config::enable_adaptive_write_buffer_size = false;
    config::write_buffer_size = column_and_reference_bytes + num_rows * sizeof(RowInBlock);
    EXPECT_TRUE(_memtable->need_flush());

    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_memtable->mem_tracker());
    auto retained_row = _memtable->_row_in_blocks->front();
    std::weak_ptr<RowInBlock> weak_row = retained_row;
    const auto before_clear = _memtable->memory_usage();
    _memtable->_row_in_blocks->clear();
    const auto after_clear = _memtable->memory_usage();
    // The retained row keeps the first batch alive; the second batch is released.
    EXPECT_GT(before_clear - after_clear, NUM_ROWS * sizeof(RowInBlock));
    EXPECT_FALSE(_memtable->need_flush());
    EXPECT_EQ(retained_row->_row_pos, 0);
    EXPECT_EQ(retained_row->_allocated_lsn, GetParam() ? 1000 : 0);
    retained_row.reset();
    EXPECT_TRUE(weak_row.expired());
    // allocate_shared keeps the batch and control block allocation until the
    // final weak reference disappears.
    EXPECT_EQ(_memtable->memory_usage(), after_clear);
    weak_row.reset();
    EXPECT_GT(after_clear - _memtable->memory_usage(), NUM_ROWS * sizeof(RowInBlock));
}

TEST_P(MemTableMemoryTrackingTest, ClusterKeySortMemory) {
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_memtable->mem_tracker());
    auto input = _memtable->_input_mutable_block.to_block();
    ASSERT_TRUE(_memtable->_put_into_output(input).ok());
    const auto before_sort = _memtable->memory_usage();
    auto sort_tracker = std::make_shared<MemTracker>();
    {
        SCOPED_CONSUME_MEM_TRACKER(sort_tracker);
        ASSERT_TRUE(_memtable->_sort_by_cluster_keys().ok());
    }
    // All row objects coexist during sorting, in addition to the reference array.
    EXPECT_GT(sort_tracker->peak_consumption(),
              NUM_ROWS * (sizeof(RowInBlock) + sizeof(std::shared_ptr<RowInBlock>)));
    EXPECT_EQ(_memtable->memory_usage(), before_sort);
    EXPECT_EQ(sort_tracker->consumption(), 0);
    check_sorted_output(*_memtable->_output_mutable_block.get_column_by_position(0),
                        *_memtable->_output_mutable_block.get_column_by_position(1));
    // A schema mismatch returns after allocating the temporary rows. Their memory must
    // still be released, together with the block that could not be sorted.
    const auto output_bytes = _memtable->_output_mutable_block.allocated_bytes();
    _memtable->_tablet_schema->_cluster_key_uids = {999};
    auto status = _memtable->_sort_by_cluster_keys();
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(_memtable->memory_usage(), before_sort - output_bytes);
}

TEST_P(MemTableMemoryTrackingTest, AggregateAndFlush) {
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
    for (int round = 0; round < 4; ++round) {
        {
            auto columns = _input.mutate_columns_scoped();
            auto& value_column = columns.mutable_columns()[2];
            value_column->clear();
            for (uint32_t i = 0; i < NUM_ROWS; ++i) {
                auto value = std::string(64, 'a' + round) + std::to_string(i);
                value_column->insert_data(value.data(), value.size());
            }
        }
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                _memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
        SCOPED_CONSUME_MEM_TRACKER(_memtable->mem_tracker());
        std::weak_ptr<RowInBlock> previous_batch = _memtable->_row_in_blocks->front();
        ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
        std::weak_ptr<RowInBlock> inserted_batch = _memtable->_row_in_blocks->back();
        _memtable->shrink_memtable_by_agg();
        EXPECT_EQ(_memtable->_row_in_blocks->size(), NUM_ROWS);
        // Surviving rows must not pin batches containing merged-away rows.
        EXPECT_TRUE(previous_batch.expired());
        EXPECT_TRUE(inserted_batch.expired());
    }
    // Leave duplicate rows for the final aggregation during flush as well.
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());

    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_memtable->mem_tracker());
    std::unique_ptr<Block> output;
    ASSERT_TRUE(_memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), NUM_ROWS);
    check_sorted_output(*output->get_by_position(0).column, *output->get_by_position(1).column);
    for (uint32_t i = 0; i < NUM_ROWS; ++i) {
        EXPECT_EQ(output->get_by_position(2).column->get_data_at(i).to_string(),
                  std::string(64, 'd') + std::to_string(NUM_ROWS - 1 - i));
    }
    _memtable->_is_flush_success = true;
}

TEST_P(MemTableMemoryTrackingTest, SingleRowBatches) {
    _rows.row_idxs.resize(1);
    if (GetParam()) {
        _rows.allocated_lsns.resize(1);
    }
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
    _memtable->shrink_memtable_by_agg();
    ASSERT_EQ(_memtable->_row_in_blocks->size(), 1);

    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_memtable->mem_tracker());
    std::unique_ptr<Block> output;
    ASSERT_TRUE(_memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), 1);
    EXPECT_EQ(output->get_by_position(0).column->get_int(0), 0);
    EXPECT_EQ(output->get_by_position(1).column->get_int(0), NUM_ROWS);
    EXPECT_EQ(output->get_by_position(2).column->get_data_at(0).to_string(),
              std::string(64, 'v') + "0");
    if (GetParam()) {
        EXPECT_EQ((*_memtable->_output_allocated_lsns)[0], 1000);
    }
    _memtable->_is_flush_success = true;
}

INSTANTIATE_TEST_SUITE_P(WithAndWithoutLsn, MemTableMemoryTrackingTest, testing::Bool());

TEST(MemTableMemoryTrackingAuxTest, TieMemoryTracking) {
    SCOPED_INIT_THREAD_CONTEXT();
    auto tracker = std::make_shared<MemTracker>();
    SCOPED_CONSUME_MEM_TRACKER(tracker);
    {
        Tie empty(10, 10);
        EXPECT_EQ(tracker->consumption(), 0);
        Tie tie(10, 1034);
        EXPECT_GE(tracker->consumption(), 1024);
        EXPECT_EQ(tie[10], 1);
        EXPECT_EQ(tie[1033], 1);
    }
    EXPECT_EQ(tracker->consumption(), 0);
}

} // namespace doris
