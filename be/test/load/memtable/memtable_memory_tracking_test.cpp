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
        _thread_context = std::make_unique<ScopedInitThreadContext>();
        auto resource_ctx = ResourceContext::create_shared();
        resource_ctx->memory_context()->set_mem_tracker(MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, "MemTableMemoryTrackingTest"));

        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(UNIQUE_KEYS);
        testutil::add_column_pb(&schema_pb, 0, "k1", "INT", true, false);
        testutil::add_column_pb(&schema_pb, 1, "k2", "INT", true, false);
        schema_pb.add_cluster_key_uids(1);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);

        auto tdesc = testutil::create_descriptor_table(
                {{.type = TYPE_INT, .column_name = "k1", .nullable = false},
                 {.type = TYPE_INT, .column_name = "k2", .nullable = false}});
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
            _rows.row_idxs.push_back(i);
            if (GetParam()) {
                _rows.allocated_lsns.push_back(1000 + i);
            }
        }
    }

    void TearDown() override {
        auto tracker = _memtable->mem_tracker();
        // Memtables can be destroyed by a flush worker instead of the inserting thread.
        std::thread destroyer([memtable = std::move(_memtable)]() mutable { memtable.reset(); });
        destroyer.join();
        EXPECT_EQ(tracker->consumption(), 0);
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
    std::unique_ptr<ScopedInitThreadContext> _thread_context;
    ObjectPool _pool;
    Block _input;
    TabletAddRowsPayload _rows;
    std::unique_ptr<MemTable> _memtable;
};

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
    EXPECT_GT(before_clear - after_clear, (num_rows - 1) * sizeof(RowInBlock));
    EXPECT_FALSE(_memtable->need_flush());
    EXPECT_EQ(retained_row->_row_pos, 0);
    EXPECT_EQ(retained_row->_allocated_lsn, GetParam() ? 1000 : 0);
    retained_row.reset();
    EXPECT_TRUE(weak_row.expired());
    // allocate_shared keeps its allocation until the final weak reference disappears.
    EXPECT_EQ(_memtable->memory_usage(), after_clear);
    weak_row.reset();
    EXPECT_GT(after_clear - _memtable->memory_usage(), sizeof(RowInBlock));
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
    ASSERT_TRUE(_memtable->insert(&_input, _rows).ok());
    _memtable->shrink_memtable_by_agg();
    EXPECT_EQ(_memtable->_row_in_blocks->size(), NUM_ROWS);

    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_memtable->mem_tracker());
    std::unique_ptr<Block> output;
    ASSERT_TRUE(_memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), NUM_ROWS);
    check_sorted_output(*output->get_by_position(0).column, *output->get_by_position(1).column);
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
