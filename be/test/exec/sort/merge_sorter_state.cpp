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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <random>
#include <utility>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant.h"
#include "exec/sort/heap_sorter.h"
#include "exec/sort/sorter.h"
#include "exec/sort/topn_sorter.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/defer_op.h"

namespace doris {

struct MergeSorterStateTest : public testing::Test {
    void SetUp() override {
        row_desc.reset(new MockRowDescriptor({std::make_shared<DataTypeInt64>()}, &pool));
    }
    MockRuntimeState _state;
    RuntimeProfile _profile {"test"};

    std::shared_ptr<MergeSorterState> state;

    std::unique_ptr<MockRowDescriptor> row_desc;

    ObjectPool pool;
};

std::shared_ptr<Block> create_block(std::vector<int64_t> data) {
    auto block = std::make_shared<Block>();
    *block = ColumnHelper::create_block<DataTypeInt64>(data);
    return block;
}

TEST_F(MergeSorterStateTest, test1) {
    state.reset(new MergeSorterState(*row_desc, 0));
    state->add_sorted_block(create_block({1, 2, 3}));
    state->add_sorted_block(create_block({4, 5, 6}));
    state->add_sorted_block(create_block({}));
    EXPECT_EQ(state->num_rows(), 6);
    EXPECT_EQ(state->data_size(), 48);
    EXPECT_EQ(state->get_queue().size(), 0);

    SortDescription desc {SortColumnDescription {0, 1, -1}};
    EXPECT_TRUE(state->build_merge_tree(desc));
    EXPECT_EQ(state->get_queue().size(), 2);

    {
        Block block;
        bool eos = false;
        Status status = state->merge_sort_read(&block, 2, &eos);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(ColumnHelper::block_equal(block,
                                              ColumnHelper::create_block<DataTypeInt64>({1, 2})));
    }

    {
        Block block;
        bool eos = false;
        Status status = state->merge_sort_read(&block, 2, &eos);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(ColumnHelper::block_equal(block,
                                              ColumnHelper::create_block<DataTypeInt64>({3, 4})));
    }

    {
        Block block;
        bool eos = false;
        Status status = state->merge_sort_read(&block, 2, &eos);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(ColumnHelper::block_equal(block,
                                              ColumnHelper::create_block<DataTypeInt64>({5, 6})));
    }
}

TEST_F(MergeSorterStateTest, whole_block_fast_path_swaps_block) {
    state.reset(new MergeSorterState(*row_desc, 0));
    auto first_block = create_block({1, 2, 3});
    auto second_block = create_block({4, 5, 6});
    auto first_column = first_block->get_by_position(0).column;

    state->add_sorted_block(first_block);
    state->add_sorted_block(second_block);

    SortDescription desc {SortColumnDescription {0, 1, -1}};
    ASSERT_TRUE(state->build_merge_tree(desc));

    Block block;
    bool eos = false;
    Status status = state->merge_sort_read(&block, 3, &eos);
    ASSERT_TRUE(status.ok());
    EXPECT_FALSE(eos);
    EXPECT_TRUE(
            ColumnHelper::block_equal(block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3})));
    EXPECT_EQ(block.get_by_position(0).column.get(), first_column.get());
}

TEST_F(MergeSorterStateTest, whole_block_fast_path_allows_smaller_than_batch) {
    state.reset(new MergeSorterState(*row_desc, 0));
    auto first_block = create_block({1, 2, 3});
    auto second_block = create_block({4, 5, 6});
    auto first_column = first_block->get_by_position(0).column;
    auto second_column = second_block->get_by_position(0).column;

    state->add_sorted_block(first_block);
    state->add_sorted_block(second_block);

    SortDescription desc {SortColumnDescription {0, 1, -1}};
    ASSERT_TRUE(state->build_merge_tree(desc));

    {
        Block block;
        bool eos = false;
        Status status = state->merge_sort_read(&block, 4, &eos);
        ASSERT_TRUE(status.ok());
        EXPECT_FALSE(eos);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3})));
        EXPECT_EQ(block.get_by_position(0).column.get(), first_column.get());
    }

    {
        Block block;
        bool eos = false;
        Status status = state->merge_sort_read(&block, 4, &eos);
        ASSERT_TRUE(status.ok());
        EXPECT_FALSE(eos);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({4, 5, 6})));
        EXPECT_EQ(block.get_by_position(0).column.get(), second_column.get());
    }

    {
        Block block;
        bool eos = false;
        Status status = state->merge_sort_read(&block, 4, &eos);
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 0);
    }
}

TEST_F(MergeSorterStateTest, test_reset_clears_all_state) {
    state.reset(new MergeSorterState(*row_desc, 0));

    // Add sorted blocks and build merge tree (simulates a sort-write cycle)
    state->add_sorted_block(create_block({1, 3, 5}));
    state->add_sorted_block(create_block({2, 4, 6}));
    EXPECT_EQ(state->num_rows(), 6);

    SortDescription desc {SortColumnDescription {0, 1, -1}};
    ASSERT_TRUE(state->build_merge_tree(desc));
    EXPECT_EQ(state->get_queue().size(), 2);

    // Drain the queue (simulates _write_sorted_data completing)
    Block block;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(state->merge_sort_read(&block, 10, &eos).ok());
        block.clear_column_data();
    }
    EXPECT_EQ(state->get_queue().size(), 0);

    // reset() must clear all state for the next batch
    state->reset();
    EXPECT_EQ(state->get_sorted_block().size(), 0); // _sorted_blocks cleared
    EXPECT_EQ(state->get_queue().size(), 0);        // _queue cleared
    EXPECT_EQ(state->num_rows(), 0);                // _num_rows reset
    EXPECT_EQ(state->data_size(), 0);               // no accumulated data

    // Verify the sorter is fully reusable after reset
    state->add_sorted_block(create_block({10, 20}));
    EXPECT_EQ(state->num_rows(), 2);
    ASSERT_TRUE(state->build_merge_tree(desc));
    EXPECT_EQ(state->get_queue().size(), 1);

    Block block2;
    bool eos2 = false;
    ASSERT_TRUE(state->merge_sort_read(&block2, 10, &eos2).ok());
    EXPECT_TRUE(
            ColumnHelper::block_equal(block2, ColumnHelper::create_block<DataTypeInt64>({10, 20})));
}

TEST_F(MergeSorterStateTest, test_reset_with_partial_drain) {
    state.reset(new MergeSorterState(*row_desc, 0));

    state->add_sorted_block(create_block({1, 2, 3}));
    state->add_sorted_block(create_block({4, 5, 6}));

    SortDescription desc {SortColumnDescription {0, 1, -1}};
    ASSERT_TRUE(state->build_merge_tree(desc));
    EXPECT_EQ(state->get_queue().size(), 2);

    // Read only part of the data — queue is NOT fully drained
    Block block;
    bool eos = false;
    ASSERT_TRUE(state->merge_sort_read(&block, 2, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_GT(state->get_queue().size(), 0);

    // reset() must cleanly discard the in-flight queue
    state->reset();
    EXPECT_EQ(state->get_queue().size(), 0);
    EXPECT_EQ(state->num_rows(), 0);

    // Sorter must be fully usable after mid-cycle reset
    state->add_sorted_block(create_block({7, 8}));
    ASSERT_TRUE(state->build_merge_tree(desc));
    Block block2;
    bool eos2 = false;
    ASSERT_TRUE(state->merge_sort_read(&block2, 10, &eos2).ok());
    EXPECT_TRUE(
            ColumnHelper::block_equal(block2, ColumnHelper::create_block<DataTypeInt64>({7, 8})));
}

TEST_F(MergeSorterStateTest, test_reset_on_fresh_state) {
    state.reset(new MergeSorterState(*row_desc, 0));

    // reset() on a state that has never had data must not crash
    state->reset();
    EXPECT_EQ(state->get_sorted_block().size(), 0);
    EXPECT_EQ(state->get_queue().size(), 0);
    EXPECT_EQ(state->num_rows(), 0);
    EXPECT_EQ(state->data_size(), 0);
}

TEST_F(MergeSorterStateTest, variant_v2_state_uses_matching_physical_column) {
    const bool previous = config::enable_variant_v2;
    config::enable_variant_v2 = true;
    Defer restore_config([previous] { config::enable_variant_v2 = previous; });

    DataTypePtr type = make_nullable(std::make_shared<DataTypeVariant>());
    MockRowDescriptor variant_row_desc({type}, &pool);
    state = std::make_shared<MergeSorterState>(variant_row_desc, 0);

    auto& destination = state->unsorted_block()->get_by_position(0).column;
    const auto* nullable = check_and_get_column<ColumnNullable>(destination.get());
    ASSERT_NE(nullable, nullptr);
    EXPECT_NE(check_and_get_column<ColumnVariantV2>(&nullable->get_nested_column()), nullptr);

    MutableColumnPtr source =
            ColumnNullable::create(ColumnVariantV2::create(), ColumnUInt8::create());
    source->insert_many_defaults(2);
    destination->assert_mutable()->insert_range_from(*source, 0, source->size());
    EXPECT_EQ(destination->size(), 2);
}
} // namespace doris
