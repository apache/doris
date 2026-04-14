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

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "exec/sort/heap_sorter.h"
#include "exec/sort/sorter.h"
#include "exec/sort/topn_sorter.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

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
        Status status = state->merge_sort_read(&block, 2, /*block_max_bytes=*/0, &eos);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(ColumnHelper::block_equal(block,
                                              ColumnHelper::create_block<DataTypeInt64>({1, 2})));
    }

    {
        Block block;
        bool eos = false;
        Status status = state->merge_sort_read(&block, 2, /*block_max_bytes=*/0, &eos);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(ColumnHelper::block_equal(block,
                                              ColumnHelper::create_block<DataTypeInt64>({3, 4})));
    }

    {
        Block block;
        bool eos = false;
        Status status = state->merge_sort_read(&block, 2, /*block_max_bytes=*/0, &eos);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(ColumnHelper::block_equal(block,
                                              ColumnHelper::create_block<DataTypeInt64>({5, 6})));
    }
}

TEST_F(MergeSorterStateTest, BLOCK_MAX_BYTES_LIMITS_OUTPUT) {
    // With block_max_bytes set small, merge_sort_read should produce smaller blocks.
    // Use multiple sorted blocks so the merge loop iterates multiple times.
    // Each sorted block: 10 Int64 rows (80 bytes). 10 blocks = 100 rows total.
    // block_max_bytes=50 triggers the break after consuming the first cursor batch.
    state.reset(new MergeSorterState(*row_desc, 0));
    for (int b = 0; b < 10; b++) {
        std::vector<int64_t> values;
        for (int i = 0; i < 10; i++) {
            values.push_back(b * 10 + i);
        }
        state->add_sorted_block(create_block(values));
    }
    EXPECT_EQ(state->num_rows(), 100);

    SortDescription desc {SortColumnDescription {0, 1, -1}};
    EXPECT_TRUE(state->build_merge_tree(desc));

    size_t total_rows = 0;
    int num_blocks = 0;
    bool eos = false;
    while (!eos) {
        Block block;
        Status status = state->merge_sort_read(&block, 100, /*block_max_bytes=*/50, &eos);
        EXPECT_TRUE(status.ok());
        if (block.rows() > 0) {
            total_rows += block.rows();
            num_blocks++;
        }
    }
    EXPECT_EQ(total_rows, 100);
    EXPECT_GT(num_blocks, 1) << "Should produce multiple blocks due to bytes limit";
}

TEST_F(MergeSorterStateTest, BLOCK_MAX_BYTES_ZERO_DISABLES_CHECK) {
    // block_max_bytes=0 should not limit — all rows in one call (but eos follows).
    state.reset(new MergeSorterState(*row_desc, 0));
    state->add_sorted_block(create_block({1, 2, 3, 4, 5}));
    state->add_sorted_block(create_block({6, 7, 8, 9, 10}));

    SortDescription desc {SortColumnDescription {0, 1, -1}};
    EXPECT_TRUE(state->build_merge_tree(desc));

    Block block;
    bool eos = false;
    Status status = state->merge_sort_read(&block, 100, /*block_max_bytes=*/0, &eos);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(block.rows(), 10) << "block_max_bytes=0 should not limit rows";
    // eos is set to (merged_rows == 0), so after reading 10 rows, eos=false
    // Next call will return 0 rows with eos=true
    EXPECT_FALSE(eos);
    Block block2;
    status = state->merge_sort_read(&block2, 100, /*block_max_bytes=*/0, &eos);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(eos);
}
} // namespace doris
