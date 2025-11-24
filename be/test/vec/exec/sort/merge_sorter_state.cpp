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
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/common/assert_cast.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/block.h"

namespace doris::vectorized {

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
} // namespace doris::vectorized