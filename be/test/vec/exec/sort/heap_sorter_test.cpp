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

#include "vec/common/sort/heap_sorter.h"

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
#include "vec/common/sort/sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/block.h"

namespace doris::vectorized {

struct HeapSorterTest : public testing::Test {
    void SetUp() override {
        row_desc.reset(new MockRowDescriptor({std::make_shared<DataTypeInt64>()}, &pool));

        sort_exec_exprs._sort_tuple_slot_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        sort_exec_exprs._materialize_tuple = false;

        sort_exec_exprs._ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());

        sort_exec_exprs._sort_tuple_slot_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    }
    MockRuntimeState _state;
    RuntimeProfile _profile {"test"};

    std::unique_ptr<HeapSorter> sorter;

    std::unique_ptr<MockRowDescriptor> row_desc;

    ObjectPool pool;

    VSortExecExprs sort_exec_exprs;

    std::vector<bool> is_asc_order {true, true};
    std::vector<bool> nulls_first {false, false};
};

TEST_F(HeapSorterTest, test_topn_sorter1) {
    DataTypes data_types {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    row_desc.reset(new MockRowDescriptor(data_types, &pool));

    sort_exec_exprs._sort_tuple_slot_expr_ctxs = MockSlotRef::create_mock_contexts(data_types);

    sort_exec_exprs._materialize_tuple = true;

    sort_exec_exprs._ordering_expr_ctxs = MockSlotRef::create_mock_contexts(data_types);

    sort_exec_exprs._sort_tuple_slot_expr_ctxs = MockSlotRef::create_mock_contexts(data_types);

    sorter = HeapSorter::create_unique(sort_exec_exprs, 6, 0, &pool, is_asc_order, nulls_first,
                                       *row_desc);

    sorter->init_profile(&_profile);

    {
        Block block =
                ColumnHelper::create_block<DataTypeInt64>({7, 5, 4, 3, 2, 1}, {7, 5, 4, 3, 2, 1});
        auto st = sorter->append_block(&block);
        EXPECT_TRUE(st.ok());
    }

    EXPECT_EQ(sorter->_queue_row_num, 6);

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({6}, {6});
        auto st = sorter->append_block(&block);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(sorter->_queue_row_num, 6);

        auto value = sorter->get_top_value();
        Field real;
        block.get_by_position(0).column->get(0, real);
        EXPECT_EQ(value, real);
    }

    EXPECT_TRUE(sorter->prepare_for_read(false));

    {
        Block block;
        bool eos = false;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos));
        EXPECT_EQ(block.rows(), 6);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                Block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6}),
                       ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6})}));

        block.clear_column_data();

        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos));
        EXPECT_EQ(block.rows(), 0);
        EXPECT_EQ(eos, true);
    }
}

} // namespace doris::vectorized