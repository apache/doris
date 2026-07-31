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
#include <random>
#include <utility>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "exec/sort/heap_sorter.h"
#include "exec/sort/sorter.h"
#include "exec/sort/topn_sorter.h"
#include "exprs/vexpr_fwd.h"
#include "format/orc/vorc_reader.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
namespace doris {
class SortTest : public testing::Test {
public:
    SortTest() = default;
    ~SortTest() override = default;
};

enum class TestSortType { FULL_SORT, TOPN_SORT, HEAP_SORT }; // enum class TestSortType

class SortTestParam {
public:
    SortTestParam(TestSortType sort_type, int64_t limit, int64_t offset)
            : sort_type(sort_type), limit(limit), offset(offset) {
        std::vector<DataTypePtr> data_types {std::make_shared<DataTypeInt32>()};
        row_desc = std::make_unique<MockRowDescriptor>(data_types, &pool);

        ordering_expr_ctxs.push_back(VExprContext::create_shared(std::make_shared<MockSlotRef>(0)));

        switch (sort_type) {
        case TestSortType::FULL_SORT:
            sorter = FullSorter::create_unique(ordering_expr_ctxs, limit, offset, &pool,
                                               is_asc_order, nulls_first, *row_desc, &_state,
                                               nullptr);
            break;
        case TestSortType::TOPN_SORT:
            sorter = TopNSorter::create_unique(ordering_expr_ctxs, limit, offset, &pool,
                                               is_asc_order, nulls_first, *row_desc, &_state,
                                               nullptr);
            break;
        case TestSortType::HEAP_SORT:
            sorter = HeapSorter::create_unique(ordering_expr_ctxs, &_state, limit, offset, &pool,
                                               is_asc_order, nulls_first, *row_desc);
            break;
        default:
            break;
        }

        sorter->init_profile(profile.get());
    }

    void append_block(ColumnInt32::Ptr column) {
        Block block = VectorizedUtils::create_empty_block(*row_desc);
        block.get_by_position(0).column = column->clone();
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    void prepare_for_read() { EXPECT_TRUE(sorter->prepare_for_read(false).ok()); }

    void check_sort_column(ColumnPtr column) {
        MutableBlock sorted_block(VectorizedUtils::create_columns_with_type_and_name(*row_desc));
        Block output_block;
        bool eos = false;
        MockRuntimeState state;
        while (!eos) {
            output_block.clear();
            EXPECT_TRUE(sorter->get_next(&state, &output_block, &eos).ok());

            std::cout << output_block.dump_data() << std::endl;
            EXPECT_TRUE(sorted_block.merge(std::move(output_block)).ok());
        }
        Block result_block = sorted_block.to_block();
        const auto* except_column = assert_cast<const ColumnInt32*>(column.get());
        const auto* result_column =
                assert_cast<const ColumnInt32*>(result_block.get_by_position(0).column.get());
        EXPECT_EQ(except_column->size(), result_column->size());
        for (int i = 0; i < except_column->size(); i++) {
            EXPECT_EQ(except_column->get_element(i), result_column->get_element(i));
        }
    }
    TestSortType sort_type;
    int64_t limit;
    int64_t offset;
    VExprContextSPtrs ordering_expr_ctxs;
    ObjectPool pool;
    std::unique_ptr<MockRowDescriptor> row_desc;
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("");

    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};

    MockRuntimeState _state;

    std::unique_ptr<Sorter> sorter;
}; // class SortTestParam

std::pair<ColumnInt32::Ptr, ColumnInt32::Ptr> get_unsort_and_sorted_column(int64_t rows,
                                                                           int64_t limit,
                                                                           int64_t offset) {
    std::vector<int32_t> unsort_data;

    for (int i = 0; i < rows; i++) {
        unsort_data.push_back(i);
    }

    std::vector<int32_t> sorted_data;
    for (int i = offset; i < limit + offset; i++) {
        sorted_data.push_back(i);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(unsort_data.begin(), unsort_data.end(), g);

    auto unsort_column = ColumnInt32::create();
    for (auto i : unsort_data) {
        unsort_column->insert_value(i);
    }
    auto sorted_column = ColumnInt32::create();
    for (auto i : sorted_data) {
        sorted_column->insert_value(i);
    }
    return {std::move(unsort_column), std::move(sorted_column)};
}

void test_sort(TestSortType sort_type, int64_t rows, int64_t limit, int64_t offset) {
    SortTestParam param(sort_type, limit, offset);
    auto [unsort_column, sorted_column] = get_unsort_and_sorted_column(rows, limit, offset);
    param.append_block(unsort_column);
    param.prepare_for_read();
    param.check_sort_column(sorted_column->clone());
}

TEST_F(SortTest, test_full_sort) {
    test_sort(TestSortType::FULL_SORT, 100, 10, 10);
    test_sort(TestSortType::FULL_SORT, 1000, 10, 100);
}

TEST_F(SortTest, test_topn_sort) {
    test_sort(TestSortType::TOPN_SORT, 100, 10, 10);
    test_sort(TestSortType::TOPN_SORT, 1000, 10, 100);
}

TEST_F(SortTest, test_heap_sort) {
    test_sort(TestSortType::HEAP_SORT, 100, 10, 10);
    test_sort(TestSortType::HEAP_SORT, 1000, 10, 100);
}

TEST_F(SortTest, test_sorter) {
    VExprContextSPtrs ordering_expr_ctxs;
    ObjectPool pool;
    std::unique_ptr<MockRowDescriptor> row_desc;
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("");

    std::vector<bool> is_asc_order {true, true};
    std::vector<bool> nulls_first {false, false};

    std::unique_ptr<Sorter> sorter;
    DataTypes data_types {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    row_desc.reset(new MockRowDescriptor(data_types, &pool));

    ordering_expr_ctxs = MockSlotRef::create_mock_contexts(data_types);

    MockRuntimeState _state;
    sorter = FullSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order, nulls_first,
                                       *row_desc, &_state, nullptr);

    {
        Block src_block = ColumnHelper::create_block<DataTypeInt64>({4, 1, 2}, {10, 1, 3});
        Block dest_block = src_block.clone_empty();
        auto st = sorter->partial_sort(src_block, dest_block);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << dest_block.dump_data() << std::endl;
    }
}

} // namespace doris
