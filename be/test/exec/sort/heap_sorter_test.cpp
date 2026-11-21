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

#include "exec/sort/heap_sorter.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <random>
#include <utility>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "exec/sort/sort_block.h"
#include "exec/sort/sorter.h"
#include "exec/sort/topn_sorter.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

struct HeapSorterTest : public testing::Test {
    void SetUp() override {
        row_desc.reset(new MockRowDescriptor({std::make_shared<DataTypeInt64>()}, &pool));

        ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    }
    MockRuntimeState _state;
    RuntimeProfile _profile {"test"};

    std::unique_ptr<HeapSorter> sorter;

    std::unique_ptr<MockRowDescriptor> row_desc;

    ObjectPool pool;

    VExprContextSPtrs ordering_expr_ctxs;

    std::vector<bool> is_asc_order {true, true};
    std::vector<bool> nulls_first {false, false};

    // Every row the sorter returns, in order, as (first column, second column).
    std::vector<std::pair<int64_t, int64_t>> drain(HeapSorter& heap) {
        EXPECT_TRUE(heap.prepare_for_read(false).ok());
        std::vector<std::pair<int64_t, int64_t>> rows;
        bool eos = false;
        while (!eos) {
            Block block;
            EXPECT_TRUE(heap.get_next(&_state, &block, &eos).ok());
            for (size_t i = 0; i < block.rows(); ++i) {
                rows.emplace_back(assert_cast<const ColumnInt64&>(*block.get_by_position(0).column)
                                          .get_element(i),
                                  assert_cast<const ColumnInt64&>(*block.get_by_position(1).column)
                                          .get_element(i));
            }
        }
        return rows;
    }

    // Sorts 5000 rows (key = payload / 7, payload distinct) by (key, payload), fed in blocks
    // smaller and larger than the heap, and compares the result with a full sort.
    void check_full_sort(bool asc, bool runtime_predicate, int64_t limit, int64_t offset) {
        SCOPED_TRACE(fmt::format("asc {} runtime predicate {} limit {} offset {}", asc,
                                 runtime_predicate, limit, offset));
        std::vector<bool> asc_order {asc, asc};
        RuntimeProfile profile {"test"};
        auto heap = HeapSorter::create_unique(ordering_expr_ctxs, &_state, limit, offset, &pool,
                                              asc_order, nulls_first, *row_desc, runtime_predicate);
        heap->init_profile(&profile);
        std::vector<int64_t> payloads(5000);
        std::iota(payloads.begin(), payloads.end(), 0);
        std::shuffle(payloads.begin(), payloads.end(), std::mt19937(20261109));
        std::vector<std::pair<int64_t, int64_t>> all;
        const std::vector<size_t> block_sizes {1, 7, 4096, 100, 700};
        for (size_t start = 0, i = 0; start < payloads.size(); ++i) {
            const size_t end =
                    std::min(payloads.size(), start + block_sizes[i % block_sizes.size()]);
            std::vector<int64_t> part(payloads.begin() + start, payloads.begin() + end);
            std::vector<int64_t> keys;
            for (const int64_t payload : part) {
                keys.push_back(payload / 7);
                all.emplace_back(payload / 7, payload);
            }
            Block block = ColumnHelper::create_block<DataTypeInt64>(keys, part);
            ASSERT_TRUE(heap->append_block(&block).ok());
            start = end;
        }
        std::ranges::sort(all);
        if (!asc) {
            std::ranges::reverse(all);
        }
        const std::vector<std::pair<int64_t, int64_t>> expected(
                all.begin() + offset, all.begin() + std::min<int64_t>(all.size(), offset + limit));
        EXPECT_EQ(expected, drain(*heap));
    }
};

TEST_F(HeapSorterTest, test_topn_sorter1) {
    DataTypes data_types {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    row_desc.reset(new MockRowDescriptor(data_types, &pool));

    ordering_expr_ctxs = MockSlotRef::create_mock_contexts(data_types);

    sorter = HeapSorter::create_unique(ordering_expr_ctxs, &_state, 6, 0, &pool, is_asc_order,
                                       nulls_first, *row_desc);

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
        MutableBlock merged_block = ColumnHelper::create_block<DataTypeInt64>({}, {});
        bool eos = false;
        while (!eos) {
            Block block;
            EXPECT_TRUE(sorter->get_next(&_state, &block, &eos));
            EXPECT_TRUE(merged_block.merge(block));
        }

        auto block = merged_block.to_block();
        EXPECT_EQ(block.rows(), 6);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                Block {ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6}),
                       ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5, 6})}));
    }
}

// Only the best limit + offset rows of a block can stay in the heap, so only those are sorted and
// kept: a block of 1000 rows under LIMIT 10 OFFSET 2 keeps 12.
TEST_F(HeapSorterTest, KeepsOnlyTheRowsOfABlockThatCanEnterTheHeap) {
    DataTypes data_types {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    row_desc = std::make_unique<MockRowDescriptor>(data_types, &pool);
    sorter = HeapSorter::create_unique(ordering_expr_ctxs, &_state, 10, 2, &pool, is_asc_order,
                                       nulls_first, *row_desc);
    sorter->init_profile(&_profile);
    std::vector<int64_t> keys(1000);
    std::iota(keys.begin(), keys.end(), 0);
    std::shuffle(keys.begin(), keys.end(), std::mt19937(7));
    Block block = ColumnHelper::create_block<DataTypeInt64>(keys, keys);
    ASSERT_TRUE(sorter->append_block(&block).ok());

    EXPECT_EQ(12, sorter->_queue_row_num);
    auto [cursor, rows] = sorter->_queue.current();
    EXPECT_EQ(12, cursor->impl->block->rows());
    // The worst row kept is the top of the heap, the bound a runtime predicate publishes.
    EXPECT_EQ(Field::create_field<TYPE_BIGINT>(11), sorter->get_top_value());
    std::vector<std::pair<int64_t, int64_t>> expected;
    for (int64_t key = 2; key < 12; ++key) {
        expected.emplace_back(key, key);
    }
    EXPECT_EQ(expected, drain(*sorter));
}

// Rows that tie with the last row kept are kept as a full sort of the block orders them, as before:
// the heap's block holds the last limit + offset rows of the block sorted worst first.
TEST_F(HeapSorterTest, KeepsTheRowsAFullSortKeepsAmongTies) {
    DataTypes data_types {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    row_desc = std::make_unique<MockRowDescriptor>(data_types, &pool);
    sorter = HeapSorter::create_unique(ordering_expr_ctxs, &_state, 10, 0, &pool, is_asc_order,
                                       nulls_first, *row_desc);
    sorter->init_profile(&_profile);
    // 1000 rows over 5 keys: the 10 rows kept tie on the smallest key with 190 others.
    std::vector<int64_t> payloads(1000);
    std::iota(payloads.begin(), payloads.end(), 0);
    std::shuffle(payloads.begin(), payloads.end(), std::mt19937(11));
    std::vector<int64_t> keys(payloads.size());
    std::ranges::transform(payloads, keys.begin(), [](int64_t payload) { return payload % 5; });

    Block full = ColumnHelper::create_block<DataTypeInt64>(keys, payloads);
    Block sorted = full.clone_empty();
    HybridSorter hybrid_sorter(_state.enable_use_hybrid_sort());
    sort_block(full, sorted, {SortColumnDescription(0, -1, 1)}, hybrid_sorter, 0);
    const auto& all = assert_cast<const ColumnInt64&>(*sorted.get_by_position(1).column);

    Block block = ColumnHelper::create_block<DataTypeInt64>(keys, payloads);
    ASSERT_TRUE(sorter->append_block(&block).ok());
    auto [cursor, rows] = sorter->_queue.current();
    const auto& kept =
            assert_cast<const ColumnInt64&>(*cursor->impl->block->get_by_position(1).column);
    ASSERT_EQ(10, kept.size());
    for (size_t i = 0; i < kept.size(); ++i) {
        EXPECT_EQ(all.get_element(all.size() - kept.size() + i), kept.get_element(i)) << i;
    }
}

TEST_F(HeapSorterTest, ReturnsTheRowsOfAFullSort) {
    DataTypes data_types {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    row_desc = std::make_unique<MockRowDescriptor>(data_types, &pool);
    ordering_expr_ctxs = MockSlotRef::create_mock_contexts(data_types);
    for (const bool asc : {true, false}) {
        for (const bool runtime_predicate : {true, false}) {
            for (const auto& [limit, offset] :
                 std::vector<std::pair<int64_t, int64_t>> {{1, 0}, {10, 0}, {10, 5}, {300, 0}}) {
                ASSERT_NO_FATAL_FAILURE(check_full_sort(asc, runtime_predicate, limit, offset));
            }
        }
    }
}

} // namespace doris
