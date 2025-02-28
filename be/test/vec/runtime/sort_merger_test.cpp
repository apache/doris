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

#include "testutil/column_helper.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/runtime/vsorted_run_merger.h"

namespace doris::vectorized {

class SortMergerTest : public testing::Test {
public:
    SortMergerTest() = default;
    ~SortMergerTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST(SortMergerTest, NULL_FIRST_ASC) {
    /**
     * in: [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     offset = 0, limit = -1, NULL_FIRST, ASC
     * out: [NULL, NULL, NULL, NULL, NULL], [1, 1, 1, 1, 1], [2, 2, 2, 2, 2], [3, 3, 3, 3, 3], [4], [4], [4], [4], [4]
     */
    const int num_children = 5;
    const int batch_size = 5;
    std::vector<int> round;
    round.resize(num_children, 0);
    const int num_round = 2;

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {true};
        std::vector<bool> nulls_first = {true};
        const int limit = -1;
        const int offset = 0;
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size,
                                          limit, offset, profile.get()));
    }
    {
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            vectorized::BlockSupplier block_supplier =
                    [&, round_vec = &round, num_round = num_round, id = child_idx](
                            vectorized::Block* block, bool* eos) {
                        *eos = ++((*round_vec)[id]) == num_round;
                        if (*eos) {
                            return Status::OK();
                        }
                        *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                                {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1,
                                 (*round_vec)[id] + 2, (*round_vec)[id] + 3},
                                {1, 0, 0, 0, 0});

                        return Status::OK();
                    };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        for (int block_idx = 0; block_idx < num_children * (num_round - 1) - 1; block_idx++) {
            vectorized::Block block;
            bool eos = false;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            auto expect_block =
                    block_idx == 0
                            ? ColumnHelper::create_nullable_column<DataTypeInt64>({0, 0, 0, 0, 0},
                                                                                  {1, 1, 1, 1, 1})
                            : ColumnHelper::create_nullable_column<DataTypeInt64>(
                                      {block_idx, block_idx, block_idx, block_idx, block_idx},
                                      {0, 0, 0, 0, 0});
            EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block))
                    << block_idx;
            EXPECT_EQ(block.rows(), batch_size);
            EXPECT_FALSE(eos);
        }
        for (int block_idx = 0; block_idx < num_children; block_idx++) {
            vectorized::Block block;
            bool eos = false;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            auto expect_block = ColumnHelper::create_nullable_column<DataTypeInt64>({4}, {0});
            EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block))
                    << ((ColumnInt64*)((ColumnNullable*)block.get_by_position(0).column.get())
                                ->get_nested_column_ptr()
                                .get())
                               ->get_data()[0];
            EXPECT_EQ(block.rows(), 1);
            EXPECT_FALSE(eos);
        }
        vectorized::Block block;
        bool eos = false;
        EXPECT_TRUE(merger->get_next(&block, &eos).ok());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_TRUE(eos);
    }
}

TEST(SortMergerTest, NULL_LAST_DESC) {
    /**
     * in: [([4, 3, 2, 1, NULL], eos = false), ([], eos = true)]
     *     [([4, 3, 2, 1, NULL], eos = false), ([], eos = true)]
     *     [([4, 3, 2, 1, NULL], eos = false), ([], eos = true)]
     *     [([4, 3, 2, 1, NULL], eos = false), ([], eos = true)]
     *     [([4, 3, 2, 1, NULL], eos = false), ([], eos = true)]
     *     offset = 0, limit = -1, NULL_LAST, DESC
     * out: [4, 4, 4, 4, 4], [3, 3, 3, 3, 3], [2, 2, 2, 2, 2], [1, 1, 1, 1, 1], [NULL], [NULL], [NULL], [NULL], [NULL]
     */
    const int num_children = 5;
    const int batch_size = 5;
    std::vector<int> round;
    round.resize(num_children, 0);
    const int num_round = 2;

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {false};
        std::vector<bool> nulls_first = {false};
        const int limit = -1;
        const int offset = 0;
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size,
                                          limit, offset, profile.get()));
    }
    {
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            vectorized::BlockSupplier block_supplier =
                    [&, round_vec = &round, num_round = num_round, id = child_idx](
                            vectorized::Block* block, bool* eos) {
                        *eos = ++((*round_vec)[id]) == num_round;
                        if (*eos) {
                            return Status::OK();
                        }
                        *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                                {(*round_vec)[id] + 3, (*round_vec)[id] + 2, (*round_vec)[id] + 1,
                                 (*round_vec)[id] + 0, 0},
                                {0, 0, 0, 0, 1});

                        return Status::OK();
                    };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        for (int block_idx = 0; block_idx < num_children * (num_round - 1) - 1; block_idx++) {
            vectorized::Block block;
            bool eos = false;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            auto expect_block = ColumnHelper::create_nullable_column<DataTypeInt64>(
                    {4 - block_idx, 4 - block_idx, 4 - block_idx, 4 - block_idx, 4 - block_idx},
                    {0, 0, 0, 0, 0});
            EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block))
                    << block_idx;
            EXPECT_EQ(block.rows(), batch_size);
            EXPECT_FALSE(eos);
        }
        for (int block_idx = 0; block_idx < num_children; block_idx++) {
            vectorized::Block block;
            bool eos = false;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            auto expect_block = ColumnHelper::create_nullable_column<DataTypeInt64>({0}, {1});
            EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block))
                    << ((ColumnInt64*)((ColumnNullable*)block.get_by_position(0).column.get())
                                ->get_nested_column_ptr()
                                .get())
                               ->get_data()[0];
            EXPECT_EQ(block.rows(), 1);
            EXPECT_FALSE(eos);
        }
        vectorized::Block block;
        bool eos = false;
        EXPECT_TRUE(merger->get_next(&block, &eos).ok());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_TRUE(eos);
    }
}

TEST(SortMergerTest, TEST_LIMIT) {
    /**
     * in: [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     [([NULL, 1, 2, 3, 4], eos = false), ([], eos = true)]
     *     offset = 20, limit = 1, NULL_FIRST, ASC
     * out: [4]
     */
    const int num_children = 5;
    const int batch_size = 5;
    std::vector<int> round;
    round.resize(num_children, 0);
    const int num_round = 2;

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {true};
        std::vector<bool> nulls_first = {true};
        const int limit = 1;
        const int offset = 20;
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size,
                                          limit, offset, profile.get()));
    }
    {
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            vectorized::BlockSupplier block_supplier =
                    [&, round_vec = &round, num_round = num_round, id = child_idx](
                            vectorized::Block* block, bool* eos) {
                        *eos = ++((*round_vec)[id]) == num_round;
                        if (*eos) {
                            return Status::OK();
                        }
                        *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                                {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1,
                                 (*round_vec)[id] + 2, (*round_vec)[id] + 3},
                                {1, 0, 0, 0, 0});

                        return Status::OK();
                    };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        vectorized::Block block;
        bool eos = false;
        EXPECT_TRUE(merger->get_next(&block, &eos).ok());
        auto expect_block = ColumnHelper::create_nullable_column<DataTypeInt64>({4}, {0});
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block));
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(eos);
    }
}

TEST(SortMergerTest, LAST_BLOCK_WITH_EOS) {
    /**
     * in: [([NULL, 0, 1, 2, 3], eos = true)]
     *     [([NULL, 0, 1, 2, 3], eos = true)]
     *     [([NULL, 0, 1, 2, 3], eos = true)]
     *     [([NULL, 0, 1, 2, 3], eos = true)]
     *     [([NULL, 0, 1, 2, 3], eos = true)]
     *     offset = 0, limit = -1, NULL_FIRST, ASC
     * out: [NULL, NULL, NULL, NULL, NULL], [0, 0, 0, 0, 0], [1, 1, 1, 1, 1], [2, 2, 2, 2, 2], [3, 3, 3, 3, 3]
     */
    const int num_children = 5;
    const int batch_size = 5;
    std::vector<int> round;
    round.resize(num_children, 0);
    const int num_round = 1;

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {true};
        std::vector<bool> nulls_first = {true};
        const int limit = -1;
        const int offset = 0;
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size,
                                          limit, offset, profile.get()));
    }
    {
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            vectorized::BlockSupplier block_supplier =
                    [&, round_vec = &round, num_round = num_round, id = child_idx](
                            vectorized::Block* block, bool* eos) {
                        *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                                {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1,
                                 (*round_vec)[id] + 2, (*round_vec)[id] + 3},
                                {1, 0, 0, 0, 0});
                        *eos = ++((*round_vec)[id]) == num_round;
                        return Status::OK();
                    };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        for (int block_idx = 0; block_idx < num_children * num_round; block_idx++) {
            vectorized::Block block;
            bool eos = false;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            auto expect_block = block_idx == 0
                                        ? ColumnHelper::create_nullable_column<DataTypeInt64>(
                                                  {0, 0, 0, 0, 0}, {1, 1, 1, 1, 1})
                                        : ColumnHelper::create_nullable_column<DataTypeInt64>(
                                                  {block_idx - 1, block_idx - 1, block_idx - 1,
                                                   block_idx - 1, block_idx - 1},
                                                  {0, 0, 0, 0, 0});
            EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block));
            EXPECT_EQ(block.rows(), batch_size);
            EXPECT_FALSE(eos);
        }
        vectorized::Block block;
        bool eos = false;
        EXPECT_TRUE(merger->get_next(&block, &eos).ok());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_TRUE(eos);
    }
}

TEST(SortMergerTest, TEST_BIG_OFFSET_SINGLE_STREAM) {
    /**
     * in: [([NULL, 0, 1, 2, 3], eos = true)]
     *     offset = 20, limit = 1, NULL_FIRST, ASC
     * out: []
     */
    const int num_children = 1;
    const int batch_size = 5;
    std::vector<int> round;
    round.resize(num_children, 0);
    const int num_round = 1;

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {true};
        std::vector<bool> nulls_first = {true};
        const int limit = 1;
        const int offset = 20;
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size,
                                          limit, offset, profile.get()));
    }
    {
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            vectorized::BlockSupplier block_supplier =
                    [&, round_vec = &round, num_round = num_round, id = child_idx](
                            vectorized::Block* block, bool* eos) {
                        *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                                {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1,
                                 (*round_vec)[id] + 2, (*round_vec)[id] + 3},
                                {1, 0, 0, 0, 0});
                        *eos = ++((*round_vec)[id]) == num_round;
                        return Status::OK();
                    };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        vectorized::Block block;
        bool eos = false;
        EXPECT_TRUE(merger->get_next(&block, &eos).ok());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_TRUE(eos);
    }
}

TEST(SortMergerTest, TEST_SMALL_OFFSET_SINGLE_STREAM) {
    /**
     * in: [([NULL, 0, 1, 2, 3], eos = true)]
     *     offset = 4, limit = 1, NULL_FIRST, ASC
     * out: [3]
     */
    const int num_children = 1;
    const int batch_size = 5;
    std::vector<int> round;
    round.resize(num_children, 0);
    const int num_round = 1;

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {true};
        std::vector<bool> nulls_first = {true};
        const int limit = 1;
        const int offset = 4;
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size,
                                          limit, offset, profile.get()));
    }
    {
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            vectorized::BlockSupplier block_supplier =
                    [&, round_vec = &round, num_round = num_round, id = child_idx](
                            vectorized::Block* block, bool* eos) {
                        *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                                {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1,
                                 (*round_vec)[id] + 2, (*round_vec)[id] + 3},
                                {1, 0, 0, 0, 0});
                        *eos = ++((*round_vec)[id]) == num_round;
                        return Status::OK();
                    };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        vectorized::Block block;
        bool eos = false;
        EXPECT_TRUE(merger->get_next(&block, &eos).ok());
        auto expect_block = ColumnHelper::create_nullable_column<DataTypeInt64>({3}, {0});
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block));
        EXPECT_TRUE(eos);
    }
}

TEST(SortMergerTest, TEST_SINGLE_STREAM) {
    /**
     * in: [([NULL], eos = true)]
     *     offset = 0, limit = -1, NULL_FIRST, ASC
     * out: [NULL]
     */
    const int num_children = 1;
    const int batch_size = 5;
    std::vector<int> round;
    round.resize(num_children, 0);
    const int num_round = 1;

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {true};
        std::vector<bool> nulls_first = {true};
        const int limit = -1;
        const int offset = 0;
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size,
                                          limit, offset, profile.get()));
    }
    {
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            vectorized::BlockSupplier block_supplier =
                    [&, round_vec = &round, num_round = num_round, id = child_idx](
                            vectorized::Block* block, bool* eos) {
                        *block = ColumnHelper::create_nullable_block<DataTypeInt64>({0}, {1});
                        *eos = ++((*round_vec)[id]) == num_round;
                        return Status::OK();
                    };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
        EXPECT_EQ(merger->_priority_queue.size(), 1);
        EXPECT_EQ(merger->_priority_queue.top()->pos, 0);
        EXPECT_EQ(merger->_priority_queue.top()->rows, 1);
        EXPECT_EQ(merger->_priority_queue.top()->block_ptr()->rows(), 1);
    }
    {
        vectorized::Block block;
        bool eos = false;
        EXPECT_TRUE(merger->get_next(&block, &eos).ok());
        auto expect_block = ColumnHelper::create_nullable_column<DataTypeInt64>({0}, {1});
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block));
        EXPECT_TRUE(eos);
    }
}

} // namespace doris::vectorized