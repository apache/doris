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

#include <cstddef>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/sort/vsorted_run_merger.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

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
    const size_t block_max_bytes =
            1 * 1024 * 1024; // 1MB, to avoid breaking early due to block size limit
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
                                          limit, offset, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
                *eos = ++((*round_vec)[id]) == num_round;
                if (*eos) {
                    return Status::OK();
                }
                *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                        {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1, (*round_vec)[id] + 2,
                         (*round_vec)[id] + 3},
                        {1, 0, 0, 0, 0});

                return Status::OK();
            };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        for (int block_idx = 0; block_idx < num_children * (num_round - 1) - 1; block_idx++) {
            Block block;
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
            Block block;
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
        Block block;
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
        const size_t block_max_bytes =
                1 * 1024 * 1024; // 1MB, to avoid breaking early due to block size limit
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size,
                                          limit, offset, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
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
            Block block;
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
            Block block;
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
        Block block;
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
    const size_t block_max_bytes =
            1 * 1024 * 1024; // 1MB, to avoid breaking early due to block size limit
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
                                          limit, offset, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
                *eos = ++((*round_vec)[id]) == num_round;
                if (*eos) {
                    return Status::OK();
                }
                *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                        {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1, (*round_vec)[id] + 2,
                         (*round_vec)[id] + 3},
                        {1, 0, 0, 0, 0});

                return Status::OK();
            };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        Block block;
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
    const size_t block_max_bytes =
            1 * 1024 * 1024; // 1MB, to avoid breaking early due to block size limit
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
                                          limit, offset, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
                *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                        {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1, (*round_vec)[id] + 2,
                         (*round_vec)[id] + 3},
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
            Block block;
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
        Block block;
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
    const size_t block_max_bytes = 1 * 024 * 1024; // 1MB
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
                                          limit, offset, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
                *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                        {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1, (*round_vec)[id] + 2,
                         (*round_vec)[id] + 3},
                        {1, 0, 0, 0, 0});
                *eos = ++((*round_vec)[id]) == num_round;
                return Status::OK();
            };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        Block block;
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
    const size_t block_max_bytes =
            1 * 1024 * 1024; // 1MB, to avoid breaking early due to block size limit
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
                                          limit, offset, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
                *block = ColumnHelper::create_nullable_block<DataTypeInt64>(
                        {0, (*round_vec)[id] + 0, (*round_vec)[id] + 1, (*round_vec)[id] + 2,
                         (*round_vec)[id] + 3},
                        {1, 0, 0, 0, 0});
                *eos = ++((*round_vec)[id]) == num_round;
                return Status::OK();
            };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        Block block;
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
    const size_t block_max_bytes =
            1 * 1024 * 1024; // 1MB, to avoid breaking early due to block size limit
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
                                          limit, offset, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
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
        Block block;
        bool eos = false;
        EXPECT_TRUE(merger->get_next(&block, &eos).ok());
        auto expect_block = ColumnHelper::create_nullable_column<DataTypeInt64>({0}, {1});
        EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(0).column, expect_block));
        EXPECT_TRUE(eos);
    }
}

TEST(SortMergerTest, BLOCK_MAX_BYTES_LIMITS_OUTPUT) {
    /**
     * Test that block_max_bytes causes the merger to produce smaller blocks.
     * Setup: 3 children, each providing 256 sorted Int64 rows. Total = 768 rows.
     * batch_size=4096, block_max_bytes=512.
     * Each nullable Int64 row ≈ 9 bytes.
     * The periodic check at 256 rows finds 256*9=2304 > 512, so it breaks.
     * Each output block should have at most 256 rows.
     */
    const int num_children = 3;
    const int batch_size = 4096;
    const size_t block_max_bytes = 512;
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
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size, -1,
                                          0, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
                std::vector<Int64> values;
                std::vector<UInt8> null_map;
                const int rows_per_block = 256;
                for (int i = 0; i < rows_per_block; i++) {
                    values.push_back(id * rows_per_block + i);
                    null_map.push_back(0);
                }
                *block = ColumnHelper::create_nullable_block<DataTypeInt64>(values, null_map);
                *eos = ++((*round_vec)[id]) == num_round;
                return Status::OK();
            };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        size_t total_rows = 0;
        int num_blocks = 0;
        bool eos = false;
        while (!eos) {
            Block block;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            if (block.rows() > 0) {
                EXPECT_LE(block.rows(), 256)
                        << "block_max_bytes should limit output block size, got " << block.rows()
                        << " rows with " << block.bytes() << " bytes";
                total_rows += block.rows();
                num_blocks++;
            }
        }
        EXPECT_EQ(total_rows, 768);
        EXPECT_GT(num_blocks, 1);
    }
}

TEST(SortMergerTest, BLOCK_MAX_BYTES_ZERO_DISABLES_CHECK) {
    /**
     * When block_max_bytes=0, blocks are limited only by batch_size.
     */
    const int num_children = 2;
    const int batch_size = 10;
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
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size, -1,
                                          0, profile.get(), /*block_max_bytes=*/0));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, round_vec = &round, num_round = num_round,
                                            id = child_idx](Block* block, bool* eos) {
                std::vector<Int64> values;
                std::vector<UInt8> null_map;
                for (int i = 0; i < 10; i++) {
                    values.push_back(id * 10 + i);
                    null_map.push_back(0);
                }
                *block = ColumnHelper::create_nullable_block<DataTypeInt64>(values, null_map);
                *eos = ++((*round_vec)[id]) == num_round;
                return Status::OK();
            };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        size_t total_rows = 0;
        bool eos = false;
        while (!eos) {
            Block block;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            if (block.rows() > 0) {
                // With block_max_bytes=0, rows per block should be exactly batch_size
                // (except possibly the last block).
                EXPECT_LE(block.rows(), batch_size);
                total_rows += block.rows();
            }
        }
        // 2 children * 10 rows each = 20 total
        EXPECT_EQ(total_rows, 20);
    }
}

TEST(SortMergerTest, BLOCK_MAX_BYTES_WITH_MANY_ROWS) {
    /**
     * Test block_max_bytes with 5 children each producing 1024 rows.
     * Total = 5120 rows. batch_size=8192. block_max_bytes=2048.
     * Verifies all rows are output and no block exceeds batch_size.
     */
    const int num_children = 5;
    const int batch_size = 8192;
    const size_t block_max_bytes = 2048;
    const int rows_per_child = 1024;
    std::vector<int> supply_idx;
    supply_idx.resize(num_children, 0);

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {true};
        std::vector<bool> nulls_first = {false};
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size, -1,
                                          0, profile.get(), block_max_bytes));
    }
    {
        std::vector<BlockSupplier> child_block_suppliers;
        for (int child_idx = 0; child_idx < num_children; child_idx++) {
            BlockSupplier block_supplier = [&, idx_ptr = &supply_idx, id = child_idx,
                                            rows_per_child = rows_per_child](Block* block,
                                                                             bool* eos) {
                const int block_rows = 256;
                int& idx = (*idx_ptr)[id];
                if (idx >= rows_per_child) {
                    *eos = true;
                    return Status::OK();
                }
                int remaining = std::min(block_rows, rows_per_child - idx);
                std::vector<Int64> values;
                std::vector<UInt8> null_map;
                for (int i = 0; i < remaining; i++) {
                    values.push_back(idx + i);
                    null_map.push_back(0);
                }
                *block = ColumnHelper::create_nullable_block<DataTypeInt64>(values, null_map);
                idx += remaining;
                *eos = (idx >= rows_per_child);
                return Status::OK();
            };
            child_block_suppliers.push_back(block_supplier);
        }
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
    }
    {
        size_t total_rows = 0;
        bool eos = false;
        while (!eos) {
            Block block;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            if (block.rows() > 0) {
                EXPECT_LE(block.rows(), static_cast<size_t>(batch_size));
                total_rows += block.rows();
            }
        }
        EXPECT_EQ(total_rows, num_children * rows_per_child);
    }
}

TEST(SortMergerTest, SINGLE_RUN_FAST_PATH_BYTE_BUDGET) {
    /**
     * Single-run fast path with byte budget: 1 child providing 256 rows in one block.
     * batch_size=4096, block_max_bytes=100.
     * Each nullable Int64 row ≈ 9 bytes (8 data + 1 null).
     * byte_limited ≈ 100/9 ≈ 11 rows per output block.
     * Verifies:
     *   - All rows are output (no data loss from cut on supplier block)
     *   - Multiple output blocks are produced (byte budget limits each block)
     *   - Data is sorted correctly
     */
    const int batch_size = 4096;
    const size_t block_max_bytes = 100;
    const int total_rows = 256;

    std::unique_ptr<VSortedRunMerger> merger;
    auto profile = std::make_shared<RuntimeProfile>("");
    auto ordering_expr = MockSlotRef::create_mock_contexts(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
    {
        std::vector<bool> is_asc_order = {true};
        std::vector<bool> nulls_first = {false};
        merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size, -1,
                                          0, profile.get(), block_max_bytes));
    }
    {
        int supply_count = 0;
        std::vector<BlockSupplier> child_block_suppliers;
        BlockSupplier block_supplier = [&supply_count](Block* block, bool* eos) {
            std::vector<Int64> values;
            std::vector<UInt8> null_map;
            for (int i = 0; i < 256; i++) {
                values.push_back(i);
                null_map.push_back(0);
            }
            *block = ColumnHelper::create_nullable_block<DataTypeInt64>(values, null_map);
            supply_count++;
            *eos = true;
            return Status::OK();
        };
        child_block_suppliers.push_back(block_supplier);
        EXPECT_TRUE(merger->prepare(child_block_suppliers).ok());
        EXPECT_EQ(merger->_priority_queue.size(), 1) << "single child = single-run fast path";
    }
    {
        size_t total_output_rows = 0;
        int num_blocks = 0;
        Int64 last_value = -1;
        bool eos = false;
        while (!eos) {
            Block block;
            EXPECT_TRUE(merger->get_next(&block, &eos).ok());
            if (block.rows() > 0) {
                num_blocks++;
                // Verify sorted order and continuity
                auto col = block.get_by_position(0).column;
                auto nullable_col =
                        assert_cast<const ColumnNullable*>(col.get())->get_nested_column_ptr();
                auto* data = assert_cast<const ColumnInt64*>(nullable_col.get())->get_data().data();
                for (size_t i = 0; i < block.rows(); i++) {
                    EXPECT_EQ(data[i], last_value + 1)
                            << "row " << (total_output_rows + i) << " mismatch";
                    last_value = data[i];
                }
                total_output_rows += block.rows();
            }
        }
        EXPECT_EQ(total_output_rows, total_rows) << "all rows must be output (no data loss)";
        EXPECT_GT(num_blocks, 1) << "byte budget should produce multiple blocks";
    }
}

} // namespace doris
