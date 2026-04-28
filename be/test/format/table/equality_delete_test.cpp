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

#include "format/table/equality_delete.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "runtime/runtime_profile.h"

namespace doris {

// ============================================================================
// Helper: Build a single-column non-nullable Int32 Block.
// ============================================================================
static Block build_int32_block(const std::string& col_name, const std::vector<int32_t>& values) {
    auto col = ColumnInt32::create();
    for (auto v : values) {
        col->insert_value(v);
    }
    Block block;
    block.insert({std::move(col), std::make_shared<DataTypeInt32>(), col_name});
    return block;
}

// ============================================================================
// Helper: Build a single-column nullable Int32 Block.
// null_flags[i] == true means the i-th row is NULL.
// ============================================================================
static Block build_nullable_int32_block(const std::string& col_name,
                                        const std::vector<int32_t>& values,
                                        const std::vector<bool>& null_flags) {
    auto nested_col = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < values.size(); ++i) {
        nested_col->insert_value(values[i]);
        null_map->insert_value(null_flags[i] ? 1 : 0);
    }
    auto nullable_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));
    Block block;
    block.insert({std::move(nullable_col),
                  std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), col_name});
    return block;
}

// ============================================================================
// Helper: Build a single-column non-nullable String Block.
// ============================================================================
static Block build_string_block(const std::string& col_name,
                                const std::vector<std::string>& values) {
    auto col = ColumnString::create();
    for (const auto& v : values) {
        col->insert_data(v.data(), v.size());
    }
    Block block;
    block.insert({std::move(col), std::make_shared<DataTypeString>(), col_name});
    return block;
}

// ============================================================================
// Helper: Build a two-column Block (Int32 + String).
// ============================================================================
static Block build_int32_string_block(const std::string& int_name,
                                      const std::vector<int32_t>& int_vals,
                                      const std::string& str_name,
                                      const std::vector<std::string>& str_vals) {
    auto int_col = ColumnInt32::create();
    for (auto v : int_vals) {
        int_col->insert_value(v);
    }
    auto str_col = ColumnString::create();
    for (const auto& v : str_vals) {
        str_col->insert_data(v.data(), v.size());
    }
    Block block;
    block.insert({std::move(int_col), std::make_shared<DataTypeInt32>(), int_name});
    block.insert({std::move(str_col), std::make_shared<DataTypeString>(), str_name});
    return block;
}

// ============================================================================
// Helper: Initialize filter to all-pass (all 1s).
// ============================================================================
static IColumn::Filter make_filter(size_t rows) {
    return IColumn::Filter(rows, 1);
}

// ============================================================================
// Factory Tests
// ============================================================================
class EqualityDeleteTest : public ::testing::Test {
protected:
    RuntimeProfile profile {"equality_delete_test"};
};

TEST_F(EqualityDeleteTest, FactorySingleColumn) {
    auto delete_block = build_int32_block("id", {1, 2, 3});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    // Single column → SimpleEqualityDelete
    auto* simple = dynamic_cast<SimpleEqualityDelete*>(impl.get());
    ASSERT_NE(simple, nullptr);
}

TEST_F(EqualityDeleteTest, FactoryMultiColumn) {
    auto delete_block = build_int32_string_block("id", {1, 2}, "name", {"a", "b"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    // Multi column → MultiEqualityDelete
    auto* multi = dynamic_cast<MultiEqualityDelete*>(impl.get());
    ASSERT_NE(multi, nullptr);
}

// ============================================================================
// SimpleEqualityDelete — Non-Nullable Int32
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleInt32NonNullable) {
    // Delete rows where id ∈ {1, 3, 5}
    auto delete_block = build_int32_block("id", {1, 3, 5});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: id = [1, 2, 3, 4, 5]
    auto data_block = build_int32_block("id", {1, 2, 3, 4, 5});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(5);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    // Rows 0(id=1), 2(id=3), 4(id=5) should be deleted (filter=0)
    EXPECT_EQ(filter[0], 0); // id=1 deleted
    EXPECT_EQ(filter[1], 1); // id=2 kept
    EXPECT_EQ(filter[2], 0); // id=3 deleted
    EXPECT_EQ(filter[3], 1); // id=4 kept
    EXPECT_EQ(filter[4], 0); // id=5 deleted
}

TEST_F(EqualityDeleteTest, SimpleInt32NoMatch) {
    // Delete set: {10, 20, 30}
    auto delete_block = build_int32_block("id", {10, 20, 30});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: [1, 2, 3]
    auto data_block = build_int32_block("id", {1, 2, 3});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(3);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    // No matches — all kept
    EXPECT_EQ(filter[0], 1);
    EXPECT_EQ(filter[1], 1);
    EXPECT_EQ(filter[2], 1);
}

TEST_F(EqualityDeleteTest, SimpleInt32AllMatch) {
    // Delete set: {1, 2, 3}
    auto delete_block = build_int32_block("id", {1, 2, 3});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: [1, 2, 3]
    auto data_block = build_int32_block("id", {1, 2, 3});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(3);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0);
    EXPECT_EQ(filter[1], 0);
    EXPECT_EQ(filter[2], 0);
}

// ============================================================================
// SimpleEqualityDelete — Nullable Int32
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleNullableInt32) {
    // Use >8 delete rows to avoid FixedContainer size check
    // (FixedContainer<N> expects exactly N non-null elements)
    auto delete_block = build_nullable_int32_block(
            "id", {1, 0, 5, 10, 20, 30, 40, 50, 60},
            {false, true, false, false, false, false, false, false, false});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: [1, 2, NULL, 4, 5]
    auto data_block =
            build_nullable_int32_block("id", {1, 2, 0, 4, 5}, {false, false, true, false, false});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(5);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // id=1 deleted
    EXPECT_EQ(filter[1], 1); // id=2 kept
    // NULL data is NOT deleted: _build_set passes null_aware=false to create_set,
    // so contain_null() always returns false regardless of NULLs in delete set
    EXPECT_EQ(filter[2], 1); // NULL kept (null_aware=false)
    EXPECT_EQ(filter[3], 1); // id=4 kept
    EXPECT_EQ(filter[4], 0); // id=5 deleted
}

TEST_F(EqualityDeleteTest, SimpleNullableNoNullInDeleteSet) {
    // Use >8 delete rows to avoid FixedContainer size check
    auto delete_block = build_nullable_int32_block(
            "id", {1, 5, 10, 20, 30, 40, 50, 60, 70},
            {false, false, false, false, false, false, false, false, false});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: [1, NULL, 5]
    auto data_block = build_nullable_int32_block("id", {1, 0, 5}, {false, true, false});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(3);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // id=1 deleted
    EXPECT_EQ(filter[1], 1); // NULL kept (no NULL in delete set)
    EXPECT_EQ(filter[2], 0); // id=5 deleted
}

// ============================================================================
// SimpleEqualityDelete — String Column
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleStringNonNullable) {
    auto delete_block = build_string_block("name", {"alice", "charlie"});
    std::vector<int> col_ids = {200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_string_block("name", {"alice", "bob", "charlie", "dave"});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"name", 0}};
    std::unordered_map<int, std::string> id_to_name = {{200, "name"}};
    auto filter = make_filter(4);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // "alice" deleted
    EXPECT_EQ(filter[1], 1); // "bob" kept
    EXPECT_EQ(filter[2], 0); // "charlie" deleted
    EXPECT_EQ(filter[3], 1); // "dave" kept
}

// ============================================================================
// SimpleEqualityDelete — Repeated filter_data_block calls (filter reuse)
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleRepeatedFilterCalls) {
    auto delete_block = build_int32_block("id", {2, 4});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};

    // First batch: [1, 2, 3]
    auto batch1 = build_int32_block("id", {1, 2, 3});
    auto filter1 = make_filter(3);
    ASSERT_TRUE(impl->filter_data_block(&batch1, &col_name_to_idx, id_to_name, filter1).ok());
    EXPECT_EQ(filter1[0], 1);
    EXPECT_EQ(filter1[1], 0); // id=2 deleted
    EXPECT_EQ(filter1[2], 1);

    // Second batch: [4, 5, 6]
    auto batch2 = build_int32_block("id", {4, 5, 6});
    auto filter2 = make_filter(3);
    ASSERT_TRUE(impl->filter_data_block(&batch2, &col_name_to_idx, id_to_name, filter2).ok());
    EXPECT_EQ(filter2[0], 0); // id=4 deleted
    EXPECT_EQ(filter2[1], 1);
    EXPECT_EQ(filter2[2], 1);
}

// ============================================================================
// SimpleEqualityDelete — Empty delete set
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleEmptyDeleteSet) {
    auto delete_block = build_int32_block("id", {});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_block("id", {1, 2, 3});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(3);
    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 1);
    EXPECT_EQ(filter[1], 1);
    EXPECT_EQ(filter[2], 1);
}

// ============================================================================
// SimpleEqualityDelete — Empty data block
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleEmptyDataBlock) {
    auto delete_block = build_int32_block("id", {1, 2});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_block("id", {});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(0);
    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
}

// ============================================================================
// SimpleEqualityDelete — Pre-filtered rows (filter already has some 0s)
// ============================================================================
TEST_F(EqualityDeleteTest, SimplePreFilteredRows) {
    auto delete_block = build_int32_block("id", {2});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_block("id", {1, 2, 3});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    // Pre-filter: row 0 already filtered out
    IColumn::Filter filter = {0, 1, 1};
    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // was already 0, stays 0
    EXPECT_EQ(filter[1], 0); // id=2 deleted
    EXPECT_EQ(filter[2], 1); // kept
}

// ============================================================================
// MultiEqualityDelete — Basic Two Columns
// ============================================================================
TEST_F(EqualityDeleteTest, MultiTwoColumnsBasic) {
    // Delete: (id=1, name="a"), (id=3, name="b")
    auto delete_block = build_int32_string_block("id", {1, 3}, "name", {"a", "b"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: (1,"a"), (1,"x"), (3,"b"), (3,"x")
    auto data_block = build_int32_string_block("id", {1, 1, 3, 3}, "name", {"a", "x", "b", "x"});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(4);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // (1,"a") deleted
    EXPECT_EQ(filter[1], 1); // (1,"x") kept
    EXPECT_EQ(filter[2], 0); // (3,"b") deleted
    EXPECT_EQ(filter[3], 1); // (3,"x") kept
}

TEST_F(EqualityDeleteTest, MultiTwoColumnsNoMatch) {
    auto delete_block = build_int32_string_block("id", {10, 20}, "name", {"x", "y"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_string_block("id", {1, 2}, "name", {"a", "b"});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(2);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 1);
    EXPECT_EQ(filter[1], 1);
}

TEST_F(EqualityDeleteTest, MultiTwoColumnsAllMatch) {
    auto delete_block = build_int32_string_block("id", {1, 2}, "name", {"a", "b"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_string_block("id", {1, 2}, "name", {"a", "b"});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(2);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0);
    EXPECT_EQ(filter[1], 0);
}

// ============================================================================
// MultiEqualityDelete — Pre-filtered (filter[i]=0 skips _equal comparison)
// ============================================================================
TEST_F(EqualityDeleteTest, MultiPreFilteredSkipsEqual) {
    auto delete_block = build_int32_string_block("id", {1}, "name", {"a"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data has (1,"a") at row 0 but pre-filtered
    auto data_block = build_int32_string_block("id", {1, 2}, "name", {"a", "b"});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    IColumn::Filter filter = {0, 1};

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // stayed 0 (pre-filtered → _equal skipped)
    EXPECT_EQ(filter[1], 1); // kept
}

// ============================================================================
// MultiEqualityDelete — Empty delete set
// ============================================================================
TEST_F(EqualityDeleteTest, MultiEmptyDeleteSet) {
    auto delete_block = build_int32_string_block("id", {}, "name", {});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_string_block("id", {1, 2}, "name", {"a", "b"});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(2);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 1);
    EXPECT_EQ(filter[1], 1);
}

// ============================================================================
// MultiEqualityDelete — Empty data block
// ============================================================================
TEST_F(EqualityDeleteTest, MultiEmptyDataBlock) {
    auto delete_block = build_int32_string_block("id", {1}, "name", {"a"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_string_block("id", {}, "name", {});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(0);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
}

// ============================================================================
// MultiEqualityDelete — Repeated filter calls (multiple batches)
// ============================================================================
TEST_F(EqualityDeleteTest, MultiRepeatedFilterCalls) {
    auto delete_block = build_int32_string_block("id", {1, 2}, "name", {"a", "b"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};

    // Batch 1
    auto batch1 = build_int32_string_block("id", {1, 3}, "name", {"a", "c"});
    auto filter1 = make_filter(2);
    ASSERT_TRUE(impl->filter_data_block(&batch1, &col_name_to_idx, id_to_name, filter1).ok());
    EXPECT_EQ(filter1[0], 0); // (1,"a") deleted
    EXPECT_EQ(filter1[1], 1); // (3,"c") kept

    // Batch 2
    auto batch2 = build_int32_string_block("id", {2, 4}, "name", {"b", "d"});
    auto filter2 = make_filter(2);
    ASSERT_TRUE(impl->filter_data_block(&batch2, &col_name_to_idx, id_to_name, filter2).ok());
    EXPECT_EQ(filter2[0], 0); // (2,"b") deleted
    EXPECT_EQ(filter2[1], 1); // (4,"d") kept
}

// ============================================================================
// MultiEqualityDelete — Type mismatch error
// ============================================================================
TEST_F(EqualityDeleteTest, MultiTypeMismatchError) {
    // Delete: Int32 + String
    auto delete_block = build_int32_string_block("id", {1}, "name", {"a"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: Int32 + Int32 (type mismatch for "name" column)
    auto int_col1 = ColumnInt32::create();
    int_col1->insert_value(1);
    auto int_col2 = ColumnInt32::create();
    int_col2->insert_value(42);
    Block data_block;
    data_block.insert({std::move(int_col1), std::make_shared<DataTypeInt32>(), "id"});
    data_block.insert({std::move(int_col2), std::make_shared<DataTypeInt32>(), "name"});

    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(1);

    auto st = impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("Not support type change") != std::string::npos);
}

// ============================================================================
// MultiEqualityDelete — Column not found error
// ============================================================================
TEST_F(EqualityDeleteTest, MultiColumnNotFoundError) {
    auto delete_block = build_int32_string_block("id", {1}, "name", {"a"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data block has "id" but not "name"
    auto data_block = build_int32_block("id", {1});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(1);

    auto st = impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not found in data block") != std::string::npos);
}

// ============================================================================
// MultiEqualityDelete — Duplicate values in delete set
// ============================================================================
TEST_F(EqualityDeleteTest, MultiDuplicateDeleteValues) {
    // Delete: same pair twice
    auto delete_block = build_int32_string_block("id", {1, 1}, "name", {"a", "a"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_string_block("id", {1, 2}, "name", {"a", "b"});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(2);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // (1,"a") deleted
    EXPECT_EQ(filter[1], 1); // (2,"b") kept
}

// ============================================================================
// Profile counter tests
// ============================================================================
TEST_F(EqualityDeleteTest, ProfileCountersSimple) {
    auto delete_block = build_int32_block("id", {10, 20, 30});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);

    RuntimeProfile test_profile("profile_test");
    ASSERT_TRUE(impl->init(&test_profile).ok());

    // Verify the profile has the EqualityDelete timer and counters
    auto* eq_delete_timer = test_profile.get_counter("EqualityDelete");
    ASSERT_NE(eq_delete_timer, nullptr);

    auto* num_rows = test_profile.get_counter("NumRowsInDeleteFile");
    ASSERT_NE(num_rows, nullptr);
    EXPECT_EQ(num_rows->value(), 3); // 3 delete rows

    auto* build_time = test_profile.get_counter("BuildHashSetTime");
    ASSERT_NE(build_time, nullptr);

    auto* filter_time = test_profile.get_counter("EqualityDeleteFilterTime");
    ASSERT_NE(filter_time, nullptr);
}

TEST_F(EqualityDeleteTest, ProfileCountersMulti) {
    auto delete_block =
            build_int32_string_block("id", {1, 2, 3, 4, 5}, "name", {"a", "b", "c", "d", "e"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);

    RuntimeProfile test_profile("profile_test");
    ASSERT_TRUE(impl->init(&test_profile).ok());

    auto* num_rows = test_profile.get_counter("NumRowsInDeleteFile");
    ASSERT_NE(num_rows, nullptr);
    EXPECT_EQ(num_rows->value(), 5);
}

// ============================================================================
// Large batch — correctness with many rows
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleLargeBatch) {
    // Delete every 10th value: 0, 10, 20, ..., 990
    std::vector<int32_t> delete_vals;
    for (int i = 0; i < 1000; i += 10) {
        delete_vals.push_back(i);
    }
    auto delete_block = build_int32_block("id", delete_vals);
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: 0, 1, 2, ..., 999
    std::vector<int32_t> data_vals;
    for (int i = 0; i < 1000; ++i) {
        data_vals.push_back(i);
    }
    auto data_block = build_int32_block("id", data_vals);
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(1000);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    for (int i = 0; i < 1000; ++i) {
        if (i % 10 == 0) {
            EXPECT_EQ(filter[i], 0) << "row " << i << " should be deleted";
        } else {
            EXPECT_EQ(filter[i], 1) << "row " << i << " should be kept";
        }
    }
}

TEST_F(EqualityDeleteTest, MultiLargeBatch) {
    // Delete every 10th pair
    std::vector<int32_t> delete_ints;
    std::vector<std::string> delete_strs;
    for (int i = 0; i < 1000; i += 10) {
        delete_ints.push_back(i);
        delete_strs.push_back(std::to_string(i));
    }
    auto delete_block = build_int32_string_block("id", delete_ints, "name", delete_strs);
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: 0..999 with matching strings
    std::vector<int32_t> data_ints;
    std::vector<std::string> data_strs;
    for (int i = 0; i < 1000; ++i) {
        data_ints.push_back(i);
        data_strs.push_back(std::to_string(i));
    }
    auto data_block = build_int32_string_block("id", data_ints, "name", data_strs);
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(1000);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    for (int i = 0; i < 1000; ++i) {
        if (i % 10 == 0) {
            EXPECT_EQ(filter[i], 0) << "row " << i << " should be deleted";
        } else {
            EXPECT_EQ(filter[i], 1) << "row " << i << " should be kept";
        }
    }
}

// ============================================================================
// MultiEqualityDelete — Partial column match (values match on one col only)
// ============================================================================
TEST_F(EqualityDeleteTest, MultiPartialColumnMatch) {
    // Delete: (1, "a")
    auto delete_block = build_int32_string_block("id", {1}, "name", {"a"});
    std::vector<int> col_ids = {100, 200};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data: (1, "b") — id matches but name doesn't
    auto data_block = build_int32_string_block("id", {1}, "name", {"b"});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}, {"name", 1}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}, {200, "name"}};
    auto filter = make_filter(1);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 1); // kept (only id matches, name doesn't)
}

// ============================================================================
// SimpleEqualityDelete — Duplicate values in delete set
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleDuplicateDeleteValues) {
    auto delete_block = build_int32_block("id", {1, 1, 2, 2, 3});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    auto data_block = build_int32_block("id", {1, 2, 3, 4});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(4);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // id=1 deleted
    EXPECT_EQ(filter[1], 0); // id=2 deleted
    EXPECT_EQ(filter[2], 0); // id=3 deleted
    EXPECT_EQ(filter[3], 1); // id=4 kept
}

// ============================================================================
// SimpleEqualityDelete — Single delete value matches multiple data rows
// ============================================================================
TEST_F(EqualityDeleteTest, SimpleSingleDeleteMatchesMultipleRows) {
    auto delete_block = build_int32_block("id", {5});
    std::vector<int> col_ids = {100};
    auto impl = EqualityDeleteBase::get_delete_impl(&delete_block, col_ids);
    ASSERT_TRUE(impl->init(&profile).ok());

    // Data has multiple 5s
    auto data_block = build_int32_block("id", {5, 1, 5, 2, 5});
    std::unordered_map<std::string, uint32_t> col_name_to_idx = {{"id", 0}};
    std::unordered_map<int, std::string> id_to_name = {{100, "id"}};
    auto filter = make_filter(5);

    ASSERT_TRUE(impl->filter_data_block(&data_block, &col_name_to_idx, id_to_name, filter).ok());
    EXPECT_EQ(filter[0], 0); // id=5 deleted
    EXPECT_EQ(filter[1], 1); // id=1 kept
    EXPECT_EQ(filter[2], 0); // id=5 deleted
    EXPECT_EQ(filter[3], 1); // id=2 kept
    EXPECT_EQ(filter[4], 0); // id=5 deleted
}

} // namespace doris
