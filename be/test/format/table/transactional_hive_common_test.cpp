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

#include "format/table/transactional_hive_common.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>

namespace doris {

// ============================================================================
// AcidRowID — Hash, Equality, Ordering
// ============================================================================
class AcidRowIDTest : public ::testing::Test {};

TEST_F(AcidRowIDTest, EqualityBasic) {
    AcidRowID a {100, 0, 42};
    AcidRowID b {100, 0, 42};
    AcidRowID c {100, 0, 43};
    AcidRowID::Eq eq;
    EXPECT_TRUE(eq(a, b));
    EXPECT_FALSE(eq(a, c));
}

TEST_F(AcidRowIDTest, EqualityDiffTransaction) {
    AcidRowID a {100, 0, 42};
    AcidRowID b {101, 0, 42};
    AcidRowID::Eq eq;
    EXPECT_FALSE(eq(a, b));
}

TEST_F(AcidRowIDTest, EqualityDiffBucket) {
    AcidRowID a {100, 0, 42};
    AcidRowID b {100, 1, 42};
    AcidRowID::Eq eq;
    EXPECT_FALSE(eq(a, b));
}

TEST_F(AcidRowIDTest, HashConsistency) {
    AcidRowID a {100, 0, 42};
    AcidRowID b {100, 0, 42};
    AcidRowID::Hash hasher;
    EXPECT_EQ(hasher(a), hasher(b));
}

TEST_F(AcidRowIDTest, HashDistribution) {
    AcidRowID::Hash hasher;
    // Different row IDs should produce different hashes (probabilistic)
    AcidRowID a {100, 0, 1};
    AcidRowID b {100, 0, 2};
    AcidRowID c {200, 0, 1};
    // While hash collisions are possible, these specific inputs should differ
    EXPECT_NE(hasher(a), hasher(b));
    EXPECT_NE(hasher(a), hasher(c));
}

TEST_F(AcidRowIDTest, OperatorLessTransaction) {
    AcidRowID a {100, 0, 42};
    AcidRowID b {200, 0, 42};
    EXPECT_TRUE(a < b);
    EXPECT_FALSE(b < a);
}

TEST_F(AcidRowIDTest, OperatorLessBucket) {
    AcidRowID a {100, 0, 42};
    AcidRowID b {100, 1, 42};
    EXPECT_TRUE(a < b);
    EXPECT_FALSE(b < a);
}

TEST_F(AcidRowIDTest, OperatorLessRowId) {
    AcidRowID a {100, 0, 42};
    AcidRowID b {100, 0, 43};
    EXPECT_TRUE(a < b);
    EXPECT_FALSE(b < a);
}

TEST_F(AcidRowIDTest, OperatorLessEqual) {
    AcidRowID a {100, 0, 42};
    AcidRowID b {100, 0, 42};
    // Equal elements: neither is less than the other
    EXPECT_FALSE(a < b);
    EXPECT_FALSE(b < a);
}

// ============================================================================
// AcidRowIDSet — Insert and Lookup
// ============================================================================
TEST_F(AcidRowIDTest, SetInsertAndFind) {
    AcidRowIDSet set;
    set.insert({100, 0, 1});
    set.insert({100, 0, 2});
    set.insert({200, 1, 3});

    EXPECT_TRUE(set.contains({100, 0, 1}));
    EXPECT_TRUE(set.contains({100, 0, 2}));
    EXPECT_TRUE(set.contains({200, 1, 3}));
    EXPECT_FALSE(set.contains({100, 0, 3}));
    EXPECT_FALSE(set.contains({300, 0, 1}));
}

TEST_F(AcidRowIDTest, SetDuplicateInsert) {
    AcidRowIDSet set;
    set.insert({100, 0, 1});
    set.insert({100, 0, 1}); // duplicate
    EXPECT_EQ(set.size(), 1);
}

TEST_F(AcidRowIDTest, SetEmpty) {
    AcidRowIDSet set;
    EXPECT_EQ(set.size(), 0);
    EXPECT_FALSE(set.contains({0, 0, 0}));
}

TEST_F(AcidRowIDTest, SetLargeInsert) {
    AcidRowIDSet set;
    for (int i = 0; i < 10000; ++i) {
        set.insert({static_cast<int64_t>(i / 100), static_cast<int64_t>(i % 10),
                    static_cast<int64_t>(i)});
    }
    EXPECT_EQ(set.size(), 10000);
    EXPECT_TRUE(set.contains({0, 0, 0}));
    EXPECT_TRUE(set.contains({99, 9, 9999}));
    EXPECT_FALSE(set.contains({100, 0, 10000}));
}

TEST_F(AcidRowIDTest, SetNegativeValues) {
    AcidRowIDSet set;
    set.insert({-1, -2, -3});
    EXPECT_TRUE(set.contains({-1, -2, -3}));
    EXPECT_FALSE(set.contains({1, 2, 3}));
}

TEST_F(AcidRowIDTest, SetZeroValues) {
    AcidRowIDSet set;
    set.insert({0, 0, 0});
    EXPECT_TRUE(set.contains({0, 0, 0}));
    EXPECT_EQ(set.size(), 1);
}

// ============================================================================
// TransactionalHive Constants — Verification
// ============================================================================
class TransactionalHiveConstantsTest : public ::testing::Test {};

TEST_F(TransactionalHiveConstantsTest, AcidColumnNames) {
    // Verify all 6 ACID column names are present and in correct order
    ASSERT_EQ(TransactionalHive::ACID_COLUMN_NAMES.size(), 6);
    EXPECT_EQ(TransactionalHive::ACID_COLUMN_NAMES[0], "operation");
    EXPECT_EQ(TransactionalHive::ACID_COLUMN_NAMES[1], "originalTransaction");
    EXPECT_EQ(TransactionalHive::ACID_COLUMN_NAMES[2], "bucket");
    EXPECT_EQ(TransactionalHive::ACID_COLUMN_NAMES[3], "rowId");
    EXPECT_EQ(TransactionalHive::ACID_COLUMN_NAMES[4], "currentTransaction");
    EXPECT_EQ(TransactionalHive::ACID_COLUMN_NAMES[5], "row");
}

TEST_F(TransactionalHiveConstantsTest, AcidColumnNamesLowerCase) {
    ASSERT_EQ(TransactionalHive::ACID_COLUMN_NAMES_LOWER_CASE.size(), 6);
    for (const auto& name : TransactionalHive::ACID_COLUMN_NAMES_LOWER_CASE) {
        // All should be lowercase
        std::string lower = name;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        EXPECT_EQ(name, lower) << "Column name '" << name << "' is not lowercase";
    }
}

TEST_F(TransactionalHiveConstantsTest, RowOffset) {
    // ROW is at index 5 in ACID columns: operation(0), originalTransaction(1),
    // bucket(2), rowId(3), currentTransaction(4), row(5)
    EXPECT_EQ(TransactionalHive::ROW_OFFSET, 5);
}

TEST_F(TransactionalHiveConstantsTest, DeleteRowParams) {
    ASSERT_EQ(TransactionalHive::DELETE_ROW_PARAMS.size(), 3);
    // originalTransaction (bigint), bucket (int), rowId (bigint)
    EXPECT_EQ(TransactionalHive::DELETE_ROW_PARAMS[0].type, PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(TransactionalHive::DELETE_ROW_PARAMS[1].type, PrimitiveType::TYPE_INT);
    EXPECT_EQ(TransactionalHive::DELETE_ROW_PARAMS[2].type, PrimitiveType::TYPE_BIGINT);
}

TEST_F(TransactionalHiveConstantsTest, ReadParams) {
    ASSERT_EQ(TransactionalHive::READ_PARAMS.size(), 3);
    EXPECT_EQ(TransactionalHive::READ_PARAMS[0].type, PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(TransactionalHive::READ_PARAMS[1].type, PrimitiveType::TYPE_INT);
    EXPECT_EQ(TransactionalHive::READ_PARAMS[2].type, PrimitiveType::TYPE_BIGINT);
}

TEST_F(TransactionalHiveConstantsTest, DeleteColumnNamesConsistency) {
    // DELETE_ROW_COLUMN_NAMES should match DELETE_ROW_PARAMS column names
    ASSERT_EQ(TransactionalHive::DELETE_ROW_COLUMN_NAMES.size(),
              TransactionalHive::DELETE_ROW_PARAMS.size());
    for (size_t i = 0; i < TransactionalHive::DELETE_ROW_PARAMS.size(); ++i) {
        EXPECT_EQ(TransactionalHive::DELETE_ROW_COLUMN_NAMES[i],
                  TransactionalHive::DELETE_ROW_PARAMS[i].column_name);
    }
}

TEST_F(TransactionalHiveConstantsTest, DeleteColumnNamesLowerCaseConsistency) {
    ASSERT_EQ(TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE.size(),
              TransactionalHive::DELETE_ROW_PARAMS.size());
    for (size_t i = 0; i < TransactionalHive::DELETE_ROW_PARAMS.size(); ++i) {
        EXPECT_EQ(TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE[i],
                  TransactionalHive::DELETE_ROW_PARAMS[i].column_lower_case);
    }
}

TEST_F(TransactionalHiveConstantsTest, ReadColumnNamesConsistency) {
    ASSERT_EQ(TransactionalHive::READ_ROW_COLUMN_NAMES.size(),
              TransactionalHive::READ_PARAMS.size());
    for (size_t i = 0; i < TransactionalHive::READ_PARAMS.size(); ++i) {
        EXPECT_EQ(TransactionalHive::READ_ROW_COLUMN_NAMES[i],
                  TransactionalHive::READ_PARAMS[i].column_name);
    }
}

TEST_F(TransactionalHiveConstantsTest, DeleteColNameToBlockIdx) {
    ASSERT_EQ(TransactionalHive::DELETE_COL_NAME_TO_BLOCK_IDX.size(), 3);
    // Should map lowercase names to sequential indices 0, 1, 2
    for (size_t i = 0; i < TransactionalHive::DELETE_ROW_PARAMS.size(); ++i) {
        auto it = TransactionalHive::DELETE_COL_NAME_TO_BLOCK_IDX.find(
                TransactionalHive::DELETE_ROW_PARAMS[i].column_lower_case);
        ASSERT_NE(it, TransactionalHive::DELETE_COL_NAME_TO_BLOCK_IDX.end());
        EXPECT_EQ(it->second, i);
    }
}

// ============================================================================
// AcidRowID ordering for sorted operations
// ============================================================================
TEST_F(AcidRowIDTest, SortedOrder) {
    std::vector<AcidRowID> rows = {
            {200, 0, 1}, {100, 0, 1}, {100, 1, 0}, {100, 0, 2}, {100, 0, 1},
    };
    std::sort(rows.begin(), rows.end());
    // Expected order: (100,0,1), (100,0,1), (100,0,2), (100,1,0), (200,0,1)
    EXPECT_EQ(rows[0].original_transaction, 100);
    EXPECT_EQ(rows[0].bucket, 0);
    EXPECT_EQ(rows[0].row_id, 1);

    EXPECT_EQ(rows[1].original_transaction, 100);
    EXPECT_EQ(rows[1].bucket, 0);
    EXPECT_EQ(rows[1].row_id, 1);

    EXPECT_EQ(rows[2].original_transaction, 100);
    EXPECT_EQ(rows[2].bucket, 0);
    EXPECT_EQ(rows[2].row_id, 2);

    EXPECT_EQ(rows[3].original_transaction, 100);
    EXPECT_EQ(rows[3].bucket, 1);
    EXPECT_EQ(rows[3].row_id, 0);

    EXPECT_EQ(rows[4].original_transaction, 200);
    EXPECT_EQ(rows[4].bucket, 0);
    EXPECT_EQ(rows[4].row_id, 1);
}

} // namespace doris
