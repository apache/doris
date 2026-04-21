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

#include "format/column_descriptor.h"

#include <gtest/gtest.h>

#include "exprs/vexpr_fwd.h"

namespace doris {

// ============================================================================
// Test: ColumnCategory enum values are distinct
// ============================================================================
TEST(ColumnCategoryTest, EnumValuesDistinct) {
    EXPECT_NE(ColumnCategory::REGULAR, ColumnCategory::PARTITION_KEY);
    EXPECT_NE(ColumnCategory::REGULAR, ColumnCategory::SYNTHESIZED);
    EXPECT_NE(ColumnCategory::REGULAR, ColumnCategory::GENERATED);
    EXPECT_NE(ColumnCategory::PARTITION_KEY, ColumnCategory::SYNTHESIZED);
    EXPECT_NE(ColumnCategory::PARTITION_KEY, ColumnCategory::GENERATED);
    EXPECT_NE(ColumnCategory::SYNTHESIZED, ColumnCategory::GENERATED);
}

// ============================================================================
// Test: ColumnDescriptor default construction
// ============================================================================
TEST(ColumnDescriptorTest, DefaultConstruction) {
    ColumnDescriptor desc;
    EXPECT_TRUE(desc.name.empty());
    EXPECT_EQ(desc.slot_desc, nullptr);
    EXPECT_EQ(desc.category, ColumnCategory::REGULAR);
    EXPECT_EQ(desc.default_expr, nullptr);
}

// ============================================================================
// Test: ColumnDescriptor aggregate initialization
// ============================================================================
TEST(ColumnDescriptorTest, AggregateInit) {
    ColumnDescriptor desc {"col_a", nullptr, ColumnCategory::PARTITION_KEY, nullptr};
    EXPECT_EQ(desc.name, "col_a");
    EXPECT_EQ(desc.category, ColumnCategory::PARTITION_KEY);
}

// ============================================================================
// Test: ColumnDescriptor with all categories
// ============================================================================
TEST(ColumnDescriptorTest, AllCategories) {
    ColumnDescriptor regular {"r", nullptr, ColumnCategory::REGULAR, nullptr};
    ColumnDescriptor partition {"p", nullptr, ColumnCategory::PARTITION_KEY, nullptr};
    ColumnDescriptor synthesized {"s", nullptr, ColumnCategory::SYNTHESIZED, nullptr};
    ColumnDescriptor generated {"g", nullptr, ColumnCategory::GENERATED, nullptr};

    EXPECT_EQ(regular.category, ColumnCategory::REGULAR);
    EXPECT_EQ(partition.category, ColumnCategory::PARTITION_KEY);
    EXPECT_EQ(synthesized.category, ColumnCategory::SYNTHESIZED);
    EXPECT_EQ(generated.category, ColumnCategory::GENERATED);
}

// ============================================================================
// Test: ColumnDescriptor in a vector (typical usage pattern)
// ============================================================================
TEST(ColumnDescriptorTest, VectorUsage) {
    std::vector<ColumnDescriptor> descs = {
            {"id", nullptr, ColumnCategory::REGULAR, nullptr},
            {"year", nullptr, ColumnCategory::PARTITION_KEY, nullptr},
            {"$row_id", nullptr, ColumnCategory::SYNTHESIZED, nullptr},
            {"_row_id", nullptr, ColumnCategory::GENERATED, nullptr},
    };
    EXPECT_EQ(descs.size(), 4);

    // Filter REGULAR + GENERATED (as on_before_init_reader does).
    std::vector<std::string> column_names;
    for (auto& desc : descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            column_names.push_back(desc.name);
        }
    }
    ASSERT_EQ(column_names.size(), 2);
    EXPECT_EQ(column_names[0], "id");
    EXPECT_EQ(column_names[1], "_row_id");
}

// ============================================================================
// Test: ColumnDescriptor copy and move semantics
// ============================================================================
TEST(ColumnDescriptorTest, CopySemantics) {
    ColumnDescriptor orig {"col_x", nullptr, ColumnCategory::SYNTHESIZED, nullptr};
    ColumnDescriptor copy = orig;
    EXPECT_EQ(copy.name, "col_x");
    EXPECT_EQ(copy.category, ColumnCategory::SYNTHESIZED);
}

TEST(ColumnDescriptorTest, MoveSemantics) {
    ColumnDescriptor orig {"col_y", nullptr, ColumnCategory::GENERATED, nullptr};
    ColumnDescriptor moved = std::move(orig);
    EXPECT_EQ(moved.name, "col_y");
    EXPECT_EQ(moved.category, ColumnCategory::GENERATED);
}

} // namespace doris
