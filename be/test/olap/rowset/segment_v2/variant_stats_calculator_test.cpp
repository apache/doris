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

#include "olap/rowset/segment_v2/variant_stats_calculator.h"

#include <gtest/gtest.h>

#include "gen_cpp/segment_v2.pb.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

class VariantStatsCalculatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a mock tablet schema
        _tablet_schema = std::make_shared<TabletSchema>();

        // Create a segment footer
        _footer = std::make_unique<SegmentFooterPB>();
    }

    void TearDown() override {
        _footer.reset();
        _tablet_schema.reset();
    }

    // Helper method to create a mock column with path info
    TabletColumn create_variant_column(int32_t unique_id, const std::string& name,
                                       int32_t parent_unique_id = -1,
                                       const std::string& path = "") {
        TabletColumn column;
        column.set_unique_id(unique_id);
        column.set_name(name);
        column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);

        if (parent_unique_id > 0 && !path.empty()) {
            vectorized::PathInData path_info(path);
            column.set_path_info(path_info);
            column.set_parent_unique_id(parent_unique_id);
        }
        column.set_variant_max_subcolumns_count(1);

        return column;
    }

    // Helper method to create a footer column with path info
    void add_footer_column_with_path(int32_t parent_unique_id, const std::string& path,
                                     uint32_t column_id = 0) {
        auto* column_meta = _footer->add_columns();
        column_meta->set_column_id(column_id);
        column_meta->set_unique_id(100 + _footer->columns_size());

        auto* path_info = column_meta->mutable_column_path_info();
        path_info->set_path(path);
        path_info->set_parrent_column_unique_id(parent_unique_id);
    }

    // Helper method to create a nullable column for testing
    vectorized::ColumnPtr create_nullable_column(const std::vector<bool>& null_map,
                                                 const std::vector<std::string>& values) {
        auto string_column = vectorized::ColumnString::create();
        auto null_column = vectorized::ColumnUInt8::create();

        for (size_t i = 0; i < values.size(); ++i) {
            if (null_map[i]) {
                string_column->insert_default();
                null_column->insert_value(1);
            } else {
                string_column->insert_data(values[i].data(), values[i].length());
                null_column->insert_value(0);
            }
        }

        return vectorized::ColumnNullable::create(std::move(string_column), std::move(null_column));
    }

    // Helper method to create a map column (sparse column)
    vectorized::ColumnPtr create_map_column() {
        auto keys = vectorized::ColumnString::create();
        auto values = vectorized::ColumnString::create();
        auto offsets = vectorized::ColumnArray::ColumnOffsets::create();

        // Add some sample data
        keys->insert_data("key1", 4);
        values->insert_data("value1", 6);
        keys->insert_data("key2", 4);
        values->insert_data("value2", 6);

        offsets->insert_value(0);
        offsets->insert_value(2);

        return vectorized::ColumnMap::create(std::move(keys), std::move(values),
                                             std::move(offsets));
    }

    TabletSchemaSPtr _tablet_schema;
    std::unique_ptr<SegmentFooterPB> _footer;
};

TEST_F(VariantStatsCalculatorTest, ConstructorWithEmptyFooter) {
    std::vector<uint32_t> column_ids = {0, 1, 2};

    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Test with empty footer - should not crash
    vectorized::Block block;
    auto status = calculator.calculate_variant_stats(&block, 0, 0);
    EXPECT_TRUE(status.ok());
}

TEST_F(VariantStatsCalculatorTest, ConstructorWithValidFooter) {
    // Add some columns with path info to footer
    add_footer_column_with_path(1, "sub_column_1");
    add_footer_column_with_path(1, "sub_column_2.__DORIS_VARIANT_SPARSE__");
    add_footer_column_with_path(2, "another_sub_column");

    std::vector<uint32_t> column_ids = {0, 1, 2};

    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Constructor should have built the path mapping
    vectorized::Block block;
    auto status = calculator.calculate_variant_stats(&block, 0, 0);
    EXPECT_TRUE(status.ok());
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithNoVariantColumns) {
    // Create tablet schema with regular columns (no variant columns)
    TabletColumn regular_column;
    regular_column.set_unique_id(1);
    regular_column.set_name("regular_col");
    regular_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);

    _tablet_schema->append_column(regular_column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Create a simple block
    vectorized::Block block;
    auto int_column = vectorized::ColumnVector<int32_t>::create();
    int_column->insert_value(123);
    block.insert(
            {std::move(int_column), std::make_shared<vectorized::DataTypeInt32>(), "regular_col"});

    auto status = calculator.calculate_variant_stats(&block, 0, 1);
    EXPECT_TRUE(status.ok());
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithSubColumn) {
    // Setup footer with sub column
    add_footer_column_with_path(1, "sub_column_1");

    // Create variant sub column
    TabletColumn sub_column =
            create_variant_column(2, "variant_col.sub_column_1", 1, "sub_column_1");
    _tablet_schema->append_column(sub_column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Create block with nullable column
    vectorized::Block block;
    auto nullable_column = create_nullable_column({false, true, false}, {"val1", "", "val3"});
    block.insert({std::move(nullable_column),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "sub_column_1"});

    auto status = calculator.calculate_variant_stats(&block, 0, 3);
    EXPECT_TRUE(status.ok());

    // Check that non-null size was updated
    auto& column_meta = _footer->columns(0);
    EXPECT_EQ(column_meta.none_null_size(), 2); // 2 non-null values
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithSparseColumn) {
    // Setup footer with sparse column
    add_footer_column_with_path(-1, "sparse_col");
    add_footer_column_with_path(1, "sparse_col.__DORIS_VARIANT_SPARSE__", 1);

    // Create variant sparse column
    TabletColumn parent_column = create_variant_column(1, "variant_col", -1, "sparse_col");
    TabletColumn sparse_column = create_variant_column(2, "variant_col.__DORIS_VARIANT_SPARSE__", 1,
                                                       "sparse_col.__DORIS_VARIANT_SPARSE__");
    _tablet_schema->append_column(parent_column);
    _tablet_schema->append_column(sparse_column);

    std::vector<uint32_t> column_ids = {0, 1};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Create block with map column (sparse column)
    vectorized::Block block;
    auto map_column = create_map_column();
    auto string_column = vectorized::ColumnString::create();
    // add parant column to block
    block.insert({std::move(string_column), std::make_shared<vectorized::DataTypeString>(),
                  "variant_column"});
    block.insert({std::move(map_column),
                  std::make_shared<vectorized::DataTypeMap>(
                          std::make_shared<vectorized::DataTypeString>(),
                          std::make_shared<vectorized::DataTypeString>()),
                  "sparse_column"});

    auto status = calculator.calculate_variant_stats(&block, 0, 1);
    EXPECT_TRUE(status.ok());

    // Check that variant statistics were updated
    auto& column_meta = _footer->columns(1);
    EXPECT_TRUE(column_meta.has_variant_statistics());
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithMissingFooterEntry) {
    // Create variant sub column but don't add corresponding footer entry
    TabletColumn sub_column = create_variant_column(2, "variant_col.missing_sub", 1, "missing_sub");
    _tablet_schema->append_column(sub_column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Create block with nullable column
    vectorized::Block block;
    auto nullable_column = create_nullable_column({false, true}, {"val1", ""});
    block.insert({std::move(nullable_column),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "missing_sub"});

    auto status = calculator.calculate_variant_stats(&block, 0, 2);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithMissingPathInFooter) {
    // Setup footer with different path than what tablet schema has
    add_footer_column_with_path(1, "different_path");

    // Create variant sub column with non-matching path
    TabletColumn sub_column =
            create_variant_column(2, "variant_col.sub_column", 1111, "sub_column");
    _tablet_schema->append_column(sub_column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Create block with nullable column
    vectorized::Block block;
    auto nullable_column = create_nullable_column({false}, {"val1"});
    block.insert({std::move(nullable_column),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "sub_column"});

    auto status = calculator.calculate_variant_stats(&block, 0, 1);
    EXPECT_FALSE(status.ok()) << status.to_string();
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithMultipleColumns) {
    // parent column
    add_footer_column_with_path(-1, "variant");
    TabletColumn parent_column = create_variant_column(1, "variant", -1, "variant");
    _tablet_schema->append_column(parent_column);

    // Setup footer with multiple columns
    add_footer_column_with_path(1, "sub1", 1);
    add_footer_column_with_path(1, "sub2.__DORIS_VARIANT_SPARSE__", 2);
    add_footer_column_with_path(2, "another_sub", 3);

    // Create multiple variant columns
    TabletColumn sub1 = create_variant_column(2, "variant.sub1", 1, "sub1");
    TabletColumn sparse = create_variant_column(3, "variant.__DORIS_VARIANT_SPARSE__", 1,
                                                "sub2.__DORIS_VARIANT_SPARSE__");
    TabletColumn sub2 = create_variant_column(4, "variant2.another_sub", 2, "another_sub");

    _tablet_schema->append_column(sub1);
    _tablet_schema->append_column(sparse);
    _tablet_schema->append_column(sub2);

    std::vector<uint32_t> column_ids = {0, 1, 2, 3};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Create block with multiple columns
    vectorized::Block block;

    // parent column
    auto string_column = vectorized::ColumnString::create();
    string_column->insert_data("test", 4);
    block.insert({std::move(string_column), std::make_shared<vectorized::DataTypeString>(),
                  "variant_column"});
    auto nullable_col1 = create_nullable_column({false, true, false}, {"a", "", "c"});
    block.insert({std::move(nullable_col1),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "sub1"});

    auto map_col = create_map_column();
    map_col->assume_mutable()->insert_many_defaults(3);
    block.insert({std::move(map_col),
                  std::make_shared<vectorized::DataTypeMap>(
                          std::make_shared<vectorized::DataTypeString>(),
                          std::make_shared<vectorized::DataTypeString>()),
                  "sparse"});

    auto nullable_col2 = create_nullable_column({true, false, true}, {"", "x", ""});
    block.insert({std::move(nullable_col2),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "another_sub"});

    auto status = calculator.calculate_variant_stats(&block, 0, 3);
    EXPECT_TRUE(status.ok());

    // Check that statistics were updated for sub columns
    EXPECT_EQ(_footer->columns(1).none_null_size(), 2);        // sub1: 2 non-null
    EXPECT_TRUE(_footer->columns(2).has_variant_statistics()); // sparse column
    EXPECT_EQ(_footer->columns(3).none_null_size(), 1);        // another_sub: 2 non-null
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithEmptyBlock) {
    add_footer_column_with_path(1, "sub_column");

    TabletColumn sub_column = create_variant_column(2, "variant.sub_column", 1, "sub_column");
    _tablet_schema->append_column(sub_column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Create empty block
    vectorized::Block block;
    auto empty_column = create_nullable_column({}, {});
    block.insert({std::move(empty_column),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "sub_column"});

    auto status = calculator.calculate_variant_stats(&block, 0, 0);
    EXPECT_TRUE(status.ok());

    // No change in statistics for empty block
    EXPECT_EQ(_footer->columns(0).none_null_size(), 0);
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithAllNullValues) {
    add_footer_column_with_path(1, "sub_column");

    TabletColumn sub_column = create_variant_column(2, "variant.sub_column", 1, "sub_column");
    _tablet_schema->append_column(sub_column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    // Create block with all null values
    vectorized::Block block;
    auto nullable_column = create_nullable_column({true, true, true}, {"", "", ""});
    block.insert({std::move(nullable_column),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "sub_column"});

    auto status = calculator.calculate_variant_stats(&block, 0, 3);
    EXPECT_TRUE(status.ok());

    // All null values should result in 0 non-null count
    EXPECT_EQ(_footer->columns(0).none_null_size(), 0);
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithNoPathInfo) {
    // Create regular column without path info
    TabletColumn regular_column;
    regular_column.set_unique_id(1);
    regular_column.set_name("regular");
    regular_column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    // No path info set

    _tablet_schema->append_column(regular_column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    vectorized::Block block;
    auto string_column = vectorized::ColumnString::create();
    string_column->insert_data("test", 4);
    block.insert(
            {std::move(string_column), std::make_shared<vectorized::DataTypeString>(), "regular"});

    auto status = calculator.calculate_variant_stats(&block, 0, 1);
    EXPECT_TRUE(status.ok()); // Should skip columns without path info
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsAccumulatesNonNullCount) {
    add_footer_column_with_path(1, "sub_column");

    // Set initial non-null count in footer
    _footer->mutable_columns(0)->set_none_null_size(5);

    TabletColumn sub_column = create_variant_column(2, "variant.sub_column", 1, "sub_column");
    _tablet_schema->append_column(sub_column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    vectorized::Block block;
    auto nullable_column = create_nullable_column({false, true, false}, {"a", "", "c"});
    block.insert({std::move(nullable_column),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "sub_column"});

    auto status = calculator.calculate_variant_stats(&block, 0, 3);
    EXPECT_TRUE(status.ok());

    // Should accumulate: initial 5 + new 2 = 7
    EXPECT_EQ(_footer->columns(0).none_null_size(), 7);
}

TEST_F(VariantStatsCalculatorTest, CalculateVariantStatsWithExtendedSchema) {
    add_footer_column_with_path(1, "sub_column");
    TabletColumn column;
    column.set_unique_id(1);
    column.set_name("variant");
    column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    column.set_variant_max_subcolumns_count(0);
    _tablet_schema->append_column(column);

    std::vector<uint32_t> column_ids = {0};
    VariantStatsCaculator calculator(_footer.get(), _tablet_schema, column_ids);

    vectorized::Block block;
    auto nullable_column = create_nullable_column({false, true, false}, {"a", "", "c"});
    block.insert({std::move(nullable_column),
                  std::make_shared<vectorized::DataTypeNullable>(
                          std::make_shared<vectorized::DataTypeString>()),
                  "sub_column"});

    auto status = calculator.calculate_variant_stats(&block, 0, 3);
    EXPECT_TRUE(status.ok());
}

} // namespace doris::segment_v2