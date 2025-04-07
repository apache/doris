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

#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/format/table/table_format_reader.h"

namespace doris::vectorized {
class MockTableSchemaChangeHelper : public TableSchemaChangeHelper {
public:
    MockTableSchemaChangeHelper(std::map<int, std::string> file_schema, bool exist_schema = true)
            : _file_schema(std::move(file_schema)), _exist_schema(exist_schema) {}

    Status get_file_col_id_to_name(bool& exist_schema,
                                   std::map<int, std::string>& file_col_id_to_name) override {
        exist_schema = _exist_schema;
        if (_exist_schema) {
            file_col_id_to_name = _file_schema;
        }
        return Status::OK();
    }

    bool has_schema_change() const { return _has_schema_change; }
    const std::vector<std::string>& all_required_col_names() const {
        return _all_required_col_names;
    }
    const std::vector<std::string>& not_in_file_col_names() const { return _not_in_file_col_names; }
    const std::unordered_map<std::string, std::string>& file_col_to_table_col() const {
        return _file_col_to_table_col;
    }
    const std::unordered_map<std::string, std::string>& table_col_to_file_col() const {
        return _table_col_to_file_col;
    }
    const std::unordered_map<std::string, ColumnValueRangeType>& new_colname_to_value_range()
            const {
        return _new_colname_to_value_range;
    }

private:
    std::map<int, std::string> _file_schema;
    bool _exist_schema;
};

TEST(TableSchemaChangeHelperTest, NoSchemaChange) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2"}, {3, "col3"}};
    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2"}, {3, "col3"}};

    std::vector<std::string> read_cols = {"col1", "col3"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_FALSE(helper.has_schema_change());
    ASSERT_EQ(helper.all_required_col_names().size(), 2);
    ASSERT_EQ(helper.all_required_col_names()[0], "col1");
    ASSERT_EQ(helper.all_required_col_names()[1], "col3");
    ASSERT_TRUE(helper.not_in_file_col_names().empty());
}

TEST(TableSchemaChangeHelperTest, WithSchemaChange) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2_old"}, {3, "col3_old"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2_new"}, {3, "col3_new"}};

    std::vector<std::string> read_cols = {"col1", "col2_new", "col3_new"};

    std::unordered_map<std::string, ColumnValueRangeType> col_ranges = {
            {"col2_new", ColumnValueRangeType()}};

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_TRUE(helper.has_schema_change());
    ASSERT_EQ(helper.all_required_col_names().size(), 3);
    ASSERT_EQ(helper.all_required_col_names()[0], "col1");
    ASSERT_EQ(helper.all_required_col_names()[1], "col2_old");
    ASSERT_EQ(helper.all_required_col_names()[2], "col3_old");
    ASSERT_TRUE(helper.not_in_file_col_names().empty());

    ASSERT_EQ(helper.table_col_to_file_col().size(), 3);
    ASSERT_EQ(helper.table_col_to_file_col().at("col2_new"), "col2_old");
    ASSERT_EQ(helper.table_col_to_file_col().at("col3_new"), "col3_old");

    ASSERT_EQ(helper.file_col_to_table_col().size(), 3);
    ASSERT_EQ(helper.file_col_to_table_col().at("col2_old"), "col2_new");
    ASSERT_EQ(helper.file_col_to_table_col().at("col3_old"), "col3_new");

    ASSERT_EQ(helper.new_colname_to_value_range().size(), 1);
    ASSERT_TRUE(helper.new_colname_to_value_range().find("col2_old") !=
                helper.new_colname_to_value_range().end());
}

TEST(TableSchemaChangeHelperTest, MissingColumns) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2"}

    };

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2"}, {3, "col3"}, {4, "col4"}};
    std::vector<std::string> read_cols = {"col1", "col3", "col4"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges = {
            {"col3", ColumnValueRangeType()}};
    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_FALSE(helper.has_schema_change());
    ASSERT_EQ(helper.all_required_col_names().size(), 3);
    ASSERT_EQ(helper.all_required_col_names()[0], "col1");
    ASSERT_EQ(helper.all_required_col_names()[1], "col3");
    ASSERT_EQ(helper.all_required_col_names()[2], "col4");
    ASSERT_EQ(helper.not_in_file_col_names().size(), 2);
    ASSERT_EQ(helper.not_in_file_col_names()[0], "col3");
    ASSERT_EQ(helper.not_in_file_col_names()[1], "col4");
}

TEST(TableSchemaChangeHelperTest, NoFileSchema) {
    std::map<int, std::string> file_schema;

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2"}, {3, "col3"}};

    std::vector<std::string> read_cols = {"col1", "col2"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;
    MockTableSchemaChangeHelper helper(file_schema, false);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_FALSE(helper.has_schema_change());
    ASSERT_EQ(helper.all_required_col_names().size(), 2);
    ASSERT_EQ(helper.all_required_col_names()[0], "col1");
    ASSERT_EQ(helper.all_required_col_names()[1], "col2");
    ASSERT_TRUE(helper.not_in_file_col_names().empty());
}

TEST(TableSchemaChangeHelperTest, MixedScenario) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2_old"}, {4, "col4_old"}};
    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2_new"}, {3, "col3"}, {4, "col4_new"}, {5, "col5"}};
    std::vector<std::string> read_cols = {"col1", "col2_new", "col3", "col4_new", "col5"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges = {
            {"col2_new", ColumnValueRangeType()},
            {"col3", ColumnValueRangeType()},
            {"col5", ColumnValueRangeType()}};
    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());
    ASSERT_TRUE(helper.has_schema_change());
    ASSERT_EQ(helper.all_required_col_names().size(), 5);
    ASSERT_EQ(helper.all_required_col_names()[0], "col1");
    ASSERT_EQ(helper.all_required_col_names()[1], "col2_old");
    ASSERT_EQ(helper.all_required_col_names()[2], "col3");
    ASSERT_EQ(helper.all_required_col_names()[3], "col4_old");
    ASSERT_EQ(helper.all_required_col_names()[4], "col5");
    ASSERT_EQ(helper.not_in_file_col_names().size(), 2);
    ASSERT_EQ(helper.not_in_file_col_names()[0], "col3");
    ASSERT_EQ(helper.not_in_file_col_names()[1], "col5");
    ASSERT_EQ(helper.table_col_to_file_col().at("col2_new"), "col2_old");
    ASSERT_EQ(helper.table_col_to_file_col().at("col4_new"), "col4_old");
    ASSERT_EQ(helper.new_colname_to_value_range().size(), 3);
    ASSERT_TRUE(helper.new_colname_to_value_range().find("col2_old") !=
                helper.new_colname_to_value_range().end());
    ASSERT_TRUE(helper.new_colname_to_value_range().find("col3") !=
                helper.new_colname_to_value_range().end());
    ASSERT_TRUE(helper.new_colname_to_value_range().find("col5") !=
                helper.new_colname_to_value_range().end());
}

TEST(TableSchemaChangeHelperTest, EmptySchemas) {
    std::map<int, std::string> file_schema;
    std::unordered_map<int32_t, std::string> table_id_to_name;
    std::vector<std::string> read_cols;
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;
    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_FALSE(helper.has_schema_change());
    ASSERT_TRUE(helper.all_required_col_names().empty());
    ASSERT_TRUE(helper.not_in_file_col_names().empty());
    ASSERT_TRUE(helper.table_col_to_file_col().empty());
    ASSERT_TRUE(helper.file_col_to_table_col().empty());
    ASSERT_TRUE(helper.new_colname_to_value_range().empty());
}

TEST(TableSchemaChangeHelperTest, IdMismatch) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2"}, {3, "col3"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {10, "col1"}, {20, "col2"}, {30, "col3"}};

    std::vector<std::string> read_cols = {"col1", "col2", "col3"};

    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_FALSE(helper.has_schema_change());
    ASSERT_EQ(helper.all_required_col_names().size(), 3);
    ASSERT_EQ(helper.not_in_file_col_names().size(), 3);
    ASSERT_TRUE(helper.table_col_to_file_col().empty());
    ASSERT_TRUE(helper.file_col_to_table_col().empty());
}

TEST(TableSchemaChangeHelperTest, DuplicateColumnNames) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2"}, {3, "col2"}, {4, "col1"}};

    std::vector<std::string> read_cols = {"col1", "col2"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_FALSE(helper.has_schema_change());
    ASSERT_EQ(helper.all_required_col_names().size(), 2);
    ASSERT_EQ(helper.all_required_col_names()[0], "col1");
    ASSERT_EQ(helper.all_required_col_names()[1], "col2");
    ASSERT_TRUE(helper.not_in_file_col_names().empty());
    ASSERT_EQ(helper.table_col_to_file_col().size(), 2);
}

TEST(TableSchemaChangeHelperTest, ValueRangeForNonReadColumns) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2"}, {3, "col3"}, {4, "col4"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2_new"}, {3, "col3"}, {4, "col4"}};

    std::vector<std::string> read_cols = {"col1", "col3"};

    std::unordered_map<std::string, ColumnValueRangeType> col_ranges = {
            {"col1", ColumnValueRangeType()},
            {"col2_new", ColumnValueRangeType()},
            {"col4", ColumnValueRangeType()}};

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_TRUE(helper.has_schema_change());
    ASSERT_EQ(helper.all_required_col_names().size(), 2);
    ASSERT_EQ(helper.all_required_col_names()[0], "col1");
    ASSERT_EQ(helper.all_required_col_names()[1], "col3");
    ASSERT_TRUE(helper.not_in_file_col_names().empty());

    ASSERT_EQ(helper.new_colname_to_value_range().size(), 3);
    ASSERT_TRUE(helper.new_colname_to_value_range().find("col1") !=
                helper.new_colname_to_value_range().end());
    ASSERT_TRUE(helper.new_colname_to_value_range().find("col2") !=
                helper.new_colname_to_value_range().end());
    ASSERT_TRUE(helper.new_colname_to_value_range().find("col4") !=
                helper.new_colname_to_value_range().end());
}

TEST(TableSchemaChangeHelperTest, PartialIdMatch) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2"}, {3, "col3"}, {4, "col4"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {20, "col2"}, {3, "col3_new"}, {40, "col4_new"}};
    std::vector<std::string> read_cols = {"col1", "col2", "col3_new", "col4_new"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_TRUE(helper.has_schema_change());

    ASSERT_EQ(helper.all_required_col_names().size(), 4);
    ASSERT_EQ(helper.all_required_col_names()[0], "col1");
    ASSERT_EQ(helper.all_required_col_names()[1], "col2");
    ASSERT_EQ(helper.all_required_col_names()[2], "col3");
    ASSERT_EQ(helper.all_required_col_names()[3], "col4_new");

    ASSERT_EQ(helper.not_in_file_col_names().size(), 2);
    ASSERT_EQ(helper.not_in_file_col_names()[0], "col2");
    ASSERT_EQ(helper.not_in_file_col_names()[1], "col4_new");

    ASSERT_EQ(helper.table_col_to_file_col().size(), 2);
    ASSERT_EQ(helper.table_col_to_file_col().at("col1"), "col1");
    ASSERT_EQ(helper.table_col_to_file_col().at("col3_new"), "col3");
}

Block create_test_block(const std::vector<std::string>& column_names) {
    Block block;
    for (const auto& name : column_names) {
        auto column = ColumnString::create();
        block.insert(
                ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), name));
    }
    return block;
}

TEST(TableSchemaChangeHelperTest, BasicColumnNameConversion) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2_old"}, {3, "col3_old"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2_new"}, {3, "col3_new"}};

    std::vector<std::string> read_cols = {"col1", "col2_new", "col3_new"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_TRUE(helper.has_schema_change());

    Block before_block = create_test_block({"col1", "col2_new", "col3_new"});
    ASSERT_TRUE(helper.get_next_block_before(&before_block).ok());

    ASSERT_EQ(before_block.get_by_position(0).name, "col1");
    ASSERT_EQ(before_block.get_by_position(1).name, "col2_old");
    ASSERT_EQ(before_block.get_by_position(2).name, "col3_old");

    Block after_block = create_test_block({"col1", "col2_old", "col3_old"});
    ASSERT_TRUE(helper.get_next_block_after(&after_block).ok());

    ASSERT_EQ(after_block.get_by_position(0).name, "col1");
    ASSERT_EQ(after_block.get_by_position(1).name, "col2_new");
    ASSERT_EQ(after_block.get_by_position(2).name, "col3_new");
}

TEST(TableSchemaChangeHelperTest, NoSchemaChangeBlocks) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2"}, {3, "col3"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2"}, {3, "col3"}};

    std::vector<std::string> read_cols = {"col1", "col2", "col3"};

    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_FALSE(helper.has_schema_change());

    Block before_block = create_test_block({"col1", "col2", "col3"});
    ASSERT_TRUE(helper.get_next_block_before(&before_block).ok());

    ASSERT_EQ(before_block.get_by_position(0).name, "col1");
    ASSERT_EQ(before_block.get_by_position(1).name, "col2");
    ASSERT_EQ(before_block.get_by_position(2).name, "col3");

    Block after_block = create_test_block({"col1", "col2", "col3"});
    ASSERT_TRUE(helper.get_next_block_after(&after_block).ok());

    ASSERT_EQ(after_block.get_by_position(0).name, "col1");
    ASSERT_EQ(after_block.get_by_position(1).name, "col2");
    ASSERT_EQ(after_block.get_by_position(2).name, "col3");
}

TEST(TableSchemaChangeHelperTest, MixedColumnNameConversion) {
    std::map<int, std::string> file_schema = {
            {1, "col1"}, {2, "col2_old"}, {3, "col3"}, {4, "col4_old"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2_new"}, {3, "col3"}, {4, "col4_new"}, {5, "col5"}};

    std::vector<std::string> read_cols = {"col1", "col2_new", "col3", "col4_new", "col5"};

    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;
    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_TRUE(helper.has_schema_change());
    Block before_block =
            create_test_block({"col1", "col2_new", "col3", "col4_new", "col5", "extra_col"});

    ASSERT_TRUE(helper.get_next_block_before(&before_block).ok());

    ASSERT_EQ(before_block.get_by_position(0).name, "col1");
    ASSERT_EQ(before_block.get_by_position(1).name, "col2_old");
    ASSERT_EQ(before_block.get_by_position(2).name, "col3");
    ASSERT_EQ(before_block.get_by_position(3).name, "col4_old");
    ASSERT_EQ(before_block.get_by_position(4).name, "col5");
    ASSERT_EQ(before_block.get_by_position(5).name, "extra_col");

    Block after_block =
            create_test_block({"col1", "col2_old", "col3", "col4_old", "col5", "extra_col"});

    ASSERT_TRUE(helper.get_next_block_after(&after_block).ok());
    ASSERT_EQ(after_block.get_by_position(0).name, "col1");
    ASSERT_EQ(after_block.get_by_position(1).name, "col2_new");
    ASSERT_EQ(after_block.get_by_position(2).name, "col3");
    ASSERT_EQ(after_block.get_by_position(3).name, "col4_new");
    ASSERT_EQ(after_block.get_by_position(4).name, "col5");
    ASSERT_EQ(after_block.get_by_position(5).name, "extra_col");
}

TEST(TableSchemaChangeHelperTest, EmptyAndSingleColumnBlocks) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2_old"}};

    std::unordered_map<int32_t, std::string> table_id_to_name = {{1, "col1"}, {2, "col2_new"}};

    std::vector<std::string> read_cols = {"col1", "col2_new"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;

    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());
    ASSERT_TRUE(helper.has_schema_change());

    Block empty_block;
    ASSERT_TRUE(helper.get_next_block_before(&empty_block).ok());
    ASSERT_TRUE(helper.get_next_block_after(&empty_block).ok());
    ASSERT_EQ(empty_block.columns(), 0);

    Block single_block1 = create_test_block({"col1"});
    ASSERT_TRUE(helper.get_next_block_before(&single_block1).ok());
    ASSERT_EQ(single_block1.get_by_position(0).name, "col1");

    ASSERT_TRUE(helper.get_next_block_after(&single_block1).ok());
    ASSERT_EQ(single_block1.get_by_position(0).name, "col1");

    Block single_block2 = create_test_block({"col2_new"});
    ASSERT_TRUE(helper.get_next_block_before(&single_block2).ok());
    ASSERT_EQ(single_block2.get_by_position(0).name, "col2_old");

    Block single_block3 = create_test_block({"col2_old"});
    ASSERT_TRUE(helper.get_next_block_after(&single_block3).ok());
    ASSERT_EQ(single_block3.get_by_position(0).name, "col2_new");
}

TEST(TableSchemaChangeHelperTest, ColumnOrderChange) {
    std::map<int, std::string> file_schema = {{1, "col1"}, {2, "col2_old"}, {3, "col3_old"}};
    std::unordered_map<int32_t, std::string> table_id_to_name = {
            {1, "col1"}, {2, "col2_new"}, {3, "col3_new"}};
    std::vector<std::string> read_cols = {"col1", "col2_new", "col3_new"};
    std::unordered_map<std::string, ColumnValueRangeType> col_ranges;
    MockTableSchemaChangeHelper helper(file_schema);
    ASSERT_TRUE(helper.init_schema_info(read_cols, table_id_to_name, &col_ranges).ok());

    ASSERT_TRUE(helper.has_schema_change());

    Block before_block = create_test_block({"col3_new", "col1", "col2_new"});
    ASSERT_TRUE(helper.get_next_block_before(&before_block).ok());

    ASSERT_EQ(before_block.get_by_position(0).name, "col3_old");
    ASSERT_EQ(before_block.get_by_position(1).name, "col1");
    ASSERT_EQ(before_block.get_by_position(2).name, "col2_old");

    Block after_block = create_test_block({"col3_old", "col1", "col2_old"});
    ASSERT_TRUE(helper.get_next_block_after(&after_block).ok());

    ASSERT_EQ(after_block.get_by_position(0).name, "col3_new");
    ASSERT_EQ(after_block.get_by_position(1).name, "col1");
    ASSERT_EQ(after_block.get_by_position(2).name, "col2_new");
}
} // namespace doris::vectorized
