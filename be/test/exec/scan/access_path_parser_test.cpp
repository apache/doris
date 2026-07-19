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

#include "exec/scan/access_path_parser.h"

#include <gen_cpp/Descriptors_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"

namespace doris {
namespace {

TColumnAccessPath data_access_path(std::vector<std::string> path) {
    TColumnAccessPath access_path;
    access_path.__set_type(TAccessPathType::DATA);
    TDataAccessPath data_path;
    data_path.__set_path(std::move(path));
    access_path.__set_data_access_path(std::move(data_path));
    return access_path;
}

TColumnAccessPath data_access_path_without_payload() {
    TColumnAccessPath access_path;
    access_path.__set_type(TAccessPathType::DATA);
    return access_path;
}

TColumnAccessPath meta_access_path() {
    TColumnAccessPath access_path;
    access_path.__set_type(TAccessPathType::META);
    return access_path;
}

format::ColumnDefinition field(int32_t id, std::string name, DataTypePtr type,
                               std::vector<format::ColumnDefinition> children = {},
                               std::vector<std::string> aliases = {},
                               bool has_name_mapping = false) {
    return {
            .identifier = Field::create_field<TYPE_INT>(id),
            .name = std::move(name),
            .name_mapping = std::move(aliases),
            .has_name_mapping = has_name_mapping,
            .type = std::move(type),
            .children = std::move(children),
    };
}

format::ColumnDefinition root_column(int32_t id, std::string name, DataTypePtr type) {
    return {
            .identifier = Field::create_field<TYPE_INT>(id),
            .name = std::move(name),
            .type = std::move(type),
    };
}

void expect_child(const format::ColumnDefinition& child, int32_t id, const std::string& name) {
    ASSERT_TRUE(child.has_identifier_field_id());
    EXPECT_EQ(child.get_identifier_field_id(), id);
    EXPECT_EQ(child.name, name);
}

const format::ColumnDefinition* find_child_by_name(const format::ColumnDefinition& parent,
                                                   const std::string& name) {
    for (const auto& child : parent.children) {
        if (child.name == name) {
            return &child;
        }
    }
    return nullptr;
}

} // namespace

// Scenario: primitive columns and scanner-materialized virtual columns should not build nested
// children, even when their descriptor carries access paths that are not meaningful to the parser.
TEST(AccessPathParserTest, IgnoresPrimitiveColumnsAndScannerVirtualColumns) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = std::make_shared<DataTypeString>();

    // Primitive columns have no nested children, so parser should not inspect even invalid paths.
    auto primitive = root_column(1, "id", int_type);
    auto status = AccessPathParser::build_nested_children(
            &primitive, std::vector<TColumnAccessPath> {meta_access_path()}, nullptr);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(primitive.children.empty());

    // Iceberg rowid is materialized by scanner/table-reader logic and may carry a negative access
    // path. Parser must leave it untouched.
    auto rowid_type = std::make_shared<DataTypeStruct>(
            DataTypes {string_type, std::make_shared<DataTypeInt64>(),
                       std::make_shared<DataTypeInt32>(), string_type},
            Strings {"file_path", "row_pos", "partition_spec_id", "partition_data_json"});
    format::ColumnDefinition rowid {
            .identifier = Field::create_field<TYPE_STRING>(BeConsts::ICEBERG_ROWID_COL),
            .name = BeConsts::ICEBERG_ROWID_COL,
            .type = rowid_type,
    };
    status = AccessPathParser::build_nested_children(
            &rowid, std::vector<TColumnAccessPath> {data_access_path({"-1"})}, nullptr);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(rowid.children.empty());
}

// Scenario: reject unsupported top-level inputs before recursive type parsing, including META
// paths, missing DATA payloads, and access paths whose root does not match the projected slot.
TEST(AccessPathParserTest, RejectsUnsupportedTopLevelAccessPathInputs) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"});

    struct Case {
        std::string name;
        format::ColumnDefinition column;
        std::vector<TColumnAccessPath> paths;
    };
    std::vector<Case> cases;
    cases.push_back({"meta path", root_column(100, "s", struct_type), {meta_access_path()}});
    cases.push_back({"missing DATA payload",
                     root_column(100, "s", struct_type),
                     {data_access_path_without_payload()}});
    cases.push_back({"wrong root name",
                     root_column(100, "s", struct_type),
                     {data_access_path({"other", "a"})}});
    cases.push_back({"wrong root field id",
                     root_column(100, "s", struct_type),
                     {data_access_path({"101", "a"})}});

    for (auto& test_case : cases) {
        auto status = AccessPathParser::build_nested_children(&test_case.column, test_case.paths,
                                                              nullptr);
        EXPECT_FALSE(status.ok()) << test_case.name;
    }
}

// Scenario: struct access paths support field-id lookup, alias lookup, case-insensitive name
// fallback, and whole-struct expansion; reserved array/map path tokens remain invalid.
TEST(AccessPathParserTest, StructAccessPathMatrix) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    format::ColumnDefinition schema {
            .identifier = Field::create_field<TYPE_INT>(100),
            .name = "s",
            .type = struct_type,
            .children =
                    {
                            field(101, "a", int_type, {}, {}, true),
                            field(205, "b", int_type, {}, {"old_b"}),
                    },
    };

    {
        auto column = root_column(100, "s", struct_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"s", "A"})}, nullptr);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 1);
        expect_child(column.children[0], 0, "a");
    }
    {
        auto column = root_column(100, "s", struct_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"100", "205"})},
                &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 1);
        expect_child(column.children[0], 205, "b");
    }
    {
        auto column = root_column(100, "s", struct_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"s", "old_b"})},
                &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 1);
        expect_child(column.children[0], 205, "b");
        EXPECT_EQ(column.children[0].name_mapping, std::vector<std::string>({"old_b"}));
    }
    {
        auto column = root_column(100, "s", struct_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"s", "a"})}, &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 1);
        expect_child(column.children[0], 101, "a");
        EXPECT_TRUE(column.children[0].has_name_mapping);
    }
    {
        auto column = root_column(100, "s", struct_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"s"})}, &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 2);
        expect_child(column.children[0], 101, "a");
        EXPECT_TRUE(column.children[0].has_name_mapping);
        expect_child(column.children[1], 205, "b");
    }

    for (const auto& invalid_child : {"OFFSET", "*", "KEYS", "VALUES", "missing"}) {
        auto column = root_column(100, "s", struct_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"s", invalid_child})},
                &schema);
        EXPECT_FALSE(status.ok()) << invalid_child;
    }
}

// Scenario: array access paths must pass through the "*" element token, then reuse struct child
// parsing under the element wrapper; invalid array tokens are rejected.
TEST(AccessPathParserTest, ArrayAccessPathMatrix) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = std::make_shared<DataTypeString>();
    auto element_type = std::make_shared<DataTypeStruct>(DataTypes {string_type, int_type},
                                                         Strings {"item", "quantity"});
    auto array_type = std::make_shared<DataTypeArray>(element_type);
    format::ColumnDefinition schema {
            .identifier = Field::create_field<TYPE_INT>(200),
            .name = "items",
            .type = array_type,
            .children =
                    {
                            field(201, "element", element_type,
                                  {
                                          field(202, "item", string_type, {}, {"old_item"}),
                                          field(203, "quantity", int_type, {}, {}, true),
                                  }),
                    },
    };

    {
        auto column = root_column(200, "items", array_type);
        auto status = AccessPathParser::build_nested_children(
                &column,
                std::vector<TColumnAccessPath> {data_access_path({"items", "*", "old_item"})},
                &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 1);
        expect_child(column.children[0], 201, "element");
        ASSERT_EQ(column.children[0].children.size(), 1);
        expect_child(column.children[0].children[0], 202, "item");
        EXPECT_EQ(column.children[0].children[0].name_mapping,
                  std::vector<std::string>({"old_item"}));
    }
    {
        auto column = root_column(200, "items", array_type);
        auto status = AccessPathParser::build_nested_children(
                &column,
                std::vector<TColumnAccessPath> {data_access_path({"items", "*", "quantity"})},
                &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 1);
        ASSERT_EQ(column.children[0].children.size(), 1);
        expect_child(column.children[0].children[0], 203, "quantity");
        EXPECT_TRUE(column.children[0].children[0].has_name_mapping);
    }
    {
        auto column = root_column(200, "items", array_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"items"})}, &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 1);
        expect_child(column.children[0], 201, "element");
        ASSERT_EQ(column.children[0].children.size(), 2);
        expect_child(column.children[0].children[0], 202, "item");
        expect_child(column.children[0].children[1], 203, "quantity");
        EXPECT_TRUE(column.children[0].children[1].has_name_mapping);
    }

    for (const auto& invalid_path : std::vector<std::vector<std::string>> {
                 {"items", "OFFSET"}, {"items", "item"}, {"items", "*", "missing"}}) {
        auto column = root_column(200, "items", array_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path(invalid_path)}, &schema);
        EXPECT_FALSE(status.ok()) << invalid_path.back();
    }
}

// Scenario: map access paths split KEYS/VALUES, force the missing side needed for materialization,
// merge repeated value-child requests, and reject unsupported map child tokens.
TEST(AccessPathParserTest, MapAccessPathMatrix) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = std::make_shared<DataTypeString>();
    auto value_type = std::make_shared<DataTypeStruct>(
            DataTypes {string_type, int_type, string_type}, Strings {"full_name", "age", "gender"});
    auto map_type = std::make_shared<DataTypeMap>(string_type, value_type);
    format::ColumnDefinition schema {
            .identifier = Field::create_field<TYPE_INT>(300),
            .name = "m",
            .type = map_type,
            .children =
                    {
                            field(301, "key", string_type),
                            field(302, "value", value_type,
                                  {
                                          field(303, "full_name", string_type, {}, {"name"}),
                                          field(304, "age", int_type, {}, {}, true),
                                          field(305, "gender", string_type),
                                  }),
                    },
    };

    {
        auto column = root_column(300, "m", map_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"m", "KEYS"})}, &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 2);
        expect_child(column.children[0], 301, "key");
        expect_child(column.children[1], 302, "value");
        ASSERT_EQ(column.children[1].children.size(), 3);
        const auto* full_name = find_child_by_name(column.children[1], "full_name");
        ASSERT_NE(full_name, nullptr);
        expect_child(*full_name, 303, "full_name");
        const auto* age = find_child_by_name(column.children[1], "age");
        ASSERT_NE(age, nullptr);
        expect_child(*age, 304, "age");
        const auto* gender = find_child_by_name(column.children[1], "gender");
        ASSERT_NE(gender, nullptr);
        expect_child(*gender, 305, "gender");
    }
    {
        auto column = root_column(300, "m", map_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"m", "VALUES", "age"})},
                &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 2);
        expect_child(column.children[0], 301, "key");
        expect_child(column.children[1], 302, "value");
        ASSERT_EQ(column.children[1].children.size(), 1);
        expect_child(column.children[1].children[0], 304, "age");
        EXPECT_TRUE(column.children[1].children[0].has_name_mapping);
    }
    {
        auto column = root_column(300, "m", map_type);
        auto status = AccessPathParser::build_nested_children(
                &column,
                std::vector<TColumnAccessPath> {
                        data_access_path({"m", "VALUES", "name"}),
                        data_access_path({"m", "*", "gender"}),
                },
                &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 2);
        ASSERT_EQ(column.children[1].children.size(), 2);
        const auto* full_name = find_child_by_name(column.children[1], "full_name");
        ASSERT_NE(full_name, nullptr);
        expect_child(*full_name, 303, "full_name");
        EXPECT_EQ(full_name->name_mapping, std::vector<std::string>({"name"}));
        const auto* gender = find_child_by_name(column.children[1], "gender");
        ASSERT_NE(gender, nullptr);
        expect_child(*gender, 305, "gender");
    }
    {
        auto column = root_column(300, "m", map_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path({"m"})}, &schema);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column.children.size(), 2);
        ASSERT_EQ(column.children[1].children.size(), 3);
        const auto* age = find_child_by_name(column.children[1], "age");
        ASSERT_NE(age, nullptr);
        EXPECT_TRUE(age->has_name_mapping);
    }

    for (const auto& invalid_path : std::vector<std::vector<std::string>> {
                 {"m", "OFFSET"}, {"m", "ENTRY"}, {"m", "VALUES", "missing"}}) {
        auto column = root_column(300, "m", map_type);
        auto status = AccessPathParser::build_nested_children(
                &column, std::vector<TColumnAccessPath> {data_access_path(invalid_path)}, &schema);
        EXPECT_FALSE(status.ok()) << invalid_path.back();
    }
}

TEST(AccessPathParserTest, PreservesNestedInitialDefaultMetadata) {
    auto binary_type = std::make_shared<DataTypeString>();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {binary_type}, Strings {"data"});
    auto defaulted_child = field(101, "data", binary_type);
    defaulted_child.initial_default_value = "AAEC/w==";
    defaulted_child.initial_default_value_is_base64 = true;
    auto schema = field(100, "s", struct_type, {defaulted_child});

    auto column = root_column(100, "s", struct_type);
    auto status = AccessPathParser::build_nested_children(
            &column, std::vector<TColumnAccessPath> {data_access_path({"s", "data"})}, &schema);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(column.children.size(), 1);
    EXPECT_EQ(column.children[0].initial_default_value, std::optional<std::string>("AAEC/w=="));
    EXPECT_TRUE(column.children[0].initial_default_value_is_base64);
}

} // namespace doris
