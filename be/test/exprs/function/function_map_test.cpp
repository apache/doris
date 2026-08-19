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

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"

namespace doris {

namespace {

MutableColumnPtr make_nullable_int_column(const std::vector<std::optional<int32_t>>& values) {
    auto nested = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        nested->insert_value(value.value_or(0));
        null_map->insert_value(value.has_value() ? 0 : 1);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

MutableColumnPtr make_nullable_bool_column(const std::vector<std::optional<bool>>& values) {
    auto nested = ColumnUInt8::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        nested->insert_value(value.value_or(false));
        null_map->insert_value(value.has_value() ? 0 : 1);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

MutableColumnPtr make_offsets(const std::vector<size_t>& offsets) {
    auto result = ColumnArray::ColumnOffsets::create();
    for (size_t offset : offsets) {
        result->insert_value(offset);
    }
    return result;
}

ColumnPtr make_int_map(const std::vector<std::optional<int32_t>>& keys,
                       const std::vector<std::optional<int32_t>>& values,
                       const std::vector<size_t>& offsets) {
    return ColumnMap::create(make_nullable_int_column(keys), make_nullable_int_column(values),
                             make_offsets(offsets));
}

ColumnPtr make_int_array(const std::vector<std::optional<int32_t>>& values,
                         const std::vector<size_t>& offsets) {
    return ColumnArray::create(make_nullable_int_column(values), make_offsets(offsets));
}

ColumnPtr make_bool_array(const std::vector<std::optional<bool>>& values,
                          const std::vector<size_t>& offsets) {
    return ColumnArray::create(make_nullable_bool_column(values), make_offsets(offsets));
}

ColumnPtr make_int_entry_array(const std::vector<std::optional<int32_t>>& keys,
                               const std::vector<std::optional<int32_t>>& values,
                               const std::vector<size_t>& offsets,
                               const std::vector<bool>& null_entries = {}) {
    auto entries = ColumnStruct::create(
            Columns {make_nullable_int_column(keys), make_nullable_int_column(values)});
    auto null_map = ColumnUInt8::create(entries->size(), 0);
    for (size_t i = 0; i < null_entries.size(); ++i) {
        null_map->get_data()[i] = null_entries[i];
    }
    return ColumnArray::create(ColumnNullable::create(std::move(entries), std::move(null_map)),
                               make_offsets(offsets));
}

int32_t get_nullable_int(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    return assert_cast<const ColumnInt32&>(nullable.get_nested_column()).get_element(row);
}

Status execute_map_function(const std::string& name, Block& block, const ColumnNumbers& arguments,
                            uint32_t result, const DataTypePtr& return_type) {
    ColumnsWithTypeAndName argument_template;
    for (uint32_t argument : arguments) {
        argument_template.push_back(block.get_by_position(argument));
    }
    auto function =
            SimpleFunctionFactory::instance().get_function(name, argument_template, return_type);
    if (function == nullptr) {
        return Status::InternalError("function {} is not registered", name);
    }
    return function->execute(nullptr, block, arguments, result, block.rows());
}

} // namespace

TEST(FunctionMapTest, deduplicate_map) {
    const std::string func_name = "deduplicate_map";

    auto type_map = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                                  std::make_shared<DataTypeInt32>());
    auto argument_template = ColumnsWithTypeAndName {{nullptr, type_map, "map"}};

    auto function = SimpleFunctionFactory::instance().get_function(
            func_name, argument_template, type_map, {true},
            BeExecVersionManager::get_newest_version());

    ASSERT_TRUE(function != nullptr);

    Block block;

    auto key_column = ColumnString::create();
    auto value_column = ColumnInt32::create();
    auto offset_column = ColumnArray::ColumnOffsets::create();

    const size_t count = 1024;
    for (size_t i = 0; i < count; ++i) {
        // keys with duplicates
        auto value = int32_t(i % 8);
        auto key = fmt::format("key_{}", value);

        key_column->insert_data(key.data(), key.size());
        value_column->insert_data(reinterpret_cast<const char*>(&value), 4);
    }

    const size_t rows = 32;
    size_t offset = 0;
    for (size_t i = 0; i < rows; ++i) {
        offset += count / rows;
        offset_column->insert_data(reinterpret_cast<const char*>(&offset), sizeof(offset));
    }

    auto column_map = ColumnMap::create(std::move(key_column), std::move(value_column),
                                        std::move(offset_column));
    block.insert({std::move(column_map), type_map, "map"});
    block.insert({nullptr, type_map, "result"});
    uint32_t result = 1;

    auto st = function->execute(nullptr, block, {0}, result, rows);
    ASSERT_TRUE(st.ok()) << "execute failed: " << st.to_string();

    auto result_column = block.get_by_position(result).column;
    auto& result_map_column = assert_cast<const ColumnMap&>(*result_column);
    for (size_t i = 0; i < rows; ++i) {
        auto map_size = result_map_column.get_offsets()[i] -
                        (i == 0 ? 0 : result_map_column.get_offsets()[i - 1]);
        ASSERT_EQ(map_size, 8) << "deduplicate map failed at row " << i;
    }
}

TEST(FunctionMapTest, map_lambda_post_process_functions) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(nullable_int, nullable_int);
    auto int_array_type = std::make_shared<DataTypeArray>(nullable_int);
    auto bool_array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeBool>()));

    {
        Block block;
        block.insert({make_int_map({1, 2, 3}, {10, 20, 30}, {2, 3}), map_type, "map"});
        block.insert({make_bool_array({true, std::nullopt, false}, {2, 3}), bool_array_type,
                      "predicate"});
        block.insert({nullptr, map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_filter", block, {0, 1}, 2, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
        ASSERT_EQ(result.get_offsets()[0], 1);
        ASSERT_EQ(result.get_offsets()[1], 1);
        ASSERT_EQ(get_nullable_int(result.get_keys(), 0), 1);
        ASSERT_EQ(get_nullable_int(result.get_values(), 0), 10);
    }

    {
        Block block;
        block.insert({make_int_array({2, 2, 4}, {2, 3}), int_array_type, "keys"});
        block.insert({make_int_array({10, 20, 30}, {2, 3}), int_array_type, "values"});
        block.insert({nullptr, map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_from_arrays", block, {0, 1}, 2, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
        ASSERT_EQ(result.get_offsets()[0], 1);
        ASSERT_EQ(result.get_offsets()[1], 2);
        ASSERT_EQ(get_nullable_int(result.get_keys(), 0), 2);
        ASSERT_EQ(get_nullable_int(result.get_values(), 0), 20);
        ASSERT_EQ(get_nullable_int(result.get_keys(), 1), 4);
        ASSERT_EQ(get_nullable_int(result.get_values(), 1), 30);
    }

    {
        Block block;
        block.insert({make_int_array({1, 2, 3}, {2, 3}), int_array_type, "keys"});
        block.insert({make_int_array({10, 20, 30}, {1, 3}), int_array_type, "values"});
        block.insert({nullptr, map_type, "result"});

        auto status = execute_map_function("map_from_arrays", block, {0, 1}, 2, map_type);
        ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
    }
}

TEST(FunctionMapTest, map_from_arrays_reuses_columns_without_duplicates) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(nullable_int, nullable_int);
    auto int_array_type = std::make_shared<DataTypeArray>(nullable_int);
    ColumnPtr keys = make_int_array({1, 2, 3}, {2, 3});
    ColumnPtr values = make_int_array({10, 20, 30}, {2, 3});
    const auto& key_array = assert_cast<const ColumnArray&>(*keys);
    const auto& value_array = assert_cast<const ColumnArray&>(*values);
    const auto* expected_keys = key_array.get_data_ptr().get();
    const auto* expected_values = value_array.get_data_ptr().get();
    const auto* expected_offsets = key_array.get_offsets_ptr().get();

    Block block;
    block.insert({keys, int_array_type, "keys"});
    block.insert({values, int_array_type, "values"});
    block.insert({nullptr, map_type, "result"});

    ASSERT_TRUE(execute_map_function("map_from_arrays", block, {0, 1}, 2, map_type).ok());
    const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_keys_ptr().get(), expected_keys);
    EXPECT_EQ(result.get_values_ptr().get(), expected_values);
    EXPECT_EQ(result.get_offsets_ptr().get(), expected_offsets);
}

TEST(FunctionMapTest, map_from_arrays_unique_skips_deduplication) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(nullable_int, nullable_int);
    auto int_array_type = std::make_shared<DataTypeArray>(nullable_int);

    Block block;
    block.insert({make_int_array({1, 1}, {2}), int_array_type, "keys"});
    block.insert({make_int_array({10, 20}, {2}), int_array_type, "values"});
    block.insert({nullptr, map_type, "result"});

    ASSERT_TRUE(execute_map_function("%map_from_arrays_unique%", block, {0, 1}, 2, map_type).ok());
    const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_keys().size(), 2);
    EXPECT_EQ(result.get_values().size(), 2);
}

TEST(FunctionMapTest, map_from_entries) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto entry_struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int, nullable_int}, Strings {"key", "value"});
    auto entry_array_type = std::make_shared<DataTypeArray>(make_nullable(entry_struct_type));
    auto map_type = std::make_shared<DataTypeMap>(nullable_int, nullable_int);

    {
        ColumnPtr entries = make_int_entry_array({1, 2, 3}, {10, 20, 30}, {2, 3});
        const auto& entry_array = assert_cast<const ColumnArray&>(*entries);
        const auto& entry_struct = assert_cast<const ColumnStruct&>(
                assert_cast<const ColumnNullable&>(entry_array.get_data()).get_nested_column());
        const auto* expected_keys = entry_struct.get_column_ptr(0).get();
        const auto* expected_values = entry_struct.get_column_ptr(1).get();
        const auto* expected_offsets = entry_array.get_offsets_ptr().get();

        Block block;
        block.insert({entries, entry_array_type, "entries"});
        block.insert({nullptr, map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_from_entries", block, {0}, 1, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(1).column);
        EXPECT_EQ(result.get_keys_ptr().get(), expected_keys);
        EXPECT_EQ(result.get_values_ptr().get(), expected_values);
        EXPECT_EQ(result.get_offsets_ptr().get(), expected_offsets);
    }

    {
        Block block;
        block.insert({ColumnConst::create(make_int_entry_array({1, 2}, {10, 20}, {2}), 3),
                      entry_array_type, "entries"});
        block.insert({nullptr, map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_from_entries", block, {0}, 1, map_type).ok());
        const auto& result = assert_cast<const ColumnConst&>(*block.get_by_position(1).column);
        EXPECT_EQ(result.size(), 3);
        const auto& result_map = assert_cast<const ColumnMap&>(result.get_data_column());
        ASSERT_EQ(result_map.get_offsets().size(), 1);
        EXPECT_EQ(result_map.get_offsets()[0], 2);
    }

    {
        auto null_literal = std::make_shared<DataTypeUInt8>();
        null_literal->set_null_literal(true);
        auto nullable_null_literal = make_nullable(null_literal);
        auto nullable_null_map_type = make_nullable(
                std::make_shared<DataTypeMap>(nullable_null_literal, nullable_null_literal));

        Block block;
        block.insert({nullable_null_literal->create_column_const(2, Field()), nullable_null_literal,
                      "entries"});
        block.insert({nullptr, nullable_null_map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_from_entries", block, {0}, 1, nullable_null_map_type)
                            .ok());
        const auto& result = assert_cast<const ColumnConst&>(*block.get_by_position(1).column);
        EXPECT_EQ(result.size(), 2);
        EXPECT_TRUE(assert_cast<const ColumnNullable&>(result.get_data_column()).is_null_at(0));
    }

    {
        Block block;
        block.insert({make_int_entry_array({1, 1, 2}, {10, 20, 30}, {2, 3}), entry_array_type,
                      "entries"});
        block.insert({nullptr, map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_from_entries", block, {0}, 1, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(1).column);
        ASSERT_EQ(result.get_offsets()[0], 1);
        ASSERT_EQ(result.get_offsets()[1], 2);
        EXPECT_EQ(get_nullable_int(result.get_keys(), 0), 1);
        EXPECT_EQ(get_nullable_int(result.get_values(), 0), 20);
        EXPECT_EQ(get_nullable_int(result.get_keys(), 1), 2);
        EXPECT_EQ(get_nullable_int(result.get_values(), 1), 30);
    }

    {
        Block block;
        block.insert({make_int_entry_array({1, 2}, {10, 20}, {2}, {false, true}), entry_array_type,
                      "entries"});
        block.insert({nullptr, map_type, "result"});

        auto status = execute_map_function("map_from_entries", block, {0}, 1, map_type);
        ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
    }

    {
        auto array_null_map = ColumnUInt8::create();
        array_null_map->insert_value(0);
        array_null_map->insert_value(1);
        array_null_map->insert_value(0);
        auto nullable_entry_array_type = make_nullable(entry_array_type);
        auto nullable_map_type = make_nullable(map_type);

        Block block;
        block.insert({ColumnNullable::create(make_int_entry_array({1, 2, 3}, {10, 20, 30},
                                                                  {1, 2, 3}, {false, true, false}),
                                             std::move(array_null_map)),
                      nullable_entry_array_type, "entries"});
        block.insert({nullptr, nullable_map_type, "result"});

        ASSERT_TRUE(
                execute_map_function("map_from_entries", block, {0}, 1, nullable_map_type).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        EXPECT_FALSE(result.is_null_at(0));
        EXPECT_TRUE(result.is_null_at(1));
        EXPECT_FALSE(result.is_null_at(2));
        const auto& result_map = assert_cast<const ColumnMap&>(result.get_nested_column());
        EXPECT_EQ(get_nullable_int(result_map.get_keys(), 0), 1);
        EXPECT_EQ(get_nullable_int(result_map.get_values(), 0), 10);
        EXPECT_EQ(get_nullable_int(result_map.get_keys(), 2), 3);
        EXPECT_EQ(get_nullable_int(result_map.get_values(), 2), 30);
    }
}

TEST(FunctionMapTest, map_lambda_partial_const_arguments) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(nullable_int, nullable_int);
    auto int_array_type = std::make_shared<DataTypeArray>(nullable_int);
    auto bool_array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeBool>()));

    {
        Block block;
        block.insert({ColumnConst::create(make_int_array({1, 2}, {2}), 2), int_array_type, "keys"});
        block.insert({make_int_array({11, 21, 12, 22}, {2, 4}), int_array_type, "values"});
        block.insert({nullptr, map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_from_arrays", block, {0, 1}, 2, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
        ASSERT_EQ(result.get_offsets()[0], 2);
        ASSERT_EQ(result.get_offsets()[1], 4);
        EXPECT_EQ(get_nullable_int(result.get_keys(), 2), 1);
        EXPECT_EQ(get_nullable_int(result.get_values(), 0), 11);
        EXPECT_EQ(get_nullable_int(result.get_values(), 2), 12);
    }

    {
        Block block;
        block.insert({make_int_array({1, 2, 3, 4}, {2, 4}), int_array_type, "keys"});
        block.insert(
                {ColumnConst::create(make_int_array({10, 20}, {2}), 2), int_array_type, "values"});
        block.insert({nullptr, map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_from_arrays", block, {0, 1}, 2, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
        EXPECT_EQ(get_nullable_int(result.get_values(), 0), 10);
        EXPECT_EQ(get_nullable_int(result.get_values(), 2), 10);
    }

    {
        Block block;
        block.insert(
                {ColumnConst::create(make_int_map({1, 2}, {10, 20}, {2}), 3), map_type, "map"});
        block.insert({make_bool_array({true, true, true, false, false, true}, {2, 4, 6}),
                      bool_array_type, "predicate"});
        block.insert({nullptr, map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_filter", block, {0, 1}, 2, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
        ASSERT_EQ(result.get_offsets()[0], 2);
        ASSERT_EQ(result.get_offsets()[1], 3);
        ASSERT_EQ(result.get_offsets()[2], 4);
        EXPECT_EQ(get_nullable_int(result.get_keys(), 2), 1);
        EXPECT_EQ(get_nullable_int(result.get_keys(), 3), 2);
    }
}

TEST(FunctionMapTest, map_filter_top_level_nullable_inputs) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(nullable_int, nullable_int);
    auto nullable_map_type = make_nullable(map_type);
    auto bool_array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeBool>()));
    auto nullable_bool_array_type = make_nullable(bool_array_type);

    {
        auto map_null_map = ColumnUInt8::create();
        map_null_map->insert_value(0);
        map_null_map->insert_value(0);
        map_null_map->insert_value(1);

        Block block;
        block.insert({ColumnNullable::create(make_int_map({1, 2}, {10, 20}, {1, 2, 2}),
                                             std::move(map_null_map)),
                      nullable_map_type, "map"});
        block.insert(
                {make_bool_array({true, false, true}, {1, 2, 3}), bool_array_type, "predicate"});
        block.insert({nullptr, nullable_map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_filter", block, {0, 1}, 2, nullable_map_type).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        EXPECT_FALSE(result.is_null_at(0));
        EXPECT_FALSE(result.is_null_at(1));
        EXPECT_TRUE(result.is_null_at(2));
        const auto& nested = assert_cast<const ColumnMap&>(result.get_nested_column());
        EXPECT_EQ(nested.get_offsets()[0], 1);
        EXPECT_EQ(nested.get_offsets()[1], 1);
        EXPECT_EQ(nested.get_offsets()[2], 1);
        ASSERT_EQ(nested.get_keys().size(), 1);
        EXPECT_EQ(get_nullable_int(nested.get_keys(), 0), 1);
        EXPECT_EQ(get_nullable_int(nested.get_values(), 0), 10);
    }

    {
        auto predicate_null_map = ColumnUInt8::create();
        predicate_null_map->insert_value(0);
        predicate_null_map->insert_value(1);

        Block block;
        block.insert({make_int_map({1, 2}, {10, 20}, {1, 2}), map_type, "map"});
        block.insert({ColumnNullable::create(make_bool_array({true}, {1, 1}),
                                             std::move(predicate_null_map)),
                      nullable_bool_array_type, "predicate"});
        block.insert({nullptr, nullable_map_type, "result"});

        ASSERT_TRUE(execute_map_function("map_filter", block, {0, 1}, 2, nullable_map_type).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        EXPECT_FALSE(result.is_null_at(0));
        EXPECT_TRUE(result.is_null_at(1));
        const auto& nested = assert_cast<const ColumnMap&>(result.get_nested_column());
        EXPECT_EQ(nested.get_offsets()[0], 1);
        EXPECT_EQ(nested.get_offsets()[1], 1);
    }
}

TEST(FunctionMapTest, map_from_arrays_top_level_nullable_input) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto array_type = std::make_shared<DataTypeArray>(nullable_int);
    auto nullable_array_type = make_nullable(array_type);
    auto map_type = std::make_shared<DataTypeMap>(nullable_int, nullable_int);
    auto nullable_map_type = make_nullable(map_type);
    auto key_null_map = ColumnUInt8::create();
    key_null_map->insert_value(0);
    key_null_map->insert_value(1);
    key_null_map->insert_value(0);
    auto value_null_map = ColumnUInt8::create();
    value_null_map->insert_value(0);
    value_null_map->insert_value(0);
    value_null_map->insert_value(1);

    Block block;
    block.insert(
            {ColumnNullable::create(make_int_array({1, 2, 3}, {1, 1, 3}), std::move(key_null_map)),
             nullable_array_type, "keys"});
    block.insert({ColumnNullable::create(make_int_array({10, 20, 30}, {1, 3, 3}),
                                         std::move(value_null_map)),
                  nullable_array_type, "values"});
    block.insert({nullptr, nullable_map_type, "result"});

    ASSERT_TRUE(execute_map_function("map_from_arrays", block, {0, 1}, 2, nullable_map_type).ok());
    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_FALSE(result.is_null_at(0));
    EXPECT_TRUE(result.is_null_at(1));
    EXPECT_TRUE(result.is_null_at(2));
    const auto& nested = assert_cast<const ColumnMap&>(result.get_nested_column());
    ASSERT_EQ(nested.get_offsets()[0], 1);
    ASSERT_EQ(nested.get_offsets()[1], 1);
    ASSERT_EQ(nested.get_offsets()[2], 1);
    ASSERT_EQ(nested.get_keys().size(), 1);
    ASSERT_EQ(nested.get_values().size(), 1);
    EXPECT_EQ(get_nullable_int(nested.get_keys(), 0), 1);
    EXPECT_EQ(get_nullable_int(nested.get_values(), 0), 10);

    auto const_null_map = ColumnUInt8::create();
    const_null_map->insert_value(1);
    Block const_block;
    const_block.insert({ColumnConst::create(ColumnNullable::create(make_int_array({1}, {1}),
                                                                   std::move(const_null_map)),
                                            2),
                        nullable_array_type, "keys"});
    const_block.insert({make_int_array({10, 20}, {1, 2}), array_type, "values"});
    const_block.insert({nullptr, nullable_map_type, "result"});

    ASSERT_TRUE(execute_map_function("map_from_arrays", const_block, {0, 1}, 2, nullable_map_type)
                        .ok());
    const auto& const_result =
            assert_cast<const ColumnNullable&>(*const_block.get_by_position(2).column);
    EXPECT_TRUE(const_result.is_null_at(0));
    EXPECT_TRUE(const_result.is_null_at(1));
    const auto& const_nested = assert_cast<const ColumnMap&>(const_result.get_nested_column());
    EXPECT_EQ(const_nested.get_offsets()[0], 0);
    EXPECT_EQ(const_nested.get_offsets()[1], 0);
}

TEST(FunctionMapTest, map_lambda_empty_inputs) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(nullable_int, nullable_int);
    auto int_array_type = std::make_shared<DataTypeArray>(nullable_int);
    auto bool_array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeBool>()));

    {
        Block block;
        block.insert({make_int_array({}, {0, 0}), int_array_type, "keys"});
        block.insert({make_int_array({}, {0, 0}), int_array_type, "values"});
        block.insert({nullptr, map_type, "result"});
        ASSERT_TRUE(execute_map_function("map_from_arrays", block, {0, 1}, 2, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
        EXPECT_EQ(result.get_keys().size(), 0);
        EXPECT_EQ(result.get_offsets()[0], 0);
        EXPECT_EQ(result.get_offsets()[1], 0);
    }

    {
        Block block;
        block.insert({make_int_map({}, {}, {0, 0}), map_type, "map"});
        block.insert({make_bool_array({}, {0, 0}), bool_array_type, "predicate"});
        block.insert({nullptr, map_type, "result"});
        ASSERT_TRUE(execute_map_function("map_filter", block, {0, 1}, 2, map_type).ok());
        const auto& result = assert_cast<const ColumnMap&>(*block.get_by_position(2).column);
        EXPECT_EQ(result.get_keys().size(), 0);
        EXPECT_EQ(result.get_offsets()[0], 0);
        EXPECT_EQ(result.get_offsets()[1], 0);
    }
}

} // namespace doris
