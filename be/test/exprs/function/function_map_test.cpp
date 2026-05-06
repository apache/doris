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

#include <string>

#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/datetime_ut_util.h"

namespace doris {

namespace {

ColumnPtr create_nullable_timestamptz_column(std::initializer_list<TimestampTzValue> values) {
    auto nested = ColumnTimeStampTz::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        nested->insert_value(value);
        null_map->insert_value(0);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnPtr create_nullable_string_column(std::initializer_list<std::string> values) {
    auto nested = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        nested->insert_data(value.data(), value.size());
        null_map->insert_value(0);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnArray::ColumnOffsets::MutablePtr create_offsets(ColumnArray::Offset64 offset) {
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(offset);
    return offsets;
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

TEST(FunctionMapTest, map_contains_timestamptz) {
    auto timestamptz_type = std::make_shared<DataTypeTimeStampTz>(6);
    auto nullable_timestamptz_type = make_nullable(timestamptz_type);
    auto nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
    auto return_type = std::make_shared<DataTypeBool>();

    auto pre = make_timestamptz(2024, 11, 3, 1, 5, 0, 0);
    auto post = make_timestamptz(2024, 11, 3, 1, 5, 0, 0);

    {
        const std::string func_name = "map_contains_key";
        auto map_type =
                std::make_shared<DataTypeMap>(nullable_timestamptz_type, nullable_string_type);
        auto arguments_template = ColumnsWithTypeAndName {{nullptr, map_type, "map"},
                                                          {nullptr, timestamptz_type, "key"}};
        auto function = SimpleFunctionFactory::instance().get_function(
                func_name, arguments_template, return_type, {true},
                BeExecVersionManager::get_newest_version());
        ASSERT_TRUE(function != nullptr);

        auto probe_column = ColumnTimeStampTz::create();
        probe_column->insert_value(post);

        Block block;
        block.insert({ColumnMap::create(create_nullable_timestamptz_column({pre, post}),
                                        create_nullable_string_column({"pre", "post"}),
                                        create_offsets(2)),
                      map_type, "map"});
        block.insert({std::move(probe_column), timestamptz_type, "key"});
        block.insert({nullptr, return_type, "result"});

        auto status = function->execute(nullptr, block, {0, 1}, 2, 1);
        ASSERT_TRUE(status.ok()) << status.to_string();

        const auto& result_column =
                assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column);
        EXPECT_EQ(result_column.get_element(0), 1);
    }

    {
        const std::string func_name = "map_contains_value";
        auto map_type =
                std::make_shared<DataTypeMap>(nullable_string_type, nullable_timestamptz_type);
        auto arguments_template = ColumnsWithTypeAndName {{nullptr, map_type, "map"},
                                                          {nullptr, timestamptz_type, "value"}};
        auto function = SimpleFunctionFactory::instance().get_function(
                func_name, arguments_template, return_type, {true},
                BeExecVersionManager::get_newest_version());
        ASSERT_TRUE(function != nullptr);

        auto probe_column = ColumnTimeStampTz::create();
        probe_column->insert_value(post);

        Block block;
        block.insert({ColumnMap::create(create_nullable_string_column({"pre", "post"}),
                                        create_nullable_timestamptz_column({pre, post}),
                                        create_offsets(2)),
                      map_type, "map"});
        block.insert({std::move(probe_column), timestamptz_type, "value"});
        block.insert({nullptr, return_type, "result"});

        auto status = function->execute(nullptr, block, {0, 1}, 2, 1);
        ASSERT_TRUE(status.ok()) << status.to_string();

        const auto& result_column =
                assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column);
        EXPECT_EQ(result_column.get_element(0), 1);
    }
}
} // namespace doris