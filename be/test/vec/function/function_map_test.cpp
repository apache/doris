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

#include "function_test_util.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_map.h"

namespace doris::vectorized {

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
} // namespace doris::vectorized