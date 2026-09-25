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

#include <memory>
#include <string>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/typeid_cast.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

TEST(FunctionStructTest, requiredArgumentForRequiredFieldSucceeds) {
    auto integer_type = std::make_shared<DataTypeInt64>();
    auto result_type = std::make_shared<DataTypeStruct>(DataTypes {integer_type});
    ColumnsWithTypeAndName argument_types {{nullptr, integer_type, "value"}};
    auto function =
            SimpleFunctionFactory::instance().get_function("struct", argument_types, result_type);
    ASSERT_NE(function, nullptr);

    auto values = ColumnInt64::create();
    values->insert_value(1);
    values->insert_value(2);

    Block block;
    block.insert({std::move(values), integer_type, "value"});
    block.insert({result_type->create_column(), result_type, "result"});

    Status status = function->execute(nullptr, block, ColumnNumbers {0}, 1, 2);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& result = assert_cast<const ColumnStruct&>(*block.get_by_position(1).column);
    EXPECT_FALSE(result.get_column(0).is_nullable());
    const auto& result_values = assert_cast<const ColumnInt64&>(result.get_column(0));
    EXPECT_EQ(result_values.get_data()[0], 1);
    EXPECT_EQ(result_values.get_data()[1], 2);
}

TEST(FunctionStructTest, requiredConstArgumentForNullableFieldIsExpandedAndWrapped) {
    auto integer_type = std::make_shared<DataTypeInt64>();
    auto nullable_integer_type = std::make_shared<DataTypeNullable>(integer_type);
    auto result_type = std::make_shared<DataTypeStruct>(DataTypes {nullable_integer_type});
    ColumnsWithTypeAndName argument_types {{nullptr, integer_type, "value"}};
    auto function =
            SimpleFunctionFactory::instance().get_function("struct", argument_types, result_type);
    ASSERT_NE(function, nullptr);

    auto value = ColumnInt64::create();
    value->insert_value(7);

    Block block;
    block.insert({ColumnConst::create(std::move(value), 3), integer_type, "value"});
    block.insert({result_type->create_column(), result_type, "result"});

    Status status = function->execute(nullptr, block, ColumnNumbers {0}, 1, 3);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& result_column = block.get_by_position(1).column;
    ASSERT_TRUE(is_column_const(*result_column));
    auto full_result_column = result_column->convert_to_full_column_if_const();
    const auto& result = assert_cast<const ColumnStruct&>(*full_result_column);
    const auto& nullable_result = assert_cast<const ColumnNullable&>(result.get_column(0));
    EXPECT_EQ(nullable_result.size(), 3);
    const auto& result_null_map = nullable_result.get_null_map_data();
    EXPECT_EQ(result_null_map[0], 0);
    EXPECT_EQ(result_null_map[1], 0);
    EXPECT_EQ(result_null_map[2], 0);
    const auto& result_values =
            assert_cast<const ColumnInt64&>(nullable_result.get_nested_column());
    EXPECT_EQ(result_values.get_data()[0], 7);
    EXPECT_EQ(result_values.get_data()[1], 7);
    EXPECT_EQ(result_values.get_data()[2], 7);
}

TEST(FunctionStructTest, nullableArgumentForNullableFieldSucceeds) {
    auto integer_type = std::make_shared<DataTypeInt64>();
    auto nullable_integer_type = std::make_shared<DataTypeNullable>(integer_type);
    auto result_type = std::make_shared<DataTypeStruct>(DataTypes {nullable_integer_type});
    ColumnsWithTypeAndName argument_types {{nullptr, nullable_integer_type, "value"}};
    auto function =
            SimpleFunctionFactory::instance().get_function("struct", argument_types, result_type);
    ASSERT_NE(function, nullptr);

    auto values = ColumnInt64::create();
    values->insert_value(11);
    values->insert_value(22);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);

    Block block;
    block.insert({ColumnNullable::create(std::move(values), std::move(null_map)),
                  nullable_integer_type, "value"});
    block.insert({result_type->create_column(), result_type, "result"});

    Status status = function->execute(nullptr, block, ColumnNumbers {0}, 1, 2);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& result = assert_cast<const ColumnStruct&>(*block.get_by_position(1).column);
    const auto& nullable_result = assert_cast<const ColumnNullable&>(result.get_column(0));
    const auto& result_null_map = nullable_result.get_null_map_data();
    EXPECT_EQ(result_null_map[0], 0);
    EXPECT_EQ(result_null_map[1], 1);
}

TEST(FunctionStructTest, nullableArgumentForRequiredFieldReturnsStatus) {
    auto integer_type = std::make_shared<DataTypeInt64>();
    auto result_type = std::make_shared<DataTypeStruct>(DataTypes {integer_type});
    ColumnsWithTypeAndName argument_types {{nullptr, integer_type, "value"}};
    auto function =
            SimpleFunctionFactory::instance().get_function("struct", argument_types, result_type);
    ASSERT_NE(function, nullptr);

    auto values = ColumnInt64::create();
    values->insert_value(1);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    auto nullable_values = ColumnNullable::create(std::move(values), std::move(null_map));

    Block block;
    // Deliberately model stale FE metadata: the block type is required while the produced column
    // is nullable. The function must report the mismatch rather than abort in insert_range_from.
    block.insert({std::move(nullable_values), integer_type, "value"});
    block.insert({result_type->create_column(), result_type, "result"});

    Status status = function->execute(nullptr, block, ColumnNumbers {0}, 1, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("non-nullable"), std::string::npos);
}

} // namespace doris
