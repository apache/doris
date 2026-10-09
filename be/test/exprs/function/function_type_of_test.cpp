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

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/function_utils.h"

namespace doris {
namespace {

constexpr size_t kRows = 3;

std::string execute_typeof(const DataTypePtr& input_type, size_t rows = kRows,
                           bool constant = false, const DataTypePtr& declared_type = nullptr) {
    ColumnPtr input_column;
    if (constant) {
        input_column = input_type->create_column_const_with_default_value(rows);
    } else {
        auto values = input_type->create_column();
        values->insert_many_defaults(rows);
        input_column = std::move(values);
    }
    const DataTypePtr argument_type = declared_type ? declared_type : input_type;
    const DataTypePtr result_type = std::make_shared<DataTypeString>();

    Block block {{std::move(input_column), input_type, "input"},
                 {result_type->create_column(), result_type, "result"}};
    FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
            "typeof", {{block.get_by_position(0).column, argument_type, "input"}}, result_type);
    EXPECT_NE(function, nullptr);
    if (function == nullptr) {
        return {};
    }

    EXPECT_FALSE(function->get_return_type()->is_nullable());
    FunctionUtils function_utils(result_type, {argument_type}, false);
    FunctionContext* context = function_utils.get_fn_ctx();
    EXPECT_TRUE(function->execute(context, block, {0}, 1, rows).ok());

    const auto& result = assert_cast<const ColumnConst&>(*block.get_by_position(1).column);
    EXPECT_EQ(result.size(), rows);
    const auto& value = assert_cast<const ColumnString&>(result.get_data_column());
    return value.get_data_at(0).to_string();
}

} // namespace

TEST(FunctionTypeOfTest, FunctionIsRegisteredAndReturnsConstString) {
    const auto input_type = std::make_shared<DataTypeInt32>();
    const auto result_type = std::make_shared<DataTypeString>();
    const ColumnsWithTypeAndName arguments {{input_type->create_column(), input_type, "input"}};

    const FunctionBasePtr function =
            SimpleFunctionFactory::instance().get_function("typeof", arguments, result_type);
    ASSERT_NE(function, nullptr);
    EXPECT_EQ(function->get_return_type()->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(execute_typeof(input_type), "integer");
}

TEST(FunctionTypeOfTest, PrimitiveAndNullableTypes) {
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeNothing>()), "unknown");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeBool>()), "boolean");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeInt8>()), "tinyint");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeInt16>()), "smallint");
    EXPECT_EQ(execute_typeof(make_nullable(std::make_shared<DataTypeInt32>())), "integer");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeInt64>()), "bigint");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeInt128>()), "decimal(38,0)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeFloat32>()), "real");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeFloat64>()), "double");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeString>()), "varchar");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeString>(10, TYPE_VARCHAR)), "varchar(10)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeString>(0, TYPE_VARCHAR)), "varchar(0)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeString>(4, TYPE_CHAR)), "char(4)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDecimal32>(5, 1)), "decimal(5,1)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDecimal64>(12, 3)), "decimal(12,3)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDecimal128>(38, 10)), "decimal(38,10)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDecimal256>(76, 20)), "decimal(76,20)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDecimalV2>(27, 9, 12, 3)), "decimal(12,3)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDate>()), "date");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDateV2>()), "date");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDateTime>()), "timestamp");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeDateTimeV2>()), "timestamp");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeTimeStampNs>()), "timestamp");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeTimeStampTz>()), "timestamp with time zone");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeTimeV2>()), "time");
}

TEST(FunctionTypeOfTest, NestedAndSpecialTypes) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeArray>(make_nullable(int_type))),
              "array(integer)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeMap>(string_type, int_type)),
              "map(varchar, integer)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type})),
              "row(integer, varchar)");
    const auto row_type = std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type},
                                                           Strings {"id", "name"});
    EXPECT_EQ(execute_typeof(row_type), "row(\"id\" integer, \"name\" varchar)");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeArray>(make_nullable(row_type))),
              "array(row(\"id\" integer, \"name\" varchar))");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeVarbinary>()), "varbinary");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeJsonb>()), "json");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeVariant>()), "variant");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeBitMap>()), "bitmap");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeHLL>()), "hll");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeQuantileState>()), "quantile_state");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeIPv4>()), "ipv4");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeIPv6>()), "ipv6");
}

TEST(FunctionTypeOfTest, NullLiteralAndConstantInputs) {
    const auto null_type = DataTypeFactory::instance().create_data_type(TYPE_NULL, true);
    ASSERT_TRUE(null_type->is_null_literal());
    EXPECT_EQ(execute_typeof(null_type), "unknown");
    EXPECT_EQ(execute_typeof(null_type, kRows, true), "unknown");
    EXPECT_EQ(execute_typeof(make_nullable(std::make_shared<DataTypeNothing>())), "unknown");
    EXPECT_EQ(execute_typeof(make_nullable(std::make_shared<DataTypeInt32>()), kRows, true),
              "integer");
    EXPECT_EQ(execute_typeof(std::make_shared<DataTypeString>(10, TYPE_VARCHAR), kRows, true),
              "varchar(10)");
}

TEST(FunctionTypeOfTest, EmptyBlocks) {
    // With zero rows there is no constant result value to compare, so only the
    // requirement that execution succeeds and yields an empty column is checked.
    const std::vector<DataTypePtr> empty_inputs = {
            std::make_shared<DataTypeInt32>(),
            make_nullable(std::make_shared<DataTypeString>())};
    for (const auto& input_type : empty_inputs) {
        ColumnPtr input_column = input_type->create_column();
        const DataTypePtr result_type = std::make_shared<DataTypeString>();
        Block block {{std::move(input_column), input_type, "input"},
                     {result_type->create_column(), result_type, "result"}};
        FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
                "typeof", {{block.get_by_position(0).column, input_type, "input"}}, result_type);
        ASSERT_NE(function, nullptr);
        FunctionUtils function_utils(result_type, {input_type}, false);
        EXPECT_TRUE(function->execute(function_utils.get_fn_ctx(), block, {0}, 1, 0).ok());
        EXPECT_EQ(block.get_by_position(1).column->size(), 0);
    }
}

TEST(FunctionTypeOfTest, UsesAnalyzedTypeInsteadOfStorageType) {
    const auto storage_type = make_nullable(std::make_shared<DataTypeString>(-1, TYPE_VARCHAR));
    const auto declared_type = make_nullable(std::make_shared<DataTypeString>(10, TYPE_VARCHAR));
    EXPECT_EQ(execute_typeof(storage_type, kRows, false, declared_type), "varchar(10)");
}

} // namespace doris
