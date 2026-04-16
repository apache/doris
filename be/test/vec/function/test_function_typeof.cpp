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

#include "runtime/primitive_type.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_varbinary.h"
#include "vec/functions/function_typeof.h"

namespace doris::vectorized {

class FunctionTypeOfTest : public testing::Test {
protected:
    FunctionPtr function = FunctionTypeOf::create();

    std::string execute_typeof(const DataTypePtr& argument_type) {
        Block block;
        block.insert({argument_type->create_column_const(1, argument_type->get_default()),
                      argument_type, "arg"});
        block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});

        ColumnNumbers arguments = {0};
        EXPECT_TRUE(function->execute_impl(nullptr, block, arguments, 1, 1).ok());

        const auto* result_column =
                assert_cast<const ColumnConst*>(block.get_by_position(1).column.get());
        const auto* string_column =
                assert_cast<const ColumnString*>(&result_column->get_data_column());
        return string_column->get_data_at(0).to_string();
    }
};

TEST_F(FunctionTypeOfTest, FormatsScalarTypes) {
    EXPECT_EQ("int", execute_typeof(std::make_shared<DataTypeInt32>()));
    EXPECT_EQ("boolean", execute_typeof(std::make_shared<DataTypeUInt8>()));
    EXPECT_EQ("datev2", execute_typeof(std::make_shared<DataTypeDateV2>()));
    EXPECT_EQ("datetimev2(3)", execute_typeof(std::make_shared<DataTypeDateTimeV2>(3)));
}

TEST_F(FunctionTypeOfTest, FormatsStringFamilies) {
    EXPECT_EQ("varchar", execute_typeof(std::make_shared<DataTypeString>()));
    EXPECT_EQ("varchar(10)", execute_typeof(std::make_shared<DataTypeString>(10, TYPE_VARCHAR)));
    EXPECT_EQ("char(5)", execute_typeof(std::make_shared<DataTypeString>(5, TYPE_CHAR)));
    EXPECT_EQ("varbinary", execute_typeof(std::make_shared<DataTypeVarbinary>()));
}

TEST_F(FunctionTypeOfTest, FormatsDecimalTypes) {
    EXPECT_EQ("decimalv2(10,2)", execute_typeof(std::make_shared<DataTypeDecimalV2>(10, 2, 10, 2)));
    EXPECT_EQ("decimal(9,2)", execute_typeof(std::make_shared<DataTypeDecimal32>(9, 2)));
    EXPECT_EQ("decimal(18,6)", execute_typeof(std::make_shared<DataTypeDecimal64>(18, 6)));
}

TEST_F(FunctionTypeOfTest, FormatsComplexTypesRecursively) {
    auto varchar10 = std::make_shared<DataTypeString>(10, TYPE_VARCHAR);
    auto decimal = std::make_shared<DataTypeDecimal32>(9, 2);
    auto nullable_decimal = make_nullable(decimal);

    EXPECT_EQ("array(null)",
              execute_typeof(std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>())));
    EXPECT_EQ("map(varchar(10),decimal(9,2))",
              execute_typeof(std::make_shared<DataTypeMap>(varchar10, nullable_decimal)));

    DataTypes fields {std::make_shared<DataTypeInt32>(), varchar10};
    std::vector<std::string> names {"id", "name"};
    EXPECT_EQ("struct<id:int,name:varchar(10)>",
              execute_typeof(std::make_shared<DataTypeStruct>(fields, names)));
}

TEST_F(FunctionTypeOfTest, HandlesNullableAndNullTypes) {
    EXPECT_EQ("int", execute_typeof(make_nullable(std::make_shared<DataTypeInt32>())));
    EXPECT_EQ("null", execute_typeof(make_nullable(std::make_shared<DataTypeNothing>())));
}

TEST_F(FunctionTypeOfTest, ExposesExpectedFunctionProperties) {
    EXPECT_EQ("typeof", function->get_name());
    EXPECT_EQ(1, function->get_number_of_arguments());
    EXPECT_FALSE(function->use_default_implementation_for_nulls());
}

} // namespace doris::vectorized
