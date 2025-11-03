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

#include <iostream>
#include <memory>

#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

TEST(FunctionStructElementTest, test_return_type) {
    auto index_type = std::make_shared<DataTypeString>();

    auto index_column = ColumnHelper::create_column<DataTypeString>({"key2"});

    DataTypes struct_types = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>(),
                              std::make_shared<DataTypeFloat64>()};

    Strings names = {"key1", "key2", "key3"};

    auto type_struct = std::make_shared<DataTypeStruct>(struct_types, names);

    auto argument_template = ColumnsWithTypeAndName {{nullptr, type_struct, "struct"},
                                                     {index_column, index_type, "index"}};

    auto function = SimpleFunctionFactory::instance().get_function(
            "struct_element", argument_template,
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), {true},
            BeExecVersionManager::get_newest_version());

    EXPECT_TRUE(function != nullptr);

    auto return_type = function->get_return_type();

    EXPECT_EQ("Nullable(INT)", return_type->get_name());
    std::cout << "Return type: " << return_type->get_name() << std::endl;
}

TEST(FunctionStructElementTest, test_return_column) {
    auto index_type = std::make_shared<DataTypeString>();

    ColumnPtr index_column =
            ColumnConst::create(ColumnHelper::create_column<DataTypeString>({"key2"}), 1);

    DataTypes struct_types = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>(),
                              std::make_shared<DataTypeFloat64>()};

    Strings names = {"key1", "key2", "key3"};

    auto type_struct = std::make_shared<DataTypeStruct>(struct_types, names);

    auto argument_template = ColumnsWithTypeAndName {{nullptr, type_struct, "struct"},
                                                     {index_column, index_type, "index"}};

    auto function = SimpleFunctionFactory::instance().get_function(
            "struct_element", argument_template,
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), {true},
            BeExecVersionManager::get_newest_version());

    EXPECT_TRUE(function != nullptr);

    MutableColumns mutable_columns;
    {
        auto col = ColumnString::create();
        col->insert_default();
        mutable_columns.push_back(std::move(col));
    }

    {
        auto col = ColumnInt32::create();
        col->insert_default();
        mutable_columns.push_back(std::move(col));
    }
    {
        auto col = ColumnFloat64::create();
        col->insert_default();
        mutable_columns.push_back(std::move(col));
    }

    Block block;
    auto struct_column = ColumnStruct::create(std::move(mutable_columns));
    block.insert({std::move(struct_column), type_struct, "struct"});
    block.insert({index_column, index_type, "index"});
    block.insert({ColumnInt32::create(0), std::make_shared<DataTypeInt32>(), "result"});

    EXPECT_TRUE(function->execute(nullptr, block, ColumnNumbers {0, 1}, 2, 1));

    EXPECT_TRUE(block.get_by_position(2).column->is_nullable());
}

} // namespace doris::vectorized