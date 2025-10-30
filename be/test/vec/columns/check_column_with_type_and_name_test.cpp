
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

#include "runtime/primitive_type.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_vector.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(CheckTypeAndColumnMatchTest, test) {
    {
        ColumnWithTypeAndName column_with_type_and_name;
        column_with_type_and_name.name = "test_column";
        column_with_type_and_name.type = std::make_shared<DataTypeInt64>();
        column_with_type_and_name.column = ColumnInt64::create();
        auto status = column_with_type_and_name.check_type_and_column_match();
        EXPECT_TRUE(status.ok()) << status.to_string();
    }
    {
        ColumnWithTypeAndName column_with_type_and_name;
        column_with_type_and_name.name = "test_column";
        column_with_type_and_name.type = std::make_shared<DataTypeInt64>();
        column_with_type_and_name.column = ColumnString::create();
        auto status = column_with_type_and_name.check_type_and_column_match();
        EXPECT_FALSE(status.ok());
    }
    {
        ColumnWithTypeAndName column_with_type_and_name;
        column_with_type_and_name.name = "test_column";
        column_with_type_and_name.type = std::make_shared<DataTypeInt64>();
        column_with_type_and_name.column = ColumnNothing ::create(9);
        auto status = column_with_type_and_name.check_type_and_column_match();
        EXPECT_TRUE(status.ok());
    }
    {
        ColumnWithTypeAndName column_with_type_and_name;
        column_with_type_and_name.name = "test_column";
        column_with_type_and_name.type = nullptr;
        column_with_type_and_name.column = ColumnInt64::create();
        auto status = column_with_type_and_name.check_type_and_column_match();
        EXPECT_FALSE(status.ok());
    }
    {
        ColumnWithTypeAndName column_with_type_and_name;
        column_with_type_and_name.name = "test_column";
        column_with_type_and_name.type = std::make_shared<DataTypeInt64>();
        column_with_type_and_name.column = nullptr;
        auto status = column_with_type_and_name.check_type_and_column_match();
        EXPECT_FALSE(status.ok());
    }
}

} // namespace doris::vectorized
