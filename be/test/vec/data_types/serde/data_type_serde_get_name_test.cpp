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

#include "runtime/primitive_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

TEST(DataTypeSerDeGetNameTest, test) {
    {
        auto type = std::make_shared<DataTypeInt64>();
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "BIGINT");
    }

    {
        auto type = std::make_shared<DataTypeString>();
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "String");
    }

    {
        auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "Array(BIGINT)");
    }

    {
        auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                                  std::make_shared<DataTypeInt64>());
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "Map(String, BIGINT)");
    }

    {
        auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "Nullable(BIGINT)");
    }

    {
        auto type = std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "Array(Nullable(BIGINT))");
    }

    {
        auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                                  std::make_shared<DataTypeInt64>());
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "Map(String, BIGINT)");
    }

    {
        auto type = std::make_shared<DataTypeMap>(
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
                std::make_shared<DataTypeInt64>());
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "Map(Nullable(String), BIGINT)");
    }

    {
        auto type = std::make_shared<DataTypeMap>(
                std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "Map(String, Nullable(BIGINT))");
    }

    {
        auto type = std::make_shared<DataTypeMap>(
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));
        auto serde = type->get_serde();
        EXPECT_EQ(serde->get_name(), "Map(Nullable(String), Nullable(BIGINT))");
    }

    {
        DataTypes types = {std::make_shared<DataTypeString>(),
                           std::make_shared<DataTypeInt64>(),
                           std::make_shared<DataTypeFloat64>(),
                           std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()),
                           std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                                         std::make_shared<DataTypeInt64>()),
                           std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
                           std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())};

        Strings names = {"field1", "field2", "field3", "field4", "field5", "field6", "field7"};
        auto type = std::make_shared<DataTypeStruct>(types, names);
        auto serde = type->get_serde();
        EXPECT_EQ(
                serde->get_name(),
                R"(Struct(field1:String, field2:BIGINT, field3:DOUBLE, field4:Array(INT), field5:Map(String, BIGINT), field6:Nullable(String), field7:Nullable(BIGINT)))");
    }
}

} // namespace doris::vectorized