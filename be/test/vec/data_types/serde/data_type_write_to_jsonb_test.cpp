
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
#include "testutil/column_helper.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/cast/cast_base.h"

namespace doris::vectorized {

std::string to_string(JsonbWriter& writer) {
    const auto* ptr = writer.getOutput()->getBuffer();
    auto len = writer.getOutput()->getSize();
    return JsonbToJson::jsonb_to_json_string(ptr, len);
}

std::string to_string(const JsonbValue* jsonb_value) {
    return JsonbToJson {}.to_json_string(jsonb_value);
}

TEST(DataTypeWritToJsonb, test_number) {
    CastParameters params;
    params.is_strict = false;
    {
        auto data = ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3, 4, 5});
        JsonbWriter writer;
        EXPECT_TRUE(data.type->get_serde()->serialize_column_to_jsonb(*data.column, 3, writer));
        EXPECT_EQ(to_string(writer), "4");

        auto data_to = ColumnInt32::create();
        EXPECT_TRUE(data.type->get_serde()->deserialize_column_from_jsonb(
                *data_to, writer.getValue(), params));
        EXPECT_EQ(data_to->get_int(0), 4);
    }

    {
        auto data = ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 5});
        JsonbWriter writer;
        EXPECT_TRUE(data.type->get_serde()->serialize_column_to_jsonb(*data.column, 3, writer));
        EXPECT_EQ(to_string(writer), "4");
        auto data_to = ColumnInt64::create();
        EXPECT_TRUE(data.type->get_serde()->deserialize_column_from_jsonb(
                *data_to, writer.getValue(), params));
        EXPECT_EQ(data_to->get_int(0), 4);
    }

    {
        auto data = ColumnHelper::create_column_with_name<DataTypeFloat32>(
                {1.1F, 2.2F, 3.3F, 4.4F, 5.5F});
        JsonbWriter writer;
        EXPECT_TRUE(data.type->get_serde()->serialize_column_to_jsonb(*data.column, 3, writer));
        EXPECT_EQ(to_string(writer), "4.40000009536743");

        auto data_to = ColumnFloat32::create();
        EXPECT_TRUE(data.type->get_serde()->deserialize_column_from_jsonb(
                *data_to, writer.getValue(), params));
        EXPECT_FLOAT_EQ(data_to->get_element(0), 4.4F);
    }

    {
        auto col = ColumnDecimal128V3::create(0, 2);
        Decimal128V3 val = 12345;
        col->insert_value(val);
        auto type = std::make_shared<DataTypeDecimal128>(18, 2);
        JsonbWriter writer;
        EXPECT_TRUE(type->get_serde()->serialize_column_to_jsonb(*col, 0, writer));
        EXPECT_EQ(to_string(writer), "123.45");

        auto data_to = ColumnDecimal128V3::create(0, 2);
        EXPECT_TRUE(type->get_serde()->deserialize_column_from_jsonb(*data_to, writer.getValue(),
                                                                     params));
        EXPECT_EQ(data_to->get_element(0), val);
    }
}

TEST(DataTypeWritToJsonb, test_string) {
    CastParameters params;
    params.is_strict = false;
    {
        auto data = ColumnHelper::create_column_with_name<DataTypeString>(
                {"hello", "world", "doris", "vectorized", "test"});
        JsonbWriter writer;
        EXPECT_TRUE(data.type->get_serde()->serialize_column_to_jsonb(*data.column, 3, writer));
        EXPECT_EQ(to_string(writer), "\"vectorized\"");

        auto data_to = ColumnString::create();
        EXPECT_TRUE(data.type->get_serde()->deserialize_column_from_jsonb(
                *data_to, writer.getValue(), params));
        EXPECT_EQ(data_to->get_data_at(0).to_string(), "vectorized");
    }
}

TEST(DataTypeWritToJsonb, test_array) {
    CastParameters params;
    params.is_strict = false;
    {
        auto col_int32 = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 4, 5},
                                                                             {0, 0, 0, 1, 0});

        auto col_offset = ColumnOffset64::create();
        col_offset->insert_value(3);
        col_offset->insert_value(5);

        auto col_array = ColumnArray::create(col_int32, std::move(col_offset));

        auto type = std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()));

        {
            JsonbWriter writer;
            EXPECT_TRUE(type->get_serde()->serialize_column_to_jsonb(*col_array, 0, writer));
            EXPECT_EQ(to_string(writer), "[1,2,3]");
            auto col_array_to = col_array->clone_empty();
            EXPECT_TRUE(type->get_serde()->deserialize_column_from_jsonb(
                    *col_array_to, writer.getValue(), params));
            std::cout << type->to_string(*col_array_to, 0) << std::endl;
            EXPECT_EQ("[1, 2, 3]", type->to_string(*col_array_to, 0));
        }

        {
            JsonbWriter writer;
            EXPECT_TRUE(type->get_serde()->serialize_column_to_jsonb(*col_array, 1, writer));
            EXPECT_EQ(to_string(writer), "[null,5]");
            auto col_array_to = col_array->clone_empty();
            EXPECT_TRUE(type->get_serde()->deserialize_column_from_jsonb(
                    *col_array_to, writer.getValue(), params));
            std::cout << type->to_string(*col_array_to, 0) << std::endl;
            EXPECT_EQ("[NULL, 5]", type->to_string(*col_array_to, 0));
        }
    }
}

TEST(DataTypeWritToJsonb, test_struct) {
    CastParameters params;
    params.is_strict = false;
    {
        auto col_int32 = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 4, 5},
                                                                             {0, 0, 0, 1, 0});

        auto col_string = ColumnHelper::create_nullable_column<DataTypeString>(
                {"hello", "world", "doris", "vectorized", "test"}, {0, 0, 1, 0, 0});

        Columns columns;
        columns.push_back(col_int32);
        columns.push_back(col_string);

        auto col_struct = ColumnStruct::create(std::move(columns));

        auto type = std::make_shared<DataTypeStruct>(
                DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
                           std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
                Strings {"int_col", "string_col"});

        {
            JsonbWriter writer;
            EXPECT_TRUE(type->get_serde()->serialize_column_to_jsonb(*col_struct, 0, writer));
            EXPECT_EQ(to_string(writer), "{\"int_col\":1,\"string_col\":\"hello\"}");
            auto col_struct_to = col_struct->clone_empty();
            EXPECT_TRUE(type->get_serde()->deserialize_column_from_jsonb(
                    *col_struct_to, writer.getValue(), params));
            std::cout << type->to_string(*col_struct_to, 0) << std::endl;
            EXPECT_EQ("{1, hello}", type->to_string(*col_struct_to, 0));
        }

        {
            JsonbWriter writer;
            EXPECT_TRUE(type->get_serde()->serialize_column_to_jsonb(*col_struct, 1, writer));
            EXPECT_EQ(to_string(writer), "{\"int_col\":2,\"string_col\":\"world\"}");
            auto col_struct_to = col_struct->clone_empty();
            EXPECT_TRUE(type->get_serde()->deserialize_column_from_jsonb(
                    *col_struct_to, writer.getValue(), params));
            std::cout << type->to_string(*col_struct_to, 0) << std::endl;
            EXPECT_EQ("{2, world}", type->to_string(*col_struct_to, 0));
        }

        {
            JsonbWriter writer;
            EXPECT_TRUE(type->get_serde()->serialize_column_to_jsonb(*col_struct, 2, writer));
            EXPECT_EQ(to_string(writer), "{\"int_col\":3,\"string_col\":null}");
            auto col_struct_to = col_struct->clone_empty();
            EXPECT_TRUE(type->get_serde()->deserialize_column_from_jsonb(
                    *col_struct_to, writer.getValue(), params));
            std::cout << type->to_string(*col_struct_to, 0) << std::endl;
            EXPECT_EQ("{3, NULL}", type->to_string(*col_struct_to, 0));
        }

        {
            JsonbWriter writer;
            EXPECT_TRUE(type->get_serde()->serialize_column_to_jsonb(*col_struct, 3, writer));
            EXPECT_EQ(to_string(writer), "{\"int_col\":null,\"string_col\":\"vectorized\"}");
            auto col_struct_to = col_struct->clone_empty();
            EXPECT_TRUE(type->get_serde()->deserialize_column_from_jsonb(
                    *col_struct_to, writer.getValue(), params));
            std::cout << type->to_string(*col_struct_to, 0) << std::endl;
            EXPECT_EQ("{NULL, vectorized}", type->to_string(*col_struct_to, 0));
        }

        {
            JsonbWriter writer;
            EXPECT_TRUE(type->get_serde()->serialize_column_to_jsonb(*col_struct, 4, writer));
            EXPECT_EQ(to_string(writer), "{\"int_col\":5,\"string_col\":\"test\"}");
            auto col_struct_to = col_struct->clone_empty();
            EXPECT_TRUE(type->get_serde()->deserialize_column_from_jsonb(
                    *col_struct_to, writer.getValue(), params));
            std::cout << type->to_string(*col_struct_to, 0) << std::endl;
            EXPECT_EQ("{5, test}", type->to_string(*col_struct_to, 0));
        }
    }
}

} // namespace doris::vectorized