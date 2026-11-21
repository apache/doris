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
#include <vector>

#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "exprs/vliteral.h"

namespace doris {
namespace {

void check_const_column(const DataTypePtr& type, const Field& field, size_t logical_size,
                        bool use_default = false) {
    auto expected = type->create_column();
    expected->reserve(1);
    if (use_default) {
        expected->insert_default();
    } else {
        expected->insert(field);
    }

    const auto actual = use_default ? type->create_column_const_with_default_value(logical_size)
                                    : type->create_column_const(logical_size, field);
    const auto* constant = check_and_get_column<ColumnConst>(actual.get());
    ASSERT_NE(constant, nullptr);
    EXPECT_EQ(actual->size(), logical_size);
    EXPECT_EQ(constant->get_data_column().size(), 1);
    Field expected_field;
    Field actual_field;
    expected->get(0, expected_field);
    constant->get_data_column().get(0, actual_field);
    EXPECT_EQ(actual_field, expected_field);
    EXPECT_EQ(constant->get_data_column().allocated_bytes(), expected->allocated_bytes());

    if (logical_size == 8192) {
        auto expanded = actual->convert_to_full_column_if_const();
        ASSERT_EQ(expanded->size(), logical_size);
        Field first;
        Field last;
        expanded->get(0, first);
        expanded->get(logical_size - 1, last);
        EXPECT_EQ(first, expected_field);
        EXPECT_EQ(last, expected_field);
    }
}

TEST(DataTypeConstColumnTest, ScalarAndNullableStorage) {
    auto int_type = std::make_shared<DataTypeInt64>();
    auto float_type = std::make_shared<DataTypeFloat64>();
    auto decimal_type = DataTypeFactory::instance().create_data_type(TYPE_DECIMAL64, false, 18, 2);
    auto string_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    auto nullable_int = make_nullable(int_type);
    auto nullable_string = make_nullable(string_type);

    const std::vector<std::pair<DataTypePtr, Field>> values = {
            {int_type, Field::create_field<TYPE_BIGINT>(Int64(42))},
            {float_type, Field::create_field<TYPE_DOUBLE>(Float64(1.25))},
            {decimal_type, Field::create_field<TYPE_DECIMAL64>(Decimal64(12345))},
            {string_type, Field::create_field<TYPE_STRING>(String("abc"))},
            {nullable_int, Field::create_field<TYPE_BIGINT>(Int64(42))},
            {nullable_int, Field()},
            {nullable_string, Field::create_field<TYPE_STRING>(String("abc"))},
            {nullable_string, Field()},
    };

    for (const auto& [type, field] : values) {
        SCOPED_TRACE(type->get_name());
        SCOPED_TRACE(field.get_type());
        for (size_t size : {size_t(0), size_t(1), size_t(8192)}) {
            check_const_column(type, field, size);
            check_const_column(type, field, size, true);
        }
    }

    for (const DataTypePtr& type : std::vector<DataTypePtr> {int_type, nullable_int}) {
        auto without_reserve = type->create_column();
        without_reserve->insert_default();
        const auto constant = type->create_column_const_with_default_value(1);
        const auto& physical = assert_cast<const ColumnConst&>(*constant).get_data_column();
        EXPECT_LT(physical.allocated_bytes(), without_reserve->allocated_bytes());
    }
}

TEST(DataTypeConstColumnTest, StringLengthsAndEmbeddedZero) {
    auto type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    for (size_t length : {size_t(0), size_t(1), size_t(128), size_t(8192)}) {
        SCOPED_TRACE(length);
        check_const_column(type, Field::create_field<TYPE_STRING>(String(length, 'x')), 1);
    }
    check_const_column(type, Field::create_field<TYPE_STRING>(String("a\0b", 3)), 8192);
}

TEST(DataTypeConstColumnTest, LargeLiteralListHasCompactStorage) {
    auto type = std::make_shared<DataTypeInt64>();
    auto one = type->create_column();
    one->reserve(1);
    one->insert(Field::create_field<TYPE_BIGINT>(Int64(0)));
    const size_t bytes_per_literal = one->allocated_bytes();

    std::vector<std::shared_ptr<VLiteral>> literals;
    literals.reserve(4096);
    size_t total_bytes = 0;
    for (int64_t value = 0; value < 4096; ++value) {
        literals.push_back(VLiteral::create_shared(type, Field::create_field<TYPE_BIGINT>(value)));
        const auto& physical = assert_cast<const ColumnConst&>(*literals.back()->get_column_ptr())
                                       .get_data_column();
        Field actual;
        physical.get(0, actual);
        EXPECT_EQ(actual, Field::create_field<TYPE_BIGINT>(value));
        total_bytes += physical.allocated_bytes();
    }
    EXPECT_EQ(total_bytes, literals.size() * bytes_per_literal);
}

TEST(DataTypeConstColumnTest, ComplexValues) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    auto array_type = std::make_shared<DataTypeArray>(int_type);
    auto map_type = std::make_shared<DataTypeMap>(int_type, string_type);
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, make_nullable(string_type)});

    const auto one = Field::create_field<TYPE_INT>(Int32(1));
    const auto two = Field::create_field<TYPE_INT>(Int32(2));
    const auto text = Field::create_field<TYPE_STRING>(String("value"));
    const std::vector<std::pair<DataTypePtr, Field>> values = {
            {array_type, Field::create_field<TYPE_ARRAY>(Array {})},
            {array_type, Field::create_field<TYPE_ARRAY>(Array {one, Field(), two})},
            {map_type,
             Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {}),
                                                Field::create_field<TYPE_ARRAY>(Array {})})},
            {map_type, Field::create_field<TYPE_MAP>(
                               Map {Field::create_field<TYPE_ARRAY>(Array {one, two}),
                                    Field::create_field<TYPE_ARRAY>(Array {text, text})})},
            {struct_type, Field::create_field<TYPE_STRUCT>(Struct {one, Field()})},
            {struct_type, Field::create_field<TYPE_STRUCT>(Struct {two, text})},
            {make_nullable(array_type), Field()},
    };
    for (const auto& [type, field] : values) {
        SCOPED_TRACE(type->get_name());
        check_const_column(type, field, 8192);
        check_const_column(type, field, 1, true);
    }
}

} // namespace
} // namespace doris
