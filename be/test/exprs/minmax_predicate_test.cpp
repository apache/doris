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

#include "common/config.h"
#include "exprs/create_predicate_function.h"
#include "gtest/internal/gtest-internal.h"
#include "runtime_filter/utils.h"
#include "testutil/column_helper.h"

namespace doris {
class MinmaxPredicateTest : public testing::Test {
protected:
    MinmaxPredicateTest() {}
    ~MinmaxPredicateTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

template <PrimitiveType primitive_type>
void test_numeric() {
    using NumericType = PrimitiveTypeTraits<primitive_type>::CppType;
    using ColumnType = PrimitiveTypeTraits<primitive_type>::ColumnType;
    auto mix_func = std::make_unique<MinMaxNumFunc<NumericType>>(true);
    NumericType min = type_limit<NumericType>::min();
    NumericType max = type_limit<NumericType>::max();
    NumericType def {};
    if constexpr (std::is_same_v<NumericType, VecDateTimeValue>) {
        def.from_date_str("2010-01-01", strlen("2010-01-01"));
    } else if constexpr (std::is_same_v<NumericType, DateV2Value<DateV2ValueType>>) {
        def.from_date_str("2010-01-01", strlen("2010-01-01"));
    } else if constexpr (std::is_same_v<NumericType, DateV2Value<DateTimeV2ValueType>>) {
        def.from_date_str("2010-01-01", strlen("2010-01-01"));
    }

    vectorized::MutableColumnPtr column;
    if constexpr (vectorized::IsDecimalNumber<NumericType> ||
                  std::is_same_v<NumericType, DecimalV2Value>) {
        column = ColumnType::create(0, 8);
    } else {
        column = ColumnType::create();
    }
    column->reserve(3);
    column->insert_data(reinterpret_cast<const char*>(&min), sizeof(NumericType));
    column->insert_data(reinterpret_cast<const char*>(&max), sizeof(NumericType));
    column->insert_data(reinterpret_cast<const char*>(&def), sizeof(NumericType));
    ASSERT_EQ(column->size(), 3);

    mix_func->insert_fixed_len(column->clone(), 0);
    EXPECT_EQ(min, *(NumericType*)mix_func->get_min());
    EXPECT_EQ(max, *(NumericType*)mix_func->get_max());
}

TEST_F(MinmaxPredicateTest, Numeric) {
    test_numeric<PrimitiveType::TYPE_TINYINT>();
    test_numeric<PrimitiveType::TYPE_SMALLINT>();
    test_numeric<PrimitiveType::TYPE_INT>();
    test_numeric<PrimitiveType::TYPE_BIGINT>();
    test_numeric<PrimitiveType::TYPE_LARGEINT>();
    test_numeric<PrimitiveType::TYPE_FLOAT>();
    test_numeric<PrimitiveType::TYPE_DOUBLE>();
    test_numeric<PrimitiveType::TYPE_IPV4>();
    test_numeric<PrimitiveType::TYPE_IPV6>();
    test_numeric<PrimitiveType::TYPE_DECIMAL256>();
    test_numeric<PrimitiveType::TYPE_DECIMALV2>();
    test_numeric<PrimitiveType::TYPE_DECIMAL32>();
    test_numeric<PrimitiveType::TYPE_DECIMAL64>();
    test_numeric<PrimitiveType::TYPE_DECIMAL128I>();

    test_numeric<PrimitiveType::TYPE_DATE>();
    test_numeric<PrimitiveType::TYPE_DATEV2>();
    test_numeric<PrimitiveType::TYPE_DATETIME>();
    test_numeric<PrimitiveType::TYPE_DATETIMEV2>();
}

TEST_F(MinmaxPredicateTest, InsertFixedLen) {
    auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 5, 6, 7, 8});

    MinMaxNumFunc<int32_t> minmax_num_func(true);
    minmax_num_func.insert_fixed_len(column, 0);
    ASSERT_EQ(1, *(int32_t*)minmax_num_func.get_min());
    ASSERT_EQ(8, *(int32_t*)minmax_num_func.get_max());

    auto nullable_column = vectorized::ColumnNullable::create(
            column->clone(), vectorized::ColumnUInt8::create(column->size(), 0));
    minmax_num_func.insert_fixed_len(nullable_column->clone(), 0);
    ASSERT_EQ(1, *(int32_t*)minmax_num_func.get_min());
    ASSERT_EQ(8, *(int32_t*)minmax_num_func.get_max());

    nullable_column->get_null_map_data()[1] = 1;
    nullable_column->get_null_map_data()[3] = 1;
    nullable_column->get_null_map_data()[6] = 1;

    minmax_num_func.insert_fixed_len(nullable_column->clone(), 0);
    ASSERT_EQ(1, *(int32_t*)minmax_num_func.get_min());
    ASSERT_EQ(8, *(int32_t*)minmax_num_func.get_max());
}

TEST_F(MinmaxPredicateTest, String) {
    auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeString>(
            {"ab", "cd", "ef", "gh", "ij", "kl", "mn", "op"});

    MinMaxNumFunc<std::string> minmax_num_func(true);

    minmax_num_func.insert_fixed_len(column, 0);
    ASSERT_EQ("ab", *(std::string*)minmax_num_func.get_min());
    ASSERT_EQ("op", *(std::string*)minmax_num_func.get_max());

    auto nullable_column = vectorized::ColumnNullable::create(
            column->clone(), vectorized::ColumnUInt8::create(column->size(), 0));
    minmax_num_func.insert_fixed_len(nullable_column->clone(), 0);
    ASSERT_EQ("ab", *(std::string*)minmax_num_func.get_min());
    ASSERT_EQ("op", *(std::string*)minmax_num_func.get_max());

    nullable_column->get_null_map_data()[1] = 1;
    nullable_column->get_null_map_data()[3] = 1;
    nullable_column->get_null_map_data()[6] = 1;

    minmax_num_func.insert_fixed_len(nullable_column->clone(), 0);
    ASSERT_EQ("ab", *(std::string*)minmax_num_func.get_min());
    ASSERT_EQ("op", *(std::string*)minmax_num_func.get_max());

    auto string_overflow_size = config::string_overflow_size;
    config::string_overflow_size = 10;
    Defer defer([string_overflow_size]() { config::string_overflow_size = string_overflow_size; });

    auto string64_column = column->clone()->convert_column_if_overflow();
    ASSERT_TRUE(string64_column->is_column_string64());

    MinMaxNumFunc<std::string> minmax_num_func2(true);
    minmax_num_func2.insert_fixed_len(string64_column, 0);
    ASSERT_EQ("ab", *(std::string*)minmax_num_func2.get_min());
    ASSERT_EQ("op", *(std::string*)minmax_num_func2.get_max());

    auto nullable_column2 = vectorized::ColumnNullable::create(
            column->clone(), vectorized::ColumnUInt8::create(column->size(), 0));
    nullable_column2->get_null_map_data()[1] = 1;
    nullable_column2->get_null_map_data()[3] = 1;
    nullable_column2->get_null_map_data()[6] = 1;
    minmax_num_func2.insert_fixed_len(nullable_column2->clone(), 0);
    ASSERT_EQ("ab", *(std::string*)minmax_num_func2.get_min());
    ASSERT_EQ("op", *(std::string*)minmax_num_func2.get_max());
}
} // namespace doris