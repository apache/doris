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

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

template <PrimitiveType PT>
void test_for_scalar_type() {
    typename PrimitiveTypeTraits<PT>::ColumnType column;
    typename PrimitiveTypeTraits<PT>::DataType data_type;
    EXPECT_EQ(column.get_primitive_type(), data_type.get_primitive_type());
}

TEST(ColumnTypeTest, Testscalar) {
    test_for_scalar_type<TYPE_TINYINT>();
    test_for_scalar_type<TYPE_SMALLINT>();
    test_for_scalar_type<TYPE_INT>();
    test_for_scalar_type<TYPE_BIGINT>();
    test_for_scalar_type<TYPE_LARGEINT>();
    test_for_scalar_type<TYPE_FLOAT>();
    test_for_scalar_type<TYPE_DOUBLE>();
    test_for_scalar_type<TYPE_BOOLEAN>();
    test_for_scalar_type<TYPE_VARCHAR>();
    test_for_scalar_type<TYPE_STRING>();
    test_for_scalar_type<TYPE_DATE>();
    test_for_scalar_type<TYPE_DATETIME>();
    test_for_scalar_type<TYPE_DATEV2>();
    test_for_scalar_type<TYPE_DATETIMEV2>();
}

template <PrimitiveType PT>
void test_for_decimal_type() {
    auto type = std::make_shared<typename PrimitiveTypeTraits<PT>::DataType>(9, 3);

    auto column = type->create_column();
    EXPECT_EQ(column->get_primitive_type(), type->get_primitive_type());
}
TEST(ColumnTypeTest, TestDecimal) {
    test_for_decimal_type<TYPE_DECIMAL32>();
    test_for_decimal_type<TYPE_DECIMAL64>();
    test_for_decimal_type<TYPE_DECIMAL128I>();
    test_for_decimal_type<TYPE_DECIMAL256>();
    test_for_decimal_type<TYPE_DECIMALV2>();
}

TEST(ColumnTypeTest, TestNullableConst) {
    auto nullalbe_column = ColumnHelper::create_nullable_column<DataTypeInt64>({1}, {true});
    EXPECT_EQ(nullalbe_column->get_primitive_type(), TYPE_BIGINT);

    auto const_column = ColumnConst::create(nullalbe_column, 1);
    EXPECT_EQ(const_column->get_primitive_type(), TYPE_BIGINT);
}

} // namespace doris::vectorized