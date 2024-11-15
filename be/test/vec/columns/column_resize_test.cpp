
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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_struct.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

DataTypes _create_scala_data_types() {
    DataTypePtr dt = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDate>());
    DataTypePtr dc = std::make_shared<DataTypeNullable>(vectorized::create_decimal(10, 2, false));
    DataTypePtr dcv2 = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeDecimal<vectorized::Decimal128>>(27, 9));
    DataTypePtr n3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
    DataTypePtr n1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    DataTypePtr s1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

    DataTypes dataTypes;
    dataTypes.push_back(dt);
    dataTypes.push_back(d);
    dataTypes.push_back(dc);
    dataTypes.push_back(dcv2);
    dataTypes.push_back(n3);
    dataTypes.push_back(n1);
    dataTypes.push_back(s1);

    return dataTypes;
}

TEST(ResizeTest, ScalaTypeTest) {
    DataTypes dataTypes = _create_scala_data_types();

    for (auto d : dataTypes) {
        DataTypePtr a = std::make_shared<DataTypeArray>(d);
        auto col = a->create_column();
        col->resize(10);
        MutableColumnPtr b = a->create_column();
        b->insert_range_from(*col, 0, 10);
        EXPECT_EQ(b->size(), 10);
    }
}

TEST(ResizeTest, ArrayTypeTest) {
    DataTypes dataTypes = _create_scala_data_types();

    for (auto d : dataTypes) {
        DataTypePtr a = std::make_shared<DataTypeArray>(d);
        auto col_a = a->create_column();
        col_a->resize(10);
        MutableColumnPtr b = a->create_column();
        b->insert_range_from(*col_a, 0, 10);
        EXPECT_EQ(b->size(), 10);
        ColumnArray* col_array = reinterpret_cast<ColumnArray*>(b.get());
        for (int i = 0; i < col_array->get_offsets().size(); ++i) {
            EXPECT_EQ(0, col_array->get_offsets()[i]);
        }
    }
}

TEST(ResizeTest, ArrayTypeWithValuesTest) {
    DataTypePtr d = std::make_shared<DataTypeInt64>();

    DataTypePtr a = std::make_shared<DataTypeArray>(d);
    auto col_a = a->create_column();
    Array af;
    af.push_back(Int64(1));
    af.push_back(Int64(2));
    af.push_back(Int64(3));
    col_a->insert(af);
    col_a->insert(af);

    col_a->resize(10);
    MutableColumnPtr b = a->create_column();
    b->insert_range_from(*col_a, 0, 10);
    EXPECT_EQ(b->size(), 10);
    ColumnArray* col_array = reinterpret_cast<ColumnArray*>(b.get());
    EXPECT_EQ(col_array->get_offsets()[0], 3);
    for (int i = 1; i < col_array->get_offsets().size(); ++i) {
        EXPECT_EQ(6, col_array->get_offsets()[i]);
    }
}

TEST(ResizeTest, MapTypeTest) {
    DataTypes dataTypes = _create_scala_data_types();

    // data_type_map
    for (int i = 0; i < dataTypes.size() - 1; ++i) {
        DataTypePtr a = std::make_shared<DataTypeMap>(dataTypes[i], dataTypes[i + 1]);
        auto col_a = a->create_column();
        col_a->resize(10);
        MutableColumnPtr b = a->create_column();
        b->insert_range_from(*col_a, 0, 10);
        EXPECT_EQ(b->size(), 10);
        ColumnMap* col_map = reinterpret_cast<ColumnMap*>(b.get());
        for (int i = 0; i < col_map->get_offsets().size(); ++i) {
            EXPECT_EQ(0, col_map->get_offsets()[i]);
        }
    }
}

TEST(ResizeTest, StructTypeTest) {
    DataTypes dataTypes = _create_scala_data_types();

    DataTypePtr a = std::make_shared<DataTypeStruct>(dataTypes);
    auto col_a = a->create_column();
    col_a->resize(10);
    MutableColumnPtr b = a->create_column();
    b->insert_range_from(*col_a, 0, 10);
    EXPECT_EQ(b->size(), 10);
}

TEST(ResizeTest, ResizeColumnArray) {
    ColumnArray::MutablePtr col_arr =
            ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
    Array array1 = {1, 2, 3};
    Array array2 = {4};
    col_arr->insert(array1);
    col_arr->insert(Array());
    col_arr->insert(array2);

    // check column array result
    // array : [[1,2,3],[],[4]]
    col_arr->resize(2);
    EXPECT_EQ(col_arr->size(), 2);
    auto data_col = assert_cast<const ColumnArray&>(*col_arr).get_data_ptr();
    EXPECT_EQ(data_col->size(), 3);
    auto v = get<Array>(col_arr->operator[](0));
    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(get<int32_t>(v[0]), 1);
    EXPECT_EQ(get<int32_t>(v[1]), 2);
    EXPECT_EQ(get<int32_t>(v[2]), 3);
    v = get<Array>(col_arr->operator[](1));
    EXPECT_EQ(v.size(), 0);
    EXPECT_EQ(get<int32_t>(data_col->operator[](0)), 1);
    EXPECT_EQ(get<int32_t>(data_col->operator[](1)), 2);
    EXPECT_EQ(get<int32_t>(data_col->operator[](2)), 3);

    // expand will not make data expand
    EXPECT_EQ(col_arr->size(), 2);
    col_arr->resize(10);
    EXPECT_EQ(col_arr->size(), 10);
    data_col = assert_cast<const ColumnArray&>(*col_arr).get_data_ptr();
    EXPECT_EQ(data_col->size(), 3);
    v = get<Array>(col_arr->operator[](0));
    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(get<int32_t>(v[0]), 1);
    EXPECT_EQ(get<int32_t>(v[1]), 2);
    EXPECT_EQ(get<int32_t>(v[2]), 3);
    v = get<Array>(col_arr->operator[](1));
    EXPECT_EQ(v.size(), 0);
    v = get<Array>(col_arr->operator[](2));
    EXPECT_EQ(v.size(), 0);
}

TEST(ResizeTest, ResizeColumnMap) {
    ColumnMap::MutablePtr col_map = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                                      ColumnArray::ColumnOffsets::create());
    Array k1 = {"a", "b", "c"};
    Array v1 = {1, 2, 3};
    Array k2 = {"d"};
    Array v2 = {4};
    Array a = Array();
    Map map1, map2, map3;
    map1.push_back(k1);
    map1.push_back(v1);
    col_map->insert(map1);
    map3.push_back(a);
    map3.push_back(a);
    col_map->insert(map3);
    map2.push_back(k2);
    map2.push_back(v2);
    col_map->insert(map2);
    // check column map result
    // map : {"a":1,"b":2,"c":3},{:},{"d":4}
    col_map->resize(2);
    EXPECT_EQ(col_map->size(), 2);
    auto data_col = assert_cast<const ColumnMap&>(*col_map).get_values_ptr();
    EXPECT_EQ(data_col->size(), 3);
    auto v = get<Map>(col_map->operator[](0));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]), Array({"a", "b", "c"}));
    EXPECT_EQ(get<Array>(v[1]), Array({1, 2, 3}));
    v = get<Map>(col_map->operator[](1));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]), Array());
    EXPECT_EQ(get<Array>(v[1]), Array());
    EXPECT_EQ(get<int32_t>(data_col->operator[](0)), 1);
    EXPECT_EQ(get<int32_t>(data_col->operator[](1)), 2);
    EXPECT_EQ(get<int32_t>(data_col->operator[](2)), 3);

    // expand will not make data expand
    EXPECT_EQ(col_map->size(), 2);
    col_map->resize(10);
    EXPECT_EQ(col_map->size(), 10);
    data_col = assert_cast<const ColumnMap&>(*col_map).get_values_ptr();
    EXPECT_EQ(data_col->size(), 3);
    v = get<Map>(col_map->operator[](0));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]), Array({"a", "b", "c"}));
    EXPECT_EQ(get<Array>(v[1]), Array({1, 2, 3}));
    v = get<Map>(col_map->operator[](1));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]), Array());
    EXPECT_EQ(get<Array>(v[1]), Array());
    v = get<Map>(col_map->operator[](2));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]), Array());
    EXPECT_EQ(get<Array>(v[1]), Array());
}

} // namespace doris::vectorized
