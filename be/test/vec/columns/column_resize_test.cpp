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
            std::make_shared<DataTypeDecimal<vectorized::Decimal128V2>>(27, 9));
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

} // namespace doris::vectorized
