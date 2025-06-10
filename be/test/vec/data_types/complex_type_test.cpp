
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

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

TEST(ComplexTypeTest, CreateColumnConstWithDefaultValue) {
    DataTypePtr n1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr n2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    DataTypePtr n3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
    DataTypePtr s1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

    DataTypes dataTypes;
    dataTypes.push_back(n1);
    dataTypes.push_back(n2);
    dataTypes.push_back(n3);
    dataTypes.push_back(s1);

    // data_type_struct
    DataTypePtr s = std::make_shared<DataTypeStruct>(dataTypes);
    ColumnPtr col_s = s->create_column_const_with_default_value(1);
    Field t;
    EXPECT_EQ(1, col_s->size());
    col_s->get(0, t);
    EXPECT_EQ(PrimitiveType::TYPE_STRUCT, t.get_type());

    // data_type_map
    DataTypePtr m = std::make_shared<DataTypeMap>(n1, s1);
    ColumnPtr col_m = m->create_column_const_with_default_value(1);
    EXPECT_EQ(1, col_m->size());
    Field mf;
    col_m->get(0, mf);
    EXPECT_EQ(PrimitiveType::TYPE_MAP, mf.get_type());

    // data_type_array
    DataTypePtr a = std::make_shared<DataTypeArray>(s1);
    ColumnPtr col_a = a->create_column_const_with_default_value(1);
    EXPECT_EQ(1, col_a->size());
    Field af;
    col_a->get(0, af);
    EXPECT_EQ(PrimitiveType::TYPE_ARRAY, af.get_type());
}
} // namespace doris::vectorized
