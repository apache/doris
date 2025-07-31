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

#include "vec/data_types/data_type_nothing.h"

#include <gtest/gtest.h>

#include "vec/columns/column_nothing.h"
#include "vec/core/field.h"

namespace doris::vectorized {

TEST(DataTypeNothingTest, GetFieldWithDataTypeTest) {
    // 1. Create a DataTypeNothing instance and a corresponding column
    DataTypeNothing data_type_nothing;
    auto column = data_type_nothing.create_column();
    column->insert_default(); // A Nothing column can only store empty rows.

    // 2. Call the function under test
    FieldWithDataType fdt = data_type_nothing.get_field_with_data_type(*column, 0);

    // 3. Assert the results
    // The returned FieldWithDataType should contain a default-constructed Field,
    // which is a Null field.
    EXPECT_TRUE(fdt.field.is_null());
    EXPECT_EQ(fdt.field.get_type(), PrimitiveType::TYPE_NULL);

    // Other metadata should have their default values.
    EXPECT_EQ(fdt.base_scalar_type_id, PrimitiveType::INVALID_TYPE);
    EXPECT_EQ(fdt.num_dimensions, 0);
    EXPECT_EQ(fdt.precision, -1);
    EXPECT_EQ(fdt.scale, -1);
}

} // namespace doris::vectorized