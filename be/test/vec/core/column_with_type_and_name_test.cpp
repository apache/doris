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

#include "vec/core/column_with_type_and_name.h"

#include <gtest/gtest.h>

#include "testutil/column_helper.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(ColumnWithTypeAndNameTest, get_nested_test) {
    ColumnWithTypeAndName column_with_type_and_name;
    auto null_column = ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({1}),
                                              ColumnHelper::create_column<DataTypeUInt8>({true}));
    column_with_type_and_name.column = ColumnConst::create(std::move(null_column), 3);
    column_with_type_and_name.type =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    column_with_type_and_name.name = "column_with_type_and_name";
    column_with_type_and_name.get_nested(true);
}

} // namespace doris::vectorized
