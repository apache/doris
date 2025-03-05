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

#include "agg_function_test.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

struct AggregateFunctionBitTest : public AggregateFunctiontest {};

TEST_F(AggregateFunctionBitTest, test_group_bit_or) {
    create_agg("group_bit_or", false, {std::make_shared<DataTypeInt64>()});

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({31231, 214142, 123123})}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({261631}));
}

TEST_F(AggregateFunctionBitTest, test_group_bit_and) {
    create_agg("group_bit_and", false, {std::make_shared<DataTypeInt64>()});

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({213123, 123123, 51431})}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({16515}));
}

TEST_F(AggregateFunctionBitTest, test_group_bit_xor) {
    create_agg("group_bit_xor", false, {std::make_shared<DataTypeInt64>()});

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>(
                    {213123, 2131231, 23151521451})}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({23149669175}));
}
} // namespace doris::vectorized
