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

struct AggregateFunctionBoolUnionTest : public AggregateFunctiontest {};

TEST_F(AggregateFunctionBoolUnionTest, test_bool_or) {
    {
        create_agg("bool_or", false, {std::make_shared<DataTypeBool>()});

        execute(Block({ColumnHelper::create_column_with_name<DataTypeBool>({false, false, true})}),
                ColumnHelper::create_column_with_name<DataTypeBool>({true}));
    }

    {
        create_agg("boolor_agg", false, {std::make_shared<DataTypeBool>()});

        execute(Block({ColumnHelper::create_column_with_name<DataTypeBool>({true, false, true})}),
                ColumnHelper::create_column_with_name<DataTypeBool>({true}));
    }
}

TEST_F(AggregateFunctionBoolUnionTest, test_bool_and) {
    {
        create_agg("bool_and", false, {std::make_shared<DataTypeBool>()});

        execute(Block({ColumnHelper::create_column_with_name<DataTypeBool>({true, true})}),
                ColumnHelper::create_column_with_name<DataTypeBool>({true}));
    }

    {
        create_agg("booland_agg", false, {std::make_shared<DataTypeBool>()});

        execute(Block({ColumnHelper::create_column_with_name<DataTypeBool>({true, false, true})}),
                ColumnHelper::create_column_with_name<DataTypeBool>({false}));
    }
}

TEST_F(AggregateFunctionBoolUnionTest, test_bool_xor) {
    {
        create_agg("bool_xor", false, {std::make_shared<DataTypeBool>()});

        execute(Block({ColumnHelper::create_column_with_name<DataTypeBool>({true, true, true})}),
                ColumnHelper::create_column_with_name<DataTypeBool>({false}));
    }

    {
        create_agg("boolxor_agg", false, {std::make_shared<DataTypeBool>()});

        execute(Block({ColumnHelper::create_column_with_name<DataTypeBool>({true, false, false})}),
                ColumnHelper::create_column_with_name<DataTypeBool>({true}));
    }
}
} // namespace doris::vectorized