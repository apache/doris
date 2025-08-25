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

#include "agg_function_test.h"
#include "testutil/column_helper.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

struct AggregateFunctionRegrTest : public AggregateFunctiontest {};

TEST_F(AggregateFunctionRegrTest, test_regr_avgx) {
    create_agg("regr_avgx", false,
               {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});

    // Test data: x values [10, 20, 30], y values [5, 15, 25]
    // Expected regr_avgx result: (10+20+30)/3 = 20
    auto expected_column =
            ColumnHelper::create_nullable_column_with_name<DataTypeFloat64>({20}, {0});
    execute(Block({ColumnHelper::create_column_with_name<DataTypeFloat64>({5, 15, 25}), // Y values
                   ColumnHelper::create_column_with_name<DataTypeFloat64>(
                           {10, 20, 30})}), // X values
            expected_column);
}

TEST_F(AggregateFunctionRegrTest, test_regr_avgy) {
    create_agg("regr_avgy", false,
               {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});

    // Test data: x values [10, 20, 30], y values [5, 15, 25]
    // Expected regr_avgy result: (5+15+25)/3 = 15
    auto expected_column =
            ColumnHelper::create_nullable_column_with_name<DataTypeFloat64>({15}, {0});
    execute(Block({ColumnHelper::create_column_with_name<DataTypeFloat64>({5, 15, 25}),
                   ColumnHelper::create_column_with_name<DataTypeFloat64>({10, 20, 30})}),
            expected_column);
}

TEST_F(AggregateFunctionRegrTest, test_regr_count) {
    create_agg("regr_count", false,
               {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});

    // Test data: 3 non-null pairs
    // Expected regr_count result: 3
    auto expected_column = ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({3}, {0});
    execute(Block({ColumnHelper::create_column_with_name<DataTypeFloat64>({5, 15, 25}),
                   ColumnHelper::create_column_with_name<DataTypeFloat64>({10, 20, 30})}),
            expected_column);
}

} // namespace doris::vectorized
