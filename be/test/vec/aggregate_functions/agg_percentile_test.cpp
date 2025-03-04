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

namespace doris::vectorized {

struct AggregateFunctionPercentiletest : public AggregateFunctiontest {
    void create_percentile(bool result_nullable, DataTypes args_type) {
        agg_fn = create_agg_fn(pool, "percentile", args_type, result_nullable);
    }
};

TEST_F(AggregateFunctionPercentiletest, test_not_nullable) {
    create_percentile(false,
                      {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeFloat64>()});

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3}),
                   ColumnHelper::create_column_with_name<DataTypeFloat64>({0.5, 0.5, 0.5})}),
            ColumnHelper::create_column_with_name<DataTypeFloat64>({2}));
}

TEST_F(AggregateFunctionPercentiletest, test_nullable) {
    create_percentile(true, {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                             std::make_shared<DataTypeFloat64>()});
    execute(Block({ColumnHelper::create_nullable_column_with_name<DataTypeInt64>(
                           {1, 2, 3}, {false, false, false}),
                   ColumnHelper::create_column_with_name<DataTypeFloat64>({0.5, 0.5, 0.5})}),
            ColumnHelper::create_nullable_column_with_name<DataTypeFloat64>({2}, {false}));
}

} // namespace doris::vectorized
