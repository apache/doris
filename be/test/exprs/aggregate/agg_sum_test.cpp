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

#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/agg_function_test.h"

namespace doris {

struct AggregateFunctionSumTest : public AggregateFunctiontest {};

TEST_F(AggregateFunctionSumTest, test_int64) {
    create_agg("sum", false, {std::make_shared<DataTypeInt64>()},
               std::make_shared<DataTypeInt64>());

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3})}),
            ColumnHelper::create_column_with_name<DataTypeInt64>({6}));
}

TEST_F(AggregateFunctionSumTest, test_incremental_mode_only_for_exact_sum) {
    create_agg("sum", false, {std::make_shared<DataTypeInt64>()},
               std::make_shared<DataTypeInt64>());
    EXPECT_TRUE(agg_fn->supported_incremental_mode());

    // Floating-point sums are not exactly invertible, so sliding frames must be recomputed.
    create_agg("sum", false, {std::make_shared<DataTypeFloat64>()},
               std::make_shared<DataTypeFloat64>());
    EXPECT_FALSE(agg_fn->supported_incremental_mode());
}
} // namespace doris
