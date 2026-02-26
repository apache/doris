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

struct AggregateFunctionEntropyTest : public AggregateFunctiontest {};

// ------------------------------------------------------------
// 1. Numeric entropy test
// ------------------------------------------------------------
TEST_F(AggregateFunctionEntropyTest, test_numeric_entropy) {
    create_agg("entropy", false, {std::make_shared<DataTypeInt32>()},
               std::make_shared<DataTypeFloat64>());

    // values: 1,1,2,2,3
    Block block = ColumnHelper::create_block<DataTypeInt32>({1, 1, 2, 2, 3});

    double p1 = 2.0 / 5;
    double p2 = 2.0 / 5;
    double p3 = 1.0 / 5;
    double expected = -(p1 * log2(p1) + p2 * log2(p2) + p3 * log2(p3));

    execute(block, ColumnHelper::create_column_with_name<DataTypeFloat64>({expected}));
}

// ------------------------------------------------------------
// 2. String entropy test
// ------------------------------------------------------------
TEST_F(AggregateFunctionEntropyTest, test_string_entropy) {
    create_agg("entropy", false, {std::make_shared<DataTypeString>()},
               std::make_shared<DataTypeFloat64>());

    Block block = ColumnHelper::create_block<DataTypeString>({"a", "a", "b"});

    double p1 = 2.0 / 3;
    double p2 = 1.0 / 3;
    double expected = -(p1 * log2(p1) + p2 * log2(p2));

    execute(block, ColumnHelper::create_column_with_name<DataTypeFloat64>({expected}));
}

// ------------------------------------------------------------
// 3. Generic entropy test
// ------------------------------------------------------------
TEST_F(AggregateFunctionEntropyTest, test_generic_entropy) {
    create_agg("entropy", false,
               {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()},
               std::make_shared<DataTypeFloat64>());

    // rows:
    // (1, "a")
    // (1, "a")
    // (2, "b")
    Block block =
            ColumnHelper::create_block<DataTypeInt32, DataTypeString>({1, 1, 2}, {"a", "a", "b"});

    double p1 = 2.0 / 3;
    double p2 = 1.0 / 3;
    double expected = -(p1 * log2(p1) + p2 * log2(p2));

    execute(block, ColumnHelper::create_column_with_name<DataTypeFloat64>({expected}));
}

// ------------------------------------------------------------
// 4. NULL entropy test
// ------------------------------------------------------------
TEST_F(AggregateFunctionEntropyTest, test_nullable_entropy) {
    create_agg("entropy", false,
               {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())},
               std::make_shared<DataTypeFloat64>());

    // values: 1,1,NULL,2,NULL
    Block block =
            ColumnHelper::create_nullable_block<DataTypeInt32>({1, 1, 0, 2, 0}, {0, 0, 1, 0, 1});

    // only non-null values: 1,1,2
    double p1 = 2.0 / 3;
    double p2 = 1.0 / 3;
    double expected = -(p1 * log2(p1) + p2 * log2(p2));

    execute(block, ColumnHelper::create_column_with_name<DataTypeFloat64>({expected}));
}

// ------------------------------------------------------------
// 5. Empty input test
// ------------------------------------------------------------
TEST_F(AggregateFunctionEntropyTest, test_empty) {
    create_agg("entropy", false, {std::make_shared<DataTypeInt32>()},
               std::make_shared<DataTypeFloat64>());

    Block block = ColumnHelper::create_block<DataTypeInt32>({});

    // entropy of empty set = 0
    execute(block, ColumnHelper::create_column_with_name<DataTypeFloat64>({0.0}));
}

} // namespace doris::vectorized
