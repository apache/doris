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

#include "vec/functions/function_case.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "testutil/column_helper.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(VFunctionCase, test_case) {
    auto function = FunctionCase<false, true>();
    auto arguments = {
            /*
            CASE  
            WHEN false THEN 1
            WHEN false THEN 2
            WHEN false THEN 3
            WHEN true THEN 4
            else 5
            END
        */
            ColumnHelper::create_column_with_name<DataTypeUInt8>({false}),
            ColumnHelper::create_column_with_name<DataTypeInt32>({1}),
            ColumnHelper::create_column_with_name<DataTypeUInt8>({false}),
            ColumnHelper::create_column_with_name<DataTypeInt32>({2}),
            ColumnHelper::create_column_with_name<DataTypeUInt8>({false}),
            ColumnHelper::create_column_with_name<DataTypeInt32>({3}),
            ColumnHelper::create_column_with_name<DataTypeUInt8>({true}),
            ColumnHelper::create_column_with_name<DataTypeInt32>({4}),
            ColumnHelper::create_column_with_name<DataTypeInt32>({5}),
    };
    Block block = arguments;
    uint32_t num_columns_without_result = block.columns();
    block.insert({nullptr, std::make_shared<DataTypeInt32>(), "result"});
    auto st = function.execute(nullptr, block, {0, 1, 2, 3, 4, 5, 6, 7, 8},
                               num_columns_without_result, block.rows(), false);
    EXPECT_TRUE(st.ok()) << st.msg();

    std::cout << block.dump_data() << std::endl;

    EXPECT_TRUE(ColumnHelper::column_equal(block.get_by_position(num_columns_without_result).column,
                                           ColumnHelper::create_column<DataTypeInt32>({4})));
}
} // namespace doris::vectorized
