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

#include "mock_literal_expr.h"

#include <gtest/gtest.h>

#include "testutil/column_helper.h"
#include "vec/core/materialize_block.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(MockLiteralTest, test) {
    {
        auto ctxs = MockLiteral::create<DataTypeInt64>({1, 2, 3, 4});
        Block block;
        for (auto& ctx : ctxs) {
            int result_column_id = -1;
            EXPECT_TRUE(ctx->execute(&block, &result_column_id));
        }

        std::cout << block.dump_data() << std::endl;

        EXPECT_TRUE(ColumnHelper::block_equal(
                block, Block {
                               ColumnHelper::create_column_with_name<DataTypeInt64>({1}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({2}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({3}),
                               ColumnHelper::create_column_with_name<DataTypeInt64>({4}),
                       }));
    }
}

TEST(MockLiteralTest, test_const) {
    {
        auto ctxs = MockLiteral::create_const<DataTypeInt64>({1, 2, 3, 4}, 5);
        Block block;
        for (auto& ctx : ctxs) {
            int result_column_id = -1;
            EXPECT_TRUE(ctx->execute(&block, &result_column_id));
        }

        materialize_block_inplace(block);

        std::cout << block.dump_data() << std::endl;

        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                Block {
                        ColumnHelper::create_column_with_name<DataTypeInt64>({1, 1, 1, 1, 1}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({2, 2, 2, 2, 2}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({3, 3, 3, 3, 3}),
                        ColumnHelper::create_column_with_name<DataTypeInt64>({4, 4, 4, 4, 4}),
                }));
    }
}
} // namespace doris::vectorized
