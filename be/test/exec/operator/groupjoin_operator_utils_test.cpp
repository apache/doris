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

#include "exec/operator/groupjoin_operator_utils.h"

#include <gtest/gtest.h>

#include "core/column/column_nullable.h"
#include "core/data_type/data_type_number.h"
#include "testutil/column_helper.h"

namespace doris {
TEST(GroupJoinOperatorUtilsTest, RejectsNodeWithoutAggregateFunctions) {
    TPlanNode tnode;
    tnode.__isset.group_join_node = true;
    tnode.group_join_node.join_op = TJoinOp::INNER_JOIN;
    tnode.group_join_node.agg_output_mode = TGroupJoinAggOutputMode::FINAL_RESULT;

    const auto status = groupjoin::validate_group_join_node(tnode);

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("at least one aggregate function"), std::string::npos)
            << status;
}
TEST(GroupJoinOperatorUtilsTest, ExtractKeyColumnsBuildsCombinedNullMap) {
    auto key0 = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 4}, {0, 1, 0, 0});
    auto key1 = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30, 40}, {0, 0, 1, 0});
    std::vector<ColumnPtr> key_columns_holder {key0, key1};

    ColumnRawPtrs key_not_nullable_columns;
    ColumnUInt8::MutablePtr null_map_column;
    ASSERT_TRUE(groupjoin::extract_key_columns(4, key_columns_holder, key_not_nullable_columns,
                                               null_map_column)
                        .ok());

    ASSERT_TRUE(null_map_column.get() != nullptr);
    ASSERT_EQ(key_not_nullable_columns.size(), 2);
    EXPECT_EQ(key_not_nullable_columns[0],
              &assert_cast<const ColumnNullable&>(*key0).get_nested_column());
    EXPECT_EQ(key_not_nullable_columns[1],
              &assert_cast<const ColumnNullable&>(*key1).get_nested_column());

    const auto& null_map = null_map_column->get_data();
    ASSERT_EQ(null_map.size(), 4);
    EXPECT_EQ(null_map[0], 0);
    EXPECT_EQ(null_map[1], 1);
    EXPECT_EQ(null_map[2], 1);
    EXPECT_EQ(null_map[3], 0);
}

TEST(GroupJoinOperatorUtilsTest, RejectsUnknownAggregateInputSide) {
    TPlanNode node;
    node.__isset.group_join_node = true;
    auto& group_join = node.group_join_node;
    group_join.join_op = TJoinOp::INNER_JOIN;
    group_join.agg_output_mode = TGroupJoinAggOutputMode::FINAL_RESULT;
    group_join.aggregate_functions.emplace_back();
    group_join.aggregate_functions.back().input_side = static_cast<TGroupJoinAggSide::type>(99);
    const auto status = groupjoin::validate_group_join_node(node);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("aggregate input side"), std::string::npos) << status;
}

} // namespace doris
