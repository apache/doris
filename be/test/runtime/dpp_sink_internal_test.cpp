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

#include "runtime/dpp_sink_internal.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"

namespace doris {

class DppSinkInternalTest : public testing::Test {
public:
    DppSinkInternalTest() {}

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

TEST_F(DppSinkInternalTest, PartitionInfoNormal) {
    ObjectPool pool;
    TRangePartition t_partition;
    // Two Keys
    {
        TExpr expr;
        TExprNode node;
        node.node_type = TExprNodeType::INT_LITERAL;
        node.type.type = TPrimitiveType::INT;
        node.num_children = 0;
        node.__set_int_literal(TIntLiteral());
        expr.nodes.push_back(node);
        t_partition.distributed_exprs.push_back(expr);
        t_partition.distribute_bucket = 1;
    }
    {
        TExpr expr;
        TExprNode node;
        node.node_type = TExprNodeType::INT_LITERAL;
        node.type.type = TPrimitiveType::INT;
        node.num_children = 0;
        node.__set_int_literal(TIntLiteral());
        expr.nodes.push_back(node);
        t_partition.distributed_exprs.push_back(expr);
        t_partition.distribute_bucket = 1;
    }
    t_partition.range.start_key.sign = -1;
    t_partition.range.end_key.sign = 1;

    PartitionInfo info;

    ASSERT_TRUE(PartitionInfo::from_thrift(&pool, t_partition, &info).ok());
    ASSERT_TRUE(info.prepare(nullptr, RowDescriptor()).ok());
}

TEST_F(DppSinkInternalTest, ZeroBucket) {
    ObjectPool pool;
    TRangePartition t_partition;
    // Two Keys
    {
        TExpr expr;
        TExprNode node;
        node.node_type = TExprNodeType::INT_LITERAL;
        node.type.type = TPrimitiveType::INT;
        node.num_children = 0;
        node.__set_int_literal(TIntLiteral());
        expr.nodes.push_back(node);
        t_partition.distributed_exprs.push_back(expr);
    }
    t_partition.range.start_key.sign = -1;
    t_partition.range.end_key.sign = 1;

    PartitionInfo info;

    ASSERT_FALSE(PartitionInfo::from_thrift(&pool, t_partition, &info).ok());
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
