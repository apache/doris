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

#include "exec/arrow/arrow_range.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "exprs/binary_predicate.h"
#include "exprs/in_predicate.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"

namespace doris {

class RuntimeState;

class ArrowRangeTest : public testing::Test {
public:
    ArrowRangeTest() {
        _mem_pool.reset(new MemPool());
        _state = _pool.add(new RuntimeState(TQueryGlobals()));
        _state->init_instance_mem_tracker();
        _state->_exec_env = ExecEnv::GetInstance();
    }

protected:
    RuntimeState* _state;
    ObjectPool _pool;
    std::shared_ptr<MemPool> _mem_pool;
    std::vector<ExprContext*> _binary_expr;
    std::vector<ExprContext*> _in_expr;

    std::vector<ExprContext*> create_binary_exprs(TExprOpcode::type op);

    virtual void SetUp() {}

    virtual void TearDown() {}
};

std::vector<ExprContext*> ArrowRangeTest::create_binary_exprs(TExprOpcode::type op) {
    std::vector<ExprContext*> _binary_expr;
    TExpr texpr;
    {
        TExprNode node0;
        node0.opcode = op;
        node0.child_type = TPrimitiveType::BIGINT;
        node0.node_type = TExprNodeType::BINARY_PRED;
        node0.num_children = 2;
        node0.__isset.opcode = true;
        node0.__isset.child_type = true;
        node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        texpr.nodes.emplace_back(node0);

        TExprNode node1;
        node1.node_type = TExprNodeType::SLOT_REF;
        node1.type = gen_type_desc(TPrimitiveType::INT);
        node1.__isset.slot_ref = true;
        node1.num_children = 0;
        node1.slot_ref.slot_id = 0;
        node1.slot_ref.tuple_id = 0;
        node1.output_column = true;
        node1.__isset.output_column = true;
        texpr.nodes.emplace_back(node1);

        TExprNode node2;
        TIntLiteral intLiteral;
        intLiteral.value = 10;
        node2.node_type = TExprNodeType::INT_LITERAL;
        node2.type = gen_type_desc(TPrimitiveType::BIGINT);
        node2.__isset.int_literal = true;
        node2.int_literal = intLiteral;
        texpr.nodes.emplace_back(node2);
    }

    std::vector<TExpr> conjuncts;
    conjuncts.emplace_back(texpr);
    Expr::create_expr_trees(&_pool, conjuncts, &_binary_expr);

    return _binary_expr;
}

TEST_F(ArrowRangeTest, binary_ops) {
    // 1. = 10
    {
        std::vector<ExprContext*> binary_expr = create_binary_exprs(TExprOpcode::EQ);
        IntegerArrowRange range1(0, 8);
        EXPECT_TRUE(range1.determine_filter_row_group(binary_expr));
        IntegerArrowRange range2(8, 10);
        EXPECT_FALSE(range2.determine_filter_row_group(binary_expr));
        IntegerArrowRange range3(11, 20);
        EXPECT_TRUE(range3.determine_filter_row_group(binary_expr));
    }

    // 2. != 10
    {
        std::vector<ExprContext*> binary_expr = create_binary_exprs(TExprOpcode::NE);
        IntegerArrowRange range4(0, 8);
        EXPECT_FALSE(range4.determine_filter_row_group(binary_expr));
        IntegerArrowRange range5(8, 10);
        EXPECT_TRUE(range5.determine_filter_row_group(binary_expr));
        IntegerArrowRange range6(11, 20);
        EXPECT_FALSE(range6.determine_filter_row_group(binary_expr));
    }

    // 3. > 10
    {
        std::vector<ExprContext*> binary_expr = create_binary_exprs(TExprOpcode::GT);
        IntegerArrowRange range1(0, 8);
        EXPECT_TRUE(range1.determine_filter_row_group(binary_expr));
        IntegerArrowRange range2(8, 10);
        EXPECT_TRUE(range2.determine_filter_row_group(binary_expr));
        IntegerArrowRange range3(11, 20);
        EXPECT_FALSE(range3.determine_filter_row_group(binary_expr));
    }
    // 4. >= 10
    {
        std::vector<ExprContext*> binary_expr = create_binary_exprs(TExprOpcode::GE);
        IntegerArrowRange range1(0, 8);
        EXPECT_TRUE(range1.determine_filter_row_group(binary_expr));
        IntegerArrowRange range2(8, 10);
        EXPECT_FALSE(range2.determine_filter_row_group(binary_expr));
        IntegerArrowRange range3(11, 20);
        EXPECT_FALSE(range3.determine_filter_row_group(binary_expr));
    }
    // 5. < 10
    {
        std::vector<ExprContext*> binary_expr = create_binary_exprs(TExprOpcode::LT);
        IntegerArrowRange range1(0, 8);
        EXPECT_FALSE(range1.determine_filter_row_group(binary_expr));
        IntegerArrowRange range2(8, 10);
        EXPECT_FALSE(range2.determine_filter_row_group(binary_expr));
        IntegerArrowRange range3(11, 20);
        EXPECT_TRUE(range3.determine_filter_row_group(binary_expr));

        IntegerArrowRange range4(10, 20);
        EXPECT_TRUE(range4.determine_filter_row_group(binary_expr));
    }
    // 6. <= 10
    {
        std::vector<ExprContext*> binary_expr = create_binary_exprs(TExprOpcode::LE);
        IntegerArrowRange range1(0, 8);
        EXPECT_FALSE(range1.determine_filter_row_group(binary_expr));
        IntegerArrowRange range2(8, 10);
        EXPECT_FALSE(range2.determine_filter_row_group(binary_expr));
        IntegerArrowRange range3(11, 20);
        EXPECT_TRUE(range3.determine_filter_row_group(binary_expr));

        IntegerArrowRange range4(10, 20);
        EXPECT_FALSE(range4.determine_filter_row_group(binary_expr));
    }
    // 7. non support op
    {
        std::vector<ExprContext*> binary_expr = create_binary_exprs(TExprOpcode::EQ_FOR_NULL);
        IntegerArrowRange range1(0, 8);
        EXPECT_FALSE(range1.determine_filter_row_group(binary_expr));
    }
}

} // end namespace doris
