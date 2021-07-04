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

#include "exprs/runtime_filter.h"

#include <array>
#include <memory>

#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "gen_cpp/Planner_types.h"
#include "gen_cpp/Types_types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"

namespace doris {
TTypeDesc create_type_desc(PrimitiveType type);

class RuntimeFilterTest : public testing::Test {
public:
    RuntimeFilterTest() {}
    virtual void SetUp() {
        ExecEnv* exec_env = ExecEnv::GetInstance();
        exec_env = nullptr;
        _runtime_stat.reset(
                new RuntimeState(_fragment_id, _query_options, _query_globals, exec_env));
        _runtime_stat->init_instance_mem_tracker();
    }
    virtual void TearDown() { _obj_pool.clear(); }

private:
    ObjectPool _obj_pool;
    TUniqueId _fragment_id;
    TQueryOptions _query_options;
    TQueryGlobals _query_globals;

    std::unique_ptr<RuntimeState> _runtime_stat;
    // std::unique_ptr<IRuntimeFilter> _runtime_filter;
};

TEST_F(RuntimeFilterTest, runtime_filter_basic_test) {
    TRuntimeFilterDesc desc;
    desc.__set_filter_id(0);
    desc.__set_expr_order(0);
    desc.__set_has_local_targets(true);
    desc.__set_has_remote_targets(false);
    desc.__set_is_broadcast_join(true);
    desc.__set_type(TRuntimeFilterType::BLOOM);
    desc.__set_bloom_filter_size_bytes(4096);

    // build src expr context

    {
        TExpr build_expr;
        TExprNode expr_node;
        expr_node.__set_node_type(TExprNodeType::SLOT_REF);
        expr_node.__set_type(create_type_desc(TYPE_INT));
        expr_node.__set_num_children(0);
        expr_node.__isset.slot_ref = true;
        TSlotRef slot_ref;
        slot_ref.__set_slot_id(0);
        slot_ref.__set_tuple_id(0);
        expr_node.__set_slot_ref(slot_ref);
        expr_node.__isset.output_column = true;
        expr_node.__set_output_column(0);
        build_expr.nodes.push_back(expr_node);
        desc.__set_src_expr(build_expr);
    }
    // build dst expr
    {
        TExpr target_expr;
        TExprNode expr_node;
        expr_node.__set_node_type(TExprNodeType::SLOT_REF);
        expr_node.__set_type(create_type_desc(TYPE_INT));
        expr_node.__set_num_children(0);
        expr_node.__isset.slot_ref = true;
        TSlotRef slot_ref;
        slot_ref.__set_slot_id(0);
        slot_ref.__set_tuple_id(0);
        expr_node.__set_slot_ref(slot_ref);
        expr_node.__isset.output_column = true;
        expr_node.__set_output_column(0);
        target_expr.nodes.push_back(expr_node);
        std::map<int, TExpr> planid_to_target_expr = {{0, target_expr}};
        desc.__set_planId_to_target_expr(planid_to_target_expr);
    }

    // size_t prob_index = 0;
    SlotRef* expr = _obj_pool.add(new SlotRef(TYPE_INT, 0));
    ExprContext* prob_expr_ctx = _obj_pool.add(new ExprContext(expr));
    ExprContext* build_expr_ctx = _obj_pool.add(new ExprContext(expr));

    IRuntimeFilter* runtime_filter = nullptr;

    Status status = IRuntimeFilter::create(_runtime_stat.get(),
                                           _runtime_stat->instance_mem_tracker().get(), &_obj_pool,
                                           &desc, RuntimeFilterRole::PRODUCER, -1, &runtime_filter);

    ASSERT_TRUE(status.ok());

    // generate data
    std::array<TupleRow, 1024> tuple_rows;
    int generator_index = 0;
    auto generator = [&]() {
        std::array<int, 2>* data = _obj_pool.add(new std::array<int, 2>());
        data->at(0) = data->at(1) = generator_index++;
        TupleRow row;
        row._tuples[0] = (Tuple*)data->data();
        return row;
    };
    std::generate(tuple_rows.begin(), tuple_rows.end(), generator);

    std::array<TupleRow, 1024> not_exist_data;
    // generate not exist data
    std::generate(not_exist_data.begin(), not_exist_data.end(), generator);

    // build runtime filter
    for (TupleRow& row : tuple_rows) {
        void* val = build_expr_ctx->get_value(&row);
        runtime_filter->insert(val);
    }
    // get expr context from filter

    std::list<ExprContext*> expr_context_list;
    ASSERT_TRUE(runtime_filter->get_push_expr_ctxs(&expr_context_list, prob_expr_ctx).ok());
    ASSERT_TRUE(!expr_context_list.empty());

    // test data in
    for (TupleRow& row : tuple_rows) {
        for (ExprContext* ctx : expr_context_list) {
            ASSERT_TRUE(ctx->get_boolean_val(&row).val);
        }
    }
    // test not exist data
    for (TupleRow& row : not_exist_data) {
        for (ExprContext* ctx : expr_context_list) {
            ASSERT_FALSE(ctx->get_boolean_val(&row).val);
        }
    }

    // test null
    for (ExprContext* ctx : expr_context_list) {
        std::array<int, 2>* data = _obj_pool.add(new std::array<int, 2>());
        data->at(0) = data->at(1) = generator_index++;
        TupleRow row;
        row._tuples[0] = nullptr;
        ASSERT_FALSE(ctx->get_boolean_val(&row).val);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}