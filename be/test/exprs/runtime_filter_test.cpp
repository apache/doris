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

#include "exprs/bloom_filter_func.h"
#include "gen_cpp/Planner_types.h"
#include "gen_cpp/Types_types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"

namespace doris {
TTypeDesc create_type_desc(PrimitiveType type, int precision, int scale);

class RuntimeFilterTest : public testing::Test {
public:
    RuntimeFilterTest() {}
    virtual void SetUp() {}
    virtual void TearDown() { _obj_pool.clear(); }

private:
    ObjectPool _obj_pool;
    TUniqueId _fragment_id;
    TQueryOptions _query_options;
    TQueryGlobals _query_globals;

    std::unique_ptr<RuntimeState> _runtime_stat;
    // std::unique_ptr<IRuntimeFilter> _runtime_filter;
};

IRuntimeFilter* create_runtime_filter(TRuntimeFilterType::type type, TQueryOptions* options,
                                      RuntimeState* _runtime_stat, ObjectPool* _obj_pool) {
    TRuntimeFilterDesc desc;
    desc.__set_filter_id(0);
    desc.__set_expr_order(0);
    desc.__set_has_local_targets(true);
    desc.__set_has_remote_targets(false);
    desc.__set_is_broadcast_join(true);
    desc.__set_type(type);
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

    IRuntimeFilter* runtime_filter = nullptr;
    Status status = IRuntimeFilter::create(RuntimeFilterParamsContext::create(_runtime_stat),
                                           _obj_pool, &desc, options, RuntimeFilterRole::PRODUCER,
                                           -1, &runtime_filter);

    EXPECT_TRUE(status.ok()) << status.to_string();

    if (auto bf = runtime_filter->get_bloomfilter()) {
        status = bf->init_with_fixed_length();
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    return status.ok() ? runtime_filter : nullptr;
}

} // namespace doris
