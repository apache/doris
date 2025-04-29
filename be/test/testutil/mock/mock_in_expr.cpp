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

#include "mock_in_expr.h"

#include <gtest/gtest.h>

#include <memory>

#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/functions/in.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
TEST(MockInExprTest, test) {
    auto in = std::make_shared<MockInExpr>();
    auto expr_ctx = std::make_shared<VExprContext>(in);
    MockRowDescriptor desc;
    {
        auto st = in->prepare(nullptr, desc, expr_ctx.get());
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    { auto st = in->open(nullptr, nullptr, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL); }

    {
        auto* fn_ctx = expr_ctx->fn_context(in->fn_context_index());
        std::shared_ptr<InState> state = std::make_shared<InState>();
        fn_ctx->set_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL, state);
    }

    {
        auto* state = reinterpret_cast<vectorized::InState*>(
                expr_ctx->fn_context(in->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        state->use_set = true;
        state->hybrid_set.reset(create_set(TYPE_BIGINT, 0, true));
    }
}

TEST(MockInExprTest, create_with_ctx) {
    auto expr_ctx =
            MockInExpr::create_with_ctx(ColumnHelper::create_column<DataTypeInt64>({1, 1, 100}));
    auto pred = expr_ctx->root();

    auto* state = reinterpret_cast<vectorized::InState*>(
            expr_ctx->fn_context(pred->fn_context_index())
                    ->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    EXPECT_TRUE(state->use_set);
}

VExprContextSPtr MockInExpr::create_with_ctx(ColumnPtr column, bool is_not_in) {
    auto in = std::make_shared<MockInExpr>();
    auto expr_ctx = std::make_shared<VExprContext>(in);
    MockRowDescriptor desc;
    in->_is_not_in = is_not_in;
    EXPECT_TRUE(in->prepare(nullptr, desc, expr_ctx.get()));
    auto* fn_ctx = expr_ctx->fn_context(in->fn_context_index());
    std::shared_ptr<InState> state = std::make_shared<InState>();
    fn_ctx->set_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL, state);
    state->use_set = true;
    state->hybrid_set.reset(create_set(TYPE_BIGINT, 0, true));
    for (int i = 0; i < column->size(); i++) {
        state->hybrid_set->insert((void*)column->get_data_at(i).data, column->get_data_at(i).size);
    }
    return expr_ctx;
}

Status MockInExpr::prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) {
    if (is_not_in()) {
        _function = std::make_shared<FunctionIn<false>>();
    } else {
        _function = std::make_shared<FunctionIn<true>>();
    }

    context->_fn_contexts.push_back(FunctionContext::create_context(nullptr, {}, {}));
    _fn_context_index = context->_fn_contexts.size() - 1;
    _prepare_finished = true;
    return Status::OK();
}

Status MockInExpr::open(RuntimeState* state, VExprContext* context,
                        FunctionContext::FunctionStateScope scope) {
    _open_finished = true;
    return Status::OK();
}

} // namespace doris::vectorized
