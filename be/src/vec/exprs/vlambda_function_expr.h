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

#pragma once
#include "common/global_types.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"

namespace doris::vectorized {
class VLambdaFunctionExpr final : public VExpr {
    ENABLE_FACTORY_CREATOR(VLambdaFunctionExpr);

public:
    VLambdaFunctionExpr(const TExprNode& node) : VExpr(node) {}
    ~VLambdaFunctionExpr() override = default;

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
        _prepare_finished = true;
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        DCHECK(_prepare_finished);
        RETURN_IF_ERROR(VExpr::open(state, context, scope));
        _open_finished = true;
        return Status::OK();
    }

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        DCHECK(_open_finished || _getting_const_col);
        return get_child(0)->execute(context, block, result_column_id);
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const std::string _expr_name = "vlambda_function_expr";
};
} // namespace doris::vectorized
