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

#include "vec/exprs/vexpr.h"
#include "vec/functions/function_case.h"

namespace doris::vectorized {

class VCaseExpr final : public VExpr {
public:
    VCaseExpr(const TExprNode& node);
    ~VCaseExpr() = default;
    virtual Status execute(VExprContext* context, vectorized::Block* block,
                           int* result_column_id) override;
    virtual Status prepare(RuntimeState* state, const RowDescriptor& desc,
                           VExprContext* context) override;
    virtual Status open(RuntimeState* state, VExprContext* context,
                        FunctionContext::FunctionStateScope scope) override;
    virtual void close(RuntimeState* state, VExprContext* context,
                       FunctionContext::FunctionStateScope scope) override;
    virtual VExpr* clone(ObjectPool* pool) const override {
        return pool->add(new VCaseExpr(*this));
    }
    virtual const std::string& expr_name() const override;

private:
    bool _is_prepare;
    bool _has_case_expr;
    bool _has_else_expr;

    FunctionBasePtr _function;
    std::string _function_name = "case";
    const std::string _expr_name = "vcase expr";
};
} // namespace doris::vectorized
