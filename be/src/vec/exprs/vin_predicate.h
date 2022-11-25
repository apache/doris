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
#include "vec/functions/function.h"

namespace doris::vectorized {
class VInPredicate final : public VExpr {
public:
    VInPredicate(const TExprNode& node);
    ~VInPredicate() override = default;
    doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                          int* result_column_id) override;
    doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                          VExprContext* context) override;
    doris::Status open(doris::RuntimeState* state, VExprContext* context,
                       FunctionContext::FunctionStateScope scope) override;
    void close(doris::RuntimeState* state, VExprContext* context,
               FunctionContext::FunctionStateScope scope) override;
    VExpr* clone(doris::ObjectPool* pool) const override {
        return pool->add(new VInPredicate(*this));
    }
    const std::string& expr_name() const override;

    std::string debug_string() const override;

    const FunctionBasePtr function() { return _function; };

    const bool is_not_in() const { return _is_not_in; };

private:
    FunctionBasePtr _function;
    std::string _expr_name;

    const bool _is_not_in;
    static const constexpr char* function_name = "in";
};
} // namespace doris::vectorized