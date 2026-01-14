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
#include <string>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

class Block;
class VExprContext;

class ShortCircuitIfExpr final : public VExpr {
public:
    ENABLE_FACTORY_CREATOR(ShortCircuitIfExpr);
    ShortCircuitIfExpr(const TExprNode& node) : VExpr(node) {}
    ~ShortCircuitIfExpr() override = default;

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) final;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) final;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) final;

    std::string debug_string() const override;

    const std::string& expr_name() const override { return IF_NAME; }

    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;

private:
    static size_t count_true_with_notnull(const ColumnPtr& col);

    inline static const std::string IF_NAME = "if";
};

} // namespace doris::vectorized