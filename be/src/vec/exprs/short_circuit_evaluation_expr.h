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
#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

class Block;
class VExprContext;
struct ColumnAndSelector;

class ShortCircuitExpr : public VExpr {
public:
    ShortCircuitExpr(const TExprNode& node) : VExpr(node) {}
    ~ShortCircuitExpr() override = default;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) final;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) final;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) final;

    std::string debug_string() const final;

protected:
    // Helper method to dispatch fill operation for two-branch case (e.g., IF, IFNULL).
    // Uses ScalarFillWithSelector for scalar types, NonScalarFillWithSelector otherwise.
    [[nodiscard]] ColumnPtr dispatch_fill_columns(const ColumnPtr& true_column,
                                                  const Selector& true_selector,
                                                  const ColumnPtr& false_column,
                                                  const Selector& false_selector,
                                                  size_t count) const;

    // Helper method to dispatch fill operation for multi-branch case (e.g., COALESCE, CASE).
    // Uses ScalarFillWithSelector for scalar types, NonScalarFillWithSelector otherwise.
    [[nodiscard]] ColumnPtr dispatch_fill_columns(
            const std::vector<ColumnAndSelector>& columns_and_selectors, size_t count) const;
};

class ShortCircuitIfExpr final : public ShortCircuitExpr {
public:
    ENABLE_FACTORY_CREATOR(ShortCircuitIfExpr);
    ShortCircuitIfExpr(const TExprNode& node) : ShortCircuitExpr(node) {}
    ~ShortCircuitIfExpr() override = default;

    const std::string& expr_name() const override { return IF_NAME; }

    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;

private:
    inline static const std::string IF_NAME = "if";
};
class ShortCircuitCaseExpr final : public ShortCircuitExpr {
public:
    ENABLE_FACTORY_CREATOR(ShortCircuitCaseExpr);
    ShortCircuitCaseExpr(const TExprNode& node);
    ~ShortCircuitCaseExpr() override = default;
    const std::string& expr_name() const override { return CASE_NAME; }
    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;

private:
    const bool _has_else_expr;
    inline static const std::string CASE_NAME = "case";
};

class ShortCircuitIfNullExpr final : public ShortCircuitExpr {
public:
    ENABLE_FACTORY_CREATOR(ShortCircuitIfNullExpr);
    ShortCircuitIfNullExpr(const TExprNode& node) : ShortCircuitExpr(node) {}
    ~ShortCircuitIfNullExpr() override = default;

    const std::string& expr_name() const override { return IFNULL_NAME; }
    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;

private:
    inline static const std::string IFNULL_NAME = "ifnull";
};

class ShortCircuitCoalesceExpr final : public ShortCircuitExpr {
public:
    ENABLE_FACTORY_CREATOR(ShortCircuitCoalesceExpr);
    ShortCircuitCoalesceExpr(const TExprNode& node) : ShortCircuitExpr(node) {}
    ~ShortCircuitCoalesceExpr() override = default;
    const std::string& expr_name() const override { return COALESCE_NAME; }
    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;

private:
    inline static const std::string COALESCE_NAME = "coalesce";
};
} // namespace doris::vectorized