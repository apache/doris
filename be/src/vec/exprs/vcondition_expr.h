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
#include "vec/core/column_numbers.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/function.h"
#include "vec/functions/if.h"
namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;
} // namespace doris

namespace doris::vectorized {

class Block;
class VExprContext;

class VConditionExpr : public VExpr {
public:
    VConditionExpr(const TExprNode& node) : VExpr(node) {}
    ~VConditionExpr() override = default;

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) final;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) final;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) final;

    std::string debug_string() const override;

protected:
    static size_t count_true_with_notnull(const ColumnPtr& col);
};

class VectorizedIfExpr : public VConditionExpr {
    ENABLE_FACTORY_CREATOR(VectorizedIfExpr);

public:
    VectorizedIfExpr(const TExprNode& node) : VConditionExpr(node) {}

    Status execute_column(VExprContext* context, const Block* block, size_t count,
                          ColumnPtr& result_column) const override;

    const std::string& expr_name() const override { return IF_NAME; }
    inline static const std::string IF_NAME = "if";

protected:
    Status _execute_impl_internal(Block& block, const ColumnNumbers& arguments, uint32_t result,
                                  size_t input_rows_count) const;

private:
    template <PrimitiveType PType>
    Status execute_basic_type(Block& block, const ColumnUInt8* cond_col,
                              const ColumnWithTypeAndName& then_col,
                              const ColumnWithTypeAndName& else_col, uint32_t result,
                              Status& status) const {
        if (then_col.type->get_primitive_type() != else_col.type->get_primitive_type()) {
            return Status::InternalError(
                    "then and else column type must be same for function {} , but got {} , {}",
                    expr_name(), then_col.type->get_name(), else_col.type->get_name());
        }

        auto res_column =
                NumIfImpl<PType>::execute_if(cond_col->get_data(), then_col.column, else_col.column,
                                             block.get_by_position(result).type->get_scale());
        if (!res_column) {
            return Status::InternalError("unexpected args column {} , {} , of function {}",
                                         then_col.column->get_name(), else_col.column->get_name(),
                                         expr_name());
        }
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

    Status execute_generic(Block& block, const ColumnUInt8* cond_col,
                           const ColumnWithTypeAndName& then_col_type_name,
                           const ColumnWithTypeAndName& else_col_type_name, uint32_t result,
                           size_t input_row_count) const;

    Status execute_for_null_then_else(Block& block, const ColumnWithTypeAndName& arg_cond,
                                      const ColumnWithTypeAndName& arg_then,
                                      const ColumnWithTypeAndName& arg_else, uint32_t result,
                                      size_t input_rows_count, bool& handled) const;

    Status execute_for_null_condition(Block& block, const ColumnNumbers& arguments,
                                      const ColumnWithTypeAndName& arg_cond,
                                      const ColumnWithTypeAndName& arg_then,
                                      const ColumnWithTypeAndName& arg_else, uint32_t result,
                                      bool& handled) const;

    Status execute_for_nullable_then_else(Block& block, const ColumnWithTypeAndName& arg_cond,
                                          const ColumnWithTypeAndName& arg_then,
                                          const ColumnWithTypeAndName& arg_else, uint32_t result,
                                          size_t input_rows_count, bool& handled) const;
};

class VectorizedIfNullExpr : public VectorizedIfExpr {
    ENABLE_FACTORY_CREATOR(VectorizedIfNullExpr);

public:
    VectorizedIfNullExpr(const TExprNode& node) : VectorizedIfExpr(node) {}
    const std::string& expr_name() const override { return IF_NULL_NAME; }
    inline static const std::string IF_NULL_NAME = "ifnull";

    Status execute_column(VExprContext* context, const Block* block, size_t count,
                          ColumnPtr& result_column) const override;
};

class VectorizedCoalesceExpr : public VConditionExpr {
    ENABLE_FACTORY_CREATOR(VectorizedCoalesceExpr);

public:
    Status execute_column(VExprContext* context, const Block* block, size_t count,
                          ColumnPtr& result_column) const override;
    VectorizedCoalesceExpr(const TExprNode& node) : VConditionExpr(node) {}
    const std::string& expr_name() const override { return NAME; }
    inline static const std::string NAME = "coalesce";
};

} // namespace doris::vectorized
