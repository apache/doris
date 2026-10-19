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

#include "exprs/vcase_expr.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <ostream>

#include "common/status.h"
#include "core/block/block.h"
#include "core/call_on_type_index.h"
#include "core/column/column.h"
#include "core/column/column_execute_util.h"
#include "core/custom_allocator.h"
#include "exec/common/util.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris {

VCaseExpr::VCaseExpr(const TExprNode& node)
        : VExpr(node),
          _has_case_expr(node.case_expr.has_case_expr),
          _has_else_expr(node.case_expr.has_else_expr) {}

Status VCaseExpr::prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_has_case_expr) {
        DORIS_CHECK(_children.size() >= 3U + _has_else_expr);
        DORIS_CHECK((_children.size() - 1 - _has_else_expr) % 2 == 0);
        DORIS_CHECK((_children.size() - 1 - _has_else_expr) / 2 <= 256);
        const auto input_type = remove_nullable(_children[0]->data_type());
        for (size_t i = 1; i < _children.size() - _has_else_expr; i += 2) {
            DORIS_CHECK(_children[i]->is_literal());
            DORIS_CHECK(!_children[i]->data_type()->is_nullable());
            const auto key_type = _children[i]->data_type();
            if (input_type->get_primitive_type() == TYPE_STRING ||
                input_type->get_primitive_type() == TYPE_VARCHAR) {
                DORIS_CHECK(key_type->get_primitive_type() == TYPE_STRING ||
                            key_type->get_primitive_type() == TYPE_VARCHAR);
            } else {
                DORIS_CHECK(input_type->equals(*key_type));
            }
        }
    }

    VExpr::register_function_context(state, context);
    _prepare_finished = true;
    return Status::OK();
}

Status VCaseExpr::open(RuntimeState* state, VExprContext* context,
                       FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void VCaseExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    VExpr::close(context, scope);
}

Status VCaseExpr::execute_column_impl(VExprContext* context, const Block* block,
                                      const Selector* selector, size_t count,
                                      ColumnPtr& result_column) const {
    if (is_const_and_have_executed()) { // const have execute in open function
        result_column = get_result_from_const(count);
        return Status::OK();
    }
    DCHECK(_open_finished || block == nullptr);

    if (_has_case_expr) {
        if (count == 0) {
            result_column = data_type()->create_column();
            return Status::OK();
        }
        Status status;
        const auto execute = [&]<typename Type>(Type) {
            constexpr auto PType = Type::PType;
            if constexpr ((is_int(PType) && PType != TYPE_LARGEINT) || is_decimalv3(PType) ||
                          PType == TYPE_STRING) {
                status =
                        _execute_simple_case<PType>(context, block, selector, count, result_column);
                return true;
            }
            return false;
        };
        const auto input_type = remove_nullable(_children[0]->data_type())->get_primitive_type();
        if (input_type == TYPE_CHAR || !dispatch_switch_all(input_type, execute)) {
            return Status::NotSupported("simple case input type {} is not supported",
                                        _children[0]->data_type()->get_name());
        }
        return status;
    }

    size_t rows_count = count;
    std::vector<ColumnPtr> when_columns;
    std::vector<ColumnPtr> then_columns;

    if (_has_else_expr) {
        ColumnPtr else_column_ptr;
        RETURN_IF_ERROR(
                _children.back()->execute_column(context, block, selector, count, else_column_ptr));
        then_columns.emplace_back(else_column_ptr);
    } else {
        then_columns.emplace_back(nullptr);
    }

    for (int i = 0; i < _children.size() - _has_else_expr; i += 2) {
        ColumnPtr when_column_ptr;
        RETURN_IF_ERROR(
                _children[i]->execute_column(context, block, selector, count, when_column_ptr));
        if (calculate_false_number(when_column_ptr) == rows_count) {
            continue;
        }
        when_columns.emplace_back(when_column_ptr);
        ColumnPtr then_column_ptr;
        RETURN_IF_ERROR(
                _children[i + 1]->execute_column(context, block, selector, count, then_column_ptr));
        then_columns.emplace_back(then_column_ptr);
    }

    if (then_columns.size() > UINT16_MAX) {
        return Status::NotSupported(
                "case when do not support more than UINT16_MAX then conditions");
    } else if (then_columns.size() > UINT8_MAX) {
        result_column = _execute_impl<uint16_t>(when_columns, then_columns, rows_count);
    } else {
        result_column = _execute_impl<uint8_t>(when_columns, then_columns, rows_count);
    }
    if (result_column->size() != count) {
        return Status::InternalError("case when result column size {} not equal input count {}",
                                     result_column->size(), count);
    }
    return Status::OK();
}

template <PrimitiveType PType>
Status VCaseExpr::_execute_simple_case(VExprContext* context, const Block* block,
                                       const Selector* selector, size_t count,
                                       ColumnPtr& result_column) const {
    const size_t branch_count = (_children.size() - 1 - _has_else_expr) / 2;
    ColumnPtr input;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, input));
    if (input->only_null()) {
        if (!_has_else_expr) {
            result_column = data_type()->create_column_const(count, Field());
            return Status::OK();
        }
        return _children.back()->execute_column(context, block, selector, count, result_column);
    }
    const auto input_view = ColumnView<PType>::create(input);
    DorisVector<uint16_t> selected(count, 0);
    std::vector<ColumnPtr> then_columns(branch_count + 1);
    if (_has_else_expr) {
        RETURN_IF_ERROR(
                _children.back()->execute_column(context, block, selector, count, then_columns[0]));
    }
    for (size_t branch = 0; branch < branch_count; ++branch) {
        ColumnPtr when_column;
        RETURN_IF_ERROR(_children[1 + branch * 2]->execute_column(context, block, selector, count,
                                                                  when_column));
        const auto key_view = ColumnView<PType>::create(when_column);
        DCHECK(key_view.is_const);
        const auto key = key_view.value_at(0);
        const auto branch_index = static_cast<uint16_t>(branch + 1);
        // Keep searched CASE's batch skipping, including shadowed duplicate keys.
        // A matching THEN still evaluates the entire batch, not just selected rows.
        bool has_match = false;
        if (input_view.is_const) {
            has_match = input_view.value_at(0) == key;
            if (has_match && selected[0] == 0) {
                std::ranges::fill(selected, branch_index);
            }
        } else {
            for (size_t row = 0; row < count; ++row) {
                // Compare before masking NULL rows to keep this loop branch-free and
                // friendly to automatic vectorization.
                bool matches = input_view.value_at(row) == key;
                matches &= !input_view.is_null_at(row);
                has_match |= matches;
                selected[row] = matches && selected[row] == 0 ? branch_index : selected[row];
            }
        }
        if (has_match) {
            RETURN_IF_ERROR(_children[2 + branch * 2]->execute_column(
                    context, block, selector, count, then_columns[branch + 1]));
        }
    }
    result_column = _execute_update_result<uint16_t>(selected.data(), then_columns, count);
    return Status::OK();
}

const std::string& VCaseExpr::expr_name() const {
    return EXPR_NAME;
}

std::string VCaseExpr::debug_string() const {
    std::stringstream out;
    out << "CaseExpr(has_case_expr=" << _has_case_expr << " has_else_expr=" << _has_else_expr
        << " function=" << FUNCTION_NAME << "){";
    bool first = true;
    for (const auto& input_expr : children()) {
        if (first) {
            first = false;
        } else {
            out << ",";
        }
        out << input_expr->debug_string();
    }
    out << "}";
    return out.str();
}

} // namespace doris
