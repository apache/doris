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

#include "short_circuit_evaluation_expr.h"

#include <gen_cpp/Exprs_types.h>

#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "short_circuit_util.h"
#include "udf/udf.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

Status ShortCircuitExpr::prepare(RuntimeState* state, const RowDescriptor& desc,
                                 VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    _prepare_finished = true;
    return Status::OK();
}

Status ShortCircuitExpr::open(RuntimeState* state, VExprContext* context,
                              FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

void ShortCircuitExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    VExpr::close(context, scope);
}

std::string ShortCircuitExpr::debug_string() const {
    std::string result = expr_name() + "(";
    for (size_t i = 0; i < _children.size(); ++i) {
        if (i != 0) {
            result += ", ";
        }
        result += _children[i]->debug_string();
    }
    result += ")";
    return result;
}

ColumnPtr ShortCircuitExpr::dispatch_fill_columns(const ColumnPtr& true_column,
                                                  const Selector& true_selector,
                                                  const ColumnPtr& false_column,
                                                  const Selector& false_selector,
                                                  size_t count) const {
    ColumnPtr result_column;
    auto scalar_fill = [&](const auto& type) -> bool {
        using DataType = std::decay_t<decltype(type)>;
        result_column = ScalarFillWithSelector<DataType::PType>::fill(
                _data_type, true_column, true_selector, false_column, false_selector, count);
        return true;
    };

    if (!dispatch_switch_scalar(_data_type->get_primitive_type(), scalar_fill)) {
        result_column = NonScalarFillWithSelector::fill(_data_type, true_column, true_selector,
                                                        false_column, false_selector, count);
    }
    return result_column;
}

ColumnPtr ShortCircuitExpr::dispatch_fill_columns(
        const std::vector<ColumnAndSelector>& columns_and_selectors, size_t count) const {
    ColumnPtr result_column;
    auto scalar_fill = [&](const auto& type) -> bool {
        using DataType = std::decay_t<decltype(type)>;
        result_column = ScalarFillWithSelector<DataType::PType>::fill(_data_type,
                                                                      columns_and_selectors, count);
        return true;
    };

    if (!dispatch_switch_scalar(_data_type->get_primitive_type(), scalar_fill)) {
        result_column = NonScalarFillWithSelector::fill(_data_type, columns_and_selectors, count);
    }
    return result_column;
}

void execute_if_selector(const ColumnPtr& cond_column, size_t count, Selector& true_selector,
                         Selector& false_selector) {
    ConditionColumnView condition_view = ConditionColumnView::create(cond_column, nullptr, count);

    auto null_func = [&](size_t i) { false_selector.push_back(i); };
    auto true_func = [&](size_t i) { true_selector.push_back(i); };
    auto false_func = [&](size_t i) { false_selector.push_back(i); };

    condition_view.for_each(null_func, true_func, false_func);
}

Status ShortCircuitIfExpr::execute_column(VExprContext* context, const Block* block,
                                          Selector* selector, size_t count,
                                          ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);
    ColumnPtr cond_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, cond_column));
    DCHECK_EQ(cond_column->size(), count);

    Selector true_selector;
    Selector false_selector;
    true_selector.reserve(count);
    false_selector.reserve(count);
    execute_if_selector(cond_column, count, true_selector, false_selector);

    ColumnPtr true_column;
    RETURN_IF_ERROR(_children[1]->execute_column(context, block, &true_selector,
                                                 true_selector.size(), true_column));

    ColumnPtr false_column;
    RETURN_IF_ERROR(_children[2]->execute_column(context, block, &false_selector,
                                                 false_selector.size(), false_column));

    result_column =
            dispatch_fill_columns(true_column, true_selector, false_column, false_selector, count);
    return Status::OK();
}

void execute_ifnull_selector(const ColumnPtr& cond_column, size_t count, Selector& null_selector,
                             Selector& not_null_selector) {
    ConditionColumnNullView condition_view =
            ConditionColumnNullView::create(cond_column, nullptr, count);
    auto null_func = [&](size_t i, size_t result_index) { null_selector.push_back(result_index); };
    auto not_null_func = [&](size_t i, size_t result_index) {
        not_null_selector.push_back(result_index);
    };

    condition_view.for_each(null_func, not_null_func);
}

Status ShortCircuitIfNullExpr::execute_column(VExprContext* context, const Block* block,
                                              Selector* selector, size_t count,
                                              ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);
    ColumnPtr expr1_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, expr1_column));
    DCHECK_EQ(expr1_column->size(), count);

    Selector not_null_selector;
    Selector null_selector;
    not_null_selector.reserve(count);
    null_selector.reserve(count);

    execute_ifnull_selector(expr1_column, count, null_selector, not_null_selector);
    // filter not null part
    expr1_column =
            filter_column_with_selector(expr1_column, &not_null_selector, not_null_selector.size());

    ColumnPtr expr2_column;
    RETURN_IF_ERROR(_children[1]->execute_column(context, block, &null_selector,
                                                 null_selector.size(), expr2_column));

    result_column = dispatch_fill_columns(expr1_column, not_null_selector, expr2_column,
                                          null_selector, count);
    return Status::OK();
}

void execute_coalesce_selector(const ColumnPtr& cond_column, const Selector* cond_selector,
                               size_t count, Selector& null_selector, Selector& not_null_selector,
                               Selector& cond_self_selector) {
    ConditionColumnNullView condition_view =
            ConditionColumnNullView::create(cond_column, cond_selector, count);

    auto null_func = [&](size_t i, size_t result_index) { null_selector.push_back(result_index); };

    auto not_null_func = [&](size_t i, size_t result_index) {
        not_null_selector.push_back(result_index);
        cond_self_selector.push_back(i);
    };

    condition_view.for_each(null_func, not_null_func);
}

Status ShortCircuitCoalesceExpr::execute_column(VExprContext* context, const Block* block,
                                                Selector* selector, size_t count,
                                                ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);

    std::vector<ColumnAndSelector> columns_and_selectors;
    columns_and_selectors.resize(_children.size() +
                                 1); // the last one represents the selector for null output

    Selector* current_selector = selector;
    size_t current_count = count;

    Selector left_null_selector; // used to store the remaining null rows

    Selector self_selector; // used to store the not-null positions in the current column

    for (int64_t i = 0; i < _children.size(); ++i) {
        ColumnPtr child_column;
        RETURN_IF_ERROR(_children[i]->execute_column(context, block, current_selector,
                                                     current_count, child_column));

        DCHECK(current_selector == nullptr || current_selector->size() == child_column->size());

        Selector null_selector;
        self_selector.clear();

        execute_coalesce_selector(child_column, current_selector, current_count,
                                  null_selector /*null selector*/,
                                  columns_and_selectors[i].selector /*not null selector*/,
                                  self_selector /*self selector*/);

        columns_and_selectors[i].column =
                filter_column_with_selector(child_column, &self_selector, self_selector.size());

        DCHECK_EQ(columns_and_selectors[i].column->size(),
                  columns_and_selectors[i].selector.size());

        left_null_selector.swap(null_selector);

        // the rows that the next column needs to process are the null rows of the current column
        current_selector = &left_null_selector;
        current_count = left_null_selector.size();

        if (left_null_selector.size() == 0) {
            // all rows are filled, no need to process further
            break;
        }
    }

    // the remaining null rows at the end
    columns_and_selectors.back().selector.swap(left_null_selector);

    result_column = dispatch_fill_columns(columns_and_selectors, count);
    return Status::OK();
}

ShortCircuitCaseExpr::ShortCircuitCaseExpr(const TExprNode& node)
        : ShortCircuitExpr(node), _has_else_expr(node.case_expr.has_else_expr) {}

void execute_case_when_selector(const ColumnPtr& when_column, const Selector* when_selector,
                                size_t count, Selector& matched_selector,
                                Selector& not_matched_selector) {
    DCHECK(when_column->size() == count);
    ConditionColumnView condition_view =
            ConditionColumnView::create(when_column, when_selector, count);

    auto null_func = [&](size_t i) { not_matched_selector.push_back(i); };
    auto true_func = [&](size_t i) { matched_selector.push_back(i); };
    auto false_func = [&](size_t i) { not_matched_selector.push_back(i); };

    condition_view.for_each(null_func, true_func, false_func);
}

Status ShortCircuitCaseExpr::execute_column(VExprContext* context, const Block* block,
                                            Selector* selector, size_t count,
                                            ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);

    // Structure: WHEN expr1 THEN result1 [WHEN expr2 THEN result2 ...] [ELSE else_result]
    // _children layout: [when1, then1, when2, then2, ..., else?]
    // Number of when/then pairs = _children.size() / 2 (rounded down if has_else)
    // +1 for the else branch or null output when no else is provided
    const size_t num_branches = _children.size() / 2 + 1;
    std::vector<ColumnAndSelector> columns_and_selectors;
    columns_and_selectors.resize(num_branches);

    Selector* current_selector = selector;
    size_t current_count = count;

    Selector left_not_matched_selector; // used to store the remaining unmatched rows

    for (int64_t i = 0; i < static_cast<int64_t>(_children.size()) - _has_else_expr; i += 2) {
        ColumnPtr when_column_ptr;
        RETURN_IF_ERROR(_children[i]->execute_column(context, block, current_selector,
                                                     current_count, when_column_ptr));

        DCHECK(current_selector == nullptr || current_selector->size() == when_column_ptr->size());

        Selector not_matched_selector;

        execute_case_when_selector(when_column_ptr, current_selector, current_count,
                                   columns_and_selectors[i / 2].selector /*matched selector*/,
                                   not_matched_selector /*not matched selector*/);

        auto& then_column_ptr = columns_and_selectors[i / 2].column;

        auto& then_selector = columns_and_selectors[i / 2].selector;

        RETURN_IF_ERROR(_children[i + 1]->execute_column(context, block, &then_selector,
                                                         then_selector.size(), then_column_ptr));

        DCHECK_EQ(columns_and_selectors[i / 2].column->size(),
                  columns_and_selectors[i / 2].selector.size());

        left_not_matched_selector.swap(not_matched_selector);
        // the rows that the next column needs to process are the not-matched rows of the current column
        current_selector = &left_not_matched_selector;
        current_count = left_not_matched_selector.size();

        if (left_not_matched_selector.size() == 0) {
            // all rows are matched, no need to process further
            break;
        }
    }

    // handle the else branch
    if (_has_else_expr) {
        DCHECK_EQ(columns_and_selectors.size(), (_children.size() + 1) / 2);

        ColumnPtr else_column_ptr;
        RETURN_IF_ERROR(_children.back()->execute_column(context, block, current_selector,
                                                         current_count, else_column_ptr));
        columns_and_selectors.back().column = else_column_ptr;
        columns_and_selectors.back().selector.swap(left_not_matched_selector);
    } else {
        // no else branch, all remaining rows are null
        columns_and_selectors.back().selector.swap(left_not_matched_selector);
    }

    result_column = dispatch_fill_columns(columns_and_selectors, count);
    return Status::OK();
}

} // namespace doris::vectorized