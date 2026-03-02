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
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

// For short-circuit execution, we need to distinguish between three types of indices:
// 1. self_index: The index of the current column (the 'i' in for(int i=0; i<size; i++),
//    which always refers to the currently executing column)
// 2. executor_index: The index relative to the entire expr tree executor, passed down through
//    execute_column and ultimately used in leaf nodes (e.g., SlotRef)
// 3. current_index: The index relative to the column returned by the current expr node
//
// For if/ifnull, self_index and current_index are the same because only one column serves
// as the condition column.
//
// For coalesce/case, self_index and current_index are different because multiple columns
// serve as condition columns (however, for the first condition column execution, they are
// the same, similar to if/ifnull)
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

// Returns empty result column if count==0
// For some exprs, executing with a size-0 column may cause errors.
// These are issues with the exprs themselves, but for convenience, we handle this case uniformly in short-circuit expr.
/// TODO: Once all exprs support size-0 columns in the future, this function can be removed.
[[nodiscard]] Status try_early_return_on_empty(const VExprSPtr& expr, VExprContext* context,
                                               const Block* block, Selector* selector, size_t count,
                                               ColumnPtr& result_columnn) {
    if (count == 0) {
        result_columnn = expr->execute_type(block)->create_column();
        DCHECK_EQ(result_columnn->size(), 0);
        return Status::OK();
    }

    return expr->execute_column(context, block, selector, count, result_columnn);
}

ShortCircuitCaseExpr::ShortCircuitCaseExpr(const TExprNode& node)
        : ShortCircuitExpr(node), _has_else_expr(node.case_expr.has_else_expr) {}

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

/// TODO: Potential optimization opportunities:
// 1. The logic of IF and CASE is similar; IFNULL and COALESCE are also similar.
//    A better approach would be to keep only CASE and COALESCE, with the optimizer
//    rewriting IF to CASE and IFNULL to COALESCE.
// 2. The following functions (execute_if_selector, execute_case_selector,
//    execute_ifnull_selector, execute_coalesce_selector) could theoretically be
//    further abstracted, e.g., by introducing a "column with selector" structure.
//    However, since there are currently few places handling selectors and keeping
//    them separate makes code review easier, each function is implemented individually.

void execute_if_selector(const ColumnPtr& cond_column, const Selector* selector, size_t count,
                         Selector& matched_executor_selector, Selector& matched_self_selector,
                         Selector& not_matched_executor_selector,
                         Selector& not_matched_self_selector) {
    ConditionColumnView condition_view = ConditionColumnView::create(cond_column, selector, count);

    matched_executor_selector.reserve(count);
    matched_self_selector.reserve(count);
    not_matched_executor_selector.reserve(count);
    not_matched_self_selector.reserve(count);

    auto null_func = [&](size_t self_index, size_t executor_index) {
        not_matched_self_selector.push_back(self_index);
        not_matched_executor_selector.push_back(executor_index);
    };

    auto true_func = [&](size_t self_index, size_t executor_index) {
        matched_self_selector.push_back(self_index);
        matched_executor_selector.push_back(executor_index);
    };
    auto false_func = [&](size_t self_index, size_t executor_index) {
        not_matched_self_selector.push_back(self_index);
        not_matched_executor_selector.push_back(executor_index);
    };

    condition_view.for_each(null_func, true_func, false_func);
}

Status ShortCircuitIfExpr::execute_column(VExprContext* context, const Block* block,
                                          Selector* selector, size_t count,
                                          ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);
    ColumnPtr cond_column;
    RETURN_IF_ERROR(
            try_early_return_on_empty(_children[0], context, block, selector, count, cond_column));
    DCHECK_EQ(cond_column->size(), count);

    Selector true_executor_selector;
    Selector true_self_selector;
    Selector false_executor_selector;
    Selector false_self_selector;

    execute_if_selector(cond_column, selector, count, true_executor_selector, true_self_selector,
                        false_executor_selector, false_self_selector);

    ColumnPtr true_column;

    RETURN_IF_ERROR(try_early_return_on_empty(_children[1], context, block, &true_executor_selector,
                                              true_executor_selector.size(), true_column));
    ColumnPtr false_column;
    RETURN_IF_ERROR(try_early_return_on_empty(_children[2], context, block,
                                              &false_executor_selector,
                                              false_executor_selector.size(), false_column));

    result_column = dispatch_fill_columns(true_column, true_self_selector, false_column,
                                          false_self_selector, count);
    return Status::OK();
}

void execute_case_selector(const ColumnPtr& cond_column, const Selector* executor_selector,
                           size_t executor_count, const Selector* current_selector,
                           Selector& matched_executor_selector, Selector& matched_current_selector,
                           Selector& not_matched_executor_selector,
                           Selector& not_matched_current_selector) {
    ConditionColumnView condition_view =
            ConditionColumnView::create(cond_column, executor_selector, executor_count);
    if (current_selector == nullptr) {
        // If current_selector is nullptr, this is the first condition column execution,
        // so self_index and current_index are the same
        auto null_func = [&](size_t self_index, size_t executor_index) {
            not_matched_current_selector.push_back(self_index);
            not_matched_executor_selector.push_back(executor_index);
        };
        auto true_func = [&](size_t self_index, size_t executor_index) {
            matched_current_selector.push_back(self_index);
            matched_executor_selector.push_back(executor_index);
        };
        auto false_func = [&](size_t self_index, size_t executor_index) {
            not_matched_current_selector.push_back(self_index);
            not_matched_executor_selector.push_back(executor_index);
        };
        condition_view.for_each(null_func, true_func, false_func);
    } else {
        // If current_selector is not nullptr, this is not the first condition column execution,
        // so self_index and current_index are different.
        // We need to determine current_index based on self_index and current_selector
        const auto& current_selector_data = *current_selector;
        DCHECK_EQ(current_selector_data.size(), executor_count);

        auto null_func = [&](size_t self_index, size_t executor_index) {
            not_matched_current_selector.push_back(current_selector_data[self_index]);
            not_matched_executor_selector.push_back(executor_index);
        };
        auto true_func = [&](size_t self_index, size_t executor_index) {
            matched_current_selector.push_back(current_selector_data[self_index]);
            matched_executor_selector.push_back(executor_index);
        };
        auto false_func = [&](size_t self_index, size_t executor_index) {
            not_matched_current_selector.push_back(current_selector_data[self_index]);
            not_matched_executor_selector.push_back(executor_index);
        };
        condition_view.for_each(null_func, true_func, false_func);
    }
}

Status ShortCircuitCaseExpr::execute_column(VExprContext* context, const Block* block,
                                            Selector* selector, size_t count,
                                            ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);

    // Structure: WHEN expr1 THEN result1 [WHEN expr2 THEN result2 ...] [ELSE else_result]
    // _children layout: [when1, then1, when2, then2, ..., else?]
    //
    // Examples:
    //   CASE WHEN a THEN b END           -> children=[a,b], size=2, num_branches=2 (b + null)
    //   CASE WHEN a THEN b ELSE c END    -> children=[a,b,c], size=3, num_branches=2 (b + c)
    //   CASE WHEN a THEN b WHEN c THEN d END -> children=[a,b,c,d], size=4, num_branches=3 (b + d + null)
    //
    // num_branches = number of when/then pairs + 1 (for else or null output)
    const size_t num_branches = _children.size() / 2 + 1;
    std::vector<ColumnAndSelector> columns_and_selectors;
    columns_and_selectors.resize(num_branches);

    Selector* executor_selector = selector;
    size_t executor_count = count;

    Selector* current_selector = nullptr;

    Selector left_not_matched_executor_selector;
    Selector left_not_matched_current_selector;

    int64_t executed_branches = 0;

    for (int64_t i = 0; i < static_cast<int64_t>(_children.size()) - _has_else_expr; i += 2) {
        executed_branches++;
        ColumnPtr when_column_ptr;

        RETURN_IF_ERROR(try_early_return_on_empty(_children[i], context, block, executor_selector,
                                                  executor_count, when_column_ptr));

        DCHECK(executor_selector == nullptr ||
               executor_selector->size() == when_column_ptr->size());

        Selector matched_executor_selector;
        Selector matched_current_selector;
        Selector not_matched_executor_selector;
        Selector not_matched_current_selector;

        execute_case_selector(when_column_ptr, executor_selector, executor_count, current_selector,
                              matched_executor_selector, matched_current_selector,
                              not_matched_executor_selector, not_matched_current_selector);

        ColumnPtr then_column_ptr;
        RETURN_IF_ERROR(try_early_return_on_empty(
                _children[i + 1], context, block, &matched_executor_selector,
                matched_executor_selector.size(), then_column_ptr));

        columns_and_selectors[i / 2].column = then_column_ptr;
        columns_and_selectors[i / 2].selector.swap(matched_current_selector);

        left_not_matched_executor_selector.swap(not_matched_executor_selector);
        left_not_matched_current_selector.swap(not_matched_current_selector);

        executor_selector = &left_not_matched_executor_selector;
        executor_count = left_not_matched_executor_selector.size();
        current_selector = &left_not_matched_current_selector;

        if (executor_count == 0) {
            columns_and_selectors.resize(executed_branches);
            // All rows have been matched; no need to process other branch
            result_column = dispatch_fill_columns(columns_and_selectors, count);
            return Status::OK();
        }
    }

    // handle the else branch
    if (_has_else_expr) {
        DCHECK_EQ(columns_and_selectors.size(), (_children.size() + 1) / 2);

        ColumnPtr else_column_ptr;

        RETURN_IF_ERROR(try_early_return_on_empty(_children.back(), context, block,
                                                  executor_selector, executor_count,
                                                  else_column_ptr));
        columns_and_selectors.back().column = else_column_ptr;
        columns_and_selectors.back().selector.swap(left_not_matched_current_selector);
    } else {
        // no else branch, all remaining rows are null
        columns_and_selectors.back().selector.swap(left_not_matched_current_selector);
    }

    result_column = dispatch_fill_columns(columns_and_selectors, count);
    return Status::OK();
}

// For ifnull(expr1, expr2), we distinguish between null and not-null rows of expr1:
// - Not-null rows: Use expr1's value directly (already computed), only need self_selector
//   to filter the column. No need for executor_selector since we don't execute further.
// - Null rows: Need executor_selector to execute expr2, and current_selector for result filling.
//
// This differs from IF where both branches need to execute child expressions.

void execute_ifnull_selector(const ColumnPtr& cond_column, const Selector* selector, size_t count,
                             Selector& null_executor_selector, Selector& null_current_selector,
                             Selector& not_null_self_selector) {
    ConditionColumnNullView condition_view =
            ConditionColumnNullView::create(cond_column, selector, count);

    null_executor_selector.reserve(count);
    null_current_selector.reserve(count);
    not_null_self_selector.reserve(count);

    auto null_func = [&](size_t self_index, size_t executor_index) {
        null_current_selector.push_back(self_index);
        null_executor_selector.push_back(executor_index);
    };
    auto not_null_func = [&](size_t self_index, size_t executor_index) {
        not_null_self_selector.push_back(self_index);
    };

    condition_view.for_each(null_func, not_null_func);
}

Status ShortCircuitIfNullExpr::execute_column(VExprContext* context, const Block* block,
                                              Selector* selector, size_t count,
                                              ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);
    ColumnPtr expr1_column;
    RETURN_IF_ERROR(
            try_early_return_on_empty(_children[0], context, block, selector, count, expr1_column));
    DCHECK_EQ(expr1_column->size(), count);
    Selector null_executor_selector;
    Selector null_current_selector;
    Selector not_null_self_selector;

    execute_ifnull_selector(expr1_column, selector, count, null_executor_selector,
                            null_current_selector, not_null_self_selector);
    // filter not null part
    expr1_column = filter_column_with_selector(expr1_column, &not_null_self_selector,
                                               not_null_self_selector.size());

    ColumnPtr expr2_column;

    RETURN_IF_ERROR(try_early_return_on_empty(_children[1], context, block, &null_executor_selector,
                                              null_executor_selector.size(), expr2_column));

    result_column = dispatch_fill_columns(expr1_column, not_null_self_selector, expr2_column,
                                          null_current_selector, count);
    return Status::OK();
}

void execute_coalesce_selector(const ColumnPtr& cond_column, const Selector* executor_selector,
                               size_t executor_count, const Selector* current_selector,
                               Selector& null_executor_selector, Selector& null_current_selector,
                               Selector& not_null_current_selector,
                               Selector& not_null_self_selector) {
    ConditionColumnNullView condition_view =
            ConditionColumnNullView::create(cond_column, executor_selector, executor_count);
    if (current_selector == nullptr) {
        // If current_selector is nullptr, this is the first condition column execution,
        // so self_index and current_index are the same
        auto null_func = [&](size_t self_index, size_t executor_index) {
            null_current_selector.push_back(self_index);
            null_executor_selector.push_back(executor_index);
        };
        auto not_null_func = [&](size_t self_index, size_t executor_index) {
            not_null_current_selector.push_back(self_index);
            not_null_self_selector.push_back(self_index);
        };
        condition_view.for_each(null_func, not_null_func);
    } else {
        // If current_selector is not nullptr, this is not the first condition column execution,
        // so self_index and current_index are different.
        // We need to determine current_index based on self_index and current_selector
        const auto& current_selector_data = *current_selector;
        DCHECK_EQ(current_selector_data.size(), executor_count);
        auto null_func = [&](size_t self_index, size_t executor_index) {
            null_current_selector.push_back(current_selector_data[self_index]);
            null_executor_selector.push_back(executor_index);
        };
        auto not_null_func = [&](size_t self_index, size_t executor_index) {
            not_null_current_selector.push_back(current_selector_data[self_index]);
            not_null_self_selector.push_back(self_index);
        };
        condition_view.for_each(null_func, not_null_func);
    }
}

Status ShortCircuitCoalesceExpr::execute_column(VExprContext* context, const Block* block,
                                                Selector* selector, size_t count,
                                                ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);

    std::vector<ColumnAndSelector> columns_and_selectors;
    columns_and_selectors.resize(_children.size() +
                                 1); // the last one represents the selector for null output

    Selector* executor_selector = selector;
    size_t executor_count = count;

    Selector* current_selector = nullptr;
    Selector left_null_executor_selector;
    Selector left_null_current_selector;

    int64_t executed_branches = 0;

    for (int64_t i = 0; i < _children.size(); ++i) {
        executed_branches++;
        ColumnPtr child_column_ptr;

        RETURN_IF_ERROR(try_early_return_on_empty(_children[i], context, block, executor_selector,
                                                  executor_count, child_column_ptr));

        DCHECK(executor_selector == nullptr ||
               executor_selector->size() == child_column_ptr->size());

        Selector null_executor_selector;
        Selector null_current_selector;
        Selector not_null_current_selector;
        Selector not_null_self_selector;

        execute_coalesce_selector(child_column_ptr, executor_selector, executor_count,
                                  current_selector, null_executor_selector, null_current_selector,
                                  not_null_current_selector, not_null_self_selector);

        // use not_null_self_selector to filter child_column_ptr
        child_column_ptr = filter_column_with_selector(child_column_ptr, &not_null_self_selector,
                                                       not_null_self_selector.size());

        columns_and_selectors[i].column = child_column_ptr;
        columns_and_selectors[i].selector.swap(not_null_current_selector);

        left_null_executor_selector.swap(null_executor_selector);
        left_null_current_selector.swap(null_current_selector);

        executor_selector = &left_null_executor_selector;
        executor_count = left_null_executor_selector.size();
        current_selector = &left_null_current_selector;

        if (executor_count == 0) {
            columns_and_selectors.resize(executed_branches);
            // All rows have been matched; no need to process other branch
            result_column = dispatch_fill_columns(columns_and_selectors, count);
            return Status::OK();
        }
    }

    // the remaining null rows at the end
    columns_and_selectors.back().selector.swap(left_null_current_selector);

    result_column = dispatch_fill_columns(columns_and_selectors, count);
    return Status::OK();
}
} // namespace doris::vectorized