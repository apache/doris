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

void execute_if_selector(const ColumnPtr& cond_column, size_t count, Selector& true_selector,
                         Selector& false_selector) {
    if (const auto* column_nullable = check_and_get_column<ColumnNullable>(cond_column.get())) {
        const auto& null_map = column_nullable->get_null_map_data();
        const auto& nested_column = column_nullable->get_nested_column_ptr();
        const auto* column_uint8 = assert_cast<const ColumnBool*>(nested_column.get());
        const auto& data = column_uint8->get_data();
        for (size_t i = 0; i < count; ++i) {
            if (null_map[i]) {
                false_selector.push_back(i);
            } else {
                if (data[i]) {
                    true_selector.push_back(i);
                } else {
                    false_selector.push_back(i);
                }
            }
        }
    } else {
        const auto* column_uint8 = assert_cast<const ColumnBool*>(cond_column.get());
        const auto& data = column_uint8->get_data();
        for (size_t i = 0; i < count; ++i) {
            if (data[i]) {
                true_selector.push_back(i);
            } else {
                false_selector.push_back(i);
            }
        }
    }
}

Status ShortCircuitIfExpr::execute_column(VExprContext* context, const Block* block,
                                          Selector* selector, size_t count,
                                          ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);
    ColumnPtr cond_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, cond_column));
    DCHECK_EQ(cond_column->size(), count);
    cond_column = cond_column->convert_to_full_column_if_const();

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

    auto vec_exec = [&](const auto& type) -> bool {
        using DataType = std::decay_t<decltype(type)>;
        result_column = ScalarFillWithSelector<DataType::PType>::fill(
                _data_type, true_column, true_selector, false_column, false_selector, count);
        return true;
    };

    if (!dispatch_switch_scalar(_data_type->get_primitive_type(), vec_exec)) {
        result_column = NonScalarFillWithSelector::fill(_data_type, true_column, true_selector,
                                                        false_column, false_selector, count);
    }
    return Status::OK();
}

Status ShortCircuitIfNullExpr::execute_column(VExprContext* context, const Block* block,
                                              Selector* selector, size_t count,
                                              ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);
    ColumnPtr expr1_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, expr1_column));
    DCHECK_EQ(expr1_column->size(), count);
    expr1_column = expr1_column->convert_to_full_column_if_const();

    Selector not_null_selector;
    Selector null_selector;
    not_null_selector.reserve(count);
    null_selector.reserve(count);

    if (const auto* column_nullable = check_and_get_column<ColumnNullable>(expr1_column.get())) {
        const auto& null_map = column_nullable->get_null_map_data();
        for (size_t i = 0; i < count; ++i) {
            if (null_map[i]) {
                null_selector.push_back(i);
            } else {
                not_null_selector.push_back(i);
            }
        }
    } else {
        /// TODO: This optimization has already been implemented in the FE,
        // so we probably don't need to handle it here. Should we just raise an error?
        result_column = expr1_column;
        return Status::OK();
    }

    // filter not null part
    expr1_column =
            filter_column_with_selector(expr1_column, &not_null_selector, not_null_selector.size());

    ColumnPtr expr2_column;
    RETURN_IF_ERROR(_children[1]->execute_column(context, block, &null_selector,
                                                 null_selector.size(), expr2_column));

    auto vec_exec = [&](const auto& type) -> bool {
        using DataType = std::decay_t<decltype(type)>;
        result_column = ScalarFillWithSelector<DataType::PType>::fill(
                _data_type, expr1_column, not_null_selector, expr2_column, null_selector, count);
        return true;
    };

    if (!dispatch_switch_scalar(_data_type->get_primitive_type(), vec_exec)) {
        result_column = NonScalarFillWithSelector::fill(_data_type, expr1_column, not_null_selector,
                                                        expr2_column, null_selector, count);
    }
    return Status::OK();
}

/*

coalesce 的nullable属性
    public boolean nullable() {
        for (Expression argument : children) {
            if (!argument.nullable()) {
                return false;
            }
        }
        return true;
    }
    如果所有参数都都有nullable，那么coalesce的结果也是nullable
    如果存在一个参数是不nullable，那么coalesce的结果就是不nullable（因为至少都可以从这个参数取到值）
*/

// 输入一个条件列，和这个条件列的selector(如果selector为nullptr，则表示全量)
// 注意这里的selecotr是最终的result的位置
// 输出两个selector，分别表示条件列中null和not null的位置
// cond_self_selector 用来存储 cond_column 中not null 的位置，在调用完这个函数后，需要利用这个来filter cond_column列
void execute_coalesce_selector(const ColumnPtr& cond_column, const Selector* cond_selector,
                               size_t count, Selector& null_selector, Selector& not_null_selector,
                               Selector& cond_self_selector) {
    if (const auto* column_nullable = check_and_get_column<ColumnNullable>(cond_column.get())) {
        const auto& null_map = column_nullable->get_null_map_data();
        if (cond_selector == nullptr) {
            for (size_t i = 0; i < count; ++i) {
                if (null_map[i]) {
                    null_selector.push_back(i);
                } else {
                    not_null_selector.push_back(i);
                    cond_self_selector.push_back(i);
                }
            }
        } else {
            DCHECK_EQ(cond_selector->size(), count);
            for (size_t i = 0; i < cond_selector->size(); ++i) {
                auto result_index = (*cond_selector)[i];
                if (null_map[i]) {
                    null_selector.push_back(result_index);
                } else {
                    not_null_selector.push_back(result_index);
                    cond_self_selector.push_back(i);
                }
            }
        }
    } else {
        // all not null
        if (cond_selector == nullptr) {
            for (size_t i = 0; i < count; ++i) {
                not_null_selector.push_back(i);
                cond_self_selector.push_back(i);
            }
        } else {
            for (size_t i = 0; i < cond_selector->size(); ++i) {
                not_null_selector.push_back((*cond_selector)[i]);
                cond_self_selector.push_back(i);
            }
        }
    }
}

Status ShortCircuitCoalesceExpr::execute_column(VExprContext* context, const Block* block,
                                                Selector* selector, size_t count,
                                                ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);

    std::vector<ColumnAndSelector> columns_and_selectors;
    columns_and_selectors.resize(_children.size() + 1); // 最后一个表示输出null的selector

    Selector* current_selector = selector;
    size_t current_count = count;

    Selector left_null_selector; // 用来存储还剩下的null行

    Selector self_selector; // 用来存储当前列中not null的位置

    for (int i = 0; i < _children.size(); ++i) {
        ColumnPtr child_column;
        RETURN_IF_ERROR(_children[i]->execute_column(context, block, current_selector,
                                                     current_count, child_column));
        child_column = child_column->convert_to_full_column_if_const();

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

        // 下一个列需要处理的行，是当前的列的null行
        current_selector = &left_null_selector;
        current_count = left_null_selector.size();
    }

    // 最后剩下的null行
    columns_and_selectors.back().selector.swap(left_null_selector);

    auto vec_exec = [&](const auto& type) -> bool {
        using DataType = std::decay_t<decltype(type)>;
        result_column = ScalarFillWithSelector<DataType::PType>::fill(_data_type,
                                                                      columns_and_selectors, count);
        return true;
    };

    if (!dispatch_switch_scalar(_data_type->get_primitive_type(), vec_exec)) {
        result_column = NonScalarFillWithSelector::fill(_data_type, columns_and_selectors, count);
    }

    return Status::OK();
}

ShortCircuitCaseExpr::ShortCircuitCaseExpr(const TExprNode& node)
        : ShortCircuitExpr(node), _has_else_expr(node.case_expr.has_else_expr) {}

void execute_case_when_selector(const ColumnPtr& when_column, const Selector* when_selector,
                                size_t count, Selector& matched_selector,
                                Selector& not_matched_selector) {
    if (const auto* column_nullable = check_and_get_column<ColumnNullable>(when_column.get())) {
        const auto& null_map = column_nullable->get_null_map_data();
        const auto& nested_column = column_nullable->get_nested_column_ptr();
        const auto* column_uint8 = assert_cast<const ColumnBool*>(nested_column.get());
        const auto& boolean_data = column_uint8->get_data();
        if (when_selector == nullptr) {
            for (size_t i = 0; i < count; ++i) {
                if (null_map[i]) {
                    not_matched_selector.push_back(i);
                } else {
                    if (boolean_data[i]) {
                        matched_selector.push_back(i);
                    } else {
                        not_matched_selector.push_back(i);
                    }
                }
            }
        } else {
            DCHECK_EQ(when_selector->size(), count);
            for (size_t i = 0; i < when_selector->size(); ++i) {
                auto result_index = (*when_selector)[i];
                if (null_map[i]) {
                    not_matched_selector.push_back(result_index);
                } else {
                    if (boolean_data[i]) {
                        matched_selector.push_back(result_index);
                    } else {
                        not_matched_selector.push_back(result_index);
                    }
                }
            }
        }
    } else {
        const auto* column_uint8 = assert_cast<const ColumnBool*>(when_column.get());
        const auto& boolean_data = column_uint8->get_data();
        if (when_selector == nullptr) {
            for (size_t i = 0; i < count; ++i) {
                if (boolean_data[i]) {
                    matched_selector.push_back(i);
                } else {
                    not_matched_selector.push_back(i);
                }
            }
        } else {
            DCHECK_EQ(when_selector->size(), count);
            for (size_t i = 0; i < when_selector->size(); ++i) {
                auto result_index = (*when_selector)[i];
                if (boolean_data[i]) {
                    matched_selector.push_back(result_index);
                } else {
                    not_matched_selector.push_back(result_index);
                }
            }
        }
    }
}

Status ShortCircuitCaseExpr::execute_column(VExprContext* context, const Block* block,
                                            Selector* selector, size_t count,
                                            ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);

    std::vector<ColumnAndSelector> columns_and_selectors;     // 存储每个then列和对应的selector
    columns_and_selectors.resize((_children.size()) / 2 + 1); //

    Selector* current_selector = selector;
    size_t current_count = count;

    Selector left_not_matched_selector; // 用来存储还剩下的没有匹配的行

    for (int i = 0; i < _children.size() - _has_else_expr; i += 2) {
        ColumnPtr when_column_ptr;
        RETURN_IF_ERROR(_children[i]->execute_column(context, block, current_selector,
                                                     current_count, when_column_ptr));

        when_column_ptr = when_column_ptr->convert_to_full_column_if_const();

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
        // 下一个列需要处理的行，是当前的列的not matched行
        current_selector = &left_not_matched_selector;
        current_count = left_not_matched_selector.size();
    }

    // 处理else分支
    if (_has_else_expr) {
        DCHECK_EQ(columns_and_selectors.size(), (_children.size() + 1) / 2);

        ColumnPtr else_column_ptr;
        RETURN_IF_ERROR(_children.back()->execute_column(context, block, current_selector,
                                                         current_count, else_column_ptr));
        columns_and_selectors.back().column = else_column_ptr;
        columns_and_selectors.back().selector.swap(left_not_matched_selector);
    } else {
        // 没有else分支，剩下的行全部是null
        columns_and_selectors.back().selector.swap(left_not_matched_selector);
    }

    auto vec_exec = [&](const auto& type) -> bool {
        using DataType = std::decay_t<decltype(type)>;
        result_column = ScalarFillWithSelector<DataType::PType>::fill(_data_type,
                                                                      columns_and_selectors, count);
        return true;
    };

    if (!dispatch_switch_scalar(_data_type->get_primitive_type(), vec_exec)) {
        result_column = NonScalarFillWithSelector::fill(_data_type, columns_and_selectors, count);
    }

    return Status::OK();
}

} // namespace doris::vectorized