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

#include "generic_reader.h"

#include <glog/logging.h>

#include "olap/predicate_creator.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/exprs/vtopn_pred.h"

namespace doris::vectorized {

#include "common/compile_check_begin.h"

Status ExprPushDownHelper::_extract_predicates(const VExprSPtr& expr, int& cid, TypeDetail& detail,
                                               std::vector<Field>& values, bool& parsed) const {
    parsed = false;
    values.clear();
    if (!expr->children()[0]->is_slot_ref()) [[unlikely]] {
        return Status::OK();
    }
    const auto* slot_ref = assert_cast<const VSlotRef*>(expr->children()[0].get());
    cid = slot_ref->column_id();
    values.resize(expr->children().size() - 1);
    detail.type = remove_nullable(slot_ref->data_type())->get_primitive_type();
    detail.precision = slot_ref->data_type()->get_precision();
    detail.scale = slot_ref->data_type()->get_scale();
    if (is_string_type(detail.type)) {
        detail.len =
                assert_cast<const DataTypeString*>(remove_nullable(slot_ref->data_type()).get())
                        ->len();
    }
    for (size_t child_id = 1; child_id < expr->children().size(); child_id++) {
        auto child_expr = expr->children()[child_id];
        if (!child_expr->is_literal()) {
            return Status::OK();
        }
        const auto* literal = static_cast<const VLiteral*>(child_expr.get());
        if (literal->get_column_ptr()->is_null_at(0)) {
            continue;
        }
        values[child_id - 1] = literal->get_column_ptr()->operator[](0);
    }
    parsed = true;
    return Status::OK();
}

Status ExprPushDownHelper::convert_predicates(const VExprSPtrs& exprs,
                                              std::vector<ColumnPredicate*>& predicates,
                                              Arena& arena) {
    if (exprs.empty()) {
        return Status::OK();
    }

    int cid;
    TypeDetail detail;
    std::vector<Field> values;
    bool parsed = false;
    for (const auto& expr : exprs) {
        cid = -1;
        values.clear();
        parsed = false;
        switch (expr->node_type()) {
        case TExprNodeType::BINARY_PRED: {
            decltype(create_comparison_predicate_by_type<PredicateType::UNKNOWN>)* create = nullptr;
            if (expr->op() == TExprOpcode::EQ) {
                create = create_comparison_predicate_by_type<PredicateType::EQ>;
            } else if (expr->op() == TExprOpcode::NE) {
                create = create_comparison_predicate_by_type<PredicateType::NE>;
            } else if (expr->op() == TExprOpcode::LT) {
                create = create_comparison_predicate_by_type<PredicateType::LT>;
            } else if (expr->op() == TExprOpcode::LE) {
                create = create_comparison_predicate_by_type<PredicateType::LE>;
            } else if (expr->op() == TExprOpcode::GT) {
                create = create_comparison_predicate_by_type<PredicateType::GT>;
            } else if (expr->op() == TExprOpcode::GE) {
                create = create_comparison_predicate_by_type<PredicateType::GE>;
            } else {
                break;
            }
            RETURN_IF_ERROR(_extract_predicates(expr, cid, detail, values, parsed));
            if (parsed) {
                // TODO(gabriel): Use string view
                predicates.push_back(
                        create(detail, cid, std::string(values[0].as_string_view()), false, arena));
            }
            break;
        }
        case TExprNodeType::IN_PRED: {
            switch (expr->op()) {
            case TExprOpcode::FILTER_IN: {
                RETURN_IF_ERROR(_extract_predicates(expr, cid, detail, values, parsed));
                if (parsed) {
                    // TODO(gabriel): Use string view
                    std::vector<std::string> conditions(values.size());
                    for (size_t i = 0; i < conditions.size(); i++) {
                        conditions[i] = std::string(values[i].as_string_view());
                    }
                    predicates.push_back(create_list_predicate_by_type<PredicateType::IN_LIST>(
                            detail, cid, conditions, false, arena));
                }
                break;
            }
            default: {
                break;
            }
            }
        }
        case TExprNodeType::COMPOUND_PRED: {
            switch (expr->op()) {
            case TExprOpcode::COMPOUND_AND: {
                for (const auto& child : expr->children()) {
                    RETURN_IF_ERROR(convert_predicates({child}, predicates, arena));
                }
                break;
            }
                // FIXME(gabriel):
                //            case TExprOpcode::COMPOUND_OR: {
                //                std::ranges::for_each(expr->children(), [&](const auto& child) {
                //                    RETURN_IF_ERROR(convert_predicates({child}, predicates, arena));
                //                });
                //            }
            default: {
                break;
            }
            }
        }
        case TExprNodeType::FUNCTION_CALL: {
            auto fn_name = expr->fn().name.function_name;
            // only support `is null` and `is not null`
            if (fn_name == "is_null_pred" || fn_name == "is_not_null_pred") {
                RETURN_IF_ERROR(_extract_predicates(expr, cid, detail, values, parsed));
                if (parsed) {
                    predicates.push_back(
                            new NullPredicate(cid, true, fn_name == "is_not_null_pred"));
                }
            }
            break;
        }
        default:
            break;
        }
    }

    return Status::OK();
}

bool ExprPushDownHelper::check_expr_can_push_down(const VExprSPtr& expr) const {
    if (expr == nullptr) {
        return false;
    }

    switch (expr->node_type()) {
    case TExprNodeType::BINARY_PRED:
    case TExprNodeType::IN_PRED: {
        switch (expr->op()) {
        case TExprOpcode::GE:
        case TExprOpcode::GT:
        case TExprOpcode::LE:
        case TExprOpcode::LT:
        case TExprOpcode::EQ:
        case TExprOpcode::FILTER_IN:
            return _check_slot_can_push_down(expr) && _check_other_children_is_literal(expr);
        default: {
            return false;
        }
        }
    }
    case TExprNodeType::COMPOUND_PRED: {
        switch (expr->op()) {
        case TExprOpcode::COMPOUND_AND: {
            // at least one child can be pushed down
            return std::ranges::any_of(expr->children(), [this](const auto& child) {
                return check_expr_can_push_down(child);
            });
        }
        case TExprOpcode::COMPOUND_OR: {
            // all children must be pushed down
            return std::ranges::all_of(expr->children(), [this](const auto& child) {
                return check_expr_can_push_down(child);
            });
        }
        default: {
            return false;
        }
        }
    }
    case TExprNodeType::FUNCTION_CALL: {
        auto fn_name = expr->fn().name.function_name;
        // only support `is null` and `is not null`
        if (fn_name == "is_null_pred" || fn_name == "is_not_null_pred") {
            return _check_slot_can_push_down(expr);
        }
        return false;
    }
    default: {
        return false;
    }
    }
}

bool ExprPushDownHelper::_check_slot_can_push_down(const VExprSPtr& expr) const {
    if (!expr->children()[0]->is_slot_ref()) {
        return false;
    }

    const auto* slot_ref = assert_cast<const VSlotRef*>(expr->children()[0].get());
    // check if the slot exists in parquet file.
    if (!_exists_in_file(slot_ref)) {
        return false;
    }
    return _type_matches(slot_ref);
}

bool ExprPushDownHelper::_check_other_children_is_literal(const VExprSPtr& expr) const {
    for (size_t child_id = 1; child_id < expr->children().size(); child_id++) {
        auto child_expr = expr->children()[child_id];
        if (!child_expr->is_literal()) {
            return false;
        }
    }
    return true;
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
