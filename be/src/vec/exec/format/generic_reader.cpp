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

#include "vec/exec/format/generic_reader.h"

#include "olap/predicate_creator.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/exprs/vtopn_pred.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status ExprPushDownHelper::_extract_predicates(const VExprSPtr& expr, int& cid,
                                               DataTypePtr& data_type, std::vector<Field>& values,
                                               bool null_pred, bool& parsed) const {
    parsed = false;
    values.clear();
    if (!expr->children()[0]->is_slot_ref()) [[unlikely]] {
        return Status::OK();
    }
    const auto* slot_ref = assert_cast<const VSlotRef*>(expr->children()[0].get());
    cid = slot_ref->column_id();
    values.reserve(expr->children().size() - 1);
    data_type = remove_nullable(slot_ref->data_type());
    if (null_pred) {
        DCHECK_EQ(expr->children().size(), 1);
        parsed = true;
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
        values.emplace_back(literal->get_column_ptr()->operator[](0));
        parsed = true;
    }
    return Status::OK();
}

Status ExprPushDownHelper::convert_predicates(
        const VExprSPtrs& exprs, std::vector<std::unique_ptr<ColumnPredicate>>& predicates,
        std::unique_ptr<MutilColumnBlockPredicate>& root, Arena& arena) {
    if (exprs.empty()) {
        return Status::OK();
    }

    int cid;
    DataTypePtr data_type;
    std::vector<Field> values;
    bool parsed = false;
    for (const auto& expr : exprs) {
        cid = -1;
        values.clear();
        parsed = false;
        switch (expr->node_type()) {
        case TExprNodeType::BINARY_PRED: {
            decltype(create_comparison_predicate<PredicateType::UNKNOWN>)* create = nullptr;
            if (expr->op() == TExprOpcode::EQ) {
                create = create_comparison_predicate<PredicateType::EQ>;
            } else if (expr->op() == TExprOpcode::NE) {
                create = create_comparison_predicate<PredicateType::NE>;
            } else if (expr->op() == TExprOpcode::LT) {
                create = create_comparison_predicate<PredicateType::LT>;
            } else if (expr->op() == TExprOpcode::LE) {
                create = create_comparison_predicate<PredicateType::LE>;
            } else if (expr->op() == TExprOpcode::GT) {
                create = create_comparison_predicate<PredicateType::GT>;
            } else if (expr->op() == TExprOpcode::GE) {
                create = create_comparison_predicate<PredicateType::GE>;
            } else {
                break;
            }
            RETURN_IF_ERROR(_extract_predicates(expr, cid, data_type, values, false, parsed));
            if (parsed) {
                // TODO(gabriel): Use string view
                predicates.push_back(std::unique_ptr<ColumnPredicate>(
                        create(data_type, cid, values[0].to_string(), false, arena)));
                root->add_column_predicate(
                        SingleColumnBlockPredicate::create_unique(predicates.back().get()));
            }
            break;
        }
        case TExprNodeType::IN_PRED: {
            switch (expr->op()) {
            case TExprOpcode::FILTER_IN: {
                RETURN_IF_ERROR(_extract_predicates(expr, cid, data_type, values, false, parsed));
                if (parsed) {
                    // TODO(gabriel): Use string view
                    std::vector<std::string> conditions(values.size());
                    for (size_t i = 0; i < conditions.size(); i++) {
                        conditions[i] = values[i].to_string();
                    }
                    predicates.push_back(std::unique_ptr<ColumnPredicate>(
                            create_list_predicate<PredicateType::IN_LIST>(
                                    data_type, cid, conditions, false, arena)));
                    root->add_column_predicate(
                            SingleColumnBlockPredicate::create_unique(predicates.back().get()));
                }
                break;
            }
            default: {
                break;
            }
            }
            break;
        }
        case TExprNodeType::COMPOUND_PRED: {
            switch (expr->op()) {
            case TExprOpcode::COMPOUND_AND: {
                for (const auto& child : expr->children()) {
                    RETURN_IF_ERROR(convert_predicates({child}, predicates, root, arena));
                }
                break;
            }
            case TExprOpcode::COMPOUND_OR: {
                std::unique_ptr<MutilColumnBlockPredicate> new_root =
                        OrBlockColumnPredicate::create_unique();
                for (const auto& child : expr->children()) {
                    RETURN_IF_ERROR(convert_predicates({child}, predicates, new_root, arena));
                }
                root->add_column_predicate(std::move(new_root));
                break;
            }
            default: {
                break;
            }
            }
            break;
        }
        case TExprNodeType::FUNCTION_CALL: {
            auto fn_name = expr->fn().name.function_name;
            // only support `is null` and `is not null`
            if (fn_name == "is_null_pred" || fn_name == "is_not_null_pred") {
                RETURN_IF_ERROR(_extract_predicates(expr, cid, data_type, values, true, parsed));
                if (parsed) {
                    predicates.push_back(std::unique_ptr<ColumnPredicate>(
                            new NullPredicate(cid, true, fn_name == "is_not_null_pred")));
                    root->add_column_predicate(
                            SingleColumnBlockPredicate::create_unique(predicates.back().get()));
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
