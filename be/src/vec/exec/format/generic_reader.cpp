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
                                               DataTypePtr& data_type,
                                               std::vector<StringRef>& values, bool null_pred,
                                               bool& parsed) const {
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
        values.emplace_back(literal->get_column_ptr()->get_data_at(0));
        parsed = true;
    }
    return Status::OK();
}

Status ExprPushDownHelper::convert_predicates(const VExprSPtrs& exprs,
                                              std::unique_ptr<MutilColumnBlockPredicate>& root,
                                              Arena& arena) {
    if (exprs.empty()) {
        return Status::OK();
    }

    int cid;
    DataTypePtr data_type;
    std::vector<StringRef> values;
    bool parsed = false;
    for (const auto& expr : exprs) {
        cid = -1;
        values.clear();
        parsed = false;
        switch (expr->node_type()) {
        case TExprNodeType::BINARY_PRED: {
            RETURN_IF_ERROR(_extract_predicates(expr, cid, data_type, values, false, parsed));
            if (parsed) {
                std::shared_ptr<ColumnPredicate> predicate;
                if (expr->op() == TExprOpcode::EQ) {
                    predicate = create_comparison_predicate0<PredicateType::EQ>(
                            cid, data_type, values[0], false, arena);
                } else if (expr->op() == TExprOpcode::NE) {
                    predicate = create_comparison_predicate0<PredicateType::NE>(
                            cid, data_type, values[0], false, arena);
                } else if (expr->op() == TExprOpcode::LT) {
                    predicate = create_comparison_predicate0<PredicateType::LT>(
                            cid, data_type, values[0], false, arena);
                } else if (expr->op() == TExprOpcode::LE) {
                    predicate = create_comparison_predicate0<PredicateType::LE>(
                            cid, data_type, values[0], false, arena);
                } else if (expr->op() == TExprOpcode::GT) {
                    predicate = create_comparison_predicate0<PredicateType::GT>(
                            cid, data_type, values[0], false, arena);
                } else if (expr->op() == TExprOpcode::GE) {
                    predicate = create_comparison_predicate0<PredicateType::GE>(
                            cid, data_type, values[0], false, arena);
                } else {
                    break;
                }
                root->add_column_predicate(SingleColumnBlockPredicate::create_unique(predicate));
            }
            break;
        }
        case TExprNodeType::IN_PRED: {
            switch (expr->op()) {
            case TExprOpcode::FILTER_IN: {
                std::shared_ptr<HybridSetBase> set;
                RETURN_IF_ERROR(_extract_predicates(expr, cid, data_type, values, false, parsed));
                if (parsed) {
                    switch (data_type->get_primitive_type()) {
#define BUILD_SET_CASE(PType)     \
    case PType: {                 \
        set = build_set<PType>(); \
        break;                    \
    }
                        BUILD_SET_CASE(TYPE_TINYINT);
                        BUILD_SET_CASE(TYPE_SMALLINT);
                        BUILD_SET_CASE(TYPE_INT);
                        BUILD_SET_CASE(TYPE_BIGINT);
                        BUILD_SET_CASE(TYPE_LARGEINT);
                        BUILD_SET_CASE(TYPE_FLOAT);
                        BUILD_SET_CASE(TYPE_DOUBLE);
                        BUILD_SET_CASE(TYPE_CHAR);
                        BUILD_SET_CASE(TYPE_STRING);
                        BUILD_SET_CASE(TYPE_DATE);
                        BUILD_SET_CASE(TYPE_DATETIME);
                        BUILD_SET_CASE(TYPE_DATEV2);
                        BUILD_SET_CASE(TYPE_DATETIMEV2);
                        BUILD_SET_CASE(TYPE_BOOLEAN);
                        BUILD_SET_CASE(TYPE_IPV4);
                        BUILD_SET_CASE(TYPE_IPV6);
                        BUILD_SET_CASE(TYPE_DECIMALV2);
                        BUILD_SET_CASE(TYPE_DECIMAL32);
                        BUILD_SET_CASE(TYPE_DECIMAL64);
                        BUILD_SET_CASE(TYPE_DECIMAL128I);
                        BUILD_SET_CASE(TYPE_DECIMAL256);
                    case TYPE_VARCHAR: {
                        set = build_set<TYPE_STRING>();
                        break;
                    }
#undef BUILD_SET_CASE
                    default:
                        throw Exception(Status::Error<ErrorCode::INVALID_ARGUMENT>(
                                "unsupported data type in delete handler. type={}",
                                type_to_string(data_type->get_primitive_type())));
                    }
                    if (is_string_type(data_type->get_primitive_type())) {
                        for (size_t i = 0; i < values.size(); i++) {
                            set->insert(reinterpret_cast<const void*>(&values[i]));
                        }
                    } else {
                        for (size_t i = 0; i < values.size(); i++) {
                            set->insert(reinterpret_cast<const void*>(values[i].data));
                        }
                    }
                    root->add_column_predicate(SingleColumnBlockPredicate::create_unique(
                            create_in_list_predicate<PredicateType::IN_LIST>(cid, data_type, set,
                                                                             false)));
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
                    RETURN_IF_ERROR(convert_predicates({child}, root, arena));
                }
                break;
            }
            case TExprOpcode::COMPOUND_OR: {
                std::unique_ptr<MutilColumnBlockPredicate> new_root =
                        OrBlockColumnPredicate::create_unique();
                for (const auto& child : expr->children()) {
                    RETURN_IF_ERROR(convert_predicates({child}, new_root, arena));
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
                    root->add_column_predicate(SingleColumnBlockPredicate::create_unique(
                            NullPredicate::create_shared(cid, true, data_type->get_primitive_type(),
                                                         fn_name == "is_not_null_pred")));
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
