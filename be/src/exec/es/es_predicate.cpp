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

#include "exec/es/es_predicate.h"

#include <gutil/strings/substitute.h>
#include <stdint.h>

#include <boost/algorithm/string.hpp>
#include <map>
#include <sstream>

#include "common/logging.h"
#include "common/status.h"
#include "exec/es/es_query_builder.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/in_predicate.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/olap_common.h"
#include "olap/utils.h"
#include "runtime/client_cache.h"
#include "runtime/datetime_value.h"
#include "runtime/large_int_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris {

using namespace std;

#define RETURN_ERROR_IF_EXPR_IS_NOT_SLOTREF(expr)                                          \
    do {                                                                                   \
        const Expr* expr_without_cast = Expr::expr_without_cast(expr);                     \
        if (expr_without_cast->node_type() != TExprNodeType::SLOT_REF) {                   \
            return Status::InternalError("build disjuncts failed: child is not slot ref"); \
        }                                                                                  \
    } while (false)

std::string ExtLiteral::value_to_string() {
    std::stringstream ss;
    switch (_type) {
    case TYPE_TINYINT:
        ss << std::to_string(get_byte());
        break;
    case TYPE_SMALLINT:
        ss << std::to_string(get_short());
        break;
    case TYPE_INT:
        ss << std::to_string(get_int());
        break;
    case TYPE_BIGINT:
        ss << std::to_string(get_long());
        break;
    case TYPE_FLOAT:
        ss << std::to_string(get_float());
        break;
    case TYPE_DOUBLE:
        ss << std::to_string(get_double());
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        ss << get_string();
        break;
    case TYPE_DATE:
    case TYPE_DATETIME:
        ss << get_date_string();
        break;
    case TYPE_BOOLEAN:
        ss << std::to_string(get_bool());
        break;
    case TYPE_DECIMALV2:
        ss << get_decimalv2_string();
        break;
    case TYPE_LARGEINT:
        ss << get_largeint_string();
        break;
    default:
        DCHECK(false);
        break;
    }
    return ss.str();
}

ExtLiteral::~ExtLiteral() {}

int8_t ExtLiteral::get_byte() {
    DCHECK(_type == TYPE_TINYINT);
    return *(reinterpret_cast<int8_t*>(_value));
}

int16_t ExtLiteral::get_short() {
    DCHECK(_type == TYPE_SMALLINT);
    return *(reinterpret_cast<int16_t*>(_value));
}

int32_t ExtLiteral::get_int() {
    DCHECK(_type == TYPE_INT);
    return *(reinterpret_cast<int32_t*>(_value));
}

int64_t ExtLiteral::get_long() {
    DCHECK(_type == TYPE_BIGINT);
    return *(reinterpret_cast<int64_t*>(_value));
}

float ExtLiteral::get_float() {
    DCHECK(_type == TYPE_FLOAT);
    return *(reinterpret_cast<float*>(_value));
}

double ExtLiteral::get_double() {
    DCHECK(_type == TYPE_DOUBLE);
    return *(reinterpret_cast<double*>(_value));
}

std::string ExtLiteral::get_string() {
    DCHECK(_type == TYPE_VARCHAR || _type == TYPE_CHAR || _type == TYPE_STRING);
    return (reinterpret_cast<StringValue*>(_value))->to_string();
}

std::string ExtLiteral::get_date_string() {
    DCHECK(_type == TYPE_DATE || _type == TYPE_DATETIME);
    DateTimeValue date_value = *reinterpret_cast<DateTimeValue*>(_value);
    if (_type == TYPE_DATE) {
        date_value.cast_to_date();
    }

    char str[MAX_DTVALUE_STR_LEN];
    date_value.to_string(str);
    return std::string(str, strlen(str));
}

bool ExtLiteral::get_bool() {
    DCHECK(_type == TYPE_BOOLEAN);
    return *(reinterpret_cast<bool*>(_value));
}

std::string ExtLiteral::get_decimalv2_string() {
    DCHECK(_type == TYPE_DECIMALV2);
    return reinterpret_cast<DecimalV2Value*>(_value)->to_string();
}

std::string ExtLiteral::get_largeint_string() {
    DCHECK(_type == TYPE_LARGEINT);
    return LargeIntValue::to_string(*reinterpret_cast<__int128*>(_value));
}

EsPredicate::EsPredicate(ExprContext* context, const TupleDescriptor* tuple_desc, ObjectPool* pool)
        : _context(context), _tuple_desc(tuple_desc), _es_query_status(Status::OK()), _pool(pool) {}

EsPredicate::~EsPredicate() {
    for (int i = 0; i < _disjuncts.size(); i++) {
        delete _disjuncts[i];
    }
    _disjuncts.clear();
}

Status EsPredicate::build_disjuncts_list() {
    return build_disjuncts_list(_context->root());
}

// make sure to build by build_disjuncts_list
const std::vector<ExtPredicate*>& EsPredicate::get_predicate_list() const {
    return _disjuncts;
}

static bool ignore_cast(const SlotDescriptor* slot, const Expr* expr) {
    if (slot->type().is_date_type() && expr->type().is_date_type()) {
        return true;
    }
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
        return true;
    }
    return false;
}

static bool is_literal_node(const Expr* expr) {
    switch (expr->node_type()) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::LARGE_INT_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DATE_LITERAL:
        return true;
    default:
        return false;
    }
}

Status EsPredicate::build_disjuncts_list(const Expr* conjunct) {
    // process binary predicate
    if (TExprNodeType::BINARY_PRED == conjunct->node_type()) {
        if (conjunct->children().size() != 2) {
            return Status::InternalError("build disjuncts failed: number of children is not 2");
        }
        SlotRef* slot_ref = nullptr;
        TExprOpcode::type op;
        Expr* expr = nullptr;
        // k1 = 2  k1 is float (marked for processing later),
        // doris on es should ignore this doris native cast transformation, we push down this `cast` to elasticsearch
        // conjunct->get_child(0)->node_type() return CAST_EXPR
        // conjunct->get_child(1)->node_type()return FLOAT_LITERAL
        // the left child is literal and right child is SlotRef maybe not happened, but here we just process
        // this situation regardless of the rewrite logic from the FE's Query Engine
        if (TExprNodeType::SLOT_REF == conjunct->get_child(0)->node_type() ||
            TExprNodeType::CAST_EXPR == conjunct->get_child(0)->node_type()) {
            expr = conjunct->get_child(1);
            // process such as sub-query: select * from (select split_part(k, "_", 1) as new_field from table) t where t.new_field > 1;
            RETURN_ERROR_IF_EXPR_IS_NOT_SLOTREF(conjunct->get_child(0));
            // process cast expr, such as:
            // k (float) > 2.0, k(int) > 3.2
            slot_ref = (SlotRef*)Expr::expr_without_cast(conjunct->get_child(0));
            op = conjunct->op();
        } else if (TExprNodeType::SLOT_REF == conjunct->get_child(1)->node_type() ||
                   TExprNodeType::CAST_EXPR == conjunct->get_child(1)->node_type()) {
            expr = conjunct->get_child(0);
            RETURN_ERROR_IF_EXPR_IS_NOT_SLOTREF(conjunct->get_child(1));
            slot_ref = (SlotRef*)Expr::expr_without_cast(conjunct->get_child(1));
            op = conjunct->op();
        } else {
            return Status::InternalError("build disjuncts failed: no SLOT_REF child");
        }

        const SlotDescriptor* slot_desc = get_slot_desc(slot_ref);
        if (slot_desc == nullptr) {
            return Status::InternalError("build disjuncts failed: slot_desc is null");
        }

        if (!is_literal_node(expr)) {
            return Status::InternalError("build disjuncts failed: expr is not literal type");
        }

        ExtLiteral literal(expr->type().type, _context->get_value(expr, nullptr));
        std::string col = slot_desc->col_name();
        if (_field_context.find(col) != _field_context.end()) {
            col = _field_context[col];
        }
        ExtPredicate* predicate = new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, col,
                                                         slot_desc->type(), op, literal);

        _disjuncts.push_back(predicate);
        return Status::OK();
    }
    // process function call predicate: esquery, is_null_pred, is_not_null_pred
    if (TExprNodeType::FUNCTION_CALL == conjunct->node_type()) {
        std::string fname = conjunct->fn().name.function_name;
        if (fname == "esquery") {
            if (conjunct->children().size() != 2) {
                return Status::InternalError("build disjuncts failed: number of children is not 2");
            }
            Expr* expr = conjunct->get_child(1);
            ExtLiteral literal(expr->type().type, _context->get_value(expr, nullptr));
            std::vector<ExtLiteral> query_conditions;
            query_conditions.emplace_back(literal);
            std::vector<ExtColumnDesc> cols;
            ExtPredicate* predicate = new ExtFunction(TExprNodeType::FUNCTION_CALL, "esquery", cols,
                                                      query_conditions);
            if (_es_query_status.ok()) {
                _es_query_status = BooleanQueryBuilder::check_es_query(*(ExtFunction*)predicate);
                if (!_es_query_status.ok()) {
                    delete predicate;
                    return _es_query_status;
                }
            }
            _disjuncts.push_back(predicate);
        } else if (fname == "is_null_pred" || fname == "is_not_null_pred") {
            if (conjunct->children().size() != 1) {
                return Status::InternalError("build disjuncts failed: number of children is not 1");
            }
            // such as sub-query: select * from (select split_part(k, "_", 1) as new_field from table) t where t.new_field > 1;
            // conjunct->get_child(0)->node_type() == TExprNodeType::FUNCTION_CALL, at present doris on es can not support push down function
            RETURN_ERROR_IF_EXPR_IS_NOT_SLOTREF(conjunct->get_child(0));
            SlotRef* slot_ref = (SlotRef*)(conjunct->get_child(0));
            const SlotDescriptor* slot_desc = get_slot_desc(slot_ref);
            if (slot_desc == nullptr) {
                return Status::InternalError("build disjuncts failed: no SLOT_REF child");
            }
            bool is_not_null = fname == "is_not_null_pred" ? true : false;
            std::string col = slot_desc->col_name();
            if (_field_context.find(col) != _field_context.end()) {
                col = _field_context[col];
            }
            // use TExprNodeType::IS_NULL_PRED for BooleanQueryBuilder translate
            ExtIsNullPredicate* predicate = new ExtIsNullPredicate(TExprNodeType::IS_NULL_PRED, col,
                                                                   slot_desc->type(), is_not_null);
            _disjuncts.push_back(predicate);
        } else if (fname == "like") {
            if (conjunct->children().size() != 2) {
                return Status::InternalError("build disjuncts failed: number of children is not 2");
            }
            SlotRef* slot_ref = nullptr;
            Expr* expr = nullptr;
            if (TExprNodeType::SLOT_REF == conjunct->get_child(0)->node_type()) {
                expr = conjunct->get_child(1);
                slot_ref = (SlotRef*)(conjunct->get_child(0));
            } else if (TExprNodeType::SLOT_REF == conjunct->get_child(1)->node_type()) {
                expr = conjunct->get_child(0);
                slot_ref = (SlotRef*)(conjunct->get_child(1));
            } else {
                return Status::InternalError("build disjuncts failed: no SLOT_REF child");
            }
            const SlotDescriptor* slot_desc = get_slot_desc(slot_ref);
            if (slot_desc == nullptr) {
                return Status::InternalError("build disjuncts failed: slot_desc is null");
            }

            PrimitiveType type = expr->type().type;
            if (type != TYPE_VARCHAR && type != TYPE_CHAR && type != TYPE_STRING) {
                return Status::InternalError("build disjuncts failed: like value is not a string");
            }
            std::string col = slot_desc->col_name();
            if (_field_context.find(col) != _field_context.end()) {
                col = _field_context[col];
            }
            ExtLiteral literal(type, _context->get_value(expr, nullptr));
            ExtPredicate* predicate =
                    new ExtLikePredicate(TExprNodeType::LIKE_PRED, col, slot_desc->type(), literal);

            _disjuncts.push_back(predicate);
        } else {
            std::stringstream ss;
            ss << "can not process function predicate[ " << fname << " ]";
            return Status::InternalError(ss.str());
        }
        return Status::OK();
    }

    if (TExprNodeType::IN_PRED == conjunct->node_type()) {
        // the op code maybe FILTER_NEW_IN, it means there is function in list
        // like col_a in (abs(1))
        if (TExprOpcode::FILTER_IN != conjunct->op() &&
            TExprOpcode::FILTER_NOT_IN != conjunct->op()) {
            return Status::InternalError(
                    "build disjuncts failed: "
                    "opcode in IN_PRED is neither FILTER_IN nor FILTER_NOT_IN");
        }

        std::vector<ExtLiteral> in_pred_values;
        const InPredicate* pred = static_cast<const InPredicate*>(conjunct);
        const Expr* expr = Expr::expr_without_cast(pred->get_child(0));
        if (expr->node_type() != TExprNodeType::SLOT_REF) {
            return Status::InternalError("build disjuncts failed: node type is not slot ref");
        }

        const SlotDescriptor* slot_desc = get_slot_desc((const SlotRef*)expr);
        if (slot_desc == nullptr) {
            return Status::InternalError("build disjuncts failed: slot_desc is null");
        }

        if (pred->get_child(0)->type().type != slot_desc->type().type) {
            if (!ignore_cast(slot_desc, pred->get_child(0))) {
                return Status::InternalError("build disjuncts failed");
            }
        }

        HybridSetBase::IteratorBase* iter = pred->hybrid_set()->begin();
        while (iter->has_next()) {
            if (nullptr == iter->get_value()) {
                return Status::InternalError("build disjuncts failed: hybrid set has a null value");
            }

            ExtLiteral literal(slot_desc->type().type, const_cast<void*>(iter->get_value()));
            in_pred_values.emplace_back(literal);
            iter->next();
        }
        std::string col = slot_desc->col_name();
        if (_field_context.find(col) != _field_context.end()) {
            col = _field_context[col];
        }
        ExtPredicate* predicate = new ExtInPredicate(TExprNodeType::IN_PRED, pred->is_not_in(), col,
                                                     slot_desc->type(), in_pred_values);
        _disjuncts.push_back(predicate);

        return Status::OK();
    }
    if (TExprNodeType::COMPOUND_PRED == conjunct->node_type()) {
        // process COMPOUND_AND, such as:
        // k = 1 or (k1 = 7 and (k2 in (6,7) or k3 = 12))
        // k1 = 7 and (k2 in (6,7) or k3 = 12) is compound pred, we should rebuild this sub tree
        if (conjunct->op() == TExprOpcode::COMPOUND_AND) {
            std::vector<EsPredicate*> conjuncts;
            for (int i = 0; i < conjunct->get_num_children(); ++i) {
                EsPredicate* predicate = _pool->add(new EsPredicate(_context, _tuple_desc, _pool));
                predicate->set_field_context(_field_context);
                Status status = predicate->build_disjuncts_list(conjunct->children()[i]);
                if (status.ok()) {
                    conjuncts.push_back(predicate);
                } else {
                    return Status::InternalError("build COMPOUND_AND conjuncts failed");
                }
            }
            ExtCompPredicates* compound_predicate =
                    new ExtCompPredicates(TExprOpcode::COMPOUND_AND, conjuncts);
            _disjuncts.push_back(compound_predicate);
            return Status::OK();
        } else if (conjunct->op() == TExprOpcode::COMPOUND_NOT) {
            // reserved for processing COMPOUND_NOT
            return Status::InternalError("currently do not support COMPOUND_NOT push-down");
        }
        DCHECK(conjunct->op() == TExprOpcode::COMPOUND_OR);
        Status status = build_disjuncts_list(conjunct->get_child(0));
        if (!status.ok()) {
            return status;
        }
        status = build_disjuncts_list(conjunct->get_child(1));
        if (!status.ok()) {
            return status;
        }
        return Status::OK();
    }

    // if go to here, report error
    std::stringstream ss;
    ss << "build disjuncts failed: node type " << conjunct->node_type() << " is not supported";
    return Status::InternalError(ss.str());
}

const SlotDescriptor* EsPredicate::get_slot_desc(const SlotRef* slotRef) {
    const SlotDescriptor* slot_desc = nullptr;
    for (SlotDescriptor* slot : _tuple_desc->slots()) {
        if (slot->id() == slotRef->slot_id()) {
            slot_desc = slot;
            break;
        }
    }
    return slot_desc;
}

} // namespace doris
