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

#include "exec/es_predicate.h"

#include <stdint.h>
#include <map>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "common/status.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/in_predicate.h"

#include "gen_cpp/PlanNodes_types.h"
#include "olap/olap_common.h"
#include "olap/utils.h"
#include "runtime/client_cache.h"
#include "runtime/runtime_state.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"

#include "service/backend_options.h"
#include "util/runtime_profile.h"
#include "util/debug_util.h"

namespace doris {

using namespace std;

EsPredicate::EsPredicate(ExprContext* conjunct_ctx,
            const TupleDescriptor* tuple_desc) :
    _context(conjunct_ctx),
    _disjuncts_num(0),
    _tuple_desc(tuple_desc) {
}

EsPredicate::~EsPredicate() {
}

bool EsPredicate::build_disjuncts() {
    return build_disjuncts(_context->root(), _disjuncts);
}

vector<ExtPredicate> EsPredicate::get_predicate_list(){
    return _disjuncts;
}

bool EsPredicate::build_disjuncts(Expr* conjunct, vector<ExtPredicate>& disjuncts) {
    if (TExprNodeType::BINARY_PRED == conjunct->node_type()) {
        if (conjunct->children().size() != 2) {
            VLOG(1) << "get disjuncts fail: number of childs is not 2";
            return false;
        }

        SlotRef* slotRef = nullptr;
        TExprOpcode::type op;
        Expr* expr = nullptr;
        if (TExprNodeType::SLOT_REF == conjunct->get_child(0)->node_type()) {
            expr = conjunct->get_child(1);
            slotRef = (SlotRef*)(conjunct->get_child(0));
            op = conjunct->op();
        } else if (TExprNodeType::SLOT_REF == conjunct->get_child(1)->node_type()) {
            expr = conjunct->get_child(0);
            slotRef = (SlotRef*)(conjunct->get_child(1));
            op = conjunct->op();
        } else {
            VLOG(1) << "get disjuncts fail: no SLOT_REF child";
            return false;
        }

        SlotDescriptor* slot_desc = get_slot_desc(slotRef);
        if (slot_desc == nullptr) {
            VLOG(1) << "get disjuncts fail: slot_desc is null";
            return false;
        }

        TExtLiteral literal;
        if (!to_ext_literal(_context, expr, &literal)) {
            VLOG(1) << "get disjuncts fail: can't get literal, node_type="
                    << expr->node_type();
            return false;
        }

        std::unique_ptr<ExtPredicate> predicate(new ExtBinaryPredicate(
                    TExprNodeType::BINARY_PRED,
                    slot_desc->col_name(),
                    slot_desc->type(),
                    op,
                    literal));

        disjuncts.emplace_back(std::move(*predicate));
        return true;
    }
    
    if (is_match_func(conjunct)) {
        TExtLiteral literal;
        if (!to_ext_literal(_context, conjunct->get_child(1), &literal)) {
            VLOG(1) << "get disjuncts fail: can't get literal, node_type="
                    << conjunct->get_child(1)->node_type();
            return false;
        }

        vector<TExtLiteral> query_conditions;
        query_conditions.push_back(std::move(literal));
        vector<ExtColumnDesc> cols; //TODO

        std::unique_ptr<ExtPredicate> predicate(new ExtFunction(
                        TExprNodeType::FUNCTION_CALL,
                        conjunct->fn().name.function_name,
                        cols,
                        query_conditions));
        disjuncts.emplace_back(std::move(*predicate));

        return true;
    } 
      
    if (TExprNodeType::IN_PRED == conjunct->node_type()) {
        TExtInPredicate ext_in_predicate;
        vector<TExtLiteral> in_pred_values;
        InPredicate* pred = dynamic_cast<InPredicate*>(conjunct);
        ext_in_predicate.__set_is_not_in(pred->is_not_in());
        if (Expr::type_without_cast(pred->get_child(0)) != TExprNodeType::SLOT_REF) {
            return false;
        }

        SlotRef* slot_ref = (SlotRef*)(conjunct->get_child(0));
        SlotDescriptor* slot_desc = get_slot_desc(slot_ref);
        if (slot_desc == nullptr) {
            return false;
        }

        for (int i = 1; i < pred->children().size(); ++i) {
            // varchar, string, all of them are string type, but varchar != string
            // TODO add date, datetime support?
            if (pred->get_child(0)->type().is_string_type()) {
                if (!pred->get_child(i)->type().is_string_type()) {
                    return false;
                }
            } else {
                if (pred->get_child(i)->type().type != pred->get_child(0)->type().type) {
                    return false;
                }
            }
            TExtLiteral literal;
            if (!to_ext_literal(_context, pred->get_child(i), &literal)) {
                VLOG(1) << "get disjuncts fail: can't get literal, node_type="
                        << pred->get_child(i)->node_type();
                return false;
            }
            in_pred_values.push_back(literal);
        }

        std::unique_ptr<ExtPredicate> predicate(new ExtInPredicate(
                    TExprNodeType::IN_PRED,
                    slot_desc->col_name(),
                    slot_desc->type(),
                    in_pred_values));

        disjuncts.emplace_back(std::move(*predicate));

        return true;
    } 
    
    if (TExprNodeType::COMPOUND_PRED == conjunct->node_type()) {
        if (TExprOpcode::COMPOUND_OR != conjunct->op()) {
            VLOG(1) << "get disjuncts fail: op is not COMPOUND_OR";
            return false;
        }
        if (!build_disjuncts(conjunct->get_child(0), disjuncts)) {
            return false;
        }
        if (!build_disjuncts(conjunct->get_child(1), disjuncts)) {
            return false;
        }

        return true;
    }

    // if go to here, report error
    VLOG(1) << "get disjuncts fail: node type is " << conjunct->node_type()
        << ", should be BINARY_PRED or COMPOUND_PRED";
    return false;
}

bool EsPredicate::is_match_func(Expr* conjunct) {
    if (TExprNodeType::FUNCTION_CALL == conjunct->node_type()
        && conjunct->fn().name.function_name == "esquery") {
            return true;
    }
    return false;
}

SlotDescriptor* EsPredicate::get_slot_desc(SlotRef* slotRef) {
    std::vector<SlotId> slot_ids;
    slotRef->get_slot_ids(&slot_ids);
    SlotDescriptor* slot_desc = nullptr;
    for (SlotDescriptor* slot : _tuple_desc->slots()) {
        if (slot->id() == slot_ids[0]) {
            slot_desc = slot;
            break;
        }
    }
    return slot_desc;
}

bool EsPredicate::to_ext_literal(ExprContext* _context, Expr* expr, TExtLiteral* literal) {
    literal->__set_node_type(expr->node_type());
    switch (expr->node_type()) {
    case TExprNodeType::BOOL_LITERAL: {
        TBoolLiteral bool_literal;
        void* value = _context->get_value(expr, NULL);
        bool_literal.__set_value(*reinterpret_cast<bool*>(value));
        literal->__set_bool_literal(bool_literal);
        return true;
    }
    case TExprNodeType::DATE_LITERAL: {
        void* value = _context->get_value(expr, NULL);
        DateTimeValue date_value = *reinterpret_cast<DateTimeValue*>(value);
        char str[MAX_DTVALUE_STR_LEN];
        date_value.to_string(str);
        TDateLiteral date_literal;
        date_literal.__set_value(str);
        literal->__set_date_literal(date_literal);
        return true;
    }
    case TExprNodeType::FLOAT_LITERAL: {
        TFloatLiteral float_literal;
        void* value = _context->get_value(expr, NULL);
        float_literal.__set_value(*reinterpret_cast<float*>(value));
        literal->__set_float_literal(float_literal);
        return true;
    }
    case TExprNodeType::INT_LITERAL: {
        TIntLiteral int_literal;
        void* value = _context->get_value(expr, NULL);
        int_literal.__set_value(*reinterpret_cast<int32_t*>(value));
        literal->__set_int_literal(int_literal);
        return true;
    }
    case TExprNodeType::STRING_LITERAL: {
        TStringLiteral string_literal;
        void* value = _context->get_value(expr, NULL);
        string_literal.__set_value(*reinterpret_cast<string*>(value));
        literal->__set_string_literal(string_literal);
        return true;
    }
    case TExprNodeType::DECIMAL_LITERAL: {
        TDecimalLiteral decimal_literal;
        void* value = _context->get_value(expr, NULL);
        decimal_literal.__set_value(reinterpret_cast<DecimalValue*>(value)->to_string());
        literal->__set_decimal_literal(decimal_literal);
        return true;
    }
    case TExprNodeType::LARGE_INT_LITERAL: {
        char buf[48];
        int len = 48;
        void* value = _context->get_value(expr, NULL);
        char* v = LargeIntValue::to_string(*reinterpret_cast<__int128*>(value), buf, &len);
        TLargeIntLiteral large_int_literal;
        large_int_literal.__set_value(v);
        literal->__set_large_int_literal(large_int_literal);
        return true;
    }
    default:
        return false;
    }
}

}
