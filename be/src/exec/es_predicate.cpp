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
#include <sstream>
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
#include "runtime/datetime_value.h"
#include "runtime/large_int_value.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"

#include "service/backend_options.h"
#include "util/runtime_profile.h"
#include "util/debug_util.h"

namespace doris {

using namespace std;

ExtLiteral::~ExtLiteral(){
}

int8_t ExtLiteral::to_byte() {
    DCHECK(_type != TYPE_TINYINT);
    return *(reinterpret_cast<int8_t*>(_value));
}

int16_t ExtLiteral::to_short() {
    DCHECK(_type != TYPE_SMALLINT);
    return *(reinterpret_cast<int16_t*>(_value));
}

int32_t ExtLiteral::to_int() {
    DCHECK(_type != TYPE_INT);
    return *(reinterpret_cast<int32_t*>(_value));
}

int64_t ExtLiteral::to_long() {
    DCHECK(_type != TYPE_BIGINT);
    return *(reinterpret_cast<int64_t*>(_value));
}

float ExtLiteral::to_float() {
    DCHECK(_type != TYPE_FLOAT);
    return *(reinterpret_cast<float*>(_value));
}

double ExtLiteral::to_double() {
    DCHECK(_type != TYPE_DOUBLE);
    return *(reinterpret_cast<double*>(_value));
}

std::string ExtLiteral::to_string() {
    DCHECK(_type != TYPE_VARCHAR && _type != TYPE_CHAR);
    return (reinterpret_cast<StringValue*>(_value))->to_string();
}

std::string ExtLiteral::to_date_string() {
    DCHECK(_type != TYPE_DATE && _type != TYPE_DATETIME);
    DateTimeValue date_value = *reinterpret_cast<DateTimeValue*>(_value);
    char str[MAX_DTVALUE_STR_LEN];
    date_value.to_string(str);
    return std::string(str, strlen(str)); 
}

bool ExtLiteral::to_bool() {
    DCHECK(_type != TYPE_BOOLEAN);
    return *(reinterpret_cast<bool*>(_value));
}

std::string ExtLiteral::to_decimal_string() {
    DCHECK(_type != TYPE_DECIMAL);
    return reinterpret_cast<DecimalValue*>(_value)->to_string();
}

std::string ExtLiteral::to_decimalv2_string() {
    DCHECK(_type != TYPE_DECIMALV2);
    return reinterpret_cast<DecimalV2Value*>(_value)->to_string();
}

std::string ExtLiteral::to_largeint_string() {
    DCHECK(_type != TYPE_LARGEINT);
    return LargeIntValue::to_string(*reinterpret_cast<__int128*>(_value));
}

std::string ExtLiteral::value_to_string() {
    std::stringstream ss;
    switch (_type) {
        case TYPE_TINYINT:
            ss << to_byte();
            break;
        case TYPE_SMALLINT:
            ss << to_short();
            break;
        case TYPE_INT:
            ss << to_int();
            break;
        case TYPE_BIGINT:
            ss << to_long();
            break;
        case TYPE_FLOAT:
            ss << to_float();
            break;
        case TYPE_DOUBLE:
            ss << to_double();
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
            ss << to_string();
            break;
        case TYPE_DATE:
        case TYPE_DATETIME:
            ss << to_date_string();
            break;
        case TYPE_BOOLEAN:
            ss << to_bool();
            break;
        case TYPE_DECIMAL:
            ss << to_decimal_string();
            break;
        case TYPE_DECIMALV2:
            ss << to_decimalv2_string();
            break;
        case TYPE_LARGEINT:
            ss << to_largeint_string();
            break;
        default:
            DCHECK(false);
            break;
    }
    return ss.str();
}

EsPredicate::EsPredicate(ExprContext* context,
            const TupleDescriptor* tuple_desc) :
    _context(context),
    _disjuncts_num(0),
    _tuple_desc(tuple_desc) {
}

EsPredicate::~EsPredicate() {
    for(int i=0; i < _disjuncts.size(); i++) {
        delete _disjuncts[i];
    }
    _disjuncts.clear();
}

bool EsPredicate::build_disjuncts_list() {
    return build_disjuncts_list(_context->root(), _disjuncts);
}

vector<ExtPredicate*> EsPredicate::get_predicate_list(){
    return _disjuncts;
}

bool EsPredicate::build_disjuncts_list(Expr* conjunct, vector<ExtPredicate*>& disjuncts) {
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

        const SlotDescriptor* slot_desc = get_slot_desc(slotRef);
        if (slot_desc == nullptr) {
            VLOG(1) << "get disjuncts fail: slot_desc is null";
            return false;
        }


        ExtLiteral literal(expr->type().type, _context->get_value(expr, NULL));
        ExtPredicate* predicate = new ExtBinaryPredicate(
                    TExprNodeType::BINARY_PRED,
                    slot_desc->col_name(),
                    slot_desc->type(),
                    op,
                    literal);

        disjuncts.push_back(predicate);
        return true;
    }
    
    if (is_match_func(conjunct)) {

        Expr* expr = conjunct->get_child(1);
        ExtLiteral literal(expr->type().type, _context->get_value(expr, NULL));
        vector<ExtLiteral> query_conditions;
        query_conditions.emplace_back(literal);
        vector<ExtColumnDesc> cols; //TODO
        ExtPredicate* predicate = new ExtFunction(
                        TExprNodeType::FUNCTION_CALL,
                        conjunct->fn().name.function_name,
                        cols,
                        query_conditions);
        disjuncts.push_back(predicate);

        return true;
    } 
      
    if (TExprNodeType::IN_PRED == conjunct->node_type()) {
        TExtInPredicate ext_in_predicate;
        vector<ExtLiteral> in_pred_values;
        InPredicate* pred = dynamic_cast<InPredicate*>(conjunct);
        ext_in_predicate.__set_is_not_in(pred->is_not_in());
        if (Expr::type_without_cast(pred->get_child(0)) != TExprNodeType::SLOT_REF) {
            return false;
        }

        SlotRef* slot_ref = (SlotRef*)(conjunct->get_child(0));
        const SlotDescriptor* slot_desc = get_slot_desc(slot_ref);
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

            Expr* expr = conjunct->get_child(i);
            ExtLiteral literal(expr->type().type, _context->get_value(expr, NULL));
            in_pred_values.emplace_back(literal);
        }

        ExtPredicate* predicate = new ExtInPredicate(
                    TExprNodeType::IN_PRED,
                    slot_desc->col_name(),
                    slot_desc->type(),
                    in_pred_values);
        disjuncts.push_back(predicate);

        return true;
    } 
    
    if (TExprNodeType::COMPOUND_PRED == conjunct->node_type()) {
        if (TExprOpcode::COMPOUND_OR != conjunct->op()) {
            VLOG(1) << "get disjuncts fail: op is not COMPOUND_OR";
            return false;
        }
        if (!build_disjuncts_list(conjunct->get_child(0), disjuncts)) {
            return false;
        }
        if (!build_disjuncts_list(conjunct->get_child(1), disjuncts)) {
            return false;
        }

        return true;
    }

    // if go to here, report error
    VLOG(1) << "get disjuncts fail: node type is " << conjunct->node_type()
        << ", should be BINARY_PRED or COMPOUND_PRED";
    return false;
}

bool EsPredicate::is_match_func(const Expr* conjunct) {
    if (TExprNodeType::FUNCTION_CALL == conjunct->node_type()
        && conjunct->fn().name.function_name == "esquery") {
            return true;
    }
    return false;
}

const SlotDescriptor* EsPredicate::get_slot_desc(SlotRef* slotRef) {
    std::vector<SlotId> slot_ids;
    slotRef->get_slot_ids(&slot_ids);
    const SlotDescriptor* slot_desc = nullptr;
    for (SlotDescriptor* slot : _tuple_desc->slots()) {
        if (slot->id() == slot_ids[0]) {
            slot_desc = slot;
            break;
        }
    }
    return slot_desc;
}

}
