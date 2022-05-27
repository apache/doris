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

#include "exprs/binary_predicate.h"

#include <sstream>

#include "gen_cpp/Exprs_types.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "util/debug_util.h"

namespace doris {

Expr* BinaryPredicate::from_thrift(const TExprNode& node) {
    switch (node.opcode) {
    case TExprOpcode::EQ: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new EqBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new EqTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new EqSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new EqIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new EqBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new EqLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new EqFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new EqDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::STRING:
            return new EqStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new EqDateTimeValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new EqDecimalV2ValPred(node);
        default:
            return nullptr;
        }
    }
    case TExprOpcode::NE: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new NeBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new NeTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new NeSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new NeIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new NeBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new NeLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new NeFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new NeDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::STRING:
            return new NeStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new NeDateTimeValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new NeDecimalV2ValPred(node);
        default:
            return nullptr;
        }
    }
    case TExprOpcode::LT: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new LtBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new LtTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new LtSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new LtIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new LtBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new LtLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new LtFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new LtDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::STRING:
            return new LtStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new LtDateTimeValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new LtDecimalV2ValPred(node);
        default:
            return nullptr;
        }
    }
    case TExprOpcode::LE: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new LeBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new LeTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new LeSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new LeIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new LeBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new LeLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new LeFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new LeDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::STRING:
            return new LeStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new LeDateTimeValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new LeDecimalV2ValPred(node);
        default:
            return nullptr;
        }
    }
    case TExprOpcode::GT: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new GtBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new GtTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new GtSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new GtIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new GtBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new GtLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new GtFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new GtDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::STRING:
            return new GtStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new GtDateTimeValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new GtDecimalV2ValPred(node);
        default:
            return nullptr;
        }
    }
    case TExprOpcode::GE: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new GeBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new GeTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new GeSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new GeIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new GeBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new GeLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new GeFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new GeDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::STRING:
            return new GeStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new GeDateTimeValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new GeDecimalV2ValPred(node);
        default:
            return nullptr;
        }
    }
    case TExprOpcode::EQ_FOR_NULL: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new EqForNullBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new EqForNullTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new EqForNullSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new EqForNullIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new EqForNullBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new EqForNullLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new EqForNullFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new EqForNullDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::STRING:
            return new EqForNullStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new EqForNullDateTimeValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new EqForNullDecimalV2ValPred(node);
        default:
            return nullptr;
        }
    }
    default:
        return nullptr;
    }
    return nullptr;
}

std::string BinaryPredicate::debug_string() const {
    std::stringstream out;
    out << "BinaryPredicate(" << Expr::debug_string() << ")";
    return out.str();
}

#define BINARY_PRED_FN(CLASS, TYPE, FN, OP, LLVM_PRED)                   \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        TYPE v1 = _children[0]->FN(ctx, row);                            \
        if (v1.is_null) {                                                \
            return BooleanVal::null();                                   \
        }                                                                \
        TYPE v2 = _children[1]->FN(ctx, row);                            \
        if (v2.is_null) {                                                \
            return BooleanVal::null();                                   \
        }                                                                \
        return BooleanVal(v1.val OP v2.val);                             \
    }

// add '/**/' to pass code style check of cooder
#define BINARY_PRED_INT_FNS(TYPE, FN)                                         \
    BINARY_PRED_FN(Eq##TYPE##Pred, TYPE, FN, /**/ == /**/, CmpInst::ICMP_EQ)  \
    BINARY_PRED_FN(Ne##TYPE##Pred, TYPE, FN, /**/ != /**/, CmpInst::ICMP_NE)  \
    BINARY_PRED_FN(Lt##TYPE##Pred, TYPE, FN, /**/ < /**/, CmpInst::ICMP_SLT)  \
    BINARY_PRED_FN(Le##TYPE##Pred, TYPE, FN, /**/ <= /**/, CmpInst::ICMP_SLE) \
    BINARY_PRED_FN(Gt##TYPE##Pred, TYPE, FN, /**/ > /**/, CmpInst::ICMP_SGT)  \
    BINARY_PRED_FN(Ge##TYPE##Pred, TYPE, FN, /**/ >= /**/, CmpInst::ICMP_SGE)

BINARY_PRED_INT_FNS(BooleanVal, get_boolean_val);
BINARY_PRED_INT_FNS(TinyIntVal, get_tiny_int_val);
BINARY_PRED_INT_FNS(SmallIntVal, get_small_int_val);
BINARY_PRED_INT_FNS(IntVal, get_int_val);
BINARY_PRED_INT_FNS(BigIntVal, get_big_int_val);
BINARY_PRED_INT_FNS(LargeIntVal, get_large_int_val);

#define BINARY_PRED_FLOAT_FNS(TYPE, FN)                             \
    BINARY_PRED_FN(Eq##TYPE##Pred, TYPE, FN, ==, CmpInst::FCMP_OEQ) \
    BINARY_PRED_FN(Ne##TYPE##Pred, TYPE, FN, !=, CmpInst::FCMP_UNE) \
    BINARY_PRED_FN(Lt##TYPE##Pred, TYPE, FN, <, CmpInst::FCMP_OLT)  \
    BINARY_PRED_FN(Le##TYPE##Pred, TYPE, FN, <=, CmpInst::FCMP_OLE) \
    BINARY_PRED_FN(Gt##TYPE##Pred, TYPE, FN, >, CmpInst::FCMP_OGT)  \
    BINARY_PRED_FN(Ge##TYPE##Pred, TYPE, FN, >=, CmpInst::FCMP_OGE)

BINARY_PRED_FLOAT_FNS(FloatVal, get_float_val);
BINARY_PRED_FLOAT_FNS(DoubleVal, get_double_val);

#define COMPLICATE_BINARY_PRED_FN(CLASS, TYPE, FN, DORIS_TYPE, FROM_FUNC, OP) \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) {      \
        TYPE v1 = _children[0]->FN(ctx, row);                                 \
        if (v1.is_null) {                                                     \
            return BooleanVal::null();                                        \
        }                                                                     \
        TYPE v2 = _children[1]->FN(ctx, row);                                 \
        if (v2.is_null) {                                                     \
            return BooleanVal::null();                                        \
        }                                                                     \
        DORIS_TYPE pv1 = DORIS_TYPE::FROM_FUNC(v1);                           \
        DORIS_TYPE pv2 = DORIS_TYPE::FROM_FUNC(v2);                           \
        return BooleanVal(pv1 OP pv2);                                        \
    }

#define COMPLICATE_BINARY_PRED_FNS(TYPE, FN, DORIS_TYPE, FROM_FUNC)                \
    COMPLICATE_BINARY_PRED_FN(Eq##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, ==) \
    COMPLICATE_BINARY_PRED_FN(Ne##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, !=) \
    COMPLICATE_BINARY_PRED_FN(Lt##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, <)  \
    COMPLICATE_BINARY_PRED_FN(Le##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, <=) \
    COMPLICATE_BINARY_PRED_FN(Gt##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, >)  \
    COMPLICATE_BINARY_PRED_FN(Ge##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, >=)

COMPLICATE_BINARY_PRED_FNS(DecimalV2Val, get_decimalv2_val, DecimalV2Value, from_decimal_val)

#define DATETIME_BINARY_PRED_FN(CLASS, OP, LLVM_PRED)                    \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        DateTimeVal v1 = _children[0]->get_datetime_val(ctx, row);       \
        if (v1.is_null) {                                                \
            return BooleanVal::null();                                   \
        }                                                                \
        DateTimeVal v2 = _children[1]->get_datetime_val(ctx, row);       \
        if (v2.is_null) {                                                \
            return BooleanVal::null();                                   \
        }                                                                \
        return BooleanVal(v1.packed_time OP v2.packed_time);             \
    }

#define DATETIME_BINARY_PRED_FNS()                                        \
    DATETIME_BINARY_PRED_FN(Eq##DateTimeVal##Pred, ==, CmpInst::ICMP_EQ)  \
    DATETIME_BINARY_PRED_FN(Ne##DateTimeVal##Pred, !=, CmpInst::ICMP_NE)  \
    DATETIME_BINARY_PRED_FN(Lt##DateTimeVal##Pred, <, CmpInst::ICMP_SLT)  \
    DATETIME_BINARY_PRED_FN(Le##DateTimeVal##Pred, <=, CmpInst::ICMP_SLE) \
    DATETIME_BINARY_PRED_FN(Gt##DateTimeVal##Pred, >, CmpInst::ICMP_SGT)  \
    DATETIME_BINARY_PRED_FN(Ge##DateTimeVal##Pred, >=, CmpInst::ICMP_SGE)

DATETIME_BINARY_PRED_FNS()

#define STRING_BINARY_PRED_FN(CLASS, OP)                                 \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        StringVal v1 = _children[0]->get_string_val(ctx, row);           \
        if (v1.is_null) {                                                \
            return BooleanVal::null();                                   \
        }                                                                \
        StringVal v2 = _children[1]->get_string_val(ctx, row);           \
        if (v2.is_null) {                                                \
            return BooleanVal::null();                                   \
        }                                                                \
        StringValue pv1 = StringValue::from_string_val(v1);              \
        StringValue pv2 = StringValue::from_string_val(v2);              \
        return BooleanVal(pv1 OP pv2);                                   \
    }

#define STRING_BINARY_PRED_FNS()                   \
    STRING_BINARY_PRED_FN(Ne##StringVal##Pred, !=) \
    STRING_BINARY_PRED_FN(Lt##StringVal##Pred, <)  \
    STRING_BINARY_PRED_FN(Le##StringVal##Pred, <=) \
    STRING_BINARY_PRED_FN(Gt##StringVal##Pred, >)  \
    STRING_BINARY_PRED_FN(Ge##StringVal##Pred, >=)

STRING_BINARY_PRED_FNS()

BooleanVal EqStringValPred::get_boolean_val(ExprContext* ctx, TupleRow* row) {
    StringVal v1 = _children[0]->get_string_val(ctx, row);
    if (v1.is_null) {
        return BooleanVal::null();
    }
    StringVal v2 = _children[1]->get_string_val(ctx, row);
    if (v2.is_null) {
        return BooleanVal::null();
    }
    if (v1.len != v2.len) {
        return BooleanVal(false);
    }
    return BooleanVal(string_compare((char*)v1.ptr, v1.len, (char*)v2.ptr, v2.len, v1.len) == 0);
}

#define BINARY_PRED_FOR_NULL_FN(CLASS, TYPE, FN, OP, LLVM_PRED)          \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        TYPE v1 = _children[0]->FN(ctx, row);                            \
        TYPE v2 = _children[1]->FN(ctx, row);                            \
        if (v1.is_null && v2.is_null) {                                  \
            return BooleanVal(true);                                     \
        } else if (v1.is_null || v2.is_null) {                           \
            return BooleanVal(false);                                    \
        }                                                                \
        return BooleanVal(v1.val OP v2.val);                             \
    }

// add '/**/' to pass code style check of cooder
#define BINARY_PRED_FOR_NULL_INT_FNS(TYPE, FN) \
    BINARY_PRED_FOR_NULL_FN(EqForNull##TYPE##Pred, TYPE, FN, /**/ == /**/, CmpInst::ICMP_EQ)

BINARY_PRED_FOR_NULL_INT_FNS(BooleanVal, get_boolean_val);
BINARY_PRED_FOR_NULL_INT_FNS(TinyIntVal, get_tiny_int_val);
BINARY_PRED_FOR_NULL_INT_FNS(SmallIntVal, get_small_int_val);
BINARY_PRED_FOR_NULL_INT_FNS(IntVal, get_int_val);
BINARY_PRED_FOR_NULL_INT_FNS(BigIntVal, get_big_int_val);
BINARY_PRED_FOR_NULL_INT_FNS(LargeIntVal, get_large_int_val);

#define BINARY_PRED_FOR_NULL_FLOAT_FNS(TYPE, FN) \
    BINARY_PRED_FOR_NULL_FN(EqForNull##TYPE##Pred, TYPE, FN, ==, CmpInst::FCMP_OEQ)

BINARY_PRED_FOR_NULL_FLOAT_FNS(FloatVal, get_float_val);
BINARY_PRED_FOR_NULL_FLOAT_FNS(DoubleVal, get_double_val);

#define COMPLICATE_BINARY_FOR_NULL_PRED_FN(CLASS, TYPE, FN, DORIS_TYPE, FROM_FUNC, OP) \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) {               \
        TYPE v1 = _children[0]->FN(ctx, row);                                          \
        TYPE v2 = _children[1]->FN(ctx, row);                                          \
        if (v1.is_null && v2.is_null) {                                                \
            return BooleanVal(true);                                                   \
        } else if (v1.is_null || v2.is_null) {                                         \
            return BooleanVal(false);                                                  \
        }                                                                              \
        DORIS_TYPE pv1 = DORIS_TYPE::FROM_FUNC(v1);                                    \
        DORIS_TYPE pv2 = DORIS_TYPE::FROM_FUNC(v2);                                    \
        return BooleanVal(pv1 OP pv2);                                                 \
    }

#define COMPLICATE_BINARY_FOR_NULL_PRED_FNS(TYPE, FN, DORIS_TYPE, FROM_FUNC) \
    COMPLICATE_BINARY_FOR_NULL_PRED_FN(EqForNull##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, ==)

COMPLICATE_BINARY_FOR_NULL_PRED_FNS(DecimalV2Val, get_decimalv2_val, DecimalV2Value,
                                    from_decimal_val)

#define DATETIME_BINARY_FOR_NULL_PRED_FN(CLASS, OP, LLVM_PRED)           \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        DateTimeVal v1 = _children[0]->get_datetime_val(ctx, row);       \
        DateTimeVal v2 = _children[1]->get_datetime_val(ctx, row);       \
        if (v1.is_null && v2.is_null) {                                  \
            return BooleanVal(true);                                     \
        } else if (v1.is_null || v2.is_null) {                           \
            return BooleanVal(false);                                    \
        }                                                                \
        return BooleanVal(v1.packed_time OP v2.packed_time);             \
    }

#define DATETIME_BINARY_FOR_NULL_PRED_FNS() \
    DATETIME_BINARY_FOR_NULL_PRED_FN(EqForNull##DateTimeVal##Pred, ==, CmpInst::ICMP_EQ)

DATETIME_BINARY_FOR_NULL_PRED_FNS()

BooleanVal EqForNullStringValPred::get_boolean_val(ExprContext* ctx, TupleRow* row) {
    StringVal v1 = _children[0]->get_string_val(ctx, row);
    StringVal v2 = _children[1]->get_string_val(ctx, row);
    if (v1.is_null && v2.is_null) {
        return BooleanVal(true);
    } else if (v1.is_null || v2.is_null) {
        return BooleanVal(false);
    }

    if (v1.len != v2.len) {
        return BooleanVal(false);
    }
    return BooleanVal(string_compare((char*)v1.ptr, v1.len, (char*)v2.ptr, v2.len, v1.len) == 0);
}

} // namespace doris
