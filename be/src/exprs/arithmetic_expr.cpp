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

#include "exprs/arithmetic_expr.h"

#include "runtime/runtime_state.h"

namespace doris {

std::set<std::string> ArithmeticExpr::_s_valid_fn_names = {
        "add", "subtract", "multiply", "divide", "int_divide",
        "mod", "bitand",   "bitor",    "bitxor", "bitnot"};

Expr* ArithmeticExpr::from_thrift(const TExprNode& node) {
    switch (node.opcode) {
    case TExprOpcode::ADD:
        return new AddExpr(node);
    case TExprOpcode::SUBTRACT:
        return new SubExpr(node);
    case TExprOpcode::MULTIPLY:
        return new MulExpr(node);
    case TExprOpcode::DIVIDE:
    case TExprOpcode::INT_DIVIDE:
        return new DivExpr(node);
    case TExprOpcode::MOD:
        return new ModExpr(node);
    case TExprOpcode::BITAND:
        return new BitAndExpr(node);
    case TExprOpcode::BITOR:
        return new BitOrExpr(node);
    case TExprOpcode::BITXOR:
        return new BitXorExpr(node);
    case TExprOpcode::BITNOT:
        return new BitNotExpr(node);
    default:
        return nullptr;
    }
    return nullptr;
}

Expr* ArithmeticExpr::from_fn_name(const TExprNode& node) {
    std::string fn_name = node.fn.name.function_name;
    if (fn_name == "add") {
        return new AddExpr(node);
    } else if (fn_name == "subtract") {
        return new SubExpr(node);
    } else if (fn_name == "multiply") {
        return new MulExpr(node);
    } else if (fn_name == "divide" || fn_name == "int_divide") {
        return new DivExpr(node);
    } else if (fn_name == "mod") {
        return new ModExpr(node);
    } else if (fn_name == "bitand") {
        return new BitAndExpr(node);
    } else if (fn_name == "bitor") {
        return new BitOrExpr(node);
    } else if (fn_name == "bitxor") {
        return new BitXorExpr(node);
    } else if (fn_name == "bitnot") {
        return new BitNotExpr(node);
    }

    return nullptr;
}

#define BINARY_OP_CHECK_ZERO_FN(TYPE, CLASS, FN, OP)      \
    TYPE CLASS::FN(ExprContext* context, TupleRow* row) { \
        TYPE v1 = _children[0]->FN(context, row);         \
        if (v1.is_null) {                                 \
            return TYPE::null();                          \
        }                                                 \
        TYPE v2 = _children[1]->FN(context, row);         \
        if (v2.is_null || v2.val == 0) {                  \
            return TYPE::null();                          \
        }                                                 \
        return TYPE(v1.val OP v2.val);                    \
    }

#define BINARY_OP_FN(TYPE, CLASS, FN, OP)                 \
    TYPE CLASS::FN(ExprContext* context, TupleRow* row) { \
        TYPE v1 = _children[0]->FN(context, row);         \
        if (v1.is_null) {                                 \
            return TYPE::null();                          \
        }                                                 \
        TYPE v2 = _children[1]->FN(context, row);         \
        if (v2.is_null) {                                 \
            return TYPE::null();                          \
        }                                                 \
        return TYPE(v1.val OP v2.val);                    \
    }

#define BINARY_ARITH_FNS(CLASS, OP)                         \
    BINARY_OP_FN(TinyIntVal, CLASS, get_tiny_int_val, OP)   \
    BINARY_OP_FN(SmallIntVal, CLASS, get_small_int_val, OP) \
    BINARY_OP_FN(IntVal, CLASS, get_int_val, OP)            \
    BINARY_OP_FN(BigIntVal, CLASS, get_big_int_val, OP)     \
    BINARY_OP_FN(LargeIntVal, CLASS, get_large_int_val, OP) \
    BINARY_OP_FN(FloatVal, CLASS, get_float_val, OP)        \
    BINARY_OP_FN(DoubleVal, CLASS, get_double_val, OP)

BINARY_ARITH_FNS(AddExpr, +)
BINARY_ARITH_FNS(SubExpr, -)
BINARY_ARITH_FNS(MulExpr, *)

#define BINARY_DIV_FNS()                                                \
    BINARY_OP_CHECK_ZERO_FN(TinyIntVal, DivExpr, get_tiny_int_val, /)   \
    BINARY_OP_CHECK_ZERO_FN(SmallIntVal, DivExpr, get_small_int_val, /) \
    BINARY_OP_CHECK_ZERO_FN(IntVal, DivExpr, get_int_val, /)            \
    BINARY_OP_CHECK_ZERO_FN(BigIntVal, DivExpr, get_big_int_val, /)     \
    BINARY_OP_CHECK_ZERO_FN(LargeIntVal, DivExpr, get_large_int_val, /) \
    BINARY_OP_CHECK_ZERO_FN(FloatVal, DivExpr, get_float_val, /)        \
    BINARY_OP_CHECK_ZERO_FN(DoubleVal, DivExpr, get_double_val, /)

BINARY_DIV_FNS()

#define BINARY_MOD_FNS()                                                \
    BINARY_OP_CHECK_ZERO_FN(TinyIntVal, ModExpr, get_tiny_int_val, %)   \
    BINARY_OP_CHECK_ZERO_FN(SmallIntVal, ModExpr, get_small_int_val, %) \
    BINARY_OP_CHECK_ZERO_FN(IntVal, ModExpr, get_int_val, %)            \
    BINARY_OP_CHECK_ZERO_FN(BigIntVal, ModExpr, get_big_int_val, %)     \
    BINARY_OP_CHECK_ZERO_FN(LargeIntVal, ModExpr, get_large_int_val, %)

BINARY_MOD_FNS()

FloatVal ModExpr::get_float_val(ExprContext* context, TupleRow* row) {
    FloatVal v1 = _children[0]->get_float_val(context, row);
    if (v1.is_null) {
        return FloatVal::null();
    }
    FloatVal v2 = _children[1]->get_float_val(context, row);
    if (v2.is_null || v2.val == 0) {
        return FloatVal::null();
    }
    return FloatVal(fmod(v1.val, v2.val));
}

DoubleVal ModExpr::get_double_val(ExprContext* context, TupleRow* row) {
    DoubleVal v1 = _children[0]->get_double_val(context, row);
    if (v1.is_null) {
        return DoubleVal::null();
    }
    DoubleVal v2 = _children[1]->get_double_val(context, row);
    if (v2.is_null || v2.val == 0) {
        return DoubleVal::null();
    }
    return DoubleVal(fmod(v1.val, v2.val));
}

#define BINARY_BIT_FNS(CLASS, OP)                           \
    BINARY_OP_FN(TinyIntVal, CLASS, get_tiny_int_val, OP)   \
    BINARY_OP_FN(SmallIntVal, CLASS, get_small_int_val, OP) \
    BINARY_OP_FN(IntVal, CLASS, get_int_val, OP)            \
    BINARY_OP_FN(BigIntVal, CLASS, get_big_int_val, OP)     \
    BINARY_OP_FN(LargeIntVal, CLASS, get_large_int_val, OP)

BINARY_BIT_FNS(BitAndExpr, &)
BINARY_BIT_FNS(BitOrExpr, |)
BINARY_BIT_FNS(BitXorExpr, ^)

#define BITNOT_OP_FN(TYPE, FN)                                 \
    TYPE BitNotExpr::FN(ExprContext* context, TupleRow* row) { \
        TYPE v = _children[0]->FN(context, row);               \
        if (v.is_null) {                                       \
            return TYPE::null();                               \
        }                                                      \
        return TYPE(~v.val);                                   \
    }

#define BITNOT_FNS()                             \
    BITNOT_OP_FN(TinyIntVal, get_tiny_int_val)   \
    BITNOT_OP_FN(SmallIntVal, get_small_int_val) \
    BITNOT_OP_FN(IntVal, get_int_val)            \
    BITNOT_OP_FN(BigIntVal, get_big_int_val)     \
    BITNOT_OP_FN(LargeIntVal, get_large_int_val)

BITNOT_FNS()

#define DECIMAL_ARITHMETIC_OP(EXPR_NAME, OP)                                         \
    DecimalV2Val EXPR_NAME::get_decimalv2_val(ExprContext* context, TupleRow* row) { \
        DecimalV2Val v1 = _children[0]->get_decimalv2_val(context, row);             \
        DecimalV2Val v2 = _children[1]->get_decimalv2_val(context, row);             \
        if (v1.is_null || v2.is_null) {                                              \
            return DecimalV2Val::null();                                             \
        }                                                                            \
        DecimalV2Value iv1 = DecimalV2Value::from_decimal_val(v1);                   \
        DecimalV2Value iv2 = DecimalV2Value::from_decimal_val(v2);                   \
        DecimalV2Value ir = iv1 OP iv2;                                              \
        DecimalV2Val result;                                                         \
        ir.to_decimal_val(&result);                                                  \
        return result;                                                               \
    }

#define DECIMAL_ARITHMETIC_OP_DIVIDE(EXPR_NAME, OP)                                  \
    DecimalV2Val EXPR_NAME::get_decimalv2_val(ExprContext* context, TupleRow* row) { \
        DecimalV2Val v1 = _children[0]->get_decimalv2_val(context, row);             \
        DecimalV2Val v2 = _children[1]->get_decimalv2_val(context, row);             \
        if (v1.is_null || v2.is_null || v2.value() == 0) {                           \
            return DecimalV2Val::null();                                             \
        }                                                                            \
        DecimalV2Value iv1 = DecimalV2Value::from_decimal_val(v1);                   \
        DecimalV2Value iv2 = DecimalV2Value::from_decimal_val(v2);                   \
        DecimalV2Value ir = iv1 OP iv2;                                              \
        DecimalV2Val result;                                                         \
        ir.to_decimal_val(&result);                                                  \
        return result;                                                               \
    }

DECIMAL_ARITHMETIC_OP(AddExpr, +);
DECIMAL_ARITHMETIC_OP(SubExpr, -);
DECIMAL_ARITHMETIC_OP(MulExpr, *);
DECIMAL_ARITHMETIC_OP_DIVIDE(DivExpr, /);
DECIMAL_ARITHMETIC_OP_DIVIDE(ModExpr, %);

} // namespace doris
