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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/cast-expr.cpp
// and modified by Doris

#include "exprs/cast_expr.h"

#include "runtime/runtime_state.h"

namespace doris {

Expr* CastExpr::from_thrift(const TExprNode& node) {
    switch (node.child_type) {
    case TPrimitiveType::BOOLEAN:
        return new CastBooleanExpr(node);
    case TPrimitiveType::TINYINT:
        return new CastTinyIntExpr(node);
    case TPrimitiveType::SMALLINT:
        return new CastSmallIntExpr(node);
    case TPrimitiveType::INT:
        return new CastIntExpr(node);
    case TPrimitiveType::BIGINT:
        return new CastBigIntExpr(node);
    case TPrimitiveType::LARGEINT:
        return new CastLargeIntExpr(node);
    case TPrimitiveType::FLOAT:
        return new CastFloatExpr(node);
    case TPrimitiveType::DOUBLE:
        return new CastDoubleExpr(node);
    default:
        return nullptr;
    }
    return nullptr;
}

#define CAST_SAME(CLASS, TYPE, FN) \
    TYPE CLASS::FN(ExprContext* context, TupleRow* row) { return _children[0]->FN(context, row); }

#define CAST_FUNCTION(CLASS, TO_TYPE, TO_FN, FROM_TYPE, FROM_FN) \
    TO_TYPE CLASS::TO_FN(ExprContext* context, TupleRow* row) {  \
        FROM_TYPE v = _children[0]->FROM_FN(context, row);       \
        if (v.is_null) {                                         \
            return TO_TYPE::null();                              \
        }                                                        \
        return TO_TYPE(v.val);                                   \
    }

#define CAST_FROM_BOOLEAN(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastBooleanExpr, TO_TYPE, TO_FN, BooleanVal, get_boolean_val)

CAST_SAME(CastBooleanExpr, BooleanVal, get_boolean_val)
CAST_FROM_BOOLEAN(TinyIntVal, get_tiny_int_val)
CAST_FROM_BOOLEAN(SmallIntVal, get_small_int_val)
CAST_FROM_BOOLEAN(IntVal, get_int_val)
CAST_FROM_BOOLEAN(BigIntVal, get_big_int_val)
CAST_FROM_BOOLEAN(LargeIntVal, get_large_int_val)
CAST_FROM_BOOLEAN(FloatVal, get_float_val)
CAST_FROM_BOOLEAN(DoubleVal, get_double_val)

#define CAST_FROM_TINYINT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastTinyIntExpr, TO_TYPE, TO_FN, TinyIntVal, get_tiny_int_val)

CAST_SAME(CastTinyIntExpr, TinyIntVal, get_tiny_int_val)
CAST_FROM_TINYINT(BooleanVal, get_boolean_val)
CAST_FROM_TINYINT(SmallIntVal, get_small_int_val)
CAST_FROM_TINYINT(IntVal, get_int_val)
CAST_FROM_TINYINT(BigIntVal, get_big_int_val)
CAST_FROM_TINYINT(LargeIntVal, get_large_int_val)
CAST_FROM_TINYINT(FloatVal, get_float_val)
CAST_FROM_TINYINT(DoubleVal, get_double_val)

#define CAST_FROM_SMALLINT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastSmallIntExpr, TO_TYPE, TO_FN, SmallIntVal, get_small_int_val)

CAST_SAME(CastSmallIntExpr, SmallIntVal, get_small_int_val)
CAST_FROM_SMALLINT(BooleanVal, get_boolean_val)
CAST_FROM_SMALLINT(TinyIntVal, get_tiny_int_val)
CAST_FROM_SMALLINT(IntVal, get_int_val)
CAST_FROM_SMALLINT(BigIntVal, get_big_int_val)
CAST_FROM_SMALLINT(LargeIntVal, get_large_int_val)
CAST_FROM_SMALLINT(FloatVal, get_float_val)
CAST_FROM_SMALLINT(DoubleVal, get_double_val)

#define CAST_FROM_INT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastIntExpr, TO_TYPE, TO_FN, IntVal, get_int_val)

CAST_SAME(CastIntExpr, IntVal, get_int_val)
CAST_FROM_INT(BooleanVal, get_boolean_val)
CAST_FROM_INT(TinyIntVal, get_tiny_int_val)
CAST_FROM_INT(SmallIntVal, get_small_int_val)
CAST_FROM_INT(BigIntVal, get_big_int_val)
CAST_FROM_INT(LargeIntVal, get_large_int_val)
CAST_FROM_INT(FloatVal, get_float_val)
CAST_FROM_INT(DoubleVal, get_double_val)

#define CAST_FROM_BIGINT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastBigIntExpr, TO_TYPE, TO_FN, BigIntVal, get_big_int_val)

CAST_SAME(CastBigIntExpr, BigIntVal, get_big_int_val)
CAST_FROM_BIGINT(BooleanVal, get_boolean_val)
CAST_FROM_BIGINT(TinyIntVal, get_tiny_int_val)
CAST_FROM_BIGINT(SmallIntVal, get_small_int_val)
CAST_FROM_BIGINT(IntVal, get_int_val)
CAST_FROM_BIGINT(LargeIntVal, get_large_int_val)
CAST_FROM_BIGINT(FloatVal, get_float_val)
CAST_FROM_BIGINT(DoubleVal, get_double_val)

#define CAST_FROM_LARGEINT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastLargeIntExpr, TO_TYPE, TO_FN, LargeIntVal, get_large_int_val)

CAST_SAME(CastLargeIntExpr, LargeIntVal, get_large_int_val)
CAST_FROM_LARGEINT(BooleanVal, get_boolean_val)
CAST_FROM_LARGEINT(TinyIntVal, get_tiny_int_val)
CAST_FROM_LARGEINT(SmallIntVal, get_small_int_val)
CAST_FROM_LARGEINT(IntVal, get_int_val)
CAST_FROM_LARGEINT(BigIntVal, get_big_int_val)
CAST_FROM_LARGEINT(FloatVal, get_float_val)
CAST_FROM_LARGEINT(DoubleVal, get_double_val)

#define CAST_FROM_FLOAT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastFloatExpr, TO_TYPE, TO_FN, FloatVal, get_float_val)

CAST_SAME(CastFloatExpr, FloatVal, get_float_val)
CAST_FROM_FLOAT(BooleanVal, get_boolean_val)
CAST_FROM_FLOAT(TinyIntVal, get_tiny_int_val)
CAST_FROM_FLOAT(SmallIntVal, get_small_int_val)
CAST_FROM_FLOAT(IntVal, get_int_val)
CAST_FROM_FLOAT(BigIntVal, get_big_int_val)
CAST_FROM_FLOAT(LargeIntVal, get_large_int_val)
CAST_FROM_FLOAT(DoubleVal, get_double_val)

#define CAST_FROM_DOUBLE(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastDoubleExpr, TO_TYPE, TO_FN, DoubleVal, get_double_val)

CAST_SAME(CastDoubleExpr, DoubleVal, get_double_val)
CAST_FROM_DOUBLE(BooleanVal, get_boolean_val)
CAST_FROM_DOUBLE(TinyIntVal, get_tiny_int_val)
CAST_FROM_DOUBLE(SmallIntVal, get_small_int_val)
CAST_FROM_DOUBLE(IntVal, get_int_val)
CAST_FROM_DOUBLE(BigIntVal, get_big_int_val)
CAST_FROM_DOUBLE(LargeIntVal, get_large_int_val)
CAST_FROM_DOUBLE(FloatVal, get_float_val)
} // namespace doris
