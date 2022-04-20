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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/conditional-functions.cc
// and modified by Doris

#include "exprs/conditional_functions.h"

#include "exprs/anyval_util.h"
#include "exprs/case_expr.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
#include "udf/udf.h"

namespace doris {

#define CTOR_DCTOR_FUN(expr_class)                                 \
    expr_class::expr_class(const TExprNode& node) : Expr(node) {}; \
                                                                   \
    expr_class::~expr_class() {};

CTOR_DCTOR_FUN(IfNullExpr);
CTOR_DCTOR_FUN(NullIfExpr);
CTOR_DCTOR_FUN(IfExpr);
CTOR_DCTOR_FUN(CoalesceExpr);

#define IF_NULL_COMPUTE_FUNCTION(type, type_name)                           \
    type IfNullExpr::get_##type_name(ExprContext* context, TupleRow* row) { \
        DCHECK_EQ(_children.size(), 2);                                     \
        type val = _children[0]->get_##type_name(context, row);             \
        if (!val.is_null) return val; /* short-circuit */                   \
        return _children[1]->get_##type_name(context, row);                 \
    }

IF_NULL_COMPUTE_FUNCTION(BooleanVal, boolean_val);
IF_NULL_COMPUTE_FUNCTION(TinyIntVal, tiny_int_val);
IF_NULL_COMPUTE_FUNCTION(SmallIntVal, small_int_val);
IF_NULL_COMPUTE_FUNCTION(IntVal, int_val);
IF_NULL_COMPUTE_FUNCTION(BigIntVal, big_int_val);
IF_NULL_COMPUTE_FUNCTION(FloatVal, float_val);
IF_NULL_COMPUTE_FUNCTION(DoubleVal, double_val);
IF_NULL_COMPUTE_FUNCTION(StringVal, string_val);
IF_NULL_COMPUTE_FUNCTION(DateTimeVal, datetime_val);
IF_NULL_COMPUTE_FUNCTION(DecimalV2Val, decimalv2_val);
IF_NULL_COMPUTE_FUNCTION(LargeIntVal, large_int_val);

#define NULL_IF_COMPUTE_FUNCTION(TYPE, type_name)                                             \
    TYPE NullIfExpr::get_##type_name(ExprContext* ctx, TupleRow* row) {                       \
        DCHECK_EQ(_children.size(), 2);                                                       \
        TYPE lhs_val = _children[0]->get_##type_name(ctx, row);                               \
        /* Short-circuit in case lhs_val is nullptr. Can never be equal to RHS. */            \
        if (lhs_val.is_null) return TYPE::null();                                             \
        /* Get rhs and return nullptr if lhs == rhs, lhs otherwise */                         \
        TYPE rhs_val = _children[1]->get_##type_name(ctx, row);                               \
        if (!rhs_val.is_null && AnyValUtil::equals(_children[0]->type(), lhs_val, rhs_val)) { \
            return TYPE::null();                                                              \
        }                                                                                     \
        return lhs_val;                                                                       \
    }

// Just for code check.....
#define NULL_IF_COMPUTE_FUNCTION_WRAPPER(TYPE, type_name) NULL_IF_COMPUTE_FUNCTION(TYPE, type_name)

NULL_IF_COMPUTE_FUNCTION_WRAPPER(BooleanVal, boolean_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(TinyIntVal, tiny_int_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(SmallIntVal, small_int_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(IntVal, int_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(BigIntVal, big_int_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(FloatVal, float_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(DoubleVal, double_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(StringVal, string_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(DateTimeVal, datetime_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(DecimalV2Val, decimalv2_val);
NULL_IF_COMPUTE_FUNCTION_WRAPPER(LargeIntVal, large_int_val);

#define IF_COMPUTE_FUNCTION(type, type_name)                            \
    type IfExpr::get_##type_name(ExprContext* context, TupleRow* row) { \
        DCHECK_EQ(_children.size(), 3);                                 \
        BooleanVal cond = _children[0]->get_boolean_val(context, row);  \
        if (cond.is_null || !cond.val) {                                \
            return _children[2]->get_##type_name(context, row);         \
        }                                                               \
        return _children[1]->get_##type_name(context, row);             \
    }

IF_COMPUTE_FUNCTION(BooleanVal, boolean_val);
IF_COMPUTE_FUNCTION(TinyIntVal, tiny_int_val);
IF_COMPUTE_FUNCTION(SmallIntVal, small_int_val);
IF_COMPUTE_FUNCTION(IntVal, int_val);
IF_COMPUTE_FUNCTION(BigIntVal, big_int_val);
IF_COMPUTE_FUNCTION(FloatVal, float_val);
IF_COMPUTE_FUNCTION(DoubleVal, double_val);
IF_COMPUTE_FUNCTION(StringVal, string_val);
IF_COMPUTE_FUNCTION(DateTimeVal, datetime_val);
IF_COMPUTE_FUNCTION(DecimalV2Val, decimalv2_val);
IF_COMPUTE_FUNCTION(LargeIntVal, large_int_val);

#define COALESCE_COMPUTE_FUNCTION(type, type_name)                            \
    type CoalesceExpr::get_##type_name(ExprContext* context, TupleRow* row) { \
        DCHECK_GE(_children.size(), 1);                                       \
        for (int i = 0; i < _children.size(); ++i) {                          \
            type val = _children[i]->get_##type_name(context, row);           \
            if (!val.is_null) return val;                                     \
        }                                                                     \
        return type::null();                                                  \
    }

COALESCE_COMPUTE_FUNCTION(BooleanVal, boolean_val);
COALESCE_COMPUTE_FUNCTION(TinyIntVal, tiny_int_val);
COALESCE_COMPUTE_FUNCTION(SmallIntVal, small_int_val);
COALESCE_COMPUTE_FUNCTION(IntVal, int_val);
COALESCE_COMPUTE_FUNCTION(BigIntVal, big_int_val);
COALESCE_COMPUTE_FUNCTION(FloatVal, float_val);
COALESCE_COMPUTE_FUNCTION(DoubleVal, double_val);
COALESCE_COMPUTE_FUNCTION(StringVal, string_val);
COALESCE_COMPUTE_FUNCTION(DateTimeVal, datetime_val);
COALESCE_COMPUTE_FUNCTION(DecimalV2Val, decimalv2_val);
COALESCE_COMPUTE_FUNCTION(LargeIntVal, large_int_val);
} // namespace doris
