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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/cast-expr.h
// and modified by Doris

#ifndef DORIS_BE_SRC_EXPRS_CAST_EXPR_H
#define DORIS_BE_SRC_EXPRS_CAST_EXPR_H

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace doris {

class CastExpr : public Expr {
public:
    CastExpr(const TExprNode& node) : Expr(node) {}
    virtual ~CastExpr() {}
    static Expr* from_thrift(const TExprNode& node);
};

#define CAST_EXPR_DEFINE(CLASS)                                                          \
    class CLASS : public CastExpr {                                                      \
    public:                                                                              \
        CLASS(const TExprNode& node) : CastExpr(node) {}                                 \
        virtual ~CLASS() {}                                                              \
        virtual Expr* clone(ObjectPool* pool) const override {                           \
            return pool->add(new CLASS(*this));                                          \
        }                                                                                \
        virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;    \
        virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;   \
        virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override; \
        virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;            \
        virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;     \
        virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override; \
        virtual FloatVal get_float_val(ExprContext* context, TupleRow*) override;        \
        virtual DoubleVal get_double_val(ExprContext* context, TupleRow*) override;      \
    };

CAST_EXPR_DEFINE(CastBooleanExpr);
CAST_EXPR_DEFINE(CastTinyIntExpr);
CAST_EXPR_DEFINE(CastSmallIntExpr);
CAST_EXPR_DEFINE(CastIntExpr);
CAST_EXPR_DEFINE(CastBigIntExpr);
CAST_EXPR_DEFINE(CastLargeIntExpr);
CAST_EXPR_DEFINE(CastFloatExpr);
CAST_EXPR_DEFINE(CastDoubleExpr);

} // namespace doris

#endif
