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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_CONDITIONAL_FUNCTIONS_H
#define DORIS_BE_SRC_QUERY_EXPRS_CONDITIONAL_FUNCTIONS_H

#include <stdint.h>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "udf/udf.h"

namespace doris {

class TupleRow;

class ConditionalFunctions {
public:
};

/// The following conditional functions require separate Expr classes to take advantage of
/// short circuiting

class IfNullExpr : public Expr {
public:
    virtual ~IfNullExpr();
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new IfNullExpr(*this));
    }
    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow* row) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow* row) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow* row) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow* row) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow* row) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow* row) override;
    virtual StringVal get_string_val(ExprContext* context, TupleRow* row) override;
    virtual DateTimeVal get_datetime_val(ExprContext* context, TupleRow* row) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow* row) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow* row) override;

    virtual std::string debug_string() const override { return Expr::debug_string("IfNullExpr"); }

protected:
    friend class Expr;
    IfNullExpr(const TExprNode& node);
};

class NullIfExpr : public Expr {
public:
    virtual ~NullIfExpr();
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new NullIfExpr(*this));
    }
    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow* row) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow* row) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow* row) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow* row) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow* row) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow* row) override;
    virtual StringVal get_string_val(ExprContext* context, TupleRow* row) override;
    virtual DateTimeVal get_datetime_val(ExprContext* context, TupleRow* row) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow* row) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow* row) override;

    virtual std::string debug_string() const override { return Expr::debug_string("NullIfExpr"); }

protected:
    friend class Expr;
    NullIfExpr(const TExprNode& node);
};

class IfExpr : public Expr {
public:
    virtual ~IfExpr();
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new IfExpr(*this)); }
    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow* row) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow* row) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow* row) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow* row) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow* row) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow* row) override;
    virtual StringVal get_string_val(ExprContext* context, TupleRow* row) override;
    virtual DateTimeVal get_datetime_val(ExprContext* context, TupleRow* row) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow* row) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow* row) override;

    virtual std::string debug_string() const override { return Expr::debug_string("IfExpr"); }

protected:
    friend class Expr;
    IfExpr(const TExprNode& node);
};

// Returns the first non-nullptr value in the list, or nullptr if there are no non-nullptr values.
class CoalesceExpr : public Expr {
public:
    virtual ~CoalesceExpr();
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new CoalesceExpr(*this));
    }
    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow* row) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow* row) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow* row) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow* row) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow* row) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow* row) override;
    virtual StringVal get_string_val(ExprContext* context, TupleRow* row) override;
    virtual DateTimeVal get_datetime_val(ExprContext* context, TupleRow* row) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow* row) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow* row) override;

    virtual std::string debug_string() const override { return Expr::debug_string("CoalesceExpr"); }

protected:
    friend class Expr;
    CoalesceExpr(const TExprNode& node);
};

} // namespace doris

#endif
