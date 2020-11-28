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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_CASE_EXPR_H
#define DORIS_BE_SRC_QUERY_EXPRS_CASE_EXPR_H

#include <string>

#include "common/object_pool.h"
#include "expr.h"

namespace doris {

class TExprNode;

class CaseExpr : public Expr {
public:
    virtual ~CaseExpr();
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new CaseExpr(*this)); }
    virtual BooleanVal get_boolean_val(ExprContext* ctx, TupleRow* row);
    virtual TinyIntVal get_tiny_int_val(ExprContext* ctx, TupleRow* row);
    virtual SmallIntVal get_small_int_val(ExprContext* ctx, TupleRow* row);
    virtual IntVal get_int_val(ExprContext* ctx, TupleRow* row);
    virtual BigIntVal get_big_int_val(ExprContext* ctx, TupleRow* row);
    virtual FloatVal get_float_val(ExprContext* ctx, TupleRow* row);
    virtual DoubleVal get_double_val(ExprContext* ctx, TupleRow* row);
    virtual StringVal get_string_val(ExprContext* ctx, TupleRow* row);
    virtual DateTimeVal get_datetime_val(ExprContext* ctx, TupleRow* row);
    virtual DecimalVal get_decimal_val(ExprContext* ctx, TupleRow* row);
    virtual DecimalV2Val get_decimalv2_val(ExprContext* ctx, TupleRow* row);

protected:
    friend class Expr;
    friend class ComputeFunctions;
    friend class ConditionalFunctions;
    friend class DecimalOperators;
    friend class DecimalV2Operators;

    CaseExpr(const TExprNode& node);
    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           ExprContext* context);
    virtual Status open(RuntimeState* state, ExprContext* context,
                        FunctionContext::FunctionStateScope scope);
    virtual void close(RuntimeState* state, ExprContext* context,
                       FunctionContext::FunctionStateScope scope);

    virtual std::string debug_string() const;

    bool has_case_expr() { return _has_case_expr; }

    bool has_else_expr() { return _has_else_expr; }

private:
    const bool _has_case_expr;
    const bool _has_else_expr;

    /// Populates 'dst' with the result of calling the appropriate Get*Val() function on the
    /// specified child expr.
    void get_child_val(int child_idx, ExprContext* ctx, TupleRow* row, AnyVal* dst);

    /// Return true iff *v1 == *v2. v1 and v2 should both be of the specified type.
    bool any_val_eq(const TypeDescriptor& type, const AnyVal* v1, const AnyVal* v2);
};

} // namespace doris

#endif
