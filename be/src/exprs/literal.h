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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_LITERAL_H
#define DORIS_BE_SRC_QUERY_EXPRS_LITERAL_H

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace doris {

class TExprNode;

class Literal : public Expr {
public:
    virtual ~Literal();

    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new Literal(*this)); }

    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow*);
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*);
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*);
    virtual IntVal get_int_val(ExprContext* context, TupleRow*);
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*);
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*);
    virtual FloatVal get_float_val(ExprContext* context, TupleRow*);
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow*);
    virtual DecimalVal get_decimal_val(ExprContext* context, TupleRow*);
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*);
    virtual DateTimeVal get_datetime_val(ExprContext* context, TupleRow*);
    virtual StringVal get_string_val(ExprContext* context, TupleRow* row);

protected:
    friend class Expr;
    Literal(const TExprNode& node);

private:
    ExprValue _value;
};

} // namespace doris

#endif
