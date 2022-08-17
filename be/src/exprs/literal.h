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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/literal.h
// and modified by Doris

#pragma once

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace doris {

class TExprNode;

class Literal final : public Expr {
public:
    Literal(const TExprNode& node);
    virtual ~Literal();

    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new Literal(*this)); }

    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
    virtual DateTimeVal get_datetime_val(ExprContext* context, TupleRow*) override;
    virtual DateV2Val get_datev2_val(ExprContext* context, TupleRow*) override;
    virtual DateTimeV2Val get_datetimev2_val(ExprContext* context, TupleRow*) override;
    virtual StringVal get_string_val(ExprContext* context, TupleRow* row) override;
    virtual CollectionVal get_array_val(ExprContext* context, TupleRow*) override;
    virtual Decimal32Val get_decimal32_val(ExprContext* context, TupleRow*) override;
    virtual Decimal64Val get_decimal64_val(ExprContext* context, TupleRow*) override;
    virtual Decimal128Val get_decimal128_val(ExprContext* context, TupleRow*) override;
    // init val before use
    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           ExprContext* context) override;

private:
    ExprValue _value;
};

} // namespace doris
