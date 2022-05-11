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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/null-literal.h
// and modified by Doris

#pragma once

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace doris {

class TExprNode;

class NullLiteral : public Expr {
public:
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new NullLiteral(*this));
    }
    // NullLiteral(PrimitiveType type);
    virtual doris_udf::BooleanVal get_boolean_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::TinyIntVal get_tiny_int_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::SmallIntVal get_small_int_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::IntVal get_int_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::BigIntVal get_big_int_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::FloatVal get_float_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::DoubleVal get_double_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::StringVal get_string_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::DateTimeVal get_datetime_val(ExprContext*, TupleRow*) override;
    virtual doris_udf::DecimalV2Val get_decimalv2_val(ExprContext*, TupleRow*) override;
    virtual CollectionVal get_array_val(ExprContext* context, TupleRow*) override;

protected:
    friend class Expr;

    NullLiteral(const TExprNode& node);

private:
    static void* return_value(Expr* e, TupleRow* row);
};

} // namespace doris
