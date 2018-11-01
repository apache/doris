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

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_NULL_LITERAL_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_NULL_LITERAL_H

#include "exprs/expr.h"

namespace llvm {
class Function;
}

namespace palo {

class TExprNode;

class NullLiteral : public Expr {
public:

    virtual Expr* clone(ObjectPool* pool) const override { 
        return pool->add(new NullLiteral(*this));
    }
    // NullLiteral(PrimitiveType type);
    virtual Status get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn);
    virtual palo_udf::BooleanVal get_boolean_val(ExprContext*, TupleRow*);
    virtual palo_udf::TinyIntVal get_tiny_int_val(ExprContext*, TupleRow*);
    virtual palo_udf::SmallIntVal get_small_int_val(ExprContext*, TupleRow*);
    virtual palo_udf::IntVal get_int_val(ExprContext*, TupleRow*);
    virtual palo_udf::BigIntVal get_big_int_val(ExprContext*, TupleRow*);
    virtual palo_udf::FloatVal get_float_val(ExprContext*, TupleRow*);
    virtual palo_udf::DoubleVal get_double_val(ExprContext*, TupleRow*);
    virtual palo_udf::StringVal get_string_val(ExprContext*, TupleRow*);
    virtual palo_udf::DateTimeVal get_datetime_val(ExprContext*, TupleRow*);
    virtual palo_udf::DecimalVal get_decimal_val(ExprContext*, TupleRow*);

protected:
    friend class Expr;

    NullLiteral(const TExprNode& node);

private:
    static void* return_value(Expr* e, TupleRow* row);
};

}

#endif
