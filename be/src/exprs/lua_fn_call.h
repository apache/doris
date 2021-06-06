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

#pragma once

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "lua.hpp"
#include "udf/udf.h"

namespace doris {
class TExprNode;

class LUAFnCall : public Expr {
public:
    virtual ~LUAFnCall();
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new LUAFnCall(*this)); }
    LUAFnCall(const TExprNode& node);

    virtual Status prepare(RuntimeState* state, const RowDescriptor& desc, ExprContext* context);
    virtual Status open(RuntimeState* state, ExprContext* context,
                        FunctionContext::FunctionStateScope scope);
    virtual void close(RuntimeState* state, ExprContext* context,
                       FunctionContext::FunctionStateScope scope);

    virtual doris_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*);
    virtual doris_udf::TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*);
    virtual doris_udf::SmallIntVal get_small_int_val(ExprContext* context, TupleRow*);
    virtual doris_udf::IntVal get_int_val(ExprContext* context, TupleRow*);
    virtual doris_udf::BigIntVal get_big_int_val(ExprContext* context, TupleRow*);
    virtual doris_udf::FloatVal get_float_val(ExprContext* context, TupleRow*);
    virtual doris_udf::DoubleVal get_double_val(ExprContext* context, TupleRow*);
    virtual doris_udf::StringVal get_string_val(ExprContext* context, TupleRow*);

private:
    int eval_children(ExprContext* context, TupleRow* row);
    template <typename T>
    T _get_value_from_lua(ExprContext* context, TupleRow* row);

    UserFunctionCacheEntry* _cache_entry;
    lua_State* _state;
    int _fn_context_index;
    std::string _lua_function_symbol;
};
}; // namespace doris