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
#include "udf/udf.h"

namespace doris {
class TExprNode;
class PFunctionService_Stub;
class PFunctionCallResponse;

class RPCFnCall : public Expr {
public:
    RPCFnCall(const TExprNode& node);
    ~RPCFnCall() = default;

    virtual Status prepare(RuntimeState* state, const RowDescriptor& desc,
                           ExprContext* context) override;
    virtual Status open(RuntimeState* state, ExprContext* context,
                        FunctionContext::FunctionStateScope scope) override;
    virtual void close(RuntimeState* state, ExprContext* context,
                       FunctionContext::FunctionStateScope scope) override;
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new RPCFnCall(*this)); }

    virtual doris_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::StringVal get_string_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DateTimeVal get_datetime_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::CollectionVal get_array_val(ExprContext* context, TupleRow*) override;

private:
    Status call_rpc(ExprContext* context, TupleRow* row, PFunctionCallResponse* response);
    template <typename RETURN_TYPE>
    RETURN_TYPE interpret_eval(ExprContext* context, TupleRow* row);
    void cancel(const std::string& msg);

    std::shared_ptr<PFunctionService_Stub> _client = nullptr;
    int _fn_context_index;
    std::string _rpc_function_symbol;
    RuntimeState* _state;
};
} // namespace doris
