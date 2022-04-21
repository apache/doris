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

#include "exprs/rpc_fn_call.h"

#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "exprs/rpc_fn.h"
#include "fmt/format.h"
#include "rpc_fn.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"

namespace doris {

RPCFnCall::RPCFnCall(const TExprNode& node) : Expr(node), _tnode(node) {
    DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::RPC);
}

RPCFnCall::~RPCFnCall() {}

Status RPCFnCall::prepare(RuntimeState* state, const RowDescriptor& desc, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, desc, context));
    DCHECK(!_fn.scalar_fn.symbol.empty());

    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> arg_types;
    bool char_arg = false;
    for (int i = 0; i < _children.size(); ++i) {
        arg_types.push_back(AnyValUtil::column_type_to_type_desc(_children[i]->type()));
        char_arg = char_arg || (_children[i]->type().type == TYPE_CHAR);
    }
    int id = context->register_func(state, return_type, arg_types, 0);

    _rpc_fn = std::make_unique<RPCFn>(state, _fn, id, false);
    if (!_rpc_fn->avliable()) {
        return Status::InternalError(
                fmt::format("rpc env init error: {}/{}", _fn.hdfs_location, _fn.scalar_fn.symbol));
    }
    return Status::OK();
}

Status RPCFnCall::open(RuntimeState* state, ExprContext* ctx,
                       FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, ctx, scope));
    return Status::OK();
}

void RPCFnCall::close(RuntimeState* state, ExprContext* context,
                      FunctionContext::FunctionStateScope scope) {
    Expr::close(state, context, scope);
}

doris_udf::IntVal RPCFnCall::get_int_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<IntVal>(context, row, _children);
}

doris_udf::BooleanVal RPCFnCall::get_boolean_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<BooleanVal>(context, row, _children);
}

doris_udf::TinyIntVal RPCFnCall::get_tiny_int_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<TinyIntVal>(context, row, _children);
}

doris_udf::SmallIntVal RPCFnCall::get_small_int_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<SmallIntVal>(context, row, _children);
}

doris_udf::BigIntVal RPCFnCall::get_big_int_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<BigIntVal>(context, row, _children);
}

doris_udf::FloatVal RPCFnCall::get_float_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<FloatVal>(context, row, _children);
}

doris_udf::DoubleVal RPCFnCall::get_double_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<DoubleVal>(context, row, _children);
}

doris_udf::StringVal RPCFnCall::get_string_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<StringVal>(context, row, _children);
}

doris_udf::LargeIntVal RPCFnCall::get_large_int_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<LargeIntVal>(context, row, _children);
}

doris_udf::DateTimeVal RPCFnCall::get_datetime_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<DateTimeVal>(context, row, _children);
}

doris_udf::DecimalV2Val RPCFnCall::get_decimalv2_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<DecimalV2Val>(context, row, _children);
}
doris_udf::CollectionVal RPCFnCall::get_array_val(ExprContext* context, TupleRow* row) {
    return _rpc_fn->call<CollectionVal>(context, row, _children);
}
} // namespace doris
