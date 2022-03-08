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
#include "fmt/format.h"
#include "gen_cpp/function_service.pb.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"

namespace doris {

RPCFnCall::RPCFnCall(const TExprNode& node) : Expr(node), _fn_context_index(-1) {
    DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::RPC);
}

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
    _fn_context_index = context->register_func(state, return_type, arg_types, 0);

    // _fn.scalar_fn.symbol
    _rpc_function_symbol = _fn.scalar_fn.symbol;

    _client = state->exec_env()->brpc_function_client_cache()->get_client(_fn.hdfs_location);

    if (_client == nullptr) {
        return Status::InternalError(
                fmt::format("rpc env init error: {}/{}", _fn.hdfs_location, _rpc_function_symbol));
    }
    return Status::OK();
}

Status RPCFnCall::open(RuntimeState* state, ExprContext* ctx,
                       FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, ctx, scope));
    _state = state;
    return Status::OK();
}

void RPCFnCall::close(RuntimeState* state, ExprContext* context,
                      FunctionContext::FunctionStateScope scope) {
    Expr::close(state, context, scope);
}

Status RPCFnCall::call_rpc(ExprContext* context, TupleRow* row, PFunctionCallResponse* response) {
    PFunctionCallRequest request;
    request.set_function_name(_rpc_function_symbol);
    for (int i = 0; i < _children.size(); ++i) {
        PValues* arg = request.add_args();
        void* src_slot = context->get_value(_children[i], row);
        PGenericType* ptype = arg->mutable_type();
        if (src_slot == nullptr) {
            arg->set_has_null(true);
            arg->add_null_map(true);
        } else {
            arg->set_has_null(false);
        }
        switch (_children[i]->type().type) {
        case TYPE_BOOLEAN: {
            ptype->set_id(PGenericType::BOOLEAN);
            arg->add_bool_value(*(bool*)src_slot);
            break;
        }
        case TYPE_TINYINT: {
            ptype->set_id(PGenericType::INT8);
            arg->add_int32_value(*(int8_t*)src_slot);
            break;
        }
        case TYPE_SMALLINT: {
            ptype->set_id(PGenericType::INT16);
            arg->add_int32_value(*(int16_t*)src_slot);
            break;
        }
        case TYPE_INT: {
            ptype->set_id(PGenericType::INT32);
            arg->add_int32_value(*(int*)src_slot);
            break;
        }
        case TYPE_BIGINT: {
            ptype->set_id(PGenericType::INT64);
            arg->add_int64_value(*(int64_t*)src_slot);
            break;
        }
        case TYPE_LARGEINT: {
            ptype->set_id(PGenericType::INT128);
            char buffer[sizeof(__int128)];
            memcpy(buffer, src_slot, sizeof(__int128));
            arg->add_bytes_value(buffer, sizeof(__int128));
            break;
        }
        case TYPE_DOUBLE: {
            ptype->set_id(PGenericType::DOUBLE);
            arg->add_double_value(*(double*)src_slot);
            break;
        }
        case TYPE_FLOAT: {
            ptype->set_id(PGenericType::FLOAT);
            arg->add_float_value(*(float*)src_slot);
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_STRING:
        case TYPE_CHAR: {
            ptype->set_id(PGenericType::STRING);
            StringValue value = *reinterpret_cast<StringValue*>(src_slot);
            arg->add_string_value(value.ptr, value.len);
            break;
        }
        case TYPE_HLL: {
            ptype->set_id(PGenericType::HLL);
            StringValue value = *reinterpret_cast<StringValue*>(src_slot);
            arg->add_string_value(value.ptr, value.len);
            break;
        }
        case TYPE_OBJECT: {
            ptype->set_id(PGenericType::BITMAP);
            StringValue value = *reinterpret_cast<StringValue*>(src_slot);
            arg->add_string_value(value.ptr, value.len);
            break;
        }
        case TYPE_DECIMALV2: {
            ptype->set_id(PGenericType::DECIMAL128);
            ptype->mutable_decimal_type()->set_precision(_children[i]->type().precision);
            ptype->mutable_decimal_type()->set_scale(_children[i]->type().scale);
            char buffer[sizeof(__int128)];
            memcpy(buffer, src_slot, sizeof(__int128));
            arg->add_bytes_value(buffer, sizeof(__int128));
            break;
        }
        case TYPE_DATE: {
            ptype->set_id(PGenericType::DATE);
            const auto* time_val = (const DateTimeValue*)(src_slot);
            PDateTime* date_time = arg->add_datetime_value();
            date_time->set_day(time_val->day());
            date_time->set_month(time_val->month());
            date_time->set_year(time_val->year());
            break;
        }
        case TYPE_DATETIME: {
            ptype->set_id(PGenericType::DATETIME);
            const auto* time_val = (const DateTimeValue*)(src_slot);
            PDateTime* date_time = arg->add_datetime_value();
            date_time->set_day(time_val->day());
            date_time->set_month(time_val->month());
            date_time->set_year(time_val->year());
            date_time->set_hour(time_val->hour());
            date_time->set_minute(time_val->minute());
            date_time->set_second(time_val->second());
            date_time->set_microsecond(time_val->microsecond());
            break;
        }
        case TYPE_TIME: {
            ptype->set_id(PGenericType::DATETIME);
            const auto* time_val = (const DateTimeValue*)(src_slot);
            PDateTime* date_time = arg->add_datetime_value();
            date_time->set_hour(time_val->hour());
            date_time->set_minute(time_val->minute());
            date_time->set_second(time_val->second());
            date_time->set_microsecond(time_val->microsecond());
            break;
        }
        default: {
            FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
            std::string error_msg =
                    fmt::format("data time not supported: {}", _children[i]->type().type);
            fn_ctx->set_error(error_msg.c_str());
            cancel(error_msg);
            break;
        }
        }
    }

    brpc::Controller cntl;
    _client->fn_call(&cntl, &request, response, nullptr);
    if (cntl.Failed()) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        std::string error_msg = fmt::format("call rpc function {} failed: {}", _rpc_function_symbol,
                                            cntl.ErrorText());
        fn_ctx->set_error(error_msg.c_str());
        cancel(error_msg);
        return Status::InternalError(error_msg);
    }
    if (!response->has_status() || !response->has_result()) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        std::string error_msg =
                fmt::format("call rpc function {} failed: status or result is not set: {}",
                            _rpc_function_symbol, response->status().DebugString());
        fn_ctx->set_error(error_msg.c_str());
        cancel(error_msg);
        return Status::InternalError(error_msg);
    }
    if (response->status().status_code() != 0) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        std::string error_msg = fmt::format("call rpc function {} failed: {}", _rpc_function_symbol,
                                            response->status().DebugString());
        fn_ctx->set_error(error_msg.c_str());
        cancel(error_msg);
        return Status::InternalError(error_msg);
    }
    return Status::OK();
}

template <typename T>
T RPCFnCall::interpret_eval(ExprContext* context, TupleRow* row) {
    PFunctionCallResponse response;
    Status st = call_rpc(context, row, &response);
    WARN_IF_ERROR(st, "call rpc udf error");
    if (!st.ok() || (response.result().has_null() && response.result().null_map(0))) {
        return T::null();
    }
    T res_val;
    // TODO(yangzhg) deal with udtf and udaf
    const PValues& result = response.result();
    if constexpr (std::is_same_v<T, TinyIntVal>) {
        DCHECK(result.type().id() == PGenericType::INT8);
        res_val.val = static_cast<int8_t>(result.int32_value(0));
    } else if constexpr (std::is_same_v<T, SmallIntVal>) {
        DCHECK(result.type().id() == PGenericType::INT16);
        res_val.val = static_cast<int16_t>(result.int32_value(0));
    } else if constexpr (std::is_same_v<T, IntVal>) {
        DCHECK(result.type().id() == PGenericType::INT32);
        res_val.val = result.int32_value(0);
    } else if constexpr (std::is_same_v<T, BigIntVal>) {
        DCHECK(result.type().id() == PGenericType::INT64);
        res_val.val = result.int64_value(0);
    } else if constexpr (std::is_same_v<T, FloatVal>) {
        DCHECK(result.type().id() == PGenericType::FLOAT);
        res_val.val = result.float_value(0);
    } else if constexpr (std::is_same_v<T, DoubleVal>) {
        DCHECK(result.type().id() == PGenericType::DOUBLE);
        res_val.val = result.double_value(0);
    } else if constexpr (std::is_same_v<T, StringVal>) {
        DCHECK(result.type().id() == PGenericType::STRING);
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        StringVal val(fn_ctx, result.string_value(0).size());
        res_val = val.copy_from(fn_ctx,
                                reinterpret_cast<const uint8_t*>(result.string_value(0).c_str()),
                                result.string_value(0).size());
    } else if constexpr (std::is_same_v<T, LargeIntVal>) {
        DCHECK(result.type().id() == PGenericType::INT128);
        memcpy(&(res_val.val), result.bytes_value(0).data(), sizeof(__int128_t));
    } else if constexpr (std::is_same_v<T, DateTimeVal>) {
        DCHECK(result.type().id() == PGenericType::DATE ||
               result.type().id() == PGenericType::DATETIME);
        DateTimeValue value;
        value.set_time(result.datetime_value(0).year(), result.datetime_value(0).month(),
                       result.datetime_value(0).day(), result.datetime_value(0).hour(),
                       result.datetime_value(0).minute(), result.datetime_value(0).second(),
                       result.datetime_value(0).microsecond());
        if (result.type().id() == PGenericType::DATE) {
            value.set_type(TimeType::TIME_DATE);
        } else if (result.type().id() == PGenericType::DATETIME) {
            if (result.datetime_value(0).has_year()) {
                value.set_type(TimeType::TIME_DATETIME);
            } else
                value.set_type(TimeType::TIME_TIME);
        }
        value.to_datetime_val(&res_val);
    } else if constexpr (std::is_same_v<T, DecimalV2Val>) {
        DCHECK(result.type().id() == PGenericType::DECIMAL128);
        memcpy(&(res_val.val), result.bytes_value(0).data(), sizeof(__int128_t));
    }
    return res_val;
} // namespace doris

doris_udf::IntVal RPCFnCall::get_int_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<IntVal>(context, row);
}

doris_udf::BooleanVal RPCFnCall::get_boolean_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<BooleanVal>(context, row);
}

doris_udf::TinyIntVal RPCFnCall::get_tiny_int_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<TinyIntVal>(context, row);
}

doris_udf::SmallIntVal RPCFnCall::get_small_int_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<SmallIntVal>(context, row);
}

doris_udf::BigIntVal RPCFnCall::get_big_int_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<BigIntVal>(context, row);
}

doris_udf::FloatVal RPCFnCall::get_float_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<FloatVal>(context, row);
}

doris_udf::DoubleVal RPCFnCall::get_double_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<DoubleVal>(context, row);
}

doris_udf::StringVal RPCFnCall::get_string_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<StringVal>(context, row);
}

doris_udf::LargeIntVal RPCFnCall::get_large_int_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<LargeIntVal>(context, row);
}

doris_udf::DateTimeVal RPCFnCall::get_datetime_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<DateTimeVal>(context, row);
}

doris_udf::DecimalV2Val RPCFnCall::get_decimalv2_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<DecimalV2Val>(context, row);
}
doris_udf::CollectionVal RPCFnCall::get_array_val(ExprContext* context, TupleRow* row) {
    return interpret_eval<CollectionVal>(context, row);
}
void RPCFnCall::cancel(const std::string& msg) {
    _state->exec_env()->fragment_mgr()->cancel(_state->fragment_instance_id(),
                                               PPlanFragmentCancelReason::CALL_RPC_ERROR, msg);
}

} // namespace doris
