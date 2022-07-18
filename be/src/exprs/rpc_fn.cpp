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

#include "exprs/rpc_fn.h"

#include <fmt/format.h>

#include "exprs/rpc_fn_comm.h"
#include "runtime/fragment_mgr.h"
#include "util/brpc_client_cache.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {

RPCFn::RPCFn(RuntimeState* state, const TFunction& fn, int fn_ctx_id, bool is_agg)
        : _state(state), _fn(fn), _fn_ctx_id(fn_ctx_id), _is_agg(is_agg) {
    _client = ExecEnv::GetInstance()->brpc_function_client_cache()->get_client(_server_addr);
    if (!_is_agg) {
        _function_name = _fn.scalar_fn.symbol;
        _server_addr = _fn.hdfs_location;
        _signature = fmt::format("{}: [{}/{}]", _fn.name.function_name, _fn.hdfs_location,
                                 _fn.scalar_fn.symbol);
    }
}

RPCFn::RPCFn(const TFunction& fn, bool is_agg) : RPCFn(nullptr, fn, -1, is_agg) {}

RPCFn::RPCFn(RuntimeState* state, const TFunction& fn, AggregationStep step, bool is_agg)
        : RPCFn(nullptr, fn, -1, is_agg) {
    _step = step;
    DCHECK(is_agg) << "Only used for agg fns";
    switch (_step) {
    case INIT: {
        _function_name = _fn.aggregate_fn.init_fn_symbol;
        _server_addr = _fn.hdfs_location;
        _signature = fmt::format("{}: [{}/{}]", _fn.name.function_name, _fn.hdfs_location,
                                 _fn.aggregate_fn.init_fn_symbol);
        break;
    }
    case UPDATE: {
        _function_name = _fn.aggregate_fn.init_fn_symbol;
        break;
    }
    case MERGE: {
        _function_name = _fn.aggregate_fn.merge_fn_symbol;
        break;
    }
    case SERIALIZE: {
        _function_name = _fn.aggregate_fn.serialize_fn_symbol;
        break;
    }
    case GET_VALUE: {
        _function_name = _fn.aggregate_fn.get_value_fn_symbol;
        break;
    }
    case FINALIZE: {
        _function_name = _fn.aggregate_fn.finalize_fn_symbol;
        break;
    }
    case REMOVE: {
        _function_name = _fn.aggregate_fn.remove_fn_symbol;
        break;
    }

    default:
        CHECK(false) << "invalid AggregationStep: " << _step;
        break;
    }
    _server_addr = _fn.hdfs_location;
    _signature = fmt::format("{}: [{}/{}]", _fn.name.function_name, _server_addr, _function_name);
}

Status RPCFn::call_internal(ExprContext* context, TupleRow* row, PFunctionCallResponse* response,
                            const std::vector<Expr*>& exprs) {
    FunctionContext* fn_ctx = context->fn_context(_fn_ctx_id);
    PFunctionCallRequest request;
    request.set_function_name(_function_name);
    for (int i = 0; i < exprs.size(); ++i) {
        PValues* arg = request.add_args();
        void* src_slot = context->get_value(exprs[i], row);
        PGenericType* ptype = arg->mutable_type();
        if (src_slot == nullptr) {
            arg->set_has_null(true);
            arg->add_null_map(true);
        } else {
            arg->set_has_null(false);
        }
        switch (exprs[i]->type().type) {
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
            ptype->mutable_decimal_type()->set_precision(exprs[i]->type().precision);
            ptype->mutable_decimal_type()->set_scale(exprs[i]->type().scale);
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
            std::string error_msg =
                    fmt::format("data time not supported: {}", exprs[i]->type().type);
            fn_ctx->set_error(error_msg.c_str());
            cancel(error_msg);
            break;
        }
        }
    }

    brpc::Controller cntl;
    _client->fn_call(&cntl, &request, response, nullptr);
    if (cntl.Failed()) {
        std::string error_msg =
                fmt::format("call rpc function {} failed: {}", _signature, cntl.ErrorText());
        fn_ctx->set_error(error_msg.c_str());
        cancel(error_msg);
        return Status::InternalError(error_msg);
    }
    if (!response->has_status() || response->result_size() == 0) {
        std::string error_msg =
                fmt::format("call rpc function {} failed: status or result is not set: {}",
                            _signature, response->status().DebugString());
        fn_ctx->set_error(error_msg.c_str());
        cancel(error_msg);
        return Status::InternalError(error_msg);
    }
    if (response->status().status_code() != 0) {
        std::string error_msg = fmt::format("call rpc function {} failed: {}", _signature,
                                            response->status().DebugString());
        fn_ctx->set_error(error_msg.c_str());
        cancel(error_msg);
        return Status::InternalError(error_msg);
    }
    return Status::OK();
}

void RPCFn::cancel(const std::string& msg) {
    _state->exec_env()->fragment_mgr()->cancel(_state->fragment_instance_id(),
                                               PPlanFragmentCancelReason::CALL_RPC_ERROR, msg);
}

Status RPCFn::vec_call(FunctionContext* context, vectorized::Block& block,
                       const vectorized::ColumnNumbers& arguments, size_t result,
                       size_t input_rows_count) {
    PFunctionCallRequest request;
    PFunctionCallResponse response;
    request.set_function_name(_function_name);
    convert_block_to_proto(block, arguments, input_rows_count, &request);
    brpc::Controller cntl;
    _client->fn_call(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status::InternalError("call to rpc function {} failed: {}", _signature,
                                     cntl.ErrorText());
    }
    if (!response.has_status() || response.result_size() == 0) {
        return Status::InternalError("call rpc function {} failed: status or result is not set.",
                                     _signature);
    }
    if (response.status().status_code() != 0) {
        return Status::InternalError("call to rpc function {} failed: {}", _signature,
                                     response.status().DebugString());
    }
    convert_to_block(block, response.result(0), result);
    return Status::OK();
}
} // namespace doris
