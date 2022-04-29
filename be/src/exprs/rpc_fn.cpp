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

#include "runtime/fragment_mgr.h"
#include "runtime/user_function_cache.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

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

template <bool nullable>
void convert_col_to_pvalue(const vectorized::ColumnPtr& column,
                           const vectorized::DataTypePtr& data_type, PValues* arg,
                           size_t row_count) {
    PGenericType* ptype = arg->mutable_type();
    switch (data_type->get_type_id()) {
    case vectorized::TypeIndex::UInt8: {
        ptype->set_id(PGenericType::UINT8);
        auto* values = arg->mutable_bool_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt8>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::UInt16: {
        ptype->set_id(PGenericType::UINT16);
        auto* values = arg->mutable_uint32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt16>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::UInt32: {
        ptype->set_id(PGenericType::UINT32);
        auto* values = arg->mutable_uint32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt32>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::UInt64: {
        ptype->set_id(PGenericType::UINT64);
        auto* values = arg->mutable_uint64_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt64>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::UInt128: {
        ptype->set_id(PGenericType::UINT128);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = 0; row_num < row_count; ++row_num) {
            if constexpr (nullable) {
                if (column->is_null_at(row_num)) {
                    arg->add_bytes_value(nullptr);
                } else {
                    StringRef data = column->get_data_at(row_num);
                    arg->add_bytes_value(data.data, data.size);
                }
            } else {
                StringRef data = column->get_data_at(row_num);
                arg->add_bytes_value(data.data, data.size);
            }
        }
        break;
    }
    case vectorized::TypeIndex::Int8: {
        ptype->set_id(PGenericType::INT8);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnInt8>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::Int16: {
        ptype->set_id(PGenericType::INT16);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnInt16>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::Int32: {
        ptype->set_id(PGenericType::INT32);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnInt32>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::Int64: {
        ptype->set_id(PGenericType::INT64);
        auto* values = arg->mutable_int64_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnInt64>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::Int128: {
        ptype->set_id(PGenericType::INT128);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = 0; row_num < row_count; ++row_num) {
            if constexpr (nullable) {
                if (column->is_null_at(row_num)) {
                    arg->add_bytes_value(nullptr);
                } else {
                    StringRef data = column->get_data_at(row_num);
                    arg->add_bytes_value(data.data, data.size);
                }
            } else {
                StringRef data = column->get_data_at(row_num);
                arg->add_bytes_value(data.data, data.size);
            }
        }
        break;
    }
    case vectorized::TypeIndex::Float32: {
        ptype->set_id(PGenericType::FLOAT);
        auto* values = arg->mutable_float_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnFloat32>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }

    case vectorized::TypeIndex::Float64: {
        ptype->set_id(PGenericType::DOUBLE);
        auto* values = arg->mutable_double_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnFloat64>(column);
        auto& data = col->get_data();
        values->Add(data.begin(), data.begin() + row_count);
        break;
    }
    case vectorized::TypeIndex::Decimal128: {
        ptype->set_id(PGenericType::DECIMAL128);
        auto dec_type = std::reinterpret_pointer_cast<
                const vectorized::DataTypeDecimal<vectorized::Decimal128>>(data_type);
        ptype->mutable_decimal_type()->set_precision(dec_type->get_precision());
        ptype->mutable_decimal_type()->set_scale(dec_type->get_scale());
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = 0; row_num < row_count; ++row_num) {
            if constexpr (nullable) {
                if (column->is_null_at(row_num)) {
                    arg->add_bytes_value(nullptr);
                } else {
                    StringRef data = column->get_data_at(row_num);
                    arg->add_bytes_value(data.data, data.size);
                }
            } else {
                StringRef data = column->get_data_at(row_num);
                arg->add_bytes_value(data.data, data.size);
            }
        }
        break;
    }
    case vectorized::TypeIndex::String: {
        ptype->set_id(PGenericType::STRING);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = 0; row_num < row_count; ++row_num) {
            if constexpr (nullable) {
                if (column->is_null_at(row_num)) {
                    arg->add_string_value(nullptr);
                } else {
                    StringRef data = column->get_data_at(row_num);
                    arg->add_string_value(data.to_string());
                }
            } else {
                StringRef data = column->get_data_at(row_num);
                arg->add_string_value(data.to_string());
            }
        }
        break;
    }
    case vectorized::TypeIndex::Date: {
        ptype->set_id(PGenericType::DATE);
        arg->mutable_datetime_value()->Reserve(row_count);
        for (size_t row_num = 0; row_num < row_count; ++row_num) {
            PDateTime* date_time = arg->add_datetime_value();
            if constexpr (nullable) {
                if (!column->is_null_at(row_num)) {
                    vectorized::VecDateTimeValue v =
                            vectorized::VecDateTimeValue::create_from_olap_date(
                                    column->get_int(row_num));
                    date_time->set_day(v.day());
                    date_time->set_month(v.month());
                    date_time->set_year(v.year());
                }
            } else {
                vectorized::VecDateTimeValue v =
                        vectorized::VecDateTimeValue::create_from_olap_date(
                                column->get_int(row_num));
                date_time->set_day(v.day());
                date_time->set_month(v.month());
                date_time->set_year(v.year());
            }
        }
        break;
    }
    case vectorized::TypeIndex::DateTime: {
        ptype->set_id(PGenericType::DATETIME);
        arg->mutable_datetime_value()->Reserve(row_count);
        for (size_t row_num = 0; row_num < row_count; ++row_num) {
            PDateTime* date_time = arg->add_datetime_value();
            if constexpr (nullable) {
                if (!column->is_null_at(row_num)) {
                    vectorized::VecDateTimeValue v =
                            vectorized::VecDateTimeValue::create_from_olap_datetime(
                                    column->get_int(row_num));
                    date_time->set_day(v.day());
                    date_time->set_month(v.month());
                    date_time->set_year(v.year());
                    date_time->set_hour(v.hour());
                    date_time->set_minute(v.minute());
                    date_time->set_second(v.second());
                }
            } else {
                vectorized::VecDateTimeValue v =
                        vectorized::VecDateTimeValue::create_from_olap_datetime(
                                column->get_int(row_num));
                date_time->set_day(v.day());
                date_time->set_month(v.month());
                date_time->set_year(v.year());
                date_time->set_hour(v.hour());
                date_time->set_minute(v.minute());
                date_time->set_second(v.second());
            }
        }
        break;
    }
    case vectorized::TypeIndex::BitMap: {
        ptype->set_id(PGenericType::BITMAP);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = 0; row_num < row_count; ++row_num) {
            if constexpr (nullable) {
                if (column->is_null_at(row_num)) {
                    arg->add_bytes_value(nullptr);
                } else {
                    StringRef data = column->get_data_at(row_num);
                    arg->add_bytes_value(data.data, data.size);
                }
            } else {
                StringRef data = column->get_data_at(row_num);
                arg->add_bytes_value(data.data, data.size);
            }
        }
        break;
    }
    case vectorized::TypeIndex::HLL: {
        ptype->set_id(PGenericType::HLL);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = 0; row_num < row_count; ++row_num) {
            if constexpr (nullable) {
                if (column->is_null_at(row_num)) {
                    arg->add_bytes_value(nullptr);
                } else {
                    StringRef data = column->get_data_at(row_num);
                    arg->add_bytes_value(data.data, data.size);
                }
            } else {
                StringRef data = column->get_data_at(row_num);
                arg->add_bytes_value(data.data, data.size);
            }
        }
        break;
    }
    default:
        LOG(INFO) << "unknown type: " << data_type->get_name();
        ptype->set_id(PGenericType::UNKNOWN);
        break;
    }
}

void convert_nullable_col_to_pvalue(const vectorized::ColumnPtr& column,
                                    const vectorized::DataTypePtr& data_type,
                                    const vectorized::ColumnUInt8& null_col, PValues* arg,
                                    size_t row_count) {
    if (column->has_null(row_count)) {
        auto* null_map = arg->mutable_null_map();
        null_map->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt8>(null_col);
        auto& data = col->get_data();
        null_map->Add(data.begin(), data.begin() + row_count);
        convert_col_to_pvalue<true>(column, data_type, arg, row_count);
    } else {
        convert_col_to_pvalue<false>(column, data_type, arg, row_count);
    }
}

void convert_block_to_proto(vectorized::Block& block, const vectorized::ColumnNumbers& arguments,
                            size_t input_rows_count, PFunctionCallRequest* request) {
    size_t row_count = std::min(block.rows(), input_rows_count);
    for (size_t col_idx : arguments) {
        PValues* arg = request->add_args();
        vectorized::ColumnWithTypeAndName& column = block.get_by_position(col_idx);
        arg->set_has_null(column.column->has_null(row_count));
        auto col = column.column->convert_to_full_column_if_const();
        if (auto* nullable =
                    vectorized::check_and_get_column<const vectorized::ColumnNullable>(*col)) {
            auto data_col = nullable->get_nested_column_ptr();
            auto& null_col = nullable->get_null_map_column();
            auto data_type =
                    std::reinterpret_pointer_cast<const vectorized::DataTypeNullable>(column.type);
            convert_nullable_col_to_pvalue(data_col->convert_to_full_column_if_const(),
                                           data_type->get_nested_type(), null_col, arg, row_count);
        } else {
            convert_col_to_pvalue<false>(col, column.type, arg, row_count);
        }
    }
}

template <bool nullable>
void convert_to_column(vectorized::MutableColumnPtr& column, const PValues& result) {
    switch (result.type().id()) {
    case PGenericType::UINT8: {
        column->reserve(result.uint32_value_size());
        column->resize(result.uint32_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnUInt8*>(column.get())->get_data();
        for (int i = 0; i < result.uint32_value_size(); ++i) {
            data[i] = result.uint32_value(i);
        }
        break;
    }
    case PGenericType::UINT16: {
        column->reserve(result.uint32_value_size());
        column->resize(result.uint32_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnUInt16*>(column.get())->get_data();
        for (int i = 0; i < result.uint32_value_size(); ++i) {
            data[i] = result.uint32_value(i);
        }
        break;
    }
    case PGenericType::UINT32: {
        column->reserve(result.uint32_value_size());
        column->resize(result.uint32_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnUInt32*>(column.get())->get_data();
        for (int i = 0; i < result.uint32_value_size(); ++i) {
            data[i] = result.uint32_value(i);
        }
        break;
    }
    case PGenericType::UINT64: {
        column->reserve(result.uint64_value_size());
        column->resize(result.uint64_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnUInt64*>(column.get())->get_data();
        for (int i = 0; i < result.uint64_value_size(); ++i) {
            data[i] = result.uint64_value(i);
        }
        break;
    }
    case PGenericType::INT8: {
        column->reserve(result.int32_value_size());
        column->resize(result.int32_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnInt16*>(column.get())->get_data();
        for (int i = 0; i < result.int32_value_size(); ++i) {
            data[i] = result.int32_value(i);
        }
        break;
    }
    case PGenericType::INT16: {
        column->reserve(result.int32_value_size());
        column->resize(result.int32_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnInt16*>(column.get())->get_data();
        for (int i = 0; i < result.int32_value_size(); ++i) {
            data[i] = result.int32_value(i);
        }
        break;
    }
    case PGenericType::INT32: {
        column->reserve(result.int32_value_size());
        column->resize(result.int32_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnInt32*>(column.get())->get_data();
        for (int i = 0; i < result.int32_value_size(); ++i) {
            data[i] = result.int32_value(i);
        }
        break;
    }
    case PGenericType::INT64: {
        column->reserve(result.int64_value_size());
        column->resize(result.int64_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnInt64*>(column.get())->get_data();
        for (int i = 0; i < result.int64_value_size(); ++i) {
            data[i] = result.int64_value(i);
        }
        break;
    }
    case PGenericType::DATE:
    case PGenericType::DATETIME: {
        column->reserve(result.datetime_value_size());
        column->resize(result.datetime_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnInt64*>(column.get())->get_data();
        for (int i = 0; i < result.datetime_value_size(); ++i) {
            vectorized::VecDateTimeValue v;
            PDateTime pv = result.datetime_value(i);
            v.set_time(pv.year(), pv.month(), pv.day(), pv.hour(), pv.minute(), pv.minute());
            data[i] = binary_cast<vectorized::VecDateTimeValue, vectorized::Int64>(v);
        }
        break;
    }
    case PGenericType::FLOAT: {
        column->reserve(result.float_value_size());
        column->resize(result.float_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnFloat32*>(column.get())->get_data();
        for (int i = 0; i < result.float_value_size(); ++i) {
            data[i] = result.float_value(i);
        }
        break;
    }
    case PGenericType::DOUBLE: {
        column->reserve(result.double_value_size());
        column->resize(result.double_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnFloat64*>(column.get())->get_data();
        for (int i = 0; i < result.double_value_size(); ++i) {
            data[i] = result.double_value(i);
        }
        break;
    }
    case PGenericType::INT128: {
        column->reserve(result.bytes_value_size());
        column->resize(result.bytes_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnInt128*>(column.get())->get_data();
        for (int i = 0; i < result.bytes_value_size(); ++i) {
            data[i] = *(int128_t*)(result.bytes_value(i).c_str());
        }
        break;
    }
    case PGenericType::STRING: {
        column->reserve(result.string_value_size());
        for (int i = 0; i < result.string_value_size(); ++i) {
            column->insert_data(result.string_value(i).c_str(), result.string_value(i).size());
        }
        break;
    }
    case PGenericType::DECIMAL128: {
        column->reserve(result.bytes_value_size());
        column->resize(result.bytes_value_size());
        auto& data = reinterpret_cast<vectorized::ColumnDecimal128*>(column.get())->get_data();
        for (int i = 0; i < result.bytes_value_size(); ++i) {
            data[i] = *(int128_t*)(result.bytes_value(i).c_str());
        }
        break;
    }
    case PGenericType::BITMAP: {
        column->reserve(result.bytes_value_size());
        for (int i = 0; i < result.bytes_value_size(); ++i) {
            column->insert_data(result.bytes_value(i).c_str(), result.bytes_value(i).size());
        }
        break;
    }
    case PGenericType::HLL: {
        column->reserve(result.bytes_value_size());
        for (int i = 0; i < result.bytes_value_size(); ++i) {
            column->insert_data(result.bytes_value(i).c_str(), result.bytes_value(i).size());
        }
        break;
    }
    default: {
        LOG(WARNING) << "unknown PGenericType: " << result.type().DebugString();
        break;
    }
    }
}

void convert_to_block(vectorized::Block& block, const PValues& result, size_t pos) {
    auto data_type = block.get_data_type(pos);
    if (data_type->is_nullable()) {
        auto null_type =
                std::reinterpret_pointer_cast<const vectorized::DataTypeNullable>(data_type);
        auto data_col = null_type->get_nested_type()->create_column();
        convert_to_column<true>(data_col, result);
        auto null_col = vectorized::ColumnUInt8::create(data_col->size(), 0);
        auto& null_map_data = null_col->get_data();
        null_col->reserve(data_col->size());
        null_col->resize(data_col->size());
        if (result.has_null()) {
            for (int i = 0; i < data_col->size(); ++i) {
                null_map_data[i] = result.null_map(i);
            }
        } else {
            for (int i = 0; i < data_col->size(); ++i) {
                null_map_data[i] = false;
            }
        }
        block.replace_by_position(
                pos, vectorized::ColumnNullable::create(std::move(data_col), std::move(null_col)));
    } else {
        auto column = data_type->create_column();
        convert_to_column<false>(column, result);
        block.replace_by_position(pos, std::move(column));
    }
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
        return Status::InternalError(
                fmt::format("call to rpc function {} failed: {}", _signature, cntl.ErrorText())
                        .c_str());
    }
    if (!response.has_status() || response.result_size() == 0) {
        return Status::InternalError(fmt::format(
                "call rpc function {} failed: status or result is not set.", _signature));
    }
    if (response.status().status_code() != 0) {
        return Status::InternalError(fmt::format("call to rpc function {} failed: {}", _signature,
                                                 response.status().DebugString()));
    }
    convert_to_block(block, response.result(0), result);
    return Status::OK();
}
} // namespace doris
