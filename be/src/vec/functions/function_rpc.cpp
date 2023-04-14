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

#include "vec/functions/function_rpc.h"

#include <fmt/format.h>

#include <memory>

#include "gen_cpp/Exprs_types.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "runtime/exec_env.h"

namespace doris::vectorized {

RPCFnImpl::RPCFnImpl(const TFunction& fn) : _fn(fn) {
    _function_name = _fn.scalar_fn.symbol;
    _server_addr = _fn.hdfs_location;
    _client = ExecEnv::GetInstance()->brpc_function_client_cache()->get_client(_server_addr);
    _signature = fmt::format("{}: [{}/{}]", _fn.name.function_name, _fn.hdfs_location,
                             _fn.scalar_fn.symbol);
}

void RPCFnImpl::convert_nullable_col_to_pvalue(const ColumnPtr& column,
                                               const DataTypePtr& data_type,
                                               const ColumnUInt8& null_col, PValues* arg, int start,
                                               int end) {
    int row_count = end - start;
    if (column->has_null(row_count)) {
        auto* null_map = arg->mutable_null_map();
        null_map->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnUInt8>(null_col);
        auto& data = col->get_data();
        null_map->Add(data.begin() + start, data.begin() + end);
        RPCFnImpl::convert_col_to_pvalue<true>(column, data_type, arg, start, end);
    } else {
        RPCFnImpl::convert_col_to_pvalue<false>(column, data_type, arg, start, end);
    }
}

template <bool nullable>
void RPCFnImpl::convert_col_to_pvalue(const ColumnPtr& column, const DataTypePtr& data_type,
                                      PValues* arg, int start, int end) {
    int row_count = end - start;
    PGenericType* ptype = arg->mutable_type();
    switch (data_type->get_type_id()) {
    case TypeIndex::UInt8: {
        ptype->set_id(PGenericType::UINT8);
        auto* values = arg->mutable_bool_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnUInt8>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::UInt16: {
        ptype->set_id(PGenericType::UINT16);
        auto* values = arg->mutable_uint32_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnUInt16>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::UInt32: {
        ptype->set_id(PGenericType::UINT32);
        auto* values = arg->mutable_uint32_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnUInt32>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::UInt64: {
        ptype->set_id(PGenericType::UINT64);
        auto* values = arg->mutable_uint64_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnUInt64>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::UInt128: {
        ptype->set_id(PGenericType::UINT128);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
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
    case TypeIndex::Int8: {
        ptype->set_id(PGenericType::INT8);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnInt8>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::Int16: {
        ptype->set_id(PGenericType::INT16);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnInt16>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::Int32: {
        ptype->set_id(PGenericType::INT32);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnInt32>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::Int64: {
        ptype->set_id(PGenericType::INT64);
        auto* values = arg->mutable_int64_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnInt64>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::Int128: {
        ptype->set_id(PGenericType::INT128);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
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
    case TypeIndex::Float32: {
        ptype->set_id(PGenericType::FLOAT);
        auto* values = arg->mutable_float_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnFloat32>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }

    case TypeIndex::Float64: {
        ptype->set_id(PGenericType::DOUBLE);
        auto* values = arg->mutable_double_value();
        values->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnFloat64>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case TypeIndex::String: {
        ptype->set_id(PGenericType::STRING);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
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
    case TypeIndex::Date: {
        ptype->set_id(PGenericType::DATE);
        arg->mutable_datetime_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
            PDateTime* date_time = arg->add_datetime_value();
            if constexpr (nullable) {
                if (!column->is_null_at(row_num)) {
                    VecDateTimeValue v =
                            VecDateTimeValue::create_from_olap_date(column->get_int(row_num));
                    date_time->set_day(v.day());
                    date_time->set_month(v.month());
                    date_time->set_year(v.year());
                }
            } else {
                VecDateTimeValue v =
                        VecDateTimeValue::create_from_olap_date(column->get_int(row_num));
                date_time->set_day(v.day());
                date_time->set_month(v.month());
                date_time->set_year(v.year());
            }
        }
        break;
    }
    case TypeIndex::DateTime: {
        ptype->set_id(PGenericType::DATETIME);
        arg->mutable_datetime_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
            PDateTime* date_time = arg->add_datetime_value();
            if constexpr (nullable) {
                if (!column->is_null_at(row_num)) {
                    VecDateTimeValue v =
                            VecDateTimeValue::create_from_olap_datetime(column->get_int(row_num));
                    date_time->set_day(v.day());
                    date_time->set_month(v.month());
                    date_time->set_year(v.year());
                    date_time->set_hour(v.hour());
                    date_time->set_minute(v.minute());
                    date_time->set_second(v.second());
                }
            } else {
                VecDateTimeValue v =
                        VecDateTimeValue::create_from_olap_datetime(column->get_int(row_num));
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
    case TypeIndex::BitMap: {
        ptype->set_id(PGenericType::BITMAP);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
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
    case TypeIndex::HLL: {
        ptype->set_id(PGenericType::HLL);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
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
    case TypeIndex::QuantileState: {
        ptype->set_id(PGenericType::QUANTILE_STATE);
        arg->mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
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

template <bool nullable>
void RPCFnImpl::_convert_to_column(MutableColumnPtr& column, const PValues& result) {
    switch (result.type().id()) {
    case PGenericType::UINT8: {
        column->reserve(result.uint32_value_size());
        column->resize(result.uint32_value_size());
        auto& data = reinterpret_cast<ColumnUInt8*>(column.get())->get_data();
        for (int i = 0; i < result.uint32_value_size(); ++i) {
            data[i] = result.uint32_value(i);
        }
        break;
    }
    case PGenericType::UINT16: {
        column->reserve(result.uint32_value_size());
        column->resize(result.uint32_value_size());
        auto& data = reinterpret_cast<ColumnUInt16*>(column.get())->get_data();
        for (int i = 0; i < result.uint32_value_size(); ++i) {
            data[i] = result.uint32_value(i);
        }
        break;
    }
    case PGenericType::UINT32: {
        column->reserve(result.uint32_value_size());
        column->resize(result.uint32_value_size());
        auto& data = reinterpret_cast<ColumnUInt32*>(column.get())->get_data();
        for (int i = 0; i < result.uint32_value_size(); ++i) {
            data[i] = result.uint32_value(i);
        }
        break;
    }
    case PGenericType::UINT64: {
        column->reserve(result.uint64_value_size());
        column->resize(result.uint64_value_size());
        auto& data = reinterpret_cast<ColumnUInt64*>(column.get())->get_data();
        for (int i = 0; i < result.uint64_value_size(); ++i) {
            data[i] = result.uint64_value(i);
        }
        break;
    }
    case PGenericType::INT8: {
        column->reserve(result.int32_value_size());
        column->resize(result.int32_value_size());
        auto& data = reinterpret_cast<ColumnInt16*>(column.get())->get_data();
        for (int i = 0; i < result.int32_value_size(); ++i) {
            data[i] = result.int32_value(i);
        }
        break;
    }
    case PGenericType::INT16: {
        column->reserve(result.int32_value_size());
        column->resize(result.int32_value_size());
        auto& data = reinterpret_cast<ColumnInt16*>(column.get())->get_data();
        for (int i = 0; i < result.int32_value_size(); ++i) {
            data[i] = result.int32_value(i);
        }
        break;
    }
    case PGenericType::INT32: {
        column->reserve(result.int32_value_size());
        column->resize(result.int32_value_size());
        auto& data = reinterpret_cast<ColumnInt32*>(column.get())->get_data();
        for (int i = 0; i < result.int32_value_size(); ++i) {
            data[i] = result.int32_value(i);
        }
        break;
    }
    case PGenericType::INT64: {
        column->reserve(result.int64_value_size());
        column->resize(result.int64_value_size());
        auto& data = reinterpret_cast<ColumnInt64*>(column.get())->get_data();
        for (int i = 0; i < result.int64_value_size(); ++i) {
            data[i] = result.int64_value(i);
        }
        break;
    }
    case PGenericType::DATE:
    case PGenericType::DATETIME: {
        column->reserve(result.datetime_value_size());
        column->resize(result.datetime_value_size());
        auto& data = reinterpret_cast<ColumnInt64*>(column.get())->get_data();
        for (int i = 0; i < result.datetime_value_size(); ++i) {
            VecDateTimeValue v;
            PDateTime pv = result.datetime_value(i);
            v.set_time(pv.year(), pv.month(), pv.day(), pv.hour(), pv.minute(), pv.minute());
            data[i] = binary_cast<VecDateTimeValue, Int64>(v);
        }
        break;
    }
    case PGenericType::FLOAT: {
        column->reserve(result.float_value_size());
        column->resize(result.float_value_size());
        auto& data = reinterpret_cast<ColumnFloat32*>(column.get())->get_data();
        for (int i = 0; i < result.float_value_size(); ++i) {
            data[i] = result.float_value(i);
        }
        break;
    }
    case PGenericType::DOUBLE: {
        column->reserve(result.double_value_size());
        column->resize(result.double_value_size());
        auto& data = reinterpret_cast<ColumnFloat64*>(column.get())->get_data();
        for (int i = 0; i < result.double_value_size(); ++i) {
            data[i] = result.double_value(i);
        }
        break;
    }
    case PGenericType::INT128: {
        column->reserve(result.bytes_value_size());
        column->resize(result.bytes_value_size());
        auto& data = reinterpret_cast<ColumnInt128*>(column.get())->get_data();
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
        auto& data = reinterpret_cast<ColumnDecimal128*>(column.get())->get_data();
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
    case PGenericType::QUANTILE_STATE: {
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

Status RPCFnImpl::vec_call(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                           size_t result, size_t input_rows_count) {
    PFunctionCallRequest request;
    PFunctionCallResponse response;
    request.set_function_name(_function_name);
    _convert_block_to_proto(block, arguments, input_rows_count, &request);
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
    _convert_to_block(block, response.result(0), result);
    return Status::OK();
}

void RPCFnImpl::_convert_block_to_proto(Block& block, const ColumnNumbers& arguments,
                                        size_t input_rows_count, PFunctionCallRequest* request) {
    size_t row_count = std::min(block.rows(), input_rows_count);
    for (size_t col_idx : arguments) {
        PValues* arg = request->add_args();
        ColumnWithTypeAndName& column = block.get_by_position(col_idx);
        arg->set_has_null(column.column->has_null(row_count));
        auto col = column.column->convert_to_full_column_if_const();
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*col)) {
            auto data_col = nullable->get_nested_column_ptr();
            auto& null_col = nullable->get_null_map_column();
            auto data_type = std::reinterpret_pointer_cast<const DataTypeNullable>(column.type);
            convert_nullable_col_to_pvalue(data_col->convert_to_full_column_if_const(),
                                           data_type->get_nested_type(), null_col, arg, 0,
                                           row_count);
        } else {
            convert_col_to_pvalue<false>(col, column.type, arg, 0, row_count);
        }
    }
}

void RPCFnImpl::_convert_to_block(Block& block, const PValues& result, size_t pos) {
    auto data_type = block.get_data_type(pos);
    if (data_type->is_nullable()) {
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(data_type);
        auto data_col = null_type->get_nested_type()->create_column();
        _convert_to_column<true>(data_col, result);
        auto null_col = ColumnUInt8::create(data_col->size(), 0);
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
        block.replace_by_position(pos,
                                  ColumnNullable::create(std::move(data_col), std::move(null_col)));
    } else {
        auto column = data_type->create_column();
        _convert_to_column<false>(column, result);
        block.replace_by_position(pos, std::move(column));
    }
}

FunctionRPC::FunctionRPC(const TFunction& fn, const DataTypes& argument_types,
                         const DataTypePtr& return_type)
        : _argument_types(argument_types), _return_type(return_type), _tfn(fn) {}

Status FunctionRPC::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    _fn = std::make_unique<RPCFnImpl>(_tfn);

    if (!_fn->available()) {
        return Status::InternalError("rpc env init error");
    }
    return Status::OK();
}

Status FunctionRPC::execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                            size_t result, size_t input_rows_count, bool dry_run) {
    return _fn->vec_call(context, block, arguments, result, input_rows_count);
}
} // namespace doris::vectorized
