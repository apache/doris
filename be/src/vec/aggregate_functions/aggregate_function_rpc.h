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

#include <cstdint>
#include <memory>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"
#include "util/brpc_client_cache.h"
#include "util/jni-util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/exception.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"
namespace doris::vectorized {

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

template <typename T>
typename std::underlying_type<T>::type PrintEnum(T const value) {
    return static_cast<typename std::underlying_type<T>::type>(value);
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
struct AggregateRpcUdafData {
private:
    std::string _update_fn;
    std::string _merge_fn;
    std::string _server_addr;
    std::string _finalize_fn;
    bool saved_last_result;
    std::shared_ptr<PFunctionService_Stub> _client;
    PFunctionCallResponse res;

public:
    AggregateRpcUdafData() = default;
    AggregateRpcUdafData(int64_t num_args) { set_last_result(false); }

    bool has_last_result() { return saved_last_result == true; }

    void set_last_result(bool flag) { saved_last_result = flag; }

    ~AggregateRpcUdafData() {}

    Status merge(const AggregateRpcUdafData& rhs) {
        if (has_last_result()) {
            PFunctionCallRequest request;
            PFunctionCallResponse response;
            brpc::Controller cntl;
            PFunctionCallResponse current_res = rhs.get_result();
            request.set_function_name(_merge_fn);
            //last result
            PValues* arg = request.add_args();
            arg->CopyFrom(res.result(0));
            arg = request.add_args();

            //current result
            arg->CopyFrom(current_res.result(0));

            //send to rpc server  that impl the merge op, the will save the result
            RETURN_IF_ERROR(send_rpc_request(cntl, request, response));
            res = response;
        } else {
            res = rhs.get_result();
            set_last_result(true);
        }
        return Status::OK();
    }

    Status init(const TFunction& fn) {
        _update_fn = fn.aggregate_fn.update_fn_symbol;
        _merge_fn = fn.aggregate_fn.merge_fn_symbol;
        _server_addr = fn.hdfs_location;
        _finalize_fn = fn.aggregate_fn.finalize_fn_symbol;
        _client = ExecEnv::GetInstance()->brpc_function_client_cache()->get_client(_server_addr);
        if (_client == nullptr) {
            std::string err_msg = "init rpc error, addr:" + _server_addr;
            LOG(ERROR) << err_msg;
            return Status::InternalError(err_msg);
        }
        return Status::OK();
    }

    Status send_rpc_request(brpc::Controller& cntl, PFunctionCallRequest& request,
                            PFunctionCallResponse& response) const {
        _client->fn_call(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            return Status::InternalError(fmt::format("call to rpc function {} failed: {}",
                                                     request.function_name(), cntl.ErrorText())
                                                 .c_str());
        }
        if (!response.has_status() || response.result_size() == 0) {
            return Status::InternalError(
                    fmt::format("call rpc function {} failed: status or result is not set.",
                                request.function_name()));
        }
        if (response.status().status_code() != 0) {
            return Status::InternalError(fmt::format("call to rpc function {} failed: {}",
                                                     request.function_name(),
                                                     response.status().DebugString()));
        }
        return Status::OK();
    }

    Status add(const IColumn** columns, int row_num, const DataTypes& argument_types) {
        PFunctionCallRequest request;
        PFunctionCallResponse response;
        brpc::Controller cntl;
        request.set_function_name(_update_fn);
        for (int i = 0; i < argument_types.size(); i++) {
            PValues* arg = request.add_args();
            if (auto* nullable = vectorized::check_and_get_column<const vectorized::ColumnNullable>(
                        *columns[i])) {
                auto data_col = nullable->get_nested_column_ptr();
                auto& null_col = nullable->get_null_map_column();
                auto data_type = std::reinterpret_pointer_cast<const vectorized::DataTypeNullable>(
                        argument_types[i]);
                convert_nullable_col_to_pvalue(data_col->convert_to_full_column_if_const(),
                                               data_type->get_nested_type(), null_col, arg,
                                               row_num);
            } else {
                convert_col_to_pvalue<false>(columns[i]->convert_to_full_column_if_const(),
                                             argument_types[i], arg, row_num);
            }
        }
        if (has_last_result()) {
            request.mutable_last_result()->CopyFrom(res.result());
        }
        RETURN_IF_ERROR(send_rpc_request(cntl, request, response));
        res = response;
        set_last_result(true);
        return Status::OK();
    }

    void serialize(BufferWritable& buf) {
        std::string serialize_data = res.SerializeAsString();
        write_binary(serialize_data, buf);
    }

    void deserialize(BufferReadable& buf) {
        std::string serialize_data;
        read_binary(serialize_data, buf);
        res.ParseFromString(serialize_data);
        set_last_result(true);
    }

    Status get(IColumn& to, const DataTypePtr& return_type) const {
        PFunctionCallRequest request;
        PFunctionCallResponse response;
        brpc::Controller cntl;
        request.set_function_name(_finalize_fn);
        request.mutable_last_result()->CopyFrom(res.result());
        send_rpc_request(cntl, request, response);

        DataTypePtr result_type = return_type;
        if (return_type->is_nullable()) {
            result_type =
                    reinterpret_cast<const DataTypeNullable*>(return_type.get())->get_nested_type();
        }
        WhichDataType which(result_type);
        if (which.is_int32()) {
            int32_t a = response.result(0).int32_value(0);
            to.insert_data((char*)&a, 0);
        }
        return Status::OK();
    }

    PFunctionCallResponse get_result() const { return res; }
};

class AggregateRpcUdaf final
        : public IAggregateFunctionDataHelper<AggregateRpcUdafData, AggregateRpcUdaf> {
public:
    AggregateRpcUdaf(const TFunction& fn, const DataTypes& argument_types, const Array& parameters,
                     const DataTypePtr& return_type)
            : IAggregateFunctionDataHelper(argument_types, parameters),
              _fn(fn),
              _return_type(return_type) {}
    ~AggregateRpcUdaf() = default;

    static AggregateFunctionPtr create(const TFunction& fn, const DataTypes& argument_types,
                                       const Array& parameters, const DataTypePtr& return_type) {
        return std::make_shared<AggregateRpcUdaf>(fn, argument_types, parameters, return_type);
    }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data(argument_types.size());
        Status status = Status::OK();
        RETURN_IF_STATUS_ERROR(status, data(place).init(_fn));
    }

    String get_name() const override { return _fn.name.function_name; }

    DataTypePtr get_return_type() const override { return _return_type; }

    // TODO: here calling add operator maybe only hava done one row, this performance may be poorly
    // so it's possible to maintain a hashtable in FE, the key is place address, value is the object
    // then we can calling add_bacth function and calculate the whole batch at once,
    // and avoid calling jni multiple times.
    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        this->data(place).add(columns, row_num, argument_types);
    }

    // TODO: Here we calling method by jni, And if we get a thrown from FE,
    // But can't let user known the error, only return directly and output error to log file.
    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        this->data(place).add(columns, batch_size, argument_types);
    }

    void reset(AggregateDataPtr place) const override {}

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(const_cast<AggregateDataPtr&>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(const_cast<AggregateDataPtr>(place)).get(to, _return_type);
    }

private:
    TFunction _fn;
    DataTypePtr _return_type;
};

} // namespace doris::vectorized