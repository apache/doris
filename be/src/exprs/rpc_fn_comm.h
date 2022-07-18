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
                           const vectorized::DataTypePtr& data_type, PValues* arg, int start,
                           int end) {
    int row_count = end - start;
    PGenericType* ptype = arg->mutable_type();
    switch (data_type->get_type_id()) {
    case vectorized::TypeIndex::UInt8: {
        ptype->set_id(PGenericType::UINT8);
        auto* values = arg->mutable_bool_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt8>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::UInt16: {
        ptype->set_id(PGenericType::UINT16);
        auto* values = arg->mutable_uint32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt16>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::UInt32: {
        ptype->set_id(PGenericType::UINT32);
        auto* values = arg->mutable_uint32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt32>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::UInt64: {
        ptype->set_id(PGenericType::UINT64);
        auto* values = arg->mutable_uint64_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt64>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::UInt128: {
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
    case vectorized::TypeIndex::Int8: {
        ptype->set_id(PGenericType::INT8);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnInt8>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::Int16: {
        ptype->set_id(PGenericType::INT16);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnInt16>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::Int32: {
        ptype->set_id(PGenericType::INT32);
        auto* values = arg->mutable_int32_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnInt32>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::Int64: {
        ptype->set_id(PGenericType::INT64);
        auto* values = arg->mutable_int64_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnInt64>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::Int128: {
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
    case vectorized::TypeIndex::Float32: {
        ptype->set_id(PGenericType::FLOAT);
        auto* values = arg->mutable_float_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnFloat32>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }

    case vectorized::TypeIndex::Float64: {
        ptype->set_id(PGenericType::DOUBLE);
        auto* values = arg->mutable_double_value();
        values->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnFloat64>(column);
        auto& data = col->get_data();
        values->Add(data.begin() + start, data.begin() + end);
        break;
    }
    case vectorized::TypeIndex::String: {
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
    case vectorized::TypeIndex::Date: {
        ptype->set_id(PGenericType::DATE);
        arg->mutable_datetime_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
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
        for (size_t row_num = start; row_num < end; ++row_num) {
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
    case vectorized::TypeIndex::HLL: {
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

void convert_nullable_col_to_pvalue(const vectorized::ColumnPtr& column,
                                    const vectorized::DataTypePtr& data_type,
                                    const vectorized::ColumnUInt8& null_col, PValues* arg,
                                    int start, int end);

void convert_block_to_proto(vectorized::Block& block, const vectorized::ColumnNumbers& arguments,
                            size_t input_rows_count, PFunctionCallRequest* request);

void convert_to_block(vectorized::Block& block, const PValues& result, size_t pos);

} // namespace doris::vectorized
