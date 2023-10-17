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

#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <ostream>
#include <string>

#include "common/status.h"
#include "data_type_serde.h"
#include "olap/olap_common.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"

namespace doris {
class JsonbOutStream;

namespace vectorized {
class Arena;

// special data type using, maybe has various serde actions, so use specific date serde
//  DataTypeDateV2 => T:UInt32
//  DataTypeDateTimeV2 => T:UInt64
//  DataTypeTime => T:Float64
//  DataTypeDate => T:Int64
//  DataTypeDateTime => T:Int64
template <typename T>
class DataTypeNumberSerDe : public DataTypeSerDe {
    static_assert(IsNumber<T>);

public:
    using ColumnType = ColumnVector<T>;

    Status serialize_one_cell_to_json(const IColumn& column, int row_num, BufferWritable& bw,
                                      FormatOptions& options, int nesting_level = 1) const override;
    Status serialize_column_to_json(const IColumn& column, int start_idx, int end_idx,
                                    BufferWritable& bw, FormatOptions& options,
                                    int nesting_level = 1) const override;
    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options,
                                          int nesting_level = 1) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               int* num_deserialized, const FormatOptions& options,
                                               int nesting_level = 1) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;

    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena* mem_pool,
                                 int32_t col_id, int row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                               arrow::ArrayBuilder* array_builder, int start,
                               int end) const override;
    void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                int end, const cctz::time_zone& ctz) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const) const override;

    Status write_column_to_orc(const IColumn& column, const NullMap* null_map,
                               orc::ColumnVectorBatch* orc_col_batch, int start, int end,
                               std::vector<StringRef>& buffer_list) const override;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const) const;
};

template <typename T>
Status DataTypeNumberSerDe<T>::read_column_from_pb(IColumn& column, const PValues& arg) const {
    if constexpr (std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16> ||
                  std::is_same_v<T, UInt32>) {
        column.resize(arg.uint32_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.uint32_value_size(); ++i) {
            data[i] = arg.uint32_value(i);
        }
    } else if constexpr (std::is_same_v<T, Int8> || std::is_same_v<T, Int16> ||
                         std::is_same_v<T, Int32>) {
        column.resize(arg.int32_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.int32_value_size(); ++i) {
            data[i] = arg.int32_value(i);
        }
    } else if constexpr (std::is_same_v<T, UInt64>) {
        column.resize(arg.uint64_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.uint64_value_size(); ++i) {
            data[i] = arg.uint64_value(i);
        }
    } else if constexpr (std::is_same_v<T, Int64>) {
        column.resize(arg.int64_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.int64_value_size(); ++i) {
            data[i] = arg.int64_value(i);
        }
    } else if constexpr (std::is_same_v<T, float>) {
        column.resize(arg.float_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.float_value_size(); ++i) {
            data[i] = arg.float_value(i);
        }
    } else if constexpr (std::is_same_v<T, double>) {
        column.resize(arg.double_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.float_value_size(); ++i) {
            data[i] = arg.double_value(i);
        }
    } else if constexpr (std::is_same_v<T, Int128>) {
        column.resize(arg.bytes_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.bytes_value_size(); ++i) {
            data[i] = *(int128_t*)(arg.bytes_value(i).c_str());
        }
    } else {
        return Status::NotSupported("unknown ColumnType for reading from pb");
    }
    return Status::OK();
}

template <typename T>
Status DataTypeNumberSerDe<T>::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                                  int end) const {
    int row_count = end - start;
    auto ptype = result.mutable_type();
    const auto* col = check_and_get_column<ColumnVector<T>>(column);
    if constexpr (std::is_same_v<T, Int128>) {
        ptype->set_id(PGenericType::INT128);
        result.mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
            StringRef single_data = col->get_data_at(row_num);
            result.add_bytes_value(single_data.data, single_data.size);
        }
        return Status::OK();
    }
    auto& data = col->get_data();
    if constexpr (std::is_same_v<T, UInt8>) {
        ptype->set_id(PGenericType::UINT8);
        auto* values = result.mutable_uint32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, UInt16>) {
        ptype->set_id(PGenericType::UINT16);
        auto* values = result.mutable_uint32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, UInt32>) {
        ptype->set_id(PGenericType::UINT32);
        auto* values = result.mutable_uint32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, UInt64>) {
        ptype->set_id(PGenericType::UINT64);
        auto* values = result.mutable_uint64_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, Int8>) {
        ptype->set_id(PGenericType::INT8);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, Int16>) {
        ptype->set_id(PGenericType::INT16);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, Int32>) {
        ptype->set_id(PGenericType::INT32);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, Int64>) {
        ptype->set_id(PGenericType::INT64);
        auto* values = result.mutable_int64_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, float>) {
        ptype->set_id(PGenericType::FLOAT);
        auto* values = result.mutable_float_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, double>) {
        ptype->set_id(PGenericType::DOUBLE);
        auto* values = result.mutable_double_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else {
        return Status::NotSupported("unknown ColumnType for writing to pb");
    }
    return Status::OK();
}

template <typename T>
void DataTypeNumberSerDe<T>::read_one_cell_from_jsonb(IColumn& column,
                                                      const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnType&>(column);
    if constexpr (std::is_same_v<T, Int8> || std::is_same_v<T, UInt8>) {
        col.insert_value(static_cast<const JsonbInt8Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, Int16> || std::is_same_v<T, UInt16>) {
        col.insert_value(static_cast<const JsonbInt16Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, Int32> || std::is_same_v<T, UInt32>) {
        col.insert_value(static_cast<const JsonbInt32Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, Int64> || std::is_same_v<T, UInt64>) {
        col.insert_value(static_cast<const JsonbInt64Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, Int128>) {
        col.insert_value(static_cast<const JsonbInt128Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, float>) {
        col.insert_value(static_cast<const JsonbFloatVal*>(arg)->val());
    } else if constexpr (std::is_same_v<T, double>) {
        col.insert_value(static_cast<const JsonbDoubleVal*>(arg)->val());
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "read_one_cell_from_jsonb with type '{}'", arg->typeName());
    }
}
template <typename T>
void DataTypeNumberSerDe<T>::write_one_cell_to_jsonb(const IColumn& column,
                                                     JsonbWriterT<JsonbOutStream>& result,
                                                     Arena* mem_pool, int32_t col_id,
                                                     int row_num) const {
    result.writeKey(col_id);
    StringRef data_ref = column.get_data_at(row_num);
    // TODO: Casting unsigned integers to signed integers may result in loss of data precision.
    // However, as Doris currently does not support unsigned integers, only the boolean type uses
    // uint8_t for representation, making the cast acceptable. In the future, we should add support for
    // both unsigned integers in Doris types and the JSONB types.
    if constexpr (std::is_same_v<T, Int8> || std::is_same_v<T, UInt8>) {
        int8_t val = *reinterpret_cast<const int8_t*>(data_ref.data);
        result.writeInt8(val);
    } else if constexpr (std::is_same_v<T, Int16> || std::is_same_v<T, UInt16>) {
        int16_t val = *reinterpret_cast<const int16_t*>(data_ref.data);
        result.writeInt16(val);
    } else if constexpr (std::is_same_v<T, Int32> || std::is_same_v<T, UInt32>) {
        int32_t val = *reinterpret_cast<const int32_t*>(data_ref.data);
        result.writeInt32(val);
    } else if constexpr (std::is_same_v<T, Int64> || std::is_same_v<T, UInt64>) {
        int64_t val = *reinterpret_cast<const int64_t*>(data_ref.data);
        result.writeInt64(val);
    } else if constexpr (std::is_same_v<T, Int128>) {
        __int128_t val = *reinterpret_cast<const __int128_t*>(data_ref.data);
        result.writeInt128(val);
    } else if constexpr (std::is_same_v<T, float>) {
        float val = *reinterpret_cast<const float*>(data_ref.data);
        result.writeFloat(val);
    } else if constexpr (std::is_same_v<T, double>) {
        double val = *reinterpret_cast<const double*>(data_ref.data);
        result.writeDouble(val);
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "write_one_cell_to_jsonb with type " + column.get_name());
    }
}

} // namespace vectorized
} // namespace doris
