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

#include <string>

#include "common/status.h"
#include "data_type_serde.h"
#include "olap/olap_common.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris {
class JsonbOutStream;
#include "common/compile_check_begin.h"
namespace vectorized {
class Arena;

// special data type using, maybe has various serde actions, so use specific date serde
//  DataTypeDateV2 => T:UInt32
//  DataTypeDateTimeV2 => T:UInt64
//  DataTypeTime => T:Float64
//  DataTypeDate => T:Int64
//  DataTypeDateTime => T:Int64
//  IPv4 => T:UInt32
//  IPv6 => T:uint128_t
template <PrimitiveType T>
class DataTypeNumberSerDe : public DataTypeSerDe {
    static_assert(is_int_or_bool(T) || is_ip(T) || is_date_type(T) || is_float_or_double(T) ||
                  T == TYPE_TIME || T == TYPE_TIMEV2);

public:
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;

    DataTypeNumberSerDe(int nesting_level = 1) : DataTypeSerDe(nesting_level) {};

    std::string get_name() const override { return type_to_string(T); }

    Status from_string(StringRef& str, IColumn& column,
                       const FormatOptions& options) const override;

    Status from_string_strict_mode(StringRef& str, IColumn& column,
                                   const FormatOptions& options) const override;

    Status from_string_batch(const ColumnString& str, ColumnNullable& column,
                             const FormatOptions& options) const override;

    Status from_string_strict_mode_batch(
            const ColumnString& str, IColumn& column, const FormatOptions& options,
            const NullMap::value_type* null_map = nullptr) const override;

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;
    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;
    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;

    Status deserialize_column_from_fixed_json(IColumn& column, Slice& slice, uint64_t rows,
                                              uint64_t* num_deserialized,
                                              const FormatOptions& options) const override;

    Status serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                     JsonbWriter& writer) const override;

    Status serialize_column_to_jsonb_vector(const IColumn& from_column,
                                            ColumnString& to_column) const override;

    Status deserialize_column_from_jsonb(IColumn& column, const JsonbValue* jsonb_value,
                                         CastParameters& castParms) const override;

    Status deserialize_column_from_jsonb_vector(ColumnNullable& column_to,
                                                const ColumnString& from_column,
                                                CastParameters& castParms) const override;

    void insert_column_last_value_multiple_times(IColumn& column, uint64_t times) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                              int64_t end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;

    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena& mem_pool,
                                 int32_t col_id, int64_t row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int64_t start, int64_t end, vectorized::Arena& arena) const override;

    void write_one_cell_to_binary(const IColumn& src_column, ColumnString::Chars& chars,
                                  int64_t row_num) const override;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int64_t row_idx, bool col_const,
                                  const FormatOptions& options) const;
};

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto old_column_size = column.size();
    if constexpr (T == TYPE_BOOLEAN) {
        column.resize(old_column_size + arg.uint32_value_size());
        auto& data = assert_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.uint32_value_size(); ++i) {
            data[old_column_size + i] =
                    cast_set<typename PrimitiveTypeTraits<T>::ColumnItemType, uint32_t, false>(
                            arg.uint32_value(i));
        }
    } else if constexpr (T == TYPE_DATEV2 || T == TYPE_IPV4) {
        column.resize(old_column_size + arg.uint32_value_size());
        auto& data = assert_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.uint32_value_size(); ++i) {
            data[old_column_size + i] = arg.uint32_value(i);
        }
    } else if constexpr (T == TYPE_TINYINT || T == TYPE_SMALLINT) {
        column.resize(old_column_size + arg.int32_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.int32_value_size(); ++i) {
            data[old_column_size + i] =
                    cast_set<typename PrimitiveTypeTraits<T>::ColumnItemType, int32_t, false>(
                            arg.int32_value(i));
        }
    } else if constexpr (T == TYPE_INT) {
        column.resize(old_column_size + arg.int32_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.int32_value_size(); ++i) {
            data[old_column_size + i] = arg.int32_value(i);
        }
    } else if constexpr (T == TYPE_DATETIMEV2) {
        column.resize(old_column_size + arg.uint64_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.uint64_value_size(); ++i) {
            data[old_column_size + i] = arg.uint64_value(i);
        }
    } else if constexpr (T == TYPE_BIGINT || T == TYPE_DATE || T == TYPE_DATETIME) {
        column.resize(old_column_size + arg.int64_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.int64_value_size(); ++i) {
            data[old_column_size + i] = arg.int64_value(i);
        }
    } else if constexpr (T == TYPE_FLOAT) {
        column.resize(old_column_size + arg.float_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.float_value_size(); ++i) {
            data[old_column_size + i] = arg.float_value(i);
        }
    } else if constexpr (T == TYPE_DOUBLE || T == TYPE_TIMEV2 || T == TYPE_TIME) {
        column.resize(old_column_size + arg.double_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.double_value_size(); ++i) {
            data[old_column_size + i] = arg.double_value(i);
        }
    } else if constexpr (T == TYPE_LARGEINT) {
        column.resize(old_column_size + arg.bytes_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.bytes_value_size(); ++i) {
            data[old_column_size + i] = *(int128_t*)(arg.bytes_value(i).c_str());
        }
    } else {
        return Status::NotSupported("unknown ColumnType for reading from pb");
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::write_column_to_pb(const IColumn& column, PValues& result,
                                                  int64_t start, int64_t end) const {
    auto row_count = cast_set<int>(end - start);
    auto* ptype = result.mutable_type();
    const auto* col = check_and_get_column<ColumnType>(column);
    if constexpr (T == TYPE_LARGEINT) {
        ptype->set_id(PGenericType::INT128);
        result.mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
            StringRef single_data = col->get_data_at(row_num);
            result.add_bytes_value(single_data.data, single_data.size);
        }
        return Status::OK();
    }
    auto& data = col->get_data();
    if constexpr (T == TYPE_BOOLEAN) {
        ptype->set_id(PGenericType::UINT8);
        auto* values = result.mutable_uint32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (T == TYPE_DATEV2 || T == TYPE_IPV4) {
        ptype->set_id(PGenericType::UINT32);
        auto* values = result.mutable_uint32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (T == TYPE_DATETIMEV2) {
        ptype->set_id(PGenericType::UINT64);
        auto* values = result.mutable_uint64_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (T == TYPE_TINYINT) {
        ptype->set_id(PGenericType::INT8);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (T == TYPE_SMALLINT) {
        ptype->set_id(PGenericType::INT16);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (T == TYPE_INT) {
        ptype->set_id(PGenericType::INT32);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (T == TYPE_BIGINT || T == TYPE_DATE || T == TYPE_DATETIME) {
        ptype->set_id(PGenericType::INT64);
        auto* values = result.mutable_int64_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (T == TYPE_FLOAT) {
        ptype->set_id(PGenericType::FLOAT);
        auto* values = result.mutable_float_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (T == TYPE_DOUBLE || T == TYPE_TIMEV2 || T == TYPE_TIME) {
        ptype->set_id(PGenericType::DOUBLE);
        auto* values = result.mutable_double_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else {
        return Status::NotSupported("unknown ColumnType for writing to pb");
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris
