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
#include "runtime/define_primitive_type.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/core/wide_integer.h"

namespace doris {

namespace vectorized {
template <typename T>
class ColumnDecimal;
class Arena;

template <typename T>
class DataTypeDecimalSerDe : public DataTypeSerDe {
    static_assert(IsDecimalNumber<T>);

public:
    static constexpr PrimitiveType get_primitive_type() {
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal32>>) {
            return TYPE_DECIMAL32;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal64>>) {
            return TYPE_DECIMAL64;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal128V3>>) {
            return TYPE_DECIMAL128I;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal128V2>>) {
            return TYPE_DECIMALV2;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal256>>) {
            return TYPE_DECIMAL256;
        }
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "get_primitive_type __builtin_unreachable");
        __builtin_unreachable();
    }

    DataTypeDecimalSerDe(int scale_, int precision_, int nesting_level = 1)
            : DataTypeSerDe(nesting_level),
              scale(scale_),
              precision(precision_),
              scale_multiplier(decimal_scale_multiplier<typename T::NativeType>(scale)) {}

    Status serialize_one_cell_to_json(const IColumn& column, int row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;

    Status serialize_column_to_json(const IColumn& column, int start_idx, int end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               int* num_deserialized,
                                               const FormatOptions& options) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;

    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena* mem_pool,
                                 int32_t col_id, int row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                               arrow::ArrayBuilder* array_builder, int start, int end,
                               const cctz::time_zone& ctz) const override;
    void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                int end, const cctz::time_zone& ctz) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int start, int end,
                               std::vector<StringRef>& buffer_list) const override;

    Status deserialize_column_from_fixed_json(IColumn& column, Slice& slice, int rows,
                                              int* num_deserialized,
                                              const FormatOptions& options) const override;

    void insert_column_last_value_multiple_times(IColumn& column, int times) const override;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const, const FormatOptions& options) const;

    int scale;
    int precision;
    const typename T::NativeType scale_multiplier;
    mutable char buf[T::max_string_length()];
};

template <typename T>
Status DataTypeDecimalSerDe<T>::write_column_to_pb(const IColumn& column, PValues& result,
                                                   int start, int end) const {
    int row_count = end - start;
    const auto* col = check_and_get_column<ColumnDecimal<T>>(column);
    auto* ptype = result.mutable_type();
    if constexpr (std::is_same_v<T, Decimal<Int128>>) {
        ptype->set_id(PGenericType::DECIMAL128);
    } else if constexpr (std::is_same_v<T, Decimal128V3>) {
        ptype->set_id(PGenericType::DECIMAL128I);
    } else if constexpr (std::is_same_v<T, Decimal256>) {
        ptype->set_id(PGenericType::DECIMAL256);
    } else if constexpr (std::is_same_v<T, Decimal<Int32>>) {
        ptype->set_id(PGenericType::INT32);
    } else if constexpr (std::is_same_v<T, Decimal<Int64>>) {
        ptype->set_id(PGenericType::INT64);
    } else {
        return Status::NotSupported("unknown ColumnType for writing to pb");
    }
    result.mutable_bytes_value()->Reserve(row_count);
    for (size_t row_num = start; row_num < end; ++row_num) {
        StringRef single_data = col->get_data_at(row_num);
        result.add_bytes_value(single_data.data, single_data.size);
    }
    return Status::OK();
}

// TODO: decimal256
template <typename T>
Status DataTypeDecimalSerDe<T>::read_column_from_pb(IColumn& column, const PValues& arg) const {
    if constexpr (std::is_same_v<T, Decimal<Int128>> || std::is_same_v<T, Decimal128V3> ||
                  std::is_same_v<T, Decimal256> || std::is_same_v<T, Decimal<Int32>>) {
        auto old_column_size = column.size();
        column.resize(old_column_size + arg.bytes_value_size());
        auto& data = reinterpret_cast<ColumnDecimal<T>&>(column).get_data();
        for (int i = 0; i < arg.bytes_value_size(); ++i) {
            data[old_column_size + i] = *(T*)(arg.bytes_value(i).c_str());
        }
        return Status::OK();
    }

    return Status::NotSupported("unknown ColumnType for reading from pb");
}

template <typename T>
void DataTypeDecimalSerDe<T>::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                      Arena* mem_pool, int32_t col_id,
                                                      int row_num) const {
    StringRef data_ref = column.get_data_at(row_num);
    result.writeKey(col_id);
    if constexpr (std::is_same_v<T, Decimal<Int128>>) {
        Decimal128V2::NativeType val =
                *reinterpret_cast<const Decimal128V2::NativeType*>(data_ref.data);
        result.writeInt128(val);
    } else if constexpr (std::is_same_v<T, Decimal128V3>) {
        Decimal128V3::NativeType val =
                *reinterpret_cast<const Decimal128V3::NativeType*>(data_ref.data);
        result.writeInt128(val);
    } else if constexpr (std::is_same_v<T, Decimal<Int32>>) {
        Decimal32::NativeType val = *reinterpret_cast<const Decimal32::NativeType*>(data_ref.data);
        result.writeInt32(val);
    } else if constexpr (std::is_same_v<T, Decimal<Int64>>) {
        Decimal64::NativeType val = *reinterpret_cast<const Decimal64::NativeType*>(data_ref.data);
        result.writeInt64(val);
    } else if constexpr (std::is_same_v<T, Decimal256>) {
        // use binary type, since jsonb does not support int256
        result.writeStartBinary();
        result.writeBinary(data_ref.data, data_ref.size);
        result.writeEndBinary();
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "write_one_cell_to_jsonb with type " + column.get_name());
    }
}

template <typename T>
void DataTypeDecimalSerDe<T>::read_one_cell_from_jsonb(IColumn& column,
                                                       const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnDecimal<T>&>(column);
    if constexpr (std::is_same_v<T, Decimal<Int128>>) {
        col.insert_value(static_cast<const JsonbInt128Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, Decimal128V3>) {
        col.insert_value(static_cast<const JsonbInt128Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, Decimal<Int32>>) {
        col.insert_value(static_cast<const JsonbInt32Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, Decimal<Int64>>) {
        col.insert_value(static_cast<const JsonbInt64Val*>(arg)->val());
    } else if constexpr (std::is_same_v<T, Decimal256>) {
        // use binary type, since jsonb does not support int256
        const wide::Int256 val = *reinterpret_cast<const wide::Int256*>(
                static_cast<const JsonbBlobVal*>(arg)->getBlob());
        col.insert_value(Decimal256(val));
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "read_one_cell_from_jsonb with type " + column.get_name());
    }
}
} // namespace vectorized
} // namespace doris
