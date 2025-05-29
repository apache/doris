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

#include "data_type_number_serde.h"

#include <arrow/builder.h>

#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "gutil/strings/numbers.h"
#include "util/mysql_global.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
// Type map的基本结构
template <typename Key, typename Value, typename... Rest>
struct TypeMap {
    using KeyType = Key;
    using ValueType = Value;
    using Next = TypeMap<Rest...>;
};

// Type map的末端
template <>
struct TypeMap<void, void> {};

// TypeMapLookup 前向声明
template <typename Key, typename Map>
struct TypeMapLookup;

// Type map查找：找到匹配的键时的情况
template <typename Key, typename Value, typename... Rest>
struct TypeMapLookup<Key, TypeMap<Key, Value, Rest...>> {
    using ValueType = Value;
};

// Type map查找：递归查找
template <typename Key, typename K, typename V, typename... Rest>
struct TypeMapLookup<Key, TypeMap<K, V, Rest...>> {
    using ValueType = typename TypeMapLookup<Key, TypeMap<Rest...>>::ValueType;
};

using DORIS_NUMERIC_ARROW_BUILDER =
        TypeMap<UInt8, arrow::BooleanBuilder, Int8, arrow::Int8Builder, UInt16,
                arrow::UInt16Builder, Int16, arrow::Int16Builder, UInt32, arrow::UInt32Builder,
                Int32, arrow::Int32Builder, UInt64, arrow::UInt64Builder, Int64,
                arrow::Int64Builder, UInt128, arrow::FixedSizeBinaryBuilder, Int128,
                arrow::FixedSizeBinaryBuilder, IPv6, arrow::FixedSizeBinaryBuilder, Float32,
                arrow::FloatBuilder, Float64, arrow::DoubleBuilder, void,
                void // Add this line to represent the end of the TypeMap
                >;

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                     arrow::ArrayBuilder* array_builder,
                                                     int64_t start, int64_t end,
                                                     const cctz::time_zone& ctz) const {
    auto& col_data = assert_cast<const ColumnType&>(column).get_data();
    using ARROW_BUILDER_TYPE =
            typename TypeMapLookup<typename PrimitiveTypeTraits<T>::ColumnItemType,
                                   DORIS_NUMERIC_ARROW_BUILDER>::ValueType;
    auto arrow_null_map = revert_null_map(null_map, start, end);
    auto* arrow_null_map_data = arrow_null_map.empty() ? nullptr : arrow_null_map.data();
    if constexpr (T == TYPE_BOOLEAN) {
        auto* null_builder = dynamic_cast<arrow::NullBuilder*>(array_builder);
        if (null_builder) {
            for (size_t i = start; i < end; ++i) {
                RETURN_IF_ERROR(checkArrowStatus(null_builder->AppendNull(), column.get_name(),
                                                 null_builder->type()->name()));
            }
        } else {
            auto& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
            RETURN_IF_ERROR(checkArrowStatus(
                    builder.AppendValues(reinterpret_cast<const uint8_t*>(col_data.data() + start),
                                         end - start,
                                         reinterpret_cast<const uint8_t*>(arrow_null_map_data)),
                    column.get_name(), array_builder->type()->name()));
        }

    } else if constexpr (T == TYPE_LARGEINT) {
        auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        for (size_t i = start; i < end; ++i) {
            auto& data_value = col_data[i];
            std::string value_str = fmt::format("{}", data_value);
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                                                 array_builder->type()->name()));
            } else {
                RETURN_IF_ERROR(checkArrowStatus(
                        string_builder.Append(value_str.data(),
                                              cast_set<int, size_t, false>(value_str.length())),
                        column.get_name(), array_builder->type()->name()));
            }
        }
    } else if constexpr (T == TYPE_IPV6) {
    } else {
        auto& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        RETURN_IF_ERROR(checkArrowStatus(
                builder.AppendValues(col_data.data() + start, end - start,
                                     reinterpret_cast<const uint8_t*>(arrow_null_map_data)),
                column.get_name(), array_builder->type()->name()));
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                              const FormatOptions& options) const {
    auto& column_data = reinterpret_cast<ColumnType&>(column);
    ReadBuffer rb(slice.data, slice.size);
    if constexpr (T == TYPE_IPV6) {
        // TODO: support for Uint128
        return Status::InvalidArgument("uint128 is not support");
    } else if constexpr (is_float_or_double(T) || T == TYPE_TIMEV2 || T == TYPE_TIME) {
        typename PrimitiveTypeTraits<T>::ColumnItemType val = 0;
        if (!read_float_text_fast_impl(val, rb)) {
            return Status::InvalidArgument("parse number fail, string: '{}'", slice.to_string());
        }
        column_data.insert_value(val);
    } else if constexpr (T == TYPE_BOOLEAN) {
        // Note: here we should handle the bool type
        typename PrimitiveTypeTraits<T>::ColumnItemType val = 0;
        if (!try_read_bool_text(val, rb)) {
            return Status::InvalidArgument("parse boolean fail, string: '{}'", slice.to_string());
        }
        column_data.insert_value(val);
    } else if constexpr (is_int_or_bool(T)) {
        typename PrimitiveTypeTraits<T>::ColumnItemType val = 0;
        if (!read_int_text_impl(val, rb)) {
            return Status::InvalidArgument("parse number fail, string: '{}'", slice.to_string());
        }
        column_data.insert_value(val);
    } else {
        DCHECK(false);
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                        int64_t end_idx, BufferWritable& bw,
                                                        FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                          BufferWritable& bw,
                                                          FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    auto data = assert_cast<const ColumnType&>(*ptr).get_element(row_num);
    if constexpr (T == TYPE_IPV6) {
        std::string hex = int128_to_string(data);
        bw.write(hex.data(), hex.size());
    } else if constexpr (T == TYPE_FLOAT) {
        // fmt::format_to maybe get inaccurate results at float type, so we use gutil implement.
        char buf[MAX_FLOAT_STR_LENGTH + 2];
        int len = FloatToBuffer(data, MAX_FLOAT_STR_LENGTH + 2, buf);
        bw.write(buf, len);
    } else if constexpr (is_int_or_bool(T) ||
                         std::numeric_limits<
                                 typename PrimitiveTypeTraits<T>::ColumnItemType>::is_iec559) {
        bw.write_number(data);
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::read_column_from_arrow(IColumn& column,
                                                      const arrow::Array* arrow_array,
                                                      int64_t start, int64_t end,
                                                      const cctz::time_zone& ctz) const {
    auto row_count = end - start;
    auto& col_data = static_cast<ColumnType&>(column).get_data();

    // now uint8 for bool
    if constexpr (T == TYPE_BOOLEAN) {
        const auto* concrete_array = dynamic_cast<const arrow::BooleanArray*>(arrow_array);
        for (size_t bool_i = 0; bool_i != static_cast<size_t>(concrete_array->length()); ++bool_i) {
            col_data.emplace_back(concrete_array->Value(bool_i));
        }
        return Status::OK();
    }

    // only for largeint(int128) type
    if (arrow_array->type_id() == arrow::Type::STRING) {
        const auto* concrete_array = dynamic_cast<const arrow::StringArray*>(arrow_array);
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

        for (size_t offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = buffer->data() + concrete_array->value_offset(offset_i);
                const auto raw_data_len = concrete_array->value_length(offset_i);

                if (raw_data_len == 0) {
                    col_data.emplace_back(Int128()); // Int128() is NULL
                } else {
                    Int128 val = 0;
                    ReadBuffer rb(raw_data, raw_data_len);
                    if (!read_int_text_impl(val, rb)) {
                        return Status::Error(ErrorCode::INVALID_ARGUMENT,
                                             "parse number fail, string: '{}'",
                                             std::string(rb.position(), rb.count()).c_str());
                    }
                    col_data.emplace_back(val);
                }
            } else {
                col_data.emplace_back(Int128()); // Int128() is NULL
            }
        }
        return Status::OK();
    }

    /// buffers[0] is a null bitmap and buffers[1] are actual values
    std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
    const auto* raw_data = reinterpret_cast<const typename PrimitiveTypeTraits<T>::ColumnItemType*>(
                                   buffer->data()) +
                           start;
    col_data.insert(raw_data, raw_data + row_count);
    return Status::OK();
}
template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::deserialize_column_from_fixed_json(
        IColumn& column, Slice& slice, uint64_t rows, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    if (rows < 1) [[unlikely]] {
        return Status::OK();
    }
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }

    DataTypeNumberSerDe::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

template <PrimitiveType T>
void DataTypeNumberSerDe<T>::insert_column_last_value_multiple_times(IColumn& column,
                                                                     uint64_t times) const {
    if (times < 1) [[unlikely]] {
        return;
    }
    auto& col = static_cast<ColumnType&>(column);
    auto sz = col.size();
    typename PrimitiveTypeTraits<T>::ColumnItemType val = col.get_element(sz - 1);
    col.insert_many_vals(val, times);
}

template <PrimitiveType T>
template <bool is_binary_format>
Status DataTypeNumberSerDe<T>::_write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<is_binary_format>& result,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    int buf_ret = 0;
    auto& data = assert_cast<const ColumnType&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    if constexpr (T == TYPE_TINYINT) {
        buf_ret = result.push_tinyint(data[col_index]);
    } else if constexpr (T == TYPE_BOOLEAN) {
        if (_nesting_level > 1 && !options.is_bool_value_num) {
            std::string bool_value = data[col_index] ? "true" : "false";
            result.push_string(bool_value.c_str(), bool_value.size());
        } else {
            buf_ret = result.push_tinyint(data[col_index]);
        }
    } else if constexpr (T == TYPE_SMALLINT) {
        buf_ret = result.push_smallint(data[col_index]);
    } else if constexpr (T == TYPE_INT || T == TYPE_DATEV2 || T == TYPE_IPV4) {
        buf_ret = result.push_int(data[col_index]);
    } else if constexpr (T == TYPE_BIGINT || T == TYPE_DATE || T == TYPE_DATETIME ||
                         T == TYPE_DATETIMEV2) {
        buf_ret = result.push_bigint(data[col_index]);
    } else if constexpr (T == TYPE_LARGEINT) {
        buf_ret = result.push_largeint(data[col_index]);
    } else if constexpr (T == TYPE_FLOAT) {
        if (std::isnan(data[col_index])) {
            // Handle NaN for float, we should push null value
            buf_ret = result.push_null();
        } else {
            buf_ret = result.push_float(data[col_index]);
        }
    } else if constexpr (T == TYPE_DOUBLE || T == TYPE_TIME || T == TYPE_TIMEV2) {
        if (std::isnan(data[col_index])) {
            // Handle NaN for double, we should push null value
            buf_ret = result.push_null();
        } else {
            buf_ret = result.push_double(data[col_index]);
        }
    }
    if (UNLIKELY(buf_ret != 0)) {
        return Status::InternalError("pack mysql buffer failed.");
    } else {
        return Status::OK();
    }
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<true>& row_buffer,
                                                     int64_t row_idx, bool col_const,
                                                     const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<false>& row_buffer,
                                                     int64_t row_idx, bool col_const,
                                                     const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

#define WRITE_INTEGRAL_COLUMN_TO_ORC(ORC_TYPE)                    \
    ORC_TYPE* cur_batch = dynamic_cast<ORC_TYPE*>(orc_col_batch); \
    for (size_t row_id = start; row_id < end; row_id++) {         \
        if (cur_batch->notNull[row_id] == 1) {                    \
            cur_batch->data[row_id] = col_data[row_id];           \
        }                                                         \
    }                                                             \
    cur_batch->numElements = end - start;

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::write_column_to_orc(const std::string& timezone,
                                                   const IColumn& column, const NullMap* null_map,
                                                   orc::ColumnVectorBatch* orc_col_batch,
                                                   int64_t start, int64_t end,
                                                   std::vector<StringRef>& buffer_list) const {
    auto& col_data = assert_cast<const ColumnType&>(column).get_data();

    if constexpr (T == TYPE_LARGEINT) { // largeint
        auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

        INIT_MEMORY_FOR_ORC_WRITER()

        for (size_t row_id = start; row_id < end; row_id++) {
            if (cur_batch->notNull[row_id] == 1) {
                std::string value_str = fmt::format("{}", col_data[row_id]);
                size_t len = value_str.size();

                REALLOC_MEMORY_FOR_ORC_WRITER()

                strcpy(const_cast<char*>(bufferRef.data) + offset, value_str.c_str());
                cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + offset;
                cur_batch->length[row_id] = len;
                offset += len;
            }
        }

        cur_batch->numElements = end - start;
    } else if constexpr (T == TYPE_BOOLEAN || T == TYPE_TINYINT) { // tinyint/boolean
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::ByteVectorBatch)
    } else if constexpr (T == TYPE_SMALLINT) { // smallint
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::ShortVectorBatch)
    } else if constexpr (T == TYPE_INT) { // int
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::IntVectorBatch)
    } else if constexpr (T == TYPE_BIGINT || T == TYPE_DATE || T == TYPE_DATETIME) { // bigint
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::LongVectorBatch)
    } else if constexpr (T == TYPE_FLOAT) { // float
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::FloatVectorBatch)
    } else if constexpr (T == TYPE_DOUBLE || T == TYPE_TIME || T == TYPE_TIMEV2) { // double
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::DoubleVectorBatch)
    } else if constexpr (T == TYPE_IPV4) { // ipv4
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::IntVectorBatch)
    }
    return Status::OK();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class DataTypeNumberSerDe<TYPE_BOOLEAN>;
template class DataTypeNumberSerDe<TYPE_TINYINT>;
template class DataTypeNumberSerDe<TYPE_SMALLINT>;
template class DataTypeNumberSerDe<TYPE_INT>;
template class DataTypeNumberSerDe<TYPE_BIGINT>;
template class DataTypeNumberSerDe<TYPE_LARGEINT>;
template class DataTypeNumberSerDe<TYPE_FLOAT>;
template class DataTypeNumberSerDe<TYPE_DOUBLE>;
template class DataTypeNumberSerDe<TYPE_DATE>;
template class DataTypeNumberSerDe<TYPE_DATEV2>;
template class DataTypeNumberSerDe<TYPE_DATETIME>;
template class DataTypeNumberSerDe<TYPE_DATETIMEV2>;
template class DataTypeNumberSerDe<TYPE_IPV4>;
template class DataTypeNumberSerDe<TYPE_IPV6>;
template class DataTypeNumberSerDe<TYPE_TIME>;
template class DataTypeNumberSerDe<TYPE_TIMEV2>;
} // namespace doris::vectorized
