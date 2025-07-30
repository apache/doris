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

#include "common/exception.h"
#include "common/status.h"
#include "util/jsonb_document.h"
#include "util/jsonb_document_cast.h"
#include "util/jsonb_writer.h"
#include "util/mysql_global.h"
#include "util/to_string.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/types.h"
#include "vec/functions/cast/cast_to_basic_number_common.h"
#include "vec/functions/cast/cast_to_boolean.h"
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
    StringRef str_ref {slice.data, slice.size};
    if constexpr (T == TYPE_IPV6) {
        // TODO: support for Uint128
        return Status::InvalidArgument("uint128 is not support");
    } else if constexpr (is_float_or_double(T) || T == TYPE_TIMEV2 || T == TYPE_TIME) {
        typename PrimitiveTypeTraits<T>::ColumnItemType val = 0;
        if (!try_read_float_text(val, str_ref)) {
            return Status::InvalidArgument("parse number fail, string: '{}'", slice.to_string());
        }
        column_data.insert_value(val);
    } else if constexpr (T == TYPE_BOOLEAN) {
        // Note: here we should handle the bool type
        typename PrimitiveTypeTraits<T>::ColumnItemType val = 0;
        if (!try_read_bool_text(val, str_ref)) {
            return Status::InvalidArgument("parse boolean fail, string: '{}'", slice.to_string());
        }
        column_data.insert_value(val);
    } else if constexpr (is_int_or_bool(T)) {
        typename PrimitiveTypeTraits<T>::ColumnItemType val = 0;
        if (!try_read_int_text(val, str_ref)) {
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
        int len = to_buffer(data, MAX_FLOAT_STR_LENGTH + 2, buf);
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
                    StringRef str_ref(raw_data, raw_data_len);
                    if (!try_read_int_text(val, str_ref)) {
                        return Status::Error(ErrorCode::INVALID_ARGUMENT,
                                             "parse number fail, string: '{}'",
                                             std::string(str_ref.data, str_ref.size).c_str());
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
constexpr bool can_write_to_jsonb_from_number() {
    return T == TYPE_BOOLEAN || T == TYPE_TINYINT || T == TYPE_SMALLINT || T == TYPE_INT ||
           T == TYPE_BIGINT || T == TYPE_LARGEINT || T == TYPE_FLOAT || T == TYPE_DOUBLE;
}

template <PrimitiveType T>
bool write_to_jsonb_from_number(auto& data, JsonbWriter& writer) {
    if constexpr (T == TYPE_BOOLEAN) {
        return writer.writeBool(data);
    } else if constexpr (T == TYPE_TINYINT) {
        return writer.writeInt8(data);
    } else if constexpr (T == TYPE_SMALLINT) {
        return writer.writeInt16(data);
    } else if constexpr (T == TYPE_INT) {
        return writer.writeInt32(data);
    } else if constexpr (T == TYPE_BIGINT) {
        return writer.writeInt64(data);
    } else if constexpr (T == TYPE_LARGEINT) {
        return writer.writeInt128(data);
    } else if constexpr (T == TYPE_FLOAT) {
        return writer.writeFloat(data);
    } else if constexpr (T == TYPE_DOUBLE) {
        return writer.writeDouble(data);
    } else {
        return false;
    }
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::serialize_column_to_jsonb(const IColumn& from_column,
                                                         int64_t row_num,
                                                         JsonbWriter& writer) const {
    if constexpr (!can_write_to_jsonb_from_number<T>()) {
        return Status::NotSupported("{} does not support serialize_column_to_jsonb", get_name());
    }
    const auto& data = assert_cast<const ColumnType&>(from_column).get_element(row_num);
    if (!write_to_jsonb_from_number<T>(data, writer)) {
        return Status::InvalidArgument("DataTypeNumberSerDe<T>::serialize_column_to_jsonb failed");
    }

    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::serialize_column_to_jsonb_vector(const IColumn& from_column,
                                                                ColumnString& to_column) const {
    if constexpr (!can_write_to_jsonb_from_number<T>()) {
        return Status::NotSupported("{} does not support serialize_column_to_jsonb", get_name());
    }
    const auto size = from_column.size();
    JsonbWriter writer;
    const auto& data = assert_cast<const ColumnType&>(from_column).get_data();
    for (int i = 0; i < size; i++) {
        writer.reset();
        if (!write_to_jsonb_from_number<T>(data[i], writer)) {
            return Status::InvalidArgument(
                    "DataTypeNumberSerDe<T>::serialize_column_to_jsonb failed for row {}", i);
        }
        to_column.insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::deserialize_column_from_jsonb(IColumn& column,
                                                             const JsonbValue* jsonb_value,
                                                             CastParameters& castParms) const {
    if constexpr (!can_write_to_jsonb_from_number<T>()) {
        return Status::NotSupported("{} does not support serialize_column_to_jsonb", get_name());
    } else {
        if (jsonb_value->isString()) {
            RETURN_IF_ERROR(parse_column_from_jsonb_string(column, jsonb_value, castParms));
            return Status::OK();
        }
        typename PrimitiveTypeTraits<T>::ColumnItemType to;
        auto cast_to_basic_number = [&]() {
            if constexpr (T == TYPE_BOOLEAN) {
                return JsonbCast::cast_from_json_to_boolean(jsonb_value, to, castParms);
            } else if constexpr (is_int(T)) {
                return JsonbCast::cast_from_json_to_int(jsonb_value, to, castParms);
            } else if constexpr (is_float_or_double(T)) {
                return JsonbCast::cast_from_json_to_float(jsonb_value, to, castParms);
            } else {
                return false;
            }
        };
        if (!cast_to_basic_number()) {
            return JsonbCast::report_error(jsonb_value, T);
        }
        auto& data = assert_cast<ColumnType&>(column).get_data();
        data.push_back(to);
        return Status::OK();
    }
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
        buf_ret = result.push_float(data[col_index]);
    } else if constexpr (T == TYPE_DOUBLE) {
        buf_ret = result.push_double(data[col_index]);
    } else if constexpr (T == TYPE_TIME || T == TYPE_TIMEV2) {
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
                                                   vectorized::Arena& arena) const {
    auto& col_data = assert_cast<const ColumnType&>(column).get_data();

    if constexpr (T == TYPE_LARGEINT) { // largeint
        auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);
        // First pass: calculate total memory needed and collect serialized values
        size_t total_size = 0;
        for (size_t row_id = start; row_id < end; row_id++) {
            if (cur_batch->notNull[row_id] == 1) {
                std::string value_str = fmt::format("{}", col_data[row_id]);
                size_t len = value_str.size();
                total_size += len;
            }
        }
        // Allocate continues memory based on calculated size
        char* ptr = arena.alloc(total_size);
        if (!ptr) {
            return Status::InternalError(
                    "malloc memory {} error when write variant column data to orc file.",
                    total_size);
        }
        // Second pass: fill the data and update the batch
        size_t offset = 0;
        for (size_t row_id = start; row_id < end; row_id++) {
            if (cur_batch->notNull[row_id] == 1) {
                std::string value_str = fmt::format("{}", col_data[row_id]);
                size_t len = value_str.size();
                if (offset + len > total_size) {
                    return Status::InternalError(
                            "Buffer overflow when writing column data to ORC file. offset {} with "
                            "len {} exceed total_size {} . ",
                            offset, len, total_size);
                }
                // do not use strcpy here, because this buffer is not null-terminated
                memcpy(ptr + offset, value_str.c_str(), len);
                cur_batch->data[row_id] = ptr + offset;
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

template <PrimitiveType T>
void DataTypeNumberSerDe<T>::read_one_cell_from_jsonb(IColumn& column,
                                                      const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnType&>(column);
    if constexpr (T == TYPE_TINYINT || T == TYPE_BOOLEAN) {
        col.insert_value(arg->unpack<JsonbInt8Val>()->val());
    } else if constexpr (T == TYPE_SMALLINT) {
        col.insert_value(arg->unpack<JsonbInt16Val>()->val());
    } else if constexpr (T == TYPE_INT || T == TYPE_DATEV2 || T == TYPE_IPV4) {
        col.insert_value(arg->unpack<JsonbInt32Val>()->val());
    } else if constexpr (T == TYPE_BIGINT || T == TYPE_DATE || T == TYPE_DATETIME ||
                         T == TYPE_DATETIMEV2) {
        col.insert_value(arg->unpack<JsonbInt64Val>()->val());
    } else if constexpr (T == TYPE_LARGEINT) {
        col.insert_value(arg->unpack<JsonbInt128Val>()->val());
    } else if constexpr (T == TYPE_FLOAT) {
        col.insert_value(arg->unpack<JsonbFloatVal>()->val());
    } else if constexpr (T == TYPE_DOUBLE || T == TYPE_TIME || T == TYPE_TIMEV2) {
        col.insert_value(arg->unpack<JsonbDoubleVal>()->val());
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "read_one_cell_from_jsonb with type '{}'", arg->typeName());
    }
}
template <PrimitiveType T>
void DataTypeNumberSerDe<T>::write_one_cell_to_jsonb(const IColumn& column,
                                                     JsonbWriterT<JsonbOutStream>& result,
                                                     Arena& mem_pool, int32_t col_id,
                                                     int64_t row_num) const {
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    StringRef data_ref = column.get_data_at(row_num);
    // TODO: Casting unsigned integers to signed integers may result in loss of data precision.
    // However, as Doris currently does not support unsigned integers, only the boolean type uses
    // uint8_t for representation, making the cast acceptable. In the future, we should add support for
    // both unsigned integers in Doris types and the JSONB types.
    if constexpr (T == TYPE_TINYINT || T == TYPE_BOOLEAN) {
        int8_t val = *reinterpret_cast<const int8_t*>(data_ref.data);
        result.writeInt8(val);
    } else if constexpr (T == TYPE_SMALLINT) {
        int16_t val = *reinterpret_cast<const int16_t*>(data_ref.data);
        result.writeInt16(val);
    } else if constexpr (T == TYPE_INT || T == TYPE_DATEV2 || T == TYPE_IPV4) {
        int32_t val = *reinterpret_cast<const int32_t*>(data_ref.data);
        result.writeInt32(val);
    } else if constexpr (T == TYPE_BIGINT || T == TYPE_DATE || T == TYPE_DATETIME ||
                         T == TYPE_DATETIMEV2) {
        int64_t val = *reinterpret_cast<const int64_t*>(data_ref.data);
        result.writeInt64(val);
    } else if constexpr (T == TYPE_LARGEINT) {
        __int128_t val = *reinterpret_cast<const __int128_t*>(data_ref.data);
        result.writeInt128(val);
    } else if constexpr (T == TYPE_FLOAT) {
        float val = *reinterpret_cast<const float*>(data_ref.data);
        result.writeFloat(val);
    } else if constexpr (T == TYPE_DOUBLE || T == TYPE_TIME || T == TYPE_TIMEV2) {
        double val = *reinterpret_cast<const double*>(data_ref.data);
        result.writeDouble(val);
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "write_one_cell_to_jsonb with type " + column.get_name());
    }
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::write_one_cell_to_json(const IColumn& column,
                                                      rapidjson::Value& result,
                                                      rapidjson::Document::AllocatorType& allocator,
                                                      Arena& mem_pool, int64_t row_num) const {
    const auto& data = reinterpret_cast<const ColumnType&>(column).get_data();
    if constexpr (T == TYPE_TINYINT || T == TYPE_SMALLINT || T == TYPE_INT) {
        result.SetInt(data[row_num]);
    } else if constexpr (T == TYPE_BOOLEAN || T == TYPE_DATEV2 || T == TYPE_IPV4) {
        result.SetUint(data[row_num]);
    } else if constexpr (T == TYPE_BIGINT || T == TYPE_DATE || T == TYPE_DATETIME) {
        result.SetInt64(data[row_num]);
    } else if constexpr (T == TYPE_DATETIMEV2) {
        result.SetUint64(data[row_num]);
    } else if constexpr (T == TYPE_FLOAT) {
        result.SetFloat(data[row_num]);
    } else if constexpr (T == TYPE_DOUBLE || T == TYPE_TIME || T == TYPE_TIMEV2) {
        result.SetDouble(data[row_num]);
    } else {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "unknown column type {} for writing to jsonb " + column.get_name());
        __builtin_unreachable();
    }
    return Status::OK();
}

template <PrimitiveType PT>
bool try_parse_impl(typename PrimitiveTypeTraits<PT>::ColumnItemType& x, const StringRef& str_ref,
                    CastParameters& params) {
    if constexpr (is_float_or_double(PT)) {
        return try_read_float_text(x, str_ref);
    } else if constexpr (PT == TYPE_BOOLEAN) {
        return CastToBool::from_string(str_ref, x, params);
    } else if constexpr (is_int(PT)) {
        return CastToInt::from_string(str_ref, x, params);
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "try_parse_impl not implemented for type: {}", type_to_string(PT));
    }
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::from_string(StringRef& str, IColumn& column,
                                           const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnType&, TypeCheckOnRelease::DISABLE>(column);
    typename PrimitiveTypeTraits<T>::ColumnItemType val;
    CastParameters params;
    params.is_strict = false;
    if (!try_parse_impl<T>(val, str, params)) {
        return Status::InvalidArgument("parse number fail, string: '{}'", str.to_string());
    }
    column_data.insert_value(val);
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::from_string_strict_mode(StringRef& str, IColumn& column,
                                                       const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnType&, TypeCheckOnRelease::DISABLE>(column);
    typename PrimitiveTypeTraits<T>::ColumnItemType val;
    CastParameters params;
    params.is_strict = true;
    if (!try_parse_impl<T>(val, str, params)) {
        return Status::InvalidArgument("parse number fail, string: '{}'", str.to_string());
    }
    column_data.insert_value(val);
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::from_string_batch(const ColumnString& str, ColumnNullable& column,
                                                 const FormatOptions& options) const {
    const auto size = str.size();
    column.resize(size);

    auto& column_to = assert_cast<ColumnType&>(column.get_nested_column());
    auto& vec_to = column_to.get_data();
    auto& null_map = column.get_null_map_data();

    size_t current_offset = 0;
    const ColumnString::Chars* chars = &str.get_chars();
    const IColumn::Offsets* offsets = &str.get_offsets();

    CastParameters params;
    params.is_strict = false;
    for (size_t i = 0; i < size; ++i) {
        size_t next_offset = (*offsets)[i];
        size_t string_size = next_offset - current_offset;

        StringRef str_ref(&(*chars)[current_offset], string_size);
        null_map[i] = !try_parse_impl<T>(vec_to[i], str_ref, params);
        current_offset = next_offset;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::read_one_cell_from_json(IColumn& column,
                                                       const rapidjson::Value& value) const {
    auto& col = reinterpret_cast<ColumnType&>(column);
    switch (value.GetType()) {
    case rapidjson::Type::kNumberType:
        if (value.IsUint()) {
            col.insert_value((typename PrimitiveTypeTraits<T>::ColumnItemType)value.GetUint());
        } else if (value.IsInt()) {
            col.insert_value((typename PrimitiveTypeTraits<T>::ColumnItemType)value.GetInt());
        } else if (value.IsUint64()) {
            col.insert_value((typename PrimitiveTypeTraits<T>::ColumnItemType)value.GetUint64());
        } else if (value.IsInt64()) {
            col.insert_value((typename PrimitiveTypeTraits<T>::ColumnItemType)value.GetInt64());
        } else if (value.IsFloat() || value.IsDouble()) {
            col.insert_value(typename PrimitiveTypeTraits<T>::ColumnItemType(value.GetDouble()));
        } else {
            CHECK(false) << "Improssible";
        }
        break;
    case rapidjson::Type::kFalseType:
        col.insert_value((typename PrimitiveTypeTraits<T>::ColumnItemType)0);
        break;
    case rapidjson::Type::kTrueType:
        col.insert_value((typename PrimitiveTypeTraits<T>::ColumnItemType)1);
        break;
    default:
        col.insert_default();
        break;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::from_string_strict_mode_batch(
        const ColumnString& str, IColumn& column, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    const auto size = str.size();
    column.resize(size);

    size_t current_offset = 0;
    const ColumnString::Chars* chars = &str.get_chars();
    const IColumn::Offsets* offsets = &str.get_offsets();

    auto& column_to = assert_cast<ColumnType&>(column);
    auto& vec_to = column_to.get_data();
    CastParameters params;
    params.is_strict = true;
    for (size_t i = 0; i < size; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        size_t next_offset = (*offsets)[i];
        size_t string_size = next_offset - current_offset;

        StringRef str_ref(&(*chars)[current_offset], string_size);
        if (!try_parse_impl<T>(vec_to[i], str_ref, params)) {
            return Status::InvalidArgument(
                    "parse number fail, string: '{}'",
                    std::string((char*)&(*chars)[current_offset], string_size));
        }
        current_offset = next_offset;
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
