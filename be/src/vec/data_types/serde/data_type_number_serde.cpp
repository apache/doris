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

namespace doris {
namespace vectorized {

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
                arrow::FixedSizeBinaryBuilder, Float32, arrow::FloatBuilder, Float64,
                arrow::DoubleBuilder, void,
                void // Add this line to represent the end of the TypeMap
                >;

template <typename T>
void DataTypeNumberSerDe<T>::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                   arrow::ArrayBuilder* array_builder, int start,
                                                   int end) const {
    auto& col_data = assert_cast<const ColumnType&>(column).get_data();
    using ARROW_BUILDER_TYPE = typename TypeMapLookup<T, DORIS_NUMERIC_ARROW_BUILDER>::ValueType;
    auto arrow_null_map = revert_null_map(null_map, start, end);
    auto arrow_null_map_data = arrow_null_map.empty() ? nullptr : arrow_null_map.data();
    if constexpr (std::is_same_v<T, UInt8>) {
        ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        checkArrowStatus(
                builder.AppendValues(reinterpret_cast<const uint8_t*>(col_data.data() + start),
                                     end - start,
                                     reinterpret_cast<const uint8_t*>(arrow_null_map_data)),
                column.get_name(), array_builder->type()->name());
    } else if constexpr (std::is_same_v<T, Int128>) {
        auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        for (size_t i = start; i < end; ++i) {
            auto& data_value = col_data[i];
            std::string value_str = fmt::format("{}", data_value);
            if (null_map && (*null_map)[i]) {
                checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                                 array_builder->type()->name());
            } else {
                checkArrowStatus(string_builder.Append(value_str.data(), value_str.length()),
                                 column.get_name(), array_builder->type()->name());
            }
        }
    } else if constexpr (std::is_same_v<T, UInt128>) {
    } else {
        ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        checkArrowStatus(
                builder.AppendValues(col_data.data() + start, end - start,
                                     reinterpret_cast<const uint8_t*>(arrow_null_map_data)),
                column.get_name(), array_builder->type()->name());
    }
}

template <typename T>
Status DataTypeNumberSerDe<T>::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                              const FormatOptions& options) const {
    auto& column_data = reinterpret_cast<ColumnType&>(column);
    ReadBuffer rb(slice.data, slice.size);
    if constexpr (std::is_same<T, UInt128>::value) {
        // TODO: support for Uint128
        return Status::InvalidArgument("uint128 is not support");
    } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
        T val = 0;
        if (!read_float_text_fast_impl(val, rb)) {
            return Status::InvalidArgument("parse number fail, string: '{}'",
                                           std::string(rb.position(), rb.count()).c_str());
        }
        column_data.insert_value(val);
    } else if constexpr (std::is_same_v<T, uint8_t>) {
        // Note: here we should handle the bool type
        T val = 0;
        if (!try_read_bool_text(val, rb)) {
            return Status::InvalidArgument("parse boolean fail, string: '{}'",
                                           std::string(rb.position(), rb.count()).c_str());
        }
        column_data.insert_value(val);
    } else if constexpr (std::is_integral<T>::value) {
        T val = 0;
        if (!read_int_text_impl(val, rb)) {
            return Status::InvalidArgument("parse number fail, string: '{}'",
                                           std::string(rb.position(), rb.count()).c_str());
        }
        column_data.insert_value(val);
    } else {
        DCHECK(false);
    }
    return Status::OK();
}

template <typename T>
Status DataTypeNumberSerDe<T>::serialize_column_to_json(const IColumn& column, int start_idx,
                                                        int end_idx, BufferWritable& bw,
                                                        FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <typename T>
Status DataTypeNumberSerDe<T>::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                          BufferWritable& bw,
                                                          FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    auto data = assert_cast<const ColumnVector<T>&>(*ptr).get_element(row_num);
    if constexpr (std::is_same<T, UInt128>::value) {
        std::string hex = int128_to_string(data);
        bw.write(hex.data(), hex.size());
    } else if constexpr (std::is_same_v<T, float>) {
        // fmt::format_to maybe get inaccurate results at float type, so we use gutil implement.
        char buf[MAX_FLOAT_STR_LENGTH + 2];
        int len = FloatToBuffer(data, MAX_FLOAT_STR_LENGTH + 2, buf);
        bw.write(buf, len);
    } else if constexpr (std::is_integral<T>::value || std::numeric_limits<T>::is_iec559) {
        bw.write_number(data);
    }
    return Status::OK();
}

template <typename T>
Status DataTypeNumberSerDe<T>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

template <typename T>
void DataTypeNumberSerDe<T>::read_column_from_arrow(IColumn& column,
                                                    const arrow::Array* arrow_array, int start,
                                                    int end, const cctz::time_zone& ctz) const {
    int row_count = end - start;
    auto& col_data = static_cast<ColumnVector<T>&>(column).get_data();

    // now uint8 for bool
    if constexpr (std::is_same_v<T, UInt8>) {
        auto concrete_array = dynamic_cast<const arrow::BooleanArray*>(arrow_array);
        for (size_t bool_i = 0; bool_i != static_cast<size_t>(concrete_array->length()); ++bool_i) {
            col_data.emplace_back(concrete_array->Value(bool_i));
        }
        return;
    }

    // only for largeint(int128) type
    if (arrow_array->type_id() == arrow::Type::STRING) {
        auto concrete_array = dynamic_cast<const arrow::StringArray*>(arrow_array);
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

        for (size_t offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = buffer->data() + concrete_array->value_offset(offset_i);
                const auto raw_data_len = concrete_array->value_length(offset_i);

                Int128 val = 0;
                ReadBuffer rb(raw_data, raw_data_len);
                if (!read_int_text_impl(val, rb)) {
                    throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                           "parse number fail, string: '{}'",
                                           std::string(rb.position(), rb.count()).c_str());
                }
                col_data.emplace_back(val);
            }
        }
        return;
    }

    /// buffers[0] is a null bitmap and buffers[1] are actual values
    std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
    const auto* raw_data = reinterpret_cast<const T*>(buffer->data()) + start;
    col_data.insert(raw_data, raw_data + row_count);
}

template <typename T>
template <bool is_binary_format>
Status DataTypeNumberSerDe<T>::_write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<is_binary_format>& result,
                                                      int row_idx, bool col_const) const {
    int buf_ret = 0;
    auto& data = assert_cast<const ColumnType&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    if constexpr (std::is_same_v<T, Int8> || std::is_same_v<T, UInt8>) {
        buf_ret = result.push_tinyint(data[col_index]);
    } else if constexpr (std::is_same_v<T, Int16> || std::is_same_v<T, UInt16>) {
        buf_ret = result.push_smallint(data[col_index]);
    } else if constexpr (std::is_same_v<T, Int32> || std::is_same_v<T, UInt32>) {
        buf_ret = result.push_int(data[col_index]);
    } else if constexpr (std::is_same_v<T, Int64> || std::is_same_v<T, UInt64>) {
        buf_ret = result.push_bigint(data[col_index]);
    } else if constexpr (std::is_same_v<T, Int128>) {
        buf_ret = result.push_largeint(data[col_index]);
    } else if constexpr (std::is_same_v<T, float>) {
        buf_ret = result.push_float(data[col_index]);
    } else if constexpr (std::is_same_v<T, double>) {
        buf_ret = result.push_double(data[col_index]);
    }
    if (UNLIKELY(buf_ret != 0)) {
        return Status::InternalError("pack mysql buffer failed.");
    } else {
        return Status::OK();
    }
}

template <typename T>
Status DataTypeNumberSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                     bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

template <typename T>
Status DataTypeNumberSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                     bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

#define WRITE_INTEGRAL_COLUMN_TO_ORC(ORC_TYPE)                    \
    ORC_TYPE* cur_batch = dynamic_cast<ORC_TYPE*>(orc_col_batch); \
    for (size_t row_id = start; row_id < end; row_id++) {         \
        if (cur_batch->notNull[row_id] == 1) {                    \
            cur_batch->data[row_id] = col_data[row_id];           \
        }                                                         \
    }                                                             \
    cur_batch->numElements = end - start;

template <typename T>
Status DataTypeNumberSerDe<T>::write_column_to_orc(const std::string& timezone,
                                                   const IColumn& column, const NullMap* null_map,
                                                   orc::ColumnVectorBatch* orc_col_batch, int start,
                                                   int end,
                                                   std::vector<StringRef>& buffer_list) const {
    auto& col_data = assert_cast<const ColumnType&>(column).get_data();

    if constexpr (std::is_same_v<T, Int128>) { // largeint
        orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

        char* ptr = (char*)malloc(BUFFER_UNIT_SIZE);
        if (!ptr) {
            return Status::InternalError(
                    "malloc memory error when write largeint column data to orc file.");
        }
        StringRef bufferRef;
        bufferRef.data = ptr;
        bufferRef.size = BUFFER_UNIT_SIZE;
        size_t offset = 0;
        const size_t begin_off = offset;

        for (size_t row_id = start; row_id < end; row_id++) {
            if (cur_batch->notNull[row_id] == 0) {
                continue;
            }
            std::string value_str = fmt::format("{}", col_data[row_id]);
            size_t len = value_str.size();

            REALLOC_MEMORY_FOR_ORC_WRITER()

            strcpy(const_cast<char*>(bufferRef.data) + offset, value_str.c_str());
            offset += len;
            cur_batch->length[row_id] = len;
        }
        size_t data_off = 0;
        for (size_t row_id = start; row_id < end; row_id++) {
            if (cur_batch->notNull[row_id] == 1) {
                cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + begin_off + data_off;
                data_off += cur_batch->length[row_id];
            }
        }
        buffer_list.emplace_back(bufferRef);
        cur_batch->numElements = end - start;
    } else if constexpr (std::is_same_v<T, Int8> || std::is_same_v<T, UInt8>) { // tinyint/boolean
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::ByteVectorBatch)
    } else if constexpr (std::is_same_v<T, Int16>) { // smallint
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::ShortVectorBatch)
    } else if constexpr (std::is_same_v<T, Int32>) { // int
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::IntVectorBatch)
    } else if constexpr (std::is_same_v<T, Int64>) { // bigint
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::LongVectorBatch)
    } else if constexpr (std::is_same_v<T, Float32>) { // float
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::FloatVectorBatch)
    } else if constexpr (std::is_same_v<T, Float64>) { // double
        WRITE_INTEGRAL_COLUMN_TO_ORC(orc::DoubleVectorBatch)
    }
    return Status::OK();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class DataTypeNumberSerDe<UInt8>;
template class DataTypeNumberSerDe<UInt16>;
template class DataTypeNumberSerDe<UInt32>;
template class DataTypeNumberSerDe<UInt64>;
template class DataTypeNumberSerDe<UInt128>;
template class DataTypeNumberSerDe<Int8>;
template class DataTypeNumberSerDe<Int16>;
template class DataTypeNumberSerDe<Int32>;
template class DataTypeNumberSerDe<Int64>;
template class DataTypeNumberSerDe<Int128>;
template class DataTypeNumberSerDe<Float32>;
template class DataTypeNumberSerDe<Float64>;
} // namespace vectorized
} // namespace doris