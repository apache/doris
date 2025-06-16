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

#include "data_type_date64_serde.h"

#include <arrow/builder.h>

#include "vec/columns/column_const.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::serialize_column_to_json(
        const IColumn& column, int64_t start_idx, int64_t end_idx, BufferWritable& bw,
        typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::serialize_one_cell_to_json(
        const IColumn& column, int64_t row_num, BufferWritable& bw,
        typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    if (DataTypeNumberSerDe<T>::_nesting_level > 1) {
        bw.write('"');
    }
    Int64 int_val = assert_cast<const ColumnVector<T>&>(*ptr).get_element(row_num);
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

    char buf[64];
    char* pos = value.to_string(buf);
    bw.write(buf, pos - buf - 1);
    if (DataTypeNumberSerDe<T>::_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::deserialize_one_cell_from_json(
        IColumn& column, Slice& slice,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnVector<T>&>(column);
    if (DataTypeNumberSerDe<T>::_nesting_level > 1) {
        slice.trim_quote();
    }
    Int64 val = 0;
    if (ReadBuffer rb(slice.data, slice.size); !read_date_text_impl<Int64>(val, rb)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                       int64_t end_idx, BufferWritable& bw,
                                                       FormatOptions& options) const {
        SERIALIZE_COLUMN_TO_JSON()}

Status DataTypeDateTimeSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                         BufferWritable& bw,
                                                         FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    Int64 int_val = assert_cast<const ColumnDateTime&>(*ptr).get_element(row_num);
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

    char buf[64];
    char* pos = value.to_string(buf);
    bw.write(buf, pos - buf - 1);
    return Status::OK();
}

Status DataTypeDateTimeSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

Status DataTypeDateTimeSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                             const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnDateTime&>(column);
    Int64 val = 0;
    if (ReadBuffer rb(slice.data, slice.size); !read_datetime_text_impl<Int64>(val, rb)) {
        return Status::InvalidArgument("parse datetime fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeSerDe::read_column_from_arrow(IColumn& column,
                                                     const arrow::Array* arrow_array, int64_t start,
                                                     int64_t end,
                                                     const cctz::time_zone& ctz) const {
    return _read_column_from_arrow<false>(column, arrow_array, start, end, ctz);
}

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                     arrow::ArrayBuilder* array_builder,
                                                     int64_t start, int64_t end,
                                                     const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<const ColumnVector<T>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        char buf[64];
        const VecDateTimeValue* time_val = (const VecDateTimeValue*)(&col_data[i]);
        size_t len = time_val->to_buffer(buf);
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
        } else {
            RETURN_IF_ERROR(checkArrowStatus(string_builder.Append(buf, cast_set<int32_t>(len)),
                                             column.get_name(), array_builder->type()->name()));
        }
    }
    return Status::OK();
}

static int64_t time_unit_divisor(arrow::TimeUnit::type unit) {
    // Doris only supports seconds
    switch (unit) {
    case arrow::TimeUnit::type::SECOND: {
        return 1L;
    }
    case arrow::TimeUnit::type::MILLI: {
        return 1000L;
    }
    case arrow::TimeUnit::type::MICRO: {
        return 1000000L;
    }
    case arrow::TimeUnit::type::NANO: {
        return 1000000000L;
    }
    default:
        return 0L;
    }
}

template <PrimitiveType T>
template <bool is_date>
Status DataTypeDate64SerDe<T>::_read_column_from_arrow(IColumn& column,
                                                       const arrow::Array* arrow_array,
                                                       int64_t start, int64_t end,
                                                       const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnVector<T>&>(column).get_data();
    int64_t divisor = 1;
    int64_t multiplier = 1;
    if (arrow_array->type()->id() == arrow::Type::DATE64) {
        const auto* concrete_array = dynamic_cast<const arrow::Date64Array*>(arrow_array);
        divisor = 1000; //ms => secs
        for (auto value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
        const auto* concrete_array = dynamic_cast<const arrow::TimestampArray*>(arrow_array);
        const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
        divisor = time_unit_divisor(type->unit());
        if (divisor == 0L) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid Time Type: " + type->name());
        }
        for (auto value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::DATE32) {
        const auto* concrete_array = dynamic_cast<const arrow::Date32Array*>(arrow_array);
        multiplier = 24 * 60 * 60; // day => secs
        for (auto value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            v.cast_to_date();
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::STRING) {
        // to be compatible with old version, we use string type for date.
        const auto* concrete_array = dynamic_cast<const arrow::StringArray*>(arrow_array);
        for (auto value_i = start; value_i < end; ++value_i) {
            auto val_str = concrete_array->GetString(value_i);
            VecDateTimeValue v;
            v.from_date_str(val_str.c_str(), val_str.length(), ctz);
            if constexpr (is_date) {
                v.cast_to_date();
            }
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else {
        return Status::Error(doris::ErrorCode::INVALID_ARGUMENT,
                             "Unsupported Arrow Type: " + arrow_array->type()->name());
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::read_column_from_arrow(IColumn& column,
                                                      const arrow::Array* arrow_array,
                                                      int64_t start, int64_t end,
                                                      const cctz::time_zone& ctz) const {
    return _read_column_from_arrow<true>(column, arrow_array, start, end, ctz);
}

template <PrimitiveType T>
template <bool is_binary_format>
Status DataTypeDate64SerDe<T>::_write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<is_binary_format>& result, int64_t row_idx,
        bool col_const, const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnVector<T>&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    auto time_num = data[col_index];
    VecDateTimeValue time_val = binary_cast<Int64, VecDateTimeValue>(time_num);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (DataTypeNumberSerDe<T>::_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_vec_datetime(time_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (DataTypeNumberSerDe<T>::_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<true>& row_buffer, int64_t row_idx, bool col_const,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<false>& row_buffer, int64_t row_idx, bool col_const,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeDate64SerDe<T>::write_column_to_orc(const std::string& timezone,
                                                   const IColumn& column, const NullMap* null_map,
                                                   orc::ColumnVectorBatch* orc_col_batch,
                                                   int64_t start, int64_t end,
                                                   std::vector<StringRef>& buffer_list) const {
    const auto& col_data = assert_cast<const ColumnVector<T>&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    auto& BUFFER_UNIT_SIZE = DataTypeNumberSerDe<T>::BUFFER_UNIT_SIZE;
    auto& BUFFER_RESERVED_SIZE = DataTypeNumberSerDe<T>::BUFFER_RESERVED_SIZE;
    INIT_MEMORY_FOR_ORC_WRITER()

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }

        size_t len = binary_cast<Int64, VecDateTimeValue>(col_data[row_id])
                             .to_buffer(const_cast<char*>(bufferRef.data) + offset);

        REALLOC_MEMORY_FOR_ORC_WRITER()

        cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + offset;
        cur_batch->length[row_id] = len;
        offset += len;
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

template class DataTypeDate64SerDe<TYPE_DATE>;
template class DataTypeDate64SerDe<TYPE_DATETIME>;

} // namespace doris::vectorized
