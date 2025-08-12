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

#include "data_type_date_or_datetime_serde.h"

#include <arrow/builder.h>
#include <cctz/time_zone.h>

#include "common/status.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/cast/cast_base.h"
#include "vec/functions/cast/cast_to_date_or_datetime_impl.hpp"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::serialize_column_to_json(
        const IColumn& column, int64_t start_idx, int64_t end_idx, BufferWritable& bw,
        typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::serialize_one_cell_to_json(
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
Status DataTypeDateSerDe<T>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::deserialize_one_cell_from_json(
        IColumn& column, Slice& slice,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnVector<T>&>(column);
    if (DataTypeNumberSerDe<T>::_nesting_level > 1) {
        slice.trim_quote();
    }
    Int64 val = 0;
    if (StringRef str(slice.data, slice.size); !read_date_text_impl<Int64>(val, str)) {
        return Status::InvalidArgument("parse date fail, string: '{}'", str.to_string());
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
    if (_nesting_level > 1) {
        bw.write('"');
    }
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

    char buf[64];
    char* pos = value.to_string(buf);
    bw.write(buf, pos - buf - 1);
    if (_nesting_level > 1) {
        bw.write('"');
    }
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
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    Int64 val = 0;
    if (StringRef str(slice.data, slice.size); !read_datetime_text_impl<Int64>(val, str)) {
        return Status::InvalidArgument("parse datetime fail, string: '{}'", str.to_string());
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
Status DataTypeDateSerDe<T>::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                   arrow::ArrayBuilder* array_builder,
                                                   int64_t start, int64_t end,
                                                   const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<const ColumnVector<T>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        char buf[64];
        const auto* time_val = (const VecDateTimeValue*)(&col_data[i]);
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
Status DataTypeDateSerDe<T>::_read_column_from_arrow(IColumn& column,
                                                     const arrow::Array* arrow_array, int64_t start,
                                                     int64_t end,
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
Status DataTypeDateSerDe<T>::read_column_from_arrow(IColumn& column,
                                                    const arrow::Array* arrow_array, int64_t start,
                                                    int64_t end, const cctz::time_zone& ctz) const {
    return _read_column_from_arrow<true>(column, arrow_array, start, end, ctz);
}

template <PrimitiveType T>
template <bool is_binary_format>
Status DataTypeDateSerDe<T>::_write_column_to_mysql(
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
Status DataTypeDateSerDe<T>::write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<true>& row_buffer, int64_t row_idx, bool col_const,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<false>& row_buffer, int64_t row_idx, bool col_const,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                 const NullMap* null_map,
                                                 orc::ColumnVectorBatch* orc_col_batch,
                                                 int64_t start, int64_t end,
                                                 vectorized::Arena& arena) const {
    const auto& col_data = assert_cast<const ColumnVector<T>&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    // First pass: calculate total memory needed and collect serialized values
    std::vector<std::string> serialized_values;
    std::vector<size_t> valid_row_indices;
    size_t total_size = 0;
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            char buf[64];
            size_t len = binary_cast<Int64, VecDateTimeValue>(col_data[row_id]).to_buffer(buf);
            total_size += len;
            // avoid copy
            serialized_values.emplace_back(buf, len);
            valid_row_indices.push_back(row_id);
        }
    }
    // Allocate continues memory based on calculated size
    char* ptr = arena.alloc(total_size);
    if (!ptr) {
        return Status::InternalError(
                "malloc memory {} error when write variant column data to orc file.", total_size);
    }
    // Second pass: copy data to allocated memory
    size_t offset = 0;
    for (size_t i = 0; i < serialized_values.size(); i++) {
        const auto& serialized_value = serialized_values[i];
        size_t row_id = valid_row_indices[i];
        size_t len = serialized_value.length();
        if (offset + len > total_size) {
            return Status::InternalError(
                    "Buffer overflow when writing column data to ORC file. offset {} with len {} "
                    "exceed total_size {} . ",
                    offset, len, total_size);
        }
        memcpy(ptr + offset, serialized_value.data(), len);
        cur_batch->data[row_id] = ptr + offset;
        cur_batch->length[row_id] = len;
        offset += len;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
template <PrimitiveType T>
Status DataTypeDateSerDe<T>::from_string_batch(
        const ColumnString& col_str, ColumnNullable& col_res,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnType&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_data_at(i);
        CppType res;
        // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
        // then we rely on return value to check success.
        // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
        // Exception!
        if (!CastToDateOrDatetime::from_string_non_strict_mode<IsDatetime>(
                    str, res, options.timezone, params)) [[unlikely]] {
            col_nullmap.get_data()[i] = true;
            //TODO: we should set `for` functions who need it then skip to set default value for null rows.
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(VecDateTimeValue::FIRST_DAY);
        } else {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(res);
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::from_string_strict_mode_batch(
        const ColumnString& col_str, IColumn& col_res,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options,
        const NullMap::value_type* null_map) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnType&>(col_res);

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < row; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        auto str = col_str.get_data_at(i);
        CppType res;
        CastToDateOrDatetime::from_string_strict_mode<true, IsDatetime>(str, res, options.timezone,
                                                                        params);
        // only after we called something with `IS_STRICT = true`, params.status will be set
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to {} failed: ", str.to_string_view(), name()));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<CppType, NativeType>(res);
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::from_string(StringRef& str, IColumn& column,
                                         const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnType&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = false};

    CppType res;
    // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
    // then we rely on return value to check success.
    // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
    // Exception!
    if (!CastToDateOrDatetime::from_string_non_strict_mode<IsDatetime>(str, res, options.timezone,
                                                                       params)) [[unlikely]] {
        return Status::InvalidArgument("parse date or datetime fail, string: '{}'",
                                       str.to_string());
    }
    col_data.insert_value(binary_cast<CppType, NativeType>(res));
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::from_string_strict_mode(StringRef& str, IColumn& column,
                                                     const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnType&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = true};

    CppType res;
    CastToDateOrDatetime::from_string_strict_mode<true, IsDatetime>(str, res, options.timezone,
                                                                    params);
    // only after we called something with `IS_STRICT = true`, params.status will be set
    if (!params.status.ok()) [[unlikely]] {
        params.status.prepend(fmt::format("parse {} to {} failed: ", str.to_string_view(), name()));
        return params.status;
    }
    col_data.insert_value(binary_cast<CppType, NativeType>(res));

    return Status::OK();
}

template <PrimitiveType T>
template <typename IntDataType>
Status DataTypeDateSerDe<T>::from_int_batch(const IntDataType::ColumnType& int_col,
                                            ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(int_col.size());
    col_nullmap.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < int_col.size(); ++i) {
        CppType val;
        if (CastToDateOrDatetime::from_integer<false, IsDatetime>(int_col.get_element(i), val,
                                                                  params)) [[likely]] {
            // did cast_to_type in `from_integer`
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(VecDateTimeValue::FIRST_DAY);
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename IntDataType>
Status DataTypeDateSerDe<T>::from_int_strict_mode_batch(const IntDataType::ColumnType& int_col,
                                                        IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col);
    col_data.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < int_col.size(); ++i) {
        CppType val;
        CastToDateOrDatetime::from_integer<true, IsDatetime>(int_col.get_element(i), val, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to {} failed: ", int_col.get_element(i), name()));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename FloatDataType>
Status DataTypeDateSerDe<T>::from_float_batch(const FloatDataType::ColumnType& float_col,
                                              ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(float_col.size());
    col_nullmap.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < float_col.size(); ++i) {
        CppType val;
        if (CastToDateOrDatetime::from_float<false, IsDatetime>(float_col.get_data()[i], val,
                                                                params)) [[likely]] {
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(VecDateTimeValue::FIRST_DAY);
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename FloatDataType>
Status DataTypeDateSerDe<T>::from_float_strict_mode_batch(
        const FloatDataType::ColumnType& float_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col);
    col_data.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < float_col.size(); ++i) {
        CppType val;
        CastToDateOrDatetime::from_float<true, IsDatetime>(float_col.get_data()[i], val, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to {} failed: ", float_col.get_data()[i], name()));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename DecimalDataType>
Status DataTypeDateSerDe<T>::from_decimal_batch(const DecimalDataType::ColumnType& decimal_col,
                                                ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(decimal_col.size());
    col_nullmap.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        CppType val;
        if (CastToDateOrDatetime::from_decimal<true, IsDatetime>(
                    decimal_col.get_intergral_part(i), decimal_col.get_fractional_part(i),
                    decimal_col.get_scale(), val, params)) [[likely]] {
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(VecDateTimeValue::FIRST_DAY);
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename DecimalDataType>
Status DataTypeDateSerDe<T>::from_decimal_strict_mode_batch(
        const DecimalDataType::ColumnType& decimal_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col);
    col_data.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        CppType val;
        CastToDateOrDatetime::from_decimal<true, IsDatetime>(decimal_col.get_intergral_part(i),
                                                             decimal_col.get_fractional_part(i),
                                                             decimal_col.get_scale(), val, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {}.{} to {} failed: ", decimal_col.get_intergral_part(i),
                                decimal_col.get_fractional_part(i), name()));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
    }
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

// instantiation of template functions
template class DataTypeDateSerDe<TYPE_DATE>;
template class DataTypeDateSerDe<TYPE_DATETIME>;

template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;

template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status
DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status
DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;
} // namespace doris::vectorized
