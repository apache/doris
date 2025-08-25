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

#include "data_type_datetimev2_serde.h"

#include <arrow/builder.h>
#include <cctz/time_zone.h>

#include <chrono> // IWYU pragma: keep
#include <cstdint>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_const.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/cast/cast_base.h"
#include "vec/functions/cast/cast_to_datetimev2_impl.hpp"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

enum {
    DIVISOR_FOR_SECOND = 1,
    DIVISOR_FOR_MILLI = 1000,
    DIVISOR_FOR_MICRO = 1000000,
    DIVISOR_FOR_NANO = 1000000000
};

namespace doris::vectorized {
static const int64_t micro_to_nano_second = 1000;
#include "common/compile_check_begin.h"

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
Status DataTypeDateTimeV2SerDe::from_string_batch(const ColumnString& col_str,
                                                  ColumnNullable& col_res,
                                                  const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_data_at(i);
        DateV2Value<DateTimeV2ValueType> res;
        // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
        // then we rely on return value to check success.
        // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
        // Exception!
        if (!CastToDatetimeV2::from_string_non_strict_mode(str, res, options.timezone, _scale,
                                                           params)) [[unlikely]] {
            col_nullmap.get_data()[i] = true;
            //TODO: we should set `for` functions who need it then skip to set default value for null rows.
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        } else {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(res);
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::from_string_strict_mode_batch(
        const ColumnString& col_str, IColumn& col_res, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnDateTimeV2&>(col_res);

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < row; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        auto str = col_str.get_data_at(i);
        DateV2Value<DateTimeV2ValueType> res;
        CastToDatetimeV2::from_string_strict_mode<true>(str, res, options.timezone, _scale, params);
        // only after we called something with `IS_STRICT = true`, params.status will be set
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to datetime failed: ", str.to_string_view()));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(res);
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::from_string(StringRef& str, IColumn& column,
                                            const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = false};

    DateV2Value<DateTimeV2ValueType> res;
    // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
    // then we rely on return value to check success.
    // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
    // Exception!
    if (!CastToDatetimeV2::from_string_non_strict_mode(str, res, options.timezone, _scale, params))
            [[unlikely]] {
        return Status::InvalidArgument("parse datetimev2 fail, string: '{}'", str.to_string());
    }
    col_data.insert_value(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(res));
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                        const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = true};

    DateV2Value<DateTimeV2ValueType> res;
    CastToDatetimeV2::from_string_strict_mode<true>(str, res, options.timezone, _scale, params);
    // only after we called something with `IS_STRICT = true`, params.status will be set
    if (!params.status.ok()) [[unlikely]] {
        params.status.prepend(fmt::format("parse {} to datetime failed: ", str.to_string_view()));
        return params.status;
    }
    col_data.insert_value(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(res));
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeDateTimeV2SerDe::from_int_batch(const IntDataType::ColumnType& int_col,
                                               ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(int_col.size());
    col_nullmap.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < int_col.size(); ++i) {
        DateV2Value<DateTimeV2ValueType> val;
        if (CastToDatetimeV2::from_integer<false>(int_col.get_element(i), val, params)) [[likely]] {
            col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        }
    }
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch(const IntDataType::ColumnType& int_col,
                                                           IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col);
    col_data.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < int_col.size(); ++i) {
        DateV2Value<DateTimeV2ValueType> val;
        CastToDatetimeV2::from_integer<true>(int_col.get_element(i), val, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to datetime failed: ", int_col.get_element(i)));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeDateTimeV2SerDe::from_float_batch(const FloatDataType::ColumnType& float_col,
                                                 ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(float_col.size());
    col_nullmap.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < float_col.size(); ++i) {
        DateV2Value<DateTimeV2ValueType> val;
        if (CastToDatetimeV2::from_float<false>(float_col.get_data()[i], val, _scale, params))
                [[likely]] {
            col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        }
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeDateTimeV2SerDe::from_float_strict_mode_batch(
        const FloatDataType::ColumnType& float_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col);
    col_data.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < float_col.size(); ++i) {
        DateV2Value<DateTimeV2ValueType> val;
        CastToDatetimeV2::from_float<true>(float_col.get_data()[i], val, _scale, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to datetime failed: ", float_col.get_data()[i]));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeDateTimeV2SerDe::from_decimal_batch(const DecimalDataType::ColumnType& decimal_col,
                                                   ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(decimal_col.size());
    col_nullmap.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        DateV2Value<DateTimeV2ValueType> val;
        if (CastToDatetimeV2::from_decimal<true>(
                    decimal_col.get_intergral_part(i), decimal_col.get_fractional_part(i),
                    decimal_col.get_scale(), val, _scale, params)) [[likely]] {
            col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        }
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch(
        const DecimalDataType::ColumnType& decimal_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col);
    col_data.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        DateV2Value<DateTimeV2ValueType> val;
        CastToDatetimeV2::from_decimal<true>(decimal_col.get_intergral_part(i),
                                             decimal_col.get_fractional_part(i),
                                             decimal_col.get_scale(), val, _scale, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(fmt::format(
                    "parse {}.{} to datetime failed: ", decimal_col.get_intergral_part(i),
                    decimal_col.get_fractional_part(i)));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                         int64_t end_idx, BufferWritable& bw,
                                                         FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDateTimeV2SerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                           BufferWritable& bw,
                                                           FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    if (_nesting_level > 1) {
        bw.write('"');
    }
    UInt64 int_val =
            assert_cast<const ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                    row_num);
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    char buf[64];
    char* pos = val.to_string(buf);
    bw.write(buf, pos - buf - 1);

    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}
Status DataTypeDateTimeV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                               const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(column);
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    UInt64 val = 0;
    if (StringRef str(slice.data, slice.size);
        !read_datetime_v2_text_impl<UInt64>(val, str, _scale)) {
        return Status::InvalidArgument("parse date fail, string: '{}'", str.to_string());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::write_column_to_arrow(const IColumn& column,
                                                      const NullMap* null_map,
                                                      arrow::ArrayBuilder* array_builder,
                                                      int64_t start, int64_t end,
                                                      const cctz::time_zone& ctz) const {
    const auto& col_data = static_cast<const ColumnDateTimeV2&>(column).get_data();
    auto& timestamp_builder = assert_cast<arrow::TimestampBuilder&>(*array_builder);
    std::shared_ptr<arrow::TimestampType> timestamp_type =
            std::static_pointer_cast<arrow::TimestampType>(array_builder->type());
    const std::string& timezone = timestamp_type->timezone();
    const cctz::time_zone& real_ctz = timezone.empty() ? cctz::utc_time_zone() : ctz;
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(timestamp_builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
        } else {
            int64_t timestamp = 0;
            DateV2Value<DateTimeV2ValueType> datetime_val =
                    binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(col_data[i]);
            datetime_val.unix_timestamp(&timestamp, real_ctz);

            if (_scale > 3) {
                uint32_t microsecond = datetime_val.microsecond();
                timestamp = (timestamp * 1000000) + microsecond;
            } else if (_scale > 0) {
                uint32_t millisecond = datetime_val.microsecond() / 1000;
                timestamp = (timestamp * 1000) + millisecond;
            }
            RETURN_IF_ERROR(checkArrowStatus(timestamp_builder.Append(timestamp), column.get_name(),
                                             array_builder->type()->name()));
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::read_column_from_arrow(IColumn& column,
                                                       const arrow::Array* arrow_array,
                                                       int64_t start, int64_t end,
                                                       const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnDateTimeV2&>(column).get_data();
    int64_t divisor = 1;
    if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
        const auto* concrete_array = dynamic_cast<const arrow::TimestampArray*>(arrow_array);
        const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
        switch (type->unit()) {
        case arrow::TimeUnit::type::SECOND: {
            divisor = DIVISOR_FOR_SECOND;
            break;
        }
        case arrow::TimeUnit::type::MILLI: {
            divisor = DIVISOR_FOR_MILLI;
            break;
        }
        case arrow::TimeUnit::type::MICRO: {
            divisor = DIVISOR_FOR_MICRO;
            break;
        }
        case arrow::TimeUnit::type::NANO: {
            divisor = DIVISOR_FOR_NANO;
            break;
        }
        default: {
            LOG(WARNING) << "not support convert to datetimev2 from time_unit:" << type->unit();
            return Status::InvalidArgument("not support convert to datetimev2 from time_unit: {}",
                                           type->unit());
        }
        }
        for (auto value_i = start; value_i < end; ++value_i) {
            auto utc_epoch = static_cast<UInt64>(concrete_array->Value(value_i));

            DateV2Value<DateTimeV2ValueType> v;
            // convert second
            v.from_unixtime(utc_epoch / divisor, ctz);
            // get rest time
            // add 0 on the right to make it 6 digits. DateTimeV2Value microsecond is 6 digits,
            // the scale decides to keep the first few digits, so the valid digits should be kept at the front.
            // "2022-01-01 11:11:11.111", utc_epoch = 1641035471111, divisor = 1000, set_microsecond(111000)
            v.set_microsecond((utc_epoch % divisor) * DIVISOR_FOR_MICRO / divisor);
            col_data.emplace_back(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(v));
        }
    } else {
        LOG(WARNING) << "not support convert to datetimev2 from arrow type:"
                     << arrow_array->type()->id();
        return Status::InternalError("not support convert to datetimev2 from arrow type: {}",
                                     arrow_array->type()->id());
    }
    return Status::OK();
}

template <bool is_binary_format>
Status DataTypeDateTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                       MysqlRowBuffer<is_binary_format>& result,
                                                       int64_t row_idx, bool col_const,
                                                       const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnDateTimeV2&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    DateV2Value<DateTimeV2ValueType> date_val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(data[col_index]);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_vec_datetime(date_val, _scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<true>& row_buffer,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<false>& row_buffer,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateTimeV2SerDe::write_column_to_orc(const std::string& timezone,
                                                    const IColumn& column, const NullMap* null_map,
                                                    orc::ColumnVectorBatch* orc_col_batch,
                                                    int64_t start, int64_t end,
                                                    vectorized::Arena& arena) const {
    const auto& col_data = assert_cast<const ColumnDateTimeV2&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::TimestampVectorBatch*>(orc_col_batch);

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }

        int64_t timestamp = 0;
        DateV2Value<DateTimeV2ValueType> datetime_val =
                binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(col_data[row_id]);
        if (!datetime_val.unix_timestamp(&timestamp, timezone)) {
            return Status::InternalError("get unix timestamp error.");
        }

        cur_batch->data[row_id] = timestamp;
        cur_batch->nanoseconds[row_id] = datetime_val.microsecond() * micro_to_nano_second;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_fixed_json(
        IColumn& column, Slice& slice, uint64_t rows, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    if (rows < 1) [[unlikely]] {
        return Status::OK();
    }
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }

    DataTypeDateTimeV2SerDe::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

void DataTypeDateTimeV2SerDe::insert_column_last_value_multiple_times(IColumn& column,
                                                                      uint64_t times) const {
    if (times < 1) [[unlikely]] {
        return;
    }
    auto& col = assert_cast<ColumnDateTimeV2&>(column);
    auto sz = col.size();
    UInt64 val = col.get_element(sz - 1);
    col.insert_many_vals(val, times);
}

void DataTypeDateTimeV2SerDe::write_one_cell_to_binary(const IColumn& src_column,
                                                       ColumnString::Chars& chars,
                                                       int64_t row_num) const {
    const uint8_t type = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_DATETIMEV2);
    const auto& data_ref =
            assert_cast<const ColumnVector<TYPE_DATETIMEV2>&>(src_column).get_data_at(row_num);
    const uint8_t sc = static_cast<uint8_t>(_scale);

    const size_t old_size = chars.size();
    const size_t new_size = old_size + sizeof(uint8_t) + sizeof(uint8_t) + data_ref.size;
    chars.resize(new_size);
    memcpy(chars.data() + old_size, reinterpret_cast<const char*>(&type), sizeof(uint8_t));
    memcpy(chars.data() + old_size + sizeof(uint8_t), reinterpret_cast<const char*>(&sc),
           sizeof(uint8_t));
    memcpy(chars.data() + old_size + sizeof(uint8_t) + sizeof(uint8_t), data_ref.data,
           data_ref.size);
}

// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

// instantiation of template functions
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;
} // namespace doris::vectorized
