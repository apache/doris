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

#include "data_type_datev2_serde.h"

#include <arrow/builder.h>
#include <cctz/time_zone.h>
#include <fmt/core.h>

#include <cstdint>

#include "io/io_common.h"
#include "vec/columns/column_const.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/cast/cast_base.h"
#include "vec/functions/cast/cast_to_datev2_impl.hpp"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

// This number represents the number of days from 0000-01-01 to 1970-01-01
static const int32_t date_threshold = 719528;
#include "common/compile_check_begin.h"

Status DataTypeDateV2SerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                     int64_t end_idx, BufferWritable& bw,
                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDateV2SerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                       BufferWritable& bw,
                                                       FormatOptions& options) const {
    if (_nesting_level > 1) {
        bw.write('"');
    }
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    UInt32 int_val = assert_cast<const ColumnDateV2&>(*ptr).get_element(row_num);
    DateV2Value<DateV2ValueType> val = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(int_val);

    char buf[64];
    char* pos = val.to_string(buf);
    // DateTime to_string the end is /0
    bw.write(buf, pos - buf - 1);
    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeDateV2SerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeDateV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                           const FormatOptions& options) const {
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    auto& column_data = assert_cast<ColumnDateV2&>(column);
    UInt32 val = 0;
    if (StringRef str(slice.data, slice.size); !read_date_v2_text_impl<UInt32>(val, str)) {
        return Status::InvalidArgument("parse date fail, string: '{}'", str.to_string());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateV2SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                  arrow::ArrayBuilder* array_builder, int64_t start,
                                                  int64_t end, const cctz::time_zone& ctz) const {
    const auto& col_data = static_cast<const ColumnDateV2&>(column).get_data();
    auto& date32_builder = assert_cast<arrow::Date32Builder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        auto daynr = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(col_data[i]).daynr() -
                     date_threshold;
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(date32_builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
        } else {
            RETURN_IF_ERROR(
                    checkArrowStatus(date32_builder.Append(cast_set<int, int64_t, false>(daynr)),
                                     column.get_name(), array_builder->type()->name()));
        }
    }
    return Status::OK();
}

Status DataTypeDateV2SerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                   int64_t start, int64_t end,
                                                   const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnDateV2&>(column).get_data();
    const auto* concrete_array = dynamic_cast<const arrow::Date32Array*>(arrow_array);
    for (auto value_i = start; value_i < end; ++value_i) {
        DateV2Value<DateV2ValueType> v;
        v.get_date_from_daynr(concrete_array->Value(value_i) + date_threshold);
        col_data.emplace_back(binary_cast<DateV2Value<DateV2ValueType>, UInt32>(v));
    }
    return Status::OK();
}

template <bool is_binary_format>
Status DataTypeDateV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int64_t row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnDateV2&>(column).get_data();
    auto col_index = index_check_const(row_idx, col_const);
    DateV2Value<DateV2ValueType> date_val =
            binary_cast<UInt32, DateV2Value<DateV2ValueType>>(data[col_index]);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_vec_datetime(date_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeDateV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int64_t row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer,
                                                  int64_t row_idx, bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateV2SerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch,
                                                int64_t start, int64_t end,
                                                vectorized::Arena& arena) const {
    const auto& col_data = assert_cast<const ColumnDateV2&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::LongVectorBatch*>(orc_col_batch);
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }
        cur_batch->data[row_id] =
                binary_cast<UInt32, DateV2Value<DateV2ValueType>>(col_data[row_id]).daynr() -
                date_threshold;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeDateV2SerDe::deserialize_column_from_fixed_json(IColumn& column, Slice& slice,
                                                               uint64_t rows,
                                                               uint64_t* num_deserialized,
                                                               const FormatOptions& options) const {
    if (rows < 1) [[unlikely]] {
        return Status::OK();
    }
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }
    DataTypeDateV2SerDe::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

void DataTypeDateV2SerDe::insert_column_last_value_multiple_times(IColumn& column,
                                                                  uint64_t times) const {
    if (times < 1) [[unlikely]] {
        return;
    }
    auto& col = assert_cast<ColumnDateV2&>(column);
    auto sz = col.size();
    UInt32 val = col.get_element(sz - 1);

    col.insert_many_vals(val, times);
}

void DataTypeDateV2SerDe::write_one_cell_to_binary(const IColumn& src_column,
                                                   ColumnString::Chars& chars,
                                                   int64_t row_num) const {
    const uint8_t type = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_DATEV2);
    const auto& data_ref =
            assert_cast<const ColumnVector<TYPE_DATEV2>&>(src_column).get_data_at(row_num);

    const size_t old_size = chars.size();
    const size_t new_size = old_size + sizeof(uint8_t) + data_ref.size;
    chars.resize(new_size);

    memcpy(chars.data() + old_size, reinterpret_cast<const char*>(&type), sizeof(uint8_t));
    memcpy(chars.data() + old_size + sizeof(uint8_t), data_ref.data, data_ref.size);
}

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
Status DataTypeDateV2SerDe::from_string_batch(const ColumnString& col_str, ColumnNullable& col_res,
                                              const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnDateV2&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_data_at(i);
        DateV2Value<DateV2ValueType> res;
        // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
        // then we rely on return value to check success.
        // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
        // Exception!
        if (!CastToDateV2::from_string_non_strict_mode(str, res, options.timezone, params))
                [[unlikely]] {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(MIN_DATE_V2);
        } else {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(res);
        }
    }
    return Status::OK();
}

Status DataTypeDateV2SerDe::from_string_strict_mode_batch(
        const ColumnString& col_str, IColumn& col_res, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnDateV2&>(col_res);

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < row; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        auto str = col_str.get_data_at(i);
        DateV2Value<DateV2ValueType> res;
        CastToDateV2::from_string_strict_mode<true>(str, res, options.timezone, params);
        // only after we called something with `IS_STRICT = true`, params.status will be set
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(fmt::format("parse {} to date failed: ", str.to_string_view()));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(res);
    }
    return Status::OK();
}

Status DataTypeDateV2SerDe::from_string(StringRef& str, IColumn& column,
                                        const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnDateV2&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = false};

    DateV2Value<DateV2ValueType> res;
    // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
    // then we rely on return value to check success.
    // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
    // Exception!
    if (!CastToDateV2::from_string_non_strict_mode(str, res, options.timezone, params))
            [[unlikely]] {
        return Status::InvalidArgument("parse datev2 fail, string: '{}'", str.to_string());
    }
    col_data.insert_value(binary_cast<DateV2Value<DateV2ValueType>, UInt32>(res));
    return Status::OK();
}

Status DataTypeDateV2SerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                    const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnDateV2&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = true};

    DateV2Value<DateV2ValueType> res;
    CastToDateV2::from_string_strict_mode<true>(str, res, options.timezone, params);
    // only after we called something with `IS_STRICT = true`, params.status will be set
    if (!params.status.ok()) [[unlikely]] {
        params.status.prepend(fmt::format("parse {} to date failed: ", str.to_string_view()));
        return params.status;
    }

    col_data.insert_value(binary_cast<DateV2Value<DateV2ValueType>, UInt32>(res));
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeDateV2SerDe::from_int_batch(const IntDataType::ColumnType& int_col,
                                           ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(int_col.size());
    col_nullmap.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < int_col.size(); ++i) {
        DateV2Value<DateV2ValueType> val;
        if (CastToDateV2::from_integer<false>(int_col.get_element(i), val, params)) [[likely]] {
            col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(MIN_DATE_V2);
        }
    }
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeDateV2SerDe::from_int_strict_mode_batch(const IntDataType::ColumnType& int_col,
                                                       IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateV2&>(target_col);
    col_data.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < int_col.size(); ++i) {
        DateV2Value<DateV2ValueType> val;
        CastToDateV2::from_integer<true>(int_col.get_element(i), val, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(fmt::format("parse {} to date failed: ", int_col.get_element(i)));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(val);
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeDateV2SerDe::from_float_batch(const FloatDataType::ColumnType& float_col,
                                             ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(float_col.size());
    col_nullmap.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < float_col.size(); ++i) {
        DateV2Value<DateV2ValueType> val;
        if (CastToDateV2::from_float<false>(float_col.get_data()[i], val, params)) [[likely]] {
            col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(MIN_DATE_V2);
        }
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeDateV2SerDe::from_float_strict_mode_batch(const FloatDataType::ColumnType& float_col,
                                                         IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateV2&>(target_col);
    col_data.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < float_col.size(); ++i) {
        DateV2Value<DateV2ValueType> val;
        CastToDateV2::from_float<true>(float_col.get_data()[i], val, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to date failed: ", float_col.get_data()[i]));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(val);
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeDateV2SerDe::from_decimal_batch(const DecimalDataType::ColumnType& decimal_col,
                                               ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(decimal_col.size());
    col_nullmap.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        DateV2Value<DateV2ValueType> val;
        if (CastToDateV2::from_decimal<true>(decimal_col.get_intergral_part(i),
                                             decimal_col.get_scale(), val, params)) [[likely]] {
            col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(val);
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(MIN_DATE_V2);
        }
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeDateV2SerDe::from_decimal_strict_mode_batch(
        const DecimalDataType::ColumnType& decimal_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateV2&>(target_col);
    col_data.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        DateV2Value<DateV2ValueType> val;
        CastToDateV2::from_decimal<true>(decimal_col.get_intergral_part(i), decimal_col.get_scale(),
                                         val, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {}.{} to date failed: ", decimal_col.get_intergral_part(i),
                                decimal_col.get_fractional_part(i)));
            return params.status;
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(val);
    }
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

// instantiation of template functions
template Status DataTypeDateV2SerDe::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;

} // namespace doris::vectorized
