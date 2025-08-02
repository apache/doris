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

#include "data_type_time_serde.h"

#include "runtime/primitive_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/cast/cast_base.h"
#include "vec/functions/cast/cast_to_time_impl.hpp"
#include "vec/runtime/time_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status DataTypeTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int64_t row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
Status DataTypeTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer,
                                                  int64_t row_idx, bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
template <bool is_binary_format>
Status DataTypeTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int64_t row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnTimeV2&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    // _nesting_level >= 2 means this time is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_timev2(data[col_index], _scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
Status DataTypeTimeV2SerDe::from_string_batch(const ColumnString& col_str, ColumnNullable& col_res,
                                              const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_data_at(i);
        TimeValue::TimeType res;
        // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
        // then we rely on return value to check success.
        // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
        // Exception!
        if (!CastToTimeV2::from_string_non_strict_mode(str, res, options.timezone, _scale, params))
                [[unlikely]] {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = 0;
        } else {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = res;
        }
    }
    return Status::OK();
}

Status DataTypeTimeV2SerDe::from_string_strict_mode_batch(
        const ColumnString& col_str, IColumn& col_res, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnTimeV2&>(col_res);

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < row; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        auto str = col_str.get_data_at(i);
        TimeValue::TimeType res;
        CastToTimeV2::from_string_strict_mode<true>(str, res, options.timezone, _scale, params);
        // only after we called something with `IS_STRICT = true`, params.status will be set
        RETURN_IF_ERROR(params.status);

        col_data.get_data()[i] = res;
    }
    return Status::OK();
}

Status DataTypeTimeV2SerDe::from_string(StringRef& str, IColumn& column,
                                        const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(column);
    CastParameters params {.status = Status::OK(), .is_strict = false};
    // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
    // then we rely on return value to check success.
    // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
    // Exception!
    TimeValue::TimeType res;
    if (!CastToTimeV2::from_string_non_strict_mode(str, res, options.timezone, _scale, params))
            [[unlikely]] {
        return Status::InvalidArgument("parse timev2 fail, string: '{}'", str.to_string());
    }
    col_data.insert_value(res);
    return Status::OK();
}

Status DataTypeTimeV2SerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                    const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = true};
    TimeValue::TimeType res;
    CastToTimeV2::from_string_strict_mode<true>(str, res, options.timezone, _scale, params);
    // only after we called something with `IS_STRICT = true`, params.status will be set
    RETURN_IF_ERROR(params.status);
    col_data.insert_value(res);
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeTimeV2SerDe::from_int_batch(const IntDataType::ColumnType& int_col,
                                           ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(int_col.size());
    col_nullmap.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < int_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        if (CastToTimeV2::from_integer<false>(int_col.get_element(i), val, params)) [[likely]] {
            col_data.get_data()[i] = val;
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = 0;
        }
    }
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeTimeV2SerDe::from_int_strict_mode_batch(const IntDataType::ColumnType& int_col,
                                                       IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < int_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        CastToTimeV2::from_integer<true>(int_col.get_element(i), val, params);
        RETURN_IF_ERROR(params.status);

        col_data.get_data()[i] = val;
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeTimeV2SerDe::from_float_batch(const FloatDataType::ColumnType& float_col,
                                             ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(float_col.size());
    col_nullmap.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < float_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        if (CastToTimeV2::from_float<false>(float_col.get_data()[i], val, _scale, params))
                [[likely]] {
            col_data.get_data()[i] = val;
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = 0;
        }
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeTimeV2SerDe::from_float_strict_mode_batch(const FloatDataType::ColumnType& float_col,
                                                         IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < float_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        CastToTimeV2::from_float<true>(float_col.get_data()[i], val, _scale, params);
        RETURN_IF_ERROR(params.status);

        col_data.get_data()[i] = val;
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeTimeV2SerDe::from_decimal_batch(const DecimalDataType::ColumnType& decimal_col,
                                               ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(decimal_col.size());
    col_nullmap.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        if (CastToTimeV2::from_decimal<true>(
                    decimal_col.get_intergral_part(i), decimal_col.get_fractional_part(i),
                    decimal_col.get_scale(), val, _scale, params)) [[likely]] {
            col_data.get_data()[i] = val;
            col_nullmap.get_data()[i] = false;
        } else {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = 0;
        }
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch(
        const DecimalDataType::ColumnType& decimal_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        CastToTimeV2::from_decimal<true>(decimal_col.get_intergral_part(i),
                                         decimal_col.get_fractional_part(i),
                                         decimal_col.get_scale(), val, _scale, params);
        RETURN_IF_ERROR(params.status);

        col_data.get_data()[i] = val;
    }
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

// instantiation of template functions
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;

} // namespace doris::vectorized
