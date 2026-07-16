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

#include "core/data_type_serde/data_type_time_serde.h"

#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "core/value/time_value.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_to_time_impl.hpp"
#include "util/unaligned.h"

namespace doris {
namespace {

TimeValue::TimeType read_time_decoded_value(const DecodedColumnView& view, int64_t row) {
    int64_t micros = 0;
    if (view.value_kind == DecodedValueKind::INT32) {
        const auto* values = reinterpret_cast<const int32_t*>(view.values);
        micros = static_cast<int64_t>(values[row]) * 1000;
    } else {
        const auto* values = reinterpret_cast<const int64_t*>(view.values);
        micros = values[row];
        if (view.time_unit == DecodedTimeUnit::MILLIS) {
            micros *= 1000;
        } else if (view.time_unit == DecodedTimeUnit::NANOS) {
            micros /= 1000;
        }
    }
    const bool negative = micros < 0;
    const int64_t abs_micros = std::abs(micros);
    return TimeValue::make_time(
            abs_micros / TimeValue::ONE_HOUR_MICROSECONDS,
            (abs_micros % TimeValue::ONE_HOUR_MICROSECONDS) / TimeValue::ONE_MINUTE_MICROSECONDS,
            (abs_micros % TimeValue::ONE_MINUTE_MICROSECONDS) / TimeValue::ONE_SECOND_MICROSECONDS,
            abs_micros % TimeValue::ONE_SECOND_MICROSECONDS, negative);
}

class TimeV2ParquetConsumer final : public ParquetFixedValueConsumer {
public:
    TimeV2ParquetConsumer(IColumn& column, const ParquetDecodeContext& context)
            : _data(assert_cast<ColumnTimeV2&>(column).get_data()), _context(context) {}

    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        const size_t old_size = _data.size();
        _data.resize(old_size + num_values);
        for (size_t row = 0; row < num_values; ++row) {
            int64_t micros;
            if (_context.physical_type == ParquetPhysicalType::INT32) {
                DORIS_CHECK_EQ(value_width, sizeof(int32_t));
                micros = static_cast<int64_t>(
                                 unaligned_load<int32_t>(values + row * sizeof(int32_t))) *
                         1000;
            } else {
                DORIS_CHECK(_context.physical_type == ParquetPhysicalType::INT64);
                DORIS_CHECK_EQ(value_width, sizeof(int64_t));
                micros = unaligned_load<int64_t>(values + row * sizeof(int64_t));
                if (_context.time_unit == ParquetTimeUnit::MILLIS) {
                    micros *= 1000;
                } else if (_context.time_unit == ParquetTimeUnit::NANOS) {
                    micros /= 1000;
                }
            }
            const bool negative = micros < 0;
            const uint64_t abs_micros = negative ? uint64_t(-(micros + 1)) + 1 : uint64_t(micros);
            _data[old_size + row] =
                    TimeValue::make_time(abs_micros / TimeValue::ONE_HOUR_MICROSECONDS,
                                         (abs_micros % TimeValue::ONE_HOUR_MICROSECONDS) /
                                                 TimeValue::ONE_MINUTE_MICROSECONDS,
                                         (abs_micros % TimeValue::ONE_MINUTE_MICROSECONDS) /
                                                 TimeValue::ONE_SECOND_MICROSECONDS,
                                         abs_micros % TimeValue::ONE_SECOND_MICROSECONDS, negative);
        }
        return Status::OK();
    }

private:
    ColumnTimeV2::Container& _data;
    const ParquetDecodeContext& _context;
};

class RejectTimeV2BinaryConsumer final : public ParquetBinaryValueConsumer {
public:
    Status consume(const StringRef* values, size_t num_values) override {
        return Status::NotSupported("Binary Parquet values cannot be materialized as TIMEV2");
    }
};

} // namespace

Status DataTypeTimeV2SerDe::write_column_to_mysql_binary(const IColumn& column,
                                                         MysqlRowBinaryBuffer& result,
                                                         int64_t row_idx, bool col_const,
                                                         const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnTimeV2&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    if (UNLIKELY(0 != result.push_timev2(data[col_index], _scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    return Status::OK();
}

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
Status DataTypeTimeV2SerDe::from_string_batch(const ColumnString& col_str, ColumnNullable& col_res,
                                              const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(col_res.get_nested_column());
    auto& col_nullmap = col_res.get_null_map_column();
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
        CastToTimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(str, res, options.timezone,
                                                                         _scale, params);
        // only after we called something with `IS_STRICT = true`, params.status will be set
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(fmt::format("parse {} to time failed: ", str.to_string_view()));
            return params.status;
        }

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

Status DataTypeTimeV2SerDe::from_olap_string(const std::string& str, Field& field,
                                             const FormatOptions& options) const {
    CastParameters params {.status = Status::OK(), .is_strict = false};
    // set false to `is_strict`, it will not set error code cuz we dont need then speed up the process.
    // then we rely on return value to check success.
    // return value only represent OK or InvalidArgument for other error(like InternalError) in parser, MUST throw
    // Exception!
    TimeValue::TimeType res;
    if (!CastToTimeV2::from_string_non_strict_mode(StringRef(str), res, options.timezone, _scale,
                                                   params)) [[unlikely]] {
        return Status::InvalidArgument("parse timev2 fail, string: '{}'", str);
    }
    field = Field::create_field<TYPE_TIMEV2>(std::move(res));
    return Status::OK();
}

Status DataTypeTimeV2SerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                    const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = true};
    TimeValue::TimeType res;
    CastToTimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(str, res, options.timezone,
                                                                     _scale, params);
    // only after we called something with `IS_STRICT = true`, params.status will be set
    if (!params.status.ok()) [[unlikely]] {
        params.status.prepend(fmt::format("parse {} to time failed: ", str.to_string_view()));
        return params.status;
    }

    col_data.insert_value(res);
    return Status::OK();
}

Status DataTypeTimeV2SerDe::read_column_from_decoded_values(IColumn& column,
                                                            const DecodedColumnView& view) const {
    if (view.value_kind != DecodedValueKind::INT32 && view.value_kind != DecodedValueKind::INT64) {
        return decoded_column_view_handle_conversion_failure(
                column, view,
                Status::NotSupported("TIMEV2 decoded reader expects INT32 or INT64 source"));
    }
    if (view.values == nullptr && decoded_column_view_has_non_null_value(view)) {
        return Status::Corruption("Decoded value buffer is null for {}", column.get_name());
    }
    auto& data = assert_cast<ColumnTimeV2&>(column).get_data();
    for (int64_t row = 0; row < view.row_count; ++row) {
        if (decoded_column_view_row_is_null(view, row)) {
            data.push_back(TimeValue::TimeType());
            continue;
        }
        data.push_back(read_time_decoded_value(view, row));
    }
    return Status::OK();
}

Status DataTypeTimeV2SerDe::read_parquet_dictionary(IColumn& column, ParquetDecodeSource& source,
                                                    const ParquetDecodeContext& context) const {
    TimeV2ParquetConsumer consumer(column, context);
    RejectTimeV2BinaryConsumer binary_consumer;
    return source.decode_dictionary(consumer, binary_consumer);
}

Status DataTypeTimeV2SerDe::read_column_from_parquet(IColumn& column, ParquetDecodeSource& source,
                                                     const ParquetDecodeContext& context,
                                                     size_t num_values,
                                                     ParquetMaterializationState& state) const {
    if ((context.physical_type != ParquetPhysicalType::INT32 &&
         context.physical_type != ParquetPhysicalType::INT64) ||
        context.logical_type != ParquetLogicalType::TIME) {
        return Status::NotSupported("TIMEV2 expects Parquet TIME stored as INT32 or INT64");
    }
    TimeV2ParquetConsumer consumer(column, context);
    if (context.encoding != ParquetValueEncoding::DICTIONARY) {
        return source.decode_fixed_values(num_values, consumer);
    }
    if (state.dictionary_generation != source.dictionary_generation()) {
        state.typed_dictionary = column.clone_empty();
        RETURN_IF_ERROR(read_parquet_dictionary(*state.typed_dictionary, source, context));
        DORIS_CHECK_EQ(state.typed_dictionary->size(), source.dictionary_size());
        state.dictionary_generation = source.dictionary_generation();
    }
    RETURN_IF_ERROR(source.decode_dictionary_indices(num_values, &state.dictionary_indices));
    DORIS_CHECK_EQ(state.dictionary_indices.size(), num_values);
    column.insert_indices_from(*state.typed_dictionary, state.dictionary_indices.data(),
                               state.dictionary_indices.data() + num_values);
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeTimeV2SerDe::from_int_batch(const typename IntDataType::ColumnType& int_col,
                                           ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = target_col.get_null_map_column();
    col_data.resize(int_col.size());
    col_nullmap.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < int_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        if (CastToTimeV2::from_integer<DatelikeParseMode::NON_STRICT>(int_col.get_element(i), val,
                                                                      params)) [[likely]] {
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
Status DataTypeTimeV2SerDe::from_int_strict_mode_batch(
        const typename IntDataType::ColumnType& int_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(int_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < int_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        CastToTimeV2::from_integer<DatelikeParseMode::STRICT>(int_col.get_element(i), val, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(fmt::format("parse {} to time failed: ", int_col.get_element(i)));
            return params.status;
        }

        col_data.get_data()[i] = val;
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeTimeV2SerDe::from_float_batch(const typename FloatDataType::ColumnType& float_col,
                                             ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = target_col.get_null_map_column();
    col_data.resize(float_col.size());
    col_nullmap.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < float_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        if (CastToTimeV2::from_float<DatelikeParseMode::NON_STRICT>(float_col.get_data()[i], val,
                                                                    _scale, params)) [[likely]] {
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
Status DataTypeTimeV2SerDe::from_float_strict_mode_batch(
        const typename FloatDataType::ColumnType& float_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(float_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < float_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        CastToTimeV2::from_float<DatelikeParseMode::STRICT>(float_col.get_data()[i], val, _scale,
                                                            params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to time failed: ", float_col.get_data()[i]));
            return params.status;
        }

        col_data.get_data()[i] = val;
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeTimeV2SerDe::from_decimal_batch(
        const typename DecimalDataType::ColumnType& decimal_col, ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = target_col.get_null_map_column();
    col_data.resize(decimal_col.size());
    col_nullmap.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        if (CastToTimeV2::from_decimal<DatelikeParseMode::NON_STRICT>(
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
        const typename DecimalDataType::ColumnType& decimal_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(decimal_col.size());

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < decimal_col.size(); ++i) {
        TimeValue::TimeType val = 0;
        CastToTimeV2::from_decimal<DatelikeParseMode::STRICT>(
                decimal_col.get_intergral_part(i), decimal_col.get_fractional_part(i),
                decimal_col.get_scale(), val, _scale, params);
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {}.{} to time failed: ", decimal_col.get_intergral_part(i),
                                decimal_col.get_fractional_part(i)));
            return params.status;
        }

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

} // namespace doris
