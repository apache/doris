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

#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <type_traits>

#include "common/status.h"
#include "core/binary_cast.hpp"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h" // IWYU pragma: keep
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/types.h"
#include "core/value/time_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "runtime/runtime_state.h"

namespace doris {
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsStringType<FromDataType> && IsDatelikeTypes<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t /*input_rows_count*/,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto* col_from =
                assert_cast<const DataTypeString::ColumnType*>(source_column.column.get());

        auto to_type = target_column.type;
        auto nested_to_type = remove_nullable(to_type);
        auto serde = nested_to_type->get_serde();

        DataTypeSerDe::FormatOptions options;
        options.timezone = &state->timezone_obj();

        if constexpr (CastMode == CastModeType::StrictMode) {
            MutableColumnPtr column_to = nested_to_type->create_column();
            // WON'T write nulls to the result column, just raise errors. null_map is only used to skip invalid rows
            RETURN_IF_ERROR(serde->from_string_strict_mode_batch(*col_from, *column_to, options,
                                                                 parent_null_map));
            target_column.column = std::move(column_to);
        } else {
            auto nullable_col_to = create_empty_nullable_column(nested_to_type);
            // may write nulls to nullable_col_to
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, *nullable_col_to, options));
            target_column.column = std::move(nullable_col_to);
        }

        return Status::OK();
    }
};

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(CastUtil::IsPureDigitType<FromDataType> && IsDatelikeTypes<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t /*input_rows_count*/,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto* col_from =
                assert_cast<const typename FromDataType::ColumnType*>(source_column.column.get());
        auto to_type = target_column.type;
        auto nested_to_type = remove_nullable(to_type);
        auto concrete_serde = std::dynamic_pointer_cast<typename ToDataType::SerDeType>(
                nested_to_type->get_serde());

        // datelike types serde must have template functions for those types. but because of they need to be
        // template functions, so we cannot make them virtual. that's why we assert_cast `serde` before.
        if constexpr (CastMode == CastModeType::StrictMode) {
            MutableColumnPtr column_to = nested_to_type->create_column();
            // WON'T write nulls to the result column, just raise errors. null_map is only used to skip invalid rows
            if constexpr (IsDataTypeInt<FromDataType>) {
                RETURN_IF_ERROR(concrete_serde->template from_int_strict_mode_batch<FromDataType>(
                        *col_from, *column_to, parent_null_map));
            } else if constexpr (IsDataTypeFloat<FromDataType>) {
                RETURN_IF_ERROR(concrete_serde->template from_float_strict_mode_batch<FromDataType>(
                        *col_from, *column_to, parent_null_map));
            } else {
                static_assert(IsDataTypeDecimal<FromDataType>);
                RETURN_IF_ERROR(
                        concrete_serde->template from_decimal_strict_mode_batch<FromDataType>(
                                *col_from, *column_to, parent_null_map));
            }
            target_column.column = std::move(column_to);
        } else {
            auto nullable_col_to = create_empty_nullable_column(nested_to_type);
            // may write nulls to nullable_col_to
            if constexpr (IsDataTypeInt<FromDataType>) {
                RETURN_IF_ERROR(concrete_serde->template from_int_batch<FromDataType>(
                        *col_from, *nullable_col_to));
            } else if constexpr (IsDataTypeFloat<FromDataType>) {
                RETURN_IF_ERROR(concrete_serde->template from_float_batch<FromDataType>(
                        *col_from, *nullable_col_to));
            } else {
                static_assert(IsDataTypeDecimal<FromDataType>);
                RETURN_IF_ERROR(concrete_serde->template from_decimal_batch<FromDataType>(
                        *col_from, *nullable_col_to));
            }
            target_column.column = std::move(nullable_col_to);
        }

        return Status::OK();
    }
};

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(std::is_same_v<FromDataType, DataTypeTimeStampNs> && IsDatelikeTypes<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from = assert_cast<const ColumnTimeStampNs&>(*source_column.column);
        auto col_to = ToDataType::ColumnType::create(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (parent_null_map && parent_null_map[i]) {
                continue;
            }

            const auto& source = col_from.get_data()[i];
            if constexpr (IsDateType<ToDataType>) {
                const auto civil = source.to_datetime();
                DataTypeDateTimeV2::cast_to_date(civil, col_to->get_data()[i]);
            } else if constexpr (IsDateV2Type<ToDataType>) {
                col_to->get_data()[i] = source.to_date();
            } else if constexpr (IsDateTimeType<ToDataType>) {
                const auto civil = source.to_datetime();
                DataTypeDateTimeV2::cast_to_date_time(civil, col_to->get_data()[i]);
            } else if constexpr (IsDateTimeV2Type<ToDataType>) {
                const auto to_scale = target_column.type->get_scale();
                const bool success =
                        transform_date_scale(to_scale, TimeStampNsValue::FRACTIONAL_DIGITS,
                                             col_to->get_data()[i], source);
                DORIS_CHECK(success);
            } else {
                static_assert(IsTimeV2Type<ToDataType>);
                const auto to_scale = target_column.type->get_scale();
                const int64_t seconds = source.time_part_to_seconds();
                uint32_t hour = static_cast<uint32_t>(seconds / 3600);
                uint32_t minute = static_cast<uint32_t>(seconds / 60 % 60);
                uint32_t second = static_cast<uint32_t>(seconds % 60);
                uint32_t nanoseconds = source.nanosecond();
                const auto divisor = static_cast<uint32_t>(common::exp10_i64(9 - to_scale));
                const uint32_t remainder = nanoseconds % divisor;
                nanoseconds = nanoseconds / divisor * divisor;
                if (remainder >= divisor / 2) {
                    nanoseconds += divisor;
                }
                uint32_t microseconds = nanoseconds / TimeStampNsValue::NANOS_PER_MICROSECOND;
                if (microseconds >= TimeValue::ONE_SECOND_MICROSECONDS) {
                    microseconds = 0;
                    if (++second == 60) {
                        second = 0;
                        if (++minute == 60) {
                            minute = 0;
                            ++hour;
                        }
                    }
                }
                col_to->get_data()[i] =
                        ((hour * 60 + minute) * 60 + second) * TimeValue::ONE_SECOND_MICROSECONDS +
                        microseconds;
            }
        }

        target_column.column = std::move(col_to);
        return Status::OK();
    }
};

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDatelikeTypes<FromDataType> && IsDatelikeTypes<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        constexpr bool Nullable = std::is_same_v<FromDataType, ToDataType> &&
                                  (IsTimeV2Type<FromDataType> || IsDateTimeV2Type<FromDataType>);

        const auto* col_from =
                assert_cast<const typename FromDataType::ColumnType*>(source_column.column.get());
        auto col_to = ToDataType::ColumnType::create(input_rows_count);
        ColumnUInt8::MutablePtr col_nullmap;

        if constexpr (Nullable) {
            col_nullmap = ColumnUInt8::create(input_rows_count, 0);
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (parent_null_map && parent_null_map[i]) {
                continue;
            }
            if constexpr (IsDateType<FromDataType> && IsDateV2Type<ToDataType>) {
                // from Date to Date
                auto dtv1 = col_from->get_data()[i];
                dtv1.cast_to_date();
                col_to->get_data()[i] =
                        binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(dtv1.to_date_v2());
            } else if constexpr (IsDateV2Type<FromDataType> && IsDateType<ToDataType>) {
                DataTypeDateV2::cast_to_date(col_from->get_data()[i], col_to->get_data()[i]);
            } else if constexpr (IsDateTimeType<FromDataType> && IsDateType<ToDataType>) {
                // from Datetime to Date
                col_to->get_data()[i] = col_from->get_data()[i];
                DataTypeDate::cast_to_date(col_to->get_data()[i]);
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsDateType<ToDataType>) {
                DataTypeDateTimeV2::cast_to_date(col_from->get_data()[i], col_to->get_data()[i]);
            } else if constexpr (IsDateTimeType<FromDataType> && IsDateV2Type<ToDataType>) {
                auto dtmv1 = col_from->get_data()[i];
                col_to->get_data()[i] =
                        binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(dtmv1.to_date_v2());
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsDateV2Type<ToDataType>) {
                DataTypeDateTimeV2::cast_to_date_v2(col_from->get_data()[i], col_to->get_data()[i]);
            } else if constexpr (IsTimeV2Type<FromDataType> && IsDateType<ToDataType>) {
                // from Time to Date
                VecDateTimeValue dtv;
                dtv.cast_to_date();
                dtv.from_unixtime(state->timestamp_ms() / 1000, state->timezone_obj());

                auto time_value = col_from->get_data()[i];
                int32_t hour = TimeValue::hour(time_value);
                int32_t minute = TimeValue::minute(time_value);
                int32_t second = TimeValue::second(time_value);
                bool neg = TimeValue::sign(time_value) < 0;

                dtv.date_add_interval<TimeUnit::HOUR, false>(TimeInterval(HOUR, hour, neg));
                dtv.date_add_interval<TimeUnit::MINUTE, false>(TimeInterval(MINUTE, minute, neg));
                dtv.date_add_interval<TimeUnit::SECOND, false>(TimeInterval(SECOND, second, neg));

                col_to->get_data()[i] = dtv;
            } else if constexpr (IsTimeV2Type<FromDataType> && IsDateV2Type<ToDataType>) {
                DateV2Value<DateV2ValueType> dtv;
                dtv.from_unixtime(state->timestamp_ms() / 1000, state->timezone_obj());

                auto time_value = col_from->get_data()[i];
                int32_t hour = TimeValue::hour(time_value);
                int32_t minute = TimeValue::minute(time_value);
                int32_t second = TimeValue::second(time_value);
                bool neg = TimeValue::sign(time_value) < 0;

                dtv.date_add_interval<TimeUnit::HOUR, false>(TimeInterval(HOUR, hour, neg));
                dtv.date_add_interval<TimeUnit::MINUTE, false>(TimeInterval(MINUTE, minute, neg));
                dtv.date_add_interval<TimeUnit::SECOND, false>(TimeInterval(SECOND, second, neg));

                col_to->get_data()[i] = dtv;
            } else if constexpr (IsDateType<FromDataType> && IsDateTimeType<ToDataType>) {
                // from Date to Datetime
                col_to->get_data()[i] = col_from->get_data()[i];
                DataTypeDateTime::cast_to_date_time(col_to->get_data()[i]);
            } else if constexpr (IsDateV2Type<FromDataType> && IsDateTimeType<ToDataType>) {
                DataTypeDateV2::cast_to_date_time(col_from->get_data()[i], col_to->get_data()[i]);
            } else if constexpr (IsDateType<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                auto dtv1 = col_from->get_data()[i];
                col_to->get_data()[i] = binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(
                        dtv1.to_datetime_v2());
            } else if constexpr (IsDateV2Type<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                DataTypeDateV2::cast_to_date_time_v2(col_from->get_data()[i],
                                                     col_to->get_data()[i]);
            } else if constexpr (IsTimeV2Type<FromDataType> && IsDateTimeType<ToDataType>) {
                // from Time to Datetime
                VecDateTimeValue dtv; // datetime by default
                dtv.from_unixtime(state->timestamp_ms() / 1000, state->timezone_obj());
                dtv.reset_time_part();

                auto time_value = col_from->get_data()[i];
                int32_t hour = TimeValue::hour(time_value);
                int32_t minute = TimeValue::minute(time_value);
                int32_t second = TimeValue::second(time_value);
                bool neg = TimeValue::sign(time_value) < 0;

                dtv.date_add_interval<TimeUnit::HOUR, false>(TimeInterval(HOUR, hour, neg));
                dtv.date_add_interval<TimeUnit::MINUTE, false>(TimeInterval(MINUTE, minute, neg));
                dtv.date_add_interval<TimeUnit::SECOND, false>(TimeInterval(SECOND, second, neg));

                col_to->get_data()[i] = dtv;
            } else if constexpr (IsTimeV2Type<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                const auto* type = assert_cast<const DataTypeTimeV2*>(source_column.type.get());
                auto scale = type->get_scale();

                DateV2Value<DateTimeV2ValueType> dtmv2;
                dtmv2.from_unixtime(state->timestamp_ms() / 1000, state->nano_seconds(),
                                    state->timezone_obj(), scale);
                dtmv2.reset_time_part();

                auto time_value = col_from->get_data()[i];
                int32_t hour = TimeValue::hour(time_value);
                int32_t minute = TimeValue::minute(time_value);
                int32_t second = TimeValue::second(time_value);
                int32_t microsecond = TimeValue::microsecond(time_value);
                bool neg = TimeValue::sign(time_value) < 0;

                dtmv2.date_add_interval<TimeUnit::HOUR, false>(TimeInterval(HOUR, hour, neg));
                dtmv2.date_add_interval<TimeUnit::MINUTE, false>(TimeInterval(MINUTE, minute, neg));
                dtmv2.date_add_interval<TimeUnit::SECOND, false>(TimeInterval(SECOND, second, neg));
                dtmv2.date_add_interval<TimeUnit::MICROSECOND, false>(
                        TimeInterval(MICROSECOND, microsecond, neg));

                col_to->get_data()[i] = dtmv2;
            } else if constexpr (IsDateTimeType<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                // from Datetime to Datetime
                auto dtmv1 = col_from->get_data()[i];
                col_to->get_data()[i] = binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(
                        dtmv1.to_datetime_v2());
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsDateTimeType<ToDataType>) {
                DataTypeDateTimeV2::cast_to_date_time(col_from->get_data()[i],
                                                      col_to->get_data()[i]);
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                const auto* type = assert_cast<const DataTypeDateTimeV2*>(source_column.type.get());
                auto scale = type->get_scale();

                const auto* to_type =
                        assert_cast<const DataTypeDateTimeV2*>(target_column.type.get());
                UInt32 to_scale = to_type->get_scale();

                bool success = transform_date_scale(to_scale, scale, col_to->get_data()[i],
                                                    col_from->get_data()[i]);
                if (!success) {
                    if constexpr (CastMode == CastModeType::StrictMode) {
                        auto format_options = DataTypeSerDe::get_default_format_options();
                        auto time_zone = cctz::utc_time_zone();
                        format_options.timezone =
                                state != nullptr ? &state->timezone_obj() : &time_zone;
                        return Status::InvalidArgument(
                                "DatetimeV2 overflow when casting {} from {} to {}",
                                type->to_string(*col_from, i, format_options), type->get_name(),
                                to_type->get_name());
                    } else {
                        col_nullmap->get_data()[i] = true;
                        //TODO: maybe we can remove all set operations on nested of null cell.
                        // the correctness should be keep by downstream user with replace_... or manually
                        // process null data if need.
                        col_to->get_data()[i] =
                                binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(
                                        MIN_DATETIME_V2);
                    }
                }

            } else if constexpr (IsTimeV2Type<FromDataType> && IsTimeV2Type<ToDataType>) {
                const auto* type = assert_cast<const DataTypeTimeV2*>(source_column.type.get());
                auto scale = type->get_scale();

                const auto* to_type = assert_cast<const DataTypeTimeV2*>(target_column.type.get());
                UInt32 to_scale = to_type->get_scale();

                if (to_scale >= scale) {
                    // nothing to do, just copy
                    col_to->get_data()[i] = col_from->get_data()[i];
                } else {
                    double time = col_from->get_data()[i];
                    auto sign = TimeValue::sign(time);
                    time = std::abs(time);
                    // e.g. scale reduce to 4, means we need to round the last 2 digits
                    // 999956: 56 > 100/2, then round up to 1000000
                    uint32_t microseconds = TimeValue::microsecond(time);
                    auto divisor = (uint32_t)common::exp10_i64(6 - to_scale);
                    uint32_t remainder = microseconds % divisor;

                    if (remainder >= divisor / 2) { // need to round up
                        // do rounding up
                        uint32_t rounded_microseconds = ((microseconds / divisor) + 1) * divisor;
                        // need carry on
                        if (rounded_microseconds >= TimeValue::ONE_SECOND_MICROSECONDS) {
                            DCHECK(rounded_microseconds == TimeValue::ONE_SECOND_MICROSECONDS);
                            time = ((int64_t)time / TimeValue::ONE_SECOND_MICROSECONDS + 1) *
                                   TimeValue::ONE_SECOND_MICROSECONDS;

                            // the input data must be valid, so max to '838:59:59.0'. this value won't carry on
                            // to second.
                            DCHECK(TimeValue::valid(time)) << col_from->get_data()[i];
                        } else {
                            time = TimeValue::reset_microsecond(time, rounded_microseconds);
                        }
                    } else {
                        // truncate
                        time = TimeValue::reset_microsecond(time, microseconds / divisor * divisor);
                    }
                    col_to->get_data()[i] = sign * time;
                }
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsTimeV2Type<ToDataType>) {
                // from Datetime to Time
                auto dtmv2 = col_from->get_data()[i];

                const auto* type = assert_cast<const DataTypeDateTimeV2*>(source_column.type.get());
                auto scale = type->get_scale();
                const auto* to_type = assert_cast<const DataTypeTimeV2*>(target_column.type.get());
                UInt32 to_scale = to_type->get_scale();

                uint32_t hour = dtmv2.hour();
                uint32_t minute = dtmv2.minute();
                uint32_t second = dtmv2.second();
                uint32_t microseconds = dtmv2.microsecond();
                if (to_scale < scale) { // need to round
                    // e.g. scale reduce to 4, means we need to round the last 2 digits
                    // 999956: 56 > 100/2, then round up to 1000000
                    DCHECK(to_scale <= 6)
                            << "to_scale should be in range [0, 6], but got " << to_scale;
                    auto divisor = (uint32_t)common::exp10_i64(6 - to_scale);
                    uint32_t remainder = microseconds % divisor;
                    microseconds = (microseconds / divisor) * divisor;
                    if (remainder >= divisor / 2) {
                        // do rounding up
                        microseconds += divisor;
                    }
                }

                // carry on if microseconds >= 1000000
                if (microseconds >= 1000000) {
                    microseconds -= 1000000;
                    second += 1;
                    if (second >= 60) {
                        second -= 60;
                        minute += 1;
                        if (minute >= 60) {
                            minute -= 60;
                            hour += 1;
                        }
                    }
                }

                auto time = TimeValue::limit_with_bound(
                        TimeValue::make_time(hour, minute, second, microseconds));
                col_to->get_data()[i] = time;
            } else if constexpr (IsDateTimeType<FromDataType> && IsTimeV2Type<ToDataType>) {
                auto dtmv1 = col_from->get_data()[i];
                auto time = TimeValue::limit_with_bound(
                        TimeValue::make_time(dtmv1.hour(), dtmv1.minute(), dtmv1.second()));
                col_to->get_data()[i] = time;
            }
        }

        if constexpr (Nullable) {
            target_column.column =
                    ColumnNullable::create(std::move(col_to), std::move(col_nullmap));
        } else {
            target_column.column = std::move(col_to);
        }
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::StrictMode, DataTypeTimeStampTz, DataTypeDateTimeV2>
        : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnTimeStampTz&>(*source_column.column).get_data();

        auto col_to = ColumnDateTimeV2::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        const auto& local_time_zone = state->timezone_obj();

        const auto tz_scale = source_column.type->get_scale();
        const auto dt_scale = target_column.type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            if (parent_null_map && parent_null_map[i]) {
                continue;
            }
            TimestampTzValue from_tz {col_from[i]};
            DateV2Value<DateTimeV2ValueType> dt;
            if (!from_tz.to_datetime(dt, local_time_zone, dt_scale, tz_scale)) {
                // The failed local conversion may also be unformattable. Render the stored
                // UTC fields so reporting the cast error cannot throw a second exception.
                return Status::InvalidArgument(
                        "can not cast from  timestamptz : {} UTC to datetime in timezone : {}",
                        from_tz.utc_dt().to_string(tz_scale), state->timezone());
            }
            col_to_data[i] = dt.to_date_int_val();
        }

        target_column.column = std::move(col_to);
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::NonStrictMode, DataTypeTimeStampTz, DataTypeDateTimeV2>
        : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnTimeStampTz&>(*source_column.column).get_data();

        auto col_to = ColumnDateTimeV2::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        auto col_null = ColumnBool::create(input_rows_count, 0);
        auto& col_null_map = col_null->get_data();
        const auto& local_time_zone = state->timezone_obj();

        const auto tz_scale = source_column.type->get_scale();
        const auto dt_scale = target_column.type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            if (parent_null_map != nullptr && parent_null_map[i] != 0) {
                continue;
            }
            TimestampTzValue from_tz {col_from[i]};
            DateV2Value<DateTimeV2ValueType> dt;
            if (from_tz.to_datetime(dt, local_time_zone, dt_scale, tz_scale)) {
                col_to_data[i] = dt;
            } else {
                col_null_map[i] = 1;
                col_to_data[i] = MIN_DATETIME_V2;
            }
        }
        target_column.column = ColumnNullable::create(std::move(col_to), std::move(col_null));
        return Status::OK();
    }
};

} // namespace doris
