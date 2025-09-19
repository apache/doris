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

#include <type_traits>

#include "cast_base.h"
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column_nullable.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h" // IWYU pragma: keep
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsStringType<FromDataType> && IsDatelikeTypes<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
                block.get_by_position(arguments[0]).column.get());

        auto to_type = block.get_by_position(result).type;
        auto serde = remove_nullable(to_type)->get_serde();
        MutableColumnPtr column_to;

        DataTypeSerDe::FormatOptions options;
        options.timezone = &context->state()->timezone_obj();

        if constexpr (CastMode == CastModeType::StrictMode) {
            DCHECK(!to_type->is_nullable()) << "shouldn't be extra nullable here. if argument is "
                                               "null, should be processed in framework.";
            column_to = to_type->create_column();
            RETURN_IF_ERROR(
                    serde->from_string_strict_mode_batch(*col_from, *column_to, options, null_map));
        } else {
            auto to_nullable_type = make_nullable(to_type);
            column_to = to_nullable_type->create_column();
            auto& nullable_col_to = assert_cast<ColumnNullable&>(*column_to);
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, nullable_col_to, options));
        }

        block.get_by_position(result).column = std::move(column_to);
        return Status::OK();
    }
};

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(CastUtil::IsPureDigitType<FromDataType> && IsDatelikeTypes<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<typename FromDataType::ColumnType>(
                block.get_by_position(arguments[0]).column.get());

        auto to_type = block.get_by_position(result).type;
        auto concrete_serde = std::dynamic_pointer_cast<typename ToDataType::SerDeType>(
                remove_nullable(to_type)->get_serde());
        MutableColumnPtr column_to;

        if constexpr (CastMode == CastModeType::StrictMode) {
            DCHECK(!to_type->is_nullable()) << "shouldn't be extra nullable here. if argument is "
                                               "null, should be processed in framework.";
            column_to = to_type->create_column();

            // datelike types serde must have template functions for those types. but because of they need to be
            // template functions, so we cannot make them virtual. that's why we assert_cast `serde` before.
            if constexpr (IsDataTypeInt<FromDataType>) {
                RETURN_IF_ERROR(concrete_serde->template from_int_strict_mode_batch<FromDataType>(
                        *col_from, *column_to));
            } else if constexpr (IsDataTypeFloat<FromDataType>) {
                RETURN_IF_ERROR(concrete_serde->template from_float_strict_mode_batch<FromDataType>(
                        *col_from, *column_to));
            } else {
                static_assert(IsDataTypeDecimal<FromDataType>);
                RETURN_IF_ERROR(
                        concrete_serde->template from_decimal_strict_mode_batch<FromDataType>(
                                *col_from, *column_to));
            }
        } else {
            auto to_nullable_type = make_nullable(to_type);
            column_to = to_nullable_type->create_column();
            auto& nullable_col_to = assert_cast<ColumnNullable&>(*column_to);

            if constexpr (IsDataTypeInt<FromDataType>) {
                RETURN_IF_ERROR(concrete_serde->template from_int_batch<FromDataType>(
                        *col_from, nullable_col_to));
            } else if constexpr (IsDataTypeFloat<FromDataType>) {
                RETURN_IF_ERROR(concrete_serde->template from_float_batch<FromDataType>(
                        *col_from, nullable_col_to));
            } else {
                static_assert(IsDataTypeDecimal<FromDataType>);
                RETURN_IF_ERROR(concrete_serde->template from_decimal_batch<FromDataType>(
                        *col_from, nullable_col_to));
            }
        }

        block.get_by_position(result).column = std::move(column_to);
        return Status::OK();
    }
};

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDatelikeTypes<FromDataType> && IsDatelikeTypes<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        constexpr bool Nullable =
                std::is_same_v<FromDataType, ToDataType> &&
                (IsTimeV2Type<FromDataType> || IsDateTimeV2Type<FromDataType>)&&CastMode ==
                        CastModeType::NonStrictMode;

        const auto* col_from = check_and_get_column<typename FromDataType::ColumnType>(
                block.get_by_position(arguments[0]).column.get());
        auto col_to = ToDataType::ColumnType::create(input_rows_count);
        ColumnUInt8::MutablePtr col_nullmap;

        if constexpr (Nullable) {
            col_nullmap = ColumnUInt8::create(input_rows_count, 0);
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (IsDateType<FromDataType> && IsDateV2Type<ToDataType>) {
                // from Date to Date
                auto dtv1 = binary_cast<Int64, VecDateTimeValue>(col_from->get_data()[i]);
                dtv1.cast_to_date();
                col_to->get_data()[i] = dtv1.to_date_v2();
            } else if constexpr (IsDateV2Type<FromDataType> && IsDateType<ToDataType>) {
                DataTypeDateV2::cast_to_date(col_from->get_data()[i], col_to->get_data()[i]);
            } else if constexpr (IsDateTimeType<FromDataType> && IsDateType<ToDataType>) {
                // from Datetime to Date
                col_to->get_data()[i] = col_from->get_data()[i];
                DataTypeDate::cast_to_date(col_to->get_data()[i]);
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsDateType<ToDataType>) {
                DataTypeDateTimeV2::cast_to_date(col_from->get_data()[i], col_to->get_data()[i]);
            } else if constexpr (IsDateTimeType<FromDataType> && IsDateV2Type<ToDataType>) {
                auto dtmv1 = binary_cast<Int64, VecDateTimeValue>(col_from->get_data()[i]);
                col_to->get_data()[i] = dtmv1.to_date_v2();
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsDateV2Type<ToDataType>) {
                DataTypeDateTimeV2::cast_to_date_v2(col_from->get_data()[i], col_to->get_data()[i]);
            } else if constexpr (IsTimeV2Type<FromDataType> && IsDateType<ToDataType>) {
                // from Time to Date
                VecDateTimeValue dtv;
                dtv.cast_to_date();
                dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                                  context->state()->timezone_obj());

                auto time_value = col_from->get_data()[i];
                int32_t hour = TimeValue::hour(time_value);
                int32_t minute = TimeValue::minute(time_value);
                int32_t second = TimeValue::second(time_value);
                bool neg = TimeValue::sign(time_value) < 0;

                dtv.date_add_interval<TimeUnit::HOUR, false>(TimeInterval(HOUR, hour, neg));
                dtv.date_add_interval<TimeUnit::MINUTE, false>(TimeInterval(MINUTE, minute, neg));
                dtv.date_add_interval<TimeUnit::SECOND, false>(TimeInterval(SECOND, second, neg));

                col_to->get_data()[i] = binary_cast<VecDateTimeValue, Int64>(dtv);
            } else if constexpr (IsTimeV2Type<FromDataType> && IsDateV2Type<ToDataType>) {
                DateV2Value<DateV2ValueType> dtv;
                dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                                  context->state()->timezone_obj());

                auto time_value = col_from->get_data()[i];
                int32_t hour = TimeValue::hour(time_value);
                int32_t minute = TimeValue::minute(time_value);
                int32_t second = TimeValue::second(time_value);
                bool neg = TimeValue::sign(time_value) < 0;

                dtv.date_add_interval<TimeUnit::HOUR, false>(TimeInterval(HOUR, hour, neg));
                dtv.date_add_interval<TimeUnit::MINUTE, false>(TimeInterval(MINUTE, minute, neg));
                dtv.date_add_interval<TimeUnit::SECOND, false>(TimeInterval(SECOND, second, neg));

                col_to->get_data()[i] = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(dtv);
            } else if constexpr (IsDateType<FromDataType> && IsDateTimeType<ToDataType>) {
                // from Date to Datetime
                col_to->get_data()[i] = col_from->get_data()[i];
                DataTypeDateTime::cast_to_date_time(col_to->get_data()[i]);
            } else if constexpr (IsDateV2Type<FromDataType> && IsDateTimeType<ToDataType>) {
                DataTypeDateV2::cast_to_date_time(col_from->get_data()[i], col_to->get_data()[i]);
            } else if constexpr (IsDateType<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                auto dtv1 = binary_cast<Int64, VecDateTimeValue>(col_from->get_data()[i]);
                col_to->get_data()[i] = dtv1.to_datetime_v2();
            } else if constexpr (IsDateV2Type<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                DataTypeDateV2::cast_to_date_time_v2(col_from->get_data()[i],
                                                     col_to->get_data()[i]);
            } else if constexpr (IsTimeV2Type<FromDataType> && IsDateTimeType<ToDataType>) {
                // from Time to Datetime
                VecDateTimeValue dtv; // datetime by default
                dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                                  context->state()->timezone_obj());
                dtv.reset_time_part();

                auto time_value = col_from->get_data()[i];
                int32_t hour = TimeValue::hour(time_value);
                int32_t minute = TimeValue::minute(time_value);
                int32_t second = TimeValue::second(time_value);
                bool neg = TimeValue::sign(time_value) < 0;

                dtv.date_add_interval<TimeUnit::HOUR, false>(TimeInterval(HOUR, hour, neg));
                dtv.date_add_interval<TimeUnit::MINUTE, false>(TimeInterval(MINUTE, minute, neg));
                dtv.date_add_interval<TimeUnit::SECOND, false>(TimeInterval(SECOND, second, neg));

                col_to->get_data()[i] = binary_cast<VecDateTimeValue, Int64>(dtv);
            } else if constexpr (IsTimeV2Type<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                const auto* type = assert_cast<const DataTypeTimeV2*>(
                        block.get_by_position(arguments[0]).type.get());
                auto scale = type->get_scale();

                DateV2Value<DateTimeV2ValueType> dtmv2;
                dtmv2.from_unixtime(context->state()->timestamp_ms() / 1000,
                                    context->state()->nano_seconds(),
                                    context->state()->timezone_obj(), scale);
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

                col_to->get_data()[i] =
                        binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(dtmv2);
            } else if constexpr (IsDateTimeType<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                // from Datetime to Datetime
                auto dtmv1 = binary_cast<Int64, VecDateTimeValue>(col_from->get_data()[i]);
                col_to->get_data()[i] = dtmv1.to_datetime_v2();
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsDateTimeType<ToDataType>) {
                DataTypeDateTimeV2::cast_to_date_time(col_from->get_data()[i],
                                                      col_to->get_data()[i]);
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsDateTimeV2Type<ToDataType>) {
                const auto* type = assert_cast<const DataTypeDateTimeV2*>(
                        block.get_by_position(arguments[0]).type.get());
                auto scale = type->get_scale();

                const auto* to_type = assert_cast<const DataTypeDateTimeV2*>(
                        block.get_by_position(result).type.get());
                UInt32 to_scale = to_type->get_scale();

                if (to_scale >= scale) {
                    // nothing to do, just copy
                    col_to->get_data()[i] = col_from->get_data()[i];
                } else {
                    DateV2Value<DateTimeV2ValueType> dtmv2 =
                            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(
                                    col_from->get_data()[i]);
                    // e.g. scale reduce to 4, means we need to round the last 2 digits
                    // 999956: 56 > 100/2, then round up to 1000000
                    uint32_t microseconds = dtmv2.microsecond();
                    DCHECK(to_scale <= 6)
                            << "to_scale should be in range [0, 6], but got " << to_scale;
                    uint32_t divisor = (uint32_t)common::exp10_i64(6 - to_scale);
                    uint32_t remainder = microseconds % divisor;

                    if (remainder >= divisor / 2) { // need to round up
                        // do rounding up
                        uint32_t rounded_microseconds = ((microseconds / divisor) + 1) * divisor;
                        // need carry on
                        if (rounded_microseconds >= 1000000) {
                            static_cast<void>(dtmv2.set_time_unit<TimeUnit::MICROSECOND>(
                                    rounded_microseconds % 1000000));

                            bool overflow = !dtmv2.date_add_interval<TimeUnit::SECOND>(
                                    TimeInterval {TimeUnit::SECOND, 1, false});
                            if (overflow) {
                                if constexpr (CastMode == CastModeType::StrictMode) {
                                    return Status::InvalidArgument(
                                            "DatetimeV2 overflow when casting {} from {} to {}",
                                            type->to_string(*col_from, i), type->get_name(),
                                            to_type->get_name());
                                } else {
                                    col_nullmap->get_data()[i] = true;
                                    col_to->get_data()[i] =
                                            binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(
                                                    MIN_DATETIME_V2);
                                }
                            }
                        } else {
                            static_cast<void>(dtmv2.set_time_unit<TimeUnit::MICROSECOND>(
                                    rounded_microseconds));
                        }
                    } else {
                        // Round down (truncate) as before
                        static_cast<void>(dtmv2.set_time_unit<TimeUnit::MICROSECOND>(
                                (microseconds / divisor) * divisor));
                    }
                    col_to->get_data()[i] =
                            binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(dtmv2);
                }
            } else if constexpr (IsDateTimeV2Type<FromDataType> && IsTimeV2Type<ToDataType>) {
                // from Datetime to Time
                auto dtmv2 = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(
                        col_from->get_data()[i]);

                const auto* type = assert_cast<const DataTypeDateTimeV2*>(
                        block.get_by_position(arguments[0]).type.get());
                auto scale = type->get_scale();
                const auto* to_type = assert_cast<const DataTypeTimeV2*>(
                        block.get_by_position(result).type.get());
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
                    uint32_t divisor = (uint32_t)common::exp10_i64(6 - to_scale);
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
                auto dtmv1 = binary_cast<Int64, VecDateTimeValue>(col_from->get_data()[i]);
                auto time = TimeValue::limit_with_bound(
                        TimeValue::make_time(dtmv1.hour(), dtmv1.minute(), dtmv1.second()));
                col_to->get_data()[i] = time;
            }
        }

        if constexpr (Nullable) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_nullmap));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

namespace CastWrapper {

template <typename ToDataType> // must datelike type
WrapperType create_datelike_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_to_datelike;

    auto make_datelike_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (CastUtil::IsPureDigitType<FromDataType> || IsDatelikeTypes<FromDataType> ||
                      IsStringType<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_to_datelike = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, ToDataType>>();
            } else {
                cast_to_datelike = std::make_shared<
                        CastToImpl<CastModeType::NonStrictMode, FromDataType, ToDataType>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(),
                                           make_datelike_wrapper)) {
        return create_unsupport_wrapper(fmt::format(
                "CAST AS {} not supported {}", ToDataType {}.get_name(), from_type->get_name()));
    }

    return [cast_to_datelike](FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, const uint32_t result,
                              size_t input_rows_count,
                              const NullMap::value_type* null_map = nullptr) {
        return cast_to_datelike->execute_impl(context, block, arguments, result, input_rows_count,
                                              null_map);
    };
}
#include "common/compile_check_end.h"
}; // namespace CastWrapper
} // namespace doris::vectorized
