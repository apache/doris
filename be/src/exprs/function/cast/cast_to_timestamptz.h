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

#include "core/assert_cast.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "exprs/function/cast/cast_to_timestamptz_impl.hpp"
#include "runtime/runtime_state.h"

namespace doris {

template <CastModeType Mode>
class CastToImpl<Mode, DataTypeString, DataTypeTimeStampTz> : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const DataTypeString::ColumnType&>(*source_column.column);

        auto to_type = target_column.type;
        auto serde = remove_nullable(to_type)->get_serde();
        MutableColumnPtr column_to;

        DataTypeSerDe::FormatOptions options;
        options.timezone = &state->timezone_obj();

        if constexpr (Mode == CastModeType::NonStrictMode) {
            auto to_nullable_type = make_nullable(to_type);
            column_to = to_nullable_type->create_column();
            auto& nullable_col_to = assert_cast<ColumnNullable&>(*column_to);
            RETURN_IF_ERROR(serde->from_string_batch(col_from, nullable_col_to, options));
        } else if constexpr (Mode == CastModeType::StrictMode) {
            column_to = to_type->create_column();
            RETURN_IF_ERROR(serde->from_string_strict_mode_batch(col_from, *column_to, options,
                                                                 parent_null_map));
        }

        target_column.column = std::move(column_to);
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::StrictMode, DataTypeDateTimeV2, DataTypeTimeStampTz>
        : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnDateTimeV2&>(*source_column.column).get_data();

        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        const auto& local_time_zone = state->timezone_obj();

        const auto dt_scale = source_column.type->get_scale();
        const auto tz_scale = target_column.type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            if (parent_null_map && parent_null_map[i]) {
                continue;
            }

            DateV2Value<DateTimeV2ValueType> from_dt {col_from[i]};
            TimestampTzValue tz_value;

            if (!tz_value.from_datetime(from_dt, local_time_zone, dt_scale, tz_scale)) {
                return Status::InvalidArgument(
                        "can not cast from  datetime : {} to timestamptz in timezone : {}",
                        from_dt.to_string(), state->timezone());
            }

            col_to_data[i] = tz_value;
        }
        target_column.column = std::move(col_to);
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::NonStrictMode, DataTypeDateTimeV2, DataTypeTimeStampTz>
        : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnDateTimeV2&>(*source_column.column).get_data();

        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        auto col_null = ColumnBool::create(input_rows_count, 0);
        auto& col_null_map = col_null->get_data();
        const auto& local_time_zone = state->timezone_obj();

        const auto dt_scale = source_column.type->get_scale();
        const auto tz_scale = target_column.type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            if (parent_null_map && parent_null_map[i]) {
                continue;
            }
            DateV2Value<DateTimeV2ValueType> from_dt {col_from[i]};
            TimestampTzValue tz_value {};

            if (tz_value.from_datetime(from_dt, local_time_zone, dt_scale, tz_scale)) {
                col_to_data[i] = tz_value;
            } else {
                col_null_map[i] = true;
            }
        }

        target_column.column = ColumnNullable::create(std::move(col_to), std::move(col_null));
        return Status::OK();
    }
};

template <CastModeType CastMode>
class CastToImpl<CastMode, DataTypeTimeStampNs, DataTypeTimeStampTz> : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnTimeStampNs&>(*source_column.column).get_data();
        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto col_null = ColumnBool::create(input_rows_count, 0);
        const auto& local_time_zone = state->timezone_obj();
        const auto target_scale = target_column.type->get_scale();

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (parent_null_map && parent_null_map[i]) {
                continue;
            }
            DateV2Value<DateTimeV2ValueType> rounded_datetime;
            TimestampTzValue target;
            const bool converted =
                    transform_date_scale(target_scale, TimeStampNsValue::FRACTIONAL_DIGITS,
                                         rounded_datetime, col_from[i]) &&
                    target.from_datetime(rounded_datetime, local_time_zone, target_scale,
                                         target_scale);
            if (!converted) {
                if constexpr (CastMode == CastModeType::StrictMode) {
                    return Status::InvalidArgument(
                            "can not cast TIMESTAMP_NS {} to timestamptz in timezone {}",
                            col_from[i].to_string(), state->timezone());
                }
                col_null->get_data()[i] = true;
                continue;
            }
            col_to->get_data()[i] = target;
        }

        if constexpr (CastMode == CastModeType::StrictMode) {
            target_column.column = std::move(col_to);
        } else {
            target_column.column = ColumnNullable::create(std::move(col_to), std::move(col_null));
        }
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::StrictMode, DataTypeTimeStampTz, DataTypeTimeStampTz>
        : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnTimeStampTz&>(*source_column.column).get_data();

        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto& col_to_data = col_to->get_data();

        const auto from_scale = source_column.type->get_scale();
        const auto to_scale = target_column.type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            if (parent_null_map && parent_null_map[i]) {
                continue;
            }

            const auto& from_tz = col_from[i];
            auto& to_tz = col_to_data[i];

            if (!transform_date_scale(to_scale, from_scale, to_tz, from_tz)) {
                // Error reporting must not format an overflowing timestamp in a session
                // timezone whose local year may itself be outside the supported range.
                return Status::InvalidArgument(
                        "can not cast from  timestamptz : {} UTC to timestamptz in timezone : {}",
                        from_tz.utc_dt().to_string(from_scale), state->timezone());
            }
        }
        target_column.column = std::move(col_to);
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::NonStrictMode, DataTypeTimeStampTz, DataTypeTimeStampTz>
        : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* parent_null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnTimeStampTz&>(*source_column.column).get_data();

        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        auto col_null = ColumnBool::create(input_rows_count, 0);
        auto& col_null_map = col_null->get_data();

        const auto from_scale = source_column.type->get_scale();
        const auto to_scale = target_column.type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            if (parent_null_map && parent_null_map[i]) {
                continue;
            }
            const auto& from_tz = col_from[i];
            auto& to_tz = col_to_data[i];

            if (!transform_date_scale(to_scale, from_scale, to_tz, from_tz)) {
                col_null_map[i] = true;
                to_tz = TimestampTzValue(MIN_DATETIME_V2);
            }
        }

        target_column.column = ColumnNullable::create(std::move(col_to), std::move(col_null));
        return Status::OK();
    }
};

} // namespace doris
