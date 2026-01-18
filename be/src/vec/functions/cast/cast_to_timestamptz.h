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

#include "cast_base.h"
#include "runtime/primitive_type.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_timestamptz.h"
#include "vec/functions/cast/cast_to_datetimev2_impl.hpp"
#include "vec/io/io_helper.h"
#include "vec/runtime/timestamptz_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

struct CastToTimstampTz {
    static inline bool from_string(const StringRef& from, TimestampTzValue& to,
                                   CastParameters& params, const cctz::time_zone* local_time_zone,
                                   uint32_t to_scale);
};

inline bool CastToTimstampTz::from_string(const StringRef& from, TimestampTzValue& to,
                                          CastParameters& params,
                                          const cctz::time_zone* local_time_zone,
                                          uint32_t to_scale) {
    return to.from_string(from, local_time_zone, params, to_scale);
}

template <CastModeType Mode>
class CastToImpl<Mode, DataTypeString, DataTypeTimeStampTz> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto& col_from = assert_cast<const DataTypeString::ColumnType&>(
                *block.get_by_position(arguments[0]).column);

        auto to_type = block.get_by_position(result).type;
        auto serde = remove_nullable(to_type)->get_serde();
        MutableColumnPtr column_to;

        DataTypeSerDe::FormatOptions options;
        options.timezone = &context->state()->timezone_obj();

        if constexpr (Mode == CastModeType::NonStrictMode) {
            auto to_nullable_type = make_nullable(to_type);
            column_to = to_nullable_type->create_column();
            auto& nullable_col_to = assert_cast<ColumnNullable&>(*column_to);
            RETURN_IF_ERROR(serde->from_string_batch(col_from, nullable_col_to, options));
        } else if constexpr (Mode == CastModeType::StrictMode) {
            column_to = to_type->create_column();
            RETURN_IF_ERROR(
                    serde->from_string_strict_mode_batch(col_from, *column_to, options, null_map));
        }

        block.get_by_position(result).column = std::move(column_to);
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::StrictMode, DataTypeDateTimeV2, DataTypeTimeStampTz>
        : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnDateTimeV2&>(*block.get_by_position(arguments[0]).column)
                        .get_data();

        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        const auto& local_time_zone = context->state()->timezone_obj();

        const auto dt_scale = block.get_by_position(arguments[0]).type->get_scale();
        const auto tz_scale = block.get_by_position(result).type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map && null_map[i]) {
                continue;
            }

            DateV2Value<DateTimeV2ValueType> from_dt {col_from[i]};
            TimestampTzValue tz_value;

            if (!tz_value.from_datetime(from_dt, local_time_zone, dt_scale, tz_scale)) {
                return Status::InternalError(
                        "can not cast from  datetime : {} to timestamptz in timezone : {}",
                        from_dt.to_string(), context->state()->timezone());
            }

            col_to_data[i] = tz_value;
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::NonStrictMode, DataTypeDateTimeV2, DataTypeTimeStampTz>
        : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type*) const override {
        const auto& col_from =
                assert_cast<const ColumnDateTimeV2&>(*block.get_by_position(arguments[0]).column)
                        .get_data();

        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        auto col_null = ColumnBool::create(input_rows_count, 0);
        auto& col_null_map = col_null->get_data();
        const auto& local_time_zone = context->state()->timezone_obj();

        const auto dt_scale = block.get_by_position(arguments[0]).type->get_scale();
        const auto tz_scale = block.get_by_position(result).type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            DateV2Value<DateTimeV2ValueType> from_dt {col_from[i]};
            TimestampTzValue tz_value {};

            if (tz_value.from_datetime(from_dt, local_time_zone, dt_scale, tz_scale)) {
                col_to_data[i] = tz_value;
            } else {
                col_null_map[i] = true;
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null));
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::StrictMode, DataTypeTimeStampTz, DataTypeTimeStampTz>
        : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnTimeStampTz&>(*block.get_by_position(arguments[0]).column)
                        .get_data();

        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        const auto& local_time_zone = context->state()->timezone_obj();

        const auto from_scale = block.get_by_position(arguments[0]).type->get_scale();
        const auto to_scale = block.get_by_position(result).type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map && null_map[i]) {
                continue;
            }

            const auto& from_tz = col_from[i];
            auto& to_tz = col_to_data[i];

            if (!transform_date_scale(to_scale, from_scale, to_tz, from_tz)) {
                return Status::InternalError(
                        "can not cast from  timestamptz : {} to timestamptz in timezone : {}",
                        TimestampTzValue {from_tz}.to_string(local_time_zone, from_scale),
                        context->state()->timezone());
            }
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::NonStrictMode, DataTypeTimeStampTz, DataTypeTimeStampTz>
        : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type*) const override {
        const auto& col_from =
                assert_cast<const ColumnTimeStampTz&>(*block.get_by_position(arguments[0]).column)
                        .get_data();

        auto col_to = ColumnTimeStampTz::create(input_rows_count);
        auto& col_to_data = col_to->get_data();
        auto col_null = ColumnBool::create(input_rows_count, 0);
        auto& col_null_map = col_null->get_data();

        const auto from_scale = block.get_by_position(arguments[0]).type->get_scale();
        const auto to_scale = block.get_by_position(result).type->get_scale();

        for (int i = 0; i < input_rows_count; ++i) {
            const auto& from_tz = col_from[i];
            auto& to_tz = col_to_data[i];

            if (!transform_date_scale(to_scale, from_scale, to_tz, from_tz)) {
                col_null_map[i] = true;
                to_tz = TimestampTzValue(MIN_DATETIME_V2);
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null));
        return Status::OK();
    }
};

namespace CastWrapper {
inline WrapperType create_timestamptz_wrapper(FunctionContext* context,
                                              const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_to_timestamptz;

    auto make_timestamptz_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (CastUtil::IsBaseCastFromType<FromDataType> ||
                      IsTimeStampTzType<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_to_timestamptz = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, DataTypeTimeStampTz>>();
            } else {
                cast_to_timestamptz =
                        std::make_shared<CastToImpl<CastModeType::NonStrictMode, FromDataType,
                                                    DataTypeTimeStampTz>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(),
                                           make_timestamptz_wrapper)) {
        return create_unsupport_wrapper(
                fmt::format("CAST AS timestamptz not supported {}", from_type->get_name()));
    }

    return [cast_to_timestamptz](FunctionContext* context, Block& block,
                                 const ColumnNumbers& arguments, uint32_t result,
                                 size_t input_rows_count,
                                 const NullMap::value_type* null_map = nullptr) {
        return cast_to_timestamptz->execute_impl(context, block, arguments, result,
                                                 input_rows_count, null_map);
    };
}

}; // namespace CastWrapper

} // namespace doris::vectorized