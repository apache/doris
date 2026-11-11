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

#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/preprocessor/repetition/repeat.hpp>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#if !defined(__APPLE__)
#include <experimental/bits/simd.h>
#endif
#include <map>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compiler_util.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/pod_array_fwd.h"
#include "core/types.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/util.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/datetime_errors.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "util/timezone_utils.h"

namespace doris {
class FunctionContext;
} // namespace doris

// FIXME: This file contains widespread UB due to unsafe type-punning casts.
//        These must be properly refactored to eliminate reliance on reinterpret-style behavior.
//
// Temporarily suppress GCC 15+ warnings on user-defined type casts to allow build to proceed.
#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-user-defined"
#endif

namespace doris {
struct DayCeil;
struct DayFloor;
struct HourCeil;
struct HourFloor;
struct MinuteCeil;
struct MinuteFloor;
struct MonthCeil;
struct MonthFloor;
struct SecondCeil;
struct SecondFloor;
struct WeekCeil;
struct WeekFloor;
struct YearCeil;
struct YearFloor;

#if (defined(FLOOR) || defined(CEIL))
#error "FLOOR or CEIL is already defined"
#else
#define FLOOR 0
#define CEIL 1
#endif

// UTC to local time conversion - only depends on DateValueType, not all template params
template <typename DateValueType>
void convert_utc_to_local_impl(const DateValueType& utc_val, DateValueType& local_val,
                               const cctz::time_zone& tz) {
    cctz::time_point<cctz::sys_seconds> utc_tp =
            cctz::convert(cctz::civil_second(utc_val.year(), utc_val.month(), utc_val.day(),
                                             utc_val.hour(), utc_val.minute(), utc_val.second()),
                          cctz::utc_time_zone());
    auto local_cs = cctz::convert(utc_tp, tz);
    local_val.unchecked_set_time(
            static_cast<uint16_t>(local_cs.year()), static_cast<uint8_t>(local_cs.month()),
            static_cast<uint8_t>(local_cs.day()), static_cast<uint8_t>(local_cs.hour()),
            static_cast<uint8_t>(local_cs.minute()), static_cast<uint8_t>(local_cs.second()),
            utc_val.microsecond());
}

namespace {

constexpr int64_t GREGORIAN_CYCLE_SECONDS = 146097LL * 24 * 3600;

bool previous_rounding_transition(const cctz::time_zone& tz, cctz::time_point<cctz::seconds> cursor,
                                  cctz::time_point<cctz::seconds> last_stored, bool recurring,
                                  cctz::time_zone::civil_transition* transition) {
    if (!recurring || cursor <= last_stored) {
        return tz.prev_transition(cursor, transition);
    }
    // CCTZ stores 401 extra years for recurring rules. Its civil conversion uses
    // a 400-year cycle, but prev_transition does not. Apply that cycle here too.
    // For example, Lord Howe's 2824 gap uses the same calendar as its 2424 gap.
    const auto seconds_after_last = (cursor - last_stored).count();
    const auto shift = cctz::seconds(((seconds_after_last - 1) / GREGORIAN_CYCLE_SECONDS + 1) *
                                     GREGORIAN_CYCLE_SECONDS);
    const bool found = tz.prev_transition(cursor - shift, transition);
    DCHECK(found); // The 401-year extension contains the full projected cycle.
    const auto instant = tz.lookup(transition->to).trans + shift;
    transition->from = tz.lookup(instant - cctz::seconds(1)).cs + 1;
    transition->to = tz.lookup(instant).cs;
    return true;
}

// The recurrence probe is shared by all search intervals in a Block. Each interval
// includes its preceding transition because a gap can shift a boundary into it.
class RoundingGapScanner {
public:
    RoundingGapScanner(const cctz::time_zone& timezone, int64_t upper) : _timezone(timezone) {
        cctz::time_zone::civil_transition last;
        _has_transitions = timezone.prev_transition(
                cctz::time_point<cctz::seconds>(cctz::seconds(upper + 1)), &last);
        if (!_has_transitions) {
            return; // A fixed-offset zone, or an interval before the first transition.
        }
        _last_stored = timezone.lookup(last.to).trans;
        cctz::time_zone::civil_transition next;
        const auto repeated = _last_stored + cctz::seconds(GREGORIAN_CYCLE_SECONDS);
        _recurring = !timezone.next_transition(_last_stored, &next) &&
                     timezone.lookup(repeated - cctz::seconds(1)).offset !=
                             timezone.lookup(repeated).offset;
    }

    template <typename Visitor>
    void for_each_gap(int64_t lower, int64_t upper, Visitor&& visit) const {
        if (!_has_transitions) {
            return;
        }
        auto cursor = cctz::time_point<cctz::seconds>(cctz::seconds(upper + 1));
        cctz::time_zone::civil_transition transition;
        while (previous_rounding_transition(_timezone, cursor, _last_stored, _recurring,
                                            &transition)) {
            const auto instant = _timezone.lookup(transition.to).trans;
            const auto gap_seconds = transition.to - transition.from;
            if (gap_seconds > 0 && instant.time_since_epoch().count() + gap_seconds > lower) {
                visit(transition);
            }
            if (instant.time_since_epoch().count() <= lower) {
                break;
            }
            cursor = instant - cctz::seconds(1);
        }
    }

private:
    const cctz::time_zone& _timezone;
    cctz::time_point<cctz::seconds> _last_stored;
    bool _has_transitions = false;
    bool _recurring = false;
};

} // namespace

// Base class: non-template overrides shared by all instantiations
class FunctionDateTimeFloorCeilBase : public IFunction {
public:
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }
};

// Forward declaration of core computation struct (only depends on Flag + PType)
template <typename Flag, PrimitiveType PType>
struct DateTimeFloorCeilCore;

template <typename Flag, PrimitiveType PType, int ArgNum, bool UseDelta = false>
class FunctionDateTimeFloorCeil : public FunctionDateTimeFloorCeilBase {
public:
    using DateType = typename PrimitiveTypeTraits<PType>::DataType;
    using DateValueType = typename PrimitiveTypeTraits<PType>::CppType;
    using DeltaDataType = DataTypeInt32;
    using Core = DateTimeFloorCeilCore<Flag, PType>;
    // return date type = DateType
    static constexpr auto name = Flag::name;

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeFloorCeil>(); }

    String get_name() const override { return name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return have_nullable(arguments) ? make_nullable(std::make_shared<DateType>())
                                        : std::make_shared<DateType>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (ArgNum == 1) {
            return {std::make_shared<DateType>()};
        } else if constexpr (ArgNum == 2) {
            if constexpr (UseDelta) {
                return {std::make_shared<DateType>(), std::make_shared<DeltaDataType>()};
            } else {
                return {std::make_shared<DateType>(), std::make_shared<DateType>()};
            }
        }
        // 3 args. both delta and origin.
        return {std::make_shared<DateType>(), std::make_shared<DeltaDataType>(),
                std::make_shared<DateType>()};
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        // Handle null map manually - update result null map from input null maps upfront
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);
        NullMap& result_null_map = result_null_map_column->get_data();

        ColumnPtr argument_columns[3];
        bool col_const[3];

        // Update result null map from all input null maps using standard approach
        for (int i = 0; i < arguments.size(); ++i) {
            ColumnPtr& col = block.get_by_position(arguments[i]).column;
            col_const[i] = is_column_const(*col);
            const NullMap* null_map = VectorizedUtils::get_null_map(col);
            if (null_map) {
                VectorizedUtils::update_null_map(result_null_map, *null_map, col_const[i]);
            }
        }

        // A constant period is validated before the vectorized loop below. Return early when every
        // row is already NULL so NULL propagation takes precedence over period validation.
        if (input_rows_count > 0 && std::all_of(result_null_map.begin(), result_null_map.end(),
                                                [](uint8_t is_null) { return is_null != 0; })) {
            auto col_to = ColumnVector<PType>::create();
            col_to->resize(input_rows_count);
            block.replace_by_position(
                    result,
                    ColumnNullable::create(std::move(col_to), std::move(result_null_map_column)));
            return Status::OK();
        }

        // Extract nested columns from const(nullable) wrappers
        argument_columns[0] = col_const[0] ? static_cast<const ColumnConst&>(
                                                     *block.get_by_position(arguments[0]).column)
                                                     .convert_to_full_column()
                                           : block.get_by_position(arguments[0]).column;
        argument_columns[0] = remove_nullable(argument_columns[0]);
        if constexpr (ArgNum == 3) {
            default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block,
                                                 arguments);
            argument_columns[1] = remove_nullable(argument_columns[1]);
            argument_columns[2] = remove_nullable(argument_columns[2]);
        } else if constexpr (ArgNum == 2) {
            default_preprocess_parameter_columns(argument_columns, col_const, {1}, block,
                                                 arguments);
            argument_columns[1] = remove_nullable(argument_columns[1]);
        } else {
            static_assert(ArgNum == 1);
        }

        const auto* sources = assert_cast<const ColumnVector<PType>*>(
                argument_columns[0].get()); // datetime or date column
        auto col_to = ColumnVector<PType>::create();
        col_to->resize(input_rows_count);

        if constexpr (ArgNum == 1) {
            Core::vector(sources->get_data(), col_to->get_data(), result_null_map, context);
        } else if constexpr (ArgNum == 2) {
            const IColumn& delta_column = *argument_columns[1];
            if (col_const[1]) {
                if (remove_nullable(block.get_by_position(arguments[1]).type)
                            ->get_primitive_type() == PrimitiveType::TYPE_INT) {
                    // time_round(datetime,const(period))
                    Int32 period = (*argument_columns[1])[0].get<TYPE_INT>();
                    bool period_is_null = block.get_by_position(arguments[1]).type->is_nullable() &&
                                          block.get_by_position(arguments[1]).column->is_null_at(0);
                    if (period < 1 && !period_is_null) [[unlikely]] {
                        throw_out_of_bound_int(Flag::name, period);
                    }
                    Core::vector_const_period(sources->get_data(), period, col_to->get_data(),
                                              result_null_map, context);
                } else {
                    // time_round(datetime, const(origin))
                    Core::vector_const_anchor(sources->get_data(),
                                              (*argument_columns[1])[0].get<PType>(),
                                              col_to->get_data(), result_null_map, context);
                }
            } else {
                if (const auto* delta_vec_column0 =
                            check_and_get_column<ColumnVector<PType>>(delta_column)) {
                    // time_round(datetime, origin)
                    Core::vector_vector_anchor(sources->get_data(), delta_vec_column0->get_data(),
                                               col_to->get_data(), result_null_map, context);
                } else {
                    const auto* delta_vec_column1 = assert_cast<const ColumnInt32*>(&delta_column);
                    // time_round(datetime, period)
                    Core::vector_vector_period(sources->get_data(), delta_vec_column1->get_data(),
                                               col_to->get_data(), result_null_map, context);
                }
            }
        } else { // 3 arg, time_round(datetime, period, origin)
            if (col_const[1] && col_const[2]) {
                // time_round(datetime, const(period), const(origin))
                Int32 period = (*argument_columns[1])[0].get<TYPE_INT>();
                auto origin = (*argument_columns[2])[0].get<PType>();
                bool period_is_null = block.get_by_position(arguments[1]).type->is_nullable() &&
                                      block.get_by_position(arguments[1]).column->is_null_at(0);
                if (period < 1 && !period_is_null) [[unlikely]] {
                    throw_out_of_bound_int(Flag::name, period);
                }
                Core::vector_const_const(sources->get_data(), period, origin, col_to->get_data(),
                                         result_null_map, context);
            } else if (col_const[1] && !col_const[2]) {
                const auto* arg2_column =
                        assert_cast<const ColumnVector<PType>*>(argument_columns[2].get());
                // time_round(datetime, const(period), origin)
                Int32 period = (*argument_columns[1])[0].get<TYPE_INT>();
                bool period_is_null = block.get_by_position(arguments[1]).type->is_nullable() &&
                                      block.get_by_position(arguments[1]).column->is_null_at(0);
                if (period < 1 && !period_is_null) [[unlikely]] {
                    throw_out_of_bound_int(Flag::name, period);
                }
                Core::vector_const_vector(sources->get_data(), period, arg2_column->get_data(),
                                          col_to->get_data(), result_null_map, context);
            } else if (!col_const[1] && col_const[2]) {
                const auto* arg1_column =
                        assert_cast<const ColumnInt32*>(argument_columns[1].get());
                // time_round(datetime, period, const(origin))
                Core::vector_vector_const(sources->get_data(), arg1_column->get_data(),
                                          (*argument_columns[2])[0].get<PType>(),
                                          col_to->get_data(), result_null_map, context);
            } else {
                const auto* arg1_column =
                        assert_cast<const ColumnInt32*>(argument_columns[1].get());
                const auto* arg2_column =
                        assert_cast<const ColumnVector<PType>*>(argument_columns[2].get());
                // time_round(datetime, period, origin)
                Core::vector_vector_vector(sources->get_data(), arg1_column->get_data(),
                                           arg2_column->get_data(), col_to->get_data(),
                                           result_null_map, context);
            }
        }

        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(
                    result,
                    ColumnNullable::create(std::move(col_to), std::move(result_null_map_column)));
        } else {
            block.replace_by_position(result, std::move(col_to));
        }

        return Status::OK();
    }
};

// Core computation struct - only depends on Flag + PType (not ArgNum/UseDelta).
// This reduces template instantiations from 192 to 48 for all heavy computation functions.
template <typename Flag, PrimitiveType PType>
struct DateTimeFloorCeilCore {
    using DateValueType = typename PrimitiveTypeTraits<PType>::CppType;
    using LocalDateTime = DateV2Value<DateTimeV2ValueType>;
    static constexpr bool need_tz_conversion =
            std::is_same_v<DateValueType, TimestampTzValue> &&
            (Flag::Unit == YEAR || Flag::Unit == QUARTER || Flag::Unit == MONTH ||
             Flag::Unit == WEEK || Flag::Unit == DAY || Flag::Unit == HOUR || Flag::Unit == MINUTE);

    static void vector(const PaddedPODArray<DateValueType>& dates,
                       PaddedPODArray<DateValueType>& res, const NullMap& result_null_map,
                       FunctionContext* context) {
        if constexpr (need_tz_conversion) {
            if (auto failed = vector_timestamptz_const(dates, 1, TimestampTzValue::FIRST_DAY, res,
                                                       result_null_map, context)) {
                throw_out_of_bound_one_date<DateValueType>(Flag::name, dates[*failed]);
            }
            return;
        }

        for (int i = 0; i < dates.size(); ++i) {
            if (result_null_map[i]) {
                continue;
            }
            if (!time_round_reinterpret_two_args(dates[i], 1, res[i], context)) {
                throw_out_of_bound_one_date<DateValueType>(Flag::name, dates[i]);
            }
        }
    }

    static void vector_const_anchor(const PaddedPODArray<DateValueType>& dates,
                                    DateValueType origin_date, PaddedPODArray<DateValueType>& res,
                                    const NullMap& result_null_map, FunctionContext* context) {
        if constexpr (need_tz_conversion) {
            if (auto failed = vector_timestamptz_const(dates, 1, origin_date, res, result_null_map,
                                                       context)) {
                throw_out_of_bound_date_date<DateValueType>(Flag::name, dates[*failed],
                                                            origin_date);
            }
            return;
        }

        for (int i = 0; i < dates.size(); ++i) {
            if (result_null_map[i]) {
                continue;
            }
            if (!time_round_reinterpret_three_args(dates[i], 1, origin_date, res[i], context)) {
                throw_out_of_bound_date_date<DateValueType>(Flag::name, dates[i], origin_date);
            }
        }
    }

    static void vector_const_period(const PaddedPODArray<DateValueType>& dates, Int32 period,
                                    PaddedPODArray<DateValueType>& res,
                                    const NullMap& result_null_map, FunctionContext* context) {
        if constexpr (need_tz_conversion) {
            if (auto failed = vector_timestamptz_const(dates, period, TimestampTzValue::FIRST_DAY,
                                                       res, result_null_map, context)) {
                throw_out_of_bound_date_int<DateValueType>(Flag::name, dates[*failed], period);
            }
            return;
        }

        // expand codes for const input periods
#define EXPAND_CODE_FOR_CONST_INPUT(X)                                                    \
    case X: {                                                                             \
        for (int i = 0; i < dates.size(); ++i) {                                          \
            if (result_null_map[i]) {                                                     \
                continue;                                                                 \
            }                                                                             \
            if (!time_round_reinterpret_two_args<X>(dates[i], period, res[i], context)) { \
                throw_out_of_bound_date_int<DateValueType>(Flag::name, dates[i], period); \
            }                                                                             \
        }                                                                                 \
        return;                                                                           \
    }
#define EXPANDER(z, n, text) EXPAND_CODE_FOR_CONST_INPUT(n)
        switch (period) {
            // expand for some constant period
            BOOST_PP_REPEAT(12, EXPANDER, ~)
        default:
            for (int i = 0; i < dates.size(); ++i) {
                if (result_null_map[i]) {
                    continue;
                }
                if (!time_round_reinterpret_two_args(dates[i], period, res[i], context)) {
                    throw_out_of_bound_date_int<DateValueType>(Flag::name, dates[i], period);
                }
            }
        }
#undef EXPAND_CODE_FOR_CONST_INPUT
#undef EXPANDER
    }

    static void vector_const_const(const PaddedPODArray<DateValueType>& dates, const Int32 period,
                                   DateValueType origin_date, PaddedPODArray<DateValueType>& res,
                                   const NullMap& result_null_map, FunctionContext* context) {
        if constexpr (!std::is_same_v<DateValueType, TimeStampNsValue>) {
            if (auto cast_date = origin_date; cast_date == DateValueType::FIRST_DAY) {
                vector_const_period(dates, period, res, result_null_map, context);
                return;
            }
        }

        if constexpr (need_tz_conversion) {
            if (auto failed = vector_timestamptz_const(dates, period, origin_date, res,
                                                       result_null_map, context)) {
                throw_out_of_bound_int_date<DateValueType>(Flag::name, dates[*failed], period,
                                                           origin_date);
            }
            return;
        }

        // expand codes for const input periods
#define EXPAND_CODE_FOR_CONST_INPUT(X)                                                   \
    case X: {                                                                            \
        for (int i = 0; i < dates.size(); ++i) {                                         \
            if (result_null_map[i]) {                                                    \
                continue;                                                                \
            }                                                                            \
            /* expand time_round_reinterpret_three_args*/                                \
            res[i] = origin_date;                                                        \
            auto ts2 = dates[i];                                                         \
            auto& ts1 = (DateValueType&)(res[i]);                                        \
            if (!time_round_two_args<X>(ts2, X, ts1, context)) {                         \
                throw_out_of_bound_int_date<DateValueType>(Flag::name, dates[i], period, \
                                                           origin_date);                 \
            }                                                                            \
        }                                                                                \
        return;                                                                          \
    }
#define EXPANDER(z, n, text) EXPAND_CODE_FOR_CONST_INPUT(n)
        switch (period) {
            // expand for some constant period
            BOOST_PP_REPEAT(12, EXPANDER, ~)
        default:
            for (int i = 0; i < dates.size(); ++i) {
                if (result_null_map[i]) {
                    continue;
                }
                // always inline here
                if (!time_round_reinterpret_three_args(dates[i], period, origin_date, res[i],
                                                       context)) {
                    throw_out_of_bound_int_date<DateValueType>(Flag::name, dates[i], period,
                                                               origin_date);
                }
            }
        }
#undef EXPAND_CODE_FOR_CONST_INPUT
#undef EXPANDER
    }

    static void vector_const_vector(const PaddedPODArray<DateValueType>& dates, const Int32 period,
                                    const PaddedPODArray<DateValueType>& origin_dates,
                                    PaddedPODArray<DateValueType>& res,
                                    const NullMap& result_null_map, FunctionContext* context) {
        for (int i = 0; i < dates.size(); ++i) {
            if (result_null_map[i]) {
                continue;
            }
            if (!time_round_reinterpret_three_args(dates[i], period, origin_dates[i], res[i],
                                                   context)) {
                throw_out_of_bound_int_date<DateValueType>(Flag::name, dates[i], period,
                                                           origin_dates[i]);
            }
        }
    }

    static void vector_vector_const(const PaddedPODArray<DateValueType>& dates,
                                    const PaddedPODArray<Int32>& periods, DateValueType origin_date,
                                    PaddedPODArray<DateValueType>& res,
                                    const NullMap& result_null_map, FunctionContext* context) {
        for (int i = 0; i < dates.size(); ++i) {
            if (result_null_map[i]) {
                continue;
            }
            if (periods[i] < 1) [[unlikely]] {
                throw_out_of_bound_int_date<DateValueType>(Flag::name, dates[i], periods[i],
                                                           origin_date);
            }
            if (!time_round_reinterpret_three_args(dates[i], periods[i], origin_date, res[i],
                                                   context)) {
                throw_out_of_bound_int_date<DateValueType>(Flag::name, dates[i], periods[i],
                                                           origin_date);
            }
        }
    }

    static void vector_vector_anchor(const PaddedPODArray<DateValueType>& dates,
                                     const PaddedPODArray<DateValueType>& origin_dates,
                                     PaddedPODArray<DateValueType>& res,
                                     const NullMap& result_null_map, FunctionContext* context) {
        // time_round(datetime, origin)
        for (int i = 0; i < dates.size(); ++i) {
            if (result_null_map[i]) {
                continue;
            }
            if (!time_round_reinterpret_three_args(dates[i], 1, origin_dates[i], res[i], context)) {
                throw_out_of_bound_date_date<DateValueType>(Flag::name, dates[i], origin_dates[i]);
            }
        }
    }

    static void vector_vector_period(const PaddedPODArray<DateValueType>& dates,
                                     const PaddedPODArray<Int32>& periods,
                                     PaddedPODArray<DateValueType>& res,
                                     const NullMap& result_null_map, FunctionContext* context) {
        // time_round(datetime, period)
        for (int i = 0; i < dates.size(); ++i) {
            if (result_null_map[i]) {
                continue;
            }
            if (periods[i] < 1) [[unlikely]] {
                throw_out_of_bound_date_int<DateValueType>(Flag::name, dates[i], periods[i]);
            }
            if (!time_round_reinterpret_two_args(dates[i], periods[i], res[i], context)) {
                throw_out_of_bound_date_int<DateValueType>(Flag::name, dates[i], periods[i]);
            }
        }
    }

    static void vector_vector_vector(const PaddedPODArray<DateValueType>& dates,
                                     const PaddedPODArray<Int32>& periods,
                                     const PaddedPODArray<DateValueType>& origin_dates,
                                     PaddedPODArray<DateValueType>& res,
                                     const NullMap& result_null_map, FunctionContext* context) {
        // time_round(datetime, period, origin)
        for (int i = 0; i < dates.size(); ++i) {
            if (result_null_map[i]) {
                continue;
            }
            if (periods[i] < 1) [[unlikely]] {
                throw_out_of_bound_int_date<DateValueType>(Flag::name, dates[i], periods[i],
                                                           origin_dates[i]);
            }
            if (!time_round_reinterpret_three_args(dates[i], periods[i], origin_dates[i], res[i],
                                                   context)) {
                throw_out_of_bound_int_date<DateValueType>(Flag::name, dates[i], periods[i],
                                                           origin_dates[i]);
            }
        }
    }

    //// time rounds
    static constexpr uint32_t MASK_YEAR_FOR_DATEV2 = ((uint32_t)-1) >> 23;
    static constexpr uint32_t BASE_MONTH_FOR_DATEV2 = 1U << 5;
    static constexpr uint32_t MASK_YEAR_MONTH_FOR_DATEV2 = ((uint32_t)-1) >> 27;
    static constexpr uint64_t MASK_YEAR_FOR_DATETIMEV2 = ((uint64_t)-1) >> 18;
    static constexpr uint64_t BASE_MONTH_FOR_DATETIMEV2 = 1ULL << 42;
    static constexpr uint64_t MASK_YEAR_MONTH_FOR_DATETIMEV2 = ((uint64_t)-1) >> 22;
    static constexpr uint64_t MASK_YEAR_MONTH_DAY_FOR_DATETIMEV2 = ((uint64_t)-1) >> 27;
    static constexpr uint64_t MASK_YEAR_MONTH_DAY_HOUR_FOR_DATETIMEV2 = ((uint64_t)-1) >> 32;
    static constexpr uint64_t MASK_YEAR_MONTH_DAY_HOUR_MINUTE_FOR_DATETIMEV2 = ((uint64_t)-1) >> 38;

    template <int const_period = 0>
    static bool time_round_reinterpret_two_args(DateValueType date, Int32 period,
                                                DateValueType& res, FunctionContext* context) {
        if constexpr (std::is_same_v<DateValueType, TimeStampNsValue>) {
            auto legacy_date = date.to_datetime();
            if (date.nanosecond_remainder() != 0 && legacy_date.microsecond() == 0) {
                legacy_date.template unchecked_set_time_unit<TimeUnit::MICROSECOND>(1);
            }
            auto legacy_result = DateV2Value<DateTimeV2ValueType>::FIRST_DAY;
            if (!DateTimeFloorCeilCore<Flag, TYPE_DATETIMEV2>::template time_round_two_args<
                        const_period>(legacy_date, period, legacy_result, context)) {
                return false;
            }
            return res.from_datetime(legacy_result);
        } else {
            auto ts_arg = date;
            auto& ts_res = res;

            if constexpr (const_period == 0) {
                if (can_use_optimize(period)) {
                    floor_opt(ts_arg, ts_res, period);
                    return true;
                } else {
                    ts_res = DateValueType::FIRST_DAY;
                    return time_round_two_args(ts_arg, period, ts_res, context);
                }
            } else {
                if (can_use_optimize(const_period)) {
                    floor_opt(ts_arg, ts_res, const_period);
                    return true;
                } else {
                    ts_res = DateValueType::FIRST_DAY;
                    return time_round_two_args<const_period>(ts_arg, const_period, ts_res, context);
                }
            }
        }
    }

    ALWAYS_INLINE static bool time_round_reinterpret_three_args(DateValueType date, Int32 period,
                                                                DateValueType origin_date,
                                                                DateValueType& res,
                                                                FunctionContext* context) {
        res = origin_date;
        auto ts2 = date;
        return time_round_two_args(ts2, period, res, context);
    }

    struct RoundingGap {
        LocalDateTime start;
        LocalDateTime end;
        int64_t end_seconds;
        int64_t instant_seconds;
        int64_t length;
        int before_offset;

        RoundingGap(const cctz::time_zone& tz,
                    const cctz::time_zone::civil_transition& transition) {
            const auto utc = cctz::utc_time_zone();
            end_seconds = cctz::convert(transition.to, utc).time_since_epoch().count();
            start.from_unixtime(cctz::convert(transition.from, utc).time_since_epoch().count(),
                                utc);
            end.from_unixtime(end_seconds, utc);
            const auto instant = tz.lookup(transition.from).trans;
            instant_seconds = instant.time_since_epoch().count();
            length = transition.to - transition.from;
            before_offset = tz.lookup(instant - cctz::seconds(1)).offset;
        }
    };

    // Check the grid inside a gap and the first grid boundary after it.
    // On Lord Howe, a 20-minute grid can contain 02:15 and 02:35.
    // They resolve to 02:45 and 02:35. Compare instants, not local fields.
    static void select_gap_boundary(const TimestampTzValue& input, Int32 period,
                                    const DateV2Value<DateTimeV2ValueType>& origin,
                                    const RoundingGap& gap,
                                    std::optional<TimestampTzValue>& selected,
                                    FunctionContext* context, bool include_after = true) {
        const auto& tz = context->state()->timezone_obj();
        const auto utc = cctz::utc_time_zone();
        int64_t input_seconds;
        input.unix_timestamp(&input_seconds, utc);
        LocalDateTime reference;
        reference.from_unixtime(input_seconds + gap.before_offset, utc);
        reference.set_microsecond(input.microsecond());
        const auto consider = [&](const LocalDateTime& point, bool inside_gap) {
            auto local = origin;
            // A yearly boundary can reach year 10000 while a shifted gap boundary
            // remains in year 9999. Discard only the candidate outside the range.
            if (!DateTimeFloorCeilCore<Flag, TYPE_DATETIMEV2>::time_round_two_args(
                        point, period, local, context)) {
                return;
            }
            if (inside_gap && (local < gap.start || local >= gap.end)) {
                return;
            }
            TimestampTzValue candidate;
            if constexpr (Flag::Unit == HOUR || Flag::Unit == MINUTE) {
                candidate.convert_local_to_utc(tz, local, input.utc_offset(tz));
            } else {
                candidate.convert_local_to_utc(tz, local);
            }
            if constexpr (Flag::Type == FLOOR) {
                if (candidate <= input && (!selected || candidate > *selected)) {
                    selected = candidate;
                }
            } else if (candidate >= input && (!selected || candidate < *selected)) {
                selected = candidate;
            }
        };
        if constexpr (Flag::Type == FLOOR) {
            if (reference >= gap.end) {
                reference.from_unixtime(gap.end_seconds - 1, utc);
                reference.set_microsecond(999999);
            }
        } else {
            reference = std::max(reference, gap.start);
        }
        consider(reference, true);
        if constexpr (Flag::Type == CEIL) {
            if (include_after) {
                consider(gap.end, false);
            }
        }
    }

    using SearchInterval = std::pair<int64_t, int64_t>;

    struct PreparedGaps {
        DorisVector<RoundingGap> gaps;
        DorisVector<LocalDateTime> after_boundaries;
        int64_t max_length = 0;
    };

    static PreparedGaps prepare_gap_boundaries(DorisVector<SearchInterval>& intervals, Int32 period,
                                               const LocalDateTime& origin,
                                               FunctionContext* context) {
        DORIS_CHECK(!intervals.empty());
        PreparedGaps prepared;
        std::sort(intervals.begin(), intervals.end());
        size_t merged = 0;
        for (const auto interval : intervals) {
            if (merged > 0 && interval.first <= intervals[merged - 1].second + 1) {
                intervals[merged - 1].second =
                        std::max(intervals[merged - 1].second, interval.second);
            } else {
                intervals[merged++] = interval;
            }
        }
        intervals.resize(merged);
        const auto& tz = context->state()->timezone_obj();
        const RoundingGapScanner scanner(tz, intervals.back().second);
        std::optional<cctz::civil_second> previous_gap;
        for (auto range = intervals.rbegin(); range != intervals.rend(); ++range) {
            scanner.for_each_gap(range->first, range->second, [&](const auto& transition) {
                // Disjoint intervals can share the same preceding gap. Prepare it only once.
                if (previous_gap && *previous_gap == transition.from) {
                    return;
                }
                previous_gap = transition.from;
                RoundingGap gap(tz, transition);
                auto point = gap.start;
                if constexpr (Flag::Type == FLOOR) {
                    point.from_unixtime(gap.end_seconds - 1, cctz::utc_time_zone());
                    point.set_microsecond(999999);
                }
                auto boundary = origin;
                // The local rounding function is monotone. Probe its maximum for FLOOR,
                // minimum for CEIL; January-1 yearly grids cannot contribute a March gap.
                // Retain the original rounding function: adding months to a clamped
                // month-end boundary would change the anchor's day.
                if (DateTimeFloorCeilCore<Flag, TYPE_DATETIMEV2>::time_round_two_args(
                            point, period, boundary, context)) {
                    const bool contributes =
                            Flag::Type == FLOOR ? boundary >= gap.start : boundary < gap.end;
                    if (contributes) {
                        prepared.max_length = std::max(prepared.max_length, gap.length);
                        prepared.gaps.push_back(gap);
                    }
                }
                if constexpr (Flag::Type == CEIL) {
                    // On Lord Howe, 02:15 shifts to 02:45, while the next 20-minute
                    // boundary 02:35 stays 02:35. Ceil must compare both instants.
                    boundary = origin;
                    if (DateTimeFloorCeilCore<Flag, TYPE_DATETIMEV2>::time_round_two_args(
                                gap.end, period, boundary, context)) {
                        prepared.after_boundaries.push_back(boundary);
                    }
                }
            });
        }
        std::sort(prepared.gaps.begin(), prepared.gaps.end(),
                  [](const auto& left, const auto& right) {
                      return left.instant_seconds < right.instant_seconds;
                  });
        auto& after = prepared.after_boundaries;
        std::sort(after.begin(), after.end());
        after.erase(std::unique(after.begin(), after.end()), after.end());
        return prepared;
    }

    using ResolvedBoundaries = DorisMap<int, DorisVector<TimestampTzValue>>;

    struct BlockGapCorrection {
        PreparedGaps prepared;
        ResolvedBoundaries by_offset;
        DorisVector<int> preferred_offsets;
        LocalDateTime origin;
        Int32 period;
    };

    static void select_prepared_gap_boundary(const TimestampTzValue& input,
                                             const BlockGapCorrection& correction,
                                             std::optional<TimestampTzValue>& selected,
                                             FunctionContext* context) {
        const auto& prepared = correction.prepared;
        if (prepared.gaps.empty() || (selected && *selected == input)) {
            return;
        }
        int64_t input_seconds;
        input.unix_timestamp(&input_seconds, cctz::utc_time_zone());
        int64_t selected_seconds = input_seconds;
        if (selected) {
            selected->unix_timestamp(&selected_seconds, cctz::utc_time_zone());
        }
        const auto lower = std::min(input_seconds, selected_seconds);
        const auto upper = std::max(input_seconds, selected_seconds);
        auto gap = std::upper_bound(
                prepared.gaps.begin(), prepared.gaps.end(), upper,
                [](int64_t value, const auto& entry) { return value < entry.instant_seconds; });
        while (gap != prepared.gaps.begin()) {
            --gap;
            // Only a gap whose shifted range intersects the row's search interval
            // can improve it. max_length also handles unequal historical gap lengths.
            if (gap->instant_seconds + prepared.max_length <= lower) {
                break;
            }
            if (gap->instant_seconds + gap->length > lower) {
                select_gap_boundary(input, correction.period, correction.origin, *gap, selected,
                                    context, false);
            }
        }
    }

    static void prepare_preferred_offsets(const PaddedPODArray<TimestampTzValue>& dates,
                                          const PaddedPODArray<TimestampTzValue>& res,
                                          const NullMap& nulls,
                                          const DorisVector<uint8_t>& has_candidate,
                                          const cctz::time_zone& tz, ResolvedBoundaries& by_offset,
                                          DorisVector<int>& preferred_offsets) {
        if constexpr (Flag::Unit == HOUR || Flag::Unit == MINUTE) {
            preferred_offsets.resize(dates.size());
            for (size_t i = 0; i < dates.size(); ++i) {
                if (nulls[i] || (has_candidate[i] && res[i] == dates[i])) {
                    continue;
                }
                // Two occurrences of a fall-back hour keep their own offset.
                preferred_offsets[i] = dates[i].utc_offset(tz);
                by_offset.try_emplace(preferred_offsets[i]);
            }
        } else {
            by_offset.try_emplace(0);
        }
    }

    static void resolve_gap_boundaries(const DorisVector<LocalDateTime>& local_boundaries,
                                       const cctz::time_zone& tz, ResolvedBoundaries& by_offset) {
        for (auto& [offset, boundaries] : by_offset) {
            boundaries.reserve(local_boundaries.size());
            for (const auto& local : local_boundaries) {
                TimestampTzValue boundary;
                if constexpr (Flag::Unit == HOUR || Flag::Unit == MINUTE) {
                    boundary.convert_local_to_utc(tz, local, offset);
                } else {
                    boundary.convert_local_to_utc(tz, local);
                }
                boundaries.push_back(boundary);
            }
            std::sort(boundaries.begin(), boundaries.end());
            boundaries.erase(std::unique(boundaries.begin(), boundaries.end()), boundaries.end());
        }
    }

    static std::optional<size_t> apply_gap_boundaries(const PaddedPODArray<TimestampTzValue>& dates,
                                                      PaddedPODArray<TimestampTzValue>& res,
                                                      const NullMap& nulls,
                                                      DorisVector<uint8_t>& has_candidate,
                                                      const BlockGapCorrection& correction,
                                                      FunctionContext* context) {
        for (size_t i = 0; i < dates.size(); ++i) {
            if (nulls[i] || (has_candidate[i] && res[i] == dates[i])) {
                continue;
            }
            if constexpr (Flag::Type == CEIL) {
                if (!correction.prepared.after_boundaries.empty()) {
                    int offset = 0;
                    if constexpr (Flag::Unit == HOUR || Flag::Unit == MINUTE) {
                        offset = correction.preferred_offsets[i];
                    }
                    const auto& boundaries = correction.by_offset.at(offset);
                    auto boundary =
                            std::lower_bound(boundaries.begin(), boundaries.end(), dates[i]);
                    if (boundary != boundaries.end() && (!has_candidate[i] || *boundary < res[i])) {
                        res[i] = *boundary;
                        has_candidate[i] = true;
                    }
                }
            }
            std::optional<TimestampTzValue> selected;
            if (has_candidate[i]) {
                selected = res[i];
            }
            select_prepared_gap_boundary(dates[i], correction, selected, context);
            if (!selected) {
                return i; // Neither the ordinary grid nor a shifted gap boundary is in range.
            }
            res[i] = *selected;
        }
        return std::nullopt;
    }

    // Return the first unrepresentable row so each vector entry point preserves its error text.
    // First calculate the ordinary boundary of every row; then enumerate the union of the
    // search intervals once and reuse sorted gap boundaries for the entire Block.
    static std::optional<size_t> vector_timestamptz_const(
            const PaddedPODArray<TimestampTzValue>& dates, Int32 period, TimestampTzValue origin,
            PaddedPODArray<TimestampTzValue>& res, const NullMap& nulls, FunctionContext* context) {
        // A one-row Block cannot amortize preparation; retain the scalar path without
        // allocating or sorting intervals (e.g. a point lookup or the last partial Block).
        if (dates.size() == 1) {
            if (nulls[0]) {
                return std::nullopt;
            }
            res[0] = origin;
            if (!time_round_two_args(dates[0], period, res[0], context)) {
                return 0;
            }
            return std::nullopt;
        }
        const auto& tz = context->state()->timezone_obj();
        int32_t fixed_offset;
        if (TimezoneUtils::try_get_fixed_offset_seconds(tz, &fixed_offset)) {
            for (size_t i = 0; i < dates.size(); ++i) {
                if (nulls[i]) {
                    continue;
                }
                res[i] = origin;
                if (!time_round_two_args<0, false>(dates[i], period, res[i], context)) {
                    return i;
                }
            }
            return std::nullopt;
        }

        DorisVector<uint8_t> has_candidate(dates.size());
        DorisVector<SearchInterval> intervals;
        intervals.reserve(dates.size());
        const auto utc = cctz::utc_time_zone();
        for (size_t i = 0; i < dates.size(); ++i) {
            if (nulls[i]) {
                continue;
            }
            res[i] = origin;
            has_candidate[i] = time_round_two_args<0, false>(dates[i], period, res[i], context);
            if (has_candidate[i] && res[i] == dates[i]) {
                continue; // No candidate can improve an exact boundary.
            }
            int64_t input_seconds;
            dates[i].unix_timestamp(&input_seconds, utc);
            int64_t selected_seconds = input_seconds;
            if (has_candidate[i]) {
                res[i].unix_timestamp(&selected_seconds, utc);
            }
            intervals.emplace_back(std::min(input_seconds, selected_seconds),
                                   std::max(input_seconds, selected_seconds));
        }
        if (intervals.empty()) {
            return std::nullopt; // Every non-NULL row is already an exact boundary.
        }
        auto local_origin = origin;
        if (origin != TimestampTzValue::FIRST_DAY) {
            convert_utc_to_local_impl(origin, local_origin, tz);
        }
        BlockGapCorrection correction {
                .prepared = prepare_gap_boundaries(
                        intervals, period, LocalDateTime(local_origin.to_date_int_val()), context),
                .by_offset = {},
                .preferred_offsets = {},
                .origin = LocalDateTime(local_origin.to_date_int_val()),
                .period = period};
        if (!correction.prepared.after_boundaries.empty()) {
            prepare_preferred_offsets(dates, res, nulls, has_candidate, tz, correction.by_offset,
                                      correction.preferred_offsets);
            resolve_gap_boundaries(correction.prepared.after_boundaries, tz, correction.by_offset);
        }
        return apply_gap_boundaries(dates, res, nulls, has_candidate, correction, context);
    }

    static void select_nearest_gap_boundary(const TimestampTzValue& input, Int32 period,
                                            const DateV2Value<DateTimeV2ValueType>& origin,
                                            std::optional<TimestampTzValue>& selected,
                                            FunctionContext* context) {
        // No boundary can improve an exact match. Fixed offsets cannot introduce gaps.
        if (selected && *selected == input) {
            return;
        }
        const auto& tz = context->state()->timezone_obj();
        int32_t offset;
        if (TimezoneUtils::try_get_fixed_offset_seconds(tz, &offset)) {
            return;
        }
        const auto utc = cctz::utc_time_zone();
        int64_t input_seconds;
        input.unix_timestamp(&input_seconds, utc);
        // An out-of-range initial candidate can still have a valid shifted boundary,
        // e.g. New York year_ceil at 9999-03-14 03:15. Include the preceding gap.
        int64_t selected_seconds = input_seconds;
        if (selected) {
            selected->unix_timestamp(&selected_seconds, utc);
        }
        const auto lower = std::min(input_seconds, selected_seconds);
        const auto upper = std::max(input_seconds, selected_seconds);
        RoundingGapScanner(tz, upper).for_each_gap(lower, upper, [&](const auto& transition) {
            select_gap_boundary(input, period, origin, RoundingGap(tz, transition), selected,
                                context);
        });
    }

    // ts_res should be initialized with the ts_origin.
    template <Int32 const_period = 0, bool correct_gaps = true>
    static bool time_round_two_args(const DateValueType& ts_arg, const Int32 period,
                                    DateValueType& ts_origin, FunctionContext* context) {
        /* For TimestampTzValue on date-based units, convert UTC to local time first, eg:
         * for YEAR_CEIL('2025-12-31 23:59:59+03:00') with time_zone: +08:00
         * the local time is: '2026-01-01 04:59:59'
         * We do ceil/floor opt based on the local time, which is '2026-01-01 00:00:00'
         * before write back ans, we should sub a time_zone offset
         * to make the ans is '2026-01-01 00:00:00+08:00' instead of '2026-01-01 08:00:00+08:00'
         */

        DateValueType local_arg;
        DateValueType local_origin;
        const cctz::time_zone& tz = context->state()->timezone_obj();

        if constexpr (need_tz_conversion) {
            convert_utc_to_local_impl(ts_arg, local_arg, tz);

            if (ts_origin == TimestampTzValue::FIRST_DAY) {
                local_origin = ts_origin;
            } else {
                convert_utc_to_local_impl(ts_origin, local_origin, tz);
            }
        }

        // Use local time for calculation if timezone conversion is needed
        const DateValueType& calc_arg = need_tz_conversion ? local_arg : ts_arg;
        const DateValueType& calc_origin = need_tz_conversion ? local_origin : ts_origin;
        DateValueType& ts_res = need_tz_conversion ? local_origin : ts_origin;

        int64_t diff;
        int64_t trivial_part_ts_res;
        int64_t trivial_part_ts_arg;
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            if constexpr (Flag::Unit == YEAR) {
                diff = (ts_arg.year() - ts_origin.year());
                trivial_part_ts_arg = ts_arg.to_int64() % 10000000000;
                trivial_part_ts_res = ts_origin.to_int64() % 10000000000;
            }
            if constexpr (Flag::Unit == QUARTER) {
                diff = (ts_arg.year() - ts_origin.year()) * 4 +
                       (ts_arg.month() - ts_origin.month()) / 3;
                trivial_part_ts_arg =
                        (ts_arg.month() - 1) % 3 * 100000000 + (ts_arg.to_int64() % 100000000);
                trivial_part_ts_res = (ts_origin.month() - 1) % 3 * 100000000 +
                                      (ts_origin.to_int64() % 100000000);
            }
            if constexpr (Flag::Unit == MONTH) {
                diff = (ts_arg.year() - ts_origin.year()) * 12 +
                       (ts_arg.month() - ts_origin.month());
                trivial_part_ts_arg = ts_arg.to_int64() % 100000000;
                trivial_part_ts_res = ts_origin.to_int64() % 100000000;
            }
            if constexpr (Flag::Unit == WEEK) {
                diff = ts_arg.daynr() / 7 - ts_origin.daynr() / 7;
                trivial_part_ts_arg = ts_arg.daynr() % 7 * 24 * 3600 + ts_arg.hour() * 3600 +
                                      ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_origin.daynr() % 7 * 24 * 3600 + ts_origin.hour() * 3600 +
                                      ts_origin.minute() * 60 + ts_origin.second();
            }
            if constexpr (Flag::Unit == DAY) {
                diff = ts_arg.daynr() - ts_origin.daynr();
                trivial_part_ts_arg = ts_arg.hour() * 3600 + ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res =
                        ts_origin.hour() * 3600 + ts_origin.minute() * 60 + ts_origin.second();
            }
            if constexpr (Flag::Unit == HOUR) {
                diff = (ts_arg.daynr() - ts_origin.daynr()) * 24 +
                       (ts_arg.hour() - ts_origin.hour());
                trivial_part_ts_arg = ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_origin.minute() * 60 + ts_origin.second();
            }
            if constexpr (Flag::Unit == MINUTE) {
                diff = (ts_arg.daynr() - ts_origin.daynr()) * 24 * 60 +
                       (ts_arg.hour() - ts_origin.hour()) * 60 +
                       (ts_arg.minute() - ts_origin.minute());
                trivial_part_ts_arg = ts_arg.second();
                trivial_part_ts_res = ts_origin.second();
            }
            if constexpr (Flag::Unit == SECOND) {
                diff = ts_arg.datetime_diff_in_seconds(ts_origin);
                trivial_part_ts_res = 0;
                trivial_part_ts_arg = 0;
            }
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
            if constexpr (Flag::Unit == YEAR) {
                diff = (ts_arg.year() - ts_origin.year());
                trivial_part_ts_arg = ts_arg.to_date_int_val() & MASK_YEAR_FOR_DATEV2;
                trivial_part_ts_res = ts_origin.to_date_int_val() & MASK_YEAR_FOR_DATEV2;
            }
            if constexpr (Flag::Unit == QUARTER) {
                diff = (ts_arg.year() - ts_origin.year()) * 4 +
                       (ts_arg.month() - ts_origin.month()) / 3;
                trivial_part_ts_arg = ((ts_arg.month() - 1) % 3) * BASE_MONTH_FOR_DATEV2 +
                                      (ts_arg.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATEV2);
                trivial_part_ts_res = ((ts_origin.month() - 1) % 3) * BASE_MONTH_FOR_DATEV2 +
                                      (ts_origin.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATEV2);
            }
            if constexpr (Flag::Unit == MONTH) {
                diff = (ts_arg.year() - ts_origin.year()) * 12 +
                       (ts_arg.month() - ts_origin.month());
                trivial_part_ts_arg = ts_arg.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATEV2;
                trivial_part_ts_res = ts_origin.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATEV2;
            }
            if constexpr (Flag::Unit == WEEK) {
                diff = ts_arg.daynr() / 7 - ts_origin.daynr() / 7;
                trivial_part_ts_arg = ts_arg.daynr() % 7 * 24 * 3600 + ts_arg.hour() * 3600 +
                                      ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_origin.daynr() % 7 * 24 * 3600 + ts_origin.hour() * 3600 +
                                      ts_origin.minute() * 60 + ts_origin.second();
            }
            if constexpr (Flag::Unit == DAY) {
                diff = ts_arg.daynr() - ts_origin.daynr();
                trivial_part_ts_arg = ts_arg.hour() * 3600 + ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res =
                        ts_origin.hour() * 3600 + ts_origin.minute() * 60 + ts_origin.second();
            }
            if constexpr (Flag::Unit == HOUR) {
                diff = (ts_arg.daynr() - ts_origin.daynr()) * 24 +
                       (ts_arg.hour() - ts_origin.hour());
                trivial_part_ts_arg = ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_origin.minute() * 60 + ts_origin.second();
            }
            if constexpr (Flag::Unit == MINUTE) {
                diff = (ts_arg.daynr() - ts_origin.daynr()) * 24 * 60 +
                       (ts_arg.hour() - ts_origin.hour()) * 60 +
                       (ts_arg.minute() - ts_origin.minute());
                trivial_part_ts_arg = ts_arg.second();
                trivial_part_ts_res = ts_origin.second();
            }
            if constexpr (Flag::Unit == SECOND) {
                diff = ts_arg.datetime_diff_in_seconds(ts_origin);
                trivial_part_ts_res = 0;
                trivial_part_ts_arg = 0;
            }
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>> ||
                             std::is_same_v<DateValueType, TimestampTzValue>) {
            if constexpr (Flag::Unit == YEAR) {
                diff = (calc_arg.year() - calc_origin.year());
                trivial_part_ts_arg = calc_arg.to_date_int_val() & MASK_YEAR_FOR_DATETIMEV2;
                trivial_part_ts_res = calc_origin.to_date_int_val() & MASK_YEAR_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == QUARTER) {
                diff = (calc_arg.year() - calc_origin.year()) * 4 +
                       (calc_arg.month() - calc_origin.month()) / 3;
                trivial_part_ts_arg = ((calc_arg.month() - 1) % 3) * BASE_MONTH_FOR_DATETIMEV2 +
                                      (calc_arg.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATETIMEV2);
                trivial_part_ts_res =
                        ((calc_origin.month() - 1) % 3) * BASE_MONTH_FOR_DATETIMEV2 +
                        (calc_origin.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATETIMEV2);
            }
            if constexpr (Flag::Unit == MONTH) {
                diff = (calc_arg.year() - calc_origin.year()) * 12 +
                       (calc_arg.month() - calc_origin.month());
                trivial_part_ts_arg = calc_arg.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATETIMEV2;
                trivial_part_ts_res =
                        calc_origin.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == WEEK) {
                constexpr int64_t MICROSECONDS_PER_SECOND = 1'000'000;
                const auto microseconds_since_week_start = [](const auto& value) {
                    return (value.daynr() % 7 * 24 * 3600 + value.hour() * 3600 +
                            value.minute() * 60 + value.second()) *
                                   MICROSECONDS_PER_SECOND +
                           static_cast<int64_t>(value.microsecond());
                };
                diff = calc_arg.daynr() / 7 - calc_origin.daynr() / 7;
                trivial_part_ts_arg = microseconds_since_week_start(calc_arg);
                trivial_part_ts_res = microseconds_since_week_start(calc_origin);
            }
            if constexpr (Flag::Unit == DAY) {
                diff = calc_arg.daynr() - calc_origin.daynr();
                trivial_part_ts_arg =
                        calc_arg.to_date_int_val() & MASK_YEAR_MONTH_DAY_FOR_DATETIMEV2;
                trivial_part_ts_res =
                        calc_origin.to_date_int_val() & MASK_YEAR_MONTH_DAY_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == HOUR) {
                diff = (calc_arg.daynr() - calc_origin.daynr()) * 24 +
                       (calc_arg.hour() - calc_origin.hour());
                trivial_part_ts_arg =
                        calc_arg.to_date_int_val() & MASK_YEAR_MONTH_DAY_HOUR_FOR_DATETIMEV2;
                trivial_part_ts_res =
                        calc_origin.to_date_int_val() & MASK_YEAR_MONTH_DAY_HOUR_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == MINUTE) {
                diff = (calc_arg.daynr() - calc_origin.daynr()) * 24 * 60 +
                       (calc_arg.hour() - calc_origin.hour()) * 60 +
                       (calc_arg.minute() - calc_origin.minute());
                trivial_part_ts_arg =
                        calc_arg.to_date_int_val() & MASK_YEAR_MONTH_DAY_HOUR_MINUTE_FOR_DATETIMEV2;
                trivial_part_ts_res = calc_origin.to_date_int_val() &
                                      MASK_YEAR_MONTH_DAY_HOUR_MINUTE_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == SECOND) {
                diff = calc_arg.datetime_diff_in_seconds(calc_origin);
                trivial_part_ts_arg = calc_arg.microsecond();
                trivial_part_ts_res = calc_origin.microsecond();
            }
        } else if constexpr (std::is_same_v<DateValueType, TimeStampNsValue>) {
            const auto nanos_since_midnight = [](const TimeStampNsValue& value) {
                return value.time_part_to_nanosecond();
            };
            const auto nanos_since_month = [&](const TimeStampNsValue& value,
                                               const auto& civil_value) {
                return (civil_value.day() - 1) * TimeStampNsValue::NANOS_PER_DAY +
                       nanos_since_midnight(value);
            };

            if constexpr (Flag::Unit == YEAR) {
                const auto arg_date = ts_arg.to_date();
                const auto origin_date = ts_origin.to_date();
                diff = arg_date.year() - origin_date.year();
                const auto calendar_remainder = [&](const TimeStampNsValue& value,
                                                    const auto& civil_value) {
                    return (static_cast<int64_t>(civil_value.month()) * 32 + civil_value.day()) *
                                   TimeStampNsValue::NANOS_PER_DAY +
                           nanos_since_midnight(value);
                };
                trivial_part_ts_arg = calendar_remainder(ts_arg, arg_date);
                trivial_part_ts_res = calendar_remainder(ts_origin, origin_date);
            }
            if constexpr (Flag::Unit == QUARTER) {
                const auto arg_date = ts_arg.to_date();
                const auto origin_date = ts_origin.to_date();
                const int64_t total_months = (arg_date.year() - origin_date.year()) * 12 +
                                             arg_date.month() - origin_date.month();
                diff = total_months / 3;
                const int64_t remaining_months = total_months % 3;
                if (remaining_months != 0) {
                    trivial_part_ts_arg = remaining_months;
                    trivial_part_ts_res = 0;
                } else {
                    trivial_part_ts_arg = nanos_since_month(ts_arg, arg_date);
                    trivial_part_ts_res = nanos_since_month(ts_origin, origin_date);
                }
            }
            if constexpr (Flag::Unit == MONTH) {
                const auto arg_date = ts_arg.to_date();
                const auto origin_date = ts_origin.to_date();
                diff = (arg_date.year() - origin_date.year()) * 12 + arg_date.month() -
                       origin_date.month();
                trivial_part_ts_arg = nanos_since_month(ts_arg, arg_date);
                trivial_part_ts_res = nanos_since_month(ts_origin, origin_date);
            }
            if constexpr (Flag::Unit == WEEK) {
                const int64_t arg_daynr = ts_arg.daynr();
                const int64_t origin_daynr = ts_origin.daynr();
                diff = arg_daynr / 7 - origin_daynr / 7;
                trivial_part_ts_arg = arg_daynr % 7 * TimeStampNsValue::NANOS_PER_DAY +
                                      nanos_since_midnight(ts_arg);
                trivial_part_ts_res = origin_daynr % 7 * TimeStampNsValue::NANOS_PER_DAY +
                                      nanos_since_midnight(ts_origin);
            }
            if constexpr (Flag::Unit == DAY) {
                diff = ts_arg.daynr() - ts_origin.daynr();
                trivial_part_ts_arg = nanos_since_midnight(ts_arg);
                trivial_part_ts_res = nanos_since_midnight(ts_origin);
            }
            if constexpr (Flag::Unit == HOUR) {
                const int64_t arg_seconds = ts_arg.time_part_to_seconds();
                const int64_t origin_seconds = ts_origin.time_part_to_seconds();
                diff = (ts_arg.daynr() - ts_origin.daynr()) * HOUR_PER_DAY + arg_seconds / 3600 -
                       origin_seconds / 3600;
                trivial_part_ts_arg = arg_seconds % 3600 * TimeStampNsValue::NANOS_PER_SECOND +
                                      ts_arg.nanosecond();
                trivial_part_ts_res = origin_seconds % 3600 * TimeStampNsValue::NANOS_PER_SECOND +
                                      ts_origin.nanosecond();
            }
            if constexpr (Flag::Unit == MINUTE) {
                const int64_t arg_seconds = ts_arg.time_part_to_seconds();
                const int64_t origin_seconds = ts_origin.time_part_to_seconds();
                diff = (ts_arg.daynr() - ts_origin.daynr()) * HOUR_PER_DAY * SECOND_PER_MINUTE +
                       arg_seconds / SECOND_PER_MINUTE - origin_seconds / SECOND_PER_MINUTE;
                trivial_part_ts_arg =
                        arg_seconds % SECOND_PER_MINUTE * TimeStampNsValue::NANOS_PER_SECOND +
                        ts_arg.nanosecond();
                trivial_part_ts_res =
                        origin_seconds % SECOND_PER_MINUTE * TimeStampNsValue::NANOS_PER_SECOND +
                        ts_origin.nanosecond();
            }
            if constexpr (Flag::Unit == SECOND) {
                diff = ts_arg.epoch_seconds() - ts_origin.epoch_seconds();
                trivial_part_ts_arg = ts_arg.nanosecond();
                trivial_part_ts_res = ts_origin.nanosecond();
            }
        }

        //round down/up to specific time-unit(HOUR/DAY/MONTH...) by increase/decrease diff variable
        if constexpr (Flag::Type == CEIL) {
            //e.g. hour_ceil(ts: 00:00:40, origin: 00:00:30), ts should be rounded to 01:00:30
            diff += trivial_part_ts_arg > trivial_part_ts_res;
        }
        if constexpr (Flag::Type == FLOOR) {
            //e.g. hour_floor(ts: 01:00:20, origin: 00:00:30), ts should be rounded to 00:00:30
            diff -= trivial_part_ts_arg < trivial_part_ts_res;
        }

        // round down/up inside time period(several time-units)
        // e.g. if period is 3, diff is 8, then delta_inside_period is 2,
        int64_t delta_inside_period;
        if constexpr (const_period != 0) {
            delta_inside_period = diff >= 0 ? diff % const_period
                                            : (diff % const_period + const_period) % const_period;
        } else {
            delta_inside_period = diff >= 0 ? diff % period : (diff % period + period) % period;
        }

        int64_t step = diff - delta_inside_period +
                       (Flag::Type == FLOOR ? 0 : (delta_inside_period == 0 ? 0 : period));
        if constexpr (need_tz_conversion) {
            // Keep the original calendar fields when selecting an earlier period.
            // Subtracting a month from an adjusted day can change the origin's day.
            const DateV2Value<DateTimeV2ValueType> round_origin(ts_res.to_date_int_val());
            int preferred_offset = 0;
            if constexpr (Flag::Unit == HOUR || Flag::Unit == MINUTE) {
                preferred_offset = ts_arg.utc_offset(tz);
            }
            const auto resolve = [&]() -> std::optional<TimestampTzValue> {
                auto local_result = round_origin;
                TimeInterval interval(Flag::Unit, std::abs(step), step < 0);
                // A large period can put this candidate outside years 0 through 9999.
                if (!local_result.template date_add_interval<Flag::Unit>(interval)) {
                    return std::nullopt;
                }
                TimestampTzValue candidate;
                if constexpr (Flag::Unit == HOUR || Flag::Unit == MINUTE) {
                    candidate.convert_local_to_utc(tz, local_result, preferred_offset);
                } else {
                    candidate.convert_local_to_utc(tz, local_result);
                }
                return candidate;
            };
            auto selected = resolve();
            if constexpr (Flag::Type == FLOOR) {
                // A gap can move the candidate after the input. On Lord Howe on
                // 2024-10-06, 02:15 becomes 02:45, which is after input 02:35.
                // Select an earlier period until the resolved result is a floor.
                while (selected && *selected > ts_arg) {
                    step -= period;
                    selected = resolve();
                }
            }
            if constexpr (correct_gaps) {
                select_nearest_gap_boundary(ts_arg, period, round_origin, selected, context);
            }
            if (!selected) {
                return false; // Neither the ordinary grid nor a gap has an in-range boundary.
            }
            ts_origin = *selected;
            return true;
        } else {
            TimeInterval interval(Flag::Unit, std::abs(step), step < 0);
            return ts_res.template date_add_interval<Flag::Unit>(interval);
        }
    }

    /// optimized path
    constexpr static bool can_use_optimize(int period) {
        if constexpr (std::is_same_v<DateValueType, TimeStampNsValue>) {
            return false;
        }

        // For TimestampTzValue on date-based units, disable optimization to ensure timezone conversion
        if constexpr (std::is_same_v<DateValueType, TimestampTzValue>) {
            if constexpr (Flag::Unit == YEAR || Flag::Unit == QUARTER || Flag::Unit == MONTH ||
                          Flag::Unit == WEEK || Flag::Unit == DAY || Flag::Unit == HOUR ||
                          Flag::Unit == MINUTE) {
                return false;
            }
        }

        if constexpr (!std::is_same_v<DateValueType, VecDateTimeValue> && Flag::Type == FLOOR) {
            if constexpr (Flag::Unit == YEAR || Flag::Unit == DAY || Flag::Unit == QUARTER) {
                return period == 1;
            }
            if constexpr (Flag::Unit == MONTH) {
                return period <= 11 && 12 % period == 0;
            }
            if constexpr (Flag::Unit == HOUR) {
                return period <= 23 && 24 % period == 0;
            }
            if constexpr (Flag::Unit == MINUTE) {
                return period <= 59 && 60 % period == 0;
            }
            if constexpr (Flag::Unit == SECOND) {
                return period <= 59 && 60 % period == 0;
            }
        }
        return false;
    }

    static void floor_opt(const DateValueType& ts2, DateValueType& ts1, int period) {
        if (period == 1) {
            // year, day, quarter must go through this path
            floor_opt_one_period(ts2, ts1);
        } else {
            static constexpr uint64_t MASK_HOUR_FLOOR =
                    0b1111111111111111111111111111111100000000000000000000000000000000;
            static constexpr uint64_t MASK_MINUTE_FLOOR =
                    0b1111111111111111111111111111111111111100000000000000000000000000;
            static constexpr uint64_t MASK_SECOND_FLOOR =
                    0b1111111111111111111111111111111111111111111100000000000000000000;
            // Optimize the performance of the datetimev2 type on the floor operation.
            // Now supports unit month hour minute second. no need to check again when set value for ts
            if constexpr (Flag::Unit == MONTH && !std::is_same_v<DateValueType, VecDateTimeValue>) {
                int month = ts2.month() - 1;
                int new_month = month / period * period;
                if (new_month >= 12) {
                    new_month = new_month % 12;
                }
                ts1.unchecked_set_time(ts2.year(), ts2.month(), 1, 0, 0, 0);
                ts1.template unchecked_set_time_unit<TimeUnit::MONTH>(new_month + 1);
            }
            if constexpr (Flag::Unit == HOUR && !std::is_same_v<DateValueType, VecDateTimeValue>) {
                int hour = ts2.hour();
                int new_hour = hour / period * period;
                if (new_hour >= 24) {
                    new_hour = new_hour % 24;
                }
                ts1.set_int_val(ts2.to_date_int_val() & MASK_HOUR_FLOOR);
                ts1.template unchecked_set_time_unit<TimeUnit::HOUR>(new_hour);
            }
            if constexpr (Flag::Unit == MINUTE &&
                          !std::is_same_v<DateValueType, VecDateTimeValue>) {
                int minute = ts2.minute();
                int new_minute = minute / period * period;
                if (new_minute >= 60) {
                    new_minute = new_minute % 60;
                }
                ts1.set_int_val(ts2.to_date_int_val() & MASK_MINUTE_FLOOR);
                ts1.template unchecked_set_time_unit<TimeUnit::MINUTE>(new_minute);
            }
            if constexpr (Flag::Unit == SECOND &&
                          !std::is_same_v<DateValueType, VecDateTimeValue>) {
                int second = ts2.second();
                int new_second = second / period * period;
                if (new_second >= 60) {
                    new_second = new_second % 60;
                }
                ts1.set_int_val(ts2.to_date_int_val() & MASK_SECOND_FLOOR);
                ts1.template unchecked_set_time_unit<TimeUnit::SECOND>(new_second);
            }
        }
    }

    static void floor_opt_one_period(const DateValueType& ts2, DateValueType& ts1) {
        if constexpr (Flag::Unit == YEAR) {
            ts1.unchecked_set_time(ts2.year(), 1, 1, 0, 0, 0);
        }
        if constexpr (Flag::Unit == QUARTER) {
            // 1,4,7,10 are the first month of each quarter
            if (ts2.month() <= 3) {
                ts1.unchecked_set_time(ts2.year(), 1, 1, 0, 0, 0);
            } else if (ts2.month() <= 6) {
                ts1.unchecked_set_time(ts2.year(), 4, 1, 0, 0, 0);
            } else if (ts2.month() <= 9) {
                ts1.unchecked_set_time(ts2.year(), 7, 1, 0, 0, 0);
            } else {
                ts1.unchecked_set_time(ts2.year(), 10, 1, 0, 0, 0);
            }
        }
        if constexpr (Flag::Unit == MONTH) {
            ts1.unchecked_set_time(ts2.year(), ts2.month(), 1, 0, 0, 0);
        }
        if constexpr (Flag::Unit == DAY) {
            ts1.unchecked_set_time(ts2.year(), ts2.month(), ts2.day(), 0, 0, 0);
        }

        // only DateTimeV2ValueType and TimestampTzValue types have hour minute second
        if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>> ||
                      std::is_same_v<DateValueType, TimestampTzValue>) {
            static constexpr uint64_t MASK_HOUR_FLOOR =
                    0b1111111111111111111111111111111100000000000000000000000000000000;
            static constexpr uint64_t MASK_MINUTE_FLOOR =
                    0b1111111111111111111111111111111111111100000000000000000000000000;
            static constexpr uint64_t MASK_SECOND_FLOOR =
                    0b1111111111111111111111111111111111111111111100000000000000000000;

            // Optimize the performance of the datetimev2 type on the floor operation.
            // Now supports unit biger than SECOND
            if constexpr (Flag::Unit == HOUR) {
                ts1.set_int_val(ts2.to_date_int_val() & MASK_HOUR_FLOOR);
            }
            if constexpr (Flag::Unit == MINUTE) {
                ts1.set_int_val(ts2.to_date_int_val() & MASK_MINUTE_FLOOR);
            }
            if constexpr (Flag::Unit == SECOND) {
                ts1.set_int_val(ts2.to_date_int_val() & MASK_SECOND_FLOOR);
            }
        }
    }
};

#define TIME_ROUND_WITH_DELTA_TYPE(IMPL, NAME, UNIT, TYPE, DELTA)                                \
    using FunctionDateV2OneArg##IMPL##DELTA = FunctionDateTimeFloorCeil<IMPL, TYPE_DATEV2, 1>;   \
    using FunctionDateV2TwoArg##IMPL##DELTA = FunctionDateTimeFloorCeil<IMPL, TYPE_DATEV2, 2>;   \
    using FunctionDateV2ThreeArg##IMPL##DELTA = FunctionDateTimeFloorCeil<IMPL, TYPE_DATEV2, 3>; \
    using FunctionDateTimeV2OneArg##IMPL##DELTA =                                                \
            FunctionDateTimeFloorCeil<IMPL, TYPE_DATETIMEV2, 1>;                                 \
    using FunctionDateTimeV2TwoArg##IMPL##DELTA =                                                \
            FunctionDateTimeFloorCeil<IMPL, TYPE_DATETIMEV2, 2>;                                 \
    using FunctionDateTimeV2ThreeArg##IMPL##DELTA =                                              \
            FunctionDateTimeFloorCeil<IMPL, TYPE_DATETIMEV2, 3>;                                 \
    using FunctionTimestampNsOneArg##IMPL##DELTA =                                               \
            FunctionDateTimeFloorCeil<IMPL, TYPE_TIMESTAMP_NS, 1>;                               \
    using FunctionTimestampNsTwoArg##IMPL##DELTA =                                               \
            FunctionDateTimeFloorCeil<IMPL, TYPE_TIMESTAMP_NS, 2>;                               \
    using FunctionTimestampNsThreeArg##IMPL##DELTA =                                             \
            FunctionDateTimeFloorCeil<IMPL, TYPE_TIMESTAMP_NS, 3>;                               \
    using FunctionTimestamptzOneArg##IMPL##DELTA =                                               \
            FunctionDateTimeFloorCeil<IMPL, TYPE_TIMESTAMPTZ, 1>;                                \
    using FunctionTimestamptzTwoArg##IMPL##DELTA =                                               \
            FunctionDateTimeFloorCeil<IMPL, TYPE_TIMESTAMPTZ, 2>;                                \
    using FunctionTimestamptzThreeArg##IMPL##DELTA =                                             \
            FunctionDateTimeFloorCeil<IMPL, TYPE_TIMESTAMPTZ, 3>;

#define TIME_ROUND_DECLARE(IMPL, NAME, UNIT, TYPE)                                            \
    struct IMPL {                                                                             \
        static constexpr auto name = #NAME;                                                   \
        static constexpr TimeUnit Unit = UNIT;                                                \
        static constexpr auto Type = TYPE;                                                    \
    };                                                                                        \
                                                                                              \
    TIME_ROUND_WITH_DELTA_TYPE(IMPL, NAME, UNIT, TYPE, Int32)                                 \
    using FunctionDateV2TwoArg##IMPL = FunctionDateTimeFloorCeil<IMPL, TYPE_DATEV2, 2, true>; \
    using FunctionDateTimeV2TwoArg##IMPL =                                                    \
            FunctionDateTimeFloorCeil<IMPL, TYPE_DATETIMEV2, 2, true>;                        \
    using FunctionTimestampNsTwoArg##IMPL =                                                   \
            FunctionDateTimeFloorCeil<IMPL, TYPE_TIMESTAMP_NS, 2, true>;                      \
    using FunctionTimestamptzTwoArg##IMPL =                                                   \
            FunctionDateTimeFloorCeil<IMPL, TYPE_TIMESTAMPTZ, 2, true>;

TIME_ROUND_DECLARE(YearFloor, year_floor, YEAR, FLOOR);
TIME_ROUND_DECLARE(QuarterFloor, quarter_floor, QUARTER, FLOOR);
TIME_ROUND_DECLARE(MonthFloor, month_floor, MONTH, FLOOR);
TIME_ROUND_DECLARE(WeekFloor, week_floor, WEEK, FLOOR);
TIME_ROUND_DECLARE(DayFloor, day_floor, DAY, FLOOR);
TIME_ROUND_DECLARE(HourFloor, hour_floor, HOUR, FLOOR);
TIME_ROUND_DECLARE(MinuteFloor, minute_floor, MINUTE, FLOOR);
TIME_ROUND_DECLARE(SecondFloor, second_floor, SECOND, FLOOR);

TIME_ROUND_DECLARE(YearCeil, year_ceil, YEAR, CEIL);
TIME_ROUND_DECLARE(QuarterCeil, quarter_ceil, QUARTER, CEIL);
TIME_ROUND_DECLARE(MonthCeil, month_ceil, MONTH, CEIL);
TIME_ROUND_DECLARE(WeekCeil, week_ceil, WEEK, CEIL);
TIME_ROUND_DECLARE(DayCeil, day_ceil, DAY, CEIL);
TIME_ROUND_DECLARE(HourCeil, hour_ceil, HOUR, CEIL);
TIME_ROUND_DECLARE(MinuteCeil, minute_ceil, MINUTE, CEIL);
TIME_ROUND_DECLARE(SecondCeil, second_ceil, SECOND, CEIL);

void register_function_datetime_floor_ceil(SimpleFunctionFactory& factory) {
#define REGISTER_FUNC_WITH_DELTA_TYPE(IMPL, DELTA)                         \
    factory.register_function<FunctionDateV2OneArg##IMPL##DELTA>();        \
    factory.register_function<FunctionDateV2TwoArg##IMPL##DELTA>();        \
    factory.register_function<FunctionDateV2ThreeArg##IMPL##DELTA>();      \
    factory.register_function<FunctionDateTimeV2OneArg##IMPL##DELTA>();    \
    factory.register_function<FunctionDateTimeV2TwoArg##IMPL##DELTA>();    \
    factory.register_function<FunctionDateTimeV2ThreeArg##IMPL##DELTA>();  \
    factory.register_function<FunctionTimestampNsOneArg##IMPL##DELTA>();   \
    factory.register_function<FunctionTimestampNsTwoArg##IMPL##DELTA>();   \
    factory.register_function<FunctionTimestampNsThreeArg##IMPL##DELTA>(); \
    factory.register_function<FunctionTimestamptzOneArg##IMPL##DELTA>();   \
    factory.register_function<FunctionTimestamptzTwoArg##IMPL##DELTA>();   \
    factory.register_function<FunctionTimestamptzThreeArg##IMPL##DELTA>(); \
    factory.register_function<FunctionDateTimeV2TwoArg##IMPL>();           \
    factory.register_function<FunctionTimestampNsTwoArg##IMPL>();          \
    factory.register_function<FunctionDateV2TwoArg##IMPL>();               \
    factory.register_function<FunctionTimestamptzTwoArg##IMPL>();

#define REGISTER_FUNC(IMPL) REGISTER_FUNC_WITH_DELTA_TYPE(IMPL, Int32)

    REGISTER_FUNC(YearFloor);
    REGISTER_FUNC(QuarterFloor);
    REGISTER_FUNC(MonthFloor);
    REGISTER_FUNC(WeekFloor);
    REGISTER_FUNC(DayFloor);
    REGISTER_FUNC(HourFloor);
    REGISTER_FUNC(MinuteFloor);
    REGISTER_FUNC(SecondFloor);

    REGISTER_FUNC(YearCeil);
    REGISTER_FUNC(QuarterCeil);
    REGISTER_FUNC(MonthCeil);
    REGISTER_FUNC(WeekCeil);
    REGISTER_FUNC(DayCeil);
    REGISTER_FUNC(HourCeil);
    REGISTER_FUNC(MinuteCeil);
    REGISTER_FUNC(SecondCeil);
}
#undef FLOOR
#undef CEIL
} // namespace doris

#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic pop
#endif
