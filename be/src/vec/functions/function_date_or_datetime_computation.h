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

#include "common/logging.h"
#include "fmt/format.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "udf/udf_internal.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/runtime/vdatetime_value.h"
namespace doris::vectorized {

template <TimeUnit unit>
inline Int64 date_time_add(const Int64& t, Int64 delta, bool& is_null) {
    auto ts_value = binary_cast<Int64, doris::vectorized::VecDateTimeValue>(t);
    TimeInterval interval(unit, delta, false);
    is_null = !ts_value.date_add_interval(interval, unit);

    return binary_cast<doris::vectorized::VecDateTimeValue, Int64>(ts_value);
}

#define ADD_TIME_FUNCTION_IMPL(CLASS, NAME, UNIT)                                   \
    struct CLASS {                                                                  \
        using ReturnType = DataTypeDateTime;                                        \
        static constexpr auto name = #NAME;                                         \
        static constexpr auto is_nullable = true;                                   \
        static inline Int64 execute(const Int64& t, Int64 delta, bool& is_null) { \
            return date_time_add<TimeUnit::UNIT>(t, delta, is_null);                \
        }                                                                           \
    }

ADD_TIME_FUNCTION_IMPL(AddSecondsImpl, seconds_add, SECOND);
ADD_TIME_FUNCTION_IMPL(AddMinutesImpl, minutes_add, MINUTE);
ADD_TIME_FUNCTION_IMPL(AddHoursImpl, hours_add, HOUR);
ADD_TIME_FUNCTION_IMPL(AddDaysImpl, days_add, DAY);
ADD_TIME_FUNCTION_IMPL(AddWeeksImpl, weeks_add, WEEK);
ADD_TIME_FUNCTION_IMPL(AddMonthsImpl, months_add, MONTH);
ADD_TIME_FUNCTION_IMPL(AddYearsImpl, years_add, YEAR);

struct AddQuartersImpl {
    using ReturnType = DataTypeDateTime;
    static constexpr auto name = "quarters_add";
    static constexpr auto is_nullable = true;
    static inline Int64 execute(const Int64& t, Int64 delta, bool& is_null) {
        return date_time_add<TimeUnit::MONTH>(t, delta * 3, is_null);
    }
};

template <typename Transform>
struct SubtractIntervalImpl {
    using ReturnType = DataTypeDateTime;
    static constexpr auto is_nullable = true;
    static inline Int64 execute(const Int64& t, Int64 delta, bool& is_null) {
        return Transform::execute(t, -delta, is_null);
    }
};

struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl> {
    static constexpr auto name = "seconds_sub";
};
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl> {
    static constexpr auto name = "minutes_sub";
};
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl> {
    static constexpr auto name = "hours_sub";
};
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl> {
    static constexpr auto name = "days_sub";
};
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl> {
    static constexpr auto name = "weeks_sub";
};
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl> {
    static constexpr auto name = "months_sub";
};
struct SubtractQuartersImpl : SubtractIntervalImpl<AddQuartersImpl> {
    static constexpr auto name = "quarters_sub";
};
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl> {
    static constexpr auto name = "years_sub";
};

struct DateDiffImpl {
    using ReturnType = DataTypeInt32;
    static constexpr auto name = "datediff";
    static constexpr auto is_nullable = false;
    static inline Int32 execute(const Int64& t0, const Int64& t1, bool& is_null) {
        const auto& ts0 = reinterpret_cast<const doris::vectorized::VecDateTimeValue&>(t0);
        const auto& ts1 = reinterpret_cast<const doris::vectorized::VecDateTimeValue&>(t1);
        is_null = !ts0.is_valid_date() || !ts1.is_valid_date();
        return ts0.daynr() - ts1.daynr();
    }
};

struct TimeDiffImpl {
    using ReturnType = DataTypeFloat64;
    static constexpr auto name = "timediff";
    static constexpr auto is_nullable = false;
    static inline double execute(const Int64& t0, const Int64& t1, bool& is_null) {
        const auto& ts0 = reinterpret_cast<const doris::vectorized::VecDateTimeValue&>(t0);
        const auto& ts1 = reinterpret_cast<const doris::vectorized::VecDateTimeValue&>(t1);
        is_null = !ts0.is_valid_date() || !ts1.is_valid_date();
        return ts0.second_diff(ts1);
    }
};

#define TIME_DIFF_FUNCTION_IMPL(CLASS, NAME, UNIT)                                         \
    struct CLASS {                                                                         \
        using ReturnType = DataTypeInt64;                                                  \
        static constexpr auto name = #NAME;                                                \
        static constexpr auto is_nullable = false;                                         \
        static inline int64_t execute(const Int64& t0, const Int64& t1, bool& is_null) { \
            const auto& ts0 = reinterpret_cast<const doris::vectorized::VecDateTimeValue&>(t0);           \
            const auto& ts1 = reinterpret_cast<const doris::vectorized::VecDateTimeValue&>(t1);           \
            is_null = !ts0.is_valid_date() || !ts1.is_valid_date();                         \
            return VecDateTimeValue::datetime_diff<TimeUnit::UNIT>(ts1, ts0);                 \
        }                                                                                  \
    }

TIME_DIFF_FUNCTION_IMPL(YearsDiffImpl, years_diff, YEAR);
TIME_DIFF_FUNCTION_IMPL(MonthsDiffImpl, months_diff, MONTH);
TIME_DIFF_FUNCTION_IMPL(WeeksDiffImpl, weeks_diff, WEEK);
TIME_DIFF_FUNCTION_IMPL(DaysDiffImpl, days_diff, DAY);
TIME_DIFF_FUNCTION_IMPL(HoursDiffImpl, hours_diff, HOUR);
TIME_DIFF_FUNCTION_IMPL(MintueSDiffImpl, minutes_diff, MINUTE);
TIME_DIFF_FUNCTION_IMPL(SecondsDiffImpl, seconds_diff, SECOND);

#define TIME_FUNCTION_TWO_ARGS_IMPL(CLASS, NAME, FUNCTION)                                   \
    struct CLASS {                                                                           \
        using ReturnType = DataTypeInt32;                                                    \
        static constexpr auto name = #NAME;                                                  \
        static constexpr auto is_nullable = false;                                           \
        static inline int64_t execute(const Int64& t0, const Int32 mode, bool& is_null) {    \
            const auto& ts0 = reinterpret_cast<const doris::vectorized::VecDateTimeValue&>(t0);           \
            is_null = !ts0.is_valid_date();                                                    \
            return ts0.FUNCTION;                                                               \
        }                                                                                      \
        static DataTypes get_variadic_argument_types() {                                       \
            return {std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeInt32>()};  \
        }                                                                                      \
    }

TIME_FUNCTION_TWO_ARGS_IMPL(ToYearWeekTwoArgsImpl, yearweek, year_week(mysql_week_mode(mode)));
TIME_FUNCTION_TWO_ARGS_IMPL(ToWeekTwoArgsImpl, week, week(mysql_week_mode(mode)));

template <typename FromType, typename ToType, typename Transform>
struct DateTimeOp {
    // use for (DateTime, DateTime) -> other_type
    static void vector_vector(const PaddedPODArray<FromType>& vec_from0,
                              const PaddedPODArray<FromType>& vec_from1,
                              PaddedPODArray<ToType>& vec_to, NullMap& null_map) {
        size_t size = vec_from0.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i) {
            // here reinterpret_cast is used to convert uint8& to bool&,
            // otherwise it will be implicitly converted to bool, causing the rvalue to fail to match the lvalue.
            // the same goes for the following.
            vec_to[i] = Transform::execute(vec_from0[i], vec_from1[i],
                                           reinterpret_cast<bool&>(null_map[i]));
        }
    }

    // use for (DateTime, int32) -> other_type
    static void vector_vector(const PaddedPODArray<FromType>& vec_from0,
                              const PaddedPODArray<Int32>& vec_from1,
                              PaddedPODArray<ToType>& vec_to, NullMap& null_map) {
        size_t size = vec_from0.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from0[i], vec_from1[i],
                    reinterpret_cast<bool&>(null_map[i]));
    }

    // use for (DateTime, const DateTime) -> other_type
    static void vector_constant(const PaddedPODArray<FromType>& vec_from,
                                PaddedPODArray<ToType>& vec_to, NullMap& null_map, Int128& delta) {
        size_t size = vec_from.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i) {
            vec_to[i] =
                    Transform::execute(vec_from[i], delta, reinterpret_cast<bool&>(null_map[i]));
        }
    }

    // use for (DateTime, const ColumnNumber) -> other_type
    static void vector_constant(const PaddedPODArray<FromType>& vec_from,
                                PaddedPODArray<ToType>& vec_to, NullMap& null_map, Int64 delta) {
        size_t size = vec_from.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i) {
            vec_to[i] =
                    Transform::execute(vec_from[i], delta, reinterpret_cast<bool&>(null_map[i]));
        }
    }

    // use for (const DateTime, ColumnNumber) -> other_type
    static void constant_vector(const FromType& from, PaddedPODArray<ToType>& vec_to,
                                NullMap& null_map, const IColumn& delta) {
        size_t size = delta.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i) {
            vec_to[i] = Transform::execute(from, delta.get_int(i),
                                           reinterpret_cast<bool&>(null_map[i]));
        }
    }

    static void constant_vector(const FromType& from, PaddedPODArray<ToType>& vec_to,
                                NullMap& null_map, const PaddedPODArray<Int64>& delta) {
        size_t size = delta.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i) {
            vec_to[i] = Transform::execute(from, delta[i], reinterpret_cast<bool&>(null_map[i]));
        }
    }
};

template <typename FromType, typename Transform>
struct DateTimeAddIntervalImpl {
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        using ToType = typename Transform::ReturnType::FieldType;
        using Op = DateTimeOp<FromType, ToType, Transform>;

        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;
        if (const auto* sources = check_and_get_column<ColumnVector<FromType>>(source_col.get())) {
            auto col_to = ColumnVector<ToType>::create();
            auto null_map = ColumnUInt8::create();
            const IColumn& delta_column = *block.get_by_position(arguments[1]).column;

            if (const auto* delta_const_column = typeid_cast<const ColumnConst*>(&delta_column)) {
                if (delta_const_column->get_field().get_type() == Field::Types::Int128) {
                    Op::vector_constant(sources->get_data(), col_to->get_data(),
                                        null_map->get_data(),
                                        delta_const_column->get_field().get<Int128>());
                } else if (delta_const_column->get_field().get_type() == Field::Types::Int64) {
                    Op::vector_constant(sources->get_data(), col_to->get_data(),
                                        null_map->get_data(),
                                        delta_const_column->get_field().get<Int64>());
                } else {
                    Op::vector_constant(sources->get_data(), col_to->get_data(),
                                        null_map->get_data(),
                                        delta_const_column->get_field().get<Int32>());
                }
            } else {
                if (const auto* delta_vec_column0 =
                        check_and_get_column<ColumnVector<FromType>>(delta_column)) {
                    Op::vector_vector(sources->get_data(), delta_vec_column0->get_data(),
                                      col_to->get_data(), null_map->get_data());
                } else {
                    const auto* delta_vec_column1 =
                        check_and_get_column<ColumnVector<Int32>>(delta_column);
                    DCHECK(delta_vec_column1 != nullptr);
                    Op::vector_vector(sources->get_data(), delta_vec_column1->get_data(),
                                      col_to->get_data(), null_map->get_data());
                }
            }

            block.get_by_position(result).column =
                        ColumnNullable::create(std::move(col_to), std::move(null_map));
        } else if (const auto* sources_const =
                           check_and_get_column_const<ColumnVector<FromType>>(source_col.get())) {
            auto col_to = ColumnVector<ToType>::create();
            auto null_map = ColumnUInt8::create();

            if (const auto* delta_vec_column = check_and_get_column<ColumnVector<FromType>>(
                        *block.get_by_position(arguments[1]).column)) {
                Op::constant_vector(sources_const->template get_value<FromType>(),
                                    col_to->get_data(), null_map->get_data(),
                                    delta_vec_column->get_data());
            } else {
                Op::constant_vector(sources_const->template get_value<FromType>(),
                                    col_to->get_data(), null_map->get_data(),
                                    *block.get_by_position(arguments[1]).column);
            }
            block.get_by_position(result).column =
                        ColumnNullable::create(std::move(col_to), std::move(null_map));
        } else {
            return Status::RuntimeError(fmt::format(
                    "Illegal column {} of first argument of function {}",
                    block.get_by_position(arguments[0]).column->get_name(), Transform::name));
        }
        return Status::OK();
    }
};

template <typename Transform>
class FunctionDateOrDateTimeComputation : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;

    static FunctionPtr create() { return std::make_shared<FunctionDateOrDateTimeComputation>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) return Transform::get_variadic_argument_types();
        return {};
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        if (arguments.size() != 2 && arguments.size() != 3) {
            LOG(FATAL) << fmt::format(
                    "Number of arguments for function {} doesn't match: passed {} , should be 2 or "
                    "3",
                    get_name(), arguments.size());
        }

        if (arguments.size() == 2) {
            if (!is_date_or_datetime(arguments[0].type)) {
                LOG(FATAL) << fmt::format(
                        "Illegal type {} of argument of function {}. Should be a date or a date "
                        "with time",
                        arguments[0].type->get_name(), get_name());
            }
        } else {
            if (!WhichDataType(arguments[0].type).is_date_time() ||
                !WhichDataType(arguments[2].type).is_string()) {
                LOG(FATAL) << fmt::format(
                        "Function {} supports 2 or 3 arguments. The 1st argument must be of type "
                        "Date or DateTime. The 2nd argument must be number. The 3rd argument "
                        "(optional) must be a constant string with timezone name. The timezone "
                        "argument is allowed only when the 1st argument has the type DateTime",
                        get_name());
            }
        }
        return make_nullable(std::make_shared<typename Transform::ReturnType>());
    }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const IDataType* from_type = block.get_by_position(arguments[0]).type.get();
        WhichDataType which(from_type);

        if (which.is_date()) {
            return DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform>::execute(
                    block, arguments, result);
        } else if (which.is_date_time()) {
            return DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform>::execute(
                    block, arguments, result);
        } else {
            return Status::RuntimeError(
                    fmt::format("Illegal type {} of argument of function {}",
                                block.get_by_position(arguments[0]).type->get_name(), get_name()));
        }
    }
};

template <typename FunctionImpl>
class FunctionCurrentDateOrDateTime : public IFunction {
public:
    static constexpr auto name = FunctionImpl::name;
    static FunctionPtr create() { return std::make_shared<FunctionCurrentDateOrDateTime>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<typename FunctionImpl::ReturnType>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK(arguments.empty());
        return FunctionImpl::execute(context, block, result, input_rows_count);
    }
};

template<typename FunctionName>
struct CurrentDateTimeImpl {
    using ReturnType = DataTypeDateTime;
    static constexpr auto name = FunctionName::name;
    static Status execute(FunctionContext* context, Block& block, size_t result,
                          size_t input_rows_count) {
        auto col_to = ColumnVector<Int64>::create();
        VecDateTimeValue dtv;
        if (dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000,
                              context->impl()->state()->timezone_obj())) {
            reinterpret_cast<VecDateTimeValue*>(&dtv)->set_type(TIME_DATETIME);
            auto date_packed_int = binary_cast<doris::vectorized::VecDateTimeValue, int64_t>(
                    *reinterpret_cast<VecDateTimeValue*>(&dtv));
            for (int i = 0; i < input_rows_count; i ++) {
                col_to->insert_data(
                        const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
            }
        } else {
            auto invalid_val = 0;
            for (int i = 0; i < input_rows_count; i ++) {
                col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)),
                                    0);
            }
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

template<typename FunctionName>
struct CurrentDateImpl {
    using ReturnType = DataTypeDate;
    static constexpr auto name = FunctionName::name;
    static Status execute(FunctionContext* context, Block& block, size_t result,
                          size_t input_rows_count) {
        auto col_to = ColumnVector<Int64>::create();
        VecDateTimeValue dtv;
        if (dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000,
                              context->impl()->state()->timezone_obj())) {
            reinterpret_cast<VecDateTimeValue*>(&dtv)->set_type(TIME_DATE);
            auto date_packed_int = binary_cast<doris::vectorized::VecDateTimeValue, int64_t>(
                    *reinterpret_cast<VecDateTimeValue*>(&dtv));
            for (int i = 0; i < input_rows_count; i ++) {
                col_to->insert_data(
                        const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
            }
        } else {
            auto invalid_val = 0;
            for (int i = 0; i < input_rows_count; i ++) {
                col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)),
                                    0);
            }
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

template<typename FunctionName>
struct CurrentTimeImpl {
    using ReturnType = DataTypeFloat64;
    static constexpr auto name = FunctionName::name;
    static Status execute(FunctionContext* context, Block& block, size_t result,
                          size_t input_rows_count) {
        auto col_to = ColumnVector<Float64>::create();
        VecDateTimeValue dtv;
        if (dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000,
                              context->impl()->state()->timezone_obj())) {
            double time = dtv.hour() * 3600 + dtv.minute() * 60 + dtv.second();
            for (int i = 0; i < input_rows_count; i ++) {
                col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&time)), 0);
            }
        } else {
            auto invalid_val = 0;
            for (int i = 0; i < input_rows_count; i ++) {
                col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)),
                                    0);
            }
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

struct UtcTimestampImpl {
    using ReturnType = DataTypeDateTime;
    static constexpr auto name = "utc_timestamp";
    static Status execute(FunctionContext* context, Block& block, size_t result,
                          size_t input_rows_count) {
        auto col_to = ColumnVector<Int64>::create();
        VecDateTimeValue dtv;
        if (dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000, "+00:00")) {
            reinterpret_cast<VecDateTimeValue*>(&dtv)->set_type(TIME_DATETIME);
            auto date_packed_int = binary_cast<doris::vectorized::VecDateTimeValue, int64_t>(
                    *reinterpret_cast<VecDateTimeValue*>(&dtv));
            for (int i = 0; i < input_rows_count; i ++) {
                col_to->insert_data(
                        const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
            }
        } else {
            auto invalid_val = 0;
            for (int i = 0; i < input_rows_count; i ++) {
                col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)),
                                    0);
            }
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

} // namespace doris::vectorized
