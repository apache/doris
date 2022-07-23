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

template <TimeUnit unit, typename Arg, typename DateValueType, typename ResultDateValueType,
          typename ResultType>
inline ResultType date_time_add(const Arg& t, Int64 delta, bool& is_null) {
    auto ts_value = binary_cast<Arg, DateValueType>(t);
    TimeInterval interval(unit, delta, false);
    if constexpr (std::is_same_v<VecDateTimeValue, DateValueType> ||
                  std::is_same_v<DateValueType, ResultDateValueType>) {
        is_null = !ts_value.date_add_interval(interval, unit);

        return binary_cast<ResultDateValueType, ResultType>(ts_value);
    } else {
        ResultDateValueType res;
        is_null = !ts_value.date_add_interval(interval, unit, res);

        return binary_cast<ResultDateValueType, ResultType>(res);
    }
}

#define ADD_TIME_FUNCTION_IMPL(CLASS, NAME, UNIT)                                                  \
    template <typename DateType, typename ArgType, typename ResultType>                            \
    struct CLASS {                                                                                 \
        using ReturnType = ResultType;                                                             \
        using ReturnNativeType = std::conditional_t<                                               \
                std::is_same_v<DateType, DataTypeDate> ||                                          \
                        std::is_same_v<DateType, DataTypeDateTime>,                                \
                Int64,                                                                             \
                std::conditional_t<                                                                \
                        std::is_same_v<DateType, DataTypeDateV2>,                                  \
                        std::conditional_t<TimeUnit::UNIT == TimeUnit::HOUR ||                     \
                                                   TimeUnit::UNIT == TimeUnit::MINUTE ||           \
                                                   TimeUnit::UNIT == TimeUnit::SECOND ||           \
                                                   TimeUnit::UNIT == TimeUnit::SECOND_MICROSECOND, \
                                           UInt64, UInt32>,                                        \
                        UInt64>>;                                                                  \
        static constexpr auto name = #NAME;                                                        \
        static constexpr auto is_nullable = true;                                                  \
        static inline ReturnNativeType execute(const ArgType& t, Int64 delta, bool& is_null) {     \
            if constexpr (std::is_same_v<DateType, DataTypeDate> ||                                \
                          std::is_same_v<DateType, DataTypeDateTime>) {                            \
                return date_time_add<TimeUnit::UNIT, ArgType, doris::vectorized::VecDateTimeValue, \
                                     doris::vectorized::VecDateTimeValue, ReturnNativeType>(       \
                        t, delta, is_null);                                                        \
            } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {                       \
                if constexpr (TimeUnit::UNIT == TimeUnit::HOUR ||                                  \
                              TimeUnit::UNIT == TimeUnit::MINUTE ||                                \
                              TimeUnit::UNIT == TimeUnit::SECOND ||                                \
                              TimeUnit::UNIT == TimeUnit::SECOND_MICROSECOND) {                    \
                    return date_time_add<TimeUnit::UNIT, ArgType, DateV2Value<DateV2ValueType>,    \
                                         DateV2Value<DateTimeV2ValueType>, ReturnNativeType>(      \
                            t, delta, is_null);                                                    \
                } else {                                                                           \
                    return date_time_add<TimeUnit::UNIT, ArgType, DateV2Value<DateV2ValueType>,    \
                                         DateV2Value<DateV2ValueType>, ReturnNativeType>(t, delta, \
                                                                                         is_null); \
                }                                                                                  \
                                                                                                   \
            } else {                                                                               \
                return date_time_add<TimeUnit::UNIT, ArgType, DateV2Value<DateTimeV2ValueType>,    \
                                     DateV2Value<DateTimeV2ValueType>, ReturnNativeType>(t, delta, \
                                                                                         is_null); \
            }                                                                                      \
        }                                                                                          \
                                                                                                   \
        static DataTypes get_variadic_argument_types() {                                           \
            return {std::make_shared<DateType>(), std::make_shared<DataTypeInt32>()};              \
        }                                                                                          \
    }

ADD_TIME_FUNCTION_IMPL(AddSecondsImpl, seconds_add, SECOND);
ADD_TIME_FUNCTION_IMPL(AddMinutesImpl, minutes_add, MINUTE);
ADD_TIME_FUNCTION_IMPL(AddHoursImpl, hours_add, HOUR);
ADD_TIME_FUNCTION_IMPL(AddDaysImpl, days_add, DAY);
ADD_TIME_FUNCTION_IMPL(AddWeeksImpl, weeks_add, WEEK);
ADD_TIME_FUNCTION_IMPL(AddMonthsImpl, months_add, MONTH);
ADD_TIME_FUNCTION_IMPL(AddYearsImpl, years_add, YEAR);

template <typename DateType, typename ArgType, typename ResultType>
struct AddQuartersImpl {
    using ReturnType = ResultType;
    using ReturnNativeType = std::conditional_t<
            std::is_same_v<DateType, DataTypeDate> || std::is_same_v<DateType, DataTypeDateTime>,
            Int64, std::conditional_t<std::is_same_v<DateType, DataTypeDateV2>, UInt32, UInt64>>;
    static constexpr auto name = "quarters_add";
    static constexpr auto is_nullable = true;
    static inline ReturnNativeType execute(const ArgType& t, Int64 delta, bool& is_null) {
        if constexpr (std::is_same_v<DateType, DataTypeDate> ||
                      std::is_same_v<DateType, DataTypeDateTime>) {
            return date_time_add<TimeUnit::MONTH, ArgType, doris::vectorized::VecDateTimeValue,
                                 doris::vectorized::VecDateTimeValue, ReturnNativeType>(t, delta,
                                                                                        is_null);
        } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
            return date_time_add<TimeUnit::MONTH, ArgType, DateV2Value<DateV2ValueType>,
                                 DateV2Value<DateV2ValueType>, ReturnNativeType>(t, delta, is_null);
        } else {
            return date_time_add<TimeUnit::MONTH, ArgType, DateV2Value<DateTimeV2ValueType>,
                                 DateV2Value<DateTimeV2ValueType>, ReturnNativeType>(t, delta,
                                                                                     is_null);
        }
    }

    static DataTypes get_variadic_argument_types() { return {std::make_shared<DateType>()}; }
};

template <typename Transform, typename DateType, typename ArgType, typename ResultType>
struct SubtractIntervalImpl {
    using ReturnType = ResultType;
    static constexpr auto is_nullable = true;
    static inline Int64 execute(const ArgType& t, Int64 delta, bool& is_null) {
        return Transform::execute(t, -delta, is_null);
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DateType>(), std::make_shared<DataTypeInt32>()};
    }
};

template <typename DateType, typename ArgType, typename ResultType>
struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl<DateType, ArgType, ResultType>,
                                                  DateType, ArgType, ResultType> {
    static constexpr auto name = "seconds_sub";
};

template <typename DateType, typename ArgType, typename ResultType>
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl<DateType, ArgType, ResultType>,
                                                  DateType, ArgType, ResultType> {
    static constexpr auto name = "minutes_sub";
};

template <typename DateType, typename ArgType, typename ResultType>
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl<DateType, ArgType, ResultType>,
                                                DateType, ArgType, ResultType> {
    static constexpr auto name = "hours_sub";
};

template <typename DateType, typename ArgType, typename ResultType>
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl<DateType, ArgType, ResultType>, DateType,
                                               ArgType, ResultType> {
    static constexpr auto name = "days_sub";
};

template <typename DateType, typename ArgType, typename ResultType>
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl<DateType, ArgType, ResultType>,
                                                DateType, ArgType, ResultType> {
    static constexpr auto name = "weeks_sub";
};

template <typename DateType, typename ArgType, typename ResultType>
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl<DateType, ArgType, ResultType>,
                                                 DateType, ArgType, ResultType> {
    static constexpr auto name = "months_sub";
};

template <typename DateType, typename ArgType, typename ResultType>
struct SubtractQuartersImpl : SubtractIntervalImpl<AddQuartersImpl<DateType, ArgType, ResultType>,
                                                   DateType, ArgType, ResultType> {
    static constexpr auto name = "quarters_sub";
};

template <typename DateType, typename ArgType, typename ResultType>
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl<DateType, ArgType, ResultType>,
                                                DateType, ArgType, ResultType> {
    static constexpr auto name = "years_sub";
};

template <typename DateValueType1, typename DateValueType2, typename DateType1, typename DateType2,
          typename ArgType1, typename ArgType2>
struct DateDiffImpl {
    using ReturnType = DataTypeInt32;
    static constexpr auto name = "datediff";
    static constexpr auto is_nullable = false;
    static inline Int32 execute(const ArgType1& t0, const ArgType2& t1, bool& is_null) {
        const auto& ts0 = reinterpret_cast<const DateValueType1&>(t0);
        const auto& ts1 = reinterpret_cast<const DateValueType2&>(t1);
        is_null = !ts0.is_valid_date() || !ts1.is_valid_date();
        return ts0.daynr() - ts1.daynr();
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DateType1>(), std::make_shared<DateType2>()};
    }
};

template <typename DateValueType1, typename DateValueType2, typename DateType1, typename DateType2,
          typename ArgType1, typename ArgType2>
struct TimeDiffImpl {
    using ReturnType = DataTypeFloat64;
    static constexpr auto name = "timediff";
    static constexpr auto is_nullable = false;
    static inline double execute(const ArgType1& t0, const ArgType2& t1, bool& is_null) {
        const auto& ts0 = reinterpret_cast<const DateValueType1&>(t0);
        const auto& ts1 = reinterpret_cast<const DateValueType2&>(t1);
        is_null = !ts0.is_valid_date() || !ts1.is_valid_date();
        return ts0.second_diff(ts1);
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DateType1>(), std::make_shared<DateType2>()};
    }
};

#define TIME_DIFF_FUNCTION_IMPL(CLASS, NAME, UNIT)                                           \
    template <typename DateValueType1, typename DateValueType2, typename DateType1,          \
              typename DateType2, typename ArgType1, typename ArgType2>                      \
    struct CLASS {                                                                           \
        using ReturnType = DataTypeInt64;                                                    \
        static constexpr auto name = #NAME;                                                  \
        static constexpr auto is_nullable = false;                                           \
        static inline Int64 execute(const ArgType1& t0, const ArgType2& t1, bool& is_null) { \
            const auto& ts0 = reinterpret_cast<const DateValueType1&>(t0);                   \
            const auto& ts1 = reinterpret_cast<const DateValueType2&>(t1);                   \
            is_null = !ts0.is_valid_date() || !ts1.is_valid_date();                          \
            return datetime_diff<TimeUnit::UNIT>(ts1, ts0);                                  \
        }                                                                                    \
                                                                                             \
        static DataTypes get_variadic_argument_types() {                                     \
            return {std::make_shared<DateType1>(), std::make_shared<DateType2>()};           \
        }                                                                                    \
    }

TIME_DIFF_FUNCTION_IMPL(YearsDiffImpl, years_diff, YEAR);
TIME_DIFF_FUNCTION_IMPL(MonthsDiffImpl, months_diff, MONTH);
TIME_DIFF_FUNCTION_IMPL(WeeksDiffImpl, weeks_diff, WEEK);
TIME_DIFF_FUNCTION_IMPL(DaysDiffImpl, days_diff, DAY);
TIME_DIFF_FUNCTION_IMPL(HoursDiffImpl, hours_diff, HOUR);
TIME_DIFF_FUNCTION_IMPL(MintueSDiffImpl, minutes_diff, MINUTE);
TIME_DIFF_FUNCTION_IMPL(SecondsDiffImpl, seconds_diff, SECOND);

#define TIME_FUNCTION_TWO_ARGS_IMPL(CLASS, NAME, FUNCTION)                                  \
    template <typename DateValueType, typename DateType, typename ArgType>                  \
    struct CLASS {                                                                          \
        using ReturnType = DataTypeInt32;                                                   \
        static constexpr auto name = #NAME;                                                 \
        static constexpr auto is_nullable = false;                                          \
        static inline int64_t execute(const ArgType& t0, const Int32 mode, bool& is_null) { \
            const auto& ts0 = reinterpret_cast<const DateValueType&>(t0);                   \
            is_null = !ts0.is_valid_date();                                                 \
            return ts0.FUNCTION;                                                            \
        }                                                                                   \
        static DataTypes get_variadic_argument_types() {                                    \
            return {std::make_shared<DateType>(), std::make_shared<DataTypeInt32>()};       \
        }                                                                                   \
    }

TIME_FUNCTION_TWO_ARGS_IMPL(ToYearWeekTwoArgsImpl, yearweek, year_week(mysql_week_mode(mode)));
TIME_FUNCTION_TWO_ARGS_IMPL(ToWeekTwoArgsImpl, week, week(mysql_week_mode(mode)));

template <typename FromType1, typename FromType2, typename ToType, typename Transform>
struct DateTimeOp {
    // use for (DateTime, DateTime) -> other_type
    static void vector_vector(const PaddedPODArray<FromType1>& vec_from0,
                              const PaddedPODArray<FromType2>& vec_from1,
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
    static void vector_vector(const PaddedPODArray<FromType1>& vec_from0,
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
    static void vector_constant(const PaddedPODArray<FromType1>& vec_from,
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
    static void vector_constant(const PaddedPODArray<FromType1>& vec_from,
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
    static void constant_vector(const FromType1& from, PaddedPODArray<ToType>& vec_to,
                                NullMap& null_map, const IColumn& delta) {
        size_t size = delta.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i) {
            vec_to[i] = Transform::execute(from, delta.get_int(i),
                                           reinterpret_cast<bool&>(null_map[i]));
        }
    }

    static void constant_vector(const FromType1& from, PaddedPODArray<ToType>& vec_to,
                                NullMap& null_map, const PaddedPODArray<FromType2>& delta) {
        size_t size = delta.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i) {
            vec_to[i] = Transform::execute(from, delta[i], reinterpret_cast<bool&>(null_map[i]));
        }
    }
};

template <typename FromType1, typename Transform, typename FromType2 = FromType1>
struct DateTimeAddIntervalImpl {
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        using ToType = typename Transform::ReturnType::FieldType;
        using Op = DateTimeOp<FromType1, FromType2, ToType, Transform>;

        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;
        if (const auto* sources = check_and_get_column<ColumnVector<FromType1>>(source_col.get())) {
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
                            check_and_get_column<ColumnVector<FromType2>>(delta_column)) {
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
                           check_and_get_column_const<ColumnVector<FromType1>>(source_col.get())) {
            auto col_to = ColumnVector<ToType>::create();
            auto null_map = ColumnUInt8::create();

            if (const auto* delta_vec_column = check_and_get_column<ColumnVector<FromType2>>(
                        *block.get_by_position(arguments[1]).column)) {
                Op::constant_vector(sources_const->template get_value<FromType1>(),
                                    col_to->get_data(), null_map->get_data(),
                                    delta_vec_column->get_data());
            } else {
                Op::constant_vector(sources_const->template get_value<FromType2>(),
                                    col_to->get_data(), null_map->get_data(),
                                    *block.get_by_position(arguments[1]).column);
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(null_map));
        } else {
            return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        Transform::name);
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
            if (!is_date_or_datetime(arguments[0].type) &&
                !is_date_v2_or_datetime_v2(arguments[0].type)) {
                LOG(FATAL) << fmt::format(
                        "Illegal type {} of argument of function {}. Should be a date or a date "
                        "with time",
                        arguments[0].type->get_name(), get_name());
            }
        } else {
            if (!WhichDataType(arguments[0].type).is_date_time() ||
                !WhichDataType(arguments[0].type).is_date_time_v2() ||
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
        const IDataType* first_arg_type = block.get_by_position(arguments[0]).type.get();
        const IDataType* second_arg_type = block.get_by_position(arguments[1]).type.get();
        WhichDataType which1(first_arg_type);
        WhichDataType which2(second_arg_type);

        if (which1.is_date() && which2.is_date()) {
            return DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform,
                                           DataTypeDate::FieldType>::execute(block, arguments,
                                                                             result);
        } else if (which1.is_date_time() && which2.is_date()) {
            return DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform,
                                           DataTypeDate::FieldType>::execute(block, arguments,
                                                                             result);
        } else if (which1.is_date_v2() && which2.is_date()) {
            return DateTimeAddIntervalImpl<DataTypeDateV2::FieldType, Transform,
                                           DataTypeDate::FieldType>::execute(block, arguments,
                                                                             result);
        } else if (which1.is_date_time_v2() && which2.is_date()) {
            return DateTimeAddIntervalImpl<DataTypeDateTimeV2::FieldType, Transform,
                                           DataTypeDate::FieldType>::execute(block, arguments,
                                                                             result);
        } else if (which1.is_date() && which2.is_date_time()) {
            return DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform,
                                           DataTypeDateTime::FieldType>::execute(block, arguments,
                                                                                 result);
        } else if (which1.is_date() && which2.is_date_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform,
                                           DataTypeDateV2::FieldType>::execute(block, arguments,
                                                                               result);
        } else if (which1.is_date() && which2.is_date_time_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform,
                                           DataTypeDateTimeV2::FieldType>::execute(block, arguments,
                                                                                   result);
        } else if (which1.is_date_v2() && which2.is_date_time()) {
            return DateTimeAddIntervalImpl<DataTypeDateV2::FieldType, Transform,
                                           DataTypeDateTime::FieldType>::execute(block, arguments,
                                                                                 result);
        } else if (which1.is_date_v2() && which2.is_date_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDateV2::FieldType, Transform,
                                           DataTypeDateV2::FieldType>::execute(block, arguments,
                                                                               result);
        } else if (which1.is_date_time_v2() && which2.is_date_time()) {
            return DateTimeAddIntervalImpl<DataTypeDateTimeV2::FieldType, Transform,
                                           DataTypeDateTime::FieldType>::execute(block, arguments,
                                                                                 result);
        } else if (which1.is_date_time_v2() && which2.is_date_time_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDateTimeV2::FieldType, Transform,
                                           DataTypeDateTimeV2::FieldType>::execute(block, arguments,
                                                                                   result);
        } else if (which1.is_date_time() && which2.is_date_time()) {
            return DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform,
                                           DataTypeDateTime::FieldType>::execute(block, arguments,
                                                                                 result);
        } else if (which1.is_date_time() && which2.is_date_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform,
                                           DataTypeDateV2::FieldType>::execute(block, arguments,
                                                                               result);
        } else if (which1.is_date_time() && which2.is_date_time_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform,
                                           DataTypeDateTimeV2::FieldType>::execute(block, arguments,
                                                                                   result);
        } else if (which1.is_date_v2() && which2.is_date_time_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDateV2::FieldType, Transform,
                                           DataTypeDateTimeV2::FieldType>::execute(block, arguments,
                                                                                   result);
        } else if (which1.is_date_time_v2() && which2.is_date_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDateTimeV2::FieldType, Transform,
                                           DataTypeDateV2::FieldType>::execute(block, arguments,
                                                                               result);
        } else if (which1.is_date()) {
            return DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform>::execute(
                    block, arguments, result);
        } else if (which1.is_date_time()) {
            return DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform>::execute(
                    block, arguments, result);
        } else if (which1.is_date_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDateV2::FieldType, Transform>::execute(
                    block, arguments, result);
        } else if (which1.is_date_time_v2()) {
            return DateTimeAddIntervalImpl<DataTypeDateTimeV2::FieldType, Transform>::execute(
                    block, arguments, result);
        } else {
            return Status::RuntimeError("Illegal type {} of argument of function {}",
                                        block.get_by_position(arguments[0]).type->get_name(),
                                        get_name());
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

template <typename FunctionName>
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
            for (int i = 0; i < input_rows_count; i++) {
                col_to->insert_data(
                        const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
            }
        } else {
            auto invalid_val = 0;
            for (int i = 0; i < input_rows_count; i++) {
                col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)),
                                    0);
            }
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

template <typename FunctionName, typename DateType, typename NativeType>
struct CurrentDateImpl {
    using ReturnType = DateType;
    static constexpr auto name = FunctionName::name;
    static Status execute(FunctionContext* context, Block& block, size_t result,
                          size_t input_rows_count) {
        auto col_to = ColumnVector<NativeType>::create();
        if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
            DateV2Value<DateV2ValueType> dtv;
            if (dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000,
                                  context->impl()->state()->timezone_obj())) {
                auto date_packed_int = binary_cast<DateV2Value<DateV2ValueType>, uint32_t>(
                        *reinterpret_cast<DateV2Value<DateV2ValueType>*>(&dtv));
                for (int i = 0; i < input_rows_count; i++) {
                    col_to->insert_data(
                            const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
                }
            } else {
                auto invalid_val = 0;
                for (int i = 0; i < input_rows_count; i++) {
                    col_to->insert_data(
                            const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)), 0);
                }
            }
        } else {
            VecDateTimeValue dtv;
            if (dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000,
                                  context->impl()->state()->timezone_obj())) {
                reinterpret_cast<VecDateTimeValue*>(&dtv)->set_type(TIME_DATE);
                auto date_packed_int = binary_cast<doris::vectorized::VecDateTimeValue, int64_t>(
                        *reinterpret_cast<VecDateTimeValue*>(&dtv));
                for (int i = 0; i < input_rows_count; i++) {
                    col_to->insert_data(
                            const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
                }
            } else {
                auto invalid_val = 0;
                for (int i = 0; i < input_rows_count; i++) {
                    col_to->insert_data(
                            const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)), 0);
                }
            }
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

template <typename FunctionName>
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
            for (int i = 0; i < input_rows_count; i++) {
                col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&time)), 0);
            }
        } else {
            auto invalid_val = 0;
            for (int i = 0; i < input_rows_count; i++) {
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
            for (int i = 0; i < input_rows_count; i++) {
                col_to->insert_data(
                        const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
            }
        } else {
            auto invalid_val = 0;
            for (int i = 0; i < input_rows_count; i++) {
                col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)),
                                    0);
            }
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

template <typename FunctionName>
class CurrentDateFunctionBuilder : public FunctionBuilderImpl {
public:
    explicit CurrentDateFunctionBuilder() = default;

    String get_name() const override { return FunctionName::name; }
    size_t get_number_of_arguments() const override { return 0; }

protected:
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeDate>());
    }
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeDate>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    FunctionBasePtr build_impl(const ColumnsWithTypeAndName& arguments,
                               const DataTypePtr& return_type) const override {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) data_types[i] = arguments[i].type;
        if (is_date_v2(return_type)) {
            auto function = FunctionCurrentDateOrDateTime<
                    CurrentDateImpl<FunctionName, DataTypeDateV2, UInt32>>::create();
            return std::make_shared<DefaultFunction>(function, data_types, return_type);
        } else if (is_date_time_v2(return_type)) {
            auto function = FunctionCurrentDateOrDateTime<
                    CurrentDateImpl<FunctionName, DataTypeDateTimeV2, UInt64>>::create();
            return std::make_shared<DefaultFunction>(function, data_types, return_type);
        } else {
            auto function = FunctionCurrentDateOrDateTime<
                    CurrentDateImpl<FunctionName, DataTypeDate, Int64>>::create();
            return std::make_shared<DefaultFunction>(function, data_types, return_type);
        }
    }
};

} // namespace doris::vectorized
