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

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/cast_set.h"
#include "common/compiler_util.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "util/binary_cast.hpp"
#include "util/datetype_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

/// because all these functions(xxx_add/xxx_sub) defined in FE use Integer as the second value
///  so Int32 as delta is enough. For upstream(FunctionDateOrDateTimeComputation) we also could use Int32.

template <TimeUnit unit, typename ArgType, typename ReturnType,
          typename InputNativeType = ArgType::FieldType,
          typename ReturnNativeType = ReturnType::FieldType>
ReturnNativeType date_time_add(const InputNativeType& t, Int32 delta, bool& is_null) {
    using DateValueType = date_cast::TypeToValueTypeV<ArgType>;
    using ResultDateValueType = date_cast::TypeToValueTypeV<ReturnType>;
    // e.g.: for DatatypeDatetimeV2, cast from u64 to DateV2Value<DateTimeV2ValueType>
    auto ts_value = binary_cast<InputNativeType, DateValueType>(t);
    TimeInterval interval(unit, delta, false);
    if constexpr (std::is_same_v<ArgType, ReturnType>) {
        is_null = !(ts_value.template date_add_interval<unit>(interval));
        // here DateValueType = ResultDateValueType
        return binary_cast<DateValueType, ReturnNativeType>(ts_value);
    } else {
        // this is for HOUR/MINUTE/SECOND/MS_ADD for datev2. got datetimev2 but not datev2. so need this two-arg reload to assign.
        ResultDateValueType res;
        is_null = !(ts_value.template date_add_interval<unit>(interval, res));

        return binary_cast<ResultDateValueType, ReturnNativeType>(res);
    }
}

#define ADD_TIME_FUNCTION_IMPL(CLASS, NAME, UNIT)                                                   \
    template <typename ArgType>                                                                     \
    struct CLASS {                                                                                  \
        /* for V1 type all return Datetime. for V2 type, if unit <= hour, increase to DatetimeV2 */ \
        using ReturnType = std::conditional_t<                                                      \
                date_cast::IsV1<ArgType>(), DataTypeDateTime,                                       \
                std::conditional_t<                                                                 \
                        std::is_same_v<ArgType, DataTypeDateV2>,                                    \
                        std::conditional_t<TimeUnit::UNIT == TimeUnit::HOUR ||                      \
                                                   TimeUnit::UNIT == TimeUnit::MINUTE ||            \
                                                   TimeUnit::UNIT == TimeUnit::SECOND ||            \
                                                   TimeUnit::UNIT == TimeUnit::SECOND_MICROSECOND,  \
                                           DataTypeDateTimeV2, DataTypeDateV2>,                     \
                        DataTypeDateTimeV2>>;                                                       \
        using ReturnNativeType = ReturnType::FieldType;                                             \
        using InputNativeType = ArgType::FieldType;                                                 \
        static constexpr auto name = #NAME;                                                         \
        static constexpr auto is_nullable = true;                                                   \
        static inline ReturnNativeType execute(const InputNativeType& t, Int32 delta,               \
                                               bool& is_null) {                                     \
            return date_time_add<TimeUnit::UNIT, ArgType, ReturnType>(t, delta, is_null);           \
        }                                                                                           \
                                                                                                    \
        static DataTypes get_variadic_argument_types() {                                            \
            return {std::make_shared<ArgType>(), std::make_shared<DataTypeInt32>()};                \
        }                                                                                           \
    }

ADD_TIME_FUNCTION_IMPL(AddMicrosecondsImpl, microseconds_add, MICROSECOND);
ADD_TIME_FUNCTION_IMPL(AddMillisecondsImpl, milliseconds_add, MILLISECOND);
ADD_TIME_FUNCTION_IMPL(AddSecondsImpl, seconds_add, SECOND);
ADD_TIME_FUNCTION_IMPL(AddMinutesImpl, minutes_add, MINUTE);
ADD_TIME_FUNCTION_IMPL(AddHoursImpl, hours_add, HOUR);
ADD_TIME_FUNCTION_IMPL(AddDaysImpl, days_add, DAY);
ADD_TIME_FUNCTION_IMPL(AddWeeksImpl, weeks_add, WEEK);
ADD_TIME_FUNCTION_IMPL(AddMonthsImpl, months_add, MONTH);
ADD_TIME_FUNCTION_IMPL(AddYearsImpl, years_add, YEAR);

template <typename ArgType>
struct AddQuartersImpl {
    using ReturnType =
            std::conditional_t<std::is_same_v<ArgType, DataTypeDate> ||
                                       std::is_same_v<ArgType, DataTypeDateTime>,
                               DataTypeDateTime,
                               std::conditional_t<std::is_same_v<ArgType, DataTypeDateV2>,
                                                  DataTypeDateV2, DataTypeDateTimeV2>>;
    using InputNativeType = ArgType::FieldType;
    using ReturnNativeType = ReturnType::FieldType;
    static constexpr auto name = "quarters_add";
    static constexpr auto is_nullable = true;
    static inline ReturnNativeType execute(const InputNativeType& t, Int32 delta, bool& is_null) {
        return date_time_add<TimeUnit::MONTH, ArgType, ReturnType>(t, 3 * delta, is_null);
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<ArgType>(), std::make_shared<DataTypeInt32>()};
    }
};

template <typename Transform, typename DateType>
struct SubtractIntervalImpl {
    using ReturnType = typename Transform::ReturnType;
    using InputNativeType = typename Transform::InputNativeType;
    using ReturnNativeType = typename Transform::ReturnNativeType;
    static constexpr auto is_nullable = true;
    static inline ReturnNativeType execute(const InputNativeType& t, Int32 delta, bool& is_null) {
        return Transform::execute(t, -delta, is_null);
    }

    static DataTypes get_variadic_argument_types() {
        return Transform::get_variadic_argument_types();
    }
};

template <typename DateType>
struct SubtractMicrosecondsImpl : SubtractIntervalImpl<AddMicrosecondsImpl<DateType>, DateType> {
    static constexpr auto name = "microseconds_sub";
};

template <typename DateType>
struct SubtractMillisecondsImpl : SubtractIntervalImpl<AddMillisecondsImpl<DateType>, DateType> {
    static constexpr auto name = "milliseconds_sub";
};

template <typename DateType>
struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl<DateType>, DateType> {
    static constexpr auto name = "seconds_sub";
};

template <typename DateType>
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl<DateType>, DateType> {
    static constexpr auto name = "minutes_sub";
};

template <typename DateType>
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl<DateType>, DateType> {
    static constexpr auto name = "hours_sub";
};

template <typename DateType>
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl<DateType>, DateType> {
    static constexpr auto name = "days_sub";
};

template <typename DateType>
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl<DateType>, DateType> {
    static constexpr auto name = "weeks_sub";
};

template <typename DateType>
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl<DateType>, DateType> {
    static constexpr auto name = "months_sub";
};

template <typename DateType>
struct SubtractQuartersImpl : SubtractIntervalImpl<AddQuartersImpl<DateType>, DateType> {
    static constexpr auto name = "quarters_sub";
};

template <typename DateType>
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl<DateType>, DateType> {
    static constexpr auto name = "years_sub";
};

#define DECLARE_DATE_FUNCTIONS(NAME, FN_NAME, RETURN_TYPE, STMT)                                  \
    template <typename DateType1, typename DateType2>                                             \
    struct NAME {                                                                                 \
        using NativeType1 = DateType1::FieldType;                                                 \
        using NativeType2 = DateType2::FieldType;                                                 \
        using DateValueType1 = date_cast::TypeToValueTypeV<DateType1>;                            \
        using DateValueType2 = date_cast::TypeToValueTypeV<DateType2>;                            \
        using ReturnType = RETURN_TYPE;                                                           \
                                                                                                  \
        static constexpr auto name = #FN_NAME;                                                    \
        static constexpr auto is_nullable = false;                                                \
        static inline ReturnType::FieldType execute(const NativeType1& t0, const NativeType2& t1, \
                                                    bool& is_null) {                              \
            const auto& ts0 = reinterpret_cast<const DateValueType1&>(t0);                        \
            const auto& ts1 = reinterpret_cast<const DateValueType2&>(t1);                        \
            is_null = !ts0.is_valid_date() || !ts1.is_valid_date();                               \
            return (STMT);                                                                        \
        }                                                                                         \
        static DataTypes get_variadic_argument_types() {                                          \
            return {std::make_shared<DateType1>(), std::make_shared<DateType2>()};                \
        }                                                                                         \
    };

DECLARE_DATE_FUNCTIONS(DateDiffImpl, datediff, DataTypeInt32, (ts0.daynr() - ts1.daynr()));
// DECLARE_DATE_FUNCTIONS(TimeDiffImpl, timediff, DataTypeTime, ts0.second_diff(ts1));
// Expands to below here because it use Time type which need some special deal.
template <typename DateType1, typename DateType2>
struct TimeDiffImpl {
    using NativeType1 = date_cast::TypeToValueTypeV<DateType1>;
    using NativeType2 = date_cast::TypeToValueTypeV<DateType2>;
    using ArgType1 = DateType1::FieldType;
    using ArgType2 = DateType2::FieldType;
    static constexpr bool UsingTimev2 =
            date_cast::IsV2<DateType1>() || date_cast::IsV2<DateType2>();

    using ReturnType = DataTypeTimeV2; // TimeV1Type also use double as native type. same as v2.

    static constexpr auto name = "timediff";
    static constexpr int64_t limit_value = 3020399000000; // 838:59:59 convert to microsecond
    static inline ReturnType::FieldType execute(const ArgType1& t0, const ArgType2& t1,
                                                bool& is_null) {
        const auto& ts0 = reinterpret_cast<const NativeType1&>(t0);
        const auto& ts1 = reinterpret_cast<const NativeType2&>(t1);
        is_null = !ts0.is_valid_date() || !ts1.is_valid_date();
        if constexpr (UsingTimev2) {
            // refer to https://dev.mysql.com/doc/refman/5.7/en/time.html
            // the time type value between '-838:59:59' and '838:59:59', so the return value should limited
            int64_t diff_m = ts0.microsecond_diff(ts1);
            if (diff_m > limit_value) {
                return (double)limit_value;
            } else if (diff_m < -1 * limit_value) {
                return (double)(-1 * limit_value);
            } else {
                return (double)diff_m;
            }
        } else {
            return TimeValue::from_second(ts0.second_diff(ts1));
        }
    }
    static DataTypes get_variadic_argument_types() {
        return {std ::make_shared<DateType1>(), std ::make_shared<DateType2>()};
    }
};
#define TIME_DIFF_FUNCTION_IMPL(CLASS, NAME, UNIT) \
    DECLARE_DATE_FUNCTIONS(CLASS, NAME, DataTypeInt64, datetime_diff<TimeUnit::UNIT>(ts1, ts0))

// all these functions implemented by datediff
TIME_DIFF_FUNCTION_IMPL(YearsDiffImpl, years_diff, YEAR);
TIME_DIFF_FUNCTION_IMPL(MonthsDiffImpl, months_diff, MONTH);
TIME_DIFF_FUNCTION_IMPL(WeeksDiffImpl, weeks_diff, WEEK);
TIME_DIFF_FUNCTION_IMPL(DaysDiffImpl, days_diff, DAY);
TIME_DIFF_FUNCTION_IMPL(HoursDiffImpl, hours_diff, HOUR);
TIME_DIFF_FUNCTION_IMPL(MintuesDiffImpl, minutes_diff, MINUTE);
TIME_DIFF_FUNCTION_IMPL(SecondsDiffImpl, seconds_diff, SECOND);
TIME_DIFF_FUNCTION_IMPL(MilliSecondsDiffImpl, milliseconds_diff, MILLISECOND);
TIME_DIFF_FUNCTION_IMPL(MicroSecondsDiffImpl, microseconds_diff, MICROSECOND);

#define TIME_FUNCTION_TWO_ARGS_IMPL(CLASS, NAME, FUNCTION, RETURN_TYPE)                  \
    template <typename DateType>                                                         \
    struct CLASS {                                                                       \
        using ArgType = DateType::FieldType;                                             \
        using DateValueType = date_cast::TypeToValueTypeV<DateType>;                     \
        using ReturnType = RETURN_TYPE;                                                  \
                                                                                         \
        static constexpr auto name = #NAME;                                              \
        static constexpr auto is_nullable = false;                                       \
        static inline ReturnType::FieldType execute(const ArgType& t0, const Int32 mode, \
                                                    bool& is_null) {                     \
            const auto& ts0 = reinterpret_cast<const DateValueType&>(t0);                \
            is_null = !ts0.is_valid_date();                                              \
            return ts0.FUNCTION;                                                         \
        }                                                                                \
        static DataTypes get_variadic_argument_types() {                                 \
            return {std::make_shared<DateType>(), std::make_shared<DataTypeInt32>()};    \
        }                                                                                \
    }

TIME_FUNCTION_TWO_ARGS_IMPL(ToYearWeekTwoArgsImpl, yearweek, year_week(mysql_week_mode(mode)),
                            DataTypeInt32);
TIME_FUNCTION_TWO_ARGS_IMPL(ToWeekTwoArgsImpl, week, week(mysql_week_mode(mode)), DataTypeInt8);

// only use for FunctionDateOrDateTimeComputation. FromTypes are NativeTypes.
template <typename DataType0, typename DataType1, typename ToType, typename Transform>
struct DateTimeOp {
    using NativeType0 = DataType0::FieldType;
    using NativeType1 = DataType1::FieldType;
    using ValueType0 = date_cast::TypeToValueTypeV<DataType0>;
    // arg1 maybe just delta value(e.g. DataTypeInt32, not datelike type)
    constexpr static bool CastType1 = std::is_same_v<DataType1, DataTypeDate> ||
                                      std::is_same_v<DataType1, DataTypeDateTime> ||
                                      std::is_same_v<DataType1, DataTypeDateV2> ||
                                      std::is_same_v<DataType1, DataTypeDateTimeV2>;

    static void throw_out_of_bound(NativeType0 arg0, NativeType1 arg1) {
        auto value0 = binary_cast<NativeType0, ValueType0>(arg0);
        char buf0[40];
        char* end0 = value0.to_string(buf0);
        if constexpr (CastType1) {
            auto value1 = binary_cast<NativeType1, date_cast::TypeToValueTypeV<DataType1>>(arg1);
            char buf1[40];
            char* end1 = value1.to_string(buf1);
            throw Exception(ErrorCode::OUT_OF_BOUND, "Operation {} of {}, {} out of range",
                            Transform::name, std::string_view {buf0, end0 - 1},
                            std::string_view {buf1, end1 - 1}); // minus 1 to skip /0
        } else {
            throw Exception(ErrorCode::OUT_OF_BOUND, "Operation {} of {}, {} out of range",
                            Transform::name, std::string_view {buf0, end0 - 1}, arg1);
        }
    }

    // execute on the null value's nested value may cause false positive exception, so use nullmaps to skip them.
    static void vector_vector(const PaddedPODArray<NativeType0>& vec_from0,
                              const PaddedPODArray<NativeType1>& vec_from1,
                              PaddedPODArray<ToType>& vec_to, const NullMap* nullmap0,
                              const NullMap* nullmap1) {
        size_t size = vec_from0.size();
        vec_to.resize(size);
        bool invalid = false;

        for (size_t i = 0; i < size; ++i) {
            if ((nullmap0 && (*nullmap0)[i]) || (nullmap1 && (*nullmap1)[i])) [[unlikely]] {
                continue;
            }
            vec_to[i] = Transform::execute(vec_from0[i], vec_from1[i], invalid);

            if (UNLIKELY(invalid)) {
                throw_out_of_bound(vec_from0[i], vec_from1[i]);
            }
        }
    }

    static void vector_constant(const PaddedPODArray<NativeType0>& vec_from,
                                PaddedPODArray<ToType>& vec_to, const NativeType1& delta,
                                const NullMap* nullmap0, const NullMap* nullmap1) {
        if (nullmap1 && (*nullmap1)[0]) [[unlikely]] {
            return;
        }
        size_t size = vec_from.size();
        vec_to.resize(size);
        bool invalid = false;

        for (size_t i = 0; i < size; ++i) {
            if (nullmap0 && (*nullmap0)[i]) [[unlikely]] {
                continue;
            }
            vec_to[i] = Transform::execute(vec_from[i], delta, invalid);

            if (UNLIKELY(invalid)) {
                throw_out_of_bound(vec_from[i], delta);
            }
        }
    }

    static void constant_vector(const NativeType0& from, PaddedPODArray<ToType>& vec_to,
                                const PaddedPODArray<NativeType1>& delta, const NullMap* nullmap0,
                                const NullMap* nullmap1) {
        if (nullmap0 && (*nullmap0)[0]) [[unlikely]] {
            return;
        }
        size_t size = delta.size();
        vec_to.resize(size);
        bool invalid = false;

        for (size_t i = 0; i < size; ++i) {
            if (nullmap1 && (*nullmap1)[i]) [[unlikely]] {
                continue;
            }
            vec_to[i] = Transform::execute(from, delta[i], invalid);

            if (UNLIKELY(invalid)) {
                throw_out_of_bound(from, delta[i]);
            }
        }
    }
};

// Used for date(time) add/sub date(time)/integer. the input types are variadic and dispatch in execute. the return type is
//  decided by Transform
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
        if constexpr (has_variadic_argument) {
            return Transform::get_variadic_argument_types();
        }
        return {};
    }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(typename Transform::ReturnType);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& first_arg_type = block.get_by_position(arguments[0]).type;
        const auto& second_arg_type = block.get_by_position(arguments[1]).type;
        WhichDataType which1(remove_nullable(first_arg_type));
        WhichDataType which2(remove_nullable(second_arg_type));

        /// now dispatch with the two arguments' type. no need to consider return type because the same arguments decide a
        /// unique return type which could be extracted from Transform.

        // for all `xxx_add/sub`, the second arg is int32.
        // for `week/yearweek`, if it has the second arg, it's int32.
        // in these situations, the first would be any datelike type.
        if (which2.is_int32()) {
            switch (which1.idx) {
            case TypeIndex::Date:
                return execute_inner<DataTypeDate, DataTypeInt32>(block, arguments, result,
                                                                  input_rows_count);
                break;
            case TypeIndex::DateTime:
                return execute_inner<DataTypeDateTime, DataTypeInt32>(block, arguments, result,
                                                                      input_rows_count);
                break;
            case TypeIndex::DateV2:
                return execute_inner<DataTypeDateV2, DataTypeInt32>(block, arguments, result,
                                                                    input_rows_count);
                break;
            case TypeIndex::DateTimeV2:
                return execute_inner<DataTypeDateTimeV2, DataTypeInt32>(block, arguments, result,
                                                                        input_rows_count);
                break;
            default:
                return Status::InternalError("Illegal argument {} and {} of function {}",
                                             block.get_by_position(arguments[0]).type->get_name(),
                                             block.get_by_position(arguments[1]).type->get_name(),
                                             get_name());
            }
        }
        // then consider datelike - datelike. everything is possible here as well.
        // for `xxx_diff`, every combination of V2 is possible. but for V1 we only support Datetime - Datetime
        if (which1.is_date_v2() && which2.is_date_v2()) {
            return execute_inner<DataTypeDateV2, DataTypeDateV2>(block, arguments, result,
                                                                 input_rows_count);
        } else if (which1.is_date_time_v2() && which2.is_date_time_v2()) {
            return execute_inner<DataTypeDateTimeV2, DataTypeDateTimeV2>(block, arguments, result,
                                                                         input_rows_count);
        } else if (which1.is_date_v2() && which2.is_date_time_v2()) {
            return execute_inner<DataTypeDateV2, DataTypeDateTimeV2>(block, arguments, result,
                                                                     input_rows_count);
        } else if (which1.is_date_time_v2() && which2.is_date_v2()) {
            return execute_inner<DataTypeDateTimeV2, DataTypeDateV2>(block, arguments, result,
                                                                     input_rows_count);
        } else if (which1.is_date_time() && which2.is_date_time()) {
            return execute_inner<DataTypeDateTime, DataTypeDateTime>(block, arguments, result,
                                                                     input_rows_count);
        }
        return Status::InternalError("Illegal argument {} and {} of function {}",
                                     block.get_by_position(arguments[0]).type->get_name(),
                                     block.get_by_position(arguments[1]).type->get_name(),
                                     get_name());
    }

    template <typename DataType0, typename DataType1>
    static Status execute_inner(Block& block, const ColumnNumbers& arguments, uint32_t result,
                                size_t input_rows_count) {
        using NativeType0 = DataType0::FieldType;
        using NativeType1 = DataType1::FieldType;
        using ResFieldType = typename Transform::ReturnType::FieldType;
        using Op = DateTimeOp<DataType0, DataType1, ResFieldType, Transform>;

        auto get_null_map = [](const ColumnPtr& col) -> const NullMap* {
            if (col->is_nullable()) {
                return &static_cast<const ColumnNullable&>(*col).get_null_map_data();
            }
            // Const(Nullable)
            if (const auto* const_col = check_and_get_column<ColumnConst>(col.get());
                const_col != nullptr && const_col->is_concrete_nullable()) {
                return &static_cast<const ColumnNullable&>(const_col->get_data_column())
                                .get_null_map_data();
            }
            return nullptr;
        };

        //ATTN: those null maps may be nullmap of ColumnConst(only 1 row)
        // src column is always datelike type.
        ColumnPtr& col0 = block.get_by_position(arguments[0]).column;
        const NullMap* nullmap0 = get_null_map(col0);
        // the second column may be delta column(xx_add/sub) or datelike column(xxx_diff)
        ColumnPtr& col1 = block.get_by_position(arguments[1]).column;
        const NullMap* nullmap1 = get_null_map(col1);

        // if null wrapped, extract nested column as src_nested_col
        const ColumnPtr src_nested_col = remove_nullable(col0);
        const auto result_nullable = block.get_by_position(result).type->is_nullable();
        auto res_col = ColumnVector<ResFieldType>::create();

        // vector-const or vector-vector
        if (const auto* sources =
                    check_and_get_column<ColumnVector<NativeType0>>(src_nested_col.get())) {
            const ColumnPtr nest_col1 = remove_nullable(col1);
            bool rconst = false;
            // vector-const
            if (const auto* nest_col1_const = check_and_get_column<ColumnConst>(*nest_col1)) {
                rconst = true;
                const auto col1_inside_const = assert_cast<const ColumnVector<NativeType1>&>(
                        nest_col1_const->get_data_column());
                Op::vector_constant(sources->get_data(), res_col->get_data(),
                                    col1_inside_const.get_data()[0], nullmap0, nullmap1);
            } else { // vector-vector
                const auto concrete_col1 =
                        assert_cast<const ColumnVector<NativeType1>&>(*nest_col1);
                Op::vector_vector(sources->get_data(), concrete_col1.get_data(),
                                  res_col->get_data(), nullmap0, nullmap1);
            }

            // update result nullmap with inputs
            if (result_nullable) {
                auto null_map = ColumnBool::create(input_rows_count, 0);
                NullMap& result_null_map = assert_cast<ColumnBool&>(*null_map).get_data();
                if (nullmap0) {
                    VectorizedUtils::update_null_map(result_null_map, *nullmap0);
                }
                if (nullmap1) {
                    VectorizedUtils::update_null_map(result_null_map, *nullmap1, rconst);
                }
                block.get_by_position(result).column =
                        ColumnNullable::create(std::move(res_col), std::move(null_map));
            } else {
                block.replace_by_position(result, std::move(res_col));
            }
        } else if (const auto* sources_const =
                           check_and_get_column_const<ColumnVector<NativeType0>>(
                                   src_nested_col.get())) {
            // const-vector
            const auto col0_inside_const =
                    assert_cast<const ColumnVector<NativeType0>&>(sources_const->get_data_column());
            const ColumnPtr nested_col1 = remove_nullable(col1);
            const auto concrete_col1 = assert_cast<const ColumnVector<NativeType1>&>(*nested_col1);
            Op::constant_vector(col0_inside_const.get_data()[0], res_col->get_data(),
                                concrete_col1.get_data(), nullmap0, nullmap1);

            // update result nullmap with inputs
            if (result_nullable) {
                auto null_map = ColumnBool::create(input_rows_count, 0);
                NullMap& result_null_map = assert_cast<ColumnBool&>(*null_map).get_data();
                if (nullmap0) {
                    VectorizedUtils::update_null_map(result_null_map, *nullmap0, true);
                }
                if (nullmap1) { // no const-const here. default impl deal it.
                    VectorizedUtils::update_null_map(result_null_map, *nullmap1);
                }
                block.get_by_position(result).column =
                        ColumnNullable::create(std::move(res_col), std::move(null_map));
            } else {
                block.replace_by_position(result, std::move(res_col));
            }
        } else { // no const-const here. default impl deal it.
            return Status::InternalError(
                    "Illegel columns for function {}:\n1: {} with type {}\n2: {} with type {}",
                    Transform::name, block.get_by_position(arguments[0]).name,
                    block.get_by_position(arguments[0]).type->get_name(),
                    block.get_by_position(arguments[1]).name,
                    block.get_by_position(arguments[1]).type->get_name());
        }
        return Status::OK();
    }
};

template <typename FunctionImpl>
class FunctionCurrentDateOrDateTime : public IFunction {
public:
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<FunctionImpl>()))>;

    static constexpr auto name = FunctionImpl::name;
    static FunctionPtr create() { return std::make_shared<FunctionCurrentDateOrDateTime>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<typename FunctionImpl::ReturnType>();
    }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return FunctionImpl::get_variadic_argument_types();
        }
        return {};
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return FunctionImpl::execute(context, block, arguments, result, input_rows_count);
    }
};

template <typename FunctionName, bool WithPrecision>
struct CurrentDateTimeImpl {
    static constexpr auto name = FunctionName::name;
    using ReturnType = std::conditional_t<WithPrecision, DataTypeDateTimeV2, DataTypeDateTime>;

    static DataTypes get_variadic_argument_types() {
        if constexpr (WithPrecision) {
            return {std::make_shared<DataTypeInt32>()};
        } else {
            return {};
        }
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        WhichDataType which(remove_nullable(block.get_by_position(result).type));
        if constexpr (WithPrecision) {
            DCHECK(which.is_date_time_v2() || which.is_date_v2());
            if (which.is_date_time_v2()) {
                return executeImpl<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        context, block, arguments, result, input_rows_count);
            } else {
                return executeImpl<DateV2Value<DateV2ValueType>, UInt32>(context, block, arguments,
                                                                         result, input_rows_count);
            }
        } else {
            if (which.is_date_time_v2()) {
                return executeImpl<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        context, block, arguments, result, input_rows_count);
            } else if (which.is_date_v2()) {
                return executeImpl<DateV2Value<DateV2ValueType>, UInt32>(context, block, arguments,
                                                                         result, input_rows_count);
            } else {
                return executeImpl<VecDateTimeValue, Int64>(context, block, arguments, result,
                                                            input_rows_count);
            }
        }
    }

    template <typename DateValueType, typename NativeType>
    static Status executeImpl(FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, uint32_t result,
                              size_t input_rows_count) {
        auto col_to = ColumnVector<NativeType>::create();
        DateValueType dtv;
        bool use_const;
        if constexpr (WithPrecision) {
            if (const auto* const_column = check_and_get_column<ColumnConst>(
                        block.get_by_position(arguments[0]).column.get())) {
                int64_t scale = const_column->get_int(0);
                dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                                  context->state()->nano_seconds(),
                                  context->state()->timezone_obj(), scale);
                if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                    reinterpret_cast<DateValueType*>(&dtv)->set_type(TIME_DATETIME);
                }
                auto date_packed_int = binary_cast<DateValueType, NativeType>(dtv);
                col_to->insert_data(
                        const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);

                use_const = true;
            } else if (const auto* nullable_column = check_and_get_column<ColumnNullable>(
                               block.get_by_position(arguments[0]).column.get())) {
                const auto& null_map = nullable_column->get_null_map_data();
                const auto& nested_column = assert_cast<const ColumnInt32*>(
                        nullable_column->get_nested_column_ptr().get());
                for (int i = 0; i < input_rows_count; i++) {
                    if (!null_map[i]) {
                        dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                                          context->state()->nano_seconds(),
                                          context->state()->timezone_obj(),
                                          nested_column->get_element(i));
                        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                            reinterpret_cast<DateValueType*>(&dtv)->set_type(TIME_DATETIME);
                        }
                        auto date_packed_int = binary_cast<DateValueType, NativeType>(dtv);
                        col_to->insert_data(
                                const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)),
                                0);
                    } else {
                        auto invalid_val = 0;
                        col_to->insert_data(
                                const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)), 0);
                    }
                }
                use_const = false;
            } else {
                const auto* int_column = assert_cast<const ColumnInt32*>(
                        block.get_by_position(arguments[0]).column.get());
                for (int i = 0; i < input_rows_count; i++) {
                    dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                                      context->state()->nano_seconds(),
                                      context->state()->timezone_obj(), int_column->get_element(i));
                    if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                        reinterpret_cast<DateValueType*>(&dtv)->set_type(TIME_DATETIME);
                    }
                    auto date_packed_int = binary_cast<DateValueType, NativeType>(dtv);
                    col_to->insert_data(
                            const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
                }
                use_const = false;
            }
        } else {
            dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                              context->state()->timezone_obj());
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                reinterpret_cast<DateValueType*>(&dtv)->set_type(TIME_DATETIME);
            }
            auto date_packed_int = binary_cast<DateValueType, NativeType>(dtv);
            col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)),
                                0);
            use_const = true;
        }

        if (use_const) {
            block.get_by_position(result).column =
                    ColumnConst::create(std::move(col_to), input_rows_count);
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

template <typename FunctionName, typename DateType, typename NativeType>
struct CurrentDateImpl {
    using ReturnType = DateType;
    static constexpr auto name = FunctionName::name;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        auto col_to = ColumnVector<NativeType>::create();
        if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
            DateV2Value<DateV2ValueType> dtv;
            dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                              context->state()->timezone_obj());
            auto date_packed_int = binary_cast<DateV2Value<DateV2ValueType>, uint32_t>(
                    *reinterpret_cast<DateV2Value<DateV2ValueType>*>(&dtv));
            col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)),
                                0);
        } else {
            VecDateTimeValue dtv;
            dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                              context->state()->timezone_obj());
            reinterpret_cast<VecDateTimeValue*>(&dtv)->set_type(TIME_DATE);
            auto date_packed_int = binary_cast<doris::VecDateTimeValue, int64_t>(
                    *reinterpret_cast<VecDateTimeValue*>(&dtv));
            col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)),
                                0);
        }
        block.get_by_position(result).column =
                ColumnConst::create(std::move(col_to), input_rows_count);
        return Status::OK();
    }
};

template <typename FunctionName>
struct CurrentTimeImpl {
    using ReturnType = DataTypeTimeV2;
    static constexpr auto name = FunctionName::name;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        auto col_to = ColumnFloat64::create();
        VecDateTimeValue dtv;
        dtv.from_unixtime(context->state()->timestamp_ms() / 1000,
                          context->state()->timezone_obj());
        auto time = TimeValue::make_time(dtv.hour(), dtv.minute(), dtv.second());
        col_to->insert_value(time);
        block.get_by_position(result).column =
                ColumnConst::create(std::move(col_to), input_rows_count);
        return Status::OK();
    }
};

struct TimeToSecImpl {
    // rethink the func should return int32
    using ReturnType = DataTypeInt32;
    static constexpr auto name = "time_to_sec";
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        auto res_col = ColumnInt32::create(input_rows_count);
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnFloat64&>(*arg_col);

        auto& res_data = res_col->get_data();
        for (int i = 0; i < input_rows_count; ++i) {
            res_data[i] = cast_set<int>(static_cast<int64_t>(column_data.get_element(i)) /
                                        (TimeValue::ONE_SECOND_MICROSECONDS));
        }
        block.replace_by_position(result, std::move(res_col));

        return Status::OK();
    }
};

struct SecToTimeImpl {
    using ReturnType = DataTypeTimeV2;
    static constexpr auto name = "sec_to_time";
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnInt32&>(*arg_col);

        auto res_col = ColumnFloat64::create(input_rows_count);
        auto& res_data = res_col->get_data();
        for (int i = 0; i < input_rows_count; ++i) {
            res_data[i] = TimeValue::from_second(column_data.get_element(i));
        }

        block.replace_by_position(result, std::move(res_col));
        return Status::OK();
    }
};
struct MicroSec {
    static constexpr auto name = "from_microsecond";
    static constexpr Int64 ratio = 1000000;
};
struct MilliSec {
    static constexpr auto name = "from_millisecond";
    static constexpr Int64 ratio = 1000;
};
struct Sec {
    static constexpr auto name = "from_second";
    static constexpr Int64 ratio = 1;
};
template <typename Impl>
struct TimestampToDateTime : IFunction {
    using ReturnType = DataTypeDateTimeV2;
    static constexpr auto name = Impl::name;
    static constexpr Int64 ratio_to_micro = (1000 * 1000) / Impl::ratio;
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<ReturnType>());
    }

    static FunctionPtr create() { return std::make_shared<TimestampToDateTime<Impl>>(); }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnInt64&>(*arg_col);
        auto res_col = ColumnUInt64::create();
        auto null_vector = ColumnVector<UInt8>::create();
        res_col->get_data().resize_fill(input_rows_count, 0);
        null_vector->get_data().resize_fill(input_rows_count, false);

        NullMap& null_map = null_vector->get_data();
        auto& res_data = res_col->get_data();
        const cctz::time_zone& time_zone = context->state()->timezone_obj();

        for (int i = 0; i < input_rows_count; ++i) {
            Int64 value = column_data.get_element(i);
            if (value < 0) {
                null_map[i] = true;
                continue;
            }

            auto& dt = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(res_data[i]);
            dt.from_unixtime(value / Impl::ratio, time_zone);

            if (dt.is_valid_date()) [[likely]] {
                dt.set_microsecond((value % Impl::ratio) * ratio_to_micro);
            } else {
                null_map[i] = true;
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_col), std::move(null_vector));
        return Status::OK();
    }
};

struct UtcTimestampImpl {
    using ReturnType = DataTypeDateTime;
    static constexpr auto name = "utc_timestamp";
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        WhichDataType which(remove_nullable(block.get_by_position(result).type));
        if (which.is_date_time_v2()) {
            return executeImpl<DateV2Value<DateTimeV2ValueType>, UInt64>(context, block, result,
                                                                         input_rows_count);
        } else if (which.is_date_v2()) {
            return executeImpl<DateV2Value<DateV2ValueType>, UInt32>(context, block, result,
                                                                     input_rows_count);
        } else {
            return executeImpl<VecDateTimeValue, Int64>(context, block, result, input_rows_count);
        }
    }

    template <typename DateValueType, typename NativeType>
    static Status executeImpl(FunctionContext* context, Block& block, uint32_t result,
                              size_t input_rows_count) {
        auto col_to = ColumnVector<Int64>::create();
        DateValueType dtv;
        if (dtv.from_unixtime(context->state()->timestamp_ms() / 1000, "+00:00")) {
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                reinterpret_cast<DateValueType*>(&dtv)->set_type(TIME_DATETIME);
            }

            auto date_packed_int =
                    binary_cast<DateValueType, NativeType>(*reinterpret_cast<DateValueType*>(&dtv));

            col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)),
                                0);

        } else {
            auto invalid_val = 0;

            col_to->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&invalid_val)), 0);
        }
        block.get_by_position(result).column =
                ColumnConst::create(std::move(col_to), input_rows_count);
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
        for (size_t i = 0; i < arguments.size(); ++i) {
            data_types[i] = arguments[i].type;
        }
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
