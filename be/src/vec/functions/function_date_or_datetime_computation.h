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
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "util/binary_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_time.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_avoid_begin.h"
/// because all these functions(xxx_add/xxx_sub) defined in FE use Integer as the second value
///  so Int64 as delta is needed to support large values. For upstream(FunctionDateOrDateTimeComputation) we use Int64.

template <TimeUnit unit, PrimitiveType ArgType, typename IntervalType>
auto date_time_add(const typename PrimitiveTypeTraits<ArgType>::DataType::FieldType& t,
                   IntervalType delta, bool& is_null) {
    using ValueType = typename PrimitiveTypeTraits<ArgType>::CppType;
    using NativeType = typename PrimitiveTypeTraits<ArgType>::DataType::FieldType;

    // e.g.: for DatatypeDatetimeV2, cast from u64 to DateV2Value<DateTimeV2ValueType>
    auto ts_value = binary_cast<NativeType, ValueType>(t);
    TimeInterval interval(unit, std::abs(delta), delta < 0);
    is_null = !(ts_value.template date_add_interval<unit>(interval));
    // here DateValueType = ResultDateValueType
    return binary_cast<ValueType, NativeType>(ts_value);
}

#define ADD_TIME_FUNCTION_IMPL(CLASS, NAME, UNIT)                                                  \
    template <PrimitiveType PType>                                                                 \
    struct CLASS {                                                                                 \
        /* return type must be same with arg type. cast have already be planned in FE */           \
        static constexpr PrimitiveType ArgPType = PType;                                           \
        static constexpr PrimitiveType ReturnType = PType;                                         \
        /* for interval <= minute, use bigint. otherwise int. */                                   \
        static constexpr PrimitiveType IntervalPType =                                             \
                (TimeUnit::UNIT == TimeUnit::YEAR || TimeUnit::UNIT == TimeUnit::QUARTER ||        \
                 TimeUnit::UNIT == TimeUnit::MONTH || TimeUnit::UNIT == TimeUnit::WEEK ||          \
                 TimeUnit::UNIT == TimeUnit::DAY || TimeUnit::UNIT == TimeUnit::HOUR)              \
                        ? PrimitiveType::TYPE_INT                                                  \
                        : PrimitiveType::TYPE_BIGINT;                                              \
        using InputNativeType = typename PrimitiveTypeTraits<PType>::DataType::FieldType;          \
        using ReturnNativeType = InputNativeType;                                                  \
        using IntervalDataType = typename PrimitiveTypeTraits<IntervalPType>::DataType;            \
        using IntervalNativeType = IntervalDataType::FieldType;                                    \
                                                                                                   \
        static constexpr auto name = #NAME;                                                        \
        static constexpr auto is_nullable = true;                                                  \
        static inline ReturnNativeType execute(const InputNativeType& t, IntervalNativeType delta, \
                                               bool& is_null) {                                    \
            return date_time_add<TimeUnit::UNIT, PType, IntervalNativeType>(t, delta, is_null);    \
        }                                                                                          \
                                                                                                   \
        static DataTypes get_variadic_argument_types() {                                           \
            return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>(),             \
                    std::make_shared<typename PrimitiveTypeTraits<IntervalPType>::DataType>()};    \
        }                                                                                          \
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

template <PrimitiveType PType>
struct AddQuartersImpl {
    static constexpr PrimitiveType ArgPType = PType;
    static constexpr PrimitiveType ReturnType = PType;
    static constexpr PrimitiveType IntervalPType = PrimitiveType::TYPE_INT;
    using InputNativeType = typename PrimitiveTypeTraits<PType>::DataType::FieldType;
    using ReturnNativeType = InputNativeType;

    static constexpr auto name = "quarters_add";
    static constexpr auto is_nullable = true;
    static inline ReturnNativeType execute(const InputNativeType& t, Int32 delta, bool& is_null) {
        return date_time_add<TimeUnit::MONTH, PType, Int32>(t, 3 * delta, is_null);
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>(),
                std::make_shared<DataTypeInt32>()};
    }
};

template <typename Transform>
struct SubtractIntervalImpl {
    static constexpr PrimitiveType ArgPType = Transform::ArgPType;
    static constexpr PrimitiveType ReturnType = Transform::ReturnType;
    using InputNativeType = typename Transform::InputNativeType;
    using ReturnNativeType = typename Transform::ReturnNativeType;
    static constexpr auto is_nullable = true;
    static inline ReturnNativeType execute(const InputNativeType& t, Int64 delta, bool& is_null) {
        return Transform::execute(t, -delta, is_null);
    }

    static DataTypes get_variadic_argument_types() {
        return Transform::get_variadic_argument_types();
    }
};

template <PrimitiveType DateType>
struct SubtractMicrosecondsImpl : SubtractIntervalImpl<AddMicrosecondsImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddMicrosecondsImpl<DateType>::IntervalPType;
    static constexpr auto name = "microseconds_sub";
};

template <PrimitiveType DateType>
struct SubtractMillisecondsImpl : SubtractIntervalImpl<AddMillisecondsImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddMillisecondsImpl<DateType>::IntervalPType;
    static constexpr auto name = "milliseconds_sub";
};

template <PrimitiveType DateType>
struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddSecondsImpl<DateType>::IntervalPType;
    static constexpr auto name = "seconds_sub";
};

template <PrimitiveType DateType>
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddMinutesImpl<DateType>::IntervalPType;
    static constexpr auto name = "minutes_sub";
};

template <PrimitiveType DateType>
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddHoursImpl<DateType>::IntervalPType;
    static constexpr auto name = "hours_sub";
};

template <PrimitiveType DateType>
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddDaysImpl<DateType>::IntervalPType;
    static constexpr auto name = "days_sub";
};

template <PrimitiveType DateType>
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddWeeksImpl<DateType>::IntervalPType;
    static constexpr auto name = "weeks_sub";
};

template <PrimitiveType DateType>
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddMonthsImpl<DateType>::IntervalPType;
    static constexpr auto name = "months_sub";
};

template <PrimitiveType DateType>
struct SubtractQuartersImpl : SubtractIntervalImpl<AddQuartersImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddQuartersImpl<DateType>::IntervalPType;
    static constexpr auto name = "quarters_sub";
};

template <PrimitiveType DateType>
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl<DateType>> {
    static constexpr PrimitiveType IntervalPType = AddYearsImpl<DateType>::IntervalPType;
    static constexpr auto name = "years_sub";
};

#define DECLARE_DATE_FUNCTIONS(NAME, FN_NAME, RETURN_TYPE, STMT)                              \
    template <PrimitiveType DateType>                                                         \
    struct NAME {                                                                             \
        using NativeType = typename PrimitiveTypeTraits<DateType>::DataType::FieldType;       \
        using DateValueType = typename PrimitiveTypeTraits<DateType>::CppType;                \
        static constexpr PrimitiveType ArgPType = DateType;                                   \
        static constexpr PrimitiveType ReturnType = PrimitiveType::RETURN_TYPE;               \
                                                                                              \
        static constexpr auto name = #FN_NAME;                                                \
        static constexpr auto is_nullable = false;                                            \
        static inline typename PrimitiveTypeTraits<RETURN_TYPE>::DataType::FieldType execute( \
                const NativeType& t0, const NativeType& t1, bool& is_null) {                  \
            const auto& ts0 = reinterpret_cast<const DateValueType&>(t0);                     \
            const auto& ts1 = reinterpret_cast<const DateValueType&>(t1);                     \
            is_null = !ts0.is_valid_date() || !ts1.is_valid_date();                           \
            return (STMT);                                                                    \
        }                                                                                     \
        static DataTypes get_variadic_argument_types() {                                      \
            return {std::make_shared<typename PrimitiveTypeTraits<DateType>::DataType>(),     \
                    std::make_shared<typename PrimitiveTypeTraits<DateType>::DataType>()};    \
        }                                                                                     \
    };

DECLARE_DATE_FUNCTIONS(DateDiffImpl, datediff, TYPE_INT, (ts0.daynr() - ts1.daynr()));
// DECLARE_DATE_FUNCTIONS(TimeDiffImpl, timediff, DataTypeTime, ts0.datetime_diff_in_seconds(ts1));
// Expands to below here because it use Time type which need some special deal.
template <PrimitiveType DateType>
struct TimeDiffImpl {
    static constexpr PrimitiveType ArgPType = DateType;
    using NativeType = typename PrimitiveTypeTraits<DateType>::CppType;
    using ArgType = typename PrimitiveTypeTraits<DateType>::DataType::FieldType;
    static constexpr bool UsingTimev2 = is_date_v2_or_datetime_v2(DateType);

    static constexpr PrimitiveType ReturnType =
            TYPE_TIMEV2; // TimeV1Type also use double as native type. same as v2.

    static constexpr auto name = "timediff";
    static constexpr int64_t limit_value = 3020399000000; // 838:59:59 convert to microsecond
    static inline DataTypeTimeV2::FieldType execute(const ArgType& t0, const ArgType& t1,
                                                    bool& is_null) {
        const auto& ts0 = reinterpret_cast<const NativeType&>(t0);
        const auto& ts1 = reinterpret_cast<const NativeType&>(t1);
        is_null = !ts0.is_valid_date() || !ts1.is_valid_date();
        if constexpr (UsingTimev2) {
            // refer to https://dev.mysql.com/doc/refman/5.7/en/time.html
            // the time type value between '-838:59:59' and '838:59:59', so the return value should limited
            int64_t diff_m = ts0.datetime_diff_in_microseconds(ts1);
            if (diff_m > limit_value) {
                return (double)limit_value;
            } else if (diff_m < -1 * limit_value) {
                return (double)(-1 * limit_value);
            } else {
                return (double)diff_m;
            }
        } else {
            return TimeValue::from_seconds_with_limit(ts0.datetime_diff_in_seconds(ts1));
        }
    }
    static DataTypes get_variadic_argument_types() {
        return {std ::make_shared<typename PrimitiveTypeTraits<DateType>::DataType>(),
                std ::make_shared<typename PrimitiveTypeTraits<DateType>::DataType>()};
    }
};
#define TIME_DIFF_FUNCTION_IMPL(CLASS, NAME, UNIT) \
    DECLARE_DATE_FUNCTIONS(CLASS, NAME, TYPE_BIGINT, datetime_diff<TimeUnit::UNIT>(ts1, ts0))

// all these functions implemented by datediff
TIME_DIFF_FUNCTION_IMPL(YearsDiffImpl, years_diff, YEAR);
TIME_DIFF_FUNCTION_IMPL(QuartersDiffImpl, quarters_diff, QUARTER);
TIME_DIFF_FUNCTION_IMPL(MonthsDiffImpl, months_diff, MONTH);
TIME_DIFF_FUNCTION_IMPL(WeeksDiffImpl, weeks_diff, WEEK);
TIME_DIFF_FUNCTION_IMPL(DaysDiffImpl, days_diff, DAY);
TIME_DIFF_FUNCTION_IMPL(HoursDiffImpl, hours_diff, HOUR);
TIME_DIFF_FUNCTION_IMPL(MintuesDiffImpl, minutes_diff, MINUTE);
TIME_DIFF_FUNCTION_IMPL(SecondsDiffImpl, seconds_diff, SECOND);
TIME_DIFF_FUNCTION_IMPL(MilliSecondsDiffImpl, milliseconds_diff, MILLISECOND);
TIME_DIFF_FUNCTION_IMPL(MicroSecondsDiffImpl, microseconds_diff, MICROSECOND);

#define TIME_FUNCTION_TWO_ARGS_IMPL(CLASS, NAME, FUNCTION, RETURN_TYPE)                         \
    template <PrimitiveType DateType>                                                           \
    struct CLASS {                                                                              \
        static constexpr PrimitiveType ArgPType = DateType;                                     \
        static constexpr PrimitiveType IntervalPType = PrimitiveType::TYPE_INT;                 \
        using ArgType = typename PrimitiveTypeTraits<DateType>::DataType::FieldType;            \
        using IntervalNativeType =                                                              \
                typename PrimitiveTypeTraits<IntervalPType>::DataType::FieldType;               \
        using DateValueType = typename PrimitiveTypeTraits<DateType>::CppType;                  \
        static constexpr PrimitiveType ReturnType = RETURN_TYPE;                                \
                                                                                                \
        static constexpr auto name = #NAME;                                                     \
        static constexpr auto is_nullable = false;                                              \
        static inline typename PrimitiveTypeTraits<RETURN_TYPE>::DataType::FieldType execute(   \
                const ArgType& t0, const IntervalNativeType mode, bool& is_null) {              \
            const auto& ts0 = reinterpret_cast<const DateValueType&>(t0);                       \
            is_null = !ts0.is_valid_date();                                                     \
            return ts0.FUNCTION;                                                                \
        }                                                                                       \
        static DataTypes get_variadic_argument_types() {                                        \
            return {std::make_shared<typename PrimitiveTypeTraits<DateType>::DataType>(),       \
                    std::make_shared<typename PrimitiveTypeTraits<IntervalPType>::DataType>()}; \
        }                                                                                       \
    }

TIME_FUNCTION_TWO_ARGS_IMPL(ToYearWeekTwoArgsImpl, yearweek, year_week(mysql_week_mode(mode)),
                            TYPE_INT);
TIME_FUNCTION_TWO_ARGS_IMPL(ToWeekTwoArgsImpl, week, week(mysql_week_mode(mode)), TYPE_TINYINT);

// only use for FunctionDateOrDateTimeComputation. FromTypes are NativeTypes.
template <PrimitiveType DataType0, PrimitiveType DataType1, typename ToType, typename Transform>
struct DateTimeOp {
    using NativeType0 = typename PrimitiveTypeTraits<DataType0>::DataType::FieldType;
    using NativeType1 = typename PrimitiveTypeTraits<DataType1>::DataType::FieldType;
    using ValueType0 = typename PrimitiveTypeTraits<DataType0>::CppType;
    // arg1 maybe just delta value(e.g. DataTypeInt32, not datelike type)
    constexpr static bool CastType1 = is_date_type(DataType1);

    static void throw_out_of_bound(NativeType0 arg0, NativeType1 arg1) {
        auto value0 = binary_cast<NativeType0, ValueType0>(arg0);
        char buf0[40];
        char* end0 = value0.to_string(buf0);
        if constexpr (CastType1) {
            auto value1 =
                    binary_cast<NativeType1, typename PrimitiveTypeTraits<DataType1>::CppType>(
                            arg1);
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
        bool invalid = false;
        for (size_t i = 0; i < vec_from0.size(); ++i) {
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

        bool invalid = false;
        for (size_t i = 0; i < vec_from.size(); ++i) {
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

        bool invalid = false;
        for (size_t i = 0; i < delta.size(); ++i) {
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

template <typename Transform>
class FunctionTimeDiff : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;

    static FunctionPtr create() { return std::make_shared<FunctionTimeDiff>(); }

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
        RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(Transform::ReturnType);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        using ResFieldType =
                typename PrimitiveTypeTraits<Transform::ReturnType>::DataType::FieldType;
        using Op = DateTimeOp<Transform::ArgPType, Transform::ArgPType, ResFieldType, Transform>;

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
        auto res_col = ColumnVector<Transform::ReturnType>::create(input_rows_count, 0);

        // vector-const or vector-vector
        if (const auto* sources =
                    check_and_get_column<ColumnVector<Transform::ArgPType>>(src_nested_col.get())) {
            const ColumnPtr nest_col1 = remove_nullable(col1);
            bool rconst = false;
            // vector-const
            if (const auto* nest_col1_const = check_and_get_column<ColumnConst>(*nest_col1)) {
                rconst = true;
                const auto col1_inside_const =
                        assert_cast<const ColumnVector<Transform::ArgPType>&>(
                                nest_col1_const->get_data_column());
                Op::vector_constant(sources->get_data(), res_col->get_data(),
                                    col1_inside_const.get_data()[0], nullmap0, nullmap1);
            } else { // vector-vector
                const auto concrete_col1 =
                        assert_cast<const ColumnVector<Transform::ArgPType>&>(*nest_col1);
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
                           check_and_get_column_const<ColumnVector<Transform::ArgPType>>(
                                   src_nested_col.get())) {
            // const-vector
            const auto col0_inside_const = assert_cast<const ColumnVector<Transform::ArgPType>&>(
                    sources_const->get_data_column());
            const ColumnPtr nested_col1 = remove_nullable(col1);
            const auto concrete_col1 =
                    assert_cast<const ColumnVector<Transform::ArgPType>&>(*nested_col1);
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
        RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(Transform::ReturnType);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        using ResFieldType =
                typename PrimitiveTypeTraits<Transform::ReturnType>::DataType::FieldType;
        using Op =
                DateTimeOp<Transform::ArgPType, Transform::IntervalPType, ResFieldType, Transform>;
        using IntervalColumnType =
                typename PrimitiveTypeTraits<Transform::IntervalPType>::ColumnType;

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
        auto res_col = ColumnVector<Transform::ReturnType>::create(input_rows_count, 0);

        // vector-const or vector-vector
        if (const auto* sources =
                    check_and_get_column<ColumnVector<Transform::ArgPType>>(src_nested_col.get())) {
            const ColumnPtr nest_col1 = remove_nullable(col1);
            bool rconst = false;
            // vector-const
            if (const auto* nest_col1_const = check_and_get_column<ColumnConst>(*nest_col1)) {
                rconst = true;
                const auto col1_inside_const =
                        assert_cast<const IntervalColumnType&>(nest_col1_const->get_data_column());
                Op::vector_constant(sources->get_data(), res_col->get_data(),
                                    col1_inside_const.get_data()[0], nullmap0, nullmap1);
            } else { // vector-vector
                const auto concrete_col1 = assert_cast<const IntervalColumnType&>(*nest_col1);
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
                           check_and_get_column_const<ColumnVector<Transform::ArgPType>>(
                                   src_nested_col.get())) {
            // const-vector
            const auto col0_inside_const = assert_cast<const ColumnVector<Transform::ArgPType>&>(
                    sources_const->get_data_column());
            const ColumnPtr nested_col1 = remove_nullable(col1);
            const auto concrete_col1 = assert_cast<const IntervalColumnType&>(*nested_col1);
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
        return std::make_shared<typename PrimitiveTypeTraits<FunctionImpl::ReturnType>::DataType>();
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
    static constexpr PrimitiveType ReturnType = WithPrecision ? TYPE_DATETIMEV2 : TYPE_DATETIME;

    static DataTypes get_variadic_argument_types() {
        if constexpr (WithPrecision) {
            return {std::make_shared<DataTypeInt32>()};
        } else {
            return {};
        }
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        if constexpr (WithPrecision) {
            DCHECK(block.get_by_position(result).type->get_primitive_type() == TYPE_DATETIMEV2 ||
                   block.get_by_position(result).type->get_primitive_type() == TYPE_DATEV2);
            if (block.get_by_position(result).type->get_primitive_type() == TYPE_DATETIMEV2) {
                return executeImpl<TYPE_DATETIMEV2>(context, block, arguments, result,
                                                    input_rows_count);
            } else {
                return executeImpl<TYPE_DATEV2>(context, block, arguments, result,
                                                input_rows_count);
            }
        } else {
            if (block.get_by_position(result).type->get_primitive_type() == TYPE_DATETIMEV2) {
                return executeImpl<TYPE_DATETIMEV2>(context, block, arguments, result,
                                                    input_rows_count);
            } else if (block.get_by_position(result).type->get_primitive_type() == TYPE_DATEV2) {
                return executeImpl<TYPE_DATEV2>(context, block, arguments, result,
                                                input_rows_count);
            } else {
                return executeImpl<TYPE_DATETIME>(context, block, arguments, result,
                                                  input_rows_count);
            }
        }
    }

    template <PrimitiveType PType>
    static Status executeImpl(FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, uint32_t result,
                              size_t input_rows_count) {
        using DateValueType = typename PrimitiveTypeTraits<PType>::CppType;
        using NativeType = typename PrimitiveTypeTraits<PType>::CppNativeType;
        auto col_to = ColumnVector<PType>::create();
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

template <typename FunctionName, PrimitiveType PType>
struct CurrentDateImpl {
    static constexpr PrimitiveType ReturnType = PType;
    static constexpr auto name = FunctionName::name;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        auto col_to = ColumnVector<PType>::create();
        if constexpr (PType == TYPE_DATEV2) {
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
            dtv.set_type(TIME_DATE);
            auto date_packed_int = binary_cast<doris::VecDateTimeValue, int64_t>(dtv);
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
    static constexpr PrimitiveType ReturnType = TYPE_TIMEV2;
    static constexpr auto name = FunctionName::name;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        auto col_to = ColumnTimeV2::create();
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
    static constexpr PrimitiveType ReturnType = TYPE_INT;
    static constexpr auto name = "time_to_sec";
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        auto res_col = ColumnInt32::create(input_rows_count);
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnTimeV2&>(*arg_col);

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
    static constexpr PrimitiveType ReturnType = TYPE_TIMEV2;
    static constexpr auto name = "sec_to_time";
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnInt32&>(*arg_col);

        auto res_col = ColumnTimeV2::create(input_rows_count);
        auto& res_data = res_col->get_data();
        for (int i = 0; i < input_rows_count; ++i) {
            res_data[i] = TimeValue::from_seconds_with_limit(column_data.get_element(i));
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
    static constexpr PrimitiveType ReturnType = TYPE_DATETIMEV2;
    static constexpr auto name = Impl::name;
    static constexpr Int64 ratio_to_micro = (1000 * 1000) / Impl::ratio;
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeDateTimeV2>());
    }

    static FunctionPtr create() { return std::make_shared<TimestampToDateTime<Impl>>(); }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnInt64&>(*arg_col);
        auto res_col = ColumnDateTimeV2::create();
        auto null_vector = ColumnUInt8::create();
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
    static constexpr PrimitiveType ReturnType = TYPE_DATETIME;
    static constexpr auto name = "utc_timestamp";
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        auto col_to = ColumnDateTimeV2::create();
        DateV2Value<DateTimeV2ValueType> dtv;
        if (dtv.from_unixtime(context->state()->timestamp_ms() / 1000, "+00:00")) {
            auto date_packed_int = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(dtv);
            col_to->insert_data(reinterpret_cast<char*>(&date_packed_int), 0);
        } else {
            uint64_t invalid_val = 0;
            col_to->insert_data(reinterpret_cast<char*>(&invalid_val), 0);
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
        if (return_type->get_primitive_type() == TYPE_DATEV2) {
            auto function = FunctionCurrentDateOrDateTime<
                    CurrentDateImpl<FunctionName, TYPE_DATEV2>>::create();
            return std::make_shared<DefaultFunction>(function, data_types, return_type);
        } else if (return_type->get_primitive_type() == TYPE_DATETIMEV2) {
            auto function = FunctionCurrentDateOrDateTime<
                    CurrentDateImpl<FunctionName, TYPE_DATETIMEV2>>::create();
            return std::make_shared<DefaultFunction>(function, data_types, return_type);
        } else {
            auto function = FunctionCurrentDateOrDateTime<
                    CurrentDateImpl<FunctionName, TYPE_DATE>>::create();
            return std::make_shared<DefaultFunction>(function, data_types, return_type);
        }
    }
};

class FunctionMonthsBetween : public IFunction {
public:
    static constexpr auto name = "months_between";
    static FunctionPtr create() { return std::make_shared<FunctionMonthsBetween>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFloat64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        CHECK_EQ(arguments.size(), 3);
        auto res = ColumnFloat64::create();

        bool date_consts[2];
        date_consts[0] = is_column_const(*block.get_by_position(arguments[0]).column);
        date_consts[1] = is_column_const(*block.get_by_position(arguments[1]).column);
        ColumnPtr date_cols[2];
        // convert const columns to full columns if necessary
        default_preprocess_parameter_columns(date_cols, date_consts, {0, 1}, block, arguments);

        const auto& [col3, col3_const] =
                unpack_if_const(block.get_by_position(arguments[2]).column);

        const auto& date1_col = *assert_cast<const ColumnDateV2*>(date_cols[0].get());
        const auto& date2_col = *assert_cast<const ColumnDateV2*>(date_cols[1].get());
        const auto& round_off_col = *assert_cast<const ColumnBool*>(col3.get());

        if (date_consts[0] && date_consts[1]) {
            execute_vector<true, false>(input_rows_count, date1_col, date2_col, round_off_col,
                                        *res);
        } else if (col3_const) {
            execute_vector<false, true>(input_rows_count, date1_col, date2_col, round_off_col,
                                        *res);
        } else {
            execute_vector<false, false>(input_rows_count, date1_col, date2_col, round_off_col,
                                         *res);
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

private:
    template <bool is_date_const, bool is_round_off_const>
    static void execute_vector(const size_t input_rows_count, const ColumnDateV2& date1_col,
                               const ColumnDateV2& date2_col, const ColumnBool& round_off_col,
                               ColumnFloat64& res) {
        res.reserve(input_rows_count);
        double months_between;
        bool round_off;

        if constexpr (is_date_const) {
            auto dtv1 = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(date1_col.get_element(0));
            auto dtv2 = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(date2_col.get_element(0));
            months_between = calc_months_between(dtv1, dtv2);
        }

        if constexpr (is_round_off_const) {
            round_off = round_off_col.get_element(0);
        }

        for (int i = 0; i < input_rows_count; ++i) {
            if constexpr (!is_date_const) {
                auto dtv1 =
                        binary_cast<UInt32, DateV2Value<DateV2ValueType>>(date1_col.get_element(i));
                auto dtv2 =
                        binary_cast<UInt32, DateV2Value<DateV2ValueType>>(date2_col.get_element(i));
                months_between = calc_months_between(dtv1, dtv2);
            }
            if constexpr (!is_round_off_const) {
                round_off = round_off_col.get_element(i);
            }
            if (round_off) {
                months_between = round_months_between(months_between);
            }
            res.insert_value(months_between);
        }
    }

    static double calc_months_between(const DateV2Value<DateV2ValueType>& dtv1,
                                      const DateV2Value<DateV2ValueType>& dtv2) {
        auto year_between = dtv1.year() - dtv2.year();
        auto months_between = dtv1.month() - dtv2.month();
        auto days_in_month1 = S_DAYS_IN_MONTH[dtv1.month()];
        if (UNLIKELY(is_leap(dtv1.year()) && dtv1.month() == 2)) {
            days_in_month1 = 29;
        }
        auto days_in_month2 = S_DAYS_IN_MONTH[dtv2.month()];
        if (UNLIKELY(is_leap(dtv2.year()) && dtv2.month() == 2)) {
            days_in_month2 = 29;
        }
        double days_between = 0;
        // if date1 and date2 are all the last day of the month, days_between is 0
        if (UNLIKELY(dtv1.day() == days_in_month1 && dtv2.day() == days_in_month2)) {
            days_between = 0;
        } else {
            days_between = (dtv1.day() - dtv2.day()) / (double)31.0;
        }

        // calculate months between
        return year_between * 12 + months_between + days_between;
    }

    static double round_months_between(double months_between) {
        return round(months_between * 100000000) / 100000000;
    }
};

class FunctionNextDay : public IFunction {
public:
    static constexpr auto name = "next_day";
    static FunctionPtr create() { return std::make_shared<FunctionNextDay>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeDateV2>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        CHECK_EQ(arguments.size(), 2);
        auto res = ColumnDateV2::create();
        res->reserve(input_rows_count);
        const auto& [left_col, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_col, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto& date_col = *assert_cast<const ColumnDateV2*>(left_col.get());
        const auto& week_col = *assert_cast<const ColumnString*>(right_col.get());
        Status status;
        if (left_const && right_const) {
            status = execute_vector<true, true>(input_rows_count, date_col, week_col, *res);
        } else if (left_const) {
            status = execute_vector<true, false>(input_rows_count, date_col, week_col, *res);
        } else if (right_const) {
            status = execute_vector<false, true>(input_rows_count, date_col, week_col, *res);
        } else {
            status = execute_vector<false, false>(input_rows_count, date_col, week_col, *res);
        }
        if (!status.ok()) {
            return status;
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

private:
    static int day_of_week(const StringRef& weekday) {
        static const std::unordered_map<std::string, int> weekday_map = {
                {"MO", 1}, {"MON", 1}, {"MONDAY", 1},    {"TU", 2}, {"TUE", 2}, {"TUESDAY", 2},
                {"WE", 3}, {"WED", 3}, {"WEDNESDAY", 3}, {"TH", 4}, {"THU", 4}, {"THURSDAY", 4},
                {"FR", 5}, {"FRI", 5}, {"FRIDAY", 5},    {"SA", 6}, {"SAT", 6}, {"SATURDAY", 6},
                {"SU", 7}, {"SUN", 7}, {"SUNDAY", 7}};
        auto weekday_upper = weekday.to_string();
        std::transform(weekday_upper.begin(), weekday_upper.end(), weekday_upper.begin(),
                       ::toupper);
        auto it = weekday_map.find(weekday_upper);
        if (it == weekday_map.end()) {
            return 0;
        }
        return it->second;
    }
    static Status compute_next_day(DateV2Value<DateV2ValueType>& dtv, const int week_day) {
        auto days_to_add = (week_day - (dtv.weekday() + 1) + 7) % 7;
        days_to_add = days_to_add == 0 ? 7 : days_to_add;
        dtv.date_add_interval<TimeUnit::DAY>(TimeInterval(TimeUnit::DAY, days_to_add, false));
        return Status::OK();
    }

    template <bool left_const, bool right_const>
    static Status execute_vector(size_t input_rows_count, const ColumnDateV2& left_col,
                                 const ColumnString& right_col, ColumnDateV2& res_col) {
        DateV2Value<DateV2ValueType> dtv;
        int week_day;
        if constexpr (left_const) {
            dtv = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(left_col.get_element(0));
        }
        if constexpr (right_const) {
            auto week = right_col.get_data_at(0);
            week_day = day_of_week(week);
            if (week_day == 0) {
                return Status::InvalidArgument("Function {} failed to parse weekday: {}", name,
                                               week);
            }
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (!left_const) {
                dtv = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(left_col.get_element(i));
            }
            if constexpr (!right_const) {
                auto week = right_col.get_data_at(i);
                week_day = day_of_week(week);
                if (week_day == 0) {
                    return Status::InvalidArgument("Function {} failed to parse weekday: {}", name,
                                                   week);
                }
            }
            RETURN_IF_ERROR(compute_next_day(dtv, week_day));
            res_col.insert_value(binary_cast<DateV2Value<DateV2ValueType>, UInt32>(dtv));
        }
        return Status::OK();
    }
};

class FunctionTime : public IFunction {
public:
    static constexpr auto name = "time";
    static FunctionPtr create() { return std::make_shared<FunctionTime>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeTimeV2>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 1);
        ColumnPtr col = block.get_by_position(arguments[0]).column;
        const auto& arg = assert_cast<const ColumnDateTimeV2&>(*col.get());
        ColumnTimeV2::MutablePtr res = ColumnTimeV2::create(input_rows_count);
        auto& res_data = res->get_data();
        for (int i = 0; i < arg.size(); i++) {
            const auto& v =
                    binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(arg.get_element(i));
            // the arg is datetimev2 type, it's store as uint64, so we need to get arg's hour minute second part
            res_data[i] = TimeValue::make_time(v.hour(), v.minute(), v.second(), v.microsecond());
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};
#include "common/compile_check_avoid_end.h"
} // namespace doris::vectorized
