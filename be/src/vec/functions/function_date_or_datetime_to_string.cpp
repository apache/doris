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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionDateOrDatetimeToString.cpp
// and modified by Doris

#include <unicode/dtfmtsym.h>
#include <unicode/locid.h>
#include <unicode/unistr.h>

#include <cstddef>
#include <memory>
#include <utility>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h" // IWYU pragma: keep
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
class DataTypeString;

template <typename Transform>
class FunctionDateOrDateTimeToString : public IFunction {
public:
    using DateType = PrimitiveTypeTraits<Transform::OpArgType>::CppType;
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;
    static FunctionPtr create() { return std::make_shared<FunctionDateOrDateTimeToString>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(TYPE_STRING);
    }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Transform::get_variadic_argument_types();
        }
        return {};
    }

    // In ICU, Week_array: {"", "Sunday", "Monday", ..., "Saturday"}, size = 8
    // Month_array: {"January", "February", ..., "December"}, size = 12
    static constexpr size_t DAY_NUM_IN_ICU = 8;
    static constexpr size_t MONTH_NUM_IN_ICU = 12;
    // day_names: {"Monday", ..., "Sunday"}
    // month_names: {"", "January", ..., "December"}
    struct LocaleDayMonthNameState {
        std::string locale_name;
        std::vector<std::string> day_name_storage {7};
        std::vector<std::string> month_name_storage {13};
        const char* day_names[7];
        const char* month_names[13];
    };

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }

        auto state = std::make_shared<LocaleDayMonthNameState>();
        state->locale_name = context->state()->lc_time_names();
#ifdef BE_TEST
        state->locale_name = "en_US";
#endif
        UErrorCode status = U_ZERO_ERROR;
        icu::Locale locale(state->locale_name.c_str());
        icu::DateFormatSymbols symbols(locale, status);
        if (U_FAILURE(status)) [[unlikely]] {
            return Status::FatalError("Failed to create ICU DateFormatSymbols for locale {}",
                                      state->locale_name);
        }

        int32_t day_count, month_count;
        const icu::UnicodeString* days = symbols.getWeekdays(day_count);
        const icu::UnicodeString* months = symbols.getMonths(month_count);
        if (month_count != MONTH_NUM_IN_ICU || day_count != DAY_NUM_IN_ICU) [[unlikely]] {
            return Status::FatalError(
                    "Wrong number of month or day names for locale {}: got {} months and {} days",
                    state->locale_name, month_count, day_count - 1);
        }
        for (int i = 0; i < MONTH_NUM_IN_ICU; ++i) {
            months[i].toUTF8String(state->month_name_storage[i + 1]);
            state->month_names[i + 1] = state->month_name_storage[i + 1].c_str();
        }

        // In ICU, the first array is always like {"", "Sunday", "Monday", ..., "Saturday"}
        // so here skip the first empty string and adjust the order of day names into {"Monday", ..., "Sunday"}
        for (int i = 1; i < DAY_NUM_IN_ICU; ++i) {
            if (i == 1) {
                days[i].toUTF8String(state->day_name_storage[6]);
                state->day_names[6] = state->day_name_storage[6].c_str();
            } else {
                days[i].toUTF8String(state->day_name_storage[i - 2]);
                state->day_names[i - 2] = state->day_name_storage[i - 2].c_str();
            }
        }

        context->set_function_state(scope, state);
        return IFunction::open(context, scope);
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;
        const NullMap* null_map = nullptr;
        ColumnPtr actual_col = source_col;

        if (is_column_nullable(*source_col)) {
            const auto* nullable_col = check_and_get_column<ColumnNullable>(source_col.get());
            actual_col = nullable_col->get_nested_column_ptr();
            null_map = &nullable_col->get_null_map_data();
        }

        const auto* sources =
                check_and_get_column<ColumnVector<Transform::OpArgType>>(actual_col.get());
        if (!sources) [[unlikely]] {
            return Status::FatalError("Illegal column {} of first argument of function {}",
                                      block.get_by_position(arguments[0]).column->get_name(), name);
        }

        auto col_res = ColumnString::create();
        vector(context, sources->get_data(), col_res->get_chars(), col_res->get_offsets(),
               null_map);

        if (null_map) {
            const auto* nullable_col = check_and_get_column<ColumnNullable>(source_col.get());
            block.replace_by_position(
                    result,
                    ColumnNullable::create(std::move(col_res),
                                           nullable_col->get_null_map_column_ptr()->clone_resized(
                                                   input_rows_count)));
        } else {
            block.replace_by_position(result, std::move(col_res));
        }
        return Status::OK();
    }

private:
    static void vector(FunctionContext* context, const PaddedPODArray<DateType>& ts,
                       ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                       const NullMap* null_map = nullptr) {
        const auto len = ts.size();
        res_data.resize(len * Transform::max_size);
        res_offsets.resize(len);

        size_t offset = 0;
        auto* state = reinterpret_cast<LocaleDayMonthNameState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        const char* const* names_ptr = nullptr;
        if constexpr (std::is_same_v<Transform, DayNameImpl<Transform::OpArgType>>) {
            names_ptr = state->day_names;
        } else if constexpr (std::is_same_v<Transform, MonthNameImpl<Transform::OpArgType>>) {
            names_ptr = state->month_names;
        }

        for (size_t i = 0; i < len; ++i) {
            if (null_map && (*null_map)[i]) {
                res_offsets[i] = cast_set<UInt32>(offset);
                continue;
            }

            const auto& date_time_value = ts[i];
            res_offsets[i] = cast_set<UInt32>(
                    Transform::execute(date_time_value, res_data, offset, names_ptr, context));
            DCHECK(date_time_value.is_valid_date());
        }
        res_data.resize(res_offsets[res_offsets.size() - 1]);
    }
};

using FunctionDayNameV2 = FunctionDateOrDateTimeToString<DayNameImpl<TYPE_DATEV2>>;
using FunctionMonthNameV2 = FunctionDateOrDateTimeToString<MonthNameImpl<TYPE_DATEV2>>;

using FunctionDateTimeV2DayName = FunctionDateOrDateTimeToString<DayNameImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2MonthName = FunctionDateOrDateTimeToString<MonthNameImpl<TYPE_DATETIMEV2>>;
using FunctionYearMonth = FunctionDateOrDateTimeToString<YearMonthImpl>;
using FunctionDayHour = FunctionDateOrDateTimeToString<DayHourImpl>;
using FunctionDayMinute = FunctionDateOrDateTimeToString<DayMinuteImpl>;
using FunctionDaySecond = FunctionDateOrDateTimeToString<DaySecondImpl>;
using FunctionDayMicrosecond = FunctionDateOrDateTimeToString<DayMicrosecondImpl>;
using FunctionHourMinute = FunctionDateOrDateTimeToString<HourMinuteImpl>;
using FunctionHourSecond = FunctionDateOrDateTimeToString<HourSecondImpl>;
using FunctionHourMicrosecond = FunctionDateOrDateTimeToString<HourMicrosecondImpl>;
using FunctionMinuteSecond = FunctionDateOrDateTimeToString<MinuteSecondImpl>;
using FunctionMinuteMicrosecond = FunctionDateOrDateTimeToString<MinuteMicrosecondImpl>;
using FunctionSecondMicrosecond = FunctionDateOrDateTimeToString<SecondMicrosecondImpl>;

using FunctionDateIso8601 = FunctionDateOrDateTimeToString<ToIso8601Impl<TYPE_DATEV2>>;
using FunctionDateTimeIso8601 = FunctionDateOrDateTimeToString<ToIso8601Impl<TYPE_DATETIMEV2>>;
using FunctionTimestampTzIso8601 = FunctionDateOrDateTimeToString<ToIso8601Impl<TYPE_TIMESTAMPTZ>>;

void register_function_date_time_to_string(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDayNameV2>();
    factory.register_function<FunctionMonthNameV2>();
    factory.register_function<FunctionDateTimeV2DayName>();
    factory.register_function<FunctionDateTimeV2MonthName>();
    factory.register_function<FunctionYearMonth>();
    factory.register_function<FunctionDayHour>();
    factory.register_function<FunctionDayMinute>();
    factory.register_function<FunctionDaySecond>();
    factory.register_function<FunctionDayMicrosecond>();
    factory.register_function<FunctionHourMinute>();
    factory.register_function<FunctionHourSecond>();
    factory.register_function<FunctionHourMicrosecond>();
    factory.register_function<FunctionMinuteSecond>();
    factory.register_function<FunctionMinuteMicrosecond>();
    factory.register_function<FunctionSecondMicrosecond>();
    factory.register_function<FunctionDateIso8601>();
    factory.register_function<FunctionDateTimeIso8601>();
    factory.register_function<FunctionTimestampTzIso8601>();
}

} // namespace doris::vectorized
