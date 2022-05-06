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

#include "vec/columns/column_vector.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionDateTimeFloorCeil : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeFloorCeil>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeDateTime>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const ColumnPtr source_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (const auto* sources = check_and_get_column<ColumnVector<Int64>>(source_col.get())) {
            auto col_to = ColumnVector<Int64>::create();
            col_to->resize(input_rows_count);
            auto null_map = ColumnVector<UInt8>::create();
            null_map->get_data().resize_fill(input_rows_count, false);

            if (arguments.size() == 1) {
                Impl::vector(sources->get_data(), col_to->get_data(), null_map->get_data());
            } else if (arguments.size() == 2) {
                const IColumn& delta_column = *block.get_by_position(arguments[1]).column;
                if (const auto* delta_const_column =
                            typeid_cast<const ColumnConst*>(&delta_column)) {
                    if (block.get_by_position(arguments[1]).type->get_type_id() !=
                        TypeIndex::Int32) {
                        Impl::vector_constant(sources->get_data(),
                                              delta_const_column->get_field().get<Int64>(),
                                              col_to->get_data(), null_map->get_data());
                    } else {
                        Impl::vector_constant(sources->get_data(),
                                              delta_const_column->get_field().get<Int32>(),
                                              col_to->get_data(), null_map->get_data());
                    }
                } else {
                    if (const auto* delta_vec_column0 =
                                check_and_get_column<ColumnVector<Int64>>(delta_column)) {
                        Impl::vector_vector(sources->get_data(), delta_vec_column0->get_data(),
                                            col_to->get_data(), null_map->get_data());
                    } else {
                        const auto* delta_vec_column1 =
                                check_and_get_column<ColumnVector<Int32>>(delta_column);
                        DCHECK(delta_vec_column1 != nullptr);
                        Impl::vector_vector(sources->get_data(), delta_vec_column1->get_data(),
                                            col_to->get_data(), null_map->get_data());
                    }
                }
            } else {
                auto arg1_column_ptr = block.get_by_position(arguments[1])
                                               .column->convert_to_full_column_if_const();
                auto arg2_column_ptr = block.get_by_position(arguments[2])
                                               .column->convert_to_full_column_if_const();

                const auto arg1_column =
                        check_and_get_column<ColumnVector<Int32>>(*arg1_column_ptr);
                const auto arg2_column =
                        check_and_get_column<ColumnVector<Int64>>(*arg2_column_ptr);
                DCHECK(arg1_column != nullptr);
                DCHECK(arg2_column != nullptr);
                Impl::vector_vector(sources->get_data(), arg1_column->get_data(),
                                    arg2_column->get_data(), col_to->get_data(),
                                    null_map->get_data());
            }

            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(null_map));
        } else {
            return Status::RuntimeError(fmt::format(
                    "Illegal column {} of first argument of function {}",
                    block.get_by_position(arguments[0]).column->get_name(), Impl::name));
        }
        return Status::OK();
    }
};

template <typename Impl>
struct FloorCeilImpl {
    static constexpr auto name = Impl::name;

    static void vector(const PaddedPODArray<Int64>& dates, PaddedPODArray<Int64>& res,
                       NullMap& null_map) {
        vector_constant(dates, Int32(1), res, null_map);
    }

    static void vector_constant(const PaddedPODArray<Int64>& dates, Int64 origin_date,
                                PaddedPODArray<Int64>& res, NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            Impl::time_round(dates[i], Int32(1), origin_date, res[i], null_map[i]);
        }
    }

    static void vector_constant(const PaddedPODArray<Int64>& dates, Int32 period,
                                PaddedPODArray<Int64>& res, NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            Impl::time_round(dates[i], period, res[i], null_map[i]);
        }
    }

    static void vector_vector(const PaddedPODArray<Int64>& dates,
                              const PaddedPODArray<Int64>& origin_dates, PaddedPODArray<Int64>& res,
                              NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            Impl::time_round(dates[i], Int32(1), origin_dates[i], res[i], null_map[i]);
        }
    }

    static void vector_vector(const PaddedPODArray<Int64>& dates,
                              const PaddedPODArray<Int32>& periods, PaddedPODArray<Int64>& res,
                              NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            Impl::time_round(dates[i], periods[i], res[i], null_map[i]);
        }
    }

    static void vector_vector(const PaddedPODArray<Int64>& dates,
                              const PaddedPODArray<Int32>& periods,
                              const PaddedPODArray<Int64>& origin_dates, PaddedPODArray<Int64>& res,
                              NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            Impl::time_round(dates[i], periods[i], origin_dates[i], res[i], null_map[i]);
        }
    }
};

template <typename Impl>
struct TimeRound {
    static constexpr auto name = Impl::name;
    static constexpr uint64_t FIRST_DAY = 19700101000000;
    static constexpr uint64_t FIRST_SUNDAY = 19700104000000;

    static void time_round(const doris::vectorized::VecDateTimeValue& ts2, Int32 period,
                           doris::vectorized::VecDateTimeValue& ts1, UInt8& is_null) {
        if (period < 1) {
            is_null = true;
            return;
        }

        int64_t diff;
        if constexpr (Impl::Unit == YEAR) {
            int year = (ts2.year() - ts1.year());
            diff = year - (ts2.to_int64() % 10000000000 < ts1.to_int64() % 10000000000);
        }
        if constexpr (Impl::Unit == MONTH) {
            int month = (ts2.year() - ts1.year()) * 12 + (ts2.month() - ts1.month());
            diff = month - (ts2.to_int64() % 100000000 < ts1.to_int64() % 100000000);
        }
        if constexpr (Impl::Unit == MONTH) {
            int month = (ts2.year() - ts1.year()) * 12 + (ts2.month() - ts1.month());
            diff = month - (ts2.to_int64() % 100000000 < ts1.to_int64() % 100000000);
        }
        if constexpr (Impl::Unit == WEEK) {
            int week = ts2.daynr() / 7 - ts1.daynr() / 7;
            diff = week - (ts2.daynr() % 7 < ts1.daynr() % 7 + (ts2.time_part_diff(ts1) < 0));
        }
        if constexpr (Impl::Unit == DAY) {
            int day = ts2.daynr() - ts1.daynr();
            diff = day - (ts2.time_part_diff(ts1) < 0);
        }
        if constexpr (Impl::Unit == HOUR) {
            int hour = (ts2.daynr() - ts1.daynr()) * 24 + (ts2.hour() - ts1.hour());
            diff = hour - ((ts2.minute() * 60 + ts2.second()) < (ts1.minute() * 60 - ts1.second()));
        }
        if constexpr (Impl::Unit == MINUTE) {
            int minute = (ts2.daynr() - ts1.daynr()) * 24 * 60 + (ts2.hour() - ts1.hour()) * 60 +
                         (ts2.minute() - ts1.minute());
            diff = minute - (ts2.second() < ts1.second());
        }
        if constexpr (Impl::Unit == SECOND) {
            diff = ts2.second_diff(ts1);
        }

        int64_t count = period;
        int64_t step = diff - (diff % count + count) % count + (Impl::Type == 0 ? 0 : count);
        bool is_neg = step < 0;

        TimeInterval interval(Impl::Unit, is_neg ? -step : step, is_neg);
        is_null = !ts1.date_add_interval(interval, Impl::Unit);
        return;
    }

    static void time_round(Int64 date, Int32 period, Int64 origin_date, Int64& res,
                           UInt8& is_null) {
        res = origin_date;
        auto ts2 = binary_cast<Int64, VecDateTimeValue>(date);
        auto& ts1 = (doris::vectorized::VecDateTimeValue&)(res);

        time_round(ts2, period, ts1, is_null);
    }

    static void time_round(Int64 date, Int32 period, Int64& res, UInt8& is_null) {
        auto ts2 = binary_cast<Int64, VecDateTimeValue>(date);
        auto& ts1 = (doris::vectorized::VecDateTimeValue&)(res);
        if constexpr (Impl::Unit != WEEK) {
            ts1.from_olap_datetime(FIRST_DAY);
        } else {
            // Only week use the FIRST SUNDAY
            ts1.from_olap_datetime(FIRST_SUNDAY);
        }

        time_round(ts2, period, ts1, is_null);
    }
};

#define TIME_ROUND(CLASS, NAME, UNIT, TYPE)    \
    struct CLASS {                             \
        static constexpr auto name = #NAME;    \
        static constexpr TimeUnit Unit = UNIT; \
        static constexpr auto Type = TYPE;     \
    };                                         \
    using Function##CLASS = FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>>;

TIME_ROUND(YearFloor, year_floor, YEAR, false);
TIME_ROUND(MonthFloor, month_floor, MONTH, false);
TIME_ROUND(WeekFloor, week_floor, WEEK, false);
TIME_ROUND(DayFloor, day_floor, DAY, false);
TIME_ROUND(HourFloor, hour_floor, HOUR, false);
TIME_ROUND(MinuteFloor, minute_floor, MINUTE, false);
TIME_ROUND(SecondFloor, second_floor, SECOND, false);

TIME_ROUND(YearCeil, year_ceil, YEAR, true);
TIME_ROUND(MonthCeil, month_ceil, MONTH, true);
TIME_ROUND(WeekCeil, week_ceil, WEEK, true);
TIME_ROUND(DayCeil, day_ceil, DAY, true);
TIME_ROUND(HourCeil, hour_ceil, HOUR, true);
TIME_ROUND(MinuteCeil, minute_ceil, MINUTE, true);
TIME_ROUND(SecondCeil, second_ceil, SECOND, true);

void register_function_datetime_floor_ceil(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionYearFloor>();
    factory.register_function<FunctionMonthFloor>();
    factory.register_function<FunctionWeekFloor>();
    factory.register_function<FunctionDayFloor>();
    factory.register_function<FunctionHourFloor>();
    factory.register_function<FunctionMinuteFloor>();
    factory.register_function<FunctionSecondFloor>();

    factory.register_function<FunctionYearCeil>();
    factory.register_function<FunctionMonthCeil>();
    factory.register_function<FunctionWeekCeil>();
    factory.register_function<FunctionDayCeil>();
    factory.register_function<FunctionHourCeil>();
    factory.register_function<FunctionMinuteCeil>();
    factory.register_function<FunctionSecondCeil>();
}
} // namespace doris::vectorized
