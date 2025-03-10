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

#include <experimental/bits/simd.h>
#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/preprocessor/repetition/repeat.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/compiler_util.h"
#include "common/status.h"
#include "util/binary_cast.hpp"
#include "util/datetype_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
#include "common/compile_check_begin.h"
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
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

template <typename Flag, typename DateType, int ArgNum, bool UseDelta = false>
class FunctionDateTimeFloorCeil : public IFunction {
public:
    using DateValueType = date_cast::TypeToValueTypeV<DateType>;
    using NativeType = DateType::FieldType;
    using DeltaDataType = DataTypeNumber<Int32>;
    // return date type = DateType
    static constexpr auto name = Flag::name;

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeFloorCeil>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        // for V1 our argument is datetime. exactly equal to the return type we want.
        return make_nullable(std::make_shared<DateType>());
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
        const ColumnPtr source_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (const auto* sources =
                    check_and_get_column<ColumnVector<NativeType>>(source_col.get())) {
            auto col_to = ColumnVector<NativeType>::create();
            col_to->resize(input_rows_count);
            auto null_map = ColumnVector<UInt8>::create();
            null_map->get_data().resize_fill(input_rows_count, false);

            if constexpr (ArgNum == 1) {
                vector(sources->get_data(), col_to->get_data(), null_map->get_data());
            } else if constexpr (ArgNum == 2) {
                const IColumn& delta_column = *block.get_by_position(arguments[1]).column;
                if (const auto* const_second_column =
                            check_and_get_column<ColumnConst>(delta_column)) {
                    if (block.get_by_position(arguments[1]).type->get_type_id() !=
                        TypeIndex::Int32) {
                        // time_round(datetime, const(origin))
                        vector_const_anchor(sources->get_data(),
                                            const_second_column->get_field().get<NativeType>(),
                                            col_to->get_data(), null_map->get_data());
                    } else {
                        // time_round(datetime,const(period))
                        vector_const_period(sources->get_data(),
                                            const_second_column->get_field().get<Int32>(),
                                            col_to->get_data(), null_map->get_data());
                    }
                } else {
                    if (const auto* delta_vec_column0 =
                                check_and_get_column<ColumnVector<NativeType>>(delta_column)) {
                        // time_round(datetime, origin)
                        vector_vector_anchor(sources->get_data(), delta_vec_column0->get_data(),
                                             col_to->get_data(), null_map->get_data());
                    } else {
                        const auto* delta_vec_column1 =
                                check_and_get_column<ColumnVector<Int32>>(delta_column);
                        DCHECK(delta_vec_column1 != nullptr);
                        // time_round(datetime, period)
                        vector_vector_period(sources->get_data(), delta_vec_column1->get_data(),
                                             col_to->get_data(), null_map->get_data());
                    }
                }
            } else { // 3 arg, time_round(datetime, period, origin)
                ColumnPtr arg1_col, arg2_col;
                bool arg1_const, arg2_const;
                std::tie(arg1_col, arg1_const) =
                        unpack_if_const(block.get_by_position(arguments[1]).column);
                std::tie(arg2_col, arg2_const) =
                        unpack_if_const(block.get_by_position(arguments[2]).column);
                if (arg1_const && arg2_const) {
                    Field arg1, arg2;
                    arg1_col->get(0, arg1);
                    arg2_col->get(0, arg2);
                    // time_round(datetime, const(period), const(origin))
                    vector_const_const(sources->get_data(), arg1.get<Int32>(),
                                       arg2.get<NativeType>(), col_to->get_data(),
                                       null_map->get_data());

                } else if (arg1_const && !arg2_const) {
                    Field arg1;
                    arg1_col->get(0, arg1);
                    const auto arg2_column =
                            check_and_get_column<ColumnVector<NativeType>>(*arg2_col);
                    // time_round(datetime, const(period), origin)
                    vector_const_vector(sources->get_data(), arg1.get<Int32>(),
                                        arg2_column->get_data(), col_to->get_data(),
                                        null_map->get_data());
                } else if (!arg1_const && arg2_const) {
                    Field arg2;
                    arg2_col->get(0, arg2);
                    const auto* arg1_column = check_and_get_column<ColumnVector<Int32>>(*arg1_col);
                    // time_round(datetime, period, const(origin))
                    vector_vector_const(sources->get_data(), arg1_column->get_data(),
                                        arg2.get<NativeType>(), col_to->get_data(),
                                        null_map->get_data());
                } else {
                    const auto* arg1_column = check_and_get_column<ColumnVector<Int32>>(*arg1_col);
                    const auto arg2_column =
                            check_and_get_column<ColumnVector<NativeType>>(*arg2_col);
                    DCHECK(arg1_column != nullptr);
                    DCHECK(arg2_column != nullptr);
                    // time_round(datetime, period, origin)
                    vector_vector_vector(sources->get_data(), arg1_column->get_data(),
                                         arg2_column->get_data(), col_to->get_data(),
                                         null_map->get_data());
                }
            }

            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(null_map));
        } else {
            return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        Flag::name);
        }
        return Status::OK();
    }

private:
    static void vector(const PaddedPODArray<NativeType>& dates, PaddedPODArray<NativeType>& res,
                       NullMap& null_map) {
        // time_round(datetime)
        for (int i = 0; i < dates.size(); ++i) {
            SET_NULLMAP_IF_FALSE((time_round_reinterpret_one_arg(dates[i], res[i])));
        }
    }

    static void vector_const_anchor(const PaddedPODArray<NativeType>& dates, NativeType origin_date,
                                    PaddedPODArray<NativeType>& res, NullMap& null_map) {
        // time_round(datetime, const(origin))
        for (int i = 0; i < dates.size(); ++i) {
            SET_NULLMAP_IF_FALSE(
                    (time_round_reinterpret_three_args(dates[i], 1, origin_date, res[i])));
        }
    }

    static void vector_const_period(const PaddedPODArray<NativeType>& dates, Int32 period,
                                    PaddedPODArray<NativeType>& res, NullMap& null_map) {
        // time_round(datetime,const(period))
        if (period < 1) [[unlikely]] {
            memset(null_map.data(), 1, sizeof(UInt8) * dates.size());
            return;
        }
        for (int i = 0; i < dates.size(); ++i) {
            SET_NULLMAP_IF_FALSE((time_round_reinterpret_two_args(dates[i], period, res[i])));
        }
    }

    static void vector_const_const(const PaddedPODArray<NativeType>& dates, const Int32 period,
                                   NativeType origin_date, PaddedPODArray<NativeType>& res,
                                   NullMap& null_map) {
        if (period < 1) {
            memset(null_map.data(), 1, sizeof(UInt8) * dates.size());
            return;
        }
        // expand codes for const input periods
#define EXPAND_CODE_FOR_CONST_INPUT(X)                                   \
    case X: {                                                            \
        for (int i = 0; i < dates.size(); ++i) {                         \
            /* expand time_round_reinterpret_three_args*/                \
            res[i] = origin_date;                                        \
            auto ts2 = binary_cast<NativeType, DateValueType>(dates[i]); \
            auto& ts1 = (DateValueType&)(res[i]);                        \
            SET_NULLMAP_IF_FALSE(time_round_two_args(ts2, X, ts1))       \
        }                                                                \
        return;                                                          \
    }
#define EXPANDER(z, n, text) EXPAND_CODE_FOR_CONST_INPUT(n)
        switch (period) {
            // expand for some constant period
            BOOST_PP_REPEAT(12, EXPANDER, ~)
        default:
            for (int i = 0; i < dates.size(); ++i) {
                // always inline here
                SET_NULLMAP_IF_FALSE(
                        (time_round_reinterpret_three_args(dates[i], period, origin_date, res[i])))
            }
        }
#undef EXPAND_CODE_FOR_CONST_INPUT
#undef EXPANDER
    }

    static void vector_const_vector(const PaddedPODArray<NativeType>& dates, const Int32 period,
                                    const PaddedPODArray<NativeType>& origin_dates,
                                    PaddedPODArray<NativeType>& res, NullMap& null_map) {
        if (period < 1) {
            memset(null_map.data(), 1, sizeof(UInt8) * dates.size());
            return;
        }
        for (int i = 0; i < dates.size(); ++i) {
            SET_NULLMAP_IF_FALSE(
                    (time_round_reinterpret_three_args(dates[i], period, origin_dates[i], res[i])));
        }
    }

    static void vector_vector_const(const PaddedPODArray<NativeType>& dates,
                                    const PaddedPODArray<Int32>& periods, NativeType origin_date,
                                    PaddedPODArray<NativeType>& res, NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            if (periods[i] < 1) [[unlikely]] {
                null_map[i] = true;
                continue;
            }
            SET_NULLMAP_IF_FALSE(
                    (time_round_reinterpret_three_args(dates[i], periods[i], origin_date, res[i])));
        }
    }

    static void vector_vector_anchor(const PaddedPODArray<NativeType>& dates,
                                     const PaddedPODArray<NativeType>& origin_dates,
                                     PaddedPODArray<NativeType>& res, NullMap& null_map) {
        // time_round(datetime, origin)
        for (int i = 0; i < dates.size(); ++i) {
            SET_NULLMAP_IF_FALSE(
                    (time_round_reinterpret_three_args(dates[i], 1, origin_dates[i], res[i])));
        }
    }

    static void vector_vector_period(const PaddedPODArray<NativeType>& dates,
                                     const PaddedPODArray<Int32>& periods,
                                     PaddedPODArray<NativeType>& res, NullMap& null_map) {
        // time_round(datetime, period)
        for (int i = 0; i < dates.size(); ++i) {
            if (periods[i] < 1) [[unlikely]] {
                null_map[i] = true;
                continue;
            }
            SET_NULLMAP_IF_FALSE((time_round_reinterpret_two_args(dates[i], periods[i], res[i])));
        }
    }

    static void vector_vector_vector(const PaddedPODArray<NativeType>& dates,
                                     const PaddedPODArray<Int32>& periods,
                                     const PaddedPODArray<NativeType>& origin_dates,
                                     PaddedPODArray<NativeType>& res, NullMap& null_map) {
        // time_round(datetime, period, origin)
        for (int i = 0; i < dates.size(); ++i) {
            if (periods[i] < 1) [[unlikely]] {
                null_map[i] = true;
                continue;
            }
            SET_NULLMAP_IF_FALSE((time_round_reinterpret_three_args(dates[i], periods[i],
                                                                    origin_dates[i], res[i])));
        }
    }

    //// time rounds
    static constexpr uint32_t MASK_YEAR_FOR_DATEV2 = ((uint32_t)-1) >> 23;
    static constexpr uint32_t MASK_YEAR_MONTH_FOR_DATEV2 = ((uint32_t)-1) >> 27;
    static constexpr uint64_t MASK_YEAR_FOR_DATETIMEV2 = ((uint64_t)-1) >> 18;
    static constexpr uint64_t MASK_YEAR_MONTH_FOR_DATETIMEV2 = ((uint64_t)-1) >> 22;
    static constexpr uint64_t MASK_YEAR_MONTH_DAY_FOR_DATETIMEV2 = ((uint64_t)-1) >> 27;
    static constexpr uint64_t MASK_YEAR_MONTH_DAY_HOUR_FOR_DATETIMEV2 = ((uint64_t)-1) >> 32;
    static constexpr uint64_t MASK_YEAR_MONTH_DAY_HOUR_MINUTE_FOR_DATETIMEV2 = ((uint64_t)-1) >> 38;

    /// time rounds interlayers
    ALWAYS_INLINE static bool time_round_reinterpret_one_arg(NativeType date, NativeType& res) {
        auto ts_arg = binary_cast<NativeType, DateValueType>(date);
        auto& ts_res = (DateValueType&)(res);
        if constexpr (Flag::Unit == WEEK) {
            // Only week use the FIRST SUNDAY
            ts_res = DateValueType::FIRST_SUNDAY;
            return time_round_two_args(ts_arg, 1, ts_res);
        } else {
            return time_round_one_arg(ts_arg, ts_res);
        }
    }

    ALWAYS_INLINE static bool time_round_reinterpret_two_args(NativeType date, Int32 period,
                                                              NativeType& res) {
        auto ts_arg = binary_cast<NativeType, DateValueType>(date);
        auto& ts_res = (DateValueType&)(res);

        if (can_use_optimize(period)) {
            floor_opt(ts_arg, ts_res, period);
            return true;
        } else {
            if constexpr (Flag::Unit == WEEK) {
                ts_res = DateValueType::FIRST_SUNDAY;
            } else {
                // Only week use the FIRST SUNDAY
                ts_res = DateValueType::FIRST_DAY;
            }

            return time_round_two_args(ts_arg, period, ts_res);
        }
    }

    ALWAYS_INLINE static bool time_round_reinterpret_three_args(NativeType date, Int32 period,
                                                                NativeType origin_date,
                                                                NativeType& res) {
        res = origin_date;
        auto ts2 = binary_cast<NativeType, DateValueType>(date);
        auto& ts1 = (DateValueType&)(res);
        return time_round_two_args(ts2, period, ts1);
    }

    /// time rounds real calculations
    static bool time_round_one_arg(const DateValueType& ts_arg, DateValueType& ts_res) {
        static_assert(Flag::Unit != WEEK);
        if constexpr (can_use_optimize(1)) {
            floor_opt_one_period(ts_arg, ts_res);
            return true;
        } else {
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                ts_res.reset_zero_by_type(ts_arg.type());
            }
            int64_t diff;
            int64_t part;
            if constexpr (Flag::Unit == YEAR) {
                diff = ts_arg.year();
                part = (ts_arg.month() - 1) + (ts_arg.day() - 1) + ts_arg.hour() + ts_arg.minute() +
                       ts_arg.second();
            }
            if constexpr (Flag::Unit == MONTH) {
                diff = ts_arg.year() * 12 + ts_arg.month() - 1;
                part = (ts_arg.day() - 1) + ts_arg.hour() + ts_arg.minute() + ts_arg.second();
            }
            if constexpr (Flag::Unit == DAY) {
                diff = ts_arg.daynr();
                part = ts_arg.hour() + ts_arg.minute() + ts_arg.second();
            }
            if constexpr (Flag::Unit == HOUR) {
                diff = ts_arg.daynr() * 24 + ts_arg.hour();
                part = ts_arg.minute() + ts_arg.second();
            }
            if constexpr (Flag::Unit == MINUTE) {
                diff = ts_arg.daynr() * 24L * 60 + ts_arg.hour() * 60 + ts_arg.minute();
                part = ts_arg.second();
            }
            if constexpr (Flag::Unit == SECOND) {
                diff = ts_arg.daynr() * 24L * 60 * 60 + ts_arg.hour() * 60L * 60 +
                       ts_arg.minute() * 60L + ts_arg.second();
                part = 0;
                if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>>) {
                    part = ts_arg.microsecond();
                }
            }

            if constexpr (Flag::Type == CEIL) {
                if (part) {
                    diff++;
                }
            }
            TimeInterval interval(Flag::Unit, diff, 1);
            return ts_res.template date_set_interval<Flag::Unit>(interval);
        }
    }

    static bool time_round_two_args(const DateValueType& ts_arg, const Int32 period,
                                    DateValueType& ts_res) {
        int64_t diff;
        int64_t trivial_part_ts_res;
        int64_t trivial_part_ts_arg;
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            if constexpr (Flag::Unit == YEAR) {
                diff = (ts_arg.year() - ts_res.year());
                trivial_part_ts_arg = ts_arg.to_int64() % 10000000000;
                trivial_part_ts_res = ts_res.to_int64() % 10000000000;
            }
            if constexpr (Flag::Unit == MONTH) {
                diff = (ts_arg.year() - ts_res.year()) * 12 + (ts_arg.month() - ts_res.month());
                trivial_part_ts_arg = ts_arg.to_int64() % 100000000;
                trivial_part_ts_res = ts_res.to_int64() % 100000000;
            }
            if constexpr (Flag::Unit == WEEK) {
                diff = ts_arg.daynr() / 7 - ts_res.daynr() / 7;
                trivial_part_ts_arg = ts_arg.daynr() % 7 * 24 * 3600 + ts_arg.hour() * 3600 +
                                      ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_res.daynr() % 7 * 24 * 3600 + ts_res.hour() * 3600 +
                                      ts_res.minute() * 60 + ts_res.second();
            }
            if constexpr (Flag::Unit == DAY) {
                diff = ts_arg.daynr() - ts_res.daynr();
                trivial_part_ts_arg = ts_arg.hour() * 3600 + ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_res.hour() * 3600 + ts_res.minute() * 60 + ts_res.second();
            }
            if constexpr (Flag::Unit == HOUR) {
                diff = (ts_arg.daynr() - ts_res.daynr()) * 24 + (ts_arg.hour() - ts_res.hour());
                trivial_part_ts_arg = ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_res.minute() * 60 + ts_res.second();
            }
            if constexpr (Flag::Unit == MINUTE) {
                diff = (ts_arg.daynr() - ts_res.daynr()) * 24 * 60 +
                       (ts_arg.hour() - ts_res.hour()) * 60 + (ts_arg.minute() - ts_res.minute());
                trivial_part_ts_arg = ts_arg.second();
                trivial_part_ts_res = ts_res.second();
            }
            if constexpr (Flag::Unit == SECOND) {
                diff = ts_arg.second_diff(ts_res);
                trivial_part_ts_res = 0;
                trivial_part_ts_arg = 0;
            }
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
            if constexpr (Flag::Unit == YEAR) {
                diff = (ts_arg.year() - ts_res.year());
                trivial_part_ts_arg = ts_arg.to_date_int_val() & MASK_YEAR_FOR_DATEV2;
                trivial_part_ts_res = ts_res.to_date_int_val() & MASK_YEAR_FOR_DATEV2;
            }
            if constexpr (Flag::Unit == MONTH) {
                diff = (ts_arg.year() - ts_res.year()) * 12 + (ts_arg.month() - ts_res.month());
                trivial_part_ts_arg = ts_arg.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATEV2;
                trivial_part_ts_res = ts_res.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATEV2;
            }
            if constexpr (Flag::Unit == WEEK) {
                diff = ts_arg.daynr() / 7 - ts_res.daynr() / 7;
                trivial_part_ts_arg = ts_arg.daynr() % 7 * 24 * 3600 + ts_arg.hour() * 3600 +
                                      ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_res.daynr() % 7 * 24 * 3600 + ts_res.hour() * 3600 +
                                      ts_res.minute() * 60 + ts_res.second();
            }
            if constexpr (Flag::Unit == DAY) {
                diff = ts_arg.daynr() - ts_res.daynr();
                trivial_part_ts_arg = ts_arg.hour() * 3600 + ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_res.hour() * 3600 + ts_res.minute() * 60 + ts_res.second();
            }
            if constexpr (Flag::Unit == HOUR) {
                diff = (ts_arg.daynr() - ts_res.daynr()) * 24 + (ts_arg.hour() - ts_res.hour());
                trivial_part_ts_arg = ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_res.minute() * 60 + ts_res.second();
            }
            if constexpr (Flag::Unit == MINUTE) {
                diff = (ts_arg.daynr() - ts_res.daynr()) * 24 * 60 +
                       (ts_arg.hour() - ts_res.hour()) * 60 + (ts_arg.minute() - ts_res.minute());
                trivial_part_ts_arg = ts_arg.second();
                trivial_part_ts_res = ts_res.second();
            }
            if constexpr (Flag::Unit == SECOND) {
                diff = ts_arg.second_diff(ts_res);
                trivial_part_ts_res = 0;
                trivial_part_ts_arg = 0;
            }
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>>) {
            if constexpr (Flag::Unit == YEAR) {
                diff = (ts_arg.year() - ts_res.year());
                trivial_part_ts_arg = ts_arg.to_date_int_val() & MASK_YEAR_FOR_DATETIMEV2;
                trivial_part_ts_res = ts_res.to_date_int_val() & MASK_YEAR_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == MONTH) {
                diff = (ts_arg.year() - ts_res.year()) * 12 + (ts_arg.month() - ts_res.month());
                trivial_part_ts_arg = ts_arg.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATETIMEV2;
                trivial_part_ts_res = ts_res.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == WEEK) {
                diff = ts_arg.daynr() / 7 - ts_res.daynr() / 7;
                trivial_part_ts_arg = ts_arg.daynr() % 7 * 24 * 3600 + ts_arg.hour() * 3600 +
                                      ts_arg.minute() * 60 + ts_arg.second();
                trivial_part_ts_res = ts_res.daynr() % 7 * 24 * 3600 + ts_res.hour() * 3600 +
                                      ts_res.minute() * 60 + ts_res.second();
            }
            if constexpr (Flag::Unit == DAY) {
                diff = ts_arg.daynr() - ts_res.daynr();
                trivial_part_ts_arg = ts_arg.to_date_int_val() & MASK_YEAR_MONTH_DAY_FOR_DATETIMEV2;
                trivial_part_ts_res = ts_res.to_date_int_val() & MASK_YEAR_MONTH_DAY_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == HOUR) {
                diff = (ts_arg.daynr() - ts_res.daynr()) * 24 + (ts_arg.hour() - ts_res.hour());
                trivial_part_ts_arg =
                        ts_arg.to_date_int_val() & MASK_YEAR_MONTH_DAY_HOUR_FOR_DATETIMEV2;
                trivial_part_ts_res =
                        ts_res.to_date_int_val() & MASK_YEAR_MONTH_DAY_HOUR_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == MINUTE) {
                diff = (ts_arg.daynr() - ts_res.daynr()) * 24 * 60 +
                       (ts_arg.hour() - ts_res.hour()) * 60 + (ts_arg.minute() - ts_res.minute());
                trivial_part_ts_arg =
                        ts_arg.to_date_int_val() & MASK_YEAR_MONTH_DAY_HOUR_MINUTE_FOR_DATETIMEV2;
                trivial_part_ts_res =
                        ts_res.to_date_int_val() & MASK_YEAR_MONTH_DAY_HOUR_MINUTE_FOR_DATETIMEV2;
            }
            if constexpr (Flag::Unit == SECOND) {
                diff = ts_arg.second_diff(ts_res);
                trivial_part_ts_arg = ts_arg.microsecond();
                trivial_part_ts_res = ts_res.microsecond();
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

        //round down/up inside time period(several time-units)
        int64_t delta_inside_period = diff >= 0 ? diff % period : (diff % period + period) % period;
        int64_t step = diff - delta_inside_period +
                       (Flag::Type == FLOOR        ? 0
                        : delta_inside_period == 0 ? 0
                                                   : period);
        bool is_neg = step < 0;
        TimeInterval interval(Flag::Unit, is_neg ? -step : step, is_neg);
        return ts_res.template date_add_interval<Flag::Unit>(interval);
    }

    /// optimized path
    constexpr static bool can_use_optimize(int period) {
        if constexpr (!std::is_same_v<DateValueType, VecDateTimeValue> && Flag::Type == FLOOR) {
            if constexpr (Flag::Unit == YEAR || Flag::Unit == DAY) {
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
        if constexpr (Flag::Unit == MONTH) {
            ts1.unchecked_set_time(ts2.year(), ts2.month(), 1, 0, 0, 0);
        }
        if constexpr (Flag::Unit == DAY) {
            ts1.unchecked_set_time(ts2.year(), ts2.month(), ts2.day(), 0, 0, 0);
        }

        // only DateTimeV2ValueType type have hour minute second
        if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>>) {
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

#define TIME_ROUND_WITH_DELTA_TYPE(IMPL, NAME, UNIT, TYPE, DELTA)                                 \
    using FunctionOneArg##IMPL##DELTA =                                                           \
            FunctionDateTimeFloorCeil<IMPL, DataTypeDateTime,                                     \
                                      1>; /*DateTime and Date is same here*/                      \
    using FunctionTwoArg##IMPL##DELTA = FunctionDateTimeFloorCeil<IMPL, DataTypeDateTime, 2>;     \
    using FunctionThreeArg##IMPL##DELTA = FunctionDateTimeFloorCeil<IMPL, DataTypeDateTime, 3>;   \
    using FunctionDateV2OneArg##IMPL##DELTA = FunctionDateTimeFloorCeil<IMPL, DataTypeDateV2, 1>; \
    using FunctionDateV2TwoArg##IMPL##DELTA = FunctionDateTimeFloorCeil<IMPL, DataTypeDateV2, 2>; \
    using FunctionDateV2ThreeArg##IMPL##DELTA =                                                   \
            FunctionDateTimeFloorCeil<IMPL, DataTypeDateV2, 3>;                                   \
    using FunctionDateTimeV2OneArg##IMPL##DELTA =                                                 \
            FunctionDateTimeFloorCeil<IMPL, DataTypeDateTimeV2, 1>;                               \
    using FunctionDateTimeV2TwoArg##IMPL##DELTA =                                                 \
            FunctionDateTimeFloorCeil<IMPL, DataTypeDateTimeV2, 2>;                               \
    using FunctionDateTimeV2ThreeArg##IMPL##DELTA =                                               \
            FunctionDateTimeFloorCeil<IMPL, DataTypeDateTimeV2, 3>;

#define TIME_ROUND(IMPL, NAME, UNIT, TYPE)                                                       \
    struct IMPL {                                                                                \
        static constexpr auto name = #NAME;                                                      \
        static constexpr TimeUnit Unit = UNIT;                                                   \
        static constexpr auto Type = TYPE;                                                       \
    };                                                                                           \
                                                                                                 \
    TIME_ROUND_WITH_DELTA_TYPE(IMPL, NAME, UNIT, TYPE, Int32)                                    \
    using FunctionDateV2TwoArg##IMPL = FunctionDateTimeFloorCeil<IMPL, DataTypeDateV2, 2, true>; \
    using FunctionDateTimeV2TwoArg##IMPL =                                                       \
            FunctionDateTimeFloorCeil<IMPL, DataTypeDateTimeV2, 2, true>;                        \
    using FunctionDateTimeTwoArg##IMPL =                                                         \
            FunctionDateTimeFloorCeil<IMPL, DataTypeDateTime, 2,                                 \
                                      true>; /*DateTime and Date is same here*/

TIME_ROUND(YearFloor, year_floor, YEAR, FLOOR);
TIME_ROUND(MonthFloor, month_floor, MONTH, FLOOR);
TIME_ROUND(WeekFloor, week_floor, WEEK, FLOOR);
TIME_ROUND(DayFloor, day_floor, DAY, FLOOR);
TIME_ROUND(HourFloor, hour_floor, HOUR, FLOOR);
TIME_ROUND(MinuteFloor, minute_floor, MINUTE, FLOOR);
TIME_ROUND(SecondFloor, second_floor, SECOND, FLOOR);

TIME_ROUND(YearCeil, year_ceil, YEAR, CEIL);
TIME_ROUND(MonthCeil, month_ceil, MONTH, CEIL);
TIME_ROUND(WeekCeil, week_ceil, WEEK, CEIL);
TIME_ROUND(DayCeil, day_ceil, DAY, CEIL);
TIME_ROUND(HourCeil, hour_ceil, HOUR, CEIL);
TIME_ROUND(MinuteCeil, minute_ceil, MINUTE, CEIL);
TIME_ROUND(SecondCeil, second_ceil, SECOND, CEIL);

void register_function_datetime_floor_ceil(SimpleFunctionFactory& factory) {
#define REGISTER_FUNC_WITH_DELTA_TYPE(IMPL, DELTA)                        \
    factory.register_function<FunctionOneArg##IMPL##DELTA>();             \
    factory.register_function<FunctionTwoArg##IMPL##DELTA>();             \
    factory.register_function<FunctionThreeArg##IMPL##DELTA>();           \
    factory.register_function<FunctionDateV2OneArg##IMPL##DELTA>();       \
    factory.register_function<FunctionDateV2TwoArg##IMPL##DELTA>();       \
    factory.register_function<FunctionDateV2ThreeArg##IMPL##DELTA>();     \
    factory.register_function<FunctionDateTimeV2OneArg##IMPL##DELTA>();   \
    factory.register_function<FunctionDateTimeV2TwoArg##IMPL##DELTA>();   \
    factory.register_function<FunctionDateTimeV2ThreeArg##IMPL##DELTA>(); \
    factory.register_function<FunctionDateTimeV2TwoArg##IMPL>();          \
    factory.register_function<FunctionDateTimeTwoArg##IMPL>();            \
    factory.register_function<FunctionDateV2TwoArg##IMPL>();

#define REGISTER_FUNC(IMPL) REGISTER_FUNC_WITH_DELTA_TYPE(IMPL, Int32)

    REGISTER_FUNC(YearFloor);
    REGISTER_FUNC(MonthFloor);
    REGISTER_FUNC(WeekFloor);
    REGISTER_FUNC(DayFloor);
    REGISTER_FUNC(HourFloor);
    REGISTER_FUNC(MinuteFloor);
    REGISTER_FUNC(SecondFloor);

    REGISTER_FUNC(YearCeil);
    REGISTER_FUNC(MonthCeil);
    REGISTER_FUNC(WeekCeil);
    REGISTER_FUNC(DayCeil);
    REGISTER_FUNC(HourCeil);
    REGISTER_FUNC(MinuteCeil);
    REGISTER_FUNC(SecondCeil);
}
#undef FLOOR
#undef CEIL
} // namespace doris::vectorized
