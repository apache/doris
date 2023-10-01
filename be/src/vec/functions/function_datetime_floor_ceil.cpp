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
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/status.h"
#include "util/binary_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/typeid_cast.h"
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
class FunctionContext;

namespace vectorized {
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
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename Impl, typename DateValueType, typename DeltaValueType, int ArgNum, bool UseDelta>
class FunctionDateTimeFloorCeil : public IFunction {
public:
    using ReturnDataType = std::conditional_t<
            std::is_same_v<DateValueType, VecDateTimeValue>, DataTypeDateTime,
            std::conditional_t<std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>,
                               DataTypeDateV2, DataTypeDateTimeV2>>;
    using NativeType = std::conditional_t<
            std::is_same_v<DateValueType, VecDateTimeValue>, Int64,
            std::conditional_t<std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>, UInt32,
                               UInt64>>;
    using DeltaDataType =
            std::conditional_t<std::is_same_v<DeltaValueType, Int32>, DataTypeInt32, DataTypeInt64>;
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeFloorCeil>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<ReturnDataType>());
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue> && ArgNum == 1) {
            return {std::make_shared<DataTypeDateTime>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>> &&
                             ArgNum == 1) {
            return {std::make_shared<DataTypeDateV2>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>> &&
                             ArgNum == 1) {
            return {std::make_shared<DataTypeDateTimeV2>()};
        } else if constexpr (std::is_same_v<DateValueType, VecDateTimeValue> && ArgNum == 2 &&
                             UseDelta) {
            return {std::make_shared<DataTypeDateTime>(), std::make_shared<DeltaDataType>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>> &&
                             ArgNum == 2 && UseDelta) {
            return {std::make_shared<DataTypeDateV2>(), std::make_shared<DeltaDataType>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>> &&
                             ArgNum == 2 && UseDelta) {
            return {std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DeltaDataType>()};
        } else if constexpr (std::is_same_v<DateValueType, VecDateTimeValue> && ArgNum == 2 &&
                             !UseDelta) {
            return {std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeDateTime>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>> &&
                             ArgNum == 2 && !UseDelta) {
            return {std::make_shared<DataTypeDateV2>(), std::make_shared<DataTypeDateV2>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>> &&
                             ArgNum == 2 && !UseDelta) {
            return {std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeDateTimeV2>()};
        } else if constexpr (std::is_same_v<DateValueType, VecDateTimeValue> && ArgNum == 3) {
            return {std::make_shared<DataTypeDateTime>(), std::make_shared<DeltaDataType>(),
                    std::make_shared<DataTypeDateTime>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>> &&
                             ArgNum == 3) {
            return {std::make_shared<DataTypeDateV2>(), std::make_shared<DeltaDataType>(),
                    std::make_shared<DataTypeDateV2>()};
        } else {
            return {std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DeltaDataType>(),
                    std::make_shared<DataTypeDateTimeV2>()};
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const ColumnPtr source_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (const auto* sources =
                    check_and_get_column<ColumnVector<NativeType>>(source_col.get())) {
            auto col_to = ColumnVector<NativeType>::create();
            col_to->resize(input_rows_count);
            auto null_map = ColumnVector<UInt8>::create();
            null_map->get_data().resize_fill(input_rows_count, false);

            if constexpr (ArgNum == 1) {
                Impl::template vector<NativeType>(sources->get_data(), col_to->get_data(),
                                                  null_map->get_data());
            } else if constexpr (ArgNum == 2) {
                const IColumn& delta_column = *block.get_by_position(arguments[1]).column;
                if (const auto* delta_const_column =
                            typeid_cast<const ColumnConst*>(&delta_column)) {
                    if (block.get_by_position(arguments[1]).type->get_type_id() !=
                        TypeIndex::Int32) {
                        Impl::template vector_constant<NativeType>(
                                sources->get_data(),
                                delta_const_column->get_field().get<NativeType>(),
                                col_to->get_data(), null_map->get_data());
                    } else {
                        Impl::template vector_constant_delta<NativeType, DeltaValueType>(
                                sources->get_data(), delta_const_column->get_field().get<Int32>(),
                                col_to->get_data(), null_map->get_data());
                    }
                } else {
                    if (const auto* delta_vec_column0 =
                                check_and_get_column<ColumnVector<NativeType>>(delta_column)) {
                        Impl::vector_vector(sources->get_data(), delta_vec_column0->get_data(),
                                            col_to->get_data(), null_map->get_data());
                    } else {
                        const auto* delta_vec_column1 =
                                check_and_get_column<ColumnVector<DeltaValueType>>(delta_column);
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
                        check_and_get_column<ColumnVector<DeltaValueType>>(*arg1_column_ptr);
                const auto arg2_column =
                        check_and_get_column<ColumnVector<NativeType>>(*arg2_column_ptr);
                DCHECK(arg1_column != nullptr);
                DCHECK(arg2_column != nullptr);
                Impl::template vector_vector<NativeType, DeltaValueType>(
                        sources->get_data(), arg1_column->get_data(), arg2_column->get_data(),
                        col_to->get_data(), null_map->get_data());
            }

            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(null_map));
        } else {
            return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        Impl::name);
        }
        return Status::OK();
    }
};

template <typename Impl>
struct FloorCeilImpl {
    static constexpr auto name = Impl::name;

    template <typename NativeType>
    static void vector(const PaddedPODArray<NativeType>& dates, PaddedPODArray<NativeType>& res,
                       NullMap& null_map) {
        // vector_constant_delta<NativeType, Int32>(dates, Int32(1), res, null_map);
        for (int i = 0; i < dates.size(); ++i) {
            if constexpr (std::is_same_v<NativeType, UInt32>) {
                Impl::template time_round<UInt32, DateV2Value<DateV2ValueType>>(dates[i], res[i],
                                                                                null_map[i]);
            } else if constexpr (std::is_same_v<NativeType, UInt64>) {
                Impl::template time_round<UInt64, DateV2Value<DateTimeV2ValueType>>(
                        dates[i], res[i], null_map[i]);
            } else {
                Impl::template time_round<Int64, VecDateTimeValue>(dates[i], res[i], null_map[i]);
            }
        }
    }

    template <typename NativeType>
    static void vector_constant(const PaddedPODArray<NativeType>& dates, NativeType origin_date,
                                PaddedPODArray<NativeType>& res, NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            if constexpr (std::is_same_v<NativeType, UInt32>) {
                Impl::template time_round<UInt32, DateV2Value<DateV2ValueType>>(
                        dates[i], Int32(1), origin_date, res[i], null_map[i]);
            } else if constexpr (std::is_same_v<NativeType, UInt64>) {
                Impl::template time_round<UInt64, DateV2Value<DateTimeV2ValueType>>(
                        dates[i], Int32(1), origin_date, res[i], null_map[i]);
            } else {
                Impl::template time_round<Int64, VecDateTimeValue>(dates[i], Int32(1), origin_date,
                                                                   res[i], null_map[i]);
            }
        }
    }

    template <typename NativeType, typename DeltaType>
    static void vector_constant_delta(const PaddedPODArray<NativeType>& dates, DeltaType period,
                                      PaddedPODArray<NativeType>& res, NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            if constexpr (std::is_same_v<NativeType, UInt32>) {
                Impl::template time_round<UInt32, DateV2Value<DateV2ValueType>>(
                        dates[i], period, res[i], null_map[i]);
            } else if constexpr (std::is_same_v<NativeType, UInt64>) {
                Impl::template time_round<UInt64, DateV2Value<DateTimeV2ValueType>>(
                        dates[i], period, res[i], null_map[i]);
            } else {
                Impl::template time_round<Int64, VecDateTimeValue>(dates[i], period, res[i],
                                                                   null_map[i]);
            }
        }
    }

    template <typename NativeType>
    static void vector_vector(const PaddedPODArray<NativeType>& dates,
                              const PaddedPODArray<NativeType>& origin_dates,
                              PaddedPODArray<NativeType>& res, NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            if constexpr (std::is_same_v<NativeType, UInt32>) {
                Impl::template time_round<UInt32, DateV2Value<DateV2ValueType>>(
                        dates[i], Int32(1), origin_dates[i], res[i], null_map[i]);
            } else if constexpr (std::is_same_v<NativeType, UInt64>) {
                Impl::template time_round<UInt64, DateV2Value<DateTimeV2ValueType>>(
                        dates[i], Int32(1), origin_dates[i], res[i], null_map[i]);
            } else {
                Impl::template time_round<Int64, VecDateTimeValue>(
                        dates[i], Int32(1), origin_dates[i], res[i], null_map[i]);
            }
        }
    }

    template <typename NativeType, typename DeltaType>
    static void vector_vector(const PaddedPODArray<NativeType>& dates,
                              const PaddedPODArray<DeltaType>& periods,
                              PaddedPODArray<NativeType>& res, NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            if constexpr (std::is_same_v<NativeType, UInt32>) {
                Impl::template time_round<UInt32, DateV2Value<DateV2ValueType>>(
                        dates[i], periods[i], res[i], null_map[i]);
            } else if constexpr (std::is_same_v<NativeType, UInt64>) {
                Impl::template time_round<UInt64, DateV2Value<DateTimeV2ValueType>>(
                        dates[i], periods[i], res[i], null_map[i]);
            } else {
                Impl::template time_round<Int64, VecDateTimeValue>(dates[i], periods[i], res[i],
                                                                   null_map[i]);
            }
        }
    }

    template <typename NativeType, typename DeltaType>
    static void vector_vector(const PaddedPODArray<NativeType>& dates,
                              const PaddedPODArray<DeltaType>& periods,
                              const PaddedPODArray<NativeType>& origin_dates,
                              PaddedPODArray<NativeType>& res, NullMap& null_map) {
        for (int i = 0; i < dates.size(); ++i) {
            if constexpr (std::is_same_v<NativeType, UInt32>) {
                Impl::template time_round<UInt32, DateV2Value<DateV2ValueType>>(
                        dates[i], periods[i], origin_dates[i], res[i], null_map[i]);
            } else if constexpr (std::is_same_v<NativeType, UInt64>) {
                Impl::template time_round<UInt64, DateV2Value<DateTimeV2ValueType>>(
                        dates[i], periods[i], origin_dates[i], res[i], null_map[i]);
            } else {
                Impl::template time_round<Int64, VecDateTimeValue>(
                        dates[i], periods[i], origin_dates[i], res[i], null_map[i]);
            }
        }
    }
};

#define FLOOR 0
#define CEIL 1

template <typename Impl, typename DateValueType>
struct TimeRoundOpt {
    constexpr static bool can_use_optimize(int period) {
        if constexpr (!std::is_same_v<DateValueType, VecDateTimeValue> && Impl::Type == FLOOR) {
            if constexpr (Impl::Unit == YEAR || Impl::Unit == MONTH || Impl::Unit == DAY ||
                          Impl::Unit == MINUTE || Impl::Unit == SECOND) {
                return period == 1;
            }
            if constexpr (Impl::Unit == HOUR) {
                return period <= 23 && 24 % period == 0;
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

            // Optimize the performance of the datetimev2 type on the floor operation.
            // Now supports unit hour
            if constexpr (Impl::Unit == HOUR && !std::is_same_v<DateValueType, VecDateTimeValue>) {
                int hour = ts2.hour();
                int new_hour = hour / period * period;
                if (new_hour >= 24) {
                    new_hour = new_hour % 24;
                }
                ts1.set_int_val(ts2.to_date_int_val() & MASK_HOUR_FLOOR);
                ts1.template set_time_unit<TimeUnit::HOUR>(new_hour);
            }
        }
    }

    static void floor_opt_one_period(const DateValueType& ts2, DateValueType& ts1) {
        if constexpr (Impl::Unit == YEAR) {
            ts1.set_time(ts2.year(), 1, 1, 0, 0, 0);
        }
        if constexpr (Impl::Unit == MONTH) {
            ts1.set_time(ts2.year(), ts2.month(), 1, 0, 0, 0);
        }
        if constexpr (Impl::Unit == DAY) {
            ts1.set_time(ts2.year(), ts2.month(), ts2.day(), 0, 0, 0);
        }

        if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>>) {
            static constexpr uint64_t MASK_HOUR_FLOOR =
                    0b1111111111111111111111111111111100000000000000000000000000000000;
            static constexpr uint64_t MASK_MINUTE_FLOOR =
                    0b1111111111111111111111111111111111111100000000000000000000000000;
            static constexpr uint64_t MASK_SECOND_FLOOR =
                    0b1111111111111111111111111111111111111111111100000000000000000000;

            // Optimize the performance of the datetimev2 type on the floor operation.
            // Now supports unit biger than SECOND
            if constexpr (Impl::Unit == HOUR) {
                ts1.set_int_val(ts2.to_date_int_val() & MASK_HOUR_FLOOR);
            }
            if constexpr (Impl::Unit == MINUTE) {
                ts1.set_int_val(ts2.to_date_int_val() & MASK_MINUTE_FLOOR);
            }
            if constexpr (Impl::Unit == SECOND) {
                ts1.set_int_val(ts2.to_date_int_val() & MASK_SECOND_FLOOR);
            }
        }
    }
};

template <typename Impl>
struct TimeRound {
    static constexpr auto name = Impl::name;
    static constexpr uint64_t FIRST_DAY = 19700101000000;
    static constexpr uint64_t FIRST_SUNDAY = 19700104000000;

    static constexpr uint32_t MASK_YEAR_FOR_DATEV2 = ((uint32_t)-1) >> 23;
    static constexpr uint32_t MASK_YEAR_MONTH_FOR_DATEV2 = ((uint32_t)-1) >> 27;

    static constexpr uint64_t MASK_YEAR_FOR_DATETIMEV2 = ((uint64_t)-1) >> 18;
    static constexpr uint64_t MASK_YEAR_MONTH_FOR_DATETIMEV2 = ((uint64_t)-1) >> 22;

    template <typename DateValueType>
    static void time_round(const DateValueType& ts2, const Int32 period, DateValueType& ts1) {
        int64_t diff;
        int64_t trivial_part_ts1;
        int64_t trivial_part_ts2;
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            if constexpr (Impl::Unit == YEAR) {
                diff = (ts2.year() - ts1.year());
                trivial_part_ts2 = ts2.to_int64() % 10000000000;
                trivial_part_ts1 = ts1.to_int64() % 10000000000;
            }
            if constexpr (Impl::Unit == MONTH) {
                diff = (ts2.year() - ts1.year()) * 12 + (ts2.month() - ts1.month());
                trivial_part_ts2 = ts2.to_int64() % 100000000;
                trivial_part_ts1 = ts1.to_int64() % 100000000;
            }
            if constexpr (Impl::Unit == WEEK) {
                diff = ts2.daynr() / 7 - ts1.daynr() / 7;
                trivial_part_ts2 = ts2.daynr() % 7 * 24 * 3600 + ts2.hour() * 3600 +
                                   ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.daynr() % 7 * 24 * 3600 + ts1.hour() * 3600 +
                                   ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == DAY) {
                diff = ts2.daynr() - ts1.daynr();
                trivial_part_ts2 = ts2.hour() * 3600 + ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.hour() * 3600 + ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == HOUR) {
                diff = (ts2.daynr() - ts1.daynr()) * 24 + (ts2.hour() - ts1.hour());
                trivial_part_ts2 = ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == MINUTE) {
                diff = (ts2.daynr() - ts1.daynr()) * 24 * 60 + (ts2.hour() - ts1.hour()) * 60 +
                       (ts2.minute() - ts1.minute());
                trivial_part_ts2 = ts2.second();
                trivial_part_ts1 = ts1.second();
            }
            if constexpr (Impl::Unit == SECOND) {
                diff = ts2.second_diff(ts1);
                trivial_part_ts1 = 0;
                trivial_part_ts2 = 0;
            }
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
            if constexpr (Impl::Unit == YEAR) {
                diff = (ts2.year() - ts1.year());
                trivial_part_ts2 = ts2.to_date_int_val() & MASK_YEAR_FOR_DATEV2;
                trivial_part_ts1 = ts1.to_date_int_val() & MASK_YEAR_FOR_DATEV2;
            }
            if constexpr (Impl::Unit == MONTH) {
                diff = (ts2.year() - ts1.year()) * 12 + (ts2.month() - ts1.month());
                trivial_part_ts2 = ts2.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATEV2;
                trivial_part_ts1 = ts1.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATEV2;
            }
            if constexpr (Impl::Unit == WEEK) {
                diff = ts2.daynr() / 7 - ts1.daynr() / 7;
                trivial_part_ts2 = ts2.daynr() % 7 * 24 * 3600 + ts2.hour() * 3600 +
                                   ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.daynr() % 7 * 24 * 3600 + ts1.hour() * 3600 +
                                   ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == DAY) {
                diff = ts2.daynr() - ts1.daynr();
                trivial_part_ts2 = ts2.hour() * 3600 + ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.hour() * 3600 + ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == HOUR) {
                diff = (ts2.daynr() - ts1.daynr()) * 24 + (ts2.hour() - ts1.hour());
                trivial_part_ts2 = ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == MINUTE) {
                diff = (ts2.daynr() - ts1.daynr()) * 24 * 60 + (ts2.hour() - ts1.hour()) * 60 +
                       (ts2.minute() - ts1.minute());
                trivial_part_ts2 = ts2.second();
                trivial_part_ts1 = ts1.second();
            }
            if constexpr (Impl::Unit == SECOND) {
                diff = ts2.second_diff(ts1);
                trivial_part_ts1 = 0;
                trivial_part_ts2 = 0;
            }
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>>) {
            if constexpr (Impl::Unit == YEAR) {
                diff = (ts2.year() - ts1.year());
                trivial_part_ts2 = ts2.to_date_int_val() & MASK_YEAR_FOR_DATETIMEV2;
                trivial_part_ts1 = ts1.to_date_int_val() & MASK_YEAR_FOR_DATETIMEV2;
            }
            if constexpr (Impl::Unit == MONTH) {
                diff = (ts2.year() - ts1.year()) * 12 + (ts2.month() - ts1.month());
                trivial_part_ts2 = ts2.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATETIMEV2;
                trivial_part_ts1 = ts1.to_date_int_val() & MASK_YEAR_MONTH_FOR_DATETIMEV2;
            }
            if constexpr (Impl::Unit == WEEK) {
                diff = ts2.daynr() / 7 - ts1.daynr() / 7;
                trivial_part_ts2 = ts2.daynr() % 7 * 24 * 3600 + ts2.hour() * 3600 +
                                   ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.daynr() % 7 * 24 * 3600 + ts1.hour() * 3600 +
                                   ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == DAY) {
                diff = ts2.daynr() - ts1.daynr();
                trivial_part_ts2 = ts2.hour() * 3600 + ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.hour() * 3600 + ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == HOUR) {
                diff = (ts2.daynr() - ts1.daynr()) * 24 + (ts2.hour() - ts1.hour());
                trivial_part_ts2 = ts2.minute() * 60 + ts2.second();
                trivial_part_ts1 = ts1.minute() * 60 + ts1.second();
            }
            if constexpr (Impl::Unit == MINUTE) {
                diff = (ts2.daynr() - ts1.daynr()) * 24 * 60 + (ts2.hour() - ts1.hour()) * 60 +
                       (ts2.minute() - ts1.minute());
                trivial_part_ts2 = ts2.second();
                trivial_part_ts1 = ts1.second();
            }
            if constexpr (Impl::Unit == SECOND) {
                diff = ts2.second_diff(ts1);
                trivial_part_ts1 = 0;
                trivial_part_ts2 = 0;
            }
        }

        //round down/up to specific time-unit(HOUR/DAY/MONTH...) by increase/decrease diff variable
        if constexpr (Impl::Type == CEIL) {
            //e.g. hour_ceil(ts: 00:00:40, origin: 00:00:30), ts should be rounded to 01:00:30
            diff += trivial_part_ts2 > trivial_part_ts1;
        }
        if constexpr (Impl::Type == FLOOR) {
            //e.g. hour_floor(ts: 01:00:20, origin: 00:00:30), ts should be rounded to 00:00:30
            diff -= trivial_part_ts2 < trivial_part_ts1;
        }

        //round down/up inside time period(several time-units)
        int64_t count = period;
        int64_t delta_inside_period = diff >= 0 ? diff % count : (diff % count + count) % count;
        int64_t step = diff - delta_inside_period +
                       (Impl::Type == FLOOR        ? 0
                        : delta_inside_period == 0 ? 0
                                                   : count);
        bool is_neg = step < 0;
        TimeInterval interval(Impl::Unit, is_neg ? -step : step, is_neg);
        ts1.template date_add_interval<Impl::Unit>(interval);
    }

    template <typename DateValueType, Int32 period>
    static void time_round_with_constant_optimization(const DateValueType& ts2,
                                                      DateValueType& ts1) {
        time_round<DateValueType>(ts2, period, ts1);
    }

    template <typename DateValueType>
    static void time_round(const DateValueType& ts2, DateValueType& ts1) {
        static_assert(Impl::Unit != WEEK);
        if constexpr (TimeRoundOpt<Impl, DateValueType>::can_use_optimize(1)) {
            TimeRoundOpt<Impl, DateValueType>::floor_opt_one_period(ts2, ts1);
        } else {
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                ts1.reset_zero_by_type(ts2.type());
            }
            int64_t diff;
            int64_t part;
            if constexpr (Impl::Unit == YEAR) {
                diff = ts2.year();
                part = (ts2.month() - 1) + (ts2.day() - 1) + ts2.hour() + ts2.minute() +
                       ts2.second();
            }
            if constexpr (Impl::Unit == MONTH) {
                diff = ts2.year() * 12 + ts2.month() - 1;
                part = (ts2.day() - 1) + ts2.hour() + ts2.minute() + ts2.second();
            }
            if constexpr (Impl::Unit == DAY) {
                diff = ts2.daynr();
                part = ts2.hour() + ts2.minute() + ts2.second();
            }
            if constexpr (Impl::Unit == HOUR) {
                diff = ts2.daynr() * 24 + ts2.hour();
                part = ts2.minute() + ts2.second();
            }
            if constexpr (Impl::Unit == MINUTE) {
                diff = ts2.daynr() * 24L * 60 + ts2.hour() * 60 + ts2.minute();
                part = ts2.second();
            }
            if constexpr (Impl::Unit == SECOND) {
                diff = ts2.daynr() * 24L * 60 * 60 + ts2.hour() * 60L * 60 + ts2.minute() * 60L +
                       ts2.second();
                part = 0;
                if constexpr (std::is_same_v<DateValueType, DateV2Value<DateTimeV2ValueType>>) {
                    part = ts2.microsecond();
                }
            }

            if constexpr (Impl::Type == CEIL) {
                if (part) {
                    diff++;
                }
            }
            TimeInterval interval(Impl::Unit, diff, 1);
            ts1.template date_set_interval<Impl::Unit>(interval);
        }
    }

    template <typename NativeType, typename DateValueType>
    static void time_round(NativeType date, Int32 period, NativeType origin_date, NativeType& res,
                           UInt8& is_null) {
        res = origin_date;
        auto ts2 = binary_cast<NativeType, DateValueType>(date);
        auto& ts1 = (DateValueType&)(res);
        TimeRound<Impl>::template time_round<DateValueType>(ts2, period, ts1);
    }

    template <typename NativeType, typename DateValueType, Int32 period>
    static void time_round_with_constant_optimization(NativeType date, NativeType origin_date,
                                                      NativeType& res) {
        res = origin_date;
        auto ts2 = binary_cast<NativeType, DateValueType>(date);
        auto& ts1 = (DateValueType&)(res);
        TimeRound<Impl>::template time_round_with_constant_optimization<DateValueType, period>(ts2,
                                                                                               ts1);
    }

    template <typename NativeType, typename DateValueType>
    static void time_round(NativeType date, Int32 period, NativeType& res, UInt8& is_null) {
        auto ts2 = binary_cast<NativeType, DateValueType>(date);
        if (!ts2.is_valid_date() || period < 0) {
            is_null = true;
            return;
        }
        auto& ts1 = (DateValueType&)(res);

        if (TimeRoundOpt<Impl, DateValueType>::can_use_optimize(period)) {
            TimeRoundOpt<Impl, DateValueType>::floor_opt(ts2, ts1, period);
        } else {
            if constexpr (Impl::Unit != WEEK) {
                ts1.from_olap_datetime(FIRST_DAY);
            } else {
                // Only week use the FIRST SUNDAY
                ts1.from_olap_datetime(FIRST_SUNDAY);
            }
            TimeRound<Impl>::template time_round<DateValueType>(ts2, period, ts1);
        }
    }

    template <typename NativeType, typename DateValueType>
    static void time_round(NativeType date, NativeType& res, UInt8& is_null) {
        auto ts2 = binary_cast<NativeType, DateValueType>(date);
        if (!ts2.is_valid_date()) {
            is_null = true;
            return;
        }
        auto& ts1 = (DateValueType&)(res);
        if constexpr (Impl::Unit != WEEK) {
            TimeRound<Impl>::template time_round<DateValueType>(ts2, ts1);
        } else {
            // Only week use the FIRST SUNDAY
            ts1.from_olap_datetime(FIRST_SUNDAY);
            TimeRound<Impl>::template time_round<DateValueType>(ts2, 1, ts1);
        }
    }
};

#define TIME_ROUND_WITH_DELTA_TYPE(CLASS, NAME, UNIT, TYPE, DELTA)                                 \
    using FunctionOneArg##CLASS##DELTA =                                                           \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>, VecDateTimeValue, DELTA, 1, \
                                      false>;                                                      \
    using FunctionTwoArg##CLASS##DELTA =                                                           \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>, VecDateTimeValue, DELTA, 2, \
                                      false>;                                                      \
    using FunctionThreeArg##CLASS##DELTA =                                                         \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>, VecDateTimeValue, DELTA, 3, \
                                      false>;                                                      \
    using FunctionDateV2OneArg##CLASS##DELTA =                                                     \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>,                             \
                                      DateV2Value<DateV2ValueType>, DELTA, 1, false>;              \
    using FunctionDateV2TwoArg##CLASS##DELTA =                                                     \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>,                             \
                                      DateV2Value<DateV2ValueType>, DELTA, 2, false>;              \
    using FunctionDateV2ThreeArg##CLASS##DELTA =                                                   \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>,                             \
                                      DateV2Value<DateV2ValueType>, DELTA, 3, false>;              \
    using FunctionDateTimeV2OneArg##CLASS##DELTA =                                                 \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>,                             \
                                      DateV2Value<DateTimeV2ValueType>, DELTA, 1, false>;          \
    using FunctionDateTimeV2TwoArg##CLASS##DELTA =                                                 \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>,                             \
                                      DateV2Value<DateTimeV2ValueType>, DELTA, 2, false>;          \
    using FunctionDateTimeV2ThreeArg##CLASS##DELTA =                                               \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>,                             \
                                      DateV2Value<DateTimeV2ValueType>, DELTA, 3, false>;

#define TIME_ROUND(CLASS, NAME, UNIT, TYPE)                                                        \
    struct CLASS {                                                                                 \
        static constexpr auto name = #NAME;                                                        \
        static constexpr TimeUnit Unit = UNIT;                                                     \
        static constexpr auto Type = TYPE;                                                         \
    };                                                                                             \
                                                                                                   \
    TIME_ROUND_WITH_DELTA_TYPE(CLASS, NAME, UNIT, TYPE, Int32)                                     \
    TIME_ROUND_WITH_DELTA_TYPE(CLASS, NAME, UNIT, TYPE, Int64)                                     \
    using FunctionDateTimeV2TwoArg##CLASS =                                                        \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>,                             \
                                      DateV2Value<DateTimeV2ValueType>, Int32, 2, true>;           \
    using FunctionDateV2TwoArg##CLASS =                                                            \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>,                             \
                                      DateV2Value<DateV2ValueType>, Int32, 2, true>;               \
    using FunctionDateTimeTwoArg##CLASS =                                                          \
            FunctionDateTimeFloorCeil<FloorCeilImpl<TimeRound<CLASS>>, VecDateTimeValue, Int32, 2, \
                                      true>;

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
#define REGISTER_FUNC_WITH_DELTA_TYPE(CLASS, DELTA)                        \
    factory.register_function<FunctionOneArg##CLASS##DELTA>();             \
    factory.register_function<FunctionTwoArg##CLASS##DELTA>();             \
    factory.register_function<FunctionThreeArg##CLASS##DELTA>();           \
    factory.register_function<FunctionDateV2OneArg##CLASS##DELTA>();       \
    factory.register_function<FunctionDateV2TwoArg##CLASS##DELTA>();       \
    factory.register_function<FunctionDateV2ThreeArg##CLASS##DELTA>();     \
    factory.register_function<FunctionDateTimeV2OneArg##CLASS##DELTA>();   \
    factory.register_function<FunctionDateTimeV2TwoArg##CLASS##DELTA>();   \
    factory.register_function<FunctionDateTimeV2ThreeArg##CLASS##DELTA>(); \
    factory.register_function<FunctionDateTimeV2TwoArg##CLASS>();          \
    factory.register_function<FunctionDateTimeTwoArg##CLASS>();            \
    factory.register_function<FunctionDateV2TwoArg##CLASS>();

#define REGISTER_FUNC(CLASS)                    \
    REGISTER_FUNC_WITH_DELTA_TYPE(CLASS, Int32) \
    REGISTER_FUNC_WITH_DELTA_TYPE(CLASS, Int64)

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
} // namespace doris::vectorized
