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

#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

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

    bool use_default_implementation_for_constants() const override { return false; }

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
        vector_constant_delta<NativeType, Int32>(dates, Int32(1), res, null_map);
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

template <typename Impl>
struct TimeRound {
    static constexpr auto name = Impl::name;
    static constexpr uint64_t FIRST_DAY = 19700101000000;
    static constexpr uint64_t FIRST_SUNDAY = 19700104000000;
    static constexpr int8_t FLOOR = 0;
    static constexpr int8_t CEIL = 1;

    static constexpr uint32_t MASK_YEAR_FOR_DATEV2 = -1 >> 23;
    static constexpr uint32_t MASK_YEAR_MONTH_FOR_DATEV2 = -1 >> 27;

    static constexpr uint64_t MASK_YEAR_FOR_DATETIMEV2 = -1 >> 18;
    static constexpr uint64_t MASK_YEAR_MONTH_FOR_DATETIMEV2 = -1 >> 22;

    template <typename NativeType, typename DateValueType>
    static void time_round(const DateValueType& ts2, Int32 period, DateValueType& ts1,
                           UInt8& is_null) {
        if (period < 1) {
            is_null = true;
            return;
        }

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
        int64_t delta_inside_period = (diff % count + count) % count;
        int64_t step = diff - delta_inside_period +
                       (Impl::Type == FLOOR        ? 0
                        : delta_inside_period == 0 ? 0
                                                   : count);
        bool is_neg = step < 0;
        TimeInterval interval(Impl::Unit, is_neg ? -step : step, is_neg);
        is_null = !ts1.template date_add_interval<Impl::Unit>(interval);
        return;
    }

    template <typename NativeType, typename DateValueType>
    static void time_round(NativeType date, Int32 period, NativeType origin_date, NativeType& res,
                           UInt8& is_null) {
        res = origin_date;
        auto ts2 = binary_cast<NativeType, DateValueType>(date);
        auto& ts1 = (DateValueType&)(res);

        TimeRound<Impl>::template time_round<NativeType, DateValueType>(ts2, period, ts1, is_null);
    }

    template <typename NativeType, typename DateValueType>
    static void time_round(NativeType date, Int32 period, NativeType& res, UInt8& is_null) {
        auto ts2 = binary_cast<NativeType, DateValueType>(date);
        auto& ts1 = (DateValueType&)(res);
        if constexpr (Impl::Unit != WEEK) {
            ts1.from_olap_datetime(FIRST_DAY);
        } else {
            // Only week use the FIRST SUNDAY
            ts1.from_olap_datetime(FIRST_SUNDAY);
        }

        TimeRound<Impl>::template time_round<NativeType, DateValueType>(ts2, period, ts1, is_null);
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
