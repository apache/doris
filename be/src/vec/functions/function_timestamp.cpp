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
#include <limits.h>
#include <parquet/column_writer.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "util/binary_cast.hpp"
#include "util/datetype_cast.hpp"
#include "util/time.h"
#include "util/time_lut.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <typename DateType>
struct StrToDate {
    static constexpr auto name = "str_to_date";

    static bool is_variadic() { return false; }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        if constexpr (IsDataTypeDateTimeV2<DateType>) {
            // max scale
            return make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
        }
        return make_nullable(std::make_shared<DateType>());
    }

    static StringRef rewrite_specific_format(const char* raw_str, size_t str_size) {
        const static std::string specific_format_strs[3] = {"yyyyMMdd", "yyyy-MM-dd",
                                                            "yyyy-MM-dd HH:mm:ss"};
        const static std::string specific_format_rewrite[3] = {"%Y%m%d", "%Y-%m-%d",
                                                               "%Y-%m-%d %H:%i:%s"};
        for (int i = 0; i < 3; i++) {
            const StringRef specific_format {specific_format_strs[i].data(),
                                             specific_format_strs[i].size()};
            if (specific_format == StringRef {raw_str, str_size}) {
                return {specific_format_rewrite[i].data(), specific_format_rewrite[i].size()};
            }
        }
        return {raw_str, str_size};
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t input_rows_count) {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        const auto& col0 = block.get_by_position(arguments[0]).column;
        bool col_const[2] = {is_column_const(*col0)};
        ColumnPtr argument_columns[2] = {
                col_const[0] ? static_cast<const ColumnConst&>(*col0).convert_to_full_column()
                             : col0};
        check_set_nullable(argument_columns[0], null_map, col_const[0]);
        //TODO: when we set default implementation for nullable, the check_set_nullable for arguments is useless. consider to remove it.

        std::tie(argument_columns[1], col_const[1]) =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        check_set_nullable(argument_columns[1], null_map, col_const[1]);

        auto specific_str_column = assert_cast<const ColumnString*>(argument_columns[0].get());
        auto specific_char_column = assert_cast<const ColumnString*>(argument_columns[1].get());

        auto& ldata = specific_str_column->get_chars();
        auto& loffsets = specific_str_column->get_offsets();

        auto& rdata = specific_char_column->get_chars();
        auto& roffsets = specific_char_column->get_offsets();

        // Because of we cant distinguish by return_type when we find function. so the return_type may NOT be same with real return type
        // which decided by FE. that's found by which.
        ColumnPtr res = nullptr;
        WhichDataType which(remove_nullable(block.get_by_position(result).type));
        if (which.is_date_time_v2()) {
            res = ColumnDateTimeV2::create();
            if (col_const[1]) {
                execute_impl_const_right<DataTypeDateTimeV2>(
                        context, ldata, loffsets, specific_char_column->get_data_at(0),
                        static_cast<ColumnDateTimeV2*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DataTypeDateTimeV2>(
                        context, ldata, loffsets, rdata, roffsets,
                        static_cast<ColumnDateTimeV2*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        } else if (which.is_date_v2()) {
            res = ColumnDateV2::create();
            if (col_const[1]) {
                execute_impl_const_right<DataTypeDateV2>(
                        context, ldata, loffsets, specific_char_column->get_data_at(0),
                        static_cast<ColumnDateV2*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DataTypeDateV2>(
                        context, ldata, loffsets, rdata, roffsets,
                        static_cast<ColumnDateV2*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        } else {
            res = ColumnDateTime::create();
            if (col_const[1]) {
                execute_impl_const_right<DataTypeDateTime>(
                        context, ldata, loffsets, specific_char_column->get_data_at(0),
                        static_cast<ColumnDateTime*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DataTypeDateTime>(
                        context, ldata, loffsets, rdata, roffsets,
                        static_cast<ColumnDateTime*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        }

        block.get_by_position(result).column = ColumnNullable::create(res, std::move(null_map));
        return Status::OK();
    }

private:
    template <typename ArgDateType,
              typename DateValueType = date_cast::TypeToValueTypeV<ArgDateType>,
              typename NativeType = date_cast::TypeToColumnV<ArgDateType>>
    static void execute_impl(FunctionContext* context, const ColumnString::Chars& ldata,
                             const ColumnString::Offsets& loffsets,
                             const ColumnString::Chars& rdata,
                             const ColumnString::Offsets& roffsets, PaddedPODArray<NativeType>& res,
                             NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            size_t l_str_size = loffsets[i] - loffsets[i - 1];

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            size_t r_str_size = roffsets[i] - roffsets[i - 1];
            const StringRef format_str = rewrite_specific_format(r_raw_str, r_str_size);
            _execute_inner_loop<DateValueType, NativeType>(l_raw_str, l_str_size, format_str.data,
                                                           format_str.size, context, res, null_map,
                                                           i);
        }
    }
    template <typename ArgDateType,
              typename DateValueType = date_cast::TypeToValueTypeV<ArgDateType>,
              typename NativeType = date_cast::TypeToColumnV<ArgDateType>>
    static void execute_impl_const_right(FunctionContext* context, const ColumnString::Chars& ldata,
                                         const ColumnString::Offsets& loffsets,
                                         const StringRef& rdata, PaddedPODArray<NativeType>& res,
                                         NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);
        const StringRef format_str = rewrite_specific_format(rdata.data, rdata.size);
        for (size_t i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            size_t l_str_size = loffsets[i] - loffsets[i - 1];

            _execute_inner_loop<DateValueType, NativeType>(l_raw_str, l_str_size, format_str.data,
                                                           format_str.size, context, res, null_map,
                                                           i);
        }
    }
    template <typename DateValueType, typename NativeType>
    static void _execute_inner_loop(const char* l_raw_str, size_t l_str_size, const char* r_raw_str,
                                    size_t r_str_size, FunctionContext* context,
                                    PaddedPODArray<NativeType>& res, NullMap& null_map,
                                    size_t index) {
        auto& ts_val = *reinterpret_cast<DateValueType*>(&res[index]);
        if (!ts_val.from_date_format_str(r_raw_str, r_str_size, l_raw_str, l_str_size)) {
            null_map[index] = 1;
        } else {
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                if (context->get_return_type().type == doris::PrimitiveType::TYPE_DATETIME) {
                    ts_val.to_datetime();
                } else {
                    ts_val.cast_to_date();
                }
            }
        }
    }
};

struct MakeDateImpl {
    static constexpr auto name = "makedate";

    static bool is_variadic() { return false; }

    static DataTypes get_variadic_argument_types() { return {}; }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return make_nullable(std::make_shared<DataTypeDateTime>());
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t input_rows_count) {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        DCHECK_EQ(arguments.size(), 2);

        const auto& col0 = block.get_by_position(arguments[0]).column;
        bool col_const[2] = {is_column_const(*col0)};
        ColumnPtr argument_columns[2] = {
                col_const[0] ? static_cast<const ColumnConst&>(*col0).convert_to_full_column()
                             : col0};
        check_set_nullable(argument_columns[0], null_map, col_const[0]);

        std::tie(argument_columns[1], col_const[1]) =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        check_set_nullable(argument_columns[1], null_map, col_const[1]);

        ColumnPtr res = nullptr;
        WhichDataType which(remove_nullable(block.get_by_position(result).type));
        if (which.is_date_v2()) {
            res = ColumnDateV2::create();
            if (col_const[1]) {
                execute_impl_right_const<DataTypeDateV2>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_element(0),
                        static_cast<ColumnDateV2*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DataTypeDateV2>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_data(),
                        static_cast<ColumnDateV2*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        } else if (which.is_date_time_v2()) {
            res = ColumnDateTimeV2::create();
            if (col_const[1]) {
                execute_impl_right_const<DataTypeDateTimeV2>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_element(0),
                        static_cast<ColumnDateTimeV2*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DataTypeDateTimeV2>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_data(),
                        static_cast<ColumnDateTimeV2*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        } else {
            res = ColumnDateTime::create();
            if (col_const[1]) {
                execute_impl_right_const<DataTypeDateTime>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_element(0),
                        static_cast<ColumnDateTime*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DataTypeDateTime>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_data(),
                        static_cast<ColumnDateTime*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        }

        block.get_by_position(result).column = ColumnNullable::create(res, std::move(null_map));
        return Status::OK();
    }

private:
    template <typename DateType, typename DateValueType = date_cast::TypeToValueTypeV<DateType>,
              typename ReturnType = date_cast::TypeToColumnV<DateType>>
    static void execute_impl(const PaddedPODArray<Int32>& ldata, const PaddedPODArray<Int32>& rdata,
                             PaddedPODArray<ReturnType>& res, NullMap& null_map) {
        auto len = ldata.size();
        res.resize(len);

        for (size_t i = 0; i < len; ++i) {
            const auto& l = ldata[i];
            const auto& r = rdata[i];
            if (r <= 0 || l < 0 || l > 9999) {
                null_map[i] = 1;
                continue;
            }
            _execute_inner_loop<DateValueType, ReturnType>(l, r, res, null_map, i);
        }
    }
    template <typename DateType, typename DateValueType = date_cast::TypeToValueTypeV<DateType>,
              typename ReturnType = date_cast::TypeToColumnV<DateType>>
    static void execute_impl_right_const(const PaddedPODArray<Int32>& ldata, Int32 rdata,
                                         PaddedPODArray<ReturnType>& res, NullMap& null_map) {
        auto len = ldata.size();
        res.resize(len);

        const auto& r = rdata;
        for (size_t i = 0; i < len; ++i) {
            const auto& l = ldata[i];
            if (r <= 0 || l < 0 || l > 9999) {
                null_map[i] = 1;
                continue;
            }
            _execute_inner_loop<DateValueType, ReturnType>(l, r, res, null_map, i);
        }
    }
    template <typename DateValueType, typename ReturnType>
    static void _execute_inner_loop(const int& l, const int& r, PaddedPODArray<ReturnType>& res,
                                    NullMap& null_map, size_t index) {
        auto& res_val = *reinterpret_cast<DateValueType*>(&res[index]);
        // l checked outside
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            VecDateTimeValue ts_value = VecDateTimeValue();
            ts_value.unchecked_set_time(l, 1, 1, 0, 0, 0);

            TimeInterval interval(DAY, r - 1, false);
            res_val = ts_value;
            if (!res_val.template date_add_interval<DAY>(interval)) {
                null_map[index] = 1;
                return;
            }
            res_val.cast_to_date();
        } else {
            res_val.unchecked_set_time(l, 1, 1, 0, 0, 0, 0);
            TimeInterval interval(DAY, r - 1, false);
            if (!res_val.template date_add_interval<DAY>(interval)) {
                null_map[index] = 1;
            }
        }
    }
};

struct DateTruncState {
    using Callback_function =
            std::function<void(const ColumnPtr&, ColumnPtr& res, NullMap& null_map, size_t)>;
    Callback_function callback_function;
};

template <typename DateType>
struct DateTrunc {
    static constexpr auto name = "date_trunc";

    using ColumnType = date_cast::TypeToColumnV<DateType>;
    using DateValueType = date_cast::TypeToValueTypeV<DateType>;
    using ArgType = date_cast::ValueTypeOfColumnV<ColumnType>;

    static bool is_variadic() { return true; }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DateType>(), std::make_shared<DataTypeString>()};
    }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return make_nullable(std::make_shared<DateType>());
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        if (!context->is_col_constant(1)) {
            return Status::InvalidArgument(
                    "date_trunc function of time unit argument must be constant.");
        }
        const auto& data_str = context->get_constant_col(1)->column_ptr->get_data_at(0);
        std::string lower_str(data_str.data, data_str.size);
        std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                       [](unsigned char c) { return std::tolower(c); });

        std::shared_ptr<DateTruncState> state = std::make_shared<DateTruncState>();
        if (std::strncmp("year", lower_str.data(), 4) == 0) {
            state->callback_function = &execute_impl_right_const<TimeUnit::YEAR>;
        } else if (std::strncmp("quarter", lower_str.data(), 7) == 0) {
            state->callback_function = &execute_impl_right_const<TimeUnit::QUARTER>;
        } else if (std::strncmp("month", lower_str.data(), 5) == 0) {
            state->callback_function = &execute_impl_right_const<TimeUnit::MONTH>;
        } else if (std::strncmp("week", lower_str.data(), 4) == 0) {
            state->callback_function = &execute_impl_right_const<TimeUnit::WEEK>;
        } else if (std::strncmp("day", lower_str.data(), 3) == 0) {
            state->callback_function = &execute_impl_right_const<TimeUnit::DAY>;
        } else if (std::strncmp("hour", lower_str.data(), 4) == 0) {
            state->callback_function = &execute_impl_right_const<TimeUnit::HOUR>;
        } else if (std::strncmp("minute", lower_str.data(), 6) == 0) {
            state->callback_function = &execute_impl_right_const<TimeUnit::MINUTE>;
        } else if (std::strncmp("second", lower_str.data(), 6) == 0) {
            state->callback_function = &execute_impl_right_const<TimeUnit::SECOND>;
        } else {
            return Status::RuntimeError(
                    "Illegal second argument column of function date_trunc. now only support "
                    "[second,minute,hour,day,week,month,quarter,year]");
        }
        context->set_function_state(scope, state);
        return Status::OK();
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 2);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        const auto& datetime_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        ColumnPtr res = ColumnType::create(input_rows_count);
        auto* state = reinterpret_cast<DateTruncState*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(state != nullptr);
        state->callback_function(datetime_column, res, null_map->get_data(), input_rows_count);
        block.get_by_position(result).column = ColumnNullable::create(res, std::move(null_map));
        return Status::OK();
    }

private:
    template <TimeUnit Unit>
    static void execute_impl_right_const(const ColumnPtr& datetime_column, ColumnPtr& result_column,
                                         NullMap& null_map, size_t input_rows_count) {
        auto& data = static_cast<const ColumnType*>(datetime_column.get())->get_data();
        auto& res = static_cast<ColumnType*>(result_column->assume_mutable().get())->get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto dt = binary_cast<ArgType, DateValueType>(data[i]);
            null_map[i] = !dt.template datetime_trunc<Unit>();
            res[i] = binary_cast<DateValueType, ArgType>(dt);
        }
    }
};

class FromDays : public IFunction {
public:
    static constexpr auto name = "from_days";

    static FunctionPtr create() { return std::make_shared<FromDays>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeDate>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        ColumnPtr& argument_column = block.get_by_position(arguments[0]).column;
        auto data_col = assert_cast<const ColumnVector<Int32>*>(argument_column.get());

        ColumnPtr res_column;
        WhichDataType which(remove_nullable(block.get_by_position(result).type));
        if (which.is_date()) {
            res_column = ColumnInt64::create(input_rows_count);
            execute_straight<VecDateTimeValue, Int64>(
                    input_rows_count, null_map->get_data(), data_col->get_data(),
                    static_cast<ColumnDateTime*>(res_column->assume_mutable().get())->get_data());
        } else {
            res_column = ColumnDateV2::create(input_rows_count);
            execute_straight<DateV2Value<DateV2ValueType>, UInt32>(
                    input_rows_count, null_map->get_data(), data_col->get_data(),
                    static_cast<ColumnDateV2*>(res_column->assume_mutable().get())->get_data());
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(res_column), std::move(null_map)));
        return Status::OK();
    }

private:
    template <typename DateValueType, typename ReturnType>
    void execute_straight(size_t input_rows_count, NullMap& null_map,
                          const PaddedPODArray<Int32>& data_col,
                          PaddedPODArray<ReturnType>& res_data) const {
        for (int i = 0; i < input_rows_count; i++) {
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                const auto& cur_data = data_col[i];
                auto& ts_value = *reinterpret_cast<DateValueType*>(&res_data[i]);
                if (!ts_value.from_date_daynr(cur_data)) {
                    null_map[i] = 1;
                    continue;
                }
            } else {
                const auto& cur_data = data_col[i];
                auto& ts_value = *reinterpret_cast<DateValueType*>(&res_data[i]);
                if (!ts_value.get_date_from_daynr(cur_data)) {
                    null_map[i] = 1;
                }
            }
        }
    }
};

struct UnixTimeStampImpl {
    static Int32 trim_timestamp(Int64 timestamp) {
        if (timestamp < 0 || timestamp > INT_MAX) {
            timestamp = 0;
        }
        return (Int32)timestamp;
    }

    static std::pair<Int32, Int32> trim_timestamp(std::pair<Int64, Int64> timestamp) {
        if (timestamp.first < 0 || timestamp.first > INT_MAX) {
            return {0, 0};
        }
        return std::make_pair((Int32)timestamp.first, (Int32)timestamp.second);
    }

    static DataTypes get_variadic_argument_types() { return {}; }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return std::make_shared<DataTypeInt32>();
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        auto col_result = ColumnVector<Int32>::create();
        col_result->resize(1);
        col_result->get_data()[0] = context->state()->timestamp_ms() / 1000;
        auto col_const = ColumnConst::create(std::move(col_result), input_rows_count);
        block.replace_by_position(result, std::move(col_const));
        return Status::OK();
    }
};

template <typename DateType>
struct UnixTimeStampDateImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DateType>()}; }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        if constexpr (std::is_same_v<DateType, DataTypeDateTimeV2>) {
            if (arguments[0].type->is_nullable()) {
                UInt32 scale = static_cast<const DataTypeNullable*>(arguments[0].type.get())
                                       ->get_nested_type()
                                       ->get_scale();
                return make_nullable(
                        std::make_shared<DataTypeDecimal<Decimal64>>(10 + scale, scale));
            }
            UInt32 scale = arguments[0].type->get_scale();
            return std::make_shared<DataTypeDecimal<Decimal64>>(10 + scale, scale);
        } else {
            if (arguments[0].type->is_nullable()) {
                return make_nullable(std::make_shared<DataTypeInt32>());
            }
            return std::make_shared<DataTypeInt32>();
        }
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const ColumnPtr& col = block.get_by_position(arguments[0]).column;
        DCHECK(!col->is_nullable());

        if constexpr (std::is_same_v<DateType, DataTypeDate> ||
                      std::is_same_v<DateType, DataTypeDateTime>) {
            const auto* col_source = assert_cast<const ColumnDate*>(col.get());
            auto col_result = ColumnVector<Int32>::create();
            auto& col_result_data = col_result->get_data();
            col_result->resize(input_rows_count);

            for (int i = 0; i < input_rows_count; i++) {
                StringRef source = col_source->get_data_at(i);
                const auto& ts_value = reinterpret_cast<const VecDateTimeValue&>(*source.data);
                int64_t timestamp {};
                ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj());
                col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
            }
            block.replace_by_position(result, std::move(col_result));
        } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
            const auto* col_source = assert_cast<const ColumnDateV2*>(col.get());
            auto col_result = ColumnVector<Int32>::create();
            auto& col_result_data = col_result->get_data();
            col_result->resize(input_rows_count);

            for (int i = 0; i < input_rows_count; i++) {
                StringRef source = col_source->get_data_at(i);
                const auto& ts_value =
                        reinterpret_cast<const DateV2Value<DateV2ValueType>&>(*source.data);
                int64_t timestamp {};
                const auto valid =
                        ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj());
                DCHECK(valid);
                col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
            }
            block.replace_by_position(result, std::move(col_result));
        } else { // DatetimeV2
            const auto* col_source = assert_cast<const ColumnDateTimeV2*>(col.get());
            UInt32 scale = block.get_by_position(arguments[0]).type->get_scale();
            auto col_result = ColumnDecimal<Decimal64>::create(input_rows_count, scale);
            auto& col_result_data = col_result->get_data();
            col_result->resize(input_rows_count);

            for (int i = 0; i < input_rows_count; i++) {
                StringRef source = col_source->get_data_at(i);
                const auto& ts_value =
                        reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(*source.data);
                std::pair<int64_t, int64_t> timestamp {};
                const auto valid =
                        ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj());
                DCHECK(valid);

                auto& [sec, ms] = timestamp;
                sec = UnixTimeStampImpl::trim_timestamp(sec);
                auto ms_str = std::to_string(ms).substr(0, scale);
                if (ms_str.empty()) {
                    ms_str = "0";
                }
                col_result_data[i] = Decimal64::from_int_frac(sec, std::stoll(ms_str), scale).value;
            }
            block.replace_by_position(result, std::move(col_result));
        }

        return Status::OK();
    }
};

template <typename DateType>
struct UnixTimeStampDatetimeImpl : public UnixTimeStampDateImpl<DateType> {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DateType>()}; }
};

// This impl doesn't use default impl to deal null value.
struct UnixTimeStampStrImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return make_nullable(std::make_shared<DataTypeDecimal<Decimal64>>(16, 6));
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        ColumnPtr col_left = nullptr, col_right = nullptr;
        bool source_const = false, format_const = false;
        std::tie(col_left, source_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        std::tie(col_right, format_const) =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        auto col_result = ColumnDecimal<Decimal64>::create(input_rows_count, 6);
        auto null_map = ColumnVector<UInt8>::create(input_rows_count);
        auto& col_result_data = col_result->get_data();
        auto& null_map_data = null_map->get_data();

        check_set_nullable(col_left, null_map, source_const);
        check_set_nullable(col_right, null_map, format_const);

        const auto* col_source = assert_cast<const ColumnString*>(col_left.get());
        const auto* col_format = assert_cast<const ColumnString*>(col_right.get());
        for (int i = 0; i < input_rows_count; i++) {
            StringRef source = col_source->get_data_at(index_check_const(i, source_const));
            StringRef fmt = col_format->get_data_at(index_check_const(i, format_const));

            DateV2Value<DateTimeV2ValueType> ts_value;
            if (!ts_value.from_date_format_str(fmt.data, fmt.size, source.data, source.size)) {
                null_map_data[i] = true;
                continue;
            }

            std::pair<int64_t, int64_t> timestamp {};
            if (!ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
                null_map_data[i] = true; // impossible now
            } else {
                null_map_data[i] = false;

                auto [sec, ms] = UnixTimeStampImpl::trim_timestamp(timestamp);
                // trailing ms
                auto ms_str = std::to_string(ms).substr(0, 6);
                if (ms_str.empty()) {
                    ms_str = "0";
                }

                col_result_data[i] = Decimal64::from_int_frac(sec, std::stoll(ms_str), 6).value;
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(col_result), std::move(null_map)));

        return Status::OK();
    }
};

template <typename Impl>
class FunctionUnixTimestamp : public IFunction {
public:
    static constexpr auto name = "unix_timestamp";
    static FunctionPtr create() { return std::make_shared<FunctionUnixTimestamp<Impl>>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    bool use_default_implementation_for_nulls() const override {
        return !static_cast<bool>(std::is_same_v<Impl, UnixTimeStampStrImpl>);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct MicroSec {
    static constexpr auto name = "microsecond_timestamp";
    static constexpr Int64 ratio = 1000000;
};
struct MilliSec {
    static constexpr auto name = "millisecond_timestamp";
    static constexpr Int64 ratio = 1000;
};
struct Sec {
    static constexpr auto name = "second_timestamp";
    static constexpr Int64 ratio = 1;
};
template <typename Impl>
class DateTimeToTimestamp : public IFunction {
public:
    using ReturnType = Int64;
    static constexpr Int64 ratio_to_micro = (1000 * 1000) / Impl::ratio;
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<DateTimeToTimestamp<Impl>>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        if (arguments[0].type->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeInt64>());
        }
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnUInt64&>(*arg_col);
        auto res_col = ColumnInt64::create();
        auto& res_data = res_col->get_data();
        res_col->get_data().resize_fill(input_rows_count, 0);
        for (int i = 0; i < input_rows_count; i++) {
            StringRef source = column_data.get_data_at(i);
            const auto& dt =
                    reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(*source.data);
            const cctz::time_zone& time_zone = context->state()->timezone_obj();
            int64_t timestamp {0};
            auto ret = dt.unix_timestamp(&timestamp, time_zone);
            // ret must be true
            DCHECK(ret);
            auto microsecond = dt.microsecond();
            timestamp = timestamp * Impl::ratio + microsecond / ratio_to_micro;
            res_data[i] = timestamp;
        }
        block.replace_by_position(result, std::move(res_col));

        return Status::OK();
    }
};

template <template <typename> class Impl, typename DateType>
class FunctionDateOrDateTimeToDate : public IFunction {
public:
    static constexpr auto name = Impl<DateType>::name;
    static FunctionPtr create() {
        return std::make_shared<FunctionDateOrDateTimeToDate<Impl, DateType>>();
    }

    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_variadic() const override { return true; }

    // input DateTime and Date, return Date
    // input DateTimeV2 and DateV2, return DateV2
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        bool is_nullable = false;
        for (auto it : arguments) {
            is_nullable = is_nullable || it.type->is_nullable();
        }

        if constexpr (date_cast::IsV1<DateType>()) {
            return make_nullable(std::make_shared<DataTypeDate>());
        } else {
            return is_nullable ? make_nullable(std::make_shared<DataTypeDateV2>())
                               : std::make_shared<DataTypeDateV2>();
        }
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DateType>()};
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return Impl<DateType>::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

template <typename DateType>
struct LastDayImpl {
    static constexpr auto name = "last_day";

    using DateValueType = date_cast::TypeToValueTypeV<DateType>;
    using ColumnType = date_cast::TypeToColumnV<DateType>;
    using NativeType = date_cast::ValueTypeOfColumnV<ColumnType>;
    using ResultType =
            std::conditional_t<date_cast::IsV1<DateType>(), DataTypeDate, DataTypeDateV2>;
    using ResultColumnType = date_cast::TypeToColumnV<ResultType>;
    using ResultNativeType = date_cast::ValueTypeOfColumnV<ResultColumnType>;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const auto is_nullable = block.get_by_position(result).type->is_nullable();
        ColumnPtr res_column;
        ColumnPtr argument_column = remove_nullable(block.get_by_position(arguments[0]).column);
        if (is_nullable) {
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            auto data_col = assert_cast<const ColumnType*>(argument_column.get());
            res_column = ResultColumnType::create(input_rows_count);
            execute_straight(
                    input_rows_count, null_map->get_data(), data_col->get_data(),
                    static_cast<ResultColumnType*>(res_column->assume_mutable().get())->get_data());

            if (const auto* nullable_col = check_and_get_column<ColumnNullable>(
                        block.get_by_position(arguments[0]).column.get())) {
                NullMap& result_null_map = assert_cast<ColumnUInt8&>(*null_map).get_data();
                const NullMap& src_null_map =
                        assert_cast<const ColumnUInt8&>(nullable_col->get_null_map_column())
                                .get_data();
                VectorizedUtils::update_null_map(result_null_map, src_null_map);
            }
            block.replace_by_position(result,
                                      ColumnNullable::create(res_column, std::move(null_map)));
        } else {
            if constexpr (date_cast::IsV2<DateType>()) {
                auto data_col = assert_cast<const ColumnType*>(argument_column.get());
                res_column = ResultColumnType::create(input_rows_count);
                execute_straight(input_rows_count, data_col->get_data(),
                                 static_cast<ResultColumnType*>(res_column->assume_mutable().get())
                                         ->get_data());
                block.replace_by_position(result, std::move(res_column));
            }
        }
        return Status::OK();
    }

    static void execute_straight(size_t input_rows_count, NullMap& null_map,
                                 const PaddedPODArray<NativeType>& data_col,
                                 PaddedPODArray<ResultNativeType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            const auto& cur_data = data_col[i];
            auto ts_value = binary_cast<NativeType, DateValueType>(cur_data);
            if (!ts_value.is_valid_date()) {
                null_map[i] = 1;
                continue;
            }
            int day = get_last_month_day(ts_value.year(), ts_value.month());
            // day is definitely legal
            if constexpr (date_cast::IsV1<DateType>()) {
                ts_value.unchecked_set_time(ts_value.year(), ts_value.month(), day, 0, 0, 0);
                ts_value.set_type(TIME_DATE);
                res_data[i] = binary_cast<VecDateTimeValue, Int64>(ts_value);
            } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                ts_value.template unchecked_set_time_unit<TimeUnit::DAY>(day);
                res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);
            } else {
                ts_value.template unchecked_set_time_unit<TimeUnit::DAY>(day);
                ts_value.unchecked_set_time(ts_value.year(), ts_value.month(), day, 0, 0, 0, 0);
                UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
            }
        }
    }

    static void execute_straight(size_t input_rows_count,
                                 const PaddedPODArray<NativeType>& data_col,
                                 PaddedPODArray<ResultNativeType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            const auto& cur_data = data_col[i];
            auto ts_value = binary_cast<NativeType, DateValueType>(cur_data);
            DCHECK(ts_value.is_valid_date());
            int day = get_last_month_day(ts_value.year(), ts_value.month());
            ts_value.template unchecked_set_time_unit<TimeUnit::DAY>(day);

            if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);
            } else if constexpr (std::is_same_v<DateType, DataTypeDateTimeV2>) {
                ts_value.unchecked_set_time(ts_value.year(), ts_value.month(), day, 0, 0, 0, 0);
                UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
            }
        }
    }

    static int get_last_month_day(int year, int month) {
        bool is_leap_year = doris::is_leap(year);
        if (month == 2) {
            return is_leap_year ? 29 : 28;
        } else {
            if (month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 ||
                month == 12) {
                return 31;
            } else {
                return 30;
            }
        }
    }
};

template <typename DateType>
struct MondayImpl {
    static constexpr auto name = "to_monday";

    using DateValueType = date_cast::TypeToValueTypeV<DateType>;
    using ColumnType = date_cast::TypeToColumnV<DateType>;
    using NativeType = date_cast::ValueTypeOfColumnV<ColumnType>;
    using ResultType =
            std::conditional_t<date_cast::IsV1<DateType>(), DataTypeDate, DataTypeDateV2>;
    using ResultColumnType = date_cast::TypeToColumnV<ResultType>;
    using ResultNativeType = date_cast::ValueTypeOfColumnV<ResultColumnType>;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const auto is_nullable = block.get_by_position(result).type->is_nullable();
        ColumnPtr argument_column = remove_nullable(block.get_by_position(arguments[0]).column);
        ColumnPtr res_column;
        if (is_nullable) {
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            auto data_col = assert_cast<const ColumnType*>(argument_column.get());
            res_column = ResultColumnType::create(input_rows_count);
            execute_straight(
                    input_rows_count, null_map->get_data(), data_col->get_data(),
                    static_cast<ResultColumnType*>(res_column->assume_mutable().get())->get_data());

            if (const auto* nullable_col = check_and_get_column<ColumnNullable>(
                        block.get_by_position(arguments[0]).column.get())) {
                NullMap& result_null_map = assert_cast<ColumnUInt8&>(*null_map).get_data();
                const NullMap& src_null_map =
                        assert_cast<const ColumnUInt8&>(nullable_col->get_null_map_column())
                                .get_data();
                VectorizedUtils::update_null_map(result_null_map, src_null_map);
            }
            block.replace_by_position(result,
                                      ColumnNullable::create(res_column, std::move(null_map)));
        } else {
            if constexpr (date_cast::IsV2<DateType>()) {
                auto data_col = assert_cast<const ColumnType*>(argument_column.get());
                res_column = ResultColumnType::create(input_rows_count);
                execute_straight(input_rows_count, data_col->get_data(),
                                 static_cast<ResultColumnType*>(res_column->assume_mutable().get())
                                         ->get_data());
                block.replace_by_position(result, std::move(res_column));
            }
        }
        return Status::OK();
    }

    // v1, maybe makes null value
    static void execute_straight(size_t input_rows_count, NullMap& null_map,
                                 const PaddedPODArray<NativeType>& data_col,
                                 PaddedPODArray<ResultNativeType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            const auto& cur_data = data_col[i];
            auto ts_value = binary_cast<NativeType, DateValueType>(cur_data);
            if (!ts_value.is_valid_date()) [[unlikely]] {
                null_map[i] = 1;
                continue;
            }
            if constexpr (date_cast::IsV1<DateType>()) {
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.unchecked_set_time(ts_value.year(), ts_value.month(), 1, 0, 0, 0);
                    ts_value.set_type(TIME_DATE);
                    res_data[i] = binary_cast<VecDateTimeValue, Int64>(ts_value);
                    continue;
                }

                // day_of_week, from 1(Mon) to 7(Sun)
                int day_of_week = ts_value.weekday() + 1;
                int gap_of_monday = day_of_week - 1;
                TimeInterval interval(DAY, gap_of_monday, true);
                ts_value.template date_add_interval<DAY>(interval);
                ts_value.set_type(TIME_DATE);
                res_data[i] = binary_cast<VecDateTimeValue, Int64>(ts_value);

            } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.template unchecked_set_time_unit<TimeUnit::DAY>(1);
                    res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);
                    continue;
                }

                // day_of_week, from 1(Mon) to 7(Sun)
                int day_of_week = ts_value.weekday() + 1;
                int gap_of_monday = day_of_week - 1;
                TimeInterval interval(DAY, gap_of_monday, true);
                ts_value.template date_add_interval<DAY>(interval);
                res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);
            } else {
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.unchecked_set_time(ts_value.year(), ts_value.month(), 1, 0, 0, 0, 0);
                    UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                    DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
                    continue;
                }
                // day_of_week, from 1(Mon) to 7(Sun)
                int day_of_week = ts_value.weekday() + 1;
                int gap_of_monday = day_of_week - 1;
                TimeInterval interval(DAY, gap_of_monday, true);
                ts_value.template date_add_interval<DAY>(interval);
                ts_value.unchecked_set_time(ts_value.year(), ts_value.month(), ts_value.day(), 0, 0,
                                            0, 0);
                UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
            }
        }
    }

    // v2, won't make null value
    static void execute_straight(size_t input_rows_count,
                                 const PaddedPODArray<NativeType>& data_col,
                                 PaddedPODArray<ResultNativeType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            const auto& cur_data = data_col[i];
            auto ts_value = binary_cast<NativeType, DateValueType>(cur_data);
            DCHECK(ts_value.is_valid_date());
            if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.template unchecked_set_time_unit<TimeUnit::DAY>(1);
                    res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);
                    continue;
                }
                // day_of_week, from 1(Mon) to 7(Sun)
                int day_of_week = ts_value.weekday() + 1;
                int gap_of_monday = day_of_week - 1;
                TimeInterval interval(DAY, gap_of_monday, true);
                ts_value.template date_add_interval<DAY>(interval);
                res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);
            } else if constexpr (std::is_same_v<DateType, DataTypeDateTimeV2>) {
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.unchecked_set_time(ts_value.year(), ts_value.month(), 1, 0, 0, 0, 0);
                    UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                    DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
                    continue;
                }
                // day_of_week, from 1(Mon) to 7(Sun)
                int day_of_week = ts_value.weekday() + 1;
                int gap_of_monday = day_of_week - 1;
                TimeInterval interval(DAY, gap_of_monday, true);
                ts_value.template date_add_interval<DAY>(interval);
                ts_value.unchecked_set_time(ts_value.year(), ts_value.month(), ts_value.day(), 0, 0,
                                            0, 0);
                UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
            }
        }
    }

    // specially, 1970-01-01, 1970-01-02, 1970-01-03 and 1970-01-04 return 1970-01-01
    static bool is_special_day(int year, int month, int day) {
        return year == 1970 && month == 1 && day > 0 && day < 5;
    }
};

template <typename Impl>
class FunctionOtherTypesToDateType : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionOtherTypesToDateType>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool is_variadic() const override { return Impl::is_variadic(); }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if constexpr (std::is_same_v<Impl, DateTrunc<DataTypeDate>> ||
                      std::is_same_v<Impl, DateTrunc<DataTypeDateV2>> ||
                      std::is_same_v<Impl, DateTrunc<DataTypeDateTime>> ||
                      std::is_same_v<Impl, DateTrunc<DataTypeDateTimeV2>>) {
            return Impl::open(context, scope);
        } else {
            return Status::OK();
        }
    }

    //TODO: add function below when we fixed be-ut.
    //ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return Impl::execute(context, block, arguments, result, input_rows_count);
    }
};

using FunctionStrToDate = FunctionOtherTypesToDateType<StrToDate<DataTypeDate>>;
using FunctionStrToDatetime = FunctionOtherTypesToDateType<StrToDate<DataTypeDateTime>>;
using FunctionStrToDateV2 = FunctionOtherTypesToDateType<StrToDate<DataTypeDateV2>>;
using FunctionStrToDatetimeV2 = FunctionOtherTypesToDateType<StrToDate<DataTypeDateTimeV2>>;
using FunctionMakeDate = FunctionOtherTypesToDateType<MakeDateImpl>;
using FunctionDateTruncDate = FunctionOtherTypesToDateType<DateTrunc<DataTypeDate>>;
using FunctionDateTruncDateV2 = FunctionOtherTypesToDateType<DateTrunc<DataTypeDateV2>>;
using FunctionDateTruncDatetime = FunctionOtherTypesToDateType<DateTrunc<DataTypeDateTime>>;
using FunctionDateTruncDatetimeV2 = FunctionOtherTypesToDateType<DateTrunc<DataTypeDateTimeV2>>;

void register_function_timestamp(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStrToDate>();
    factory.register_function<FunctionStrToDatetime>();
    factory.register_function<FunctionStrToDateV2>();
    factory.register_function<FunctionStrToDatetimeV2>();
    factory.register_function<FunctionMakeDate>();
    factory.register_function<FromDays>();
    factory.register_function<FunctionDateTruncDate>();
    factory.register_function<FunctionDateTruncDateV2>();
    factory.register_function<FunctionDateTruncDatetime>();
    factory.register_function<FunctionDateTruncDatetimeV2>();

    factory.register_function<FunctionUnixTimestamp<UnixTimeStampImpl>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDate>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateV2>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateTime>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateTimeV2>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampStrImpl>>();
    factory.register_function<FunctionDateOrDateTimeToDate<LastDayImpl, DataTypeDateTime>>();
    factory.register_function<FunctionDateOrDateTimeToDate<LastDayImpl, DataTypeDate>>();
    factory.register_function<FunctionDateOrDateTimeToDate<LastDayImpl, DataTypeDateV2>>();
    factory.register_function<FunctionDateOrDateTimeToDate<LastDayImpl, DataTypeDateTimeV2>>();
    factory.register_function<FunctionDateOrDateTimeToDate<MondayImpl, DataTypeDateV2>>();
    factory.register_function<FunctionDateOrDateTimeToDate<MondayImpl, DataTypeDateTimeV2>>();
    factory.register_function<FunctionDateOrDateTimeToDate<MondayImpl, DataTypeDate>>();
    factory.register_function<FunctionDateOrDateTimeToDate<MondayImpl, DataTypeDateTime>>();

    factory.register_function<DateTimeToTimestamp<MicroSec>>();
    factory.register_function<DateTimeToTimestamp<MilliSec>>();
    factory.register_function<DateTimeToTimestamp<Sec>>();
}

} // namespace doris::vectorized
