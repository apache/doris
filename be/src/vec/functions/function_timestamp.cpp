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
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstring>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "util/binary_cast.hpp"
#include "util/datetype_cast.hpp"
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
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

struct StrToDate {
    static constexpr auto name = "str_to_date";

    static bool is_variadic() { return false; }

    static DataTypes get_variadic_argument_types() { return {}; }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        //TODO: it doesn't matter now. maybe sometime we should find the function signature with return_type together
        return make_nullable(std::make_shared<DataTypeDateTime>());
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
            res = ColumnVector<UInt64>::create();
            if (col_const[1]) {
                execute_impl_const_right<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        context, ldata, loffsets, specific_char_column->get_data_at(0),
                        static_cast<ColumnVector<UInt64>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        context, ldata, loffsets, rdata, roffsets,
                        static_cast<ColumnVector<UInt64>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        } else if (which.is_date_v2()) {
            res = ColumnVector<UInt32>::create();
            if (col_const[1]) {
                execute_impl_const_right<DateV2Value<DateV2ValueType>, UInt32>(
                        context, ldata, loffsets, specific_char_column->get_data_at(0),
                        static_cast<ColumnVector<UInt32>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DateV2Value<DateV2ValueType>, UInt32>(
                        context, ldata, loffsets, rdata, roffsets,
                        static_cast<ColumnVector<UInt32>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        } else {
            res = ColumnVector<Int64>::create();
            if (col_const[1]) {
                execute_impl_const_right<VecDateTimeValue, Int64>(
                        context, ldata, loffsets, specific_char_column->get_data_at(0),
                        static_cast<ColumnVector<Int64>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<VecDateTimeValue, Int64>(
                        context, ldata, loffsets, rdata, roffsets,
                        static_cast<ColumnVector<Int64>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        }

        block.get_by_position(result).column = ColumnNullable::create(res, std::move(null_map));
        return Status::OK();
    }

private:
    template <typename DateValueType, typename NativeType>
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
    template <typename DateValueType, typename NativeType>
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
        }
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            if (context->get_return_type().type == doris::PrimitiveType::TYPE_DATETIME) {
                ts_val.to_datetime();
            } else {
                ts_val.cast_to_date();
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
            res = ColumnVector<UInt32>::create();
            if (col_const[1]) {
                execute_impl_right_const<DateV2Value<DateV2ValueType>, UInt32>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_element(0),
                        static_cast<ColumnVector<UInt32>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DateV2Value<DateV2ValueType>, UInt32>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        } else if (which.is_date_time_v2()) {
            res = ColumnVector<UInt64>::create();
            if (col_const[1]) {
                execute_impl_right_const<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_element(0),
                        static_cast<ColumnVector<UInt64>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_data(),
                        static_cast<ColumnVector<UInt64>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        } else {
            res = ColumnVector<Int64>::create();
            if (col_const[1]) {
                execute_impl_right_const<VecDateTimeValue, Int64>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_element(0),
                        static_cast<ColumnVector<Int64>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            } else {
                execute_impl<VecDateTimeValue, Int64>(
                        static_cast<const ColumnVector<Int32>*>(argument_columns[0].get())
                                ->get_data(),
                        static_cast<const ColumnVector<Int32>*>(argument_columns[1].get())
                                ->get_data(),
                        static_cast<ColumnVector<Int64>*>(res->assume_mutable().get())->get_data(),
                        null_map->get_data());
            }
        }

        block.get_by_position(result).column = ColumnNullable::create(res, std::move(null_map));
        return Status::OK();
    }

private:
    template <typename DateValueType, typename ReturnType>
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
    template <typename DateValueType, typename ReturnType>
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
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            VecDateTimeValue ts_value = VecDateTimeValue();
            ts_value.set_time(l, 1, 1, 0, 0, 0);

            TimeInterval interval(DAY, r - 1, false);
            res_val = ts_value;
            if (!res_val.template date_add_interval<DAY>(interval)) {
                null_map[index] = 1;
                return;
            }
            res_val.cast_to_date();
        } else {
            res_val.set_time(l, 1, 1, 0, 0, 0, 0);
            TimeInterval interval(DAY, r - 1, false);
            if (!res_val.template date_add_interval<DAY>(interval)) {
                null_map[index] = 1;
            }
        }
    }
};

template <typename DateType>
struct DateTrunc {
    static constexpr auto name = "date_trunc";

    using ColumnType = date_cast::DateToColumnV<DateType>;
    using DateValueType = date_cast::DateToDateValueTypeV<DateType>;
    using ArgType = date_cast::ValueTypeOfDateColumnV<ColumnType>;

    static bool is_variadic() { return true; }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DateType>(), std::make_shared<DataTypeString>()};
    }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return make_nullable(std::make_shared<DateType>());
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 2);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        const auto& col0 = block.get_by_position(arguments[0]).column;
        bool col_const[2] = {is_column_const(*col0)};
        ColumnPtr argument_columns[2] = {
                col_const[0] ? static_cast<const ColumnConst&>(*col0).convert_to_full_column()
                             : col0};

        std::tie(argument_columns[1], col_const[1]) =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        auto datetime_column = static_cast<const ColumnType*>(argument_columns[0].get());
        auto str_column = static_cast<const ColumnString*>(argument_columns[1].get());
        auto& rdata = str_column->get_chars();
        auto& roffsets = str_column->get_offsets();

        ColumnPtr res = ColumnType::create();
        if (col_const[1]) {
            execute_impl_right_const(
                    datetime_column->get_data(), str_column->get_data_at(0),
                    static_cast<ColumnType*>(res->assume_mutable().get())->get_data(),
                    null_map->get_data(), input_rows_count);
        } else {
            execute_impl(datetime_column->get_data(), rdata, roffsets,
                         static_cast<ColumnType*>(res->assume_mutable().get())->get_data(),
                         null_map->get_data(), input_rows_count);
        }

        block.get_by_position(result).column = ColumnNullable::create(res, std::move(null_map));
        return Status::OK();
    }

private:
    static void execute_impl(const PaddedPODArray<ArgType>& ldata, const ColumnString::Chars& rdata,
                             const ColumnString::Offsets& roffsets, PaddedPODArray<ArgType>& res,
                             NullMap& null_map, size_t input_rows_count) {
        res.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto dt = binary_cast<ArgType, DateValueType>(ldata[i]);
            const char* str_data = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            _execute_inner_loop(dt, str_data, res, null_map, i);
        }
    }
    static void execute_impl_right_const(const PaddedPODArray<ArgType>& ldata,
                                         const StringRef& rdata, PaddedPODArray<ArgType>& res,
                                         NullMap& null_map, size_t input_rows_count) {
        res.resize(input_rows_count);
        std::string lower_str(rdata.data, rdata.size);
        std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto dt = binary_cast<ArgType, DateValueType>(ldata[i]);
            _execute_inner_loop(dt, lower_str.data(), res, null_map, i);
        }
    }
    template <typename T>
    static void _execute_inner_loop(T& dt, const char* str_data, PaddedPODArray<ArgType>& res,
                                    NullMap& null_map, size_t index) {
        if (std::strncmp("year", str_data, 4) == 0) {
            null_map[index] = !dt.template datetime_trunc<YEAR>();
        } else if (std::strncmp("quarter", str_data, 7) == 0) {
            null_map[index] = !dt.template datetime_trunc<QUARTER>();
        } else if (std::strncmp("month", str_data, 5) == 0) {
            null_map[index] = !dt.template datetime_trunc<MONTH>();
        } else if (std::strncmp("week", str_data, 4) == 0) {
            null_map[index] = !dt.template datetime_trunc<WEEK>();
        } else if (std::strncmp("day", str_data, 3) == 0) {
            null_map[index] = !dt.template datetime_trunc<DAY>();
        } else if (std::strncmp("hour", str_data, 4) == 0) {
            null_map[index] = !dt.template datetime_trunc<HOUR>();
        } else if (std::strncmp("minute", str_data, 6) == 0) {
            null_map[index] = !dt.template datetime_trunc<MINUTE>();
        } else if (std::strncmp("second", str_data, 6) == 0) {
            null_map[index] = !dt.template datetime_trunc<SECOND>();
        } else {
            null_map[index] = 1;
        }
        res[index] = binary_cast<DateValueType, ArgType>(dt);
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
                    static_cast<ColumnVector<Int64>*>(res_column->assume_mutable().get())
                            ->get_data());
        } else {
            res_column = ColumnVector<UInt32>::create(input_rows_count);
            execute_straight<DateV2Value<DateV2ValueType>, UInt32>(
                    input_rows_count, null_map->get_data(), data_col->get_data(),
                    static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                            ->get_data());
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
        RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(DataTypeInt32);
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const ColumnPtr& col_source = block.get_by_position(arguments[0]).column;
        auto col_result = ColumnVector<Int32>::create();
        auto null_map = ColumnVector<UInt8>::create();
        auto& col_result_data = col_result->get_data();

        col_result->resize(input_rows_count);

        if constexpr (std::is_same_v<DateType, DataTypeDate>) {
            null_map->resize(input_rows_count);
            auto& null_map_data = null_map->get_data();

            for (int i = 0; i < input_rows_count; i++) {
                if (col_source->is_null_at(i)) {
                    null_map_data[i] = true;
                    continue;
                }

                StringRef source = col_source->get_data_at(i);
                const VecDateTimeValue& ts_value =
                        reinterpret_cast<const VecDateTimeValue&>(*source.data);
                int64_t timestamp;
                if (!ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
                    null_map_data[i] = true;
                } else {
                    null_map_data[i] = false;
                    col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
                }
            }
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_result), std::move(null_map)));
        } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
            const auto is_nullable = block.get_by_position(arguments[0]).type->is_nullable();
            if (is_nullable) {
                null_map->resize(input_rows_count);
                auto& null_map_data = null_map->get_data();
                for (int i = 0; i < input_rows_count; i++) {
                    if (col_source->is_null_at(i)) {
                        DCHECK(is_nullable);
                        null_map_data[i] = true;
                        continue;
                    }

                    StringRef source = col_source->get_data_at(i);
                    const DateV2Value<DateV2ValueType>& ts_value =
                            reinterpret_cast<const DateV2Value<DateV2ValueType>&>(*source.data);
                    int64_t timestamp;
                    if (!ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
                        null_map_data[i] = true;
                    } else {
                        null_map_data[i] = false;
                        col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
                    }
                }
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(col_result), std::move(null_map)));
            } else {
                for (int i = 0; i < input_rows_count; i++) {
                    DCHECK(!col_source->is_null_at(i));
                    StringRef source = col_source->get_data_at(i);
                    const DateV2Value<DateV2ValueType>& ts_value =
                            reinterpret_cast<const DateV2Value<DateV2ValueType>&>(*source.data);
                    int64_t timestamp;
                    const auto valid =
                            ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj());
                    DCHECK(valid);
                    col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
                }
                block.replace_by_position(result, std::move(col_result));
            }
        } else {
            const auto is_nullable = block.get_by_position(arguments[0]).type->is_nullable();
            if (is_nullable) {
                null_map->resize(input_rows_count);
                auto& null_map_data = null_map->get_data();
                for (int i = 0; i < input_rows_count; i++) {
                    if (col_source->is_null_at(i)) {
                        DCHECK(is_nullable);
                        null_map_data[i] = true;
                        continue;
                    }

                    StringRef source = col_source->get_data_at(i);
                    const DateV2Value<DateTimeV2ValueType>& ts_value =
                            reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(*source.data);
                    int64_t timestamp;
                    if (!ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
                        null_map_data[i] = true;
                    } else {
                        null_map_data[i] = false;
                        col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
                    }
                }
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(col_result), std::move(null_map)));
            } else {
                for (int i = 0; i < input_rows_count; i++) {
                    DCHECK(!col_source->is_null_at(i));
                    StringRef source = col_source->get_data_at(i);
                    const DateV2Value<DateTimeV2ValueType>& ts_value =
                            reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(*source.data);
                    int64_t timestamp;
                    const auto valid =
                            ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj());
                    DCHECK(valid);
                    col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
                }
                block.replace_by_position(result, std::move(col_result));
            }
        }

        return Status::OK();
    }
};

template <typename DateType>
struct UnixTimeStampDatetimeImpl : public UnixTimeStampDateImpl<DateType> {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DateType>()}; }
};

struct UnixTimeStampStrImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return make_nullable(std::make_shared<DataTypeInt32>());
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const ColumnPtr col_source = block.get_by_position(arguments[0]).column;
        const ColumnPtr col_format = block.get_by_position(arguments[1]).column;

        auto col_result = ColumnVector<Int32>::create();
        auto null_map = ColumnVector<UInt8>::create();

        col_result->resize(input_rows_count);
        null_map->resize(input_rows_count);

        auto& col_result_data = col_result->get_data();
        auto& null_map_data = null_map->get_data();

        for (int i = 0; i < input_rows_count; i++) {
            if (col_source->is_null_at(i) || col_format->is_null_at(i)) {
                null_map_data[i] = true;
                continue;
            }

            StringRef source = col_source->get_data_at(i);
            StringRef fmt = col_format->get_data_at(i);

            VecDateTimeValue ts_value;
            if (!ts_value.from_date_format_str(fmt.data, fmt.size, source.data, source.size)) {
                null_map_data[i] = true;
                continue;
            }

            int64_t timestamp;
            if (!ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
                null_map_data[i] = true;
            } else {
                null_map_data[i] = false;
                col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
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

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
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
        return make_nullable(std::make_shared<DataTypeInt64>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnUInt64&>(*arg_col);
        auto res_col = ColumnInt64::create();
        auto null_vector = ColumnVector<UInt8>::create();
        res_col->get_data().resize_fill(input_rows_count, 0);
        null_vector->get_data().resize_fill(input_rows_count, false);
        NullMap& null_map = null_vector->get_data();
        auto& res_data = res_col->get_data();
        const cctz::time_zone& time_zone = context->state()->timezone_obj();
        for (int i = 0; i < input_rows_count; i++) {
            if (arg_col->is_null_at(i)) {
                null_map[i] = true;
                continue;
            }
            StringRef source = column_data.get_data_at(i);
            const DateV2Value<DateTimeV2ValueType>& dt =
                    reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(*source.data);
            int64_t timestamp {0};
            if (!dt.unix_timestamp(&timestamp, time_zone)) {
                null_map[i] = true;
            } else {
                auto microsecond = dt.microsecond();
                timestamp = timestamp * Impl::ratio + microsecond / ratio_to_micro;
                res_data[i] = timestamp;
            }
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_col), std::move(null_vector));
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

        if constexpr (std::is_same_v<DateType, DataTypeDateTime> ||
                      std::is_same_v<DateType, DataTypeDate>) {
            return make_nullable(std::make_shared<DataTypeDate>());
        } else {
            return is_nullable ? make_nullable(std::make_shared<DataTypeDateV2>())
                               : std::make_shared<DataTypeDateV2>();
        }
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (std::is_same_v<DateType, DataTypeDate>) {
            return {std::make_shared<DataTypeDate>()};
        } else if constexpr (std::is_same_v<DateType, DataTypeDateTime>) {
            return {std::make_shared<DataTypeDateTime>()};
        } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
            return {std::make_shared<DataTypeDateV2>()};
        } else {
            return {std::make_shared<DataTypeDateTimeV2>()};
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return Impl<DateType>::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

template <typename DateType>
struct LastDayImpl {
    static constexpr auto name = "last_day";

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const auto is_nullable = block.get_by_position(result).type->is_nullable();
        ColumnPtr res_column;
        ColumnPtr argument_column = remove_nullable(block.get_by_position(arguments[0]).column);
        if (is_nullable) {
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            if constexpr (std::is_same_v<DateType, DataTypeDateTime> ||
                          std::is_same_v<DateType, DataTypeDate>) {
                auto data_col = assert_cast<const ColumnVector<Int64>*>(argument_column.get());
                res_column = ColumnInt64::create(input_rows_count);
                execute_straight<VecDateTimeValue, Int64, Int64>(
                        input_rows_count, null_map->get_data(), data_col->get_data(),
                        static_cast<ColumnVector<Int64>*>(res_column->assume_mutable().get())
                                ->get_data());

            } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                auto data_col = assert_cast<const ColumnVector<UInt32>*>(argument_column.get());
                res_column = ColumnVector<UInt32>::create(input_rows_count);
                execute_straight<DateV2Value<DateV2ValueType>, UInt32, UInt32>(
                        input_rows_count, null_map->get_data(), data_col->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                                ->get_data());

            } else if constexpr (std::is_same_v<DateType, DataTypeDateTimeV2>) {
                auto data_col = assert_cast<const ColumnVector<UInt64>*>(argument_column.get());
                res_column = ColumnVector<UInt32>::create(input_rows_count);
                execute_straight<DateV2Value<DateTimeV2ValueType>, UInt32, UInt64>(
                        input_rows_count, null_map->get_data(), data_col->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                                ->get_data());
            }

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
            if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                auto data_col = assert_cast<const ColumnVector<UInt32>*>(argument_column.get());
                res_column = ColumnVector<UInt32>::create(input_rows_count);
                execute_straight<DateV2Value<DateV2ValueType>, UInt32, UInt32>(
                        input_rows_count, data_col->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                                ->get_data());

            } else if constexpr (std::is_same_v<DateType, DataTypeDateTimeV2>) {
                auto data_col = assert_cast<const ColumnVector<UInt64>*>(argument_column.get());
                res_column = ColumnVector<UInt32>::create(input_rows_count);
                execute_straight<DateV2Value<DateTimeV2ValueType>, UInt32, UInt64>(
                        input_rows_count, data_col->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                                ->get_data());
            }
            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }

    template <typename DateValueType, typename ReturnType, typename InputDateType>
    static void execute_straight(size_t input_rows_count, NullMap& null_map,
                                 const PaddedPODArray<InputDateType>& data_col,
                                 PaddedPODArray<ReturnType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<Int64, VecDateTimeValue>(cur_data);
                if (!ts_value.is_valid_date()) {
                    null_map[i] = 1;
                    continue;
                }
                int day = get_last_month_day(ts_value.year(), ts_value.month());
                ts_value.set_time(ts_value.year(), ts_value.month(), day, 0, 0, 0);
                ts_value.set_type(TIME_DATE);
                res_data[i] = binary_cast<VecDateTimeValue, Int64>(ts_value);

            } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<UInt32, DateValueType>(cur_data);
                if (!ts_value.is_valid_date()) {
                    null_map[i] = 1;
                    continue;
                }
                int day = get_last_month_day(ts_value.year(), ts_value.month());
                ts_value.template set_time_unit<TimeUnit::DAY>(day);
                res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);

            } else {
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<UInt64, DateValueType>(cur_data);
                if (!ts_value.is_valid_date()) {
                    null_map[i] = 1;
                    continue;
                }
                int day = get_last_month_day(ts_value.year(), ts_value.month());
                ts_value.template set_time_unit<TimeUnit::DAY>(day);
                ts_value.set_time(ts_value.year(), ts_value.month(), day, 0, 0, 0, 0);
                UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
            }
        }
    }

    template <typename DateValueType, typename ReturnType, typename InputDateType>
    static void execute_straight(size_t input_rows_count,
                                 const PaddedPODArray<InputDateType>& data_col,
                                 PaddedPODArray<ReturnType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<UInt32, DateValueType>(cur_data);
                DCHECK(ts_value.is_valid_date());
                int day = get_last_month_day(ts_value.year(), ts_value.month());
                ts_value.template set_time_unit<TimeUnit::DAY>(day);
                res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);
            } else {
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<UInt64, DateValueType>(cur_data);
                DCHECK(ts_value.is_valid_date());
                int day = get_last_month_day(ts_value.year(), ts_value.month());
                ts_value.template set_time_unit<TimeUnit::DAY>(day);
                ts_value.set_time(ts_value.year(), ts_value.month(), day, 0, 0, 0, 0);
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

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const auto is_nullable = block.get_by_position(result).type->is_nullable();
        ColumnPtr argument_column = remove_nullable(block.get_by_position(arguments[0]).column);
        ColumnPtr res_column;
        if (is_nullable) {
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            if constexpr (std::is_same_v<DateType, DataTypeDateTime> ||
                          std::is_same_v<DateType, DataTypeDate>) {
                auto data_col = assert_cast<const ColumnVector<Int64>*>(argument_column.get());
                res_column = ColumnInt64::create(input_rows_count);
                execute_straight<VecDateTimeValue, Int64, Int64>(
                        input_rows_count, null_map->get_data(), data_col->get_data(),
                        static_cast<ColumnVector<Int64>*>(res_column->assume_mutable().get())
                                ->get_data());

            } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                auto data_col = assert_cast<const ColumnVector<UInt32>*>(argument_column.get());
                res_column = ColumnVector<UInt32>::create(input_rows_count);
                execute_straight<DateV2Value<DateV2ValueType>, UInt32, UInt32>(
                        input_rows_count, null_map->get_data(), data_col->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                                ->get_data());

            } else if constexpr (std::is_same_v<DateType, DataTypeDateTimeV2>) {
                auto data_col = assert_cast<const ColumnVector<UInt64>*>(argument_column.get());
                res_column = ColumnVector<UInt32>::create(input_rows_count);
                execute_straight<DateV2Value<DateTimeV2ValueType>, UInt32, UInt64>(
                        input_rows_count, null_map->get_data(), data_col->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                                ->get_data());
            }
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
            if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                auto data_col = assert_cast<const ColumnVector<UInt32>*>(argument_column.get());
                res_column = ColumnVector<UInt32>::create(input_rows_count);
                execute_straight<DateV2Value<DateV2ValueType>, UInt32, UInt32>(
                        input_rows_count, data_col->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                                ->get_data());

            } else if constexpr (std::is_same_v<DateType, DataTypeDateTimeV2>) {
                auto data_col = assert_cast<const ColumnVector<UInt64>*>(argument_column.get());
                res_column = ColumnVector<UInt32>::create(input_rows_count);
                execute_straight<DateV2Value<DateTimeV2ValueType>, UInt32, UInt64>(
                        input_rows_count, data_col->get_data(),
                        static_cast<ColumnVector<UInt32>*>(res_column->assume_mutable().get())
                                ->get_data());
            }
            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }

    template <typename DateValueType, typename ReturnType, typename InputDateType>
    static void execute_straight(size_t input_rows_count, NullMap& null_map,
                                 const PaddedPODArray<InputDateType>& data_col,
                                 PaddedPODArray<ReturnType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<Int64, VecDateTimeValue>(cur_data);
                if (!ts_value.is_valid_date()) {
                    null_map[i] = 1;
                    continue;
                }
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.set_time(ts_value.year(), ts_value.month(), 1, 0, 0, 0);
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

            } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<UInt32, DateValueType>(cur_data);
                if (!ts_value.is_valid_date()) {
                    null_map[i] = 1;
                    continue;
                }
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.template set_time_unit<TimeUnit::DAY>(1);
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
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<UInt64, DateValueType>(cur_data);
                if (!ts_value.is_valid_date()) {
                    null_map[i] = 1;
                    continue;
                }
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.set_time(ts_value.year(), ts_value.month(), 1, 0, 0, 0, 0);
                    UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                    DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
                    continue;
                }
                // day_of_week, from 1(Mon) to 7(Sun)
                int day_of_week = ts_value.weekday() + 1;
                int gap_of_monday = day_of_week - 1;
                TimeInterval interval(DAY, gap_of_monday, true);
                ts_value.template date_add_interval<DAY>(interval);
                ts_value.set_time(ts_value.year(), ts_value.month(), ts_value.day(), 0, 0, 0, 0);
                UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
            }
        }
    }

    template <typename DateValueType, typename ReturnType, typename InputDateType>
    static void execute_straight(size_t input_rows_count,
                                 const PaddedPODArray<InputDateType>& data_col,
                                 PaddedPODArray<ReturnType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<UInt32, DateValueType>(cur_data);
                DCHECK(ts_value.is_valid_date());
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.template set_time_unit<TimeUnit::DAY>(1);
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
                const auto& cur_data = data_col[i];
                auto ts_value = binary_cast<UInt64, DateValueType>(cur_data);
                DCHECK(ts_value.is_valid_date());
                if (is_special_day(ts_value.year(), ts_value.month(), ts_value.day())) {
                    ts_value.set_time(ts_value.year(), ts_value.month(), 1, 0, 0, 0, 0);
                    UInt64 cast_value = binary_cast<DateValueType, UInt64>(ts_value);
                    DataTypeDateTimeV2::cast_to_date_v2(cast_value, res_data[i]);
                    continue;
                }
                // day_of_week, from 1(Mon) to 7(Sun)
                int day_of_week = ts_value.weekday() + 1;
                int gap_of_monday = day_of_week - 1;
                TimeInterval interval(DAY, gap_of_monday, true);
                ts_value.template date_add_interval<DAY>(interval);
                ts_value.set_time(ts_value.year(), ts_value.month(), ts_value.day(), 0, 0, 0, 0);
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

    //TODO: add function below when we fixed be-ut.
    //ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return Impl::execute(context, block, arguments, result, input_rows_count);
    }
};

using FunctionStrToDate = FunctionOtherTypesToDateType<StrToDate>;
using FunctionMakeDate = FunctionOtherTypesToDateType<MakeDateImpl>;
using FunctionDateTruncDate = FunctionOtherTypesToDateType<DateTrunc<DataTypeDate>>;
using FunctionDateTruncDateV2 = FunctionOtherTypesToDateType<DateTrunc<DataTypeDateV2>>;
using FunctionDateTruncDatetime = FunctionOtherTypesToDateType<DateTrunc<DataTypeDateTime>>;
using FunctionDateTruncDatetimeV2 = FunctionOtherTypesToDateType<DateTrunc<DataTypeDateTimeV2>>;

void register_function_timestamp(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStrToDate>();
    factory.register_function<FunctionMakeDate>();
    factory.register_function<FromDays>();
    factory.register_function<FunctionDateTruncDate>();
    factory.register_function<FunctionDateTruncDateV2>();
    factory.register_function<FunctionDateTruncDatetime>();
    factory.register_function<FunctionDateTruncDatetimeV2>();

    factory.register_function<FunctionUnixTimestamp<UnixTimeStampImpl>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDate>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateV2>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateTimeV2>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDatetimeImpl<DataTypeDate>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDatetimeImpl<DataTypeDateV2>>>();
    factory.register_function<
            FunctionUnixTimestamp<UnixTimeStampDatetimeImpl<DataTypeDateTimeV2>>>();
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
