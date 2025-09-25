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
#include <parquet/column_writer.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "util/binary_cast.hpp"
#include "util/time_lut.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
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
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/datetime_errors.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_avoid_begin.h"

struct StrToDate {
    static constexpr auto name = "str_to_date";

    static bool is_variadic() { return true; }

    static size_t get_number_of_arguments() { return 2; }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    //ATTN: because str_to_date may have different return type with same input types (for some inputs with special
    // format, if skip FE fold or FE fold failed). in implementation, we use type of result column slot in block, so
    // it's ok. just different with get_return_type_impl.
    static bool skip_return_type_check() { return true; }
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        // it's FAKE. takes no effect.
        return std::make_shared<DataTypeDateTimeV2>(6);
    }

    // Handle nulls manually to prevent invalid default values from causing errors
    static bool use_default_implementation_for_nulls() { return false; }

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
                          uint32_t result, size_t input_rows_count) {
        // Handle null map manually - update result null map from input null maps upfront
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);
        NullMap& result_null_map = assert_cast<ColumnUInt8&>(*result_null_map_column).get_data();

        ColumnPtr argument_columns[2];
        bool col_const[2];

        // Update result null map from all input null maps
        for (int i = 0; i < 2; ++i) {
            const ColumnPtr& col = block.get_by_position(arguments[i]).column;
            col_const[i] = is_column_const(*col);
            const NullMap* null_map = VectorizedUtils::get_null_map(col);
            if (null_map) {
                VectorizedUtils::update_null_map(result_null_map, *null_map, col_const[i]);
            }
        }

        // Extract nested columns from const(nullable) wrappers
        argument_columns[0] = col_const[0] ? static_cast<const ColumnConst&>(
                                                     *block.get_by_position(arguments[0]).column)
                                                     .convert_to_full_column()
                                           : block.get_by_position(arguments[0]).column;
        argument_columns[0] = remove_nullable(argument_columns[0]);

        std::tie(argument_columns[1], col_const[1]) =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        argument_columns[1] = remove_nullable(argument_columns[1]);

        const auto* specific_str_column =
                assert_cast<const ColumnString*>(argument_columns[0].get());
        const auto* specific_char_column =
                assert_cast<const ColumnString*>(argument_columns[1].get());

        const auto& ldata = specific_str_column->get_chars();
        const auto& loffsets = specific_str_column->get_offsets();

        const auto& rdata = specific_char_column->get_chars();
        const auto& roffsets = specific_char_column->get_offsets();

        ColumnPtr res;
        // Because of we cant distinguish by return_type when we find function. so the return_type may NOT be same with real return type
        // which decided by FE. we directly use block column's type which decided by FE.
        if (block.get_by_position(result).type->get_primitive_type() == TYPE_DATEV2) {
            res = ColumnDateV2::create(input_rows_count);
            if (col_const[1]) {
                execute_impl_const_right<TYPE_DATEV2>(
                        context, ldata, loffsets, specific_char_column->get_data_at(0),
                        result_null_map,
                        static_cast<ColumnDateV2*>(res->assume_mutable().get())->get_data());
            } else {
                execute_impl<TYPE_DATEV2>(
                        context, ldata, loffsets, rdata, roffsets, result_null_map,
                        static_cast<ColumnDateV2*>(res->assume_mutable().get())->get_data());
            }
        } else {
            DCHECK(block.get_by_position(result).type->get_primitive_type() == TYPE_DATETIMEV2);

            res = ColumnDateTimeV2::create(input_rows_count);
            if (col_const[1]) {
                execute_impl_const_right<TYPE_DATETIMEV2>(
                        context, ldata, loffsets, specific_char_column->get_data_at(0),
                        result_null_map,
                        static_cast<ColumnDateTimeV2*>(res->assume_mutable().get())->get_data());
            } else {
                execute_impl<TYPE_DATETIMEV2>(
                        context, ldata, loffsets, rdata, roffsets, result_null_map,
                        static_cast<ColumnDateTimeV2*>(res->assume_mutable().get())->get_data());
            }
        }

        // Wrap result in nullable column only if input has nullable arguments
        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(
                    result,
                    ColumnNullable::create(std::move(res), std::move(result_null_map_column)));
        } else {
            block.replace_by_position(result, std::move(res));
        }
        return Status::OK();
    }

private:
    template <PrimitiveType PType>
    static void execute_impl(
            FunctionContext* context, const ColumnString::Chars& ldata,
            const ColumnString::Offsets& loffsets, const ColumnString::Chars& rdata,
            const ColumnString::Offsets& roffsets, const NullMap& result_null_map,
            PaddedPODArray<typename PrimitiveTypeTraits<PType>::CppNativeType>& res) {
        size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            // Skip processing if result should be null (determined upfront)
            if (result_null_map[i]) {
                continue;
            }
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            size_t l_str_size = loffsets[i] - loffsets[i - 1];

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            size_t r_str_size = roffsets[i] - roffsets[i - 1];
            const StringRef format_str = rewrite_specific_format(r_raw_str, r_str_size);
            _execute_inner_loop<PType>(l_raw_str, l_str_size, format_str.data, format_str.size,
                                       context, res, i);
        }
    }

    template <PrimitiveType PType>
    static void execute_impl_const_right(
            FunctionContext* context, const ColumnString::Chars& ldata,
            const ColumnString::Offsets& loffsets, const StringRef& rdata,
            const NullMap& result_null_map,
            PaddedPODArray<typename PrimitiveTypeTraits<PType>::CppNativeType>& res) {
        size_t size = loffsets.size();
        res.resize(size);
        const StringRef format_str = rewrite_specific_format(rdata.data, rdata.size);
        for (size_t i = 0; i < size; ++i) {
            // Skip processing if result should be null (determined upfront)
            if (result_null_map[i]) {
                continue;
            }
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            size_t l_str_size = loffsets[i] - loffsets[i - 1];

            _execute_inner_loop<PType>(l_raw_str, l_str_size, format_str.data, format_str.size,
                                       context, res, i);
        }
    }

    template <PrimitiveType PType>
    static void _execute_inner_loop(
            const char* l_raw_str, size_t l_str_size, const char* r_raw_str, size_t r_str_size,
            FunctionContext* context,
            PaddedPODArray<typename PrimitiveTypeTraits<PType>::CppNativeType>& res, size_t index) {
        auto& ts_val =
                *reinterpret_cast<typename PrimitiveTypeTraits<PType>::CppType*>(&res[index]);
        if (!ts_val.from_date_format_str(r_raw_str, r_str_size, l_raw_str, l_str_size))
                [[unlikely]] {
            throw_invalid_strings("str_to_date", std::string_view(l_raw_str, l_str_size),
                                  std::string_view(r_raw_str, r_str_size));
        }
    }
};

struct MakeDateImpl {
    static constexpr auto name = "makedate";
    using DateValueType = PrimitiveTypeTraits<PrimitiveType::TYPE_DATEV2>::CppType;
    using NativeType = PrimitiveTypeTraits<PrimitiveType::TYPE_DATEV2>::CppNativeType;

    static bool is_variadic() { return false; }

    static size_t get_number_of_arguments() { return 2; }

    static DataTypes get_variadic_argument_types() { return {}; }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeDateV2>();
    }

    // Handle nulls manually to prevent invalid default values from causing errors
    static bool use_default_implementation_for_nulls() { return false; }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 2);

        // Handle null map manually - update result null map from input null maps upfront
        auto result_null_map_column = ColumnBool::create(input_rows_count, 0);
        NullMap& result_null_map = assert_cast<ColumnBool&>(*result_null_map_column).get_data();

        ColumnPtr argument_columns[2];
        bool col_const[2];

        // Update result null map from all input null maps
        for (int i = 0; i < 2; ++i) {
            const ColumnPtr& col = block.get_by_position(arguments[i]).column;
            col_const[i] = is_column_const(*col);
            const NullMap* null_map = VectorizedUtils::get_null_map(col);
            if (null_map) {
                VectorizedUtils::update_null_map(result_null_map, *null_map, col_const[i]);
            }
        }

        // Extract nested columns from const(nullable) wrappers
        argument_columns[0] = col_const[0] ? static_cast<const ColumnConst&>(
                                                     *block.get_by_position(arguments[0]).column)
                                                     .convert_to_full_column()
                                           : block.get_by_position(arguments[0]).column;
        argument_columns[0] = remove_nullable(argument_columns[0]);

        std::tie(argument_columns[1], col_const[1]) =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        argument_columns[1] = remove_nullable(argument_columns[1]);

        const auto* year_col = assert_cast<const ColumnInt32*>(argument_columns[0].get());
        const auto* dayofyear_col = assert_cast<const ColumnInt32*>(argument_columns[1].get());

        ColumnPtr res_column;

        res_column = ColumnDateV2::create(input_rows_count);
        if (col_const[1]) {
            execute_impl_right_const(
                    year_col->get_data(), dayofyear_col->get_element(0), result_null_map,
                    static_cast<ColumnDateV2*>(res_column->assume_mutable().get())->get_data());
        } else {
            execute_impl(
                    year_col->get_data(), dayofyear_col->get_data(), result_null_map,
                    static_cast<ColumnDateV2*>(res_column->assume_mutable().get())->get_data());
        }

        // Wrap result in nullable column only if input has nullable arguments
        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(result,
                                      ColumnNullable::create(std::move(res_column),
                                                             std::move(result_null_map_column)));
        } else {
            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }

private:
    static void execute_impl(const PaddedPODArray<Int32>& year_data,
                             const PaddedPODArray<Int32>& dayofyear_data,
                             const NullMap& result_null_map, PaddedPODArray<NativeType>& res) {
        auto len = year_data.size();
        res.resize(len);

        for (size_t i = 0; i < len; ++i) {
            // Skip processing if result should be null (determined upfront)
            if (result_null_map[i]) {
                continue;
            }
            const auto& year = year_data[i];
            const auto& dayofyear = dayofyear_data[i];
            if (dayofyear <= 0 || year < 0 || year > 9999) [[unlikely]] {
                throw_out_of_bound_two_ints(name, year, dayofyear);
            }
            _execute_inner_loop(year, dayofyear, res, i);
        }
    }

    static void execute_impl_right_const(const PaddedPODArray<Int32>& year_data, Int32 dayofyear,
                                         const NullMap& result_null_map,
                                         PaddedPODArray<NativeType>& res) {
        auto len = year_data.size();
        res.resize(len);

        for (size_t i = 0; i < len; ++i) {
            // Skip processing if result should be null (determined upfront)
            if (result_null_map[i]) {
                continue;
            }
            const auto& year = year_data[i];
            if (dayofyear <= 0 || year < 0 || year > 9999) [[unlikely]] {
                throw_out_of_bound_two_ints(name, year, dayofyear);
            }
            _execute_inner_loop(year, dayofyear, res, i);
        }
    }

    static void _execute_inner_loop(const int& year, const int& dayofyear,
                                    PaddedPODArray<NativeType>& res, size_t index) {
        auto& res_val = *reinterpret_cast<DateValueType*>(&res[index]);
        res_val.unchecked_set_time(year, 1, 1, 0, 0, 0, 0);
        TimeInterval interval(DAY, dayofyear - 1, false);
        if (!res_val.template date_add_interval<DAY>(interval)) {
            throw_out_of_bound_two_ints(name, year, dayofyear);
        }
    }
};

struct DateTruncState {
    using Callback_function = std::function<void(const ColumnPtr&, ColumnPtr& res, size_t)>;
    Callback_function callback_function;
};

template <PrimitiveType PType, bool DateArgIsFirst>
struct DateTrunc {
    static constexpr auto name = "date_trunc";
    using DateType = PrimitiveTypeTraits<PType>::DataType;
    using ColumnType = PrimitiveTypeTraits<PType>::ColumnType;
    using DateValueType = PrimitiveTypeTraits<PType>::CppType;
    using NativeType = PrimitiveTypeTraits<PType>::CppNativeType;

    static bool is_variadic() { return true; }

    static size_t get_number_of_arguments() { return 2; }

    static DataTypes get_variadic_argument_types() {
        if constexpr (DateArgIsFirst) {
            return {std::make_shared<DateType>(), std::make_shared<DataTypeString>()};
        } else {
            return {std::make_shared<DataTypeString>(), std::make_shared<DateType>()};
        }
    }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DateType>();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        if (!context->is_col_constant(DateArgIsFirst ? 1 : 0)) {
            return Status::InvalidArgument(
                    "date_trunc function of time unit argument must be constant.");
        }
        const auto& data_str =
                context->get_constant_col(DateArgIsFirst ? 1 : 0)->column_ptr->get_data_at(0);
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
                          uint32_t result, size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 2);

        const auto& datetime_column = block.get_by_position(arguments[DateArgIsFirst ? 0 : 1])
                                              .column->convert_to_full_column_if_const();
        ColumnPtr res = ColumnType::create(input_rows_count);
        auto* state = reinterpret_cast<DateTruncState*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(state != nullptr);
        state->callback_function(datetime_column, res, input_rows_count);
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

private:
    template <TimeUnit Unit>
    static void execute_impl_right_const(const ColumnPtr& datetime_column, ColumnPtr& result_column,
                                         size_t input_rows_count) {
        auto& data = static_cast<const ColumnType*>(datetime_column.get())->get_data();
        auto& res = static_cast<ColumnType*>(result_column->assume_mutable().get())->get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto dt = binary_cast<NativeType, DateValueType>(data[i]);
            if (!dt.template datetime_trunc<Unit>()) {
                throw_out_of_bound_one_date<DateValueType>(name, data[i]);
            }
            res[i] = binary_cast<DateValueType, NativeType>(dt);
        }
    }
};

class FromDays : public IFunction {
public:
    static constexpr auto name = "from_days";

    static FunctionPtr create() { return std::make_shared<FromDays>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return have_nullable(arguments) ? make_nullable(std::make_shared<DataTypeDate>())
                                        : std::make_shared<DataTypeDate>();
    }

    // default value for FromDays is invalid, should handle nulls manually
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr& argument_column = block.get_by_position(arguments[0]).column;

        // Handle null map manually - update result null map from input null map upfront
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);
        NullMap& result_null_map = assert_cast<ColumnUInt8&>(*result_null_map_column).get_data();

        // Update result null map from input null map using standard approach
        bool col_const = is_column_const(*argument_column);
        const NullMap* input_null_map = VectorizedUtils::get_null_map(argument_column);
        if (input_null_map) {
            VectorizedUtils::update_null_map(result_null_map, *input_null_map, col_const);
        }

        // Extract nested column data
        ColumnPtr nested_column =
                col_const
                        ? static_cast<const ColumnConst&>(*argument_column).convert_to_full_column()
                        : argument_column;
        nested_column = remove_nullable(nested_column);
        const auto* data_col = assert_cast<const ColumnInt32*>(nested_column.get());

        ColumnPtr res_column;
        if (block.get_by_position(result).type->get_primitive_type() == PrimitiveType::TYPE_DATE) {
            res_column = ColumnDate::create(input_rows_count);
            _execute<VecDateTimeValue, Int64>(
                    input_rows_count, data_col->get_data(), result_null_map,
                    static_cast<ColumnDateTime*>(res_column->assume_mutable().get())->get_data());
        } else {
            res_column = ColumnDateV2::create(input_rows_count);
            _execute<DateV2Value<DateV2ValueType>, UInt32>(
                    input_rows_count, data_col->get_data(), result_null_map,
                    static_cast<ColumnDateV2*>(res_column->assume_mutable().get())->get_data());
        }

        // Wrap result in nullable column only if input has nullable arguments
        if (block.get_by_position(arguments[0]).type->is_nullable()) {
            block.replace_by_position(result,
                                      ColumnNullable::create(std::move(res_column),
                                                             std::move(result_null_map_column)));
        } else {
            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }

private:
    template <typename DateValueType, typename ReturnType>
    void _execute(size_t input_rows_count, const PaddedPODArray<Int32>& data_col,
                  const NullMap& result_null_map, PaddedPODArray<ReturnType>& res_data) const {
        for (int i = 0; i < input_rows_count; i++) {
            // Skip processing if result should be null (determined upfront)
            if (result_null_map[i]) {
                continue;
            }

            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
                const auto& cur_data = data_col[i];
                auto& ts_value = *reinterpret_cast<DateValueType*>(&res_data[i]);
                if (!ts_value.from_date_daynr(cur_data)) {
                    throw_out_of_bound_int(name, cur_data);
                }
            } else {
                const auto& cur_data = data_col[i];
                auto& ts_value = *reinterpret_cast<DateValueType*>(&res_data[i]);
                if (!ts_value.get_date_from_daynr(cur_data)) {
                    throw_out_of_bound_int(name, cur_data);
                }
            }
        }
    }
};

static int64_t trim_timestamp(int64_t timestamp, bool new_version = false) {
    if (timestamp < 0 || (!new_version && timestamp > INT_MAX)) {
        return 0;
    }
    return timestamp;
}

static std::pair<Int64, Int64> trim_timestamp(std::pair<Int64, Int64> timestamp,
                                              bool new_version = false) {
    if (timestamp.first < 0 || (!new_version && timestamp.first > INT_MAX)) {
        return {0, 0};
    }
    return timestamp;
}

template <bool NewVersion = false>
struct UnixTimeStampImpl {
    static DataTypes get_variadic_argument_types() { return {}; }

    using DataType = std::conditional_t<NewVersion, DataTypeInt64, DataTypeInt32>;
    using ColumnType = std::conditional_t<NewVersion, ColumnInt64, ColumnInt32>;

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return std::make_shared<DataType>();
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto col_result = ColumnType::create();
        col_result->resize(1);
        col_result->get_data()[0] = context->state()->timestamp_ms() / 1000;
        auto col_const = ColumnConst::create(std::move(col_result), input_rows_count);
        block.replace_by_position(result, std::move(col_const));
        return Status::OK();
    }
};

template <typename DateType, bool NewVersion = false>
struct UnixTimeStampDateImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DateType>()}; }

    using ResultDataType =
            std::conditional_t<std::is_same_v<DateType, DataTypeDateTimeV2>, DataTypeDecimal64,
                               std::conditional_t<NewVersion, DataTypeInt64, DataTypeInt32>>;
    using ResultColumnType =
            std::conditional_t<std::is_same_v<DateType, DataTypeDateTimeV2>, ColumnDecimal64,
                               std::conditional_t<NewVersion, ColumnInt64, ColumnInt32>>;

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        if constexpr (std::is_same_v<DateType, DataTypeDateTimeV2>) {
            UInt32 scale = arguments[0].type->get_scale();
            return std::make_shared<ResultDataType>(12 + scale, scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        const ColumnPtr& col = block.get_by_position(arguments[0]).column;
        DCHECK(!col->is_nullable());

        if constexpr (std::is_same_v<DateType, DataTypeDate> ||
                      std::is_same_v<DateType, DataTypeDateTime>) {
            const auto* col_source = assert_cast<const typename DateType::ColumnType*>(col.get());
            auto col_result = ResultColumnType::create();
            auto& col_result_data = col_result->get_data();
            col_result->resize(input_rows_count);

            for (int i = 0; i < input_rows_count; i++) {
                StringRef source = col_source->get_data_at(i);
                const auto& ts_value = reinterpret_cast<const VecDateTimeValue&>(*source.data);
                int64_t timestamp {};
                ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj());
                col_result_data[i] = trim_timestamp(timestamp, NewVersion);
            }
            block.replace_by_position(result, std::move(col_result));
        } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
            const auto* col_source = assert_cast<const ColumnDateV2*>(col.get());
            auto col_result = ResultColumnType::create();
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
                col_result_data[i] = trim_timestamp(timestamp, NewVersion);
            }
            block.replace_by_position(result, std::move(col_result));
        } else { // DatetimeV2
            const auto* col_source = assert_cast<const ColumnDateTimeV2*>(col.get());
            UInt32 scale = block.get_by_position(arguments[0]).type->get_scale();
            auto col_result = ColumnDecimal64::create(input_rows_count, scale);
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

                auto [sec, ms] = trim_timestamp(timestamp, NewVersion);
                col_result_data[i] =
                        Decimal64::from_int_frac(
                                sec, ms / static_cast<int64_t>(std::pow(10, 6 - scale)), scale)
                                .value;
            }
            block.replace_by_position(result, std::move(col_result));
        }

        return Status::OK();
    }
};

template <typename DateType, bool NewVersion = false>
struct UnixTimeStampDatetimeImpl : public UnixTimeStampDateImpl<DateType, NewVersion> {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DateType>()}; }
};

// This impl doesn't use default impl to deal null value.
template <bool NewVersion = false>
struct UnixTimeStampStrImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        if constexpr (NewVersion) {
            return std::make_shared<DataTypeDecimal64>(18, 6);
        }
        return std::make_shared<DataTypeDecimal64>(16, 6);
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        ColumnPtr col_left = nullptr, col_right = nullptr;
        bool source_const = false, format_const = false;
        std::tie(col_left, source_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        std::tie(col_right, format_const) =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        auto col_result = ColumnDecimal64::create(input_rows_count, 6);
        auto& col_result_data = col_result->get_data();

        const auto* col_source = assert_cast<const ColumnString*>(col_left.get());
        const auto* col_format = assert_cast<const ColumnString*>(col_right.get());
        for (int i = 0; i < input_rows_count; i++) {
            StringRef source = col_source->get_data_at(index_check_const(i, source_const));
            StringRef fmt = col_format->get_data_at(index_check_const(i, format_const));

            DateV2Value<DateTimeV2ValueType> ts_value;
            //FIXME: use new serde to parse the input string
            if (!ts_value.from_date_format_str(fmt.data, fmt.size, source.data, source.size)) {
                throw_invalid_strings("unix_timestamp", source, fmt);
            }

            std::pair<int64_t, int64_t> timestamp {};
            if (!ts_value.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
                // should not happen
            } else {
                auto [sec, ms] = trim_timestamp(timestamp, NewVersion);
                // trailing ms
                auto ms_str = std::to_string(ms).substr(0, 6);
                if (ms_str.empty()) {
                    ms_str = "0";
                }

                col_result_data[i] = Decimal64::from_int_frac(sec, std::stoll(ms_str), 6).value;
            }
        }

        block.replace_by_position(result, std::move(col_result));

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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

template <typename Impl>
class FunctionUnixTimestampNew : public IFunction {
public:
    static constexpr auto name = "unix_timestamp_new";
    static FunctionPtr create() { return std::make_shared<FunctionUnixTimestampNew<Impl>>(); }

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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
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
                        uint32_t result, size_t input_rows_count) const override {
        const auto& arg_col = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnDateTimeV2&>(*arg_col);
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

template <template <PrimitiveType> class Impl, PrimitiveType PType>
class FunctionDateOrDateTimeToDate : public IFunction {
public:
    static constexpr auto name = Impl<PType>::name;
    static FunctionPtr create() {
        return std::make_shared<FunctionDateOrDateTimeToDate<Impl, PType>>();
    }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeDateV2>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }

    //ATTN: no need to replace null value now because last_day and to_monday both process boundary case well.
    // may need to change if support more functions
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl<PType>::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

template <PrimitiveType PType>
struct LastDayImpl {
    static constexpr auto name = "last_day";
    using DateType = PrimitiveTypeTraits<PType>::DataType;
    using ColumnType = PrimitiveTypeTraits<PType>::ColumnType;
    using DateValueType = PrimitiveTypeTraits<PType>::CppType;
    using NativeType = PrimitiveTypeTraits<PType>::CppNativeType;

    constexpr static PrimitiveType ResultPType = PrimitiveType::TYPE_DATEV2;
    using ResultColumnType = PrimitiveTypeTraits<ResultPType>::ColumnType;
    using ResultNativeType = PrimitiveTypeTraits<ResultPType>::CppNativeType;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        const auto is_nullable = block.get_by_position(result).type->is_nullable();
        ColumnPtr res_column;
        ColumnPtr argument_column = remove_nullable(block.get_by_position(arguments[0]).column);
        if (is_nullable) {
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            auto data_col = assert_cast<const ColumnType*>(argument_column.get());
            res_column = ResultColumnType::create(input_rows_count);
            execute_straight(
                    input_rows_count, data_col->get_data(),
                    static_cast<ResultColumnType*>(res_column->assume_mutable().get())->get_data());

            block.replace_by_position(result,
                                      ColumnNullable::create(res_column, std::move(null_map)));
        } else {
            auto data_col = assert_cast<const ColumnType*>(argument_column.get());
            res_column = ResultColumnType::create(input_rows_count);
            execute_straight(
                    input_rows_count, data_col->get_data(),
                    static_cast<ResultColumnType*>(res_column->assume_mutable().get())->get_data());
            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }

    static void execute_straight(size_t input_rows_count,
                                 const PaddedPODArray<NativeType>& data_col,
                                 PaddedPODArray<ResultNativeType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            const auto& cur_data = data_col[i];
            auto ts_value = binary_cast<NativeType, DateValueType>(cur_data);
            if (!ts_value.is_valid_date()) {
                throw_out_of_bound_one_date<DateValueType>("last_day", cur_data);
            }
            int day = get_last_month_day(ts_value.year(), ts_value.month());
            // day is definitely legal
            if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
                ts_value.template unchecked_set_time_unit<TimeUnit::DAY>(day);
                res_data[i] = binary_cast<DateValueType, UInt32>(ts_value);
            } else { // datetimev2
                ts_value.template unchecked_set_time_unit<TimeUnit::DAY>(day);
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

template <PrimitiveType PType>
struct ToMondayImpl {
    static constexpr auto name = "to_monday";
    using DateType = PrimitiveTypeTraits<PType>::DataType;
    using ColumnType = PrimitiveTypeTraits<PType>::ColumnType;
    using DateValueType = PrimitiveTypeTraits<PType>::CppType;
    using NativeType = PrimitiveTypeTraits<PType>::CppNativeType;

    constexpr static PrimitiveType ResultPType = PrimitiveType::TYPE_DATEV2;
    using ResultColumnType = PrimitiveTypeTraits<ResultPType>::ColumnType;
    using ResultNativeType = PrimitiveTypeTraits<ResultPType>::CppNativeType;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        const auto is_nullable = block.get_by_position(result).type->is_nullable();
        ColumnPtr argument_column = remove_nullable(block.get_by_position(arguments[0]).column);
        ColumnPtr res_column;
        if (is_nullable) {
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            auto data_col = assert_cast<const ColumnType*>(argument_column.get());
            res_column = ResultColumnType::create(input_rows_count);
            execute_straight(
                    input_rows_count, data_col->get_data(),
                    static_cast<ResultColumnType*>(res_column->assume_mutable().get())->get_data());

            block.replace_by_position(result,
                                      ColumnNullable::create(res_column, std::move(null_map)));
        } else {
            auto data_col = assert_cast<const ColumnType*>(argument_column.get());
            res_column = ResultColumnType::create(input_rows_count);
            execute_straight(
                    input_rows_count, data_col->get_data(),
                    static_cast<ResultColumnType*>(res_column->assume_mutable().get())->get_data());
            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }

    // v1, throws on invalid date
    static void execute_straight(size_t input_rows_count,
                                 const PaddedPODArray<NativeType>& data_col,
                                 PaddedPODArray<ResultNativeType>& res_data) {
        for (int i = 0; i < input_rows_count; i++) {
            const auto& cur_data = data_col[i];
            auto ts_value = binary_cast<NativeType, DateValueType>(cur_data);
            if (!ts_value.is_valid_date()) [[unlikely]] {
                throw_out_of_bound_one_date<DateValueType>("to_monday", cur_data);
            }
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
            } else { // datetimev2
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

    size_t get_number_of_arguments() const override { return Impl::get_number_of_arguments(); }

    bool is_variadic() const override { return Impl::is_variadic(); }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    bool skip_return_type_check() const override {
        if constexpr (requires { Impl::skip_return_type_check(); }) {
            // for str_to_date now, it's very special
            return Impl::skip_return_type_check();
        } else {
            return false;
        }
    }

    bool use_default_implementation_for_nulls() const override {
        if constexpr (requires { Impl::use_default_implementation_for_nulls(); }) {
            return Impl::use_default_implementation_for_nulls();
        } else {
            return true;
        }
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if constexpr (std::is_same_v<Impl, DateTrunc<TYPE_DATEV2, true>> ||
                      std::is_same_v<Impl, DateTrunc<TYPE_DATETIMEV2, true>> ||
                      std::is_same_v<Impl, DateTrunc<TYPE_DATEV2, false>> ||
                      std::is_same_v<Impl, DateTrunc<TYPE_DATETIMEV2, false>>) {
            return Impl::open(context, scope);
        } else {
            return Status::OK();
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute(context, block, arguments, result, input_rows_count);
    }
};

struct FromIso8601DateV2 {
    static constexpr auto name = "from_iso8601_date";

    static size_t get_number_of_arguments() { return 1; }

    static bool is_variadic() { return false; }

    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeString>()}; }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeDateV2>();
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count) {
        const auto* src_column_ptr = block.get_by_position(arguments[0]).column.get();

        ColumnDateV2::MutablePtr res = ColumnDateV2::create(input_rows_count);
        auto& result_data = res->get_data();

        static const std::tuple<std::vector<int>, int, std::string> ISO_STRING_FORMAT[] = {
                {{
                         8,
                 },
                 1,
                 "%04d%02d%02d"},                         //YYYYMMDD
                {{4, -1, 2, -1, 2}, 1, "%04d-%02d-%02d"}, //YYYY-MM-DD
                {{4, -1, 2}, 2, "%04d-%02d"},             //YYYY-MM
                {
                        {
                                4,
                        },
                        3,
                        "%04d",
                }, //YYYY
                {
                        {4, -1, 3},
                        4,
                        "%04d-%03d",
                }, //YYYY-DDD
                {
                        {
                                7,
                        },
                        4,
                        "%04d%03d",
                }, //YYYYDDD
                {
                        {4, -1, -2, 2},
                        5,
                        "%04d-W%02d",
                }, //YYYY-Www
                {
                        {4, -2, 2},
                        5,
                        "%04dW%02d",
                }, //YYYYWww
                {
                        {4, -1, -2, 2, -1, 1},
                        6,
                        "%04d-W%02d-%1d",
                }, //YYYY-Www-D
                {
                        {4, -2, 3},
                        6,
                        "%04dW%02d%1d",
                }, //YYYYWwwD
        };

        for (size_t i = 0; i < input_rows_count; ++i) {
            int year, month, day, week, day_of_year;
            int weekday = 1; // YYYYWww  YYYY-Www  default D = 1
            auto src_string = src_column_ptr->get_data_at(i).to_string_view();

            int iso_string_format_value = 0;

            std::vector<int> src_string_values;
            src_string_values.reserve(10);

            //The maximum length of the current iso8601 format is 10.
            if (src_string.size() <= 10) {
                // The calculation string corresponds to the iso8601 format.
                // The integer represents the number of consecutive numbers.
                // -1 represent char '-'.
                // -2 represent char 'W'.
                //  The calculated vector `src_string_values`  will be compared with `ISO_STRING_FORMAT[]` later.
                for (int idx = 0; idx < src_string.size();) {
                    char current = src_string[idx];
                    if (current == '-') {
                        src_string_values.emplace_back(-1);
                        idx++;
                        continue;
                    } else if (current == 'W') {
                        src_string_values.emplace_back(-2);
                        idx++;
                        continue;
                    } else if (!isdigit(current)) {
                        iso_string_format_value = -1;
                        break;
                    }
                    int currLen = 0;
                    for (; idx < src_string.size() && isdigit(src_string[idx]); ++idx) {
                        ++currLen;
                    }
                    src_string_values.emplace_back(currLen);
                }
            } else {
                iso_string_format_value = -1;
            }

            std::string_view iso_format_string;
            if (iso_string_format_value != -1) {
                for (const auto& j : ISO_STRING_FORMAT) {
                    const auto& v = std::get<0>(j);
                    if (v == src_string_values) {
                        iso_string_format_value = std::get<1>(j);
                        iso_format_string = std::get<2>(j);
                        break;
                    }
                }
            }

            auto& ts_value = *reinterpret_cast<DateV2Value<DateV2ValueType>*>(&result_data[i]);
            if (iso_string_format_value == 1) {
                if (sscanf(src_string.data(), iso_format_string.data(), &year, &month, &day) != 3)
                        [[unlikely]] {
                    throw_invalid_string(name, src_string);
                }

                if (!(ts_value.template set_time_unit<YEAR>(year) &&
                      ts_value.template set_time_unit<MONTH>(month) &&
                      ts_value.template set_time_unit<DAY>(day))) [[unlikely]] {
                    throw_invalid_string(name, src_string);
                }
            } else if (iso_string_format_value == 2) {
                if (sscanf(src_string.data(), iso_format_string.data(), &year, &month) != 2)
                        [[unlikely]] {
                    throw_invalid_string(name, src_string);
                }

                if (!(ts_value.template set_time_unit<YEAR>(year) &&
                      ts_value.template set_time_unit<MONTH>(month))) [[unlikely]] {
                    throw_invalid_string(name, src_string);
                }
                ts_value.template unchecked_set_time_unit<DAY>(1);
            } else if (iso_string_format_value == 3) {
                if (sscanf(src_string.data(), iso_format_string.data(), &year) != 1) [[unlikely]] {
                    throw_invalid_string(name, src_string);
                }

                if (!ts_value.template set_time_unit<YEAR>(year)) [[unlikely]] {
                    throw_invalid_string(name, src_string);
                }
                ts_value.template unchecked_set_time_unit<MONTH>(1);
                ts_value.template unchecked_set_time_unit<DAY>(1);

            } else if (iso_string_format_value == 5 || iso_string_format_value == 6) {
                if (iso_string_format_value == 5) {
                    if (sscanf(src_string.data(), iso_format_string.data(), &year, &week) != 2)
                            [[unlikely]] {
                        throw_invalid_string(name, src_string);
                    }
                } else {
                    if (sscanf(src_string.data(), iso_format_string.data(), &year, &week,
                               &weekday) != 3) [[unlikely]] {
                        throw_invalid_string(name, src_string);
                    }
                }
                // weekday [1,7]    week [1,53]
                if (weekday < 1 || weekday > 7 || week < 1 || week > 53) [[unlikely]] {
                    throw_invalid_string(name, src_string);
                }

                auto first_day_of_week = getFirstDayOfISOWeek(year);
                ts_value.template unchecked_set_time_unit<YEAR>(
                        first_day_of_week.year().operator int());
                ts_value.template unchecked_set_time_unit<MONTH>(
                        first_day_of_week.month().operator unsigned int());
                ts_value.template unchecked_set_time_unit<DAY>(
                        first_day_of_week.day().operator unsigned int());

                auto day_diff = (week - 1) * 7 + weekday - 1;
                TimeInterval interval(DAY, day_diff, false);
                ts_value.date_add_interval<DAY>(interval);
            } else if (iso_string_format_value == 4) {
                if (sscanf(src_string.data(), iso_format_string.data(), &year, &day_of_year) != 2)
                        [[unlikely]] {
                    throw_invalid_string(name, src_string);
                }

                if (is_leap(year)) {
                    if (day_of_year < 0 || day_of_year > 366) [[unlikely]] {
                        throw_invalid_string(name, src_string);
                    }
                } else {
                    if (day_of_year < 0 || day_of_year > 365) [[unlikely]] {
                        throw_invalid_string(name, src_string);
                    }
                }
                ts_value.template unchecked_set_time_unit<YEAR>(year);
                ts_value.template unchecked_set_time_unit<MONTH>(1);
                ts_value.template unchecked_set_time_unit<DAY>(1);
                TimeInterval interval(DAY, day_of_year - 1, false);
                ts_value.template date_add_interval<DAY>(interval);
            } else {
                throw_invalid_string(name, src_string);
            }
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

private:
    //Get the date corresponding to Monday of the first week of the year according to the ISO8601 standard.
    static std::chrono::year_month_day getFirstDayOfISOWeek(int year) {
        using namespace std::chrono;
        auto jan4 = year_month_day {std::chrono::year(year) / January / 4};
        auto jan4_sys_days = sys_days {jan4};
        auto weekday_of_jan4 = weekday {jan4_sys_days};
        auto first_day_of_week = jan4_sys_days - days {(weekday_of_jan4.iso_encoding() - 1)};
        return year_month_day {floor<days>(first_day_of_week)};
    }
};

using FunctionStrToDate = FunctionOtherTypesToDateType<StrToDate>;
using FunctionStrToDatetime = FunctionOtherTypesToDateType<StrToDate>;

using FunctionMakeDate = FunctionOtherTypesToDateType<MakeDateImpl>;

using FunctionDateTruncDateV2 = FunctionOtherTypesToDateType<DateTrunc<TYPE_DATEV2, true>>;
using FunctionDateTruncDatetimeV2 = FunctionOtherTypesToDateType<DateTrunc<TYPE_DATETIMEV2, true>>;
using FunctionDateTruncDateV2WithCommonOrder =
        FunctionOtherTypesToDateType<DateTrunc<TYPE_DATEV2, false>>;
using FunctionDateTruncDatetimeV2WithCommonOrder =
        FunctionOtherTypesToDateType<DateTrunc<TYPE_DATETIMEV2, false>>;
using FunctionFromIso8601DateV2 = FunctionOtherTypesToDateType<FromIso8601DateV2>;

void register_function_timestamp(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStrToDate>();
    factory.register_function<FunctionStrToDatetime>();
    factory.register_function<FunctionMakeDate>();
    factory.register_function<FromDays>();
    factory.register_function<FunctionDateTruncDateV2>();
    factory.register_function<FunctionDateTruncDatetimeV2>();
    factory.register_function<FunctionDateTruncDateV2WithCommonOrder>();
    factory.register_function<FunctionDateTruncDatetimeV2WithCommonOrder>();
    factory.register_function<FunctionFromIso8601DateV2>();

    factory.register_function<FunctionUnixTimestamp<UnixTimeStampImpl<>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDate>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateV2>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateTime>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateTimeV2>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampStrImpl<>>>();
    factory.register_function<FunctionUnixTimestampNew<UnixTimeStampImpl<true>>>();
    factory.register_function<
            FunctionUnixTimestampNew<UnixTimeStampDateImpl<DataTypeDate, true>>>();
    factory.register_function<
            FunctionUnixTimestampNew<UnixTimeStampDateImpl<DataTypeDateV2, true>>>();
    factory.register_function<
            FunctionUnixTimestampNew<UnixTimeStampDateImpl<DataTypeDateTime, true>>>();
    factory.register_function<
            FunctionUnixTimestampNew<UnixTimeStampDateImpl<DataTypeDateTimeV2, true>>>();
    factory.register_function<FunctionUnixTimestampNew<UnixTimeStampStrImpl<true>>>();

    factory.register_function<FunctionDateOrDateTimeToDate<LastDayImpl, TYPE_DATEV2>>();
    factory.register_function<FunctionDateOrDateTimeToDate<LastDayImpl, TYPE_DATETIMEV2>>();
    factory.register_function<FunctionDateOrDateTimeToDate<ToMondayImpl, TYPE_DATEV2>>();
    factory.register_function<FunctionDateOrDateTimeToDate<ToMondayImpl, TYPE_DATETIMEV2>>();

    factory.register_function<DateTimeToTimestamp<MicroSec>>();
    factory.register_function<DateTimeToTimestamp<MilliSec>>();
    factory.register_function<DateTimeToTimestamp<Sec>>();
}
#include "common/compile_check_avoid_end.h"
} // namespace doris::vectorized
