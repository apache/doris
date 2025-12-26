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

#include <cctz/time_zone.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "util/binary_cast.hpp"
#include "util/timezone_utils.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/datetime_errors.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct ConvertTzState {
    bool use_state = false;
    bool is_valid = false;
    cctz::time_zone from_tz;
    cctz::time_zone to_tz;
};

class FunctionConvertTZ : public IFunction {
    constexpr static PrimitiveType PType = PrimitiveType::TYPE_DATETIMEV2;
    using DateValueType = PrimitiveTypeTraits<PType>::CppType;
    using ColumnType = PrimitiveTypeTraits<PType>::ColumnType;
    using NativeType = PrimitiveTypeTraits<PType>::CppNativeType;

public:
    static constexpr auto name = "convert_tz";

    static FunctionPtr create() { return std::make_shared<FunctionConvertTZ>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return have_nullable(arguments) ? make_nullable(std::make_shared<DataTypeDateTimeV2>())
                                        : std::make_shared<DataTypeDateTimeV2>();
    }

    // default value of timezone is invalid, should skip to avoid wrong exception
    bool use_default_implementation_for_nulls() const override { return false; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        std::shared_ptr<ConvertTzState> state = std::make_shared<ConvertTzState>();

        context->set_function_state(scope, state);
        DCHECK_EQ(context->get_num_args(), 3);
        const auto* const_from_tz = context->get_constant_col(1);
        const auto* const_to_tz = context->get_constant_col(2);

        // ConvertTzState is used only when both the second and third parameters are constants
        if (const_from_tz != nullptr && const_to_tz != nullptr) {
            state->use_state = true;
            init_convert_tz_state(state, const_from_tz, const_to_tz);
        } else {
            state->use_state = false;
        }

        return IFunction::open(context, scope);
    }

    void init_convert_tz_state(std::shared_ptr<ConvertTzState> state,
                               const ColumnPtrWrapper* const_from_tz,
                               const ColumnPtrWrapper* const_to_tz) {
        auto const_data_from_tz = const_from_tz->column_ptr->get_data_at(0);
        auto const_data_to_tz = const_to_tz->column_ptr->get_data_at(0);

        // from_tz and to_tz must both be non-null.
        if (const_data_from_tz.data == nullptr || const_data_to_tz.data == nullptr) {
            state->is_valid = false;
            return;
        }

        auto from_tz_name = const_data_from_tz.to_string();
        auto to_tz_name = const_data_to_tz.to_string();

        if (!TimezoneUtils::find_cctz_time_zone(from_tz_name, state->from_tz)) [[unlikely]] {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} invalid timezone: {}", name,
                            from_tz_name);
        }
        if (!TimezoneUtils::find_cctz_time_zone(to_tz_name, state->to_tz)) [[unlikely]] {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} invalid timezone: {}", name,
                            to_tz_name);
        }
        state->is_valid = true;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto* convert_tz_state = reinterpret_cast<ConvertTzState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!convert_tz_state) {
            return Status::RuntimeError(
                    "funciton context for function '{}' must have ConvertTzState;", get_name());
        }

        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);
        NullMap& result_null_map = assert_cast<ColumnUInt8&>(*result_null_map_column).get_data();

        ColumnPtr argument_columns[3];
        bool col_const[3];

        // calculate result null map and col_const
        for (int i = 0; i < 3; ++i) {
            ColumnPtr& col = block.get_by_position(arguments[i]).column;
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
        default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block, arguments);
        argument_columns[1] = remove_nullable(argument_columns[1]);
        argument_columns[2] = remove_nullable(argument_columns[2]);

        auto result_column = ColumnType::create();
        if (convert_tz_state->use_state) {
            // ignore argument columns, use cached timezone input in state
            execute_tz_const_with_state(convert_tz_state,
                                        assert_cast<const ColumnType*>(argument_columns[0].get()),
                                        assert_cast<ColumnType*>(result_column.get()),
                                        result_null_map, input_rows_count);
        } else if (col_const[1] && col_const[2]) {
            // arguments are const
            execute_tz_const(context, assert_cast<const ColumnType*>(argument_columns[0].get()),
                             assert_cast<const ColumnString*>(argument_columns[1].get()),
                             assert_cast<const ColumnString*>(argument_columns[2].get()),
                             assert_cast<ColumnType*>(result_column.get()), result_null_map,
                             input_rows_count);
        } else {
            _execute(context, assert_cast<const ColumnType*>(argument_columns[0].get()),
                     assert_cast<const ColumnString*>(argument_columns[1].get()),
                     assert_cast<const ColumnString*>(argument_columns[2].get()),
                     assert_cast<ColumnType*>(result_column.get()), result_null_map,
                     input_rows_count);
        } //if const

        if (block.get_data_type(result)->is_nullable()) {
            block.get_by_position(result).column = ColumnNullable::create(
                    std::move(result_column), std::move(result_null_map_column));
        } else {
            block.get_by_position(result).column = std::move(result_column);
        }
        return Status::OK();
    }

private:
    static void _execute(FunctionContext* context, const ColumnType* date_column,
                         const ColumnString* from_tz_column, const ColumnString* to_tz_column,
                         ColumnType* result_column, NullMap& result_null_map,
                         size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }
            auto from_tz = from_tz_column->get_data_at(i).to_string();
            auto to_tz = to_tz_column->get_data_at(i).to_string();
            execute_inner_loop(date_column, from_tz, to_tz, result_column, result_null_map, i);
        }
    }

    static void execute_tz_const_with_state(ConvertTzState* convert_tz_state,
                                            const ColumnType* date_column,
                                            ColumnType* result_column, NullMap& result_null_map,
                                            size_t input_rows_count) {
        cctz::time_zone& from_tz = convert_tz_state->from_tz;
        cctz::time_zone& to_tz = convert_tz_state->to_tz;
        auto push_null = [&](size_t row) {
            result_null_map[row] = true;
            result_column->insert_default();
        };
        // state isn't valid means there's NULL in timezone input. so return null rather than exception
        if (!convert_tz_state->is_valid) [[unlikely]] {
            // If an invalid timezone is present, return null
            for (size_t i = 0; i < input_rows_count; i++) {
                push_null(i);
            }
            return;
        }
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }

            DateValueType ts_value =
                    binary_cast<NativeType, DateValueType>(date_column->get_element(i));
            DateValueType ts_value2;

            std::pair<int64_t, int64_t> timestamp;
            if (!ts_value.unix_timestamp(&timestamp, from_tz)) [[unlikely]] {
                throw_invalid_string("convert_tz", from_tz.name());
            }
            ts_value2.from_unixtime(timestamp, to_tz);

            if (!ts_value2.is_valid_date()) [[unlikely]] {
                throw_out_of_bound_convert_tz<DateValueType>(date_column->get_element(i),
                                                             from_tz.name(), to_tz.name());
            }

            result_column->insert(Field::create_field<TYPE_DATETIMEV2>(
                    binary_cast<DateValueType, NativeType>(ts_value2)));
        }
    }

    static void execute_tz_const(FunctionContext* context, const ColumnType* date_column,
                                 const ColumnString* from_tz_column,
                                 const ColumnString* to_tz_column, ColumnType* result_column,
                                 NullMap& result_null_map, size_t input_rows_count) {
        auto from_tz = from_tz_column->get_data_at(0).to_string();
        auto to_tz = to_tz_column->get_data_at(0).to_string();
        cctz::time_zone from_zone, to_zone;
        if (!TimezoneUtils::find_cctz_time_zone(from_tz, from_zone)) [[unlikely]] {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} invalid timezone: {}", name,
                            from_tz);
        }
        if (!TimezoneUtils::find_cctz_time_zone(to_tz, to_zone)) [[unlikely]] {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} invalid timezone: {}", name,
                            to_tz);
        }
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }
            execute_inner_loop(date_column, from_tz, to_tz, result_column, result_null_map, i);
        }
    }

    static void execute_inner_loop(const ColumnType* date_column, const std::string& from_tz_name,
                                   const std::string& to_tz_name, ColumnType* result_column,
                                   NullMap& result_null_map, const size_t index_now) {
        DateValueType ts_value =
                binary_cast<NativeType, DateValueType>(date_column->get_element(index_now));
        cctz::time_zone from_tz {}, to_tz {};
        DateValueType ts_value2;

        if (!TimezoneUtils::find_cctz_time_zone(from_tz_name, from_tz)) [[unlikely]] {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} invalid timezone: {}", name,
                            from_tz_name);
        }
        if (!TimezoneUtils::find_cctz_time_zone(to_tz_name, to_tz)) [[unlikely]] {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} invalid timezone: {}", name,
                            to_tz_name);
        }

        std::pair<int64_t, int64_t> timestamp;
        if (!ts_value.unix_timestamp(&timestamp, from_tz)) {
            throw_invalid_string("convert_tz", from_tz.name());
        }
        ts_value2.from_unixtime(timestamp, to_tz);

        if (!ts_value2.is_valid_date()) [[unlikely]] {
            throw_out_of_bound_convert_tz<DateValueType>(date_column->get_element(index_now),
                                                         from_tz.name(), to_tz.name());
        }

        result_column->insert(Field::create_field<TYPE_DATETIMEV2>(
                binary_cast<DateValueType, NativeType>(ts_value2)));
    }
};

void register_function_convert_tz(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionConvertTZ>();
}

} // namespace doris::vectorized

#include "common/compile_check_end.h"
