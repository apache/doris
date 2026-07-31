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
#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/timestamp_ns_value.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/util.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/datetime_errors.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "util/timezone_utils.h"

namespace doris {

struct ConvertTzState {
    bool use_state = false;
    bool is_valid = false;
    cctz::time_zone from_tz;
    cctz::time_zone to_tz;
};

class FunctionConvertTZ : public IFunction {
public:
    static constexpr auto name = "convert_tz";

    static FunctionPtr create() { return std::make_shared<FunctionConvertTZ>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr result_type = std::make_shared<DataTypeDateTimeV2>();
        if (remove_nullable(arguments[0])->get_primitive_type() == TYPE_TIMESTAMP_NS) {
            result_type = std::make_shared<DataTypeTimeStampNs>();
        }
        return have_nullable(arguments) ? make_nullable(result_type) : result_type;
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
        NullMap& result_null_map = result_null_map_column->get_data();

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

        const auto argument_type =
                remove_nullable(block.get_by_position(arguments[0]).type)->get_primitive_type();
        if (argument_type == TYPE_TIMESTAMP_NS) {
            return execute_impl_typed<TYPE_TIMESTAMP_NS>(
                    context, block, result, input_rows_count, argument_columns, col_const,
                    convert_tz_state, std::move(result_null_map_column), result_null_map);
        }
        DORIS_CHECK_EQ(argument_type, TYPE_DATETIMEV2);
        return execute_impl_typed<TYPE_DATETIMEV2>(
                context, block, result, input_rows_count, argument_columns, col_const,
                convert_tz_state, std::move(result_null_map_column), result_null_map);
    }

private:
    template <PrimitiveType PType>
    static Status execute_impl_typed(FunctionContext* context, Block& block, uint32_t result,
                                     size_t input_rows_count,
                                     const ColumnPtr (&argument_columns)[3],
                                     const bool (&col_const)[3], ConvertTzState* convert_tz_state,
                                     MutableColumnPtr result_null_map_column,
                                     NullMap& result_null_map) {
        using ColumnType = PrimitiveTypeTraits<PType>::ColumnType;
        auto result_column = ColumnType::create();
        if (convert_tz_state->use_state) {
            // ignore argument columns, use cached timezone input in state
            execute_tz_const_with_state<PType>(
                    convert_tz_state, assert_cast<const ColumnType*>(argument_columns[0].get()),
                    result_column.get(), result_null_map, input_rows_count);
        } else if (col_const[1] && col_const[2]) {
            // arguments are const
            execute_tz_const<PType>(context,
                                    assert_cast<const ColumnType*>(argument_columns[0].get()),
                                    assert_cast<const ColumnString*>(argument_columns[1].get()),
                                    assert_cast<const ColumnString*>(argument_columns[2].get()),
                                    result_column.get(), result_null_map, input_rows_count);
        } else {
            execute<PType>(context, assert_cast<const ColumnType*>(argument_columns[0].get()),
                           assert_cast<const ColumnString*>(argument_columns[1].get()),
                           assert_cast<const ColumnString*>(argument_columns[2].get()),
                           result_column.get(), result_null_map, input_rows_count);
        } //if const

        if (block.get_data_type(result)->is_nullable()) {
            block.get_by_position(result).column = ColumnNullable::create(
                    std::move(result_column), std::move(result_null_map_column));
        } else {
            block.get_by_position(result).column = std::move(result_column);
        }
        return Status::OK();
    }

    template <PrimitiveType PType>
    static void execute(FunctionContext* context,
                        const typename PrimitiveTypeTraits<PType>::ColumnType* date_column,
                        const ColumnString* from_tz_column, const ColumnString* to_tz_column,
                        typename PrimitiveTypeTraits<PType>::ColumnType* result_column,
                        NullMap& result_null_map, size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }
            auto from_tz = from_tz_column->get_data_at(i).to_string();
            auto to_tz = to_tz_column->get_data_at(i).to_string();
            execute_inner_loop<PType>(date_column, from_tz, to_tz, result_column, result_null_map,
                                      i);
        }
    }

    template <typename DateValueType>
    static std::pair<int64_t, uint32_t> unix_timestamp_for_convert_tz(
            const DateValueType& ts_value, const cctz::time_zone& from_tz) {
        const auto civil_value = [&]() {
            if constexpr (std::is_same_v<DateValueType, TimeStampNsValue>) {
                return ts_value.to_datetime();
            } else {
                return ts_value;
            }
        }();
        cctz::civil_second civil_time(civil_value.year(), civil_value.month(), civil_value.day(),
                                      civil_value.hour(), civil_value.minute(),
                                      civil_value.second());
        const auto lookup = from_tz.lookup(civil_time);
        const bool skipped = lookup.kind == cctz::time_zone::civil_lookup::SKIPPED;
        const auto tp = skipped ? lookup.trans : lookup.pre;

        // Skipped civil times map to the transition instant. Do not keep the
        // input fractional part inside a local time interval that never existed.
        const uint32_t fraction = [&]() {
            if constexpr (std::is_same_v<DateValueType, TimeStampNsValue>) {
                return ts_value.nanosecond();
            }
            return ts_value.microsecond() * 1000;
        }();
        return {tp.time_since_epoch().count(), skipped ? 0 : fraction};
    }

    template <typename DateValueType>
    static bool convert_tz_value(const DateValueType& source, const cctz::time_zone& from_tz,
                                 const cctz::time_zone& to_tz, DateValueType& result) {
        const auto [seconds, nanoseconds] = unix_timestamp_for_convert_tz(source, from_tz);
        if constexpr (std::is_same_v<DateValueType, TimeStampNsValue>) {
            DateV2Value<DateTimeV2ValueType> datetime;
            datetime.from_unixtime({seconds, nanoseconds / 1000}, to_tz);
            return result.from_datetime(datetime, nanoseconds % 1000);
        } else {
            result.from_unixtime({seconds, nanoseconds / 1000}, to_tz);
            return result.is_valid_date();
        }
    }

    template <PrimitiveType PType>
    static void execute_tz_const_with_state(
            ConvertTzState* convert_tz_state,
            const typename PrimitiveTypeTraits<PType>::ColumnType* date_column,
            typename PrimitiveTypeTraits<PType>::ColumnType* result_column,
            NullMap& result_null_map, size_t input_rows_count) {
        using DateValueType = PrimitiveTypeTraits<PType>::CppType;
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

            DateValueType ts_value = date_column->get_element(i);
            DateValueType ts_value2;

            if (!convert_tz_value(ts_value, from_tz, to_tz, ts_value2)) [[unlikely]] {
                throw_out_of_bound_convert_tz<DateValueType>(date_column->get_element(i),
                                                             from_tz.name(), to_tz.name());
            }

            result_column->insert(Field::create_field<PType>(ts_value2));
        }
    }

    template <PrimitiveType PType>
    static void execute_tz_const(FunctionContext* context,
                                 const typename PrimitiveTypeTraits<PType>::ColumnType* date_column,
                                 const ColumnString* from_tz_column,
                                 const ColumnString* to_tz_column,
                                 typename PrimitiveTypeTraits<PType>::ColumnType* result_column,
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
            execute_inner_loop<PType>(date_column, from_tz, to_tz, result_column, result_null_map,
                                      i);
        }
    }

    template <PrimitiveType PType>
    static void execute_inner_loop(
            const typename PrimitiveTypeTraits<PType>::ColumnType* date_column,
            const std::string& from_tz_name, const std::string& to_tz_name,
            typename PrimitiveTypeTraits<PType>::ColumnType* result_column,
            NullMap& result_null_map, const size_t index_now) {
        using DateValueType = PrimitiveTypeTraits<PType>::CppType;
        DateValueType ts_value = date_column->get_element(index_now);
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

        if (!convert_tz_value(ts_value, from_tz, to_tz, ts_value2)) [[unlikely]] {
            throw_out_of_bound_convert_tz<DateValueType>(date_column->get_element(index_now),
                                                         from_tz.name(), to_tz.name());
        }

        result_column->insert(Field::create_field<PType>(ts_value2));
    }
};

void register_function_convert_tz(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionConvertTZ>();
}

} // namespace doris
