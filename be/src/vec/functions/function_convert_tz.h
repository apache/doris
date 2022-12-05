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

#pragma once

#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_string.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

struct ConvertTzCtx {
    std::map<std::string, cctz::time_zone> time_zone_cache;
};

template <typename DateValueType, typename ArgType>
struct ConvertTZImpl {
    using ColumnType = std::conditional_t<
            std::is_same_v<DateV2Value<DateV2ValueType>, DateValueType>, ColumnDateV2,
            std::conditional_t<std::is_same_v<DateV2Value<DateTimeV2ValueType>, DateValueType>,
                               ColumnDateTimeV2, ColumnDateTime>>;
    using NativeType = std::conditional_t<
            std::is_same_v<DateV2Value<DateV2ValueType>, DateValueType>, UInt32,
            std::conditional_t<std::is_same_v<DateV2Value<DateTimeV2ValueType>, DateValueType>,
                               UInt64, Int64>>;
    using ReturnDateType = std::conditional_t<std::is_same_v<VecDateTimeValue, DateValueType>,
                                              VecDateTimeValue, DateV2Value<DateTimeV2ValueType>>;
    using ReturnNativeType =
            std::conditional_t<std::is_same_v<VecDateTimeValue, DateValueType>, Int64, UInt64>;
    using ReturnColumnType = std::conditional_t<std::is_same_v<VecDateTimeValue, DateValueType>,
                                                ColumnDateTime, ColumnDateTimeV2>;

    static void execute(FunctionContext* context, const ColumnType* date_column,
                        const ColumnString* from_tz_column, const ColumnString* to_tz_column,
                        ReturnColumnType* result_column, NullMap& result_null_map,
                        size_t input_rows_count) {
        auto convert_ctx = reinterpret_cast<ConvertTzCtx*>(
                context->get_function_state(FunctionContext::FunctionStateScope::THREAD_LOCAL));
        std::map<std::string, cctz::time_zone> time_zone_cache_;
        auto& time_zone_cache = convert_ctx ? convert_ctx->time_zone_cache : time_zone_cache_;
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }

            auto from_tz = from_tz_column->get_data_at(i).to_string();
            auto to_tz = to_tz_column->get_data_at(i).to_string();

            DateValueType ts_value =
                    binary_cast<NativeType, DateValueType>(date_column->get_element(i));
            int64_t timestamp;

            if (time_zone_cache.find(from_tz) == time_zone_cache.cend()) {
                if (!TimezoneUtils::find_cctz_time_zone(from_tz, time_zone_cache[from_tz])) {
                    result_null_map[i] = true;
                    result_column->insert_default();
                    continue;
                }
            }

            if (time_zone_cache.find(to_tz) == time_zone_cache.cend()) {
                if (!TimezoneUtils::find_cctz_time_zone(to_tz, time_zone_cache[to_tz])) {
                    result_null_map[i] = true;
                    result_column->insert_default();
                    continue;
                }
            }

            if (!ts_value.unix_timestamp(&timestamp, time_zone_cache[from_tz])) {
                result_null_map[i] = true;
                result_column->insert_default();
                continue;
            }

            ReturnDateType ts_value2;
            if (!ts_value2.from_unixtime(timestamp, time_zone_cache[to_tz])) {
                result_null_map[i] = true;
                result_column->insert_default();
                continue;
            }

            result_column->insert(binary_cast<ReturnDateType, ReturnNativeType>(ts_value2));
        }
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<ArgType>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>()};
    }
};

template <typename Transform, typename DateType>
class FunctionConvertTZ : public IFunction {
public:
    static constexpr auto name = "convert_tz";

    static FunctionPtr create() { return std::make_shared<FunctionConvertTZ>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if constexpr (std::is_same_v<DateType, DataTypeDateTime> ||
                      std::is_same_v<DateType, DataTypeDate>) {
            return make_nullable(std::make_shared<DataTypeDateTime>());
        } else {
            return make_nullable(std::make_shared<DataTypeDateTimeV2>());
        }
    }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        return Transform::get_variadic_argument_types();
    }

    bool use_default_implementation_for_constants() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope != FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        context->set_function_state(scope, new ConvertTzCtx);
        return Status::OK();
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            auto* convert_ctx = reinterpret_cast<ConvertTzCtx*>(
                    context->get_function_state(FunctionContext::THREAD_LOCAL));
            delete convert_ctx;
            context->set_function_state(FunctionContext::THREAD_LOCAL, nullptr);
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);

        ColumnPtr argument_columns[3];

        for (int i = 0; i < 3; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(result_null_map_column->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        if constexpr (std::is_same_v<DateType, DataTypeDateTime> ||
                      std::is_same_v<DateType, DataTypeDate>) {
            auto result_column = ColumnDateTime::create();
            Transform::execute(context,
                               assert_cast<const ColumnDateTime*>(argument_columns[0].get()),
                               assert_cast<const ColumnString*>(argument_columns[1].get()),
                               assert_cast<const ColumnString*>(argument_columns[2].get()),
                               assert_cast<ColumnDateTime*>(result_column.get()),
                               assert_cast<ColumnUInt8*>(result_null_map_column.get())->get_data(),
                               input_rows_count);
            block.get_by_position(result).column = ColumnNullable::create(
                    std::move(result_column), std::move(result_null_map_column));
        } else if constexpr (std::is_same_v<DateType, DataTypeDateV2>) {
            auto result_column = ColumnDateTimeV2::create();
            Transform::execute(context, assert_cast<const ColumnDateV2*>(argument_columns[0].get()),
                               assert_cast<const ColumnString*>(argument_columns[1].get()),
                               assert_cast<const ColumnString*>(argument_columns[2].get()),
                               assert_cast<ColumnDateTimeV2*>(result_column.get()),
                               assert_cast<ColumnUInt8*>(result_null_map_column.get())->get_data(),
                               input_rows_count);
            block.get_by_position(result).column = ColumnNullable::create(
                    std::move(result_column), std::move(result_null_map_column));
        } else {
            auto result_column = ColumnDateTimeV2::create();
            Transform::execute(context,
                               assert_cast<const ColumnDateTimeV2*>(argument_columns[0].get()),
                               assert_cast<const ColumnString*>(argument_columns[1].get()),
                               assert_cast<const ColumnString*>(argument_columns[2].get()),
                               assert_cast<ColumnDateTimeV2*>(result_column.get()),
                               assert_cast<ColumnUInt8*>(result_null_map_column.get())->get_data(),
                               input_rows_count);
            block.get_by_position(result).column = ColumnNullable::create(
                    std::move(result_column), std::move(result_null_map_column));
        }

        return Status::OK();
    }
};

} // namespace doris::vectorized
