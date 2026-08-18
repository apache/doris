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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/primitive_type.h"
#include "core/value/timestamptz_value.h"
#include "exprs/function_context.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {
constexpr int64_t SECONDS_PER_HOUR = 3600;
constexpr int64_t SECONDS_PER_MINUTE = 60;

// TIMESTAMPTZ values are stored as UTC instants without the input zone, so the
// offset extracted here is the offset of the session time zone at the instant.
// See TimestampTzValue for the storage design.
Status execute_timezone_offset_part(FunctionContext* context, Block& block,
                                    const ColumnNumbers& arguments, uint32_t result,
                                    size_t input_rows_count, bool extract_hour) {
    ColumnPtr col = block.get_by_position(arguments[0]).column;
    // Unwrap nullable and const wrappers in any nesting order so that
    // ColumnNullable(ColumnConst(...)) and ColumnConst(ColumnNullable(...))
    // inputs both reach the plain ColumnTimeStampTz data below.
    col = remove_nullable(col);
    if (is_column_const(*col)) {
        col = assert_cast<const ColumnConst&>(*col).convert_to_full_column();
        col = remove_nullable(col);
    }
    const auto* tz_column = assert_cast<const ColumnTimeStampTz*>(col.get());
    const auto& tz_data = tz_column->get_data();

    auto result_column = ColumnInt64::create();
    auto& result_data = result_column->get_data();
    result_data.resize(input_rows_count);

    const cctz::time_zone& timezone = context->state()->timezone_obj();
    for (size_t i = 0; i < input_rows_count; ++i) {
        int64_t offset = tz_data[i].utc_offset(timezone);
        result_data[i] = extract_hour ? offset / SECONDS_PER_HOUR
                                      : (offset % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE;
    }

    block.get_by_position(result).column = std::move(result_column);
    return Status::OK();
}
} // namespace

class FunctionTimezoneHour : public IFunction {
public:
    static constexpr auto name = "timezone_hour";

    static FunctionPtr create() { return std::make_shared<FunctionTimezoneHour>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return execute_timezone_offset_part(context, block, arguments, result, input_rows_count,
                                            true);
    }
};

class FunctionTimezoneMinute : public IFunction {
public:
    static constexpr auto name = "timezone_minute";

    static FunctionPtr create() { return std::make_shared<FunctionTimezoneMinute>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return execute_timezone_offset_part(context, block, arguments, result, input_rows_count,
                                            false);
    }
};

void register_function_timezone_hour_minute(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionTimezoneHour>();
    factory.register_function<FunctionTimezoneMinute>();
}

} // namespace doris
