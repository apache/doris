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
#include <stddef.h>

#include <boost/iterator/iterator_facade.hpp>
// IWYU pragma: no_include <bits/chrono.h>
#include <algorithm>
#include <chrono> // IWYU pragma: keep
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

class FunctionSleep : public IFunction {
public:
    static constexpr auto name = "sleep";
    static FunctionPtr create() { return std::make_shared<FunctionSleep>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments[0]->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeUInt8>());
        }
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    // Sleep function should not be executed during open stage, this will makes fragment prepare
    // waiting too long, so we do not use default impl.
    bool use_default_implementation_for_constants() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& argument_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();

        auto res_column = ColumnUInt8::create(input_rows_count, 0);

        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*argument_column)) {
            auto null_map_column = ColumnUInt8::create();

            auto nested_column = nullable_column->get_nested_column_ptr();
            auto data_column = assert_cast<const ColumnInt32*>(nested_column.get());

            for (int i = 0; i < input_rows_count; i++) {
                if (nullable_column->is_null_at(i)) {
                    null_map_column->insert(Field::create_field<TYPE_BOOLEAN>(1));
                } else {
                    int seconds = data_column->get_data()[i];
                    std::this_thread::sleep_for(std::chrono::seconds(seconds));
                    null_map_column->insert(Field::create_field<TYPE_BOOLEAN>(0));
                }
            }

            block.replace_by_position(result, ColumnNullable::create(std::move(res_column),
                                                                     std::move(null_map_column)));
        } else {
            auto data_column = assert_cast<const ColumnInt32*>(argument_column.get());

            for (int i = 0; i < input_rows_count; i++) {
                int seconds = data_column->get_element(i);
                std::this_thread::sleep_for(std::chrono::seconds(seconds));
            }

            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }
};

class FunctionVersion : public IFunction {
public:
    static constexpr auto name = "version";

    static const std::string version;

    static FunctionPtr create() { return std::make_shared<FunctionVersion>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_column = ColumnString::create();
        res_column->insert_data(version.c_str(), version.length());
        auto col_const = ColumnConst::create(std::move(res_column), input_rows_count);
        block.replace_by_position(result, std::move(col_const));
        return Status::OK();
    }
};

class FunctionHumanReadableSeconds : public IFunction {
public:
    static constexpr auto name = "human_readable_seconds";

    static FunctionPtr create() { return std::make_shared<FunctionHumanReadableSeconds>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& argument_column = block.get_by_position(arguments[0]).column;
        const auto* data_column = check_and_get_column<ColumnFloat64>(*argument_column);
        if (data_column == nullptr) {
            return Status::InvalidArgument("Illegal column {} of first argument of function {}",
                                           argument_column->get_name(), name);
        }

        auto result_column = ColumnString::create();
        result_column->reserve(input_rows_count);
        std::string buffer;
        for (size_t i = 0; i < input_rows_count; ++i) {
            double value = data_column->get_element(i);
            if (std::isnan(value) || std::isinf(value)) {
                return Status::InvalidArgument("Invalid argument value {} for function {}", value,
                                               name);
            }
            buffer.clear();
            to_human_readable(value, buffer);
            result_column->insert_data(buffer.data(), buffer.size());
        }

        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

private:
    static void append_unit(std::string& out, int64_t value, const char* singular,
                            const char* plural) {
        if (!out.empty()) {
            out += ", ";
        }
        out += std::to_string(value);
        out += ' ';
        out += (value == 1 ? singular : plural);
    }

    static void to_human_readable(double seconds, std::string& out) {
        // Match Presto/Trino: round to whole seconds and ignore the sign.
        // Saturate at int64_t max for very large finite inputs. This both matches the
        // FE constant-folding path (Java Math.round saturates to Long.MAX_VALUE) and
        // avoids std::llround's domain error when the rounded value exceeds int64_t.
        double abs_seconds = std::fabs(seconds);
        int64_t remain;
        if (abs_seconds >= static_cast<double>(std::numeric_limits<int64_t>::max())) {
            remain = std::numeric_limits<int64_t>::max();
        } else {
            remain = std::llround(abs_seconds);
        }

        constexpr int64_t WEEK = 7 * 24 * 60 * 60;
        constexpr int64_t DAY = 24 * 60 * 60;
        constexpr int64_t HOUR = 60 * 60;
        constexpr int64_t MINUTE = 60;

        const int64_t weeks = remain / WEEK;
        remain %= WEEK;
        const int64_t days = remain / DAY;
        remain %= DAY;
        const int64_t hours = remain / HOUR;
        remain %= HOUR;
        const int64_t minutes = remain / MINUTE;
        const int64_t secs = remain % MINUTE;

        if (weeks > 0) {
            append_unit(out, weeks, "week", "weeks");
        }
        if (days > 0) {
            append_unit(out, days, "day", "days");
        }
        if (hours > 0) {
            append_unit(out, hours, "hour", "hours");
        }
        if (minutes > 0) {
            append_unit(out, minutes, "minute", "minutes");
        }
        if (secs > 0 || out.empty()) {
            append_unit(out, secs, "second", "seconds");
        }
    }
};

const std::string FunctionVersion::version = "5.7.99";

void register_function_utility(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSleep>();
    factory.register_function<FunctionVersion>();
    factory.register_function<FunctionHumanReadableSeconds>();
}

} // namespace doris
