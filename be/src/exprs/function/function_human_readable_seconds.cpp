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

#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

class FunctionHumanReadableSeconds : public IFunction {
public:
    static constexpr auto name = "human_readable_seconds";
    static FunctionPtr create() { return std::make_shared<FunctionHumanReadableSeconds>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    // We must return false here to manually handle both input nulls and computed nulls
    // (negative values -> NULL). If default null implementation is used, it strips nulls
    // before execution, conflicting with our explicit output null map creation.
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& col_with_type = block.get_by_position(arguments[0]);
        const auto* source_col = col_with_type.column.get();

        const NullMap* null_map = nullptr;
        const IColumn* actual_col = source_col;

        // Explicitly unnest ColumnNullable to handle NULL inputs correctly
        if (source_col->is_nullable()) {
            const auto* nullable_col = assert_cast<const ColumnNullable*>(source_col);
            actual_col = nullable_col->get_nested_column_ptr().get();
            null_map = &nullable_col->get_null_map_data();
        }

        auto res_column = ColumnString::create();
        auto null_column = ColumnUInt8::create(input_rows_count);

        bool success = false;
        if (execute_typed<ColumnFloat64>(actual_col, null_map, *res_column, *null_column,
                                         input_rows_count)) {
            success = true;
        } else if (execute_typed<ColumnFloat32>(actual_col, null_map, *res_column, *null_column,
                                                input_rows_count)) {
            success = true;
        }

        if (!success) [[unlikely]] {
            return Status::InvalidArgument("Unsupported column type {} for function {}",
                                           col_with_type.type->get_name(), name);
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(res_column), std::move(null_column)));
        return Status::OK();
    }

private:
    static inline size_t format_seconds(int64_t seconds, char* buf, char* buf_end) {
        if (seconds == 0) {
            static constexpr char zero_str[] = "0 seconds";
            std::memcpy(buf, zero_str, sizeof(zero_str) - 1);
            return sizeof(zero_str) - 1;
        }

        char* ptr = buf;
        auto append_unit = [&ptr, buf_end](int64_t val, std::string_view singular,
                                           std::string_view plural, bool need_comma) {
            if (need_comma) {
                *ptr++ = ',';
                *ptr++ = ' ';
            }
            auto [next, _] = std::to_chars(ptr, buf_end, val);
            ptr = next;
            *ptr++ = ' ';
            std::string_view unit = (val == 1) ? singular : plural;
            std::memcpy(ptr, unit.data(), unit.size());
            ptr += unit.size();
        };

        constexpr int64_t SECONDS_PER_MINUTE = 60;
        constexpr int64_t SECONDS_PER_HOUR = 3600;
        constexpr int64_t SECONDS_PER_DAY = 86400;
        constexpr int64_t DAYS_PER_WEEK = 7;
        constexpr int64_t SECONDS_PER_WEEK = SECONDS_PER_DAY * DAYS_PER_WEEK;

        int64_t weeks = seconds / SECONDS_PER_WEEK;
        seconds %= SECONDS_PER_WEEK;

        int64_t days = seconds / SECONDS_PER_DAY;
        seconds %= SECONDS_PER_DAY;

        int64_t hours = seconds / SECONDS_PER_HOUR;
        seconds %= SECONDS_PER_HOUR;

        int64_t minutes = seconds / SECONDS_PER_MINUTE;
        int64_t secs = seconds % SECONDS_PER_MINUTE;

        bool has_prev = false;
        if (weeks > 0) {
            append_unit(weeks, "week", "weeks", has_prev);
            has_prev = true;
        }
        if (days > 0) {
            append_unit(days, "day", "days", has_prev);
            has_prev = true;
        }
        if (hours > 0) {
            append_unit(hours, "hour", "hours", has_prev);
            has_prev = true;
        }
        if (minutes > 0) {
            append_unit(minutes, "minute", "minutes", has_prev);
            has_prev = true;
        }
        if (secs > 0) {
            append_unit(secs, "second", "seconds", has_prev);
            has_prev = true;
        }

        return ptr - buf;
    }

    template <typename ColumnType>
    static bool execute_typed(const IColumn* col, const NullMap* null_map, ColumnString& res_col,
                              ColumnUInt8& null_map_col, size_t rows) {
        const auto* vec = check_and_get_column<ColumnType>(col);
        if (!vec) {
            return false;
        }
        const auto& data = vec->get_data();
        auto& res_data = res_col.get_chars();
        auto& res_offsets = res_col.get_offsets();
        auto& null_data = null_map_col.get_data();

        res_offsets.resize(rows);
        res_data.reserve(rows * 32);

        char buf[128];
        char* buf_end = buf + sizeof(buf);

        for (size_t i = 0; i < rows; ++i) {
            if (null_map && (*null_map)[i]) {
                null_data[i] = 1;
                res_offsets[i] = cast_set<UInt32>(res_data.size());
                continue;
            }

            auto val = data[i];
            if (std::isnan(val) || std::isinf(val)) {
                null_data[i] = 1;
                res_offsets[i] = cast_set<UInt32>(res_data.size());
                continue;
            }
            double abs_val = std::abs(static_cast<double>(val));
            if (abs_val > static_cast<double>(std::numeric_limits<int64_t>::max())) {
                null_data[i] = 1;
                res_offsets[i] = cast_set<UInt32>(res_data.size());
                continue;
            }
            int64_t seconds = std::llround(abs_val);

            null_data[i] = 0;
            size_t len = format_seconds(seconds, buf, buf_end);
            size_t old_size = res_data.size();
            res_data.resize(old_size + len);
            std::memcpy(res_data.data() + old_size, buf, len);
            res_offsets[i] = cast_set<UInt32>(res_data.size());
        }
        return true;
    }
};

void register_function_human_readable_seconds(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHumanReadableSeconds>();
}

} // namespace doris
