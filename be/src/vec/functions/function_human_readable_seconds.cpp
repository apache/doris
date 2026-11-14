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

#include "vec/functions/function_human_readable_seconds.h"

#include <cmath>
#include <iomanip>
#include <sstream>
#include <string>

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"

namespace doris ::vectorized {
static std::string human_readable_seconds(double input) {
    if (std::isnan(input) || std::isinf(input) || input < 0) // 检查传入参数是否是数字和无穷大
    {
        return "";
    }

    constexpr long long SEC_PER_MIN = 60;
    constexpr long long SEC_PER_HOUR = 3600;
    constexpr long long SEC_PER_DAY = 86400;
    constexpr long long SEC_PER_WEEK = SEC_PER_DAY * 7;

    // 取绝对值和四舍五入
    double total_seconds = std::abs(input);
    long long total = static_cast<long long>(input);
    double fraction_seconds = total_seconds - total;
    // 求商取模
    long long weeks = total / SEC_PER_WEEK;
    total %= SEC_PER_WEEK;
    long long days = total / SEC_PER_DAY;
    total %= SEC_PER_DAY;
    long long hours = total / SEC_PER_HOUR;
    total %= SEC_PER_HOUR;
    long long minutes = total / SEC_PER_MIN;
    total %= SEC_PER_MIN;
    fraction_seconds += total;
    // 字符串拼接
    std::ostringstream oss;
    bool has_output = false;
    auto append = [&](long long val, const std::string& unit) {
        if (val > 0) {
            if (has_output) oss << ", ";                        // 除第一个都要加上“，”
            oss << val << " " << unit << (val == 1 ? "" : "s"); // 1second ，2seconds 复数
            has_output = true;
        }
    };
    append(weeks, "week");
    append(days, "day");
    append(hours, "hour");
    append(minutes, "minute");
    if (fraction_seconds > 0) {
        if (has_output) oss << ", ";
        if (fabs(fraction_seconds - round(fraction_seconds)) < 1e-9) {
            long long tmp = static_cast<long long>(std::round(fraction_seconds));
            oss << tmp << " second" << (tmp == 1 ? "" : "s");
        } else {
            oss << std::fixed << std::setprecision(3) << fraction_seconds << " seconds";
        }
        has_output = true;
    }

    if (!has_output) oss << "0 seconds";
    return oss.str();
}

Status FunctionHumanReadableSeconds::execute_impl(FunctionContext* context, Block& block,
                                                  const ColumnNumbers& arguments, uint32_t result,
                                                  size_t input_rows_count) const {
    const auto& src_col = block.get_by_position(arguments[0]).column;
    const ColumnFloat64* col_f64 = check_and_get_column<ColumnFloat64>(src_col.get());

    if (!col_f64) {
        return Status::RuntimeError("unsupported type for function {}: {}", get_name(),
                                    block.get_by_position(arguments[0]).type->get_name());
    }

    auto dst_col = ColumnString::create();
    auto& chars = dst_col->get_chars();
    auto& offsets = dst_col->get_offsets();
    offsets.resize(input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        double val = col_f64->get_float64(i);
        std::string result_str = human_readable_seconds(val);
        chars.insert(chars.end(), result_str.begin(), result_str.end());
        chars.push_back('\0');
        offsets[i] = chars.size();
    }

    block.replace_by_position(result, std::move(dst_col));
    return Status::OK();
}

void register_function_human_readable_seconds(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHumanReadableSeconds>();
}

} // namespace doris::vectorized