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

#include <algorithm>
#include <cstring>
#include <vector>

#include "common/status.h"
#include "util/simd/vstring_function.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class FunctionLevenshtein : public IFunction {
public:
    static constexpr auto name = "levenshtein";

    static FunctionPtr create() { return std::make_shared<FunctionLevenshtein>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt32>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr left_col = block.get_by_position(arguments[0]).column;
        const ColumnPtr right_col = block.get_by_position(arguments[1]).column;

        auto res_column = ColumnInt32::create(input_rows_count);
        auto& res_data = res_column->get_data();

        for (size_t i = 0; i < input_rows_count; ++i) {
            const StringRef left = left_col->get_data_at(i);
            const StringRef right = right_col->get_data_at(i);
            res_data[i] = levenshtein_distance(left, right);
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

private:
    static void utf8_char_offsets(const StringRef& ref, std::vector<size_t>& offsets) {
        offsets.clear();
        offsets.reserve(ref.size);
        const char* data = ref.data;
        size_t size = ref.size;
        size_t i = 0;
        while (i < size) {
            offsets.push_back(i);
            uint8_t char_len = UTF8_BYTE_LENGTH[static_cast<uint8_t>(data[i])];
            if (char_len == 0) {
                char_len = 1;
            }
            if (i + char_len > size) {
                char_len = static_cast<uint8_t>(size - i);
            }
            i += char_len;
        }
    }

    static inline bool utf8_char_equal(const StringRef& left, size_t left_off, size_t left_next,
                                       const StringRef& right, size_t right_off,
                                       size_t right_next) {
        size_t left_len = left_next - left_off;
        size_t right_len = right_next - right_off;
        if (left_len != right_len) {
            return false;
        }
        return std::memcmp(left.data + left_off, right.data + right_off, left_len) == 0;
    }

    static int levenshtein_distance(const StringRef& left, const StringRef& right) {
        if (left.size == 0) {
            return static_cast<int>(
                    simd::VStringFunctions::get_char_len(right.data, right.size));
        }
        if (right.size == 0) {
            return static_cast<int>(
                    simd::VStringFunctions::get_char_len(left.data, left.size));
        }

        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        utf8_char_offsets(left, left_offsets);
        utf8_char_offsets(right, right_offsets);

        const StringRef* left_ref = &left;
        const StringRef* right_ref = &right;
        if (right_offsets.size() > left_offsets.size()) {
            std::swap(left_offsets, right_offsets);
            std::swap(left_ref, right_ref);
        }

        const size_t m = left_offsets.size();
        const size_t n = right_offsets.size();

        if (m == 0) {
            return static_cast<int>(n);
        }
        if (n == 0) {
            return static_cast<int>(m);
        }

        std::vector<int> prev(n + 1);
        std::vector<int> curr(n + 1);
        for (size_t j = 0; j <= n; ++j) {
            prev[j] = static_cast<int>(j);
        }

        for (size_t i = 1; i <= m; ++i) {
            curr[0] = static_cast<int>(i);
            size_t left_off = left_offsets[i - 1];
            size_t left_next = (i < m) ? left_offsets[i] : left_ref->size;

            for (size_t j = 1; j <= n; ++j) {
                size_t right_off = right_offsets[j - 1];
                size_t right_next = (j < n) ? right_offsets[j] : right_ref->size;

                int cost = utf8_char_equal(*left_ref, left_off, left_next, *right_ref, right_off,
                                           right_next)
                                   ? 0
                                   : 1;

                int insert_cost = curr[j - 1] + 1;
                int delete_cost = prev[j] + 1;
                int replace_cost = prev[j - 1] + cost;
                curr[j] = std::min({insert_cost, delete_cost, replace_cost});
            }
            std::swap(prev, curr);
        }

        return prev[n];
    }
};

void register_function_levenshtein(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLevenshtein>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
