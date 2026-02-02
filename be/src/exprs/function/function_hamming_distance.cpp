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

#include <cstring>
#include <vector>

#include "common/status.h"
#include "util/simd/vstring_function.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/typeid_cast.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class FunctionHammingDistance : public IFunction {
public:
    static constexpr auto name = "hamming_distance";

    static FunctionPtr create() { return std::make_shared<FunctionHammingDistance>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& [left_col, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_col, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto* left_str_col =
                check_and_get_column<ColumnString>(remove_nullable(left_col).get());
        const auto* right_str_col =
                check_and_get_column<ColumnString>(remove_nullable(right_col).get());
        if (!left_str_col || !right_str_col) {
            return Status::NotSupported("Illegal columns {}, {} of argument of function {}",
                                        left_col->get_name(), right_col->get_name(), get_name());
        }

        auto res_column = ColumnInt64::create(input_rows_count);
        auto& res_data = res_column->get_data();

        for (size_t i = 0; i < input_rows_count; ++i) {
            const StringRef left = left_str_col->get_data_at(left_const ? 0 : i);
            const StringRef right = right_str_col->get_data_at(right_const ? 0 : i);
            RETURN_IF_ERROR(hamming_distance(left, right, res_data[i], i));
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
            uint8_t char_len =
                    doris::get_utf8_byte_length(static_cast<uint8_t>(data[i]));
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

    static Status hamming_distance(const StringRef& left, const StringRef& right, Int64& result,
                                   size_t row) {
        if (simd::VStringFunctions::is_ascii(left) && simd::VStringFunctions::is_ascii(right)) {
            if (left.size != right.size) {
                return Status::InvalidArgument(
                        "hamming_distance requires strings of the same length at row {}", row);
            }
            Int64 distance = 0;
            for (size_t i = 0; i < left.size; ++i) {
                distance += static_cast<Int64>(left.data[i] != right.data[i]);
            }
            result = distance;
            return Status::OK();
        }

        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        utf8_char_offsets(left, left_offsets);
        utf8_char_offsets(right, right_offsets);

        if (left_offsets.size() != right_offsets.size()) {
            return Status::InvalidArgument(
                    "hamming_distance requires strings of the same length at row {}", row);
        }

        Int64 distance = 0;
        const size_t len = left_offsets.size();
        for (size_t i = 0; i < len; ++i) {
            size_t left_off = left_offsets[i];
            size_t left_next = (i + 1 < len) ? left_offsets[i + 1] : left.size;
            size_t right_off = right_offsets[i];
            size_t right_next = (i + 1 < len) ? right_offsets[i + 1] : right.size;
            distance += static_cast<Int64>(
                    !utf8_char_equal(left, left_off, left_next, right, right_off, right_next));
        }
        result = distance;
        return Status::OK();
    }
};

void register_function_hamming_distance(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHammingDistance>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
