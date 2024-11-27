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

#include <cctype>
#include <cstddef>
#include <cstring>
#include <memory>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
constexpr static std::array<int, 5> SPLIT_POS = {8, 13, 18, 23, 36}; // 8-4-4-4-12
constexpr static char DELIMITER = '-';

class FunctionUuidtoInt : public IFunction {
public:
    static constexpr auto name = "uuid_to_int";

    static FunctionPtr create() { return std::make_shared<FunctionUuidtoInt>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeInt128>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& arg_column =
                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);

        auto result_column = ColumnInt128::create(input_rows_count);
        auto& result_data = result_column->get_data();
        auto null_column = ColumnUInt8::create(input_rows_count);
        auto& null_map = null_column->get_data();

        for (int row = 0; row < input_rows_count; row++) {
            auto str = arg_column.get_data_at(row);
            const auto* data = str.data;
            Int128* result_cell = &result_data[row];
            *result_cell = 0;
            null_map[row] = false;

            if (str.size == 36) {
                if (data[SPLIT_POS[0]] != DELIMITER || data[SPLIT_POS[1]] != DELIMITER ||
                    data[SPLIT_POS[2]] != DELIMITER || data[SPLIT_POS[3]] != DELIMITER) {
                    null_map[row] = true;
                    continue;
                }
                char new_data[32];
                memset(new_data, 0, sizeof(new_data));
                // ignore '-'
                memcpy(new_data, data, 8);
                memcpy(new_data + 8, data + SPLIT_POS[0] + 1, 4);
                memcpy(new_data + 12, data + SPLIT_POS[1] + 1, 4);
                memcpy(new_data + 16, data + SPLIT_POS[2] + 1, 4);
                memcpy(new_data + 20, data + SPLIT_POS[3] + 1, 12);

                if (!serialize(new_data, (char*)result_cell, 32)) {
                    null_map[row] = true;
                    continue;
                }
            } else if (str.size == 32) {
                if (!serialize(data, (char*)result_cell, 32)) {
                    null_map[row] = true;
                    continue;
                }
            } else {
                null_map[row] = true;
                continue;
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(result_column), std::move(null_column)));
        return Status::OK();
    }

    // use char* to write dst is the only legal way by 'restrict aliasing rule'
    static bool serialize(const char* __restrict src, char* __restrict dst, size_t length) {
        char target; // 8bit, contains 2 char input
        auto translate = [&target](const char ch) {
            if (isdigit(ch)) {
                target += ch - '0';
            } else if (ch >= 'a' && ch <= 'f') {
                target += ch - 'a' + 10;
            } else if (ch >= 'A' && ch <= 'F') {
                target += ch - 'A' + 10;
            } else {
                return false;
            }
            return true;
        };

        bool ok = true;
        for (size_t i = 0; i < length; i += 2, src++, dst++) {
            target = 0;
            if (!translate(*src)) {
                ok = false; // dont break for auto-simd
            }

            src++;
            target <<= 4;
            if (!translate(*src)) {
                ok = false;
            }
            *dst = target;
        }

        return ok;
    }
};

class FunctionInttoUuid : public IFunction {
public:
    static constexpr auto name = "int_to_uuid";

    static FunctionPtr create() { return std::make_shared<FunctionInttoUuid>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& arg_column =
                assert_cast<const ColumnInt128&>(*block.get_by_position(arguments[0]).column);
        auto result_column = ColumnString::create();
        constexpr int str_length = 36;
        auto& col_data = result_column->get_chars();
        auto& col_offset = result_column->get_offsets();
        col_data.resize(str_length * input_rows_count +
                        1); // for branchless deserialize, we occupy one more byte for the last '-'
        col_offset.resize(input_rows_count);

        for (int row = 0; row < input_rows_count; row++) {
            const Int128* arg = &arg_column.get_data()[row];
            col_offset[row] = col_offset[row - 1] + str_length;
            deserialize((char*)arg, col_data.data() + str_length * row);
        }
        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

    // use char* to read src is the only legal way by 'restrict aliasing rule'
    static void deserialize(const char* __restrict src, unsigned char* __restrict dst) {
        auto transform = [](char ch) -> unsigned char {
            if (ch < 10) {
                return ch + '0';
            } else {
                return ch - 10 + 'a';
            }
        };

        int j = 0;
        for (int i : SPLIT_POS) {
            for (; j < i; src++, j += 2) { // input 16 chars, 2 data per char
                dst[j] = transform(((*src) >> 4) & 0x0F);
                dst[j + 1] = transform(*src & 0x0F);
            }
            dst[j++] = DELIMITER; // we resized one more byte.
        }
    }
};

void register_function_uuid_transforms(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionUuidtoInt>();
    factory.register_function<FunctionInttoUuid>();
}

} // namespace doris::vectorized
