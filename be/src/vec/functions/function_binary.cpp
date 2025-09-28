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

#include "vec/functions/function_binary.h"

#include "util/url_coding.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_avoid_begin.h"

static constexpr int MAX_STACK_CIPHER_LEN = 1024 * 64;

struct BinaryLength {
    static constexpr auto name = "length";
    static constexpr auto is_nullable = false;

    using ReturnType = DataTypeInt32;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        const auto* arg = check_and_get_column<ColumnVarbinary>(
                block.get_by_position(arguments[0]).column.get());

        auto result_column = ColumnInt32::create(input_rows_count);

        for (size_t i = 0; i < input_rows_count; i++) {
            int binary_size = arg->get_data()[i].size();
            result_column->get_data()[i] = binary_size;
        }

        block.replace_by_position(result, std::move(result_column));

        return Status::OK();
    }
};

using FunctionBinaryLength = FunctionBinaryUnary<BinaryLength>;

struct ToBase64Binary {
    static constexpr auto name = "to_base64";
    static constexpr auto is_nullable = false;

    using ReturnType = DataTypeString;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        const auto* arg = check_and_get_column<ColumnVarbinary>(
                block.get_by_position(arguments[0]).column.get());

        auto result_column = ColumnString::create();
        result_column->get_offsets().reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; i++) {
            auto binary = arg->get_data_at(i);

            if (binary.size == 0) {
                result_column->insert_default();
                continue;
            }

            char dst_array[MAX_STACK_CIPHER_LEN];
            char* dst = dst_array;

            int cipher_len = 4 * ((binary.size + 2) / 3);
            std::unique_ptr<char[]> dst_uptr;
            if (cipher_len > MAX_STACK_CIPHER_LEN) {
                dst_uptr.reset(new char[cipher_len]);
                dst = dst_uptr.get();
            }

            auto len = doris::base64_encode(reinterpret_cast<const unsigned char*>(binary.data),
                                            binary.size, reinterpret_cast<unsigned char*>(dst));

            result_column->insert_data(dst, len);
        }

        block.replace_by_position(result, std::move(result_column));

        return Status::OK();
    }
};

using FunctionToBase64Binary = FunctionBinaryUnary<ToBase64Binary>;

struct FromBase64Binary {
    static constexpr auto name = "from_base64";
    static constexpr auto is_nullable = true;

    using ReturnType = DataTypeVarbinary;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        const auto* arg = check_and_get_column<ColumnString>(
                block.get_by_position(arguments[0]).column.get());

        auto result_column = ColumnVarbinary::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        for (size_t i = 0; i < input_rows_count; i++) {
            auto base64_string = arg->get_data_at(i);

            if (base64_string.size == 0) {
                result_column->insert_default();
                continue;
            }

            char dst_array[MAX_STACK_CIPHER_LEN];
            char* dst = dst_array;

            int cipher_len = base64_string.size;
            std::unique_ptr<char[]> dst_uptr;
            if (cipher_len > MAX_STACK_CIPHER_LEN) {
                dst_uptr.reset(new char[cipher_len]);
                dst = dst_uptr.get();
            }

            auto len = doris::base64_decode(base64_string.data, base64_string.size, dst);

            if (len < 0) {
                null_map->get_data()[i] = 1;
                result_column->insert_default();
            } else {
                result_column->insert_data(dst, len);
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(result_column), std::move(null_map)));

        return Status::OK();
    }
};

using FunctionFromBase64Binary = FunctionBinaryUnary<FromBase64Binary>;

void register_function_binary(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBinaryLength>();
    factory.register_function<FunctionToBase64Binary>();
    factory.register_function<FunctionFromBase64Binary>();
    factory.register_function<FunctionSubBinary<SubBinary2Impl>>();
    factory.register_function<FunctionSubBinary<SubBinary3Impl>>();
}

#include "common/compile_check_avoid_end.h"
} // namespace doris::vectorized
