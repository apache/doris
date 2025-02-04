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
#include <glog/logging.h>

#include <array>
#include <cctype>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "util/block_compression.h"
#include "util/faststring.h"
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

class FunctionCompress : public IFunction {
    static constexpr std::array<char, 16> HEX_ITOC = {'0', '1', '2', '3', '4', '5', '6', '7',
                                                      '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

public:
    static constexpr auto name = "compress";
    static FunctionPtr create() { return std::make_shared<FunctionCompress>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        // Get the compression algorithm object
        BlockCompressionCodec* compression_codec;
        RETURN_IF_ERROR(get_block_compression_codec(segment_v2::CompressionTypePB::ZLIB,
                                                    &compression_codec));

        const auto& arg_column =
                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);
        auto result_column = ColumnString::create();

        auto& arg_data = arg_column.get_chars();
        auto& arg_offset = arg_column.get_offsets();
        const char* arg_begin = reinterpret_cast<const char*>(arg_data.data());

        auto& col_data = result_column->get_chars();
        auto& col_offset = result_column->get_offsets();
        col_offset.resize(input_rows_count);

        faststring compressed_str;
        Slice data;

        // When the original string is large, the result is roughly this value
        size_t total = arg_offset[input_rows_count - 1];
        col_data.reserve(total / 1000);

        for (size_t row = 0; row < input_rows_count; row++) {
            size_t length = arg_offset[row] - arg_offset[row - 1];
            data = Slice(arg_begin + arg_offset[row - 1], length);

            size_t idx = col_data.size();
            if (!length) { // data is ''
                col_offset[row] = col_offset[row - 1];
                continue;
            }

            // Z_MEM_ERROR and Z_BUF_ERROR are already handled in compress, making sure st is always Z_OK
            auto st = compression_codec->compress(data, &compressed_str);
            col_data.resize(col_data.size() + 10 + compressed_str.size());

            // first ten digits represent the length of the uncompressed string
            col_data[idx] = '0', col_data[idx + 1] = 'x';
            for (size_t i = 0; i < 4; i++) {
                unsigned char byte = (length >> (i * 8)) & 0xFF;
                col_data[idx + 2 + i * 2] = HEX_ITOC[byte >> 4]; // higher four
                col_data[idx + 3 + i * 2] = HEX_ITOC[byte & 0x0F];
            }
            idx += 10;

            // The length of compress_str is not known in advance, so it cannot be compressed directly into col_data
            unsigned char* src = compressed_str.data();
            for (size_t i = 0; i < compressed_str.size(); idx++, i++, src++) {
                col_data[idx] = *src;
            }
            col_offset[row] = col_offset[row - 1] + 10 + compressed_str.size();
        }

        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }
};

class FunctionUncompress : public IFunction {
public:
    static constexpr auto name = "uncompress";
    static FunctionPtr create() { return std::make_shared<FunctionUncompress>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        // Get the compression algorithm object
        BlockCompressionCodec* compression_codec;
        RETURN_IF_ERROR(get_block_compression_codec(segment_v2::CompressionTypePB::ZLIB,
                                                    &compression_codec));

        const auto& arg_column =
                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);

        auto& arg_data = arg_column.get_chars();
        auto& arg_offset = arg_column.get_offsets();
        const char* arg_begin = reinterpret_cast<const char*>(arg_data.data());

        auto result_column = ColumnString::create();
        auto& col_data = result_column->get_chars();
        auto& col_offset = result_column->get_offsets();
        col_offset.resize(input_rows_count);

        auto null_column = ColumnUInt8::create(input_rows_count);
        auto& null_map = null_column->get_data();

        std::string uncompressed;
        Slice data;
        Slice uncompressed_slice;

        size_t total = arg_offset[input_rows_count - 1];
        col_data.reserve(total * 1000);

        for (size_t row = 0; row < input_rows_count; row++) {
            null_map[row] = false;
            data = Slice(arg_begin + arg_offset[row - 1], arg_offset[row] - arg_offset[row - 1]);
            size_t data_length = arg_offset[row] - arg_offset[row - 1];

            if (data_length == 0) { // The original data is ''
                col_offset[row] = col_offset[row - 1];
                continue;
            }

            bool illegal = false;
            // The first ten digits are "0x" and length, followed by hexadecimal, each two digits is a byte
            if (data_length < 10) {
                illegal = true;
            } else {
                if (data[0] != '0' || data[1] != 'x') {
                    illegal = true;
                }
                for (size_t i = 2; i <= 9; i++) {
                    if (!std::isxdigit(data[i])) {
                        illegal = true;
                    }
                }
            }

            if (illegal) { // The top ten don't fit the rules
                col_offset[row] = col_offset[row - 1];
                null_map[row] = true;
                continue;
            }

            unsigned int length = 0;
            for (size_t i = 2; i <= 9; i += 2) {
                unsigned char byte = 0;
                std::from_chars(data.data + i, data.data + i + 2, byte, 16);
                length += (byte << (8 * (i / 2 - 1))); //Little Endian : 0x01000000 -> 1
            }

            size_t idx = col_data.size();
            col_data.resize(col_data.size() + length);
            uncompressed_slice = Slice(col_data.data() + idx, length);

            Slice compressed_data(data.data + 10, data.size - 10);
            auto st = compression_codec->decompress(compressed_data, &uncompressed_slice);

            if (!st.ok()) {                                // is not a legal compressed string
                col_data.resize(col_data.size() - length); // remove compressed_data
                col_offset[row] = col_offset[row - 1];
                null_map[row] = true;
                continue;
            }
            col_offset[row] = col_offset[row - 1] + length;
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(result_column), std::move(null_column)));
        return Status::OK();
    }
};

void register_function_compress(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionCompress>();
    factory.register_function<FunctionUncompress>();
}

} // namespace doris::vectorized
