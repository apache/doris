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

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

static const size_t TEST_COUNT = 50;

static std::string print_hex(const StringRef& str) {
    std::string hex_str;
    for (unsigned char c : str) {
        fmt::format_to(std::back_inserter(hex_str), "{:02x} ", c);
    }
    return hex_str;
}

static std::string generate_random_bytes(size_t max_length) {
    std::srand(std::time(nullptr)); // use current time as seed for random generator

    if (max_length == 0) {
        return "";
    }

    auto randbyte = []() -> char {
        // generate a random byte, in range [0x00, 0xFF]
        return static_cast<char>(rand() % 256);
    };

    std::string str(max_length, 0);
    std::generate_n(str.begin(), max_length, randbyte);

    return str;
}

static std::string generate_random_len_and_random_bytes(size_t max_length) {
    std::srand(std::time(nullptr)); // use current time as seed for random generator

    if (max_length == 0) {
        return "";
    }

    auto randbyte = []() -> char {
        // generate a random byte, in range [0x00, 0xFF]
        return static_cast<char>(rand() % 256);
    };

    size_t random_length = rand() % (max_length + 1);
    std::string str(random_length, 0);
    std::generate_n(str.begin(), random_length, randbyte);

    return str;
}

void encode_and_decode(size_t len_of_varchar, std::string function_name) {
    const size_t input_rows_count = 4096;
    std::shared_ptr<IDataType> return_type_of_compress = nullptr;

    if (function_name == "encode_as_smallint") {
        return_type_of_compress = std::make_shared<DataTypeInt16>();
    } else if (function_name == "encode_as_int") {
        return_type_of_compress = std::make_shared<DataTypeInt32>();
    } else if (function_name == "encode_as_bigint") {
        return_type_of_compress = std::make_shared<DataTypeInt64>();
    } else if (function_name == "encode_as_largeint") {
        return_type_of_compress = std::make_shared<DataTypeInt128>();
    } else {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Invalid function name");
    }

    for (size_t m = 0; m <= len_of_varchar; ++m) {
        auto col_source_str_mutate = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (i % 100 == 0) {
                col_source_str_mutate->insert(Field(""));
                continue;
            } else if (i % 101 == 0) {
                col_source_str_mutate->insert(Field("\0"));
                continue;
            } else {
                std::string random_bytes = generate_random_len_and_random_bytes(m);
                col_source_str_mutate->insert(Field(random_bytes.c_str(), random_bytes.size()));
            }
        }

        auto col_source_str = std::move(col_source_str_mutate);

        ColumnWithTypeAndName argument_for_compress {col_source_str->clone(),
                                                     std::make_shared<DataTypeString>(), "col_str"};

        Block input_block_compress({argument_for_compress});
        input_block_compress.insert(ColumnWithTypeAndName {nullptr, return_type_of_compress, ""});

        auto compress_func = SimpleFunctionFactory::instance().get_function(
                function_name, {argument_for_compress}, return_type_of_compress);

        FunctionContext* context = nullptr;
        Status st = compress_func->execute(context, input_block_compress, {0}, 1, input_rows_count);
        ASSERT_TRUE(st.ok());

        ColumnWithTypeAndName augument_encoded = input_block_compress.get_by_position(1);
        auto return_type_for_decompress = std::make_shared<DataTypeString>();
        auto decode_func = SimpleFunctionFactory::instance().get_function(
                "decode_as_varchar", {augument_encoded}, return_type_for_decompress);
        Block input_block_for_decode({augument_encoded});
        input_block_for_decode.insert(
                ColumnWithTypeAndName {nullptr, return_type_for_decompress, ""});
        st = decode_func->execute(context, input_block_for_decode, {0}, 1, input_rows_count);

        ASSERT_TRUE(st.ok());

        // Compare the original and decompressed columns
        auto col_result_str = std::move(assert_cast<const ColumnString*>(
                input_block_for_decode.get_by_position(1).column.get()));

        for (size_t i = 0; i < input_rows_count; ++i) {
            EXPECT_EQ(col_source_str->get_data_at(i), col_result_str->get_data_at(i))
                    << fmt::format("Source byte {}, source size {} result byte {} size {}, m: {}\n",
                                   print_hex(col_source_str->get_data_at(i)),
                                   col_source_str->get_data_at(i).size,
                                   print_hex(col_result_str->get_data_at(i)),
                                   col_result_str->get_data_at(i).size, m);
        }
    }
}

TEST(CompressedMaterializationTest, test_encode_as_smallint) {
    for (size_t i = 0; i < TEST_COUNT; ++i) {
        encode_and_decode(1, "encode_as_smallint");
    }
}

TEST(CompressedMaterializationTest, test_encode_as_int) {
    for (size_t i = 0; i < TEST_COUNT; ++i) {
        encode_and_decode(3, "encode_as_int");
    }
}
TEST(CompressedMaterializationTest, test_encode_as_bigint) {
    for (size_t i = 0; i < TEST_COUNT; ++i) {
        encode_and_decode(7, "encode_as_bigint");
    }
}

TEST(CompressedMaterializationTest, test_encode_as_largeint) {
    for (size_t i = 0; i < TEST_COUNT; ++i) {
        encode_and_decode(15, "encode_as_largeint");
    }
}

TEST(CompressedMaterializationTest, abnormal_test) {
    size_t input_rows_count = 10;
    auto col_source_str_mutate = ColumnString::create();

    for (size_t i = 0; i < input_rows_count; ++i) {
        std::string random_bytes = generate_random_bytes(16);
        col_source_str_mutate->insert(Field(random_bytes.c_str(), random_bytes.size()));
    }

    auto col_source_str = std::move(col_source_str_mutate);
    std::cerr << fmt::format("max bytes {}\n", col_source_str->get_max_row_byte_size());

    std::vector<std::string> function_names = {"encode_as_smallint", "encode_as_int",
                                               "encode_as_bigint", "encode_as_largeint"};
    auto get_return_type = [](std::string function_name) -> std::shared_ptr<IDataType> {
        if (function_name == "encode_as_smallint") {
            return std::make_shared<DataTypeInt16>();
        } else if (function_name == "encode_as_int") {
            return std::make_shared<DataTypeInt32>();
        } else if (function_name == "encode_as_bigint") {
            return std::make_shared<DataTypeInt64>();
        } else if (function_name == "encode_as_largeint") {
            return std::make_shared<DataTypeInt128>();
        } else {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Invalid function name");
        }
    };

    for (auto function_name : function_names) {
        ColumnWithTypeAndName argument_for_compress {col_source_str->clone(),
                                                     std::make_shared<DataTypeString>(), "col_str"};
        Block input_block_compress({argument_for_compress});
        std::shared_ptr<IDataType> return_type_of_compress = get_return_type(function_name);
        input_block_compress.insert(ColumnWithTypeAndName {nullptr, return_type_of_compress, ""});
        auto compress_func = SimpleFunctionFactory::instance().get_function(
                function_name, {argument_for_compress}, return_type_of_compress);

        FunctionContext* context = nullptr;
        Status st = compress_func->execute(context, input_block_compress, {0}, 1, input_rows_count);
        ASSERT_FALSE(st.ok());
    }
}

} // namespace doris::vectorized
