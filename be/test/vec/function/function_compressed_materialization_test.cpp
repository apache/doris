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

void compress_and_decompress(size_t len_of_varchar, std::string function_name) {
    const size_t input_rows_count = 4096;
    std::shared_ptr<IDataType> return_type_of_compress = nullptr;

    if (function_name == "compress_as_smallint") {
        return_type_of_compress = std::make_shared<DataTypeInt16>();
    } else if (function_name == "compress_as_int") {
        return_type_of_compress = std::make_shared<DataTypeInt32>();
    } else if (function_name == "compress_as_bigint") {
        return_type_of_compress = std::make_shared<DataTypeInt64>();
    } else if (function_name == "compress_as_largeint") {
        return_type_of_compress = std::make_shared<DataTypeInt128>();
    } else {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Invalid function name");
    }

    for (size_t m = 0; m <= len_of_varchar; ++m) {
        auto col_source_str_mutate = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i) {
            std::string random_bytes = generate_random_len_and_random_bytes(m);
            col_source_str_mutate->insert(Field(random_bytes.c_str(), random_bytes.size()));
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

        ColumnWithTypeAndName augument_compressed = input_block_compress.get_by_position(1);
        auto return_type_for_decompress = std::make_shared<DataTypeString>();
        auto decompress_func = SimpleFunctionFactory::instance().get_function(
                "decompress_varchar", {augument_compressed}, return_type_for_decompress);
        Block input_block_decompress({augument_compressed});
        input_block_decompress.insert(
                ColumnWithTypeAndName {nullptr, return_type_for_decompress, ""});
        st = decompress_func->execute(context, input_block_decompress, {0}, 1, input_rows_count);

        ASSERT_TRUE(st.ok());

        // Compare the original and decompressed columns
        auto col_result_str = std::move(assert_cast<const ColumnString*>(
                input_block_decompress.get_by_position(1).column.get()));

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

TEST(CompressedMaterializationTest, test_compress_as_smallint) {
    compress_and_decompress(1, "compress_as_smallint");
}

TEST(CompressedMaterializationTest, test_compress_as_int) {
    compress_and_decompress(3, "compress_as_int");
}
TEST(CompressedMaterializationTest, test_compress_as_bigint) {
    compress_and_decompress(7, "compress_as_bigint");
}

TEST(CompressedMaterializationTest, test_compress_as_largeint) {
    compress_and_decompress(15, "compress_as_largeint");
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

    std::vector<std::string> function_names = {"compress_as_smallint", "compress_as_int",
                                               "compress_as_bigint", "compress_as_largeint"};
    auto get_return_type = [](std::string function_name) -> std::shared_ptr<IDataType> {
        if (function_name == "compress_as_smallint") {
            return std::make_shared<DataTypeInt16>();
        } else if (function_name == "compress_as_int") {
            return std::make_shared<DataTypeInt32>();
        } else if (function_name == "compress_as_bigint") {
            return std::make_shared<DataTypeInt64>();
        } else if (function_name == "compress_as_largeint") {
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
        Status st = compress_func->execute(context, input_block_compress, {0}, 1,
                                                            input_rows_count);
        ASSERT_FALSE(st.ok());
    }
}

} // namespace doris::vectorized
