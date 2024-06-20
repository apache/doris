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
    // std::srand(std::time(nullptr)); // use current time as seed for random generator
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

// static void print_column_string(const ColumnString* col_str) {
//         std::stringstream ss3;
//         for (size_t i = 0; i < col_str->get_chars().size(); ++i) {
//             ss3 << print_hex(StringRef(&col_str->get_chars()[i], 1)) << ' ';
//         }
//         std::cout << "Compressed data: \n";
//         std::cout << ss3.str() << std::endl;
//         std::stringstream ss4;
//         for (size_t i = 0; i < col_str->get_offsets().size(); ++i) {
//             ss4 << col_str->get_offsets()[i] << " ";
//         }
//         std::cout << "Compressed offset: \n";
//         std::cout << ss4.str() << std::endl;
// }

TEST(CompressedMaterializationTest, test_compress_as_tinyint) {
    const size_t M = 1;
    const size_t input_rows_count = 4096;
    for (size_t m = 0; m <= M; ++m) {
        auto col_source_str_mutate = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i) {
            std::string random_bytes = generate_random_bytes(m);
            col_source_str_mutate->insert(Field(random_bytes.c_str(), random_bytes.size()));
        }

        auto col_source_str = std::move(col_source_str_mutate);

        ColumnWithTypeAndName argument_for_compress {col_source_str->clone(),
                                                     std::make_shared<DataTypeString>(), "col_str"};
        auto return_type_of_compress = std::make_shared<DataTypeInt8>();
        Block input_block_compress({argument_for_compress});
        input_block_compress.insert(ColumnWithTypeAndName {nullptr, return_type_of_compress, ""});

        auto compress_func = SimpleFunctionFactory::instance().get_function(
                "compress_as_tinyint", {argument_for_compress}, return_type_of_compress);

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

        // print_column_string(col_source_str);
        // print_column_string(col_result_str);
    }
}

TEST(CompressedMaterializationTest, test_compress_as_int) {
    const size_t M = 3;
    const size_t input_rows_count = 4096;
    for (size_t m = 0; m <= M; ++m) {
        auto col_source_str_mutate = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i) {
            std::string random_bytes = generate_random_bytes(m);
            col_source_str_mutate->insert(Field(random_bytes.c_str(), random_bytes.size()));
        }

        auto col_source_str = std::move(col_source_str_mutate);

        ColumnWithTypeAndName argument_for_compress {col_source_str->clone(),
                                                     std::make_shared<DataTypeString>(), "col_str"};
        auto return_type_of_compress = std::make_shared<DataTypeInt32>();
        Block input_block_compress({argument_for_compress});
        input_block_compress.insert(ColumnWithTypeAndName {nullptr, return_type_of_compress, ""});

        auto compress_func = SimpleFunctionFactory::instance().get_function(
                "compress_as_int", {argument_for_compress}, return_type_of_compress);

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
            EXPECT_EQ(col_source_str->get_data_at(i), col_result_str->get_data_at(i));
        }
    }
}
TEST(CompressedMaterializationTest, test_compress_as_bigint) {
    const size_t M = 7;
    const size_t input_rows_count = 4096;
    for (size_t m = 0; m <= M; ++m) {
        auto col_source_str_mutate = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i) {
            std::string random_bytes = generate_random_bytes(m);
            col_source_str_mutate->insert(Field(random_bytes.c_str(), random_bytes.size()));
        }

        auto col_source_str = std::move(col_source_str_mutate);

        ColumnWithTypeAndName argument_for_compress {col_source_str->clone(),
                                                     std::make_shared<DataTypeString>(), "col_str"};
        auto return_type_of_compress = std::make_shared<DataTypeInt64>();
        Block input_block_compress({argument_for_compress});
        input_block_compress.insert(ColumnWithTypeAndName {nullptr, return_type_of_compress, ""});

        auto compress_func = SimpleFunctionFactory::instance().get_function(
                "compress_as_bigint", {argument_for_compress}, return_type_of_compress);

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
            EXPECT_EQ(col_source_str->get_data_at(i), col_result_str->get_data_at(i));
        }
    }
}

TEST(CompressedMaterializationTest, test_compress_as_largeint) {
    const size_t M = 15;
    const size_t input_rows_count = 4096;

    for (size_t m = 0; m <= M; ++m) {
        auto col_source_str_mutate = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i) {
            std::string random_bytes = generate_random_bytes(m);
            col_source_str_mutate->insert(Field(random_bytes.c_str(), random_bytes.size()));
        }

        auto col_source_str = std::move(col_source_str_mutate);

        ColumnWithTypeAndName argument_for_compress {col_source_str->clone(),
                                                     std::make_shared<DataTypeString>(), "col_str"};
        auto return_type_of_compress = std::make_shared<DataTypeInt128>();
        Block input_block_compress({argument_for_compress});
        input_block_compress.insert(ColumnWithTypeAndName {nullptr, return_type_of_compress, ""});

        auto compress_func = SimpleFunctionFactory::instance().get_function(
                "compress_as_largeint", {argument_for_compress}, return_type_of_compress);

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
            EXPECT_EQ(col_source_str->get_data_at(i), col_result_str->get_data_at(i));
        }
    }
}

} // namespace doris::vectorized