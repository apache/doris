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

#include "vec/functions/dictionary_util.h"

#include <gtest/gtest.h>

#include "testutil/column_helper.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_number.h"
namespace doris::vectorized {

ColumnPtr make_nullable(const std::vector<int32_t>& data, const std::vector<uint8_t>& null_data) {
    auto data_col = ColumnHelper::create_column<DataTypeInt32>(data);
    auto null_col = ColumnHelper::create_column<DataTypeUInt8>(null_data);
    return ColumnNullable::create(data_col, null_col);
}

ColumnWithTypeAndName create_data(const std::vector<int32_t>& data,
                                  const std::vector<uint8_t>& null_data, const std::string& name) {
    auto col = make_nullable(data, null_data);
    return ColumnWithTypeAndName(
            std::move(col), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
            name);
}

ColumnWithTypeAndName create_data(const std::vector<int32_t>& data, const std::string& name) {
    auto col = ColumnHelper::create_column<DataTypeInt32>(data);
    return ColumnWithTypeAndName(std::move(col), std::make_shared<DataTypeInt32>(), name);
}

Block merge_block(ColumnsWithTypeAndName& key_data, ColumnsWithTypeAndName& value_data) {
    Block block(key_data);
    for (auto& value : value_data) {
        block.insert(value);
    }
    return block;
}

void show_data(ColumnsWithTypeAndName& key_data, ColumnsWithTypeAndName& value_data) {
    Block block(key_data);
    for (auto& value : value_data) {
        block.insert(value);
    }
    std::cout << block.dump_data() << std::endl;
}

TEST(DictionaryUtilTest, not_null_test) {
    ColumnsWithTypeAndName key_data {create_data({1, 2, 3, 4, 5}, "key1"),
                                     create_data({10, 20, 30, 40, 50}, "key2")};

    ColumnsWithTypeAndName value_data {create_data({1, 2, 3, 4, 5}, "value1"),
                                       create_data({10, 20, 30, 40, 50}, "value2")};

    auto status = check_dict_input_data(key_data, value_data, false);
    EXPECT_TRUE(status.ok());

    ColumnHelper::block_equal(
            merge_block(key_data, value_data),
            Block {create_data({1, 2, 3, 4, 5}, "key1"), create_data({10, 20, 30, 40, 50}, "key2"),
                   create_data({1, 2, 3, 4, 5}, "value1"),
                   create_data({10, 20, 30, 40, 50}, "value2")});
}

TEST(DictionaryUtilTest, nullable_key_without_null_not_sikp_test) {
    ColumnsWithTypeAndName key_data {create_data({1, 2, 3, 4, 5}, {0, 0, 0, 0, 0}, "key1"),
                                     create_data({10, 20, 30, 40, 50}, "key2")};

    ColumnsWithTypeAndName value_data {create_data({1, 2, 3, 4, 5}, "value1"),
                                       create_data({10, 20, 30, 40, 50}, "value2")};

    auto status = check_dict_input_data(key_data, value_data, false);
    EXPECT_TRUE(status.ok());

    ColumnHelper::block_equal(
            merge_block(key_data, value_data),
            Block {create_data({1, 2, 3, 4, 5}, "key1"), create_data({10, 20, 30, 40, 50}, "key2"),
                   create_data({1, 2, 3, 4, 5}, "value1"),
                   create_data({10, 20, 30, 40, 50}, "value2")});
}

TEST(DictionaryUtilTest, nullable_key_with_null_not_sikp_test) {
    ColumnsWithTypeAndName key_data {create_data({1, 2, 3, 4, 5}, {0, 0, 1, 1, 0}, "key1"),
                                     create_data({10, 20, 30, 40, 50}, "key2")};

    ColumnsWithTypeAndName value_data {create_data({1, 2, 3, 4, 5}, "value1"),
                                       create_data({10, 20, 30, 40, 50}, "value2")};

    auto status = check_dict_input_data(key_data, value_data, false);
    EXPECT_TRUE(!status.ok());

    std::cout << status.msg() << std::endl;
}

TEST(DictionaryUtilTest, nullable_key_without_null_sikp_test) {
    ColumnsWithTypeAndName key_data {create_data({1, 2, 3, 4, 5}, {0, 0, 0, 0, 0}, "key1"),
                                     create_data({10, 20, 30, 40, 50}, "key2")};

    ColumnsWithTypeAndName value_data {create_data({1, 2, 3, 4, 5}, "value1"),
                                       create_data({10, 20, 30, 40, 50}, "value2")};

    auto status = check_dict_input_data(key_data, value_data, true);
    EXPECT_TRUE(status.ok());

    ColumnHelper::block_equal(
            merge_block(key_data, value_data),
            Block {create_data({1, 2, 3, 4, 5}, "key1"), create_data({10, 20, 30, 40, 50}, "key2"),
                   create_data({1, 2, 3, 4, 5}, "value1"),
                   create_data({10, 20, 30, 40, 50}, "value2")});
}

TEST(DictionaryUtilTest, nullable_key_with_null_sikp_test) {
    ColumnsWithTypeAndName key_data {create_data({1, 2, 3, 4, 5}, {0, 1, 0, 1, 0}, "key1"),
                                     create_data({10, 20, 30, 40, 50}, "key2")};

    ColumnsWithTypeAndName value_data {create_data({1, 2, 3, 4, 5}, "value1"),
                                       create_data({10, 20, 30, 40, 50}, "value2")};

    auto status = check_dict_input_data(key_data, value_data, true);
    EXPECT_TRUE(status.ok());

    ColumnHelper::block_equal(
            merge_block(key_data, value_data),
            Block {create_data({1, 3, 5}, "key1"), create_data({10, 30, 50}, "key2"),
                   create_data({1, 3, 5}, "value1"), create_data({10, 30, 50}, "value2")});
}

TEST(DictionaryUtilTest, nullable_key_with_null_sikp_test2) {
    ColumnsWithTypeAndName key_data {create_data({1, 2, 3, 4, 5}, {0, 1, 0, 1, 0}, "key1"),
                                     create_data({10, 20, 30, 40, 50}, {0, 0, 1, 1, 0}, "key2")};

    ColumnsWithTypeAndName value_data {create_data({1, 2, 3, 4, 5}, "value1"),
                                       create_data({10, 20, 30, 40, 50}, "value2")};

    auto status = check_dict_input_data(key_data, value_data, true);
    EXPECT_TRUE(status.ok());

    ColumnHelper::block_equal(
            merge_block(key_data, value_data),
            Block {create_data({1, 5}, "key1"), create_data({10, 50}, "key2"),
                   create_data({1, 5}, "value1"), create_data({10, 50}, "value2")});
}

} // namespace doris::vectorized
