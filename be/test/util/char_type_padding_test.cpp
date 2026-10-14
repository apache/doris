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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/string_ref.h"
#include "gtest/gtest_pred_impl.h"
#include "storage/storage_layout.h"

namespace doris {

using CharLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>;

TEST(CharTypePaddingTest, CharTypePaddingFullTest) {
    auto input = ColumnString::create();

    std::string str = "Allemande";
    size_t rows = 10;

    for (size_t i = 0; i < rows; i++) {
        input->insert_data(str.data(), str.length());
    }
    EXPECT_EQ(CharLayout::column_to_storage(*input, str.length()).get(), input.get());

    input->insert_data(str.data(), str.length() - 1);
    EXPECT_NE(CharLayout::column_to_storage(*input, str.length()).get(), input.get());
}

TEST(CharTypePaddingTest, CharTypePaddingDataTest) {
    auto input = ColumnString::create();

    std::string str = "Allemande";

    size_t rows = str.length();
    for (int i = 0; i < rows; i++) {
        input->insert_data(str.data(), str.length() - i);
    }

    auto output = CharLayout::column_to_storage(*input, str.length());
    ASSERT_NE(output.get(), input.get());
    ASSERT_EQ(output->size(), rows);

    for (int i = 0; i < rows; i++) {
        auto cell = output->get_data_at(i).to_string();
        EXPECT_EQ(cell.length(), str.length());

        auto str_real = std::string(cell.data(), str.length() - i);
        auto str_expect = str.substr(0, str.length() - i);
        EXPECT_EQ(str_real, str_expect);

        for (int j = str.length() - i; j < str.length(); j++) {
            EXPECT_EQ(cell[j], 0);
        }
    }
}

// The rows of a nullable column keep their positions. A NULL row's value is not copied: load
// does not check the value under a NULL, so it can be longer than the CHAR.
TEST(CharTypePaddingTest, CharTypePaddingNullableTest) {
    const std::vector<std::pair<std::string, uint8_t>> rows = {
            {"ab", 0}, {"longer than five", 1}, {"", 0}, {"abcde", 0}};
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    for (const auto& [value, is_null] : rows) {
        strings->insert_data(value.data(), value.size());
        nulls->get_data().push_back(is_null);
    }
    const auto input = ColumnNullable::create(std::move(strings), std::move(nulls));
    constexpr size_t length = 5;

    const auto output = CharLayout::column_to_storage(*input, length);
    ASSERT_EQ(output->size(), rows.size());
    EXPECT_EQ(output->get_data_at(0).to_string(), std::string("ab\0\0\0", 5));
    EXPECT_EQ(output->get_data_at(1).to_string(), std::string(length, '\0'));
    EXPECT_EQ(output->get_data_at(2).to_string(), std::string(length, '\0'));
    EXPECT_EQ(output->get_data_at(3).to_string(), "abcde");
    EXPECT_EQ(input->get_nested_column().get_data_at(0).to_string(), "ab")
            << "the source column is left alone";
}

// When the values, a NULL row's included, already total rows * `length` bytes, the nested
// column itself comes back.
TEST(CharTypePaddingTest, CharTypePaddingNullablePaddedTest) {
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    for (const std::string value : {"abcde", "fghij"}) {
        strings->insert_data(value.data(), value.size());
    }
    nulls->get_data().push_back(0);
    nulls->get_data().push_back(1);
    const auto input = ColumnNullable::create(std::move(strings), std::move(nulls));
    const IColumn* nested = &input->get_nested_column();

    EXPECT_EQ(CharLayout::column_to_storage(*input, 5).get(), nested);
}

// The check is on the total: a value under a NULL row can make up for a short one, which then
// stays unpadded.
TEST(CharTypePaddingTest, CharTypePaddingNullRowMakesUpTheSum) {
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    for (const std::string value : {"ab", "abcdefgh"}) {
        strings->insert_data(value.data(), value.size());
    }
    nulls->get_data().push_back(0);
    nulls->get_data().push_back(1);
    const auto input = ColumnNullable::create(std::move(strings), std::move(nulls));
    const IColumn* nested = &input->get_nested_column();

    const auto output = CharLayout::column_to_storage(*input, 5);
    EXPECT_EQ(output.get(), nested);
    EXPECT_EQ(output->get_data_at(0).to_string(), "ab");
}

} // namespace doris
