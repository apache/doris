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

#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris::vectorized {

using ConvertorChar = OlapBlockDataConvertor::OlapColumnDataConvertorChar;

TEST(CharTypePaddingTest, CharTypePaddingFullTest) {
    auto input = ColumnString::create();

    std::string str = "Allemande";
    size_t rows = 10;

    for (size_t i = 0; i < rows; i++) {
        input->insert_data(str.data(), str.length());
    }
    EXPECT_FALSE(ConvertorChar::should_padding(input, str.length()));

    input->insert_data(str.data(), str.length() - 1);
    EXPECT_TRUE(ConvertorChar::should_padding(input, str.length()));
}

TEST(CharTypePaddingTest, CharTypePaddingDataTest) {
    auto input = ColumnString::create();

    std::string str = "Allemande";

    size_t rows = str.length();
    for (int i = 0; i < rows; i++) {
        input->insert_data(str.data(), str.length() - i);
    }

    auto output = ConvertorChar::clone_and_padding(input, str.length());

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

} // namespace doris::vectorized
