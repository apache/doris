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

#include "vec/columns/column_string.h"

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include "vec/common/string_ref.h"
#include "vec/core/types.h"

using namespace doris;
using namespace doris::vectorized;

TEST(ColumnStringTest, shrink_padding_chars) {
    ColumnString::MutablePtr col = ColumnString::create();
    col->insert_data("123\0   ", 7);
    col->insert_data("456\0xx", 6);
    col->insert_data("78", 2);
    col->shrink_padding_chars();

    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data_at(0), StringRef("123"));
    EXPECT_EQ(col->get_data_at(0).size, 3);
    EXPECT_EQ(col->get_data_at(1), StringRef("456"));
    EXPECT_EQ(col->get_data_at(1).size, 3);
    EXPECT_EQ(col->get_data_at(2), StringRef("78"));
    EXPECT_EQ(col->get_data_at(2).size, 2);

    col->insert_data("xyz", 2); // only xy

    EXPECT_EQ(col->size(), 4);
    EXPECT_EQ(col->get_data_at(3), StringRef("xy"));
}