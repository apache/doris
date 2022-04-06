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

#include "vec/columns/column_complex.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "vec/data_types/data_type_bitmap.h"
namespace doris::vectorized {
TEST(ColumnComplexTest, BasicTest) {
    using ColumnSTLString = ColumnComplexType<std::string>;
    auto column = ColumnSTLString::create();
    EXPECT_EQ(column->size(), 0);
    std::string val0 = "";
    std::string val1 = "str-1";

    column->insert_data(reinterpret_cast<const char*>(&val0), sizeof(val0));
    column->insert_data(reinterpret_cast<const char*>(&val1), sizeof(val1));

    StringRef ref = column->get_data_at(0);
    EXPECT_EQ((*reinterpret_cast<const std::string*>(ref.data)), "");
    ref = column->get_data_at(1);
    EXPECT_EQ((*reinterpret_cast<const std::string*>(ref.data)), val1);
}

// Test the compile failed
TEST(ColumnComplexType, DataTypeBitmapTest) {
    std::make_shared<DataTypeBitMap>();
}
} // namespace doris::vectorized
