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

#include "vec/sink/writer/vhive_utils.h"

#include <gtest/gtest.h>

namespace doris::vectorized {

class VHiveUtilsTest : public testing::Test {
public:
    VHiveUtilsTest() = default;
    virtual ~VHiveUtilsTest() = default;
};

TEST_F(VHiveUtilsTest, test_make_partition_name) {
    {
        std::vector<THiveColumn> columns;
        THiveColumn column1;
        column1.name = "abc";
        columns.emplace_back(std::move(column1));
        std::vector<int> partition_columns_input_index = {0};
        EXPECT_EQ("abc=xyz",
                  VHiveUtils::make_partition_name(columns, partition_columns_input_index, {"xyz"}));
    }

    {
        std::vector<THiveColumn> columns;
        THiveColumn column1;
        column1.name = "abc:qqq";
        columns.emplace_back(std::move(column1));
        std::vector<int> partition_columns_input_index = {0};
        EXPECT_EQ("abc%3Aqqq=xyz%2Fyyy%3Dzzz",
                  VHiveUtils::make_partition_name(columns, partition_columns_input_index,
                                                  {"xyz/yyy=zzz"}));
    }

    {
        std::vector<THiveColumn> columns;
        THiveColumn column1;
        column1.name = "abc";
        columns.emplace_back(std::move(column1));
        THiveColumn column2;
        column2.name = "def";
        columns.emplace_back(std::move(column2));
        THiveColumn column3;
        column3.name = "xyz";
        columns.emplace_back(std::move(column3));
        std::vector<int> partition_columns_input_index = {0, 1, 2};
        EXPECT_EQ("abc=qqq/def=rrr/xyz=sss",
                  VHiveUtils::make_partition_name(columns, partition_columns_input_index,
                                                  {"qqq", "rrr", "sss"}));
    }
}

} // namespace doris::vectorized
