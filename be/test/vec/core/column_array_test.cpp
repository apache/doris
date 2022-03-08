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

#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace doris::vectorized {

TEST(ColumnArrayTest, IntArrayTest) {
    auto off_column = ColumnVector<IColumn::Offset>::create();
    auto data_column = ColumnVector<int32_t>::create();
    // init column array with [[1,2,3],[],[4]]
    std::vector<IColumn::Offset> offs = {0, 3, 3, 4};
    std::vector<int32_t> vals = {1, 2, 3, 4};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }

    // check column array result
    ColumnArray array_column(std::move(data_column), std::move(off_column));
    ASSERT_EQ(array_column.size(), offs.size() - 1);
    for (size_t i = 0; i < array_column.size(); ++i) {
        auto v = get<Array>(array_column[i]);
        ASSERT_EQ(v.size(), offs[i + 1] - offs[i]);
        for (size_t j = 0; j < v.size(); ++j) {
            ASSERT_EQ(vals[offs[i] + j], get<int32_t>(v[j]));
        }
    }
}

TEST(ColumnArrayTest, StringArrayTest) {
    auto off_column = ColumnVector<IColumn::Offset>::create();
    auto data_column = ColumnString::create();
    // init column array with [["abc","d"],["ef"],[], [""]];
    std::vector<IColumn::Offset> offs = {0, 2, 3, 3, 4};
    std::vector<std::string> vals = {"abc", "d", "ef", ""};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data(v.data(), v.size());
    }

    // check column array result
    ColumnArray array_column(std::move(data_column), std::move(off_column));
    ASSERT_EQ(array_column.size(), offs.size() - 1);
    for (size_t i = 0; i < array_column.size(); ++i) {
        auto v = get<Array>(array_column[i]);
        ASSERT_EQ(v.size(), offs[i + 1] - offs[i]);
        for (size_t j = 0; j < v.size(); ++j) {
            ASSERT_EQ(vals[offs[i] + j], get<std::string>(v[j]));
        }
    }
}

} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
