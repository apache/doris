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

#include "vec/columns/column_array.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"

namespace doris::vectorized {

void check_array_offsets(const IColumn& arr, const std::vector<ColumnArray::Offset64>& offs) {
    auto arr_col = check_and_get_column<ColumnArray>(arr);
    ASSERT_EQ(arr_col->size(), offs.size());
    for (size_t i = 0; i < arr_col->size(); ++i) {
        ASSERT_EQ(arr_col->get_offsets()[i], offs[i]);
    }
}
template <typename T>
void check_array_data(const IColumn& arr, const std::vector<T>& data) {
    auto arr_col = check_and_get_column<ColumnArray>(arr);
    auto data_col = arr_col->get_data_ptr();
    ASSERT_EQ(data_col->size(), data.size());
    for (size_t i = 0; i < data_col->size(); ++i) {
        auto element = data_col->get_data_at(i);
        ASSERT_EQ(*((T*)element.data), data[i]);
    }
}
template <>
void check_array_data(const IColumn& arr, const std::vector<std::string>& data) {
    auto arr_col = check_and_get_column<ColumnArray>(arr);
    auto data_col = arr_col->get_data_ptr();
    ASSERT_EQ(data_col->size(), data.size());
    for (size_t i = 0; i < data_col->size(); ++i) {
        auto element = data_col->get_data_at(i);
        ASSERT_EQ(std::string(element.data, element.size), data[i]);
    }
}

TEST(ColumnArrayTest, IntArrayTest) {
    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = ColumnVector<int32_t>::create();
    // init column array with [[1,2,3],[],[4]]
    std::vector<ColumnArray::Offset64> offs = {0, 3, 3, 4};
    std::vector<int32_t> vals = {1, 2, 3, 4};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }

    // check column array result
    ColumnArray array_column(std::move(data_column), std::move(off_column));
    EXPECT_EQ(array_column.size(), offs.size() - 1);
    for (size_t i = 0; i < array_column.size(); ++i) {
        auto v = get<Array>(array_column[i]);
        EXPECT_EQ(v.size(), offs[i + 1] - offs[i]);
        for (size_t j = 0; j < v.size(); ++j) {
            EXPECT_EQ(vals[offs[i] + j], get<int32_t>(v[j]));
        }
    }
}

TEST(ColumnArrayTest, StringArrayTest) {
    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = ColumnString::create();
    // init column array with [["abc","d"],["ef"],[], [""]];
    std::vector<ColumnArray::Offset64> offs = {0, 2, 3, 3, 4};
    std::vector<std::string> vals = {"abc", "d", "ef", ""};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data(v.data(), v.size());
    }

    // check column array result
    ColumnArray array_column(std::move(data_column), std::move(off_column));
    EXPECT_EQ(array_column.size(), offs.size() - 1);
    for (size_t i = 0; i < array_column.size(); ++i) {
        auto v = get<Array>(array_column[i]);
        EXPECT_EQ(v.size(), offs[i + 1] - offs[i]);
        for (size_t j = 0; j < v.size(); ++j) {
            EXPECT_EQ(vals[offs[i] + j], get<std::string>(v[j]));
        }
    }
}

TEST(ColumnArrayTest, IntArrayPermuteTest) {
    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = ColumnVector<int32_t>::create();
    // init column array with [[1,2,3],[],[4],[5,6]]
    std::vector<ColumnArray::Offset64> offs = {0, 3, 3, 4, 6};
    std::vector<int32_t> vals = {1, 2, 3, 4, 5, 6};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    ColumnArray array_column(std::move(data_column), std::move(off_column));

    IColumn::Permutation perm = {3, 2, 1, 0};
    // return array column: [[5,6],[4]];
    auto res1 = array_column.permute(perm, 2);
    check_array_offsets(*res1, {2, 3});
    check_array_data<int32_t>(*res1, {5, 6, 4});

    // return array column: [[5,6],[4],[],[1,2,3]]
    auto res2 = array_column.permute(perm, 0);
    check_array_offsets(*res2, {2, 3, 3, 6});
    check_array_data<int32_t>(*res2, {5, 6, 4, 1, 2, 3});
}

TEST(ColumnArrayTest, StringArrayPermuteTest) {
    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = ColumnString::create();
    // init column array with [["abc","d"],["ef"],[], [""]];
    std::vector<ColumnArray::Offset64> offs = {0, 2, 3, 3, 4};
    std::vector<std::string> vals = {"abc", "d", "ef", ""};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data(v.data(), v.size());
    }
    ColumnArray array_column(std::move(data_column), std::move(off_column));

    IColumn::Permutation perm = {3, 2, 1, 0};
    // return array column: [[""],[]];
    auto res1 = array_column.permute(perm, 2);
    check_array_offsets(*res1, {1, 1});
    check_array_data<std::string>(*res1, {""});

    // return array column: [[""],[],["ef"],["abc","d"]];
    auto res2 = array_column.permute(perm, 0);
    check_array_offsets(*res2, {1, 1, 2, 4});
    check_array_data<std::string>(*res2, {"", "ef", "abc", "d"});
}

TEST(ColumnArrayTest, EmptyArrayPermuteTest) {
    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = ColumnVector<int32_t>::create();
    // init column array with [[],[],[],[]]
    std::vector<ColumnArray::Offset64> offs = {0, 0, 0, 0, 0};
    std::vector<int32_t> vals = {};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    ColumnArray array_column(std::move(data_column), std::move(off_column));

    IColumn::Permutation perm = {3, 2, 1, 0};
    // return array column: [[],[]];
    auto res1 = array_column.permute(perm, 2);
    check_array_offsets(*res1, {0, 0});
    check_array_data<int32_t>(*res1, {});

    // return array column: [[],[],[],[]]
    auto res2 = array_column.permute(perm, 0);
    check_array_offsets(*res2, {0, 0, 0, 0});
    check_array_data<int32_t>(*res2, {});
}

} // namespace doris::vectorized
