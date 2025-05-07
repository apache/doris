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
#pragma once

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "vec/columns/column.h"
#include "vec/common/cow.h"
#include "vec/core/field.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

// this test is gonna to be a column test template for all column which should make ut test to coverage the function defined in column (all maybe we need 79 interfaces to be tested)
// for example column_array should test this function:
// size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized,
// get_shrinked_column, filter, filter_by_selector, serialize_vec, deserialize_vec, get_max_row_byte_size
//
namespace doris::vectorized {

class CommonColumnTest : public ::testing::Test {
public:
    void SetUp() override {}

    // this function helps to check sort permutation behavior for column which use column::compare_at
    static void stable_get_column_permutation(const IColumn& column, bool ascending, size_t limit,
                                              int nan_direction_hint,
                                              IColumn::Permutation& out_permutation) {
        (void)(limit);

        size_t size = column.size();
        out_permutation.resize(size);
        std::iota(out_permutation.begin(), out_permutation.end(),
                  IColumn::Permutation::value_type(0));

        std::stable_sort(out_permutation.begin(), out_permutation.end(),
                         [&](size_t lhs, size_t rhs) {
                             int res = column.compare_at(lhs, rhs, column, nan_direction_hint);
                             // to check element in column is sorted or not
                             if (ascending)
                                 return res < 0;
                             else
                                 return res > 0;
                         });
    }
};

auto check_permute = [](const IColumn& column, const IColumn::Permutation& permutation,
                        size_t limit, size_t expected_size) {
    auto res_col = column.permute(permutation, limit);
    EXPECT_EQ(res_col->size(), expected_size);
    try {
        for (size_t j = 0; j < expected_size; ++j) {
            EXPECT_EQ(res_col->compare_at(j, permutation[j], column, -1), 0);
        }
    } catch (doris::Exception& e) {
        LOG(ERROR) << "Exception: " << e.what();
        // using field check
        for (size_t j = 0; j < expected_size; ++j) {
            Field r;
            Field l;
            column.get(permutation[j], r);
            res_col->get(j, l);
            EXPECT_EQ(r, l);
        }
    }
};
auto assert_column_vector_permute = [](MutableColumns& cols, size_t num_rows) {
    for (const auto& col : cols) {
        size_t expected_size = num_rows ? std::min(col->size(), num_rows) : col->size();
        {
            IColumn::Permutation permutation;
            CommonColumnTest::stable_get_column_permutation(*col, true, col->size(), -1,
                                                            permutation);
            check_permute(*col, permutation, num_rows, expected_size);
        }
        {
            IColumn::Permutation permutation(col->size());
            std::iota(permutation.begin(), permutation.end(), IColumn::Permutation::value_type(0));
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(permutation.begin(), permutation.end(), g);
            check_permute(*col, permutation, num_rows, expected_size);
        }
    }
};

} // namespace doris::vectorized
