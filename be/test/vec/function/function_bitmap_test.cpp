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

#include "function_test_util.h"
#include "util/bitmap_value.h"
#include "vec/functions/function_totype.h"

namespace doris::vectorized {

TEST(function_bitmap_test, function_bitmap_min_test) {
    std::string func_name = "bitmap_min";
    InputTypeSet input_types = {TypeIndex::BitMap};

    auto bitmap1 = new BitmapValue(1);
    auto bitmap2 = new BitmapValue(std::vector<uint64_t>({1, 9999999}));
    auto empty_bitmap = new BitmapValue();
    DataSet data_set = {{{bitmap1}, (int64_t)1},
                        {{bitmap2}, (int64_t)1},
                        {{empty_bitmap}, (int64_t)0},
                        {{Null()}, Null()}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    delete bitmap1;
    delete bitmap2;
    delete empty_bitmap;
}
TEST(function_bitmap_test, function_bitmap_max_test) {
    std::string func_name = "bitmap_max";
    InputTypeSet input_types = {TypeIndex::BitMap};

    auto bitmap1 = new BitmapValue(1);
    auto bitmap2 = new BitmapValue(std::vector<uint64_t>({1, 9999999}));
    auto empty_bitmap = new BitmapValue();
    DataSet data_set = {{{bitmap1}, (int64_t)1},
                        {{bitmap2}, (int64_t)9999999},
                        {{empty_bitmap}, (int64_t)0},
                        {{Null()}, Null()}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    delete bitmap1;
    delete bitmap2;
    delete empty_bitmap;
}

TEST(function_bitmap_test, function_bitmap_to_string_test) {
    std::string func_name = "bitmap_to_string";
    InputTypeSet input_types = {TypeIndex::BitMap};

    auto bitmap1 = new BitmapValue(1);
    auto bitmap2 = new BitmapValue(std::vector<uint64_t>({1, 9999999}));
    auto empty_bitmap = new BitmapValue();
    DataSet data_set = {{{bitmap1}, std::string("1")},
                        {{bitmap2}, std::string("1,9999999")},
                        {{empty_bitmap}, std::string("")},
                        {{Null()}, Null()}};

    check_function<DataTypeString, true>(func_name, input_types, data_set);
    delete bitmap1;
    delete bitmap2;
    delete empty_bitmap;
}

TEST(function_bitmap_test, function_bitmap_and_count) {
    std::string func_name = "bitmap_and_count";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};
    auto bitmap1 = new BitmapValue(std::vector<uint64_t>({1, 2, 3}));
    auto bitmap2 = new BitmapValue(std::vector<uint64_t>({3, 4, 5}));
    auto empty_bitmap = new BitmapValue();
    DataSet data_set = {{{bitmap1, empty_bitmap}, (int64_t)0},
                        {{bitmap1, bitmap1}, (int64_t)3},
                        {{bitmap1, bitmap2}, (int64_t)1}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    delete bitmap1;
    delete bitmap2;
    delete empty_bitmap;
}

TEST(function_bitmap_test, function_bitmap_or_count) {
    std::string func_name = "bitmap_or_count";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};
    auto bitmap1 = new BitmapValue(std::vector<uint64_t>({1, 2, 3}));
    auto bitmap2 = new BitmapValue(std::vector<uint64_t>({1, 2, 3, 4}));
    auto bitmap3 = new BitmapValue(std::vector<uint64_t>({2, 3}));
    auto empty_bitmap = new BitmapValue();
    DataSet data_set = {{{bitmap1, empty_bitmap}, (int64_t)3},
                        {{bitmap2, bitmap3}, (int64_t)4},
                        {{bitmap1, bitmap3}, (int64_t)3}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    delete bitmap1;
    delete bitmap2;
    delete bitmap3;
    delete empty_bitmap;
}

TEST(function_bitmap_test, function_bitmap_xor_count) {
    std::string func_name = "bitmap_xor_count";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};
    auto bitmap1 = new BitmapValue(std::vector<uint64_t>({1, 2, 3}));
    auto bitmap2 = new BitmapValue(std::vector<uint64_t>({1, 2, 3, 4}));
    auto bitmap3 = new BitmapValue(std::vector<uint64_t>({2, 3}));
    auto bitmap4 = new BitmapValue(std::vector<uint64_t>({1, 2, 6}));
    auto empty_bitmap = new BitmapValue();
    DataSet data_set = {{{bitmap1, empty_bitmap}, (int64_t)3},
                        {{bitmap2, bitmap3}, (int64_t)2},
                        {{bitmap1, bitmap4}, (int64_t)2}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    delete bitmap1;
    delete bitmap2;
    delete bitmap3;
    delete bitmap4;
    delete empty_bitmap;
}
} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
