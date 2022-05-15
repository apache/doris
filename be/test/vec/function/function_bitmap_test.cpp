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

    BitmapValue bitmap1(1);
    BitmapValue bitmap2({1, 9999999});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1}, (int64_t)1},
                        {{&bitmap2}, (int64_t)1},
                        {{&empty_bitmap}, Null()},
                        {{Null()}, Null()}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
}
TEST(function_bitmap_test, function_bitmap_max_test) {
    std::string func_name = "bitmap_max";
    InputTypeSet input_types = {TypeIndex::BitMap};

    BitmapValue bitmap1(1);
    BitmapValue bitmap2({1, 9999999});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1}, (int64_t)1},
                        {{&bitmap2}, (int64_t)9999999},
                        {{&empty_bitmap}, Null()},
                        {{Null()}, Null()}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
}

TEST(function_bitmap_test, function_bitmap_to_string_test) {
    std::string func_name = "bitmap_to_string";
    InputTypeSet input_types = {TypeIndex::BitMap};

    BitmapValue bitmap1(1);
    BitmapValue bitmap2({1, 9999999});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1}, std::string("1")},
                        {{&bitmap2}, std::string("1,9999999")},
                        {{&empty_bitmap}, std::string("")},
                        {{Null()}, Null()}};

    check_function<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_bitmap_test, function_bitmap_and_count) {
    std::string func_name = "bitmap_and_count";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};
    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({3, 4, 5});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1, &empty_bitmap}, (int64_t)0},
                        {{&bitmap1, &bitmap1}, (int64_t)3},
                        {{&bitmap1, &bitmap2}, (int64_t)1}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);

    {
        InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap, TypeIndex::BitMap};
        BitmapValue bitmap1({33, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue empty_bitmap; //test empty

        DataSet data_set = {{{&bitmap1, &bitmap2, &empty_bitmap}, (int64_t)0},
                            {{&bitmap1, &bitmap2, &bitmap3}, (int64_t)1}, //33
                            {{&bitmap1, &bitmap2, Null()}, Null()},
                            {{&bitmap1, &bitmap3, &bitmap3}, (int64_t)1}}; //33

        check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    }
}

TEST(function_bitmap_test, function_bitmap_or_count) {
    std::string func_name = "bitmap_or_count";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};

    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({1, 2, 3, 4});
    BitmapValue bitmap3({2, 3});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1, &empty_bitmap}, (int64_t)3},
                        {{&bitmap2, &bitmap3}, (int64_t)4},
                        {{&bitmap1, &bitmap3}, (int64_t)3}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);

    {
        InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap, TypeIndex::BitMap};
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()}); //18446744073709551615
        BitmapValue empty_bitmap;                                           //test empty

        DataSet data_set = {{{&bitmap1, &bitmap2, &empty_bitmap}, (int64_t)5}, //0,1,33,1024,2019
                            {{&bitmap1, &bitmap2, &bitmap3},
                             (int64_t)7}, //0,1,5,33,1024,2019,18446744073709551615
                            {{&bitmap1, &empty_bitmap, Null()}, Null()},
                            {{&bitmap1, &bitmap3, &bitmap3},
                             (int64_t)6}}; //1,5,33,1024,2019,18446744073709551615

        check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    }
}

TEST(function_bitmap_test, function_bitmap_xor_count) {
    std::string func_name = "bitmap_xor_count";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};

    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({1, 2, 3, 4});
    BitmapValue bitmap3({2, 3});
    BitmapValue bitmap4({1, 2, 6});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1, &empty_bitmap}, (int64_t)3},
                        {{&bitmap2, &bitmap3}, (int64_t)2},
                        {{&bitmap1, &bitmap4}, (int64_t)2}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);

    {
        InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap, TypeIndex::BitMap};
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue empty_bitmap; //test empty

        DataSet data_set = {
                {{&bitmap1, &bitmap2, &empty_bitmap}, (int64_t)5}, //0,1,33,1024,2019
                {{&bitmap1, &bitmap2, &bitmap3}, (int64_t)6}, //0,1,5,1024,2019,18446744073709551615
                {{&bitmap1, &empty_bitmap, Null()}, Null()},
                {{&bitmap1, &bitmap3, &bitmap3}, (int64_t)3}}; //1,1024,2019

        check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    }
}

TEST(function_bitmap_test, function_bitmap_and_not_count) {
    std::string func_name = "bitmap_and_not_count";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};
    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({3, 4, std::numeric_limits<uint64_t>::min()});
    BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
    BitmapValue empty_bitmap;

    DataSet data_set = {{{&bitmap1, &empty_bitmap}, (int64_t)3}, //1,2,3
                        {{&bitmap2, Null()}, Null()},
                        {{&bitmap2, &bitmap3}, (int64_t)3},  //0,3,4
                        {{&bitmap1, &bitmap2}, (int64_t)2}}; //1,2

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
}
TEST(function_bitmap_test, function_bitmap_has_all) {
    std::string func_name = "bitmap_has_all";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};

    BitmapValue bitmap1(
            {1, 4, 5, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::min()});
    BitmapValue bitmap2(
            {4, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::min()});
    BitmapValue bitmap3 = BitmapValue({0, 1, 2});
    BitmapValue bitmap4 = BitmapValue({0, 1, 2, std::numeric_limits<uint64_t>::max()});
    BitmapValue bitmap5 = BitmapValue({0, 1, 2});
    BitmapValue empty_bitmap1;
    BitmapValue empty_bitmap2;

    DataSet data_set = {{{&bitmap1, &bitmap2}, uint8(true)},
                        {{&empty_bitmap1, &empty_bitmap2}, uint8(true)},
                        {{&bitmap3, &bitmap4}, uint8(false)},
                        {{&bitmap4, &bitmap5}, uint8(true)},
                        {{Null(), &empty_bitmap1}, Null()}};

    check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
}

} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
