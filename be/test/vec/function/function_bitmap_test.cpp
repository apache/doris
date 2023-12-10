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
#include <stdint.h>

#include <cstdint>
#include <limits>
#include <numeric>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "gutil/integral_types.h"
#include "testutil/any_type.h"
#include "util/bitmap_value.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

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

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
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

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
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

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_bitmap_test, function_bitmap_remove) {
    std::string func_name = "bitmap_remove";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::Int64};

    BitmapValue bitmap1({1, 3});
    BitmapValue bitmap2({1, 3, 5});

    BitmapValue bitmap1_res(1);
    BitmapValue bitmap2_res({1, 3, 5});
    {
        DataSet data_set = {{{&bitmap1, (int64_t)3}, bitmap1_res},
                            {{&bitmap2, (int64_t)6}, bitmap2_res},
                            {{&bitmap1, Null()}, Null()}};

        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
}
namespace doris {
namespace config {
DECLARE_Bool(enable_set_in_bitmap_value);
}
} // namespace doris
TEST(function_bitmap_test, function_bitmap_to_base64) {
    config::Register::Field field("bool", "enable_set_in_bitmap_value",
                                  &config::enable_set_in_bitmap_value, "false", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("enable_set_in_bitmap_value"), field));

    config::Register::Field field_ser_ver("int16_t", "bitmap_serialize_version",
                                          &config::bitmap_serialize_version, "1", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("bitmap_serialize_version"), field_ser_ver));

    std::string func_name = "bitmap_to_base64";
    InputTypeSet input_types = {TypeIndex::BitMap};

    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "false", false, true).ok());
    std::vector<uint64_t> bits32 {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                  12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                  24, 25, 26, 27, 28, 29, 30, 31, 32}; // SET_TYPE_THRESHOLD + 1
    std::vector<uint64_t> bits64 {
            0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
            11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            22, 23, 24, 25, 26, 27, 28, 29, 30, 31, (uint64_t)4294967296}; // SET_TYPE_THRESHOLD + 1

    BitmapValue bitmap32_1(1);            // single
    BitmapValue bitmap32_2({1, 9999999}); // bitmap
    BitmapValue bitmap32_3(bits32);       // bitmap

    BitmapValue bitmap64_1((uint64_t)4294967296);      // single
    BitmapValue bitmap64_2({1, (uint64_t)4294967296}); // bitmap
    BitmapValue bitmap64_3(bits64);                    // bitmap

    BitmapValue empty_bitmap;

    EXPECT_EQ(bitmap32_1.get_type_code(), BitmapTypeCode::SINGLE32);
    EXPECT_EQ(bitmap32_2.get_type_code(), BitmapTypeCode::BITMAP32);
    EXPECT_EQ(bitmap32_3.get_type_code(), BitmapTypeCode::BITMAP32);

    EXPECT_EQ(bitmap64_1.get_type_code(), BitmapTypeCode::SINGLE64);
    EXPECT_EQ(bitmap64_2.get_type_code(), BitmapTypeCode::BITMAP64);
    EXPECT_EQ(bitmap64_3.get_type_code(), BitmapTypeCode::BITMAP64);

    {
        DataSet data_set = {
                {{&bitmap32_1}, std::string("AQEAAAA=")},
                {{&bitmap32_2}, std::string("AjowAAACAAAAAAAAAJgAAAAYAAAAGgAAAAEAf5Y=")},
                {{&bitmap32_3}, std::string("AjswAAABAAAgAAEAAAAgAA==")},
                {{&bitmap64_1}, std::string("AwAAAAABAAAA")},
                {{&bitmap64_2},
                 std::string("BAIAAAAAOjAAAAEAAAAAAAAAEAAAAAEAAQAAADowAAABAAAAAAAAABAAAAAAAA==")},
                {{&bitmap64_3},
                 std::string("BAIAAAAAOzAAAAEAAB8AAQAAAB8AAQAAADowAAABAAAAAAAAABAAAAAAAA==")},
                {{&empty_bitmap}, std::string("AA==")},
                {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "true", false, true).ok());
    bitmap32_1 = BitmapValue(1);            // single
    bitmap32_2 = BitmapValue({1, 9999999}); // set
    bitmap32_3 = BitmapValue(bits32);       // bitmap

    bitmap64_1 = BitmapValue((uint64_t)4294967296);      // single
    bitmap64_2 = BitmapValue({1, (uint64_t)4294967296}); // set
    bitmap64_3 = BitmapValue(bits64);                    // bitmap

    EXPECT_EQ(bitmap32_1.get_type_code(), BitmapTypeCode::SINGLE32);
    EXPECT_EQ(bitmap32_2.get_type_code(), BitmapTypeCode::SET);
    EXPECT_EQ(bitmap32_3.get_type_code(), BitmapTypeCode::BITMAP32);

    EXPECT_EQ(bitmap64_1.get_type_code(), BitmapTypeCode::SINGLE64);
    EXPECT_EQ(bitmap64_2.get_type_code(), BitmapTypeCode::SET);
    EXPECT_EQ(bitmap64_3.get_type_code(), BitmapTypeCode::BITMAP64);

    {
        DataSet data_set = {
                {{&bitmap32_1}, std::string("AQEAAAA=")},
                {{&bitmap32_2}, std::string("BQIBAAAAAAAAAH+WmAAAAAAA")},
                {{&bitmap32_3}, std::string("AjswAAABAAAgAAEAAAAgAA==")},
                {{&bitmap64_1}, std::string("AwAAAAABAAAA")},
                {{&bitmap64_2}, std::string("BQIAAAAAAQAAAAEAAAAAAAAA")},
                {{&bitmap64_3},
                 std::string("BAIAAAAAOzAAAAEAAB8AAQAAAB8AAQAAADowAAABAAAAAAAAABAAAAAAAA==")},
                {{&empty_bitmap}, std::string("AA==")},
                {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        std::string base64("BQQAAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAADAAAAAAAAAA==");
        BitmapValue bitmap;
        bitmap.add(0);
        bitmap.add(1);
        bitmap.add(2);
        bitmap.add(3);
        DataSet data_set = {{{&bitmap}, base64}};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    // test bitmap serialize version2
    EXPECT_TRUE(config::set_config("bitmap_serialize_version", "2", false, true).ok());
    bitmap32_3 = BitmapValue(bits32); // bitmap32
    bitmap64_3 = BitmapValue(bits64); // bitmap64
    EXPECT_EQ(bitmap32_3.get_type_code(), BitmapTypeCode::BITMAP32_V2);
    EXPECT_EQ(bitmap64_3.get_type_code(), BitmapTypeCode::BITMAP64_V2);

    {
        DataSet data_set = {
                {{&bitmap32_3}, std::string("DAI7MAAAAQAAIAABAAAAIAA=")},
                {{&bitmap64_3}, std::string("DQIAAAAAAjswAAABAAAfAAEAAAAfAAEAAAABAQAAAAAAAAA=")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_bitmap_test, function_bitmap_from_base64) {
    config::Register::Field field("bool", "enable_set_in_bitmap_value",
                                  &config::enable_set_in_bitmap_value, "false", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("enable_set_in_bitmap_value"), field));

    config::Register::Field field_ser_ver("int16_t", "bitmap_serialize_version",
                                          &config::bitmap_serialize_version, "1", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("bitmap_serialize_version"), field_ser_ver));

    std::string func_name = "bitmap_from_base64";
    InputTypeSet input_types = {TypeIndex::String};

    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "false", false, true).ok());
    std::string bitmap32_base64_1("AQEAAAA=");
    std::string bitmap32_base64_2("AjowAAACAAAAAAAAAJgAAAAYAAAAGgAAAAEAf5Y=");
    std::string bitmap32_base64_3("AjswAAABAAAgAAEAAAAgAA==");

    std::string bitmap64_base64_1("AwAAAAABAAAA");
    std::string bitmap64_base64_2(
            "BAIAAAAAOjAAAAEAAAAAAAAAEAAAAAEAAQAAADowAAABAAAAAAAAABAAAAAAAA==");
    std::string bitmap64_base64_3("BAIAAAAAOzAAAAEAAB8AAQAAAB8AAQAAADowAAABAAAAAAAAABAAAAAAAA==");
    std::string base64_empty("AA==");

    std::vector<uint64_t> bits32 {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                  12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                  24, 25, 26, 27, 28, 29, 30, 31, 32}; // SET_TYPE_THRESHOLD + 1
    std::vector<uint64_t> bits64 {
            0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
            11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            22, 23, 24, 25, 26, 27, 28, 29, 30, 31, (uint64_t)4294967296}; // SET_TYPE_THRESHOLD + 1

    BitmapValue bitmap32_1(1);            // single
    BitmapValue bitmap32_2({1, 9999999}); // bitmap
    BitmapValue bitmap32_3(bits32);       // bitmap

    BitmapValue bitmap64_1((uint64_t)4294967296);      // single
    BitmapValue bitmap64_2({1, (uint64_t)4294967296}); // bitmap
    BitmapValue bitmap64_3(bits64);                    // bitmap

    BitmapValue empty_bitmap;
    {
        DataSet data_set = {{{bitmap32_base64_1}, bitmap32_1}, {{bitmap32_base64_2}, bitmap32_2},
                            {{bitmap32_base64_3}, bitmap32_3}, {{bitmap64_base64_1}, bitmap64_1},
                            {{bitmap64_base64_2}, bitmap64_2}, {{bitmap64_base64_3}, bitmap64_3},
                            {{base64_empty}, empty_bitmap},    {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }

    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "true", false, true).ok());
    bitmap32_base64_1 = ("AQEAAAA=");
    bitmap32_base64_2 = ("BQIBAAAAAAAAAH+WmAAAAAAA");
    bitmap32_base64_3 = ("AjswAAABAAAgAAEAAAAgAA==");

    bitmap64_base64_1 = ("AwAAAAABAAAA");
    bitmap64_base64_2 = ("BQIAAAAAAQAAAAEAAAAAAAAA");
    bitmap64_base64_3 = ("BAIAAAAAOzAAAAEAAB8AAQAAAB8AAQAAADowAAABAAAAAAAAABAAAAAAAA==");

    {
        DataSet data_set = {{{bitmap32_base64_1}, bitmap32_1}, {{bitmap32_base64_2}, bitmap32_2},
                            {{bitmap32_base64_3}, bitmap32_3}, {{bitmap64_base64_1}, bitmap64_1},
                            {{bitmap64_base64_2}, bitmap64_2}, {{bitmap64_base64_3}, bitmap64_3},
                            {{base64_empty}, empty_bitmap},    {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }

    {
        std::string base64("CgIAAAAAAAAAAAAAAAEAAAAAAAAA");
        BitmapValue bitmap;
        bitmap.add(0);
        bitmap.add(1);
        DataSet data_set = {{{base64}, bitmap}};
        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
    {
        EXPECT_TRUE(config::set_config("bitmap_serialize_version", "1", false, true).ok());

        std::string base64_32_v1(
                "AjowAAABAAAAAAAgABAAAAAAAAEAAgADAAQABQAGAAcACAAJAAoACwAMAA0ADgAPABAAEQASABMAFAAVAB"
                "YAFwAYABkAGgAbABwAHQAeAB8AIAA=");
        std::string base64_64_v1(
                "BAIAAAAAOjAAAAEAAAAAAB8AEAAAAAAAAQACAAMABAAFAAYABwAIAAkACgALAAwADQAOAA8AEAARABIAEw"
                "AUABUAFgAXABgAGQAaABsAHAAdAB4AHwABAAAAOjAAAAEAAAAAAAAAEAAAAAAA");
        DataSet data_set = {{{base64_32_v1}, bitmap32_3}, {{base64_64_v1}, bitmap64_3}};
        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
    {
        EXPECT_TRUE(config::set_config("bitmap_serialize_version", "2", false, true).ok());

        std::string base64_32_v2(
                "DAI6MAAAAQAAAAAAIAAQAAAAAAABAAIAAwAEAAUABgAHAAgACQAKAAsADAANAA4ADwAQABEAEgATABQAFQ"
                "AWABcAGAAZABoAGwAcAB0AHgAfACAA");
        std::string base64_64_v2(
                "DQIAAAAAAjowAAABAAAAAAAfABAAAAAAAAEAAgADAAQABQAGAAcACAAJAAoACwAMAA0ADgAPABAAEQASAB"
                "MAFAAVABYAFwAYABkAGgAbABwAHQAeAB8AAQAAAAEBAAAAAAAAAA==");
        DataSet data_set = {{{base64_32_v2}, bitmap32_3}, {{base64_64_v2}, bitmap64_3}};
        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
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

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));

    {
        InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap, TypeIndex::BitMap};
        BitmapValue bitmap1({33, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue empty_bitmap; //test empty

        DataSet data_set = {{{&bitmap1, &bitmap2, &empty_bitmap}, (int64_t)0},
                            {{&bitmap1, &bitmap2, &bitmap3}, (int64_t)1}, //33
                            {{&bitmap1, &bitmap2, Null()}, (int64_t)0},
                            {{&bitmap1, &bitmap3, &bitmap3}, (int64_t)1}}; //33

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
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

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));

    {
        InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap, TypeIndex::BitMap};
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()}); //18446744073709551615
        BitmapValue empty_bitmap;                                           //test empty

        DataSet data_set = {{{&bitmap1, &bitmap2, &empty_bitmap}, (int64_t)5}, //0,1,33,1024,2019
                            {{&bitmap1, &bitmap2, &bitmap3},
                             (int64_t)7}, //0,1,5,33,1024,2019,18446744073709551615
                            {{&bitmap1, &empty_bitmap, Null()}, (int64_t)3},
                            {{&bitmap1, &bitmap3, &bitmap3},
                             (int64_t)6}}; //1,5,33,1024,2019,18446744073709551615

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
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

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));

    {
        InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap, TypeIndex::BitMap};
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue empty_bitmap; //test empty

        DataSet data_set = {
                {{&bitmap1, &bitmap2, &empty_bitmap}, (int64_t)5}, //0,1,33,1024,2019
                {{&bitmap1, &bitmap2, &bitmap3}, (int64_t)6}, //0,1,5,1024,2019,18446744073709551615
                {{&bitmap1, &empty_bitmap, Null()}, (int64_t)0},
                {{&bitmap1, &bitmap3, &bitmap3}, (int64_t)3}}; //1,1024,2019

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
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
                        {{&bitmap2, Null()}, (int64_t)0},
                        {{&bitmap2, &bitmap3}, (int64_t)3},  //0,3,4
                        {{&bitmap1, &bitmap2}, (int64_t)2}}; //1,2

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}
TEST(function_bitmap_test, function_bitmap_and_not_count_alias) {
    std::string func_name = "bitmap_andnot_count";
    InputTypeSet input_types = {TypeIndex::BitMap, TypeIndex::BitMap};
    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({3, 4, std::numeric_limits<uint64_t>::min()});
    BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
    BitmapValue empty_bitmap;

    DataSet data_set = {{{&bitmap1, &empty_bitmap}, (int64_t)3}, //1,2,3
                        {{&bitmap2, Null()}, (int64_t)0},
                        {{&bitmap2, &bitmap3}, (int64_t)3},  //0,3,4
                        {{&bitmap1, &bitmap2}, (int64_t)2}}; //1,2

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
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

    static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
