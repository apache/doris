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

#include <cstdint>
#include <deque>
#include <limits>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "exprs/function/function_test_util.h"
#include "util/url_coding.h"

namespace doris {

using namespace ut_type;

TEST(function_bitmap_test, function_bitmap_min_test) {
    std::string func_name = "bitmap_min";
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP};

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
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP};

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
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP};

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
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BIGINT};

    BitmapValue bitmap1({1, 3});
    BitmapValue bitmap2({1, 3, 5});

    BitmapValue bitmap1_res(1);
    BitmapValue bitmap2_res({1, 3, 5});
    {
        DataSet data_set = {{{&bitmap1, (int64_t)3}, &bitmap1_res},
                            {{&bitmap2, (int64_t)6}, &bitmap2_res},
                            {{&bitmap1, Null()}, Null()}};

        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
}

namespace doris::config {
DECLARE_Bool(enable_set_in_bitmap_value);
}

std::string encode_bitmap_to_base64(const BitmapValue& bitmap) {
    std::string serialized(bitmap.getSizeInBytes(), '\0');
    bitmap.write_to(serialized.data());
    std::string encoded;
    base64_encode(serialized, &encoded);
    return encoded;
}

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
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP};

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
                {{&bitmap32_3}, std::string("AjswAAABAAAgAAEAAAAgAA==")},
                {{&bitmap64_1}, std::string("AwAAAAABAAAA")},
                {{&bitmap64_3},
                 std::string("BAIAAAAAOzAAAAEAAB8AAQAAAB8AAQAAADowAAABAAAAAAAAABAAAAAAAA==")},
                {{&empty_bitmap}, std::string("AA==")},
                {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        // SET serialization depends on hash-set iteration order, so do not pin a hard-coded
        // base64 string here. The contract is that the current serialization can round-trip.
        DataSet data_set = {{{&bitmap32_2}, encode_bitmap_to_base64(bitmap32_2)},
                            {{&bitmap64_2}, encode_bitmap_to_base64(bitmap64_2)}};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        BitmapValue bitmap;
        bitmap.add(0);
        bitmap.add(1);
        bitmap.add(2);
        bitmap.add(3);
        DataSet data_set = {{{&bitmap}, encode_bitmap_to_base64(bitmap)}};
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
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

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
        DataSet data_set = {{{bitmap32_base64_1}, &bitmap32_1}, {{bitmap32_base64_2}, &bitmap32_2},
                            {{bitmap32_base64_3}, &bitmap32_3}, {{bitmap64_base64_1}, &bitmap64_1},
                            {{bitmap64_base64_2}, &bitmap64_2}, {{bitmap64_base64_3}, &bitmap64_3},
                            {{base64_empty}, &empty_bitmap},    {{Null()}, Null()}};

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
        DataSet data_set = {{{bitmap32_base64_1}, &bitmap32_1}, {{bitmap32_base64_2}, &bitmap32_2},
                            {{bitmap32_base64_3}, &bitmap32_3}, {{bitmap64_base64_1}, &bitmap64_1},
                            {{bitmap64_base64_2}, &bitmap64_2}, {{bitmap64_base64_3}, &bitmap64_3},
                            {{base64_empty}, &empty_bitmap},    {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }

    {
        std::string base64("CgIAAAAAAAAAAAAAAAEAAAAAAAAA");
        BitmapValue bitmap;
        bitmap.add(0);
        bitmap.add(1);
        DataSet data_set = {{{base64}, &bitmap}};
        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
    {
        // Historical SET bytes may store the same elements in different orders; decoding must
        // preserve the bitmap contents instead of relying on the serialized payload order.
        std::string set_v1_reversed("BQJ/lpgAAAAAAAEAAAAAAAAA");
        std::string set_v2_reversed("CgIAAAB/lpgAAAAAAAEAAAAAAAAA");
        BitmapValue bitmap({1, 9999999});
        DataSet data_set = {{{set_v1_reversed}, &bitmap}, {{set_v2_reversed}, &bitmap}};
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
        DataSet data_set = {{{base64_32_v1}, &bitmap32_3}, {{base64_64_v1}, &bitmap64_3}};
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
        DataSet data_set = {{{base64_32_v2}, &bitmap32_3}, {{base64_64_v2}, &bitmap64_3}};
        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
}

TEST(function_bitmap_test, function_bitmap_and_count) {
    std::string func_name = "bitmap_and_count";
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP};
    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({3, 4, 5});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1, &empty_bitmap}, (int64_t)0}, {{&bitmap1, &bitmap1}, (int64_t)3},
                        {{&bitmap1, &bitmap2}, (int64_t)1},      {{Null(), &bitmap1}, (int64_t)0},
                        {{&bitmap1, Null()}, (int64_t)0},        {{Null(), Null()}, (int64_t)0}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP,
                                    PrimitiveType::TYPE_BITMAP};
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
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP};

    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({1, 2, 3, 4});
    BitmapValue bitmap3({2, 3});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1, &empty_bitmap}, (int64_t)3}, {{&bitmap2, &bitmap3}, (int64_t)4},
                        {{&bitmap1, &bitmap3}, (int64_t)3},      {{Null(), &bitmap1}, (int64_t)3},
                        {{&bitmap1, Null()}, (int64_t)3},        {{Null(), Null()}, (int64_t)0}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));

    {
        const auto old_config = config::enable_set_in_bitmap_value;
        config::enable_set_in_bitmap_value = true;
        BitmapValue short_bitmap1({1, 2});
        BitmapValue short_bitmap2({2, 3, 4, 5});
        EXPECT_EQ(short_bitmap1.get_type_code(), BitmapTypeCode::SET);
        EXPECT_EQ(short_bitmap2.get_type_code(), BitmapTypeCode::SET);

        DataSet set_data_set = {{{&short_bitmap1, &short_bitmap2}, (int64_t)5}};
        static_cast<void>(
                check_function<DataTypeInt64, true>(func_name, input_types, set_data_set));
        config::enable_set_in_bitmap_value = old_config;
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP,
                                    PrimitiveType::TYPE_BITMAP};
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
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP};

    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({1, 2, 3, 4});
    BitmapValue bitmap3({2, 3});
    BitmapValue bitmap4({1, 2, 6});
    BitmapValue empty_bitmap;
    DataSet data_set = {{{&bitmap1, &empty_bitmap}, (int64_t)3}, {{&bitmap2, &bitmap3}, (int64_t)2},
                        {{&bitmap1, &bitmap4}, (int64_t)2},      {{Null(), &bitmap1}, (int64_t)0},
                        {{&bitmap1, Null()}, (int64_t)0},        {{Null(), Null()}, (int64_t)0}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP,
                                    PrimitiveType::TYPE_BITMAP};
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
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP};
    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({3, 4, std::numeric_limits<uint64_t>::min()});
    BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
    BitmapValue empty_bitmap;

    DataSet data_set = {
            {{&bitmap1, &empty_bitmap}, (int64_t)3}, //1,2,3
            {{Null(), &bitmap1}, (int64_t)0},        {{&bitmap2, Null()}, (int64_t)0},
            {{Null(), Null()}, (int64_t)0},          {{&bitmap2, &bitmap3}, (int64_t)3}, //0,3,4
            {{&bitmap1, &bitmap2}, (int64_t)2}};                                         //1,2

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(function_bitmap_test, function_bitmap_op_count_v2_nullable_args) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP};
    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({3, 4, 5});

    {
        DataSet data_set = {{{&bitmap1, &bitmap2}, (int64_t)1},
                            {{Null(), &bitmap1}, (int64_t)0},
                            {{&bitmap1, Null()}, (int64_t)0},
                            {{Null(), Null()}, (int64_t)0}};
        static_cast<void>(
                check_function<DataTypeInt64, false>("bitmap_and_count_v2", input_types, data_set));
    }
    {
        DataSet data_set = {{{&bitmap1, &bitmap2}, (int64_t)5},
                            {{Null(), &bitmap1}, (int64_t)3},
                            {{&bitmap1, Null()}, (int64_t)3},
                            {{Null(), Null()}, (int64_t)0}};
        static_cast<void>(
                check_function<DataTypeInt64, false>("bitmap_or_count_v2", input_types, data_set));
    }
    {
        DataSet data_set = {{{&bitmap1, &bitmap2}, (int64_t)4},
                            {{Null(), &bitmap1}, (int64_t)0},
                            {{&bitmap1, Null()}, (int64_t)0},
                            {{Null(), Null()}, (int64_t)0}};
        static_cast<void>(
                check_function<DataTypeInt64, false>("bitmap_xor_count_v2", input_types, data_set));
    }
    {
        DataSet data_set = {{{&bitmap1, &bitmap2}, (int64_t)2},
                            {{Null(), &bitmap1}, (int64_t)0},
                            {{&bitmap1, Null()}, (int64_t)0},
                            {{Null(), Null()}, (int64_t)0}};
        static_cast<void>(check_function<DataTypeInt64, false>("bitmap_and_not_count_v2",
                                                               input_types, data_set));
    }
}

TEST(function_bitmap_test, function_bitmap_and_not_count_alias) {
    std::string func_name = "bitmap_andnot_count";
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP};
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
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BITMAP};

    BitmapValue bitmap1(
            {1, 4, 5, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::min()});
    BitmapValue bitmap2(
            {4, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::min()});
    BitmapValue bitmap3 = BitmapValue({0, 1, 2});
    BitmapValue bitmap4 = BitmapValue({0, 1, 2, std::numeric_limits<uint64_t>::max()});
    BitmapValue bitmap5 = BitmapValue({0, 1, 2});
    BitmapValue empty_bitmap1;
    BitmapValue empty_bitmap2;

    DataSet data_set = {{{&bitmap1, &bitmap2}, uint8_t(true)},
                        {{&empty_bitmap1, &empty_bitmap2}, uint8_t(true)},
                        {{&bitmap3, &bitmap4}, uint8_t(false)},
                        {{&bitmap4, &bitmap5}, uint8_t(true)},
                        {{Null(), &empty_bitmap1}, Null()}};

    static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(function_bitmap_test, function_bitmap_from_string) {
    std::string func_name = "bitmap_from_string";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    BitmapValue bitmap1(1);
    BitmapValue bitmap2({1, 2, 3});
    BitmapValue bitmap3({1, 9999999});
    BitmapValue empty_bitmap;
    BitmapValue bitmap4({0, 1, 2, 3, 4, 5});
    BitmapValue bitmap5({10, 20, 30});

    DataSet data_set = {{{std::string("1")}, &bitmap1},
                        {{std::string("1,2,3")}, &bitmap2},
                        {{std::string("1,9999999")}, &bitmap3},
                        {{std::string("")}, &empty_bitmap},
                        {{std::string("0,1,2,3,4,5")}, &bitmap4},
                        {{std::string("10,20,30")}, &bitmap5},
                        {{std::string("1,abc,3")}, Null()},
                        {{std::string("invalid")}, Null()},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
}

TEST(function_bitmap_test, function_bitmap_from_string_multiple_rows) {
    std::string func_name = "bitmap_from_string";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    BitmapValue bitmap1({1, 2, 3});
    BitmapValue bitmap2({4, 5, 6});
    BitmapValue bitmap3({7, 8, 9});
    BitmapValue bitmap4({10, 11, 12});
    BitmapValue bitmap5({13, 14, 15});
    BitmapValue bitmap6({18, 19, 20});

    DataSet data_set = {
            {{std::string("1,2,3")}, &bitmap1},    {{std::string("4,5,6")}, &bitmap2},
            {{std::string("7,8,9")}, &bitmap3},    {{std::string("10,11,12")}, &bitmap4},
            {{std::string("13,14,15")}, &bitmap5}, {{std::string("16,invalid,17")}, Null()},
            {{std::string("18,19,20")}, &bitmap6}};

    static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
}

namespace {
// Keep a byte buffer alive and hand back a non-owning VARBINARY view over it.
// ut_type::VARBINARY is a StringView that does not own its bytes and StringView rejects
// std::string&&, so dynamic payloads must be parked somewhere stable for the lifetime of
// the test process; check_function copies the bytes into the input column immediately.
// deque never relocates its elements (unlike vector), so a view over an SSO string inside it
// stays valid across later insertions.
ut_type::VARBINARY keep_alive_binary(std::string bytes) {
    static std::deque<std::string> keep_alive;
    keep_alive.push_back(std::move(bytes));
    return ut_type::VARBINARY(keep_alive.back().data(), keep_alive.back().size());
}

// Serialize a bitmap to the Doris wire format, used to build bitmap_from_binary inputs and
// bitmap_to_binary expectations without hard-coding byte strings (which depend on
// enable_set_in_bitmap_value / bitmap_serialize_version, see function_bitmap_to_base64).
ut_type::VARBINARY serialize_bitmap_to_binary(const BitmapValue& bitmap) {
    std::string serialized(bitmap.getSizeInBytes(), '\0');
    bitmap.write_to(serialized.data());
    return keep_alive_binary(std::move(serialized));
}
} // namespace

TEST(function_bitmap_test, function_bitmap_to_binary) {
    config::Register::Field field("bool", "enable_set_in_bitmap_value",
                                  &config::enable_set_in_bitmap_value, "false", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("enable_set_in_bitmap_value"), field));
    config::Register::Field field_ser_ver("int16_t", "bitmap_serialize_version",
                                          &config::bitmap_serialize_version, "1", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("bitmap_serialize_version"), field_ser_ver));

    std::string func_name = "bitmap_to_binary";
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP};

    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "false", false, true).ok());
    BitmapValue bitmap32_1(1);                         // single
    BitmapValue bitmap32_2({1, 9999999});              // bitmap
    BitmapValue bitmap64_1((uint64_t)4294967296);      // single
    BitmapValue bitmap64_2({1, (uint64_t)4294967296}); // bitmap
    BitmapValue empty_bitmap;

    {
        DataSet data_set = {{{&bitmap32_1}, VARBINARY(serialize_bitmap_to_binary(bitmap32_1))},
                            {{&bitmap32_2}, VARBINARY(serialize_bitmap_to_binary(bitmap32_2))},
                            {{&bitmap64_1}, VARBINARY(serialize_bitmap_to_binary(bitmap64_1))},
                            {{&bitmap64_2}, VARBINARY(serialize_bitmap_to_binary(bitmap64_2))},
                            {{&empty_bitmap}, VARBINARY(serialize_bitmap_to_binary(empty_bitmap))},
                            {{Null()}, Null()}};

        static_cast<void>(
                check_function<DataTypeVarbinary, true>(func_name, input_types, data_set));
    }

    // SET serialization order is not canonical (see function_bitmap_to_base64), so the
    // expectation is built from the same serialize helper, keeping the round-trip contract.
    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "true", false, true).ok());
    BitmapValue set_bitmap({1, 9999999});
    {
        EXPECT_EQ(set_bitmap.get_type_code(), BitmapTypeCode::SET);
        DataSet data_set = {{{&set_bitmap}, VARBINARY(serialize_bitmap_to_binary(set_bitmap))},
                            {{Null()}, Null()}};
        static_cast<void>(
                check_function<DataTypeVarbinary, true>(func_name, input_types, data_set));
    }

    // serialize version 2 emits BITMAP32_V2 / BITMAP64_V2 type codes; the round trip must hold.
    // enable_set_in_bitmap_value was flipped to true above, so reset it first: with it on, a
    // 6-element bitmap serializes as SET and the V2 bitmap-code assertion below would not hold.
    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "false", false, true).ok());
    EXPECT_TRUE(config::set_config("bitmap_serialize_version", "2", false, true).ok());
    BitmapValue bitmap32_3({0, 1, 2, 3, 4, 5});
    {
        EXPECT_EQ(bitmap32_3.get_type_code(), BitmapTypeCode::BITMAP32_V2);
        DataSet data_set = {{{&bitmap32_3}, VARBINARY(serialize_bitmap_to_binary(bitmap32_3))}};
        static_cast<void>(
                check_function<DataTypeVarbinary, true>(func_name, input_types, data_set));
    }
    EXPECT_TRUE(config::set_config("bitmap_serialize_version", "1", false, true).ok());
}

TEST(function_bitmap_test, function_bitmap_from_binary) {
    config::Register::Field field("bool", "enable_set_in_bitmap_value",
                                  &config::enable_set_in_bitmap_value, "false", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("enable_set_in_bitmap_value"), field));
    config::Register::Field field_ser_ver("int16_t", "bitmap_serialize_version",
                                          &config::bitmap_serialize_version, "1", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("bitmap_serialize_version"), field_ser_ver));

    std::string func_name = "bitmap_from_binary";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY};

    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "false", false, true).ok());
    BitmapValue bitmap32_1(1);                         // single
    BitmapValue bitmap32_2({1, 9999999});              // bitmap
    BitmapValue bitmap64_1((uint64_t)4294967296);      // single
    BitmapValue bitmap64_2({1, (uint64_t)4294967296}); // bitmap
    BitmapValue empty_bitmap;

    {
        // valid payloads round-trip back to the original bitmaps
        DataSet data_set = {{{VARBINARY(serialize_bitmap_to_binary(bitmap32_1))}, &bitmap32_1},
                            {{VARBINARY(serialize_bitmap_to_binary(bitmap32_2))}, &bitmap32_2},
                            {{VARBINARY(serialize_bitmap_to_binary(bitmap64_1))}, &bitmap64_1},
                            {{VARBINARY(serialize_bitmap_to_binary(bitmap64_2))}, &bitmap64_2},
                            {{VARBINARY(serialize_bitmap_to_binary(empty_bitmap))}, &empty_bitmap},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }

    {
        // invalid / truncated user-controlled bytes must yield NULL, never crash:
        // - empty payload (no type code at all)
        // - unknown type code
        // - SINGLE32 / SINGLE64 / SET payloads cut short
        // - BITMAP32 roaring payload truncated
        std::string single32 = serialize_bitmap_to_binary(bitmap32_1);
        std::string single64 = serialize_bitmap_to_binary(bitmap64_1);
        std::string set_bytes = [&] {
            config::enable_set_in_bitmap_value = true;
            BitmapValue set_bitmap;
            set_bitmap.add(1);
            set_bitmap.add(9999999);
            std::string serialized(set_bitmap.getSizeInBytes(), '\0');
            set_bitmap.write_to(serialized.data());
            config::enable_set_in_bitmap_value = false;
            return serialized;
        }();
        std::string bitmap32 = serialize_bitmap_to_binary(bitmap32_2);

        auto truncated = [](std::string s, size_t n) { return keep_alive_binary(s.substr(0, n)); };

        // A run container with zero runs: the CRoaring 2.1.2 bundled with Doris
        // predates upstream PR #560, so its internal_validate accepts the empty
        // container while every container operation (AND/OR/ANDNOT) dereferences
        // its NULL runs pointer and crashes. valid_and_deserialize must reject it
        // up front (NULL), never load it.
        auto empty_run = [&] {
            std::string bytes;
            bytes += static_cast<char>(BitmapTypeCode::BITMAP32);
            uint32_t cookie = 12347; // SERIAL_COOKIE (hasrun), size-1 = 0
            bytes.append(reinterpret_cast<const char*>(&cookie), 4);
            bytes += '\x01';                      // run bitmap: container 0 IS a run container
            uint16_t key = 0, card_minus_one = 0; // thiscard = 1 -> run container
            bytes.append(reinterpret_cast<const char*>(&key), 2);
            bytes.append(reinterpret_cast<const char*>(&card_minus_one), 2);
            uint16_t n_runs = 0; // the empty container
            bytes.append(reinterpret_cast<const char*>(&n_runs), 2);
            return keep_alive_binary(std::move(bytes));
        }();

        DataSet data_set = {{{VARBINARY("")}, Null()},
                            {{keep_alive_binary(std::string("\x7f", 1))}, Null()},
                            {{truncated(single32, 3)}, Null()},
                            {{truncated(single64, 5)}, Null()},
                            {{truncated(set_bytes, 3)}, Null()},
                            {{truncated(bitmap32, 4)}, Null()},
                            {{empty_run}, Null()}};

        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }

    // SET payloads (from enable_set_in_bitmap_value=true writers) must decode to the same
    // bitmap regardless of the reader's enable_set_in_bitmap_value setting.
    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "true", false, true).ok());
    ut_type::VARBINARY set_payload;
    {
        // build via repeated add(): the vector ctor goes through add_many which always
        // produces BITMAP, while SET is the <=32-element type formed by add()
        BitmapValue set_bitmap;
        set_bitmap.add(1);
        set_bitmap.add(9999999);
        EXPECT_EQ(set_bitmap.get_type_code(), BitmapTypeCode::SET);
        set_payload = serialize_bitmap_to_binary(set_bitmap);
        BitmapValue set_result({1, 9999999});
        DataSet data_set = {{{set_payload}, &set_result}};
        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "false", false, true).ok());
    {
        // the same SET payload, read with set disabled: converts to BITMAP internally,
        // contents must stay equal
        BitmapValue set_result({1, 9999999});
        DataSet data_set = {{{set_payload}, &set_result}};
        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }

    // serialize version 2 payloads (BITMAP32_V2 / BITMAP64_V2) round-trip too
    EXPECT_TRUE(config::set_config("bitmap_serialize_version", "2", false, true).ok());
    {
        BitmapValue bitmap32_3({0, 1, 2, 3, 4, 5});
        BitmapValue bitmap64_3({1, (uint64_t)4294967296});
        EXPECT_EQ(bitmap32_3.get_type_code(), BitmapTypeCode::BITMAP32_V2);
        EXPECT_EQ(bitmap64_3.get_type_code(), BitmapTypeCode::BITMAP64_V2);
        DataSet data_set = {{{VARBINARY(serialize_bitmap_to_binary(bitmap32_3))}, &bitmap32_3},
                            {{VARBINARY(serialize_bitmap_to_binary(bitmap64_3))}, &bitmap64_3}};
        static_cast<void>(check_function<DataTypeBitMap, true>(func_name, input_types, data_set));
    }
    EXPECT_TRUE(config::set_config("bitmap_serialize_version", "1", false, true).ok());
}

} // namespace doris
