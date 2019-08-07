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
#include <memory>

#include "olap/rowset/segment_v2/zone_map.h"

namespace doris {

namespace segment_v2 {

class ZoneMapTest : public testing::Test {
public:
    virtual ~ZoneMapTest() { }

    void test_zone_map(FieldType type, const std::string& min,
            const std::string& max, bool is_null) {
        WrapperField* min_value = WrapperField::create_by_type(type);
        if (min == "NULL") {
            min_value->set_null();
        } else {
            min_value->from_string(min);
        }
        
        WrapperField* max_value = WrapperField::create_by_type(type);
        if (max == "NULL") {
            max_value->set_null();
        } else {
            max_value->from_string(max);
        }

        ZoneMap zone_map;
        zone_map.set_min_value(min_value->serialize());
        zone_map.set_max_value(max_value->serialize());
        zone_map.set_is_null(is_null);
        std::string result;
        zone_map.serialize(&result);

        ZoneMap new_zone_map;
        new_zone_map.deserialize(result);
        WrapperField* new_min_value = WrapperField::create_by_type(type);
        new_min_value->deserialize(new_zone_map.min_value());
        if (min == "NULL") {
            ASSERT_TRUE(new_min_value->is_null());
        } else {
            ASSERT_EQ(min, new_min_value->to_string());
        }
        WrapperField* new_max_value = WrapperField::create_by_type(type);
        new_max_value->deserialize(new_zone_map.max_value());
        if (max == "NULL") {
            ASSERT_TRUE(new_max_value->is_null());
        } else {
            ASSERT_EQ(max, new_max_value->to_string());
        }
        
        ASSERT_EQ(is_null, new_zone_map.is_null());
        delete min_value;
        delete max_value;
        delete new_min_value;
        delete new_max_value;
    }
};

// Test for tiny int
TEST_F(ZoneMapTest, NormalTestTinyINT) {
    test_zone_map(OLAP_FIELD_TYPE_TINYINT, "NULL", "NULL", true);
    test_zone_map(OLAP_FIELD_TYPE_TINYINT, "-128", "127", false);
    test_zone_map(OLAP_FIELD_TYPE_TINYINT, "0", "1", false);
}

// Test for small int
TEST_F(ZoneMapTest, NormalTestSmallINT) {
    test_zone_map(OLAP_FIELD_TYPE_SMALLINT, "NULL", "NULL", true);
    test_zone_map(OLAP_FIELD_TYPE_SMALLINT, "-32768", "32767", false);
    test_zone_map(OLAP_FIELD_TYPE_SMALLINT, "0", "1", false);
}

// Test for int
TEST_F(ZoneMapTest, NormalTestINT) {
    test_zone_map(OLAP_FIELD_TYPE_INT, "NULL", "NULL", true);
    test_zone_map(OLAP_FIELD_TYPE_INT, "-2147483648", "2147483647", false);
    test_zone_map(OLAP_FIELD_TYPE_INT, "0", "1", false);
}

// Test for big int
TEST_F(ZoneMapTest, NormalTestBigINT) {
    test_zone_map(OLAP_FIELD_TYPE_BIGINT, "NULL", "NULL", true);
    test_zone_map(OLAP_FIELD_TYPE_BIGINT, "-9223372036854775808", "9223372036854775807", false);
    test_zone_map(OLAP_FIELD_TYPE_BIGINT, "0", "1", false);
}

// Test for large int
TEST_F(ZoneMapTest, NormalTestLargeINT) {
    test_zone_map(OLAP_FIELD_TYPE_LARGEINT, "NULL", "NULL", true);
    test_zone_map(OLAP_FIELD_TYPE_LARGEINT, "-9223372036854775808", "9223372036854775807", false);
    test_zone_map(OLAP_FIELD_TYPE_LARGEINT, "0", "1", false);
}

// Test for varchar
TEST_F(ZoneMapTest, NormalTestVarchar) {
    test_zone_map(OLAP_FIELD_TYPE_VARCHAR, "NULL", "NULL", true);
    test_zone_map(OLAP_FIELD_TYPE_VARCHAR, "haha", "helloworld", false);
}

// Test for char
TEST_F(ZoneMapTest, NormalTestChar) {
    test_zone_map(OLAP_FIELD_TYPE_CHAR, "NULL", "NULL", true);
    test_zone_map(OLAP_FIELD_TYPE_CHAR, "aaaaaaaaaa", "helloworld", false);
}

}

}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
