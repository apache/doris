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

#include "olap/rowset/segment_v2/column_zone_map.h"

namespace doris {
namespace segment_v2 {

class ColumnZoneMapTest : public testing::Test {
public:
    void test_string(FieldType type) {
        TypeInfo *type_info = get_type_info(OLAP_FIELD_TYPE_CHAR);
        ColumnZoneMapBuilder builder(type_info);
        std::vector<std::string> values1 = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"};
        for (auto value : values1) {
            builder.add((const uint8_t*)&value, 1);
        }
        builder.flush();
        std::vector<std::string> values2 = {"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff"};
        for (auto value : values2) {
            builder.add((const uint8_t*)&value, 1);
        }
        builder.add(nullptr, 1);
        builder.flush();
        for (int i = 0; i < 6; ++i) {
            builder.add(nullptr, 1);
        }
        builder.flush();
        Slice zone_map_page = builder.finish();
        ColumnZoneMap column_zone_map(zone_map_page);
        Status status = column_zone_map.load();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(3, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.get_column_zone_map();
        ASSERT_EQ(3, zone_maps.size());
        ASSERT_EQ("aaaa", zone_maps[0].min());
        ASSERT_EQ("ffff", zone_maps[0].max());
        ASSERT_EQ(false, zone_maps[0].has_null());
        ASSERT_EQ(true, zone_maps[0].has_not_null());

        ASSERT_EQ("aaaaa", zone_maps[1].min());
        ASSERT_EQ("fffff", zone_maps[1].max());
        ASSERT_EQ(true, zone_maps[1].has_null());
        ASSERT_EQ(true, zone_maps[1].has_not_null());

        ASSERT_EQ(true, zone_maps[2].has_null());
        ASSERT_EQ(false, zone_maps[2].has_not_null());
    }
};

// Test for int
TEST_F(ColumnZoneMapTest, NormalTestIntPage) {
    TypeInfo* type_info = get_type_info(OLAP_FIELD_TYPE_INT);
    ColumnZoneMapBuilder builder(type_info);
    std::vector<int> values1 = {1, 10, 11, 20, 21, 22};
    for (auto value : values1) {
        builder.add((const uint8_t*)&value, 1);
    }
    builder.flush();
    std::vector<int> values2 = {2, 12, 31, 23, 21, 22};
    for (auto value : values2) {
        builder.add((const uint8_t*)&value, 1);
    }
    builder.add(nullptr, 1);
    builder.flush();
    for (int i = 0; i < 6; ++i) {
        builder.add(nullptr, 1);
    }
    builder.flush();
    Slice zone_map_page = builder.finish();
    ColumnZoneMap column_zone_map(zone_map_page);
    Status status = column_zone_map.load();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(3, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.get_column_zone_map();
    ASSERT_EQ(3, zone_maps.size());

    ASSERT_EQ(std::to_string(1), zone_maps[0].min());
    ASSERT_EQ(std::to_string(22), zone_maps[0].max());
    ASSERT_EQ(false, zone_maps[0].has_null());
    ASSERT_EQ(true, zone_maps[0].has_not_null());

    ASSERT_EQ(std::to_string(2), zone_maps[1].min());
    ASSERT_EQ(std::to_string(31), zone_maps[1].max());
    ASSERT_EQ(true, zone_maps[1].has_null());
    ASSERT_EQ(true, zone_maps[1].has_not_null());

    ASSERT_EQ(true, zone_maps[2].has_null());
    ASSERT_EQ(false, zone_maps[2].has_not_null());
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestVarcharPage) {
    test_string(OLAP_FIELD_TYPE_VARCHAR);
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestCharPage) {
    test_string(OLAP_FIELD_TYPE_CHAR);
}

}
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
