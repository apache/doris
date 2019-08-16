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
    void test_column_zone_map(FieldType type, const std::vector<std::string>& values) {
        ColumnZoneMapBuilder builder;
        WrapperField *min_value = WrapperField::create_by_type(type);
        WrapperField *max_value = WrapperField::create_by_type(type);
        ZoneMapPB zone_map;
        zone_map.set_null_flag(false);

        min_value->from_string(values[0]);
        max_value->from_string(values[1]);
        zone_map.set_min(min_value->serialize());
        zone_map.set_max(max_value->serialize());
        builder.append_entry(zone_map);

        min_value->from_string(values[2]);
        max_value->from_string(values[3]);
        zone_map.set_min(min_value->serialize());
        zone_map.set_max(max_value->serialize());
        builder.append_entry(zone_map);

        min_value->from_string(values[4]);
        max_value->from_string(values[5]);
        zone_map.set_min(min_value->serialize());
        zone_map.set_max(max_value->serialize());
        builder.append_entry(zone_map);

        Slice data = builder.finish();

        ColumnZoneMap column_zone_map(data);
        Status status = column_zone_map.load();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(3, column_zone_map.num_pages());

        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.get_column_zone_map();
        ASSERT_EQ(3, zone_maps.size());

        min_value->deserialize(zone_maps[0].min());
        ASSERT_EQ(values[0], min_value->to_string());
        max_value->deserialize(zone_maps[0].max());
        ASSERT_EQ(values[1], max_value->to_string());

        min_value->deserialize(zone_maps[1].min());
        ASSERT_EQ(values[2], min_value->to_string());
        max_value->deserialize(zone_maps[1].max());
        ASSERT_EQ(values[3], max_value->to_string());

        min_value->deserialize(zone_maps[2].min());
        ASSERT_EQ(values[4], min_value->to_string());
        max_value->deserialize(zone_maps[2].max());
        ASSERT_EQ(values[5], max_value->to_string());

        delete min_value;
        delete max_value;
    }
};

// Test for int
TEST_F(ColumnZoneMapTest, NormalTestIntPage) {
    std::vector<std::string> values = {"1", "10", "11", "20", "21", "22"};
    test_column_zone_map(OLAP_FIELD_TYPE_INT, values);
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestVarcharPage) {
    std::vector<std::string> values = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"};
    test_column_zone_map(OLAP_FIELD_TYPE_VARCHAR, values);
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestCharPage) {
    std::vector<std::string> values = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"};
    test_column_zone_map(OLAP_FIELD_TYPE_CHAR, values);
}

}
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
