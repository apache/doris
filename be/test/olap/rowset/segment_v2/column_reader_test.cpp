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

#include "olap/rowset/segment_v2/column_reader.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <vector>

#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "runtime/define_primitive_type.h"

namespace doris::segment_v2 {

class ColumnReaderWriterTest : public testing::Test {
public:
    ColumnReaderWriterTest() = default;
    ~ColumnReaderWriterTest() override = default;
};

TEST_F(ColumnReaderWriterTest, merge_zone_maps) {
    std::vector<ZoneMapPB> zone_maps;
    ZoneMapPB merged_zone_map;

    zone_maps.emplace_back();
    *(zone_maps.back().mutable_min()) = "apple";
    *(zone_maps.back().mutable_max()) = "banana";

    ASSERT_EQ(zone_maps.back().min(), "apple");
    ASSERT_EQ(zone_maps.back().max(), "banana");
    zone_maps.back().set_has_null(false);
    zone_maps.back().set_pass_all(false);

    auto st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                            merged_zone_map);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_FALSE(merged_zone_map.has_not_null());

    zone_maps.back().set_has_not_null(true);

    ZoneMapPB merged_zone_map2;
    st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                       merged_zone_map2);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_FALSE(merged_zone_map2.has_null());
    ASSERT_TRUE(merged_zone_map2.has_not_null());
    ASSERT_FALSE(merged_zone_map2.pass_all());
    ASSERT_EQ(merged_zone_map2.min(), "apple");
    ASSERT_EQ(merged_zone_map2.max(), "banana");

    zone_maps.emplace_back();
    *(zone_maps.back().mutable_min()) = "able";
    *(zone_maps.back().mutable_max()) = "bad";
    zone_maps.back().set_has_not_null(true);
    zone_maps.back().set_pass_all(false);

    ZoneMapPB merged_zone_map3;
    st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                       merged_zone_map3);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_FALSE(merged_zone_map3.has_null());
    ASSERT_TRUE(merged_zone_map3.has_not_null());
    ASSERT_FALSE(merged_zone_map3.pass_all());
    ASSERT_EQ(merged_zone_map3.min(), "able");
    ASSERT_EQ(merged_zone_map3.max(), "banana");

    zone_maps.emplace_back();
    *(zone_maps.back().mutable_min()) = "xxxx";
    *(zone_maps.back().mutable_max()) = "yyyy";
    zone_maps.back().set_has_not_null(true);
    zone_maps.back().set_pass_all(false);

    ZoneMapPB merged_zone_map4;
    st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                       merged_zone_map4);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_FALSE(merged_zone_map4.has_null());
    ASSERT_TRUE(merged_zone_map4.has_not_null());
    ASSERT_FALSE(merged_zone_map4.pass_all());
    ASSERT_EQ(merged_zone_map4.min(), "able");
    ASSERT_EQ(merged_zone_map4.max(), "yyyy");

    zone_maps.emplace_back();
    *(zone_maps.back().mutable_min()) = "";
    *(zone_maps.back().mutable_max()) = "yyyy";
    zone_maps.back().set_has_not_null(true);
    zone_maps.back().set_has_null(false);
    zone_maps.back().set_pass_all(false);

    ZoneMapPB merged_zone_map5;
    st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                       merged_zone_map5);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_FALSE(merged_zone_map5.has_null());
    ASSERT_TRUE(merged_zone_map5.has_not_null());
    ASSERT_FALSE(merged_zone_map5.pass_all());

    ASSERT_EQ(merged_zone_map5.min(), "");
    ASSERT_EQ(merged_zone_map5.max(), "yyyy");

    zone_maps.emplace_back();
    *(zone_maps.back().mutable_min()) = "xxxx";
    *(zone_maps.back().mutable_max()) = "yyyy";
    zone_maps.back().set_has_not_null(true);
    zone_maps.back().set_has_null(true);
    zone_maps.back().set_pass_all(false);

    ZoneMapPB merged_zone_map6;
    st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                       merged_zone_map6);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_TRUE(merged_zone_map6.has_null());
    ASSERT_TRUE(merged_zone_map6.has_not_null());
    ASSERT_FALSE(merged_zone_map6.pass_all());

    /// because the last zone map has has_null, the min value is set to "xxxx"
    ASSERT_EQ(merged_zone_map6.min(), "xxxx");
    ASSERT_EQ(merged_zone_map6.max(), "yyyy");

    zone_maps.emplace_back();
    *(zone_maps.back().mutable_min()) = "";
    *(zone_maps.back().mutable_max()) = "yyyy";
    zone_maps.back().set_has_not_null(true);
    zone_maps.back().set_has_null(true);
    zone_maps.back().set_pass_all(true);

    ZoneMapPB merged_zone_map7;
    st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                       merged_zone_map7);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_TRUE(merged_zone_map7.has_null());
    ASSERT_TRUE(merged_zone_map7.has_not_null());
    ASSERT_TRUE(merged_zone_map7.pass_all());
}

template <PredicateType PT>
bool test_predicate(std::string value, const ZoneMapPB& zone_map) {
    AndBlockColumnPredicate predicate;
    ComparisonPredicateBase<PrimitiveType::TYPE_STRING, PT> column_predicate(
            1, {value.data(), value.size()}, false);
    std::unique_ptr<SingleColumnBlockPredicate> single_block_predicate =
            SingleColumnBlockPredicate::create_unique(&column_predicate);
    predicate.add_column_predicate(std::move(single_block_predicate));
    return ColumnReader::match_zone_map_condition(&predicate, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                                  zone_map);
};

TEST_F(ColumnReaderWriterTest, match_zone_map_condition) {
    std::vector<ZoneMapPB> zone_maps;
    ZoneMapPB merged_zone_map;

    zone_maps.emplace_back();
    *(zone_maps.back().mutable_min()) = "apple";
    *(zone_maps.back().mutable_max()) = "banana";

    ASSERT_EQ(zone_maps.back().min(), "apple");
    ASSERT_EQ(zone_maps.back().max(), "banana");
    zone_maps.back().set_has_null(false);
    zone_maps.back().set_pass_all(false);

    auto st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                            merged_zone_map);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_FALSE(merged_zone_map.has_not_null());

    zone_maps.back().set_has_not_null(true);

    ZoneMapPB merged_zone_map2;
    st = ColumnReader::merge_zone_maps(zone_maps, FieldType::OLAP_FIELD_TYPE_STRING, 32,
                                       merged_zone_map2);
    EXPECT_TRUE(st.ok()) << st;

    ASSERT_FALSE(merged_zone_map2.has_null());
    ASSERT_TRUE(merged_zone_map2.has_not_null());
    ASSERT_FALSE(merged_zone_map2.pass_all());
    ASSERT_EQ(merged_zone_map2.min(), "apple");
    ASSERT_EQ(merged_zone_map2.max(), "banana");

    ASSERT_FALSE(test_predicate<PredicateType::GT>("bb", merged_zone_map2));
    ASSERT_TRUE(test_predicate<PredicateType::GE>("banana", merged_zone_map2));
    ASSERT_FALSE(test_predicate<PredicateType::LT>("apple", merged_zone_map2));
    ASSERT_TRUE(test_predicate<PredicateType::LE>("apple", merged_zone_map2));
}

} // namespace doris::segment_v2