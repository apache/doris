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

#include "storage/iterator/binlog_block_reader_utils.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

namespace doris {

namespace {

TabletColumn make_column(std::string name, FieldType type, bool is_key, int32_t unique_id,
                         bool is_nullable = true) {
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, is_nullable,
                        unique_id, sizeof(int64_t));
    column.set_name(std::move(name));
    column.set_is_key(is_key);
    column.set_index_length(sizeof(int64_t));
    return column;
}

TabletSchemaSPtr make_colliding_name_schema() {
    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(make_column("key", FieldType::OLAP_FIELD_TYPE_BIGINT, true, 0, false));
    schema->append_column(make_column("v", FieldType::OLAP_FIELD_TYPE_BIGINT, false, 1));
    schema->append_column(
            make_column("__BEFORE__v__", FieldType::OLAP_FIELD_TYPE_BIGINT, false, 2));
    schema->append_column(make_column(binlog::build_before_column_name("v"),
                                      FieldType::OLAP_FIELD_TYPE_BIGINT, false, 3));
    schema->append_column(make_column(binlog::build_before_column_name("__BEFORE__v__"),
                                      FieldType::OLAP_FIELD_TYPE_BIGINT, false, 4));
    schema->append_column(make_column(BINLOG_TSO_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false, 5));
    schema->append_column(make_column(BINLOG_LSN_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false, 6));
    schema->append_column(make_column(BINLOG_OP_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false, 7));
    return schema;
}

} // namespace

class BinlogBlockReaderUtilsTest : public testing::Test {};
TEST_F(BinlogBlockReaderUtilsTest, BuildBeforeColumnName) {
    EXPECT_EQ(binlog::build_before_column_name("v1"), "__BEFORE__v1__");
}

TEST_F(BinlogBlockReaderUtilsTest, ResolveValuePairsByPhysicalOrdinal) {
    TabletSchemaSPtr schema = make_colliding_name_schema();
    std::vector<binlog::RowBinlogValueColumnPair> pairs;

    ASSERT_TRUE(binlog::get_row_binlog_value_column_pairs(*schema, &pairs));
    EXPECT_EQ(pairs, (std::vector<binlog::RowBinlogValueColumnPair> {{1, 3}, {2, 4}}));
}

TEST_F(BinlogBlockReaderUtilsTest, RejectMalformedValuePairLayout) {
    TabletSchema schema;
    schema.append_column(make_column("key", FieldType::OLAP_FIELD_TYPE_BIGINT, true, 0, false));
    schema.append_column(make_column("v", FieldType::OLAP_FIELD_TYPE_BIGINT, false, 1));
    schema.append_column(make_column(BINLOG_TSO_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false, 2));
    schema.append_column(make_column(BINLOG_LSN_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false, 3));
    schema.append_column(make_column(BINLOG_OP_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false, 4));
    std::vector<binlog::RowBinlogValueColumnPair> pairs;

    EXPECT_FALSE(binlog::get_row_binlog_value_column_pairs(schema, &pairs));
    EXPECT_TRUE(pairs.empty());
}

TEST_F(BinlogBlockReaderUtilsTest, ComparisonCapabilityRejectsOpaqueAndFloatingTypes) {
    TabletColumn bigint = make_column("bigint", FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0);
    TabletColumn agg_state =
            make_column("agg_state", FieldType::OLAP_FIELD_TYPE_AGG_STATE, false, 1);
    TabletColumn floating = make_column("floating", FieldType::OLAP_FIELD_TYPE_FLOAT, false, 2);

    EXPECT_TRUE(binlog::supports_min_delta_value_comparison(bigint));
    EXPECT_FALSE(binlog::supports_min_delta_value_comparison(agg_state));
    EXPECT_FALSE(binlog::supports_min_delta_value_comparison(floating));
}

TEST_F(BinlogBlockReaderUtilsTest, ComparisonCapabilityChecksNestedChildren) {
    TabletColumn int_item = make_column("item", FieldType::OLAP_FIELD_TYPE_BIGINT, false, 1);
    TabletColumn float_item = make_column("item", FieldType::OLAP_FIELD_TYPE_FLOAT, false, 2);
    TabletColumn int_array = make_column("int_array", FieldType::OLAP_FIELD_TYPE_ARRAY, false, 3);
    TabletColumn float_array =
            make_column("float_array", FieldType::OLAP_FIELD_TYPE_ARRAY, false, 4);
    int_array.add_sub_column(int_item);
    float_array.add_sub_column(float_item);

    EXPECT_TRUE(binlog::supports_min_delta_value_comparison(int_array));
    EXPECT_FALSE(binlog::supports_min_delta_value_comparison(float_array));
}

} // namespace doris
