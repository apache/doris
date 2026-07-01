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
#include <string>
#include <vector>

#include "core/field.h"
#include "core/value/decimalv2_value.h"
#include "storage/binlog.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/segment/row_binlog_segment_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

namespace {

TabletColumn create_column(int32_t unique_id, const std::string& name, FieldType type, bool is_key,
                           bool visible) {
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, !is_key);
    column.set_unique_id(unique_id);
    column.set_name(name);
    column.set_is_key(is_key);
    column.set_length(type == FieldType::OLAP_FIELD_TYPE_LARGEINT ? 16 : 4);
    column.set_index_length(column.length());
    column._visible = visible;
    return column;
}

TabletSchemaSPtr create_source_schema() {
    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(create_column(0, "k1", FieldType::OLAP_FIELD_TYPE_INT, true, true));
    schema->append_column(create_column(1, "__DORIS_TEST_HIDDEN_KEY__",
                                        FieldType::OLAP_FIELD_TYPE_LARGEINT, true, false));
    schema->append_column(create_column(2, "v1", FieldType::OLAP_FIELD_TYPE_INT, false, true));
    schema->append_column(create_column(3, "__DORIS_TEST_HIDDEN_VALUE__",
                                        FieldType::OLAP_FIELD_TYPE_INT, false, false));
    schema->_keys_type = UNIQUE_KEYS;
    return schema;
}

Block create_source_block(const TabletSchemaSPtr& schema) {
    Block block = schema->create_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (int i = 0; i < 2; ++i) {
        columns[0]->insert(Field::create_field<TYPE_INT>(10 + i));
        columns[1]->insert(Field::create_field<TYPE_LARGEINT>(static_cast<int128_t>(1000 + i)));
        columns[2]->insert(Field::create_field<TYPE_INT>(100 + i));
        columns[3]->insert(Field::create_field<TYPE_INT>(10000 + i));
    }
    return block;
}

} // namespace

TEST(RowBinlogSourceDataWriterTest, collectHiddenKeyInNormalPrefix) {
    auto source_schema = create_source_schema();
    SegmentWriteBinlogOptions options;
    options.source.tablet_schema = source_schema;

    RowBinlogSourceDataWriter writer(options);
    ASSERT_TRUE(writer.init().ok());
    EXPECT_EQ(3, writer.normal_column_count());

    Block block = create_source_block(source_schema);
    Block full_block = source_schema->create_block();
    std::vector<uint32_t> partial_source_cids;
    ASSERT_TRUE(
            writer.prepare_by_source_block(&block, 0, 2, partial_source_cids, &full_block).ok());

    const auto& key_columns = writer.source_key_columns();
    ASSERT_EQ(2, key_columns.size());

    ASSERT_NE(nullptr, key_columns[0]->get_data_at(1));
    EXPECT_EQ(11, *reinterpret_cast<const int32_t*>(key_columns[0]->get_data_at(1)));

    ASSERT_NE(nullptr, key_columns[1]->get_data_at(1));
    EXPECT_EQ(static_cast<int128_t>(1001),
              *reinterpret_cast<const int128_t*>(key_columns[1]->get_data_at(1)));
}

TEST(RowBinlogSourceDataWriterTest, rejectVisibleColumnAfterHiddenNonKeyColumn) {
    auto source_schema = std::make_shared<TabletSchema>();
    source_schema->append_column(
            create_column(0, "k1", FieldType::OLAP_FIELD_TYPE_INT, true, true));
    source_schema->append_column(create_column(1, "__DORIS_TEST_HIDDEN_VALUE__",
                                               FieldType::OLAP_FIELD_TYPE_INT, false, false));
    source_schema->append_column(
            create_column(2, "v1", FieldType::OLAP_FIELD_TYPE_INT, false, true));
    source_schema->_keys_type = UNIQUE_KEYS;

    SegmentWriteBinlogOptions options;
    options.source.tablet_schema = source_schema;

    RowBinlogSourceDataWriter writer(options);
    auto status = writer.init();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos,
              status.to_string().find("visible/key column after hidden non-key"));
}

} // namespace doris::segment_v2
