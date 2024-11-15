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

#include "vec/common/schema_util.h"

#include <gtest/gtest.h>

namespace doris {

class SchemaUtilTest : public testing::Test {};

void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                      const std::string& index_name, int32_t col_unique_id,
                      const std::string& column_type, const std::string& column_name,
                      const IndexType& index_type) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_nullable(true);
    column_pb->set_is_bf_column(true);
    tablet_index->set_index_id(index_id);
    tablet_index->set_index_name(index_name);
    tablet_index->set_index_type(index_type);
    tablet_index->add_col_unique_id(col_unique_id);
}

void construct_subcolumn(TabletSchemaSPtr schema, const FieldType& type, int32_t col_unique_id,
                         std::string_view path, std::vector<TabletColumn>* subcolumns) {
    TabletColumn subcol;
    subcol.set_type(type);
    subcol.set_is_nullable(true);
    subcol.set_unique_id(-1);
    subcol.set_parent_unique_id(col_unique_id);
    vectorized::PathInData col_path(path);
    subcol.set_path_info(col_path);
    subcol.set_name(col_path.get_path());
    schema->append_column(subcol);
    subcolumns->emplace_back(std::move(subcol));
}

TEST_F(SchemaUtilTest, inherit_column_attributes) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "key_index", 0, "INT",
                     "key", IndexType::INVERTED);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1, "VARIANT",
                     "v1", IndexType::INVERTED);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10003, "v3_index", 3, "VARIANT",
                     "v3", IndexType::INVERTED);

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    std::vector<TabletColumn> subcolumns;

    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_STRING, 1, "v1.b", &subcolumns);
    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_INT, 1, "v1.c", &subcolumns);

    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_ARRAY, 3, "v3.d", &subcolumns);
    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_FLOAT, 3, "v3.a", &subcolumns);

    vectorized::schema_util::inherit_column_attributes(tablet_schema);
    for (const auto& col : subcolumns) {
        switch (col._parent_col_unique_id) {
        case 1:
            EXPECT_TRUE(tablet_schema->inverted_index(col) != nullptr);
            break;
        case 3:
            EXPECT_TRUE(tablet_schema->inverted_index(col) == nullptr);
            break;
        default:
            EXPECT_TRUE(false);
        }
    }
    EXPECT_EQ(tablet_schema->inverted_indexes().size(), 7);

    for (const auto& col : tablet_schema->_cols) {
        if (!col->is_extracted_column()) {
            continue;
        }
        switch (col->_parent_col_unique_id) {
        case 1:
            EXPECT_TRUE(col->is_bf_column());
            break;
        case 3:
            EXPECT_TRUE(!col->is_bf_column());
            break;
        default:
            EXPECT_TRUE(false);
        }
    }
}

} // namespace doris
