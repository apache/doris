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

    if (type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        TabletColumn array_item_col;
        // double not support inverted index
        array_item_col.set_type(FieldType::OLAP_FIELD_TYPE_DOUBLE);
        array_item_col.set_is_nullable(true);
        array_item_col.set_unique_id(-1);
        array_item_col.set_parent_unique_id(col_unique_id);

        subcol.add_sub_column(array_item_col);
    }

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
            EXPECT_TRUE(!tablet_schema->inverted_indexs(col).empty());
            break;
        case 3:
            EXPECT_TRUE(tablet_schema->inverted_indexs(col).empty());
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

TEST_F(SchemaUtilTest, test_multiple_index_inheritance) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "v1_index_alpha", 1,
                     "VARIANT", "v1", IndexType::INVERTED);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index_beta", 1,
                     "VARIANT", "v1", IndexType::INVERTED);

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    std::vector<TabletColumn> subcolumns;

    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_STRING, 1, "v1.name",
                        &subcolumns);

    vectorized::schema_util::inherit_column_attributes(tablet_schema);

    const auto& subcol = subcolumns[0];
    auto inherited_indexes = tablet_schema->inverted_indexs(subcol);

    EXPECT_EQ(inherited_indexes.size(), 2);
    EXPECT_EQ(inherited_indexes[0]->index_name(), "v1_index_alpha");
    EXPECT_EQ(inherited_indexes[1]->index_name(), "v1_index_beta");

    for (const auto& index : inherited_indexes) {
        EXPECT_EQ(index->get_index_suffix(), "v1%2Ename");
    }
}

TEST_F(SchemaUtilTest, test_index_update_logic) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "v1_index_orig1", 1,
                     "VARIANT", "v1", IndexType::INVERTED);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index_orig2", 1,
                     "VARIANT", "v1", IndexType::INVERTED);

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    std::vector<TabletColumn> subcolumns;

    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_STRING, 1, "v1.name",
                        &subcolumns);
    vectorized::schema_util::inherit_column_attributes(tablet_schema);

    const auto& subcol = subcolumns[0];
    auto initial_indexes = tablet_schema->inverted_indexs(subcol);
    ASSERT_EQ(initial_indexes.size(), 2);
    EXPECT_EQ(initial_indexes[0]->index_name(), "v1_index_orig1");
    EXPECT_EQ(initial_indexes[1]->index_name(), "v1_index_orig2");

    std::vector<TabletIndex> updated_indexes;
    TabletIndexPB tablet_index_pb1;
    tablet_index_pb1.set_index_id(10002);
    tablet_index_pb1.set_index_name("v1_index_updated1");
    tablet_index_pb1.set_index_type(IndexType::INVERTED);
    tablet_index_pb1.add_col_unique_id(1);
    TabletIndex tablet_index1;
    tablet_index1.init_from_pb(tablet_index_pb1);
    updated_indexes.emplace_back(std::move(tablet_index1));

    TabletIndexPB tablet_index_pb2;
    tablet_index_pb2.set_index_id(10003);
    tablet_index_pb2.set_index_name("v1_index_updated2");
    tablet_index_pb2.set_index_type(IndexType::INVERTED);
    tablet_index_pb2.add_col_unique_id(1);
    TabletIndex tablet_index2;
    tablet_index2.init_from_pb(tablet_index_pb2);
    updated_indexes.emplace_back(std::move(tablet_index2));

    tablet_schema->update_index(tablet_schema->column(1), IndexType::INVERTED,
                                std::move(updated_indexes));

    vectorized::schema_util::inherit_column_attributes(tablet_schema);
    auto updated_subcol_indexes = tablet_schema->inverted_indexs(subcol);

    EXPECT_EQ(updated_subcol_indexes.size(), 2);
    EXPECT_EQ(updated_subcol_indexes[0]->index_name(), "v1_index_updated1");
    EXPECT_EQ(updated_subcol_indexes[1]->index_name(), "v1_index_updated2");
    EXPECT_EQ(updated_subcol_indexes[0]->get_index_suffix(), "v1%2Ename");
}

} // namespace doris
