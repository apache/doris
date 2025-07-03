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

#include "olap/tablet_schema.h"
#include "vec/common/schema_util.h"

namespace doris {

class TabletIndexTest : public testing::Test {};

void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                      const std::string& index_name, int32_t col_unique_id,
                      const std::string& column_type, const std::string& column_name,
                      const IndexType& index_type, bool is_bf_column) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_nullable(true);
    column_pb->set_is_bf_column(is_bf_column);
    tablet_index->set_index_id(index_id);
    tablet_index->set_index_name(index_name);
    tablet_index->set_index_type(index_type);
    tablet_index->add_col_unique_id(col_unique_id);
    if (index_type == IndexType::NGRAM_BF) {
        auto* properties = tablet_index->mutable_properties();
        (*properties)["gram_size"] = "5";
        (*properties)["bf_size"] = "1024";
    }
}

TEST_F(TabletIndexTest, test_inverted_index) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "key_index", 0, "INT",
                     "key", IndexType::INVERTED, true);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1, "STRING",
                     "v1", IndexType::INVERTED, false);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10002, "v2_index", 2, "STRING",
                     "v2", IndexType::NGRAM_BF, true);

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    EXPECT_TRUE(tablet_schema->has_inverted_index());
    EXPECT_EQ(tablet_schema->inverted_indexes().size(), 2);
    EXPECT_TRUE(tablet_schema->inverted_index(tablet_schema->column_by_uid(0)) != nullptr);
    EXPECT_TRUE(tablet_schema->inverted_index(tablet_schema->column_by_uid(1)) != nullptr);
    EXPECT_TRUE(tablet_schema->inverted_index(tablet_schema->column_by_uid(2)) == nullptr);
    EXPECT_TRUE(tablet_schema->inverted_index(3) == nullptr);
    EXPECT_TRUE(tablet_schema->inverted_index(4, "v1.a") == nullptr);
}

TEST_F(TabletIndexTest, test_schema_index_diff) {
    TabletSchemaPB new_schema_pb;
    new_schema_pb.set_keys_type(KeysType::DUP_KEYS);
    new_schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

    construct_column(new_schema_pb.add_column(), new_schema_pb.add_index(), 10000, "key_index", 0,
                     "INT", "key", IndexType::INVERTED, true);
    construct_column(new_schema_pb.add_column(), new_schema_pb.add_index(), 10001, "v1_index", 1,
                     "STRING", "v1", IndexType::INVERTED, false);
    construct_column(new_schema_pb.add_column(), new_schema_pb.add_index(), 10002, "v2_index", 2,
                     "STRING", "v2", IndexType::NGRAM_BF, true);

    TabletSchemaSPtr new_tablet_schema = std::make_shared<TabletSchema>();
    new_tablet_schema->init_from_pb(new_schema_pb);

    TabletSchemaPB old_schema_pb;
    old_schema_pb.set_keys_type(KeysType::DUP_KEYS);
    old_schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

    construct_column(old_schema_pb.add_column(), old_schema_pb.add_index(), 10000, "key_index", 0,
                     "INT", "key", IndexType::INVERTED, true);
    construct_column(old_schema_pb.add_column(), old_schema_pb.add_index(), 10001, "v1_index", 1,
                     "STRING", "v1", IndexType::INVERTED, true);
    construct_column(old_schema_pb.add_column(), old_schema_pb.add_index(), 10002, "v2_index", 2,
                     "STRING", "v2", IndexType::INVERTED, true);

    TabletSchemaSPtr old_tablet_schema = std::make_shared<TabletSchema>();
    old_tablet_schema->init_from_pb(old_schema_pb);

    EXPECT_FALSE(vectorized::schema_util::has_schema_index_diff(new_tablet_schema.get(),
                                                                old_tablet_schema.get(), 0, 0));
    EXPECT_TRUE(vectorized::schema_util::has_schema_index_diff(new_tablet_schema.get(),
                                                               old_tablet_schema.get(), 1, 1));
    EXPECT_TRUE(vectorized::schema_util::has_schema_index_diff(new_tablet_schema.get(),
                                                               old_tablet_schema.get(), 2, 2));
}

} // namespace doris
