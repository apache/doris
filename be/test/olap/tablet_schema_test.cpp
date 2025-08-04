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

#include "olap/tablet_schema.h"

#include <gtest/gtest.h>

#include <set>

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "vec/json/path_in_data.h"

namespace doris {

class TabletSchemaTest : public testing::Test {
protected:
    void SetUp() override {}
};

TEST_F(TabletSchemaTest, test_tablet_column_init_from_pb) {
    ColumnPB column_pb;
    column_pb.set_unique_id(1001);
    column_pb.set_name("test_column");
    column_pb.set_type("INT");
    column_pb.set_is_key(true);
    column_pb.set_is_nullable(false);
    column_pb.set_length(4);
    column_pb.set_aggregation("NONE");
    column_pb.set_precision(10);
    column_pb.set_frac(0);
    column_pb.set_is_bf_column(false);
    column_pb.set_has_bitmap_index(false);
    column_pb.set_visible(true);
    column_pb.set_variant_max_subcolumns_count(100);
    column_pb.set_pattern_type(PatternTypePB::MATCH_NAME_GLOB);
    column_pb.set_variant_enable_typed_paths_to_sparse(true);

    TabletColumn tablet_column;
    tablet_column.init_from_pb(column_pb);

    EXPECT_EQ(1001, tablet_column.unique_id());
    EXPECT_EQ("test_column", tablet_column.name());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_INT, tablet_column.type());
    EXPECT_TRUE(tablet_column.is_key());
    EXPECT_FALSE(tablet_column.is_nullable());
    EXPECT_EQ(4, tablet_column.length());
    EXPECT_EQ(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, tablet_column.aggregation());
    EXPECT_EQ(10, tablet_column.precision());
    EXPECT_EQ(0, tablet_column.frac());
    EXPECT_FALSE(tablet_column.is_bf_column());
    EXPECT_FALSE(tablet_column.has_bitmap_index());
    EXPECT_TRUE(tablet_column.visible());
    EXPECT_EQ(100, tablet_column.variant_max_subcolumns_count());
    EXPECT_EQ(PatternTypePB::MATCH_NAME_GLOB, tablet_column.pattern_type());
    EXPECT_TRUE(tablet_column.variant_enable_typed_paths_to_sparse());
}

TEST_F(TabletSchemaTest, test_tablet_column_init_from_thrift) {
    TColumn tcolumn;
    tcolumn.__set_column_name("thrift_column");
    TColumnType column_type;
    column_type.__set_type(TPrimitiveType::STRING);
    column_type.__set_variant_max_subcolumns_count(100);
    column_type.__set_len(255);
    tcolumn.__set_column_type(column_type);
    tcolumn.__set_is_key(false);
    tcolumn.__set_is_allow_null(true);
    tcolumn.__set_aggregation_type(TAggregationType::SUM);
    tcolumn.__set_is_bloom_filter_column(true);
    tcolumn.__set_has_bitmap_index(true);
    tcolumn.__set_visible(false);
    tcolumn.__set_default_value("default_test");
    tcolumn.__set_variant_enable_typed_paths_to_sparse(false);
    tcolumn.__set_pattern_type(TPatternType::MATCH_NAME_GLOB);

    TabletColumn tablet_column;
    tablet_column.init_from_thrift(tcolumn);

    EXPECT_EQ("thrift_column", tablet_column.name());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_STRING, tablet_column.type());
    EXPECT_FALSE(tablet_column.is_key());
    EXPECT_TRUE(tablet_column.is_nullable());
    EXPECT_EQ(259, tablet_column.length());
    EXPECT_EQ(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM, tablet_column.aggregation());
    EXPECT_TRUE(tablet_column.is_bf_column());
    EXPECT_TRUE(tablet_column.has_bitmap_index());
    EXPECT_FALSE(tablet_column.visible());
    EXPECT_TRUE(tablet_column.has_default_value());
    EXPECT_EQ("default_test", tablet_column.default_value());
    EXPECT_FALSE(tablet_column.variant_enable_typed_paths_to_sparse());
}

TEST_F(TabletSchemaTest, test_tablet_index_init_from_pb) {
    TabletIndexPB index_pb;
    index_pb.set_index_id(12345);
    index_pb.set_index_name("test_inverted_index");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(1001);
    index_pb.add_col_unique_id(1002);

    auto* properties = index_pb.mutable_properties();
    (*properties)["parser"] = "standard";
    (*properties)["support_phrase"] = "true";
    (*properties)["char_filter"] = "char_replace";

    TabletIndex tablet_index;
    tablet_index.init_from_pb(index_pb);

    EXPECT_EQ(12345, tablet_index.index_id());
    EXPECT_EQ("test_inverted_index", tablet_index.index_name());
    EXPECT_EQ(IndexType::INVERTED, tablet_index.index_type());
    EXPECT_TRUE(tablet_index.is_inverted_index());

    const auto& col_uids = tablet_index.col_unique_ids();
    EXPECT_EQ(2, col_uids.size());
    EXPECT_EQ(1001, col_uids[0]);
    EXPECT_EQ(1002, col_uids[1]);

    const auto& props = tablet_index.properties();
    EXPECT_EQ("standard", props.at("parser"));
    EXPECT_EQ("true", props.at("support_phrase"));
    EXPECT_EQ("char_replace", props.at("char_filter"));
}

TEST_F(TabletSchemaTest, test_tablet_index_init_from_thrift) {
    TOlapTableIndex tindex;
    tindex.__set_index_id(54321);
    tindex.__set_index_name("thrift_index");
    tindex.__set_index_type(TIndexType::INVERTED);
    tindex.__set_columns({"col1", "col2"});

    std::map<std::string, std::string> properties;
    properties["parser"] = "unicode";
    properties["gram_size"] = "2";
    properties["bf_size"] = "1024";
    tindex.__set_properties(properties);

    std::vector<int32_t> column_uids = {2001, 2002};

    TabletIndex tablet_index;
    tablet_index.init_from_thrift(tindex, column_uids);

    EXPECT_EQ(54321, tablet_index.index_id());
    EXPECT_EQ("thrift_index", tablet_index.index_name());
    EXPECT_EQ(IndexType::INVERTED, tablet_index.index_type());
    EXPECT_TRUE(tablet_index.is_inverted_index());

    const auto& col_uids = tablet_index.col_unique_ids();
    EXPECT_EQ(2, col_uids.size());
    EXPECT_EQ(2001, col_uids[0]);
    EXPECT_EQ(2002, col_uids[1]);

    // Test gram size and bf size parsing
    EXPECT_EQ(2, tablet_index.get_gram_size());
    EXPECT_EQ(1024, tablet_index.get_gram_bf_size());

    const auto& props = tablet_index.properties();
    EXPECT_EQ("unicode", props.at("parser"));
}

TEST_F(TabletSchemaTest, test_tablet_schema_inverted_indexs) {
    TabletSchema schema;

    TabletColumn col1;
    col1.set_unique_id(1001);
    col1.set_name("col1");
    col1.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
    schema.append_column(col1);

    TabletColumn col2;
    col2.set_unique_id(1002);
    col2.set_name("col2");
    col2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    schema.append_column(col2);

    TabletIndex index1;
    index1.init_from_pb([&]() {
        TabletIndexPB pb;
        pb.set_index_id(1);
        pb.set_index_name("idx1");
        pb.set_index_type(IndexType::INVERTED);
        pb.add_col_unique_id(1001);
        return pb;
    }());

    TabletIndex index2;
    index2.init_from_pb([&]() {
        TabletIndexPB pb;
        pb.set_index_id(2);
        pb.set_index_name("idx2");
        pb.set_index_type(IndexType::INVERTED);
        pb.add_col_unique_id(1002);
        return pb;
    }());

    TabletIndex bitmap_index;
    bitmap_index.init_from_pb([&]() {
        TabletIndexPB pb;
        pb.set_index_id(3);
        pb.set_index_name("bitmap_idx");
        pb.set_index_type(IndexType::BITMAP);
        pb.add_col_unique_id(1001);
        return pb;
    }());

    schema.append_index(std::move(index1));
    schema.append_index(std::move(index2));
    schema.append_index(std::move(bitmap_index));

    auto inverted_indexes_col1 = schema.inverted_indexs(col1);
    EXPECT_EQ(1, inverted_indexes_col1.size());
    EXPECT_EQ("idx1", inverted_indexes_col1[0]->index_name());

    auto inverted_indexes_col2 = schema.inverted_indexs(col2);
    EXPECT_EQ(1, inverted_indexes_col2.size());
    EXPECT_EQ("idx2", inverted_indexes_col2[0]->index_name());

    auto inverted_indexes_by_uid = schema.inverted_indexs(1001);
    EXPECT_EQ(1, inverted_indexes_by_uid.size());
    EXPECT_EQ("idx1", inverted_indexes_by_uid[0]->index_name());

    EXPECT_TRUE(schema.has_inverted_index());

    auto all_inverted_indexes = schema.inverted_indexes();
    EXPECT_EQ(2, all_inverted_indexes.size());
}

TEST_F(TabletSchemaTest, test_tablet_schema_update_indexes_from_thrift) {
    TabletSchema schema;

    TabletColumn col1;
    col1.set_unique_id(3001);
    col1.set_name("text_col");
    col1.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    schema.append_column(col1);

    TabletColumn col2;
    col2.set_unique_id(3002);
    col2.set_name("varchar_col");
    col2.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
    schema.append_column(col2);

    std::vector<TOlapTableIndex> tindexes;

    TOlapTableIndex tindex1;
    tindex1.index_id = 101;
    tindex1.index_name = "text_idx";
    tindex1.index_type = TIndexType::INVERTED;
    tindex1.columns = {"text_col"};
    tindex1.properties["parser"] = "standard";
    tindexes.push_back(tindex1);

    TOlapTableIndex tindex2;
    tindex2.index_id = 102;
    tindex2.index_name = "varchar_idx";
    tindex2.index_type = TIndexType::INVERTED;
    tindex2.columns = {"varchar_col"};
    tindex2.properties["support_phrase"] = "false";
    tindexes.push_back(tindex2);

    schema.update_indexes_from_thrift(tindexes);

    EXPECT_TRUE(schema.has_inverted_index());
    auto all_inverted_indexes = schema.inverted_indexes();
    EXPECT_EQ(2, all_inverted_indexes.size());

    bool found_text_idx = false;
    bool found_varchar_idx = false;

    for (const auto* index : all_inverted_indexes) {
        if (index->index_name() == "text_idx") {
            found_text_idx = true;
            EXPECT_EQ(101, index->index_id());
            EXPECT_EQ(IndexType::INVERTED, index->index_type());
        } else if (index->index_name() == "varchar_idx") {
            found_varchar_idx = true;
            EXPECT_EQ(102, index->index_id());
            EXPECT_EQ(IndexType::INVERTED, index->index_type());
        }
    }

    EXPECT_TRUE(found_text_idx);
    EXPECT_TRUE(found_varchar_idx);
}

TEST_F(TabletSchemaTest, test_tablet_schema_append_index) {
    TabletSchema schema;

    TabletColumn col;
    col.set_unique_id(4001);
    col.set_name("test_col");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    schema.append_column(col);

    TabletIndex index;
    TabletIndexPB index_pb;
    index_pb.set_index_id(201);
    index_pb.set_index_name("append_test_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(4001);
    index.init_from_pb(index_pb);

    schema.append_index(std::move(index));

    EXPECT_TRUE(schema.has_inverted_index());
    auto indexes = schema.inverted_indexes();
    EXPECT_EQ(1, indexes.size());
    EXPECT_EQ("append_test_idx", indexes[0]->index_name());
    EXPECT_EQ(201, indexes[0]->index_id());
}

TEST_F(TabletSchemaTest, test_tablet_schema_update_indexs) {
    TabletSchema schema;

    TabletColumn col;
    col.set_unique_id(5001);
    col.set_name("update_col");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    schema.append_column(col);

    std::vector<TabletIndex> indexes;
    for (int i = 0; i < 3; ++i) {
        TabletIndex index;
        TabletIndexPB index_pb;
        index_pb.set_index_id(300 + i);
        index_pb.set_index_name("update_idx_" + std::to_string(i));
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.add_col_unique_id(5001);
        index.init_from_pb(index_pb);
        schema.append_index(std::move(index));
    }

    std::vector<TabletIndex> new_indexes;

    for (int i = 0; i < 3; ++i) {
        TabletIndex index;
        TabletIndexPB index_pb;
        index_pb.set_index_id(300 + i);
        index_pb.set_index_name("update_idx_" + std::to_string(i));
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.add_col_unique_id(5001);
        index.init_from_pb(index_pb);
        new_indexes.push_back(std::move(index));
    }

    schema.update_index(col, IndexType::INVERTED, std::move(indexes));

    EXPECT_TRUE(schema.has_inverted_index());
    auto all_indexes = schema.inverted_indexs(5001);
    EXPECT_EQ(3, all_indexes.size());

    std::set<std::string> expected_names = {"update_idx_0", "update_idx_1", "update_idx_2"};
    std::set<std::string> actual_names;
    for (const auto* index : all_indexes) {
        actual_names.insert(index->index_name());
    }
    EXPECT_EQ(expected_names, actual_names);
}

TEST_F(TabletSchemaTest, test_tablet_column_protobuf_roundtrip) {
    TabletColumn original;
    original.set_unique_id(6001);
    original.set_name("variant_col");
    original.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    original.set_is_nullable(true);
    original.set_variant_max_subcolumns_count(500);

    ColumnPB column_pb;
    original.to_schema_pb(&column_pb);

    TabletColumn deserialized;
    deserialized.init_from_pb(column_pb);

    EXPECT_EQ(original.unique_id(), deserialized.unique_id());
    EXPECT_EQ(original.name(), deserialized.name());
    EXPECT_EQ(original.type(), deserialized.type());
    EXPECT_EQ(original.is_nullable(), deserialized.is_nullable());
    EXPECT_EQ(original.variant_max_subcolumns_count(), deserialized.variant_max_subcolumns_count());
    EXPECT_EQ(original.pattern_type(), deserialized.pattern_type());
    EXPECT_EQ(original.variant_enable_typed_paths_to_sparse(),
              deserialized.variant_enable_typed_paths_to_sparse());
}

TEST_F(TabletSchemaTest, test_tablet_schema_remove_and_clear_index) {
    TabletSchema schema;

    TabletColumn col;
    col.set_unique_id(8001);
    col.set_name("test_col");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    schema.append_column(col);

    for (int i = 0; i < 3; ++i) {
        TabletIndex index;
        TabletIndexPB index_pb;
        index_pb.set_index_id(400 + i);
        index_pb.set_index_name("remove_test_idx_" + std::to_string(i));
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.add_col_unique_id(8001);
        index.init_from_pb(index_pb);
        schema.append_index(std::move(index));
    }

    EXPECT_TRUE(schema.has_inverted_index());
    auto indexes_before = schema.inverted_indexes();
    EXPECT_EQ(3, indexes_before.size());

    schema.remove_index(401); // Remove the middle one

    auto indexes_after_remove = schema.inverted_indexes();
    EXPECT_EQ(2, indexes_after_remove.size());

    bool found_400 = false, found_402 = false;
    for (const auto* index : indexes_after_remove) {
        if (index->index_id() == 400) {
            found_400 = true;
        }
        if (index->index_id() == 402) {
            found_402 = true;
        }
    }
    EXPECT_TRUE(found_400);
    EXPECT_TRUE(found_402);

    schema.clear_index();
    EXPECT_FALSE(schema.has_inverted_index());
    auto indexes_after_clear = schema.inverted_indexes();
    EXPECT_EQ(0, indexes_after_clear.size());
}

TEST_F(TabletSchemaTest, test_tablet_schema_path_set_info_inverted_indexs) {
    TabletSchema schema;

    TabletColumn variant_col;
    variant_col.set_unique_id(9001);
    variant_col.set_name("variant_col");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    schema.append_column(variant_col);

    auto create_index = [](int64_t id, const std::string& name, int32_t col_uid) {
        auto index = std::make_shared<TabletIndex>();
        TabletIndexPB index_pb;
        index_pb.set_index_id(id);
        index_pb.set_index_name(name);
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.add_col_unique_id(col_uid);
        index->init_from_pb(index_pb);
        return index;
    };

    auto typed_index1 = create_index(1001, "typed_path_idx1", 9001);
    auto typed_index2 = create_index(1002, "typed_path_idx2", 9001);

    auto subcolumn_index1 = create_index(2001, "subcolumn_idx1", 9001);
    auto subcolumn_index2 = create_index(2002, "subcolumn_idx2", 9001);

    TabletSchema::PathsSetInfo path_set_info;
    TabletSchema::SubColumnInfo typed_sub_col1;
    typed_sub_col1.column = variant_col;
    typed_sub_col1.indexes.push_back(typed_index1);
    path_set_info.typed_path_set["user.name"] = typed_sub_col1;

    TabletSchema::SubColumnInfo typed_sub_col2;
    typed_sub_col2.column = variant_col;
    typed_sub_col2.indexes.push_back(typed_index2);
    path_set_info.typed_path_set["user.age"] = typed_sub_col2;

    TabletIndexes subcolumn_indexes1 = {subcolumn_index1};
    TabletIndexes subcolumn_indexes2 = {subcolumn_index2};
    path_set_info.subcolumn_indexes["product.id"] = subcolumn_indexes1;
    path_set_info.subcolumn_indexes["product.price"] = subcolumn_indexes2;

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> path_set_info_map;
    path_set_info_map[9001] = std::move(path_set_info);
    schema.set_path_set_info(std::move(path_set_info_map));

    TabletColumn typed_extracted_col1;
    typed_extracted_col1.set_unique_id(-1);
    typed_extracted_col1.set_name("variant_col.user.name");
    typed_extracted_col1.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    typed_extracted_col1.set_parent_unique_id(9001);

    vectorized::PathInData typed_path1("variant_col.user.name", true);
    typed_extracted_col1.set_path_info(typed_path1);

    auto typed_indexes = schema.inverted_indexs(typed_extracted_col1);
    EXPECT_EQ(1, typed_indexes.size());
    EXPECT_EQ("typed_path_idx1", typed_indexes[0]->index_name());
    EXPECT_EQ(1001, typed_indexes[0]->index_id());

    TabletColumn typed_extracted_col2;
    typed_extracted_col2.set_unique_id(-1);
    typed_extracted_col2.set_name("variant_col.user.age");
    typed_extracted_col2.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    typed_extracted_col2.set_parent_unique_id(9001);

    vectorized::PathInData typed_path2("variant_col.user.age", true);
    typed_extracted_col2.set_path_info(typed_path2);

    auto typed_indexes2 = schema.inverted_indexs(typed_extracted_col2);
    EXPECT_EQ(1, typed_indexes2.size());
    EXPECT_EQ("typed_path_idx2", typed_indexes2[0]->index_name());
    EXPECT_EQ(1002, typed_indexes2[0]->index_id());

    // Test subcolumn path (non-typed)
    TabletColumn subcolumn_extracted_col1;
    subcolumn_extracted_col1.set_unique_id(-1);
    subcolumn_extracted_col1.set_name("variant_col.product.id");
    subcolumn_extracted_col1.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    subcolumn_extracted_col1.set_parent_unique_id(9001);

    vectorized::PathInData subcolumn_path1("variant_col.product.id");
    subcolumn_extracted_col1.set_path_info(subcolumn_path1);

    auto subcolumn_indexes = schema.inverted_indexs(subcolumn_extracted_col1);
    EXPECT_EQ(1, subcolumn_indexes.size());
    EXPECT_EQ("subcolumn_idx1", subcolumn_indexes[0]->index_name());
    EXPECT_EQ(2001, subcolumn_indexes[0]->index_id());

    TabletColumn non_existing_col;
    non_existing_col.set_unique_id(-1);
    non_existing_col.set_name("non_existing");
    non_existing_col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    non_existing_col.set_parent_unique_id(9001);

    vectorized::PathInData non_existing_path("variant_col.non.existing");
    non_existing_col.set_path_info(non_existing_path);

    auto no_indexes = schema.inverted_indexs(non_existing_col);
    EXPECT_EQ(0, no_indexes.size());

    TabletColumn wrong_parent_col;
    wrong_parent_col.set_unique_id(-1);
    wrong_parent_col.set_name("wrong_parent");
    wrong_parent_col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    wrong_parent_col.set_parent_unique_id(9999); // Non-existing parent

    vectorized::PathInData wrong_parent_path("wrong_variant.some.path");
    wrong_parent_col.set_path_info(wrong_parent_path);

    auto no_indexes_wrong_parent = schema.inverted_indexs(wrong_parent_col);
    EXPECT_EQ(0, no_indexes_wrong_parent.size());
}

TEST_F(TabletSchemaTest, test_tablet_schema_path_set_info_accessors) {
    TabletSchema schema;

    TabletColumn variant_col;
    variant_col.set_unique_id(10001);
    variant_col.set_name("json_data");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    schema.append_column(variant_col);

    TabletSchema::PathsSetInfo path_info;
    path_info.sub_path_set.insert("extracted_path1");
    path_info.sub_path_set.insert("extracted_path2");
    path_info.sparse_path_set.insert("sparse_path1");
    path_info.sparse_path_set.insert("sparse_path2");

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> path_map;
    path_map[10001] = std::move(path_info);
    schema.set_path_set_info(std::move(path_map));

    const auto& retrieved_info = schema.path_set_info(10001);
    EXPECT_EQ(2, retrieved_info.sub_path_set.size());
    EXPECT_EQ(2, retrieved_info.sparse_path_set.size());
    EXPECT_TRUE(retrieved_info.sub_path_set.count("extracted_path1") > 0);
    EXPECT_TRUE(retrieved_info.sub_path_set.count("extracted_path2") > 0);
    EXPECT_TRUE(retrieved_info.sparse_path_set.count("sparse_path1") > 0);
    EXPECT_TRUE(retrieved_info.sparse_path_set.count("sparse_path2") > 0);
}

TEST_F(TabletSchemaTest, test_tablet_schema_inverted_index_by_field_pattern) {
    TabletSchema schema;

    TabletColumn col1;
    col1.set_unique_id(11001);
    col1.set_name("variant_col1");
    col1.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    schema.append_column(col1);

    TabletColumn col2;
    col2.set_unique_id(11002);
    col2.set_name("variant_col2");
    col2.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    schema.append_column(col2);

    TabletIndex index1;
    TabletIndexPB index1_pb;
    index1_pb.set_index_id(3001);
    index1_pb.set_index_name("pattern_idx1");
    index1_pb.set_index_type(IndexType::INVERTED);
    index1_pb.add_col_unique_id(11001);

    auto* properties1 = index1_pb.mutable_properties();
    (*properties1)["field_pattern"] = "user.*";
    (*properties1)["parser"] = "standard";

    index1.init_from_pb(index1_pb);

    TabletIndex index2;
    TabletIndexPB index2_pb;
    index2_pb.set_index_id(3002);
    index2_pb.set_index_name("pattern_idx2");
    index2_pb.set_index_type(IndexType::INVERTED);
    index2_pb.add_col_unique_id(11001);

    auto* properties2 = index2_pb.mutable_properties();
    (*properties2)["field_pattern"] = "product.*";
    (*properties2)["parser"] = "unicode";

    index2.init_from_pb(index2_pb);

    TabletIndex index3;
    TabletIndexPB index3_pb;
    index3_pb.set_index_id(3003);
    index3_pb.set_index_name("pattern_idx3");
    index3_pb.set_index_type(IndexType::INVERTED);
    index3_pb.add_col_unique_id(11002);

    auto* properties3 = index3_pb.mutable_properties();
    (*properties3)["field_pattern"] = "user.*";
    (*properties3)["parser"] = "keyword";

    index3.init_from_pb(index3_pb);

    TabletIndex index4;
    TabletIndexPB index4_pb;
    index4_pb.set_index_id(3004);
    index4_pb.set_index_name("no_pattern_idx");
    index4_pb.set_index_type(IndexType::INVERTED);
    index4_pb.add_col_unique_id(11001);

    index4.init_from_pb(index4_pb);

    TabletIndex index5;
    TabletIndexPB index5_pb;
    index5_pb.set_index_id(3005);
    index5_pb.set_index_name("pattern_idx5");
    index5_pb.set_index_type(IndexType::INVERTED);
    index5_pb.add_col_unique_id(11001);

    auto* properties5 = index5_pb.mutable_properties();
    (*properties5)["field_pattern"] = "user.*";
    (*properties5)["parser"] = "english";

    index5.init_from_pb(index5_pb);

    schema.append_index(std::move(index1));
    schema.append_index(std::move(index2));
    schema.append_index(std::move(index3));
    schema.append_index(std::move(index4));
    schema.append_index(std::move(index5));

    auto user_indexes_col1 = schema.inverted_index_by_field_pattern(11001, "user.*");
    EXPECT_EQ(2, user_indexes_col1.size());

    std::set<std::string> expected_names_user_col1 = {"pattern_idx1", "pattern_idx5"};
    std::set<std::string> actual_names_user_col1;
    for (const auto& index : user_indexes_col1) {
        actual_names_user_col1.insert(index->index_name());
    }
    EXPECT_EQ(expected_names_user_col1, actual_names_user_col1);

    auto product_indexes_col1 = schema.inverted_index_by_field_pattern(11001, "product.*");
    EXPECT_EQ(1, product_indexes_col1.size());
    EXPECT_EQ("pattern_idx2", product_indexes_col1[0]->index_name());
    EXPECT_EQ(3002, product_indexes_col1[0]->index_id());

    auto user_indexes_col2 = schema.inverted_index_by_field_pattern(11002, "user.*");
    EXPECT_EQ(1, user_indexes_col2.size());
    EXPECT_EQ("pattern_idx3", user_indexes_col2[0]->index_name());
    EXPECT_EQ(3003, user_indexes_col2[0]->index_id());

    auto non_existing_pattern = schema.inverted_index_by_field_pattern(11001, "non.existing.*");
    EXPECT_EQ(0, non_existing_pattern.size());

    auto non_existing_column = schema.inverted_index_by_field_pattern(99999, "user.*");
    EXPECT_EQ(0, non_existing_column.size());

    auto empty_pattern = schema.inverted_index_by_field_pattern(11001, "");
    EXPECT_EQ(0, empty_pattern.size());

    auto different_pattern = schema.inverted_index_by_field_pattern(11001, "user.name");
    EXPECT_EQ(0, different_pattern.size());
}

TEST_F(TabletSchemaTest, test_tablet_index_field_pattern_property) {
    TabletIndex index_with_pattern;
    TabletIndexPB index_pb;
    index_pb.set_index_id(4001);
    index_pb.set_index_name("test_pattern_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(12001);

    auto* properties = index_pb.mutable_properties();
    (*properties)["field_pattern"] = "data.*.value";
    (*properties)["parser"] = "standard";

    index_with_pattern.init_from_pb(index_pb);

    EXPECT_EQ("data.*.value", index_with_pattern.field_pattern());

    TabletIndex index_without_pattern;
    TabletIndexPB index_pb2;
    index_pb2.set_index_id(4002);
    index_pb2.set_index_name("test_no_pattern_idx");
    index_pb2.set_index_type(IndexType::INVERTED);
    index_pb2.add_col_unique_id(12001);

    auto* properties2 = index_pb2.mutable_properties();
    (*properties2)["parser"] = "unicode";

    index_without_pattern.init_from_pb(index_pb2);

    EXPECT_EQ("", index_without_pattern.field_pattern());

    TabletIndex index_empty_pattern;
    TabletIndexPB index_pb3;
    index_pb3.set_index_id(4003);
    index_pb3.set_index_name("test_empty_pattern_idx");
    index_pb3.set_index_type(IndexType::INVERTED);
    index_pb3.add_col_unique_id(12001);

    auto* properties3 = index_pb3.mutable_properties();
    (*properties3)["field_pattern"] = "";
    (*properties3)["parser"] = "keyword";

    index_empty_pattern.init_from_pb(index_pb3);

    EXPECT_EQ("", index_empty_pattern.field_pattern());
}

TEST_F(TabletSchemaTest, test_tablet_schema_variant_max_subcolumns_count) {
    TabletSchema schema;

    EXPECT_EQ(0, schema.variant_max_subcolumns_count());

    TabletColumn non_variant_col;
    non_variant_col.set_unique_id(12001);
    non_variant_col.set_name("string_col");
    non_variant_col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    schema.append_column(non_variant_col);

    EXPECT_EQ(0, schema.variant_max_subcolumns_count());

    TabletColumn variant_col1;
    variant_col1.set_unique_id(12002);
    variant_col1.set_name("variant_col1");
    variant_col1.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_col1.set_variant_max_subcolumns_count(100);
    schema.append_column(variant_col1);

    EXPECT_EQ(100, schema.variant_max_subcolumns_count());

    TabletColumn variant_col2;
    variant_col2.set_unique_id(12003);
    variant_col2.set_name("variant_col2");
    variant_col2.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_col2.set_variant_max_subcolumns_count(200);
    schema.append_column(variant_col2);

    EXPECT_EQ(100, schema.variant_max_subcolumns_count());

    TabletSchema schema_with_zero_variant;
    TabletColumn variant_col_zero;
    variant_col_zero.set_unique_id(12004);
    variant_col_zero.set_name("variant_col_zero");
    variant_col_zero.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_col_zero.set_variant_max_subcolumns_count(0);
    schema_with_zero_variant.append_column(variant_col_zero);

    EXPECT_EQ(0, schema_with_zero_variant.variant_max_subcolumns_count());
}

TEST_F(TabletSchemaTest, test_tablet_schema_need_record_variant_extended_schema) {
    TabletSchema schema_empty;
    EXPECT_TRUE(schema_empty.need_record_variant_extended_schema());

    TabletSchema schema_non_variant;
    TabletColumn non_variant_col;
    non_variant_col.set_unique_id(13001);
    non_variant_col.set_name("int_col");
    non_variant_col.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    schema_non_variant.append_column(non_variant_col);
    EXPECT_TRUE(schema_non_variant.need_record_variant_extended_schema());

    TabletSchema schema_variant_zero;
    TabletColumn variant_col_zero;
    variant_col_zero.set_unique_id(13002);
    variant_col_zero.set_name("variant_col_zero");
    variant_col_zero.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_col_zero.set_variant_max_subcolumns_count(0);
    schema_variant_zero.append_column(variant_col_zero);
    EXPECT_TRUE(schema_variant_zero.need_record_variant_extended_schema());

    TabletSchema schema_variant_non_zero;
    TabletColumn variant_col_non_zero;
    variant_col_non_zero.set_unique_id(13003);
    variant_col_non_zero.set_name("variant_col_non_zero");
    variant_col_non_zero.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_col_non_zero.set_variant_max_subcolumns_count(50);
    schema_variant_non_zero.append_column(variant_col_non_zero);
    EXPECT_FALSE(schema_variant_non_zero.need_record_variant_extended_schema());

    TabletSchema schema_mixed;
    TabletColumn regular_col;
    regular_col.set_unique_id(13004);
    regular_col.set_name("regular_col");
    regular_col.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
    schema_mixed.append_column(regular_col);

    TabletColumn variant_col_100;
    variant_col_100.set_unique_id(13005);
    variant_col_100.set_name("variant_col_100");
    variant_col_100.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_col_100.set_variant_max_subcolumns_count(100);
    schema_mixed.append_column(variant_col_100);
    EXPECT_FALSE(schema_mixed.need_record_variant_extended_schema());

    TabletSchema schema_multiple_variants;
    TabletColumn variant_col1;
    variant_col1.set_unique_id(13006);
    variant_col1.set_name("variant_col1");
    variant_col1.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_col1.set_variant_max_subcolumns_count(150);
    schema_multiple_variants.append_column(variant_col1);

    TabletColumn variant_col2;
    variant_col2.set_unique_id(13007);
    variant_col2.set_name("variant_col2");
    variant_col2.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_col2.set_variant_max_subcolumns_count(300);
    schema_multiple_variants.append_column(variant_col2);
    EXPECT_FALSE(schema_multiple_variants.need_record_variant_extended_schema());
}

} // namespace doris