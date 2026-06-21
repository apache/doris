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

#include "storage/tablet/tablet_schema.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <set>
#include <vector>

#include "storage/schema.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "storage/tablet_info.h"
#include "util/json/path_in_data.h"

namespace doris {

class TabletSchemaTest : public testing::Test {
protected:
    void SetUp() override {}
};

TEST_F(TabletSchemaTest, test_commit_tso_col_idx_from_append_column) {
    TabletSchema tablet_schema;
    tablet_schema.append_column(*create_int_key(0, false));
    EXPECT_EQ(-1, tablet_schema.commit_tso_col_idx());

    tablet_schema.append_column(*create_commit_tso_column(1));
    EXPECT_EQ(1, tablet_schema.commit_tso_col_idx());

    std::vector<ColumnId> column_ids {0, 1};
    Schema read_schema(tablet_schema.columns(), column_ids);
    EXPECT_EQ(1, read_schema.commit_tso_col_idx());
}

TEST_F(TabletSchemaTest, test_commit_tso_col_idx_from_current_index_schema) {
    TabletSchema ori_schema;
    ori_schema.append_column(*create_int_key(0, false));

    TabletColumn key_column = *create_int_key(0, false);
    TabletColumn commit_tso_column = *create_commit_tso_column(1);
    OlapTableIndexSchema index_schema;
    index_schema.index_id = 100;
    index_schema.schema_hash = 200;
    index_schema.columns.push_back(&key_column);
    index_schema.columns.push_back(&commit_tso_column);

    TabletSchema current;
    current.build_current_tablet_schema(index_schema.index_id, 2, &index_schema, ori_schema);
    EXPECT_EQ(1, current.commit_tso_col_idx());
}

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

    schema.append_index(std::move(index1));
    schema.append_index(std::move(index2));

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

TEST_F(TabletSchemaTest, test_get_bloom_filter_fpp_prefers_index_property) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_bf_fpp(0.11);

    auto* value_column = schema_pb.add_column();
    value_column->set_unique_id(1001);
    value_column->set_name("v1");
    value_column->set_type("INT");
    value_column->set_is_key(false);
    value_column->set_is_nullable(true);
    value_column->set_length(4);
    value_column->set_aggregation("NONE");
    value_column->set_is_bf_column(true);

    auto* bf_index = schema_pb.add_index();
    bf_index->set_index_id(1);
    bf_index->set_index_name("idx_v1");
    bf_index->set_index_type(IndexType::BLOOMFILTER);
    bf_index->add_col_unique_id(1001);
    (*bf_index->mutable_properties())["bloom_filter_fpp"] = "0.03";

    TabletSchema schema;
    schema.init_from_pb(schema_pb);

    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(1001), 0.03);
    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(schema.column_by_uid(1001)), 0.03);
}

TEST_F(TabletSchemaTest, test_get_bloom_filter_fpp_named_index_without_property_uses_default) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_bf_fpp(0.07);

    auto* value_column = schema_pb.add_column();
    value_column->set_unique_id(1002);
    value_column->set_name("v2");
    value_column->set_type("INT");
    value_column->set_is_key(false);
    value_column->set_is_nullable(true);
    value_column->set_length(4);
    value_column->set_aggregation("NONE");
    value_column->set_is_bf_column(true);

    auto* bf_index = schema_pb.add_index();
    bf_index->set_index_id(2);
    bf_index->set_index_name("idx_v2");
    bf_index->set_index_type(IndexType::BLOOMFILTER);
    bf_index->add_col_unique_id(1002);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);

    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(1002), BLOOM_FILTER_DEFAULT_FPP);
    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(schema.column_by_uid(1002)),
                     BLOOM_FILTER_DEFAULT_FPP);
}

TEST_F(TabletSchemaTest,
       test_get_bloom_filter_fpp_named_index_without_property_ignores_table_level_fpp) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_bf_fpp(0.07);

    auto* legacy_bf_column = schema_pb.add_column();
    legacy_bf_column->set_unique_id(1003);
    legacy_bf_column->set_name("legacy_bf_col");
    legacy_bf_column->set_type("INT");
    legacy_bf_column->set_is_key(false);
    legacy_bf_column->set_is_nullable(true);
    legacy_bf_column->set_length(4);
    legacy_bf_column->set_aggregation("NONE");
    legacy_bf_column->set_is_bf_column(true);

    auto* named_bf_column = schema_pb.add_column();
    named_bf_column->set_unique_id(1004);
    named_bf_column->set_name("named_bf_col");
    named_bf_column->set_type("INT");
    named_bf_column->set_is_key(false);
    named_bf_column->set_is_nullable(true);
    named_bf_column->set_length(4);
    named_bf_column->set_aggregation("NONE");
    named_bf_column->set_is_bf_column(true);

    auto* bf_index = schema_pb.add_index();
    bf_index->set_index_id(4);
    bf_index->set_index_name("idx_named_bf_col");
    bf_index->set_index_type(IndexType::BLOOMFILTER);
    bf_index->add_col_unique_id(1004);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);

    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(1003), 0.07);
    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(1004), BLOOM_FILTER_DEFAULT_FPP);
}

TEST_F(TabletSchemaTest, test_get_bloom_filter_fpp_legacy_column_uses_table_property) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_bf_fpp(0.07);

    auto* value_column = schema_pb.add_column();
    value_column->set_unique_id(1004);
    value_column->set_name("v4");
    value_column->set_type("INT");
    value_column->set_is_key(false);
    value_column->set_is_nullable(true);
    value_column->set_length(4);
    value_column->set_aggregation("NONE");
    value_column->set_is_bf_column(true);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);

    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(1004), 0.07);
    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(schema.column_by_uid(1004)), 0.07);
}

TEST_F(TabletSchemaTest, test_get_bloom_filter_fpp_uses_default_without_table_property) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);

    auto* value_column = schema_pb.add_column();
    value_column->set_unique_id(1003);
    value_column->set_name("v3");
    value_column->set_type("INT");
    value_column->set_is_key(false);
    value_column->set_is_nullable(true);
    value_column->set_length(4);
    value_column->set_aggregation("NONE");
    value_column->set_is_bf_column(true);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);

    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(1003), BLOOM_FILTER_DEFAULT_FPP);
    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(schema.column_by_uid(1003)),
                     BLOOM_FILTER_DEFAULT_FPP);
}

TEST_F(TabletSchemaTest, test_get_bloom_filter_fpp_for_extracted_column_uses_parent_index) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_bf_fpp(0.09);

    auto* variant_column = schema_pb.add_column();
    variant_column->set_unique_id(2001);
    variant_column->set_name("v");
    variant_column->set_type("VARIANT");
    variant_column->set_is_key(false);
    variant_column->set_is_nullable(true);
    variant_column->set_length(4);
    variant_column->set_aggregation("NONE");
    variant_column->set_is_bf_column(true);
    variant_column->set_variant_max_subcolumns_count(8);

    auto* bf_index = schema_pb.add_index();
    bf_index->set_index_id(3);
    bf_index->set_index_name("idx_v");
    bf_index->set_index_type(IndexType::BLOOMFILTER);
    bf_index->add_col_unique_id(2001);
    (*bf_index->mutable_properties())["bloom_filter_fpp"] = "0.02";

    TabletSchema schema;
    schema.init_from_pb(schema_pb);

    TabletColumn extracted;
    extracted.set_name("v.a");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    extracted.set_parent_unique_id(2001);
    extracted.set_path_info(PathInData("v.a"));
    extracted.set_is_nullable(true);

    EXPECT_TRUE(extracted.is_extracted_column());
    EXPECT_DOUBLE_EQ(schema.get_bloom_filter_fpp(extracted), 0.02);
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

    PathInData typed_path1("variant_col.user.name", true);
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

    PathInData typed_path2("variant_col.user.age", true);
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

    PathInData subcolumn_path1("variant_col.product.id");
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

    PathInData non_existing_path("variant_col.non.existing");
    non_existing_col.set_path_info(non_existing_path);

    auto no_indexes = schema.inverted_indexs(non_existing_col);
    EXPECT_EQ(0, no_indexes.size());

    TabletColumn wrong_parent_col;
    wrong_parent_col.set_unique_id(-1);
    wrong_parent_col.set_name("wrong_parent");
    wrong_parent_col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    wrong_parent_col.set_parent_unique_id(9999); // Non-existing parent

    PathInData wrong_parent_path("wrong_variant.some.path");
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

TEST_F(TabletSchemaTest, test_tablet_schema_get_index) {
    TabletSchema schema;

    TabletColumn col1;
    col1.set_unique_id(14001);
    col1.set_name("test_col1");
    col1.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    schema.append_column(col1);

    TabletColumn col2;
    col2.set_unique_id(14002);
    col2.set_name("test_col2");
    col2.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
    schema.append_column(col2);

    TabletIndex inverted_index;
    TabletIndexPB inverted_index_pb;
    inverted_index_pb.set_index_id(5001);
    inverted_index_pb.set_index_name("inverted_idx");
    inverted_index_pb.set_index_type(IndexType::INVERTED);
    inverted_index_pb.add_col_unique_id(14001);
    inverted_index.init_from_pb(inverted_index_pb);

    TabletIndex ann_index;
    TabletIndexPB ann_index_pb;
    ann_index_pb.set_index_id(5003);
    ann_index_pb.set_index_name("ann_idx");
    ann_index_pb.set_index_type(IndexType::ANN);
    ann_index_pb.add_col_unique_id(14002);
    ann_index.init_from_pb(ann_index_pb);

    TabletIndex ngram_bf_index;
    TabletIndexPB ngram_bf_index_pb;
    ngram_bf_index_pb.set_index_id(5004);
    ngram_bf_index_pb.set_index_name("ngram_bf_idx");
    ngram_bf_index_pb.set_index_type(IndexType::NGRAM_BF);
    ngram_bf_index_pb.add_col_unique_id(14002);
    ngram_bf_index.init_from_pb(ngram_bf_index_pb);

    schema.append_index(std::move(inverted_index));
    schema.append_index(std::move(ann_index));
    schema.append_index(std::move(ngram_bf_index));

    const TabletIndex* found_inverted = schema.get_index(14001, IndexType::INVERTED, "");
    EXPECT_NE(nullptr, found_inverted);
    EXPECT_EQ("inverted_idx", found_inverted->index_name());
    EXPECT_EQ(5001, found_inverted->index_id());
    const TabletIndex* found_ann = schema.get_index(14002, IndexType::ANN, "");
    EXPECT_NE(nullptr, found_ann);
    EXPECT_EQ("ann_idx", found_ann->index_name());
    EXPECT_EQ(5003, found_ann->index_id());
    const TabletIndex* found_ngram_bf = schema.get_index(14002, IndexType::NGRAM_BF, "");
    EXPECT_NE(nullptr, found_ngram_bf);
    EXPECT_EQ("ngram_bf_idx", found_ngram_bf->index_name());
    EXPECT_EQ(5004, found_ngram_bf->index_id());
    const TabletIndex* not_found = schema.get_index(99999, IndexType::INVERTED, "");
    EXPECT_EQ(nullptr, not_found);
    const TabletIndex* empty_suffix = schema.get_index(14001, IndexType::INVERTED, "");
    EXPECT_NE(nullptr, empty_suffix);
    EXPECT_EQ("inverted_idx", empty_suffix->index_name());
    const TabletIndex* with_suffix = schema.get_index(14001, IndexType::INVERTED, "test_suffix");
    EXPECT_EQ(nullptr, with_suffix);

    EXPECT_TRUE(found_inverted->is_inverted_index());
    EXPECT_EQ(IndexType::INVERTED, found_inverted->index_type());
    EXPECT_EQ(IndexType::ANN, found_ann->index_type());
    EXPECT_EQ(IndexType::NGRAM_BF, found_ngram_bf->index_type());

    const auto& inverted_col_ids = found_inverted->col_unique_ids();
    EXPECT_EQ(1, inverted_col_ids.size());
    EXPECT_EQ(14001, inverted_col_ids[0]);
    const auto& ann_col_ids = found_ann->col_unique_ids();
    EXPECT_EQ(1, ann_col_ids.size());
    EXPECT_EQ(14002, ann_col_ids[0]);
}

// Rolling-upgrade compat: a TabletSchemaPB persisted by an old BE has the three
// legacy V3-flavor flags but no storage_format. init_from_pb must derive V3.
TEST_F(TabletSchemaTest, init_from_pb_legacy_flags_derive_v3) {
    TabletSchemaPB pb;
    pb.set_keys_type(DUP_KEYS);
    pb.set_is_external_segment_column_meta_used(true);
    pb.set_integer_type_default_use_plain_encoding(true);
    pb.set_binary_plain_encoding_default_impl(BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2);
    // storage_format is intentionally not set
    ASSERT_FALSE(pb.has_storage_format());

    TabletSchema schema;
    schema.init_from_pb(pb);
    EXPECT_EQ(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3, schema.storage_format());
}

// PB with neither storage_format nor any of the legacy flags falls back to V2.
TEST_F(TabletSchemaTest, init_from_pb_no_flags_defaults_v2) {
    TabletSchemaPB pb;
    pb.set_keys_type(DUP_KEYS);
    ASSERT_FALSE(pb.has_storage_format());

    TabletSchema schema;
    schema.init_from_pb(pb);
    EXPECT_EQ(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2, schema.storage_format());
}

// Rolling-downgrade compat: a V3 TabletSchema must redundantly emit the three
// legacy flags so an old BE rolled back from a new one can still recognize V3.
TEST_F(TabletSchemaTest, to_schema_pb_v3_emits_legacy_flags) {
    TabletSchemaPB in;
    in.set_keys_type(DUP_KEYS);
    in.set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);

    TabletSchema schema;
    schema.init_from_pb(in);
    ASSERT_EQ(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3, schema.storage_format());

    TabletSchemaPB out;
    schema.to_schema_pb(&out);
    EXPECT_EQ(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3, out.storage_format());
    EXPECT_TRUE(out.is_external_segment_column_meta_used());
    EXPECT_TRUE(out.integer_type_default_use_plain_encoding());
    EXPECT_EQ(BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2,
              out.binary_plain_encoding_default_impl());
}

// V2 schemas must NOT emit the V3-flavor legacy flags.
TEST_F(TabletSchemaTest, to_schema_pb_v2_skips_legacy_flags) {
    TabletSchemaPB in;
    in.set_keys_type(DUP_KEYS);
    in.set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);

    TabletSchema schema;
    schema.init_from_pb(in);
    ASSERT_EQ(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2, schema.storage_format());

    TabletSchemaPB out;
    schema.to_schema_pb(&out);
    EXPECT_EQ(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2, out.storage_format());
    EXPECT_FALSE(out.is_external_segment_column_meta_used());
    EXPECT_FALSE(out.integer_type_default_use_plain_encoding());
    EXPECT_NE(BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2,
              out.binary_plain_encoding_default_impl());
}

} // namespace doris
