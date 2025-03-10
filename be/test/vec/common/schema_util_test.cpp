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

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/variant_column_writer_impl.h"

using namespace doris::vectorized;

using namespace doris::segment_v2;

using namespace doris;

class SchemaUtilTest : public testing::Test {
public:
    SchemaUtilTest() = default;
    virtual ~SchemaUtilTest() = default;
};

static void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                             const std::string& index_name, int32_t col_unique_id,
                             const std::string& column_type, const std::string& column_name,
                             const IndexType& index_type) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_nullable(false);
    column_pb->set_is_bf_column(true);
    tablet_index->set_index_id(index_id);
    tablet_index->set_index_name(index_name);
    tablet_index->set_index_type(index_type);
    tablet_index->add_col_unique_id(col_unique_id);
}

static void construct_subcolumn(TabletSchemaSPtr schema, const FieldType& type,
                                int32_t col_unique_id, std::string_view path,
                                std::vector<TabletColumn>* subcolumns) {
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

    schema_util::inherit_column_attributes(tablet_schema);
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

static std::unordered_map<std::string, int> construct_column_map_with_random_values(
        auto& column_map, int key_size, int value_size, const std::string& prefix) {
    std::unordered_map<std::string, int> key_value_counts;
    auto& key = assert_cast<ColumnString&>(column_map->get_keys());
    auto& value = assert_cast<ColumnString&>(column_map->get_values());
    auto& offsets = column_map->get_offsets();

    std::srand(42);

    for (int i = 0; i < key_size; ++i) {
        std::string current_key = prefix + std::to_string(i);

        int value_count = std::rand() % value_size + 1;
        key_value_counts[current_key] = value_count;

        for (int j = 0; j < value_count; ++j) {
            key.insert_data(current_key.data(), current_key.size());
            auto value_str = prefix + std::to_string(j);
            value.insert_data(value_str.data(), value_str.size());
        }
        offsets.push_back(key.size());
    }

    return key_value_counts;
}

TEST_F(SchemaUtilTest, calculate_variant_stats) {
    VariantStatisticsPB stats;
    auto column_map = ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                        ColumnArray::ColumnOffsets::create());

    const auto& key_value_counts =
            construct_column_map_with_random_values(column_map, 200, 100, "key_");

    // calculate stats
    schema_util::calculate_variant_stats(*column_map, &stats, 0, 200);
    EXPECT_EQ(stats.sparse_column_non_null_size_size(), key_value_counts.size());

    for (const auto& kv : key_value_counts) {
        auto it = stats.sparse_column_non_null_size().find(kv.first);
        EXPECT_NE(it, stats.sparse_column_non_null_size().end());
        EXPECT_EQ(it->second, kv.second);
    }

    // test with different key size
    column_map->clear();
    const auto& key_value_counts2 =
            construct_column_map_with_random_values(column_map, 3000, 100, "key_");
    schema_util::calculate_variant_stats(*column_map, &stats, 0, 3000);
    EXPECT_EQ(stats.sparse_column_non_null_size_size(), 3000);

    for (const auto& [path, size] : stats.sparse_column_non_null_size()) {
        auto first_size = key_value_counts.find(path) == key_value_counts.end()
                                  ? 0
                                  : key_value_counts.find(path)->second;
        auto second_size = key_value_counts2.find(path) == key_value_counts2.end()
                                   ? 0
                                   : key_value_counts2.find(path)->second;
        EXPECT_EQ(size, first_size + second_size);
    }

    // test with max size
    column_map->clear();
    const auto& key_value_counts3 = construct_column_map_with_random_values(
            column_map, VariantStatistics::MAX_SPARSE_DATA_STATISTICS_SIZE, 5, "key2_");
    schema_util::calculate_variant_stats(*column_map, &stats, 0, VariantStatistics::MAX_SPARSE_DATA_STATISTICS_SIZE);
    EXPECT_EQ(VariantStatistics::MAX_SPARSE_DATA_STATISTICS_SIZE,
              stats.sparse_column_non_null_size_size());

    for (const auto& [path, size] : stats.sparse_column_non_null_size()) {
        auto first_size = key_value_counts.find(path) == key_value_counts.end()
                                  ? 0
                                  : key_value_counts.find(path)->second;
        auto second_size = key_value_counts2.find(path) == key_value_counts2.end()
                                   ? 0
                                   : key_value_counts2.find(path)->second;
        auto third_size = key_value_counts3.find(path) == key_value_counts3.end()
                                  ? 0
                                  : key_value_counts3.find(path)->second;
        EXPECT_EQ(size, first_size + second_size + third_size);
    }
}

TEST_F(SchemaUtilTest, get_subpaths) {
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {
            {"path1", 1000}, {"path2", 800}, {"path3", 500}, {"path4", 300}, {"path5", 200}};

    // get subpaths
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::get_subpaths(variant, path_stats, uid_to_paths_set_info);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 3);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 2);

    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path1") !=
                uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path2") !=
                uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path3") !=
                uid_to_paths_set_info[1].sub_path_set.end());

    EXPECT_TRUE(uid_to_paths_set_info[1].sparse_path_set.find("path4") !=
                uid_to_paths_set_info[1].sparse_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sparse_path_set.find("path5") !=
                uid_to_paths_set_info[1].sparse_path_set.end());
}

TEST_F(SchemaUtilTest, get_subpaths_equal_to_max) {
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {{"path1", 1000}, {"path2", 800}, {"path3", 500}};

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::get_subpaths(variant, path_stats, uid_to_paths_set_info);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 3);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 0);

    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path1") !=
                uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path2") !=
                uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path3") !=
                uid_to_paths_set_info[1].sub_path_set.end());
}

TEST_F(SchemaUtilTest, get_subpaths_multiple_variants) {
    TabletColumn variant1;
    variant1.set_unique_id(1);
    variant1.set_variant_max_subcolumns_count(3);

    TabletColumn variant2;
    variant2.set_unique_id(2);
    variant2.set_variant_max_subcolumns_count(2);

    TabletColumn variant3;
    variant3.set_unique_id(3);
    variant3.set_variant_max_subcolumns_count(4);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {
            {"path1", 1000}, {"path2", 800}, {"path3", 500}, {"path4", 300}, {"path5", 200}};
    path_stats[2] = {{"path1", 1000}, {"path2", 800}};
    path_stats[3] = {{"path1", 1000}, {"path2", 800}, {"path3", 500}, {"path4", 300}};
    path_stats[4] = {
            {"path1", 1000}, {"path2", 800}, {"path3", 500}, {"path4", 300}, {"path5", 200}};

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::get_subpaths(variant1, path_stats, uid_to_paths_set_info);
    schema_util::get_subpaths(variant2, path_stats, uid_to_paths_set_info);
    schema_util::get_subpaths(variant3, path_stats, uid_to_paths_set_info);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 3);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 2);

    EXPECT_EQ(uid_to_paths_set_info[2].sub_path_set.size(), 2);
    EXPECT_EQ(uid_to_paths_set_info[2].sparse_path_set.size(), 0);

    EXPECT_EQ(uid_to_paths_set_info[3].sub_path_set.size(), 4);
    EXPECT_EQ(uid_to_paths_set_info[3].sparse_path_set.size(), 0);

    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path1") !=
                uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path2") !=
                uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path3") !=
                uid_to_paths_set_info[1].sub_path_set.end());

    EXPECT_TRUE(uid_to_paths_set_info[1].sparse_path_set.find("path4") !=
                uid_to_paths_set_info[1].sparse_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sparse_path_set.find("path5") !=
                uid_to_paths_set_info[1].sparse_path_set.end());

    EXPECT_TRUE(uid_to_paths_set_info[2].sub_path_set.find("path1") !=
                uid_to_paths_set_info[2].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[2].sub_path_set.find("path2") !=
                uid_to_paths_set_info[2].sub_path_set.end());

    EXPECT_TRUE(uid_to_paths_set_info[3].sub_path_set.find("path1") !=
                uid_to_paths_set_info[3].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[3].sub_path_set.find("path2") !=
                uid_to_paths_set_info[3].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[3].sub_path_set.find("path3") !=
                uid_to_paths_set_info[3].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[3].sub_path_set.find("path4") !=
                uid_to_paths_set_info[3].sub_path_set.end());
}

TEST_F(SchemaUtilTest, get_subpaths_no_path_stats) {
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[2] = {{"path1", 1000}, {"path2", 800}};

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::get_subpaths(variant, path_stats, uid_to_paths_set_info);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 0);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 0);
}
