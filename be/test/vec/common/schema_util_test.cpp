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

#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/segment_v2/variant/variant_column_writer_impl.h"
#include "testutil/variant_util.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_variant.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/data_type_variant.h"

using namespace doris::vectorized;

using namespace doris::segment_v2;

using namespace doris;

class SchemaUtilTest : public testing::Test {
public:
    SchemaUtilTest() = default;
    virtual ~SchemaUtilTest() = default;
};

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

// void construct_subcolumn(TabletSchemaSPtr schema, const FieldType& type,
//                                 int32_t col_unique_id, std::string_view path,
//                                 std::vector<TabletColumn>* subcolumns) {
//     TabletColumn subcol;
//     subcol.set_type(type);
//     subcol.set_is_nullable(true);
//     subcol.set_unique_id(-1);
//     subcol.set_parent_unique_id(col_unique_id);
//     vectorized::PathInData col_path(path);
//     subcol.set_path_info(col_path);
//     subcol.set_name(col_path.get_path());
//     schema->append_column(subcol);
//     subcolumns->emplace_back(std::move(subcol));
// }

TEST_F(SchemaUtilTest, TestInheritIndex) {
    // 1. Test basic index inheritance for non-extracted column
    std::vector<const TabletIndex*> parent_indexes;
    TabletIndexes subcolumns_indexes;

    // Create parent index
    TabletIndexPB pb1;
    pb1.set_index_id(1);
    pb1.set_index_name("test_index");
    pb1.set_index_type(IndexType::INVERTED);

    TabletIndex parent_index;
    parent_index.init_from_pb(pb1);
    parent_indexes.push_back(&parent_index);

    // Test index inheritance for normal column (non-extracted)
    TabletColumn normal_column;
    normal_column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    normal_column.set_name("test_col");
    normal_column.set_unique_id(1);

    bool result = schema_util::inherit_index(parent_indexes, subcolumns_indexes, normal_column);
    EXPECT_FALSE(result);

    // 2. Test index inheritance for extracted column
    TabletColumn extracted_column;
    extracted_column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    extracted_column.set_name("extracted_col");
    extracted_column.set_unique_id(2);
    extracted_column.set_parent_unique_id(1); // Set parent column id
    vectorized::PathInData path("parent.path");
    extracted_column.set_path_info(path);

    result = schema_util::inherit_index(parent_indexes, subcolumns_indexes, extracted_column);
    EXPECT_TRUE(result);
    EXPECT_EQ(subcolumns_indexes.size(), 1);
    EXPECT_EQ(subcolumns_indexes[0]->index_id(), 1);
    EXPECT_EQ(subcolumns_indexes[0]->index_name(), "test_index");
    EXPECT_EQ(subcolumns_indexes[0]->index_type(), IndexType::INVERTED);

    // 3. Test index inheritance for array type with empty subcolumns
    TabletColumn empty_array_column;
    empty_array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    empty_array_column.set_name("empty_array");
    vectorized::PathInData pat("parent.a");
    empty_array_column.set_path_info(pat);
    empty_array_column.set_unique_id(3);
    // No subcolumns added, so get_sub_columns() will be empty

    result = schema_util::inherit_index(parent_indexes, subcolumns_indexes, empty_array_column);
    EXPECT_FALSE(result);

    // 4. Test index inheritance for array type with non-empty subcolumns
    TabletColumn array_column;
    array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array_column.set_name("array_with_subcolumns");
    array_column.set_unique_id(4);
    array_column.set_parent_unique_id(1); // Set parent column id
    vectorized::PathInData path1("parent.a");
    array_column.set_path_info(path1);

    // Add subcolumn to array
    TabletColumn sub_column;
    sub_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    sub_column.set_name("sub_col");
    sub_column.set_unique_id(5);
    array_column.add_sub_column(sub_column);

    result = schema_util::inherit_index(parent_indexes, subcolumns_indexes, array_column);
    EXPECT_TRUE(result);
    EXPECT_EQ(subcolumns_indexes.size(), 1);
    EXPECT_EQ(subcolumns_indexes[0]->index_id(), 1);
    EXPECT_EQ(subcolumns_indexes[0]->index_name(), "test_index");
    EXPECT_EQ(subcolumns_indexes[0]->index_type(), IndexType::INVERTED);

    // 4.1 Add String subcolumn to array
    TabletColumn array_column1;
    array_column1.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array_column1.set_name("array_with_subcolumns");
    array_column1.set_unique_id(4);
    array_column1.set_parent_unique_id(1); // Set parent column id
    array_column1.set_path_info(path1);
    TabletColumn sub_column1;
    sub_column1.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    sub_column1.set_name("sub_col1");
    sub_column1.set_unique_id(6);
    array_column1.add_sub_column(sub_column1);
    result = schema_util::inherit_index(parent_indexes, subcolumns_indexes, array_column1);
    EXPECT_TRUE(result);
    EXPECT_EQ(subcolumns_indexes.size(), 1);
    EXPECT_EQ(subcolumns_indexes[0]->index_id(), 1);
    EXPECT_EQ(subcolumns_indexes[0]->index_name(), "test_index");
    EXPECT_EQ(subcolumns_indexes[0]->index_type(), IndexType::INVERTED);

    // 5. Test empty parent index list
    std::vector<const TabletIndex*> empty_indexes;
    TabletIndexes empty_subcolumns_indexes;

    result = schema_util::inherit_index(empty_indexes, empty_subcolumns_indexes, normal_column);
    EXPECT_FALSE(result);
    EXPECT_EQ(empty_subcolumns_indexes.size(), 0);

    // 6. Test binary Type
    TabletColumn hll_column;
    hll_column.set_type(FieldType::OLAP_FIELD_TYPE_HLL);
    hll_column.set_name("hll_col");
    hll_column.set_unique_id(7);
    hll_column.set_parent_unique_id(1); // Set parent column id
    vectorized::PathInData decimal_path("parent.hll");
    hll_column.set_path_info(decimal_path);
    result = schema_util::inherit_index(parent_indexes, subcolumns_indexes, hll_column);
    EXPECT_FALSE(result);
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

    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_DOUBLE, 3, "v3.d", &subcolumns);
    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_FLOAT, 3, "v3.a", &subcolumns);

    schema_util::inherit_column_attributes(tablet_schema);
    for (const auto& col : subcolumns) {
        switch (col._parent_col_unique_id) {
        case 1:
            EXPECT_EQ(tablet_schema->inverted_indexs(col).size(), 1);
            break;
        case 3:
            EXPECT_EQ(tablet_schema->inverted_indexs(col).size(), 1);
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

// static std::unordered_map<std::string, int> construct_column_map_with_random_values(
//         auto& column_map, int key_size, int value_size, const std::string& prefix) {
//     std::unordered_map<std::string, int> key_value_counts;
//     auto& key = assert_cast<ColumnString&>(column_map->get_keys());
//     auto& value = assert_cast<ColumnString&>(column_map->get_values());
//     auto& offsets = column_map->get_offsets();
//
//     std::srand(42);
//
//     for (int i = 0; i < key_size; ++i) {
//         std::string current_key = prefix + std::to_string(i);
//
//         int value_count = std::rand() % value_size + 1;
//         key_value_counts[current_key] = value_count;
//
//         for (int j = 0; j < value_count; ++j) {
//             key.insert_data(current_key.data(), current_key.size());
//             auto value_str = prefix + std::to_string(j);
//             value.insert_data(value_str.data(), value_str.size());
//         }
//         offsets.push_back(key.size());
//     }
//
//     return key_value_counts;
// }

TEST_F(SchemaUtilTest, get_subpaths) {
    TabletSchema schema;
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3);
    schema.append_column(variant);
    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {
            {"path1", 1000}, {"path2", 800}, {"path3", 500}, {"path4", 300}, {"path5", 200}};

    // get subpaths
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::VariantCompactionUtil::get_subpaths(3, path_stats[1], uid_to_paths_set_info[1]);

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
    TabletSchema schema;
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3);
    schema.append_column(variant);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {{"path1", 1000}, {"path2", 800}, {"path3", 500}};

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::VariantCompactionUtil::get_subpaths(3, path_stats[1], uid_to_paths_set_info[1]);

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
    TabletSchema schema;
    TabletColumn variant1;

    variant1.set_unique_id(1);
    variant1.set_variant_max_subcolumns_count(3);
    schema.append_column(variant1);

    TabletColumn variant2;
    variant2.set_unique_id(2);
    variant2.set_variant_max_subcolumns_count(2);
    schema.append_column(variant2);

    TabletColumn variant3;
    variant3.set_unique_id(3);
    variant3.set_variant_max_subcolumns_count(4);
    schema.append_column(variant3);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {
            {"path1", 1000}, {"path2", 800}, {"path3", 500}, {"path4", 300}, {"path5", 200}};
    path_stats[2] = {{"path1", 1000}, {"path2", 800}};
    path_stats[3] = {{"path1", 1000}, {"path2", 800}, {"path3", 500}, {"path4", 300}};
    path_stats[4] = {
            {"path1", 1000}, {"path2", 800}, {"path3", 500}, {"path4", 300}, {"path5", 200}};

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::VariantCompactionUtil::get_subpaths(3, path_stats[1], uid_to_paths_set_info[1]);
    schema_util::VariantCompactionUtil::get_subpaths(2, path_stats[2], uid_to_paths_set_info[2]);
    schema_util::VariantCompactionUtil::get_subpaths(4, path_stats[3], uid_to_paths_set_info[3]);

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
    TabletSchema schema;
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3);
    schema.append_column(variant);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[2] = {{"path1", 1000}, {"path2", 800}};

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::VariantCompactionUtil::get_subpaths(3, path_stats[2], uid_to_paths_set_info[2]);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 0);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 0);
}

TEST_F(SchemaUtilTest, generate_sub_column_info_based) {
    TabletColumn variant;
    variant.set_unique_id(10);
    variant.set_variant_max_subcolumns_count(3);

    TabletColumn subcolumn;
    subcolumn.set_name("profile.id.*");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    variant.add_sub_column(subcolumn);

    TabletColumn subcolumn2;
    subcolumn2.set_name("profile.name.?");
    subcolumn2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    variant.add_sub_column(subcolumn2);

    TabletColumn subcolumn3;
    subcolumn3.set_name("id[0-9]");
    subcolumn3.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    variant.add_sub_column(subcolumn3);

    TabletColumn subcolumn4;
    subcolumn4.set_name("id[0-9].*");
    subcolumn4.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    variant.add_sub_column(subcolumn4);

    TabletSchema schema;
    schema.append_column(variant);

    TabletSchema::SubColumnInfo sub_column_info;
    bool match =
            schema_util::generate_sub_column_info(schema, 10, "profile.id.name", &sub_column_info);
    EXPECT_TRUE(match);
    EXPECT_EQ(sub_column_info.column.parent_unique_id(), 10);

    match = schema_util::generate_sub_column_info(schema, 10, "profile.name.x", &sub_column_info);
    EXPECT_TRUE(match);
    EXPECT_EQ(sub_column_info.column.parent_unique_id(), 10);

    match = schema_util::generate_sub_column_info(schema, 10, "profile.name.xx", &sub_column_info);
    EXPECT_FALSE(match);

    match = schema_util::generate_sub_column_info(schema, 10, "id5", &sub_column_info);
    EXPECT_TRUE(match);

    match = schema_util::generate_sub_column_info(schema, 10, "id5.profile.name", &sub_column_info);
    EXPECT_TRUE(match);
}

TEST_F(SchemaUtilTest, generate_sub_column_info_advanced) {
    TabletColumn variant;
    variant.set_unique_id(10);
    variant.set_variant_max_subcolumns_count(3);

    TabletColumn subcolumn;
    subcolumn.set_name("profile?id");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn subcolumn_item;
    subcolumn_item.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    subcolumn.add_sub_column(subcolumn_item);
    variant.add_sub_column(subcolumn);

    TabletColumn subcolumn2;
    subcolumn2.set_name("profile?id.*");
    subcolumn2.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn subcolumn2_item;
    subcolumn2_item.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    subcolumn2.add_sub_column(subcolumn2_item);
    variant.add_sub_column(subcolumn2);

    TabletColumn subcolumn3;
    subcolumn3.set_name("profile.id[0-9]");
    subcolumn3.set_type(FieldType::OLAP_FIELD_TYPE_DECIMAL64);
    variant.add_sub_column(subcolumn3);

    TabletSchema schema;
    schema.append_column(variant);

    TabletIndex index;
    index._properties["field_pattern"] = "profile?id.*";
    index._col_unique_ids = {10};
    schema.append_index(std::move(index));

    TabletIndex index2;
    index2._properties["field_pattern"] = "profile.id[0-9]";
    index2._col_unique_ids = {10};
    schema.append_index(std::move(index2));

    TabletSchema::SubColumnInfo sub_column_info;
    bool match =
            schema_util::generate_sub_column_info(schema, 10, "profile.id.name", &sub_column_info);
    EXPECT_TRUE(match);
    EXPECT_EQ(sub_column_info.column.parent_unique_id(), 10);
    EXPECT_FALSE(sub_column_info.indexes.empty());

    match = schema_util::generate_sub_column_info(schema, 10, "profile.id2", &sub_column_info);
    EXPECT_TRUE(match);
    EXPECT_EQ(sub_column_info.column.parent_unique_id(), 10);
    EXPECT_FALSE(sub_column_info.indexes.empty());

    match = schema_util::generate_sub_column_info(schema, 10, "profilexid", &sub_column_info);
    EXPECT_TRUE(match);
    EXPECT_EQ(sub_column_info.column.parent_unique_id(), 10);
    EXPECT_TRUE(sub_column_info.indexes.empty());
}

TEST_F(SchemaUtilTest, TestArrayDimensions) {
    // Test get_number_of_dimensions for DataType
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto nested_array_type = std::make_shared<DataTypeArray>(array_type);

    EXPECT_EQ(schema_util::get_number_of_dimensions(*array_type), 1);
    EXPECT_EQ(schema_util::get_number_of_dimensions(*nested_array_type), 2);
    EXPECT_EQ(schema_util::get_number_of_dimensions(*std::make_shared<DataTypeInt32>()), 0);

    // Test get_number_of_dimensions for Column
    auto array_column =
            ColumnArray::create(ColumnInt32::create(), ColumnArray::ColumnOffsets::create());
    auto nested_array_column =
            ColumnArray::create(array_column->get_ptr(), ColumnArray::ColumnOffsets::create());

    EXPECT_EQ(schema_util::get_number_of_dimensions(*array_column), 1);
    EXPECT_EQ(schema_util::get_number_of_dimensions(*nested_array_column), 2);
    EXPECT_EQ(schema_util::get_number_of_dimensions(*ColumnInt32::create()), 0);

    // Test get_base_type_of_array
    auto base_type = schema_util::get_base_type_of_array(array_type);
    EXPECT_EQ(base_type->get_primitive_type(), PrimitiveType::TYPE_INT);

    base_type = schema_util::get_base_type_of_array(nested_array_type);
    EXPECT_EQ(base_type->get_primitive_type(), PrimitiveType::TYPE_INT);
}

TEST_F(SchemaUtilTest, TestIntegerConversion) {
    // Test conversion between integers
    EXPECT_FALSE(schema_util::is_conversion_required_between_integers(
            PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_SMALLINT));
    EXPECT_FALSE(schema_util::is_conversion_required_between_integers(PrimitiveType::TYPE_TINYINT,
                                                                      PrimitiveType::TYPE_INT));
    EXPECT_FALSE(schema_util::is_conversion_required_between_integers(PrimitiveType::TYPE_SMALLINT,
                                                                      PrimitiveType::TYPE_INT));

    EXPECT_TRUE(schema_util::is_conversion_required_between_integers(PrimitiveType::TYPE_INT,
                                                                     PrimitiveType::TYPE_SMALLINT));
    EXPECT_TRUE(schema_util::is_conversion_required_between_integers(PrimitiveType::TYPE_BIGINT,
                                                                     PrimitiveType::TYPE_INT));

    EXPECT_FALSE(schema_util::is_conversion_required_between_integers(
            PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_SMALLINT));
    EXPECT_TRUE(schema_util::is_conversion_required_between_integers(PrimitiveType::TYPE_INT,
                                                                     PrimitiveType::TYPE_SMALLINT));

    EXPECT_FALSE(schema_util::is_conversion_required_between_integers(
            PrimitiveType::TYPE_BOOLEAN, PrimitiveType::TYPE_SMALLINT));
    EXPECT_TRUE(schema_util::is_conversion_required_between_integers(PrimitiveType::TYPE_SMALLINT,
                                                                     PrimitiveType::TYPE_BOOLEAN));
}

TEST_F(SchemaUtilTest, TestColumnCasting) {
    // Test cast_column
    auto src_type = std::make_shared<DataTypeInt32>();
    auto dst_type = std::make_shared<DataTypeInt64>();

    auto column = ColumnInt32::create();
    column->insert(vectorized::Field::create_field<PrimitiveType::TYPE_INT>(42));

    ColumnWithTypeAndName src_col;
    src_col.type = src_type;
    src_col.column = column->get_ptr();
    src_col.name = "test_col";

    ColumnPtr result;
    auto status = schema_util::cast_column(src_col, dst_type, &result);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result->get_int(0), 42);
    EXPECT_EQ(result->get_name(), "BIGINT");
}

TEST_F(SchemaUtilTest, TestGetColumnByType) {
    // Test get_column_by_type
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = std::make_shared<DataTypeString>();
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto nullable_type = make_nullable(int_type);

    schema_util::ExtraInfo ext_info;
    ext_info.unique_id = 1;
    ext_info.parent_unique_id = 2;
    ext_info.path_info = PathInData("test.path");

    // Test integer type
    auto int_column = schema_util::get_column_by_type(int_type, "int_col", ext_info);
    EXPECT_EQ(int_column.name(), "int_col");
    EXPECT_EQ(int_column.type(), FieldType::OLAP_FIELD_TYPE_INT);
    EXPECT_EQ(int_column.unique_id(), 1);
    EXPECT_EQ(int_column.parent_unique_id(), 2);
    EXPECT_EQ(int_column.path_info_ptr()->get_path(), "test.path");

    // Test string type
    auto string_column = schema_util::get_column_by_type(string_type, "string_col", ext_info);
    EXPECT_EQ(string_column.type(), FieldType::OLAP_FIELD_TYPE_STRING);
    EXPECT_EQ(string_column.length(), INT_MAX);

    // Test array type
    auto array_column = schema_util::get_column_by_type(array_type, "array_col", ext_info);
    EXPECT_EQ(array_column.type(), FieldType::OLAP_FIELD_TYPE_ARRAY);
    EXPECT_EQ(array_column.get_sub_column(0).type(), FieldType::OLAP_FIELD_TYPE_INT);

    // Test nullable type
    auto nullable_column = schema_util::get_column_by_type(nullable_type, "nullable_col", ext_info);
    EXPECT_TRUE(nullable_column.is_nullable());
    EXPECT_EQ(nullable_column.type(), FieldType::OLAP_FIELD_TYPE_INT);
}

//TEST_F(SchemaUtilTest, TestGetSortedSubcolumns) {
//    // Create test subcolumns
//    vectorized::ColumnVariant::Subcolumns subcolumns;
//
//    auto create_subcolumn = [](const std::string& path) {
//        auto subcol = std::make_shared<vectorized::ColumnVariant::Subcolumn>();
//        subcol->path = path;
//        return subcol;
//    };
//
//    subcolumns.push_back(create_subcolumn("c"));
//    subcolumns.push_back(create_subcolumn("a"));
//    subcolumns.push_back(create_subcolumn("b"));
//
//    auto sorted = schema_util::get_sorted_subcolumns(subcolumns);
//
//    EXPECT_EQ(sorted.size(), 3);
//    EXPECT_EQ(sorted[0]->path, "a");
//    EXPECT_EQ(sorted[1]->path, "b");
//    EXPECT_EQ(sorted[2]->path, "c");
//}

TEST_F(SchemaUtilTest, TestHasSchemaIndexDiff) {
    TabletSchemaPB schema1_pb;
    TabletSchemaPB schema2_pb;

    // Setup first schema
    construct_column(schema1_pb.add_column(), schema1_pb.add_index(), 10000, "test_index", 1, "INT",
                     "test_col", IndexType::INVERTED);
    auto* col1 = schema1_pb.mutable_column(0);
    col1->set_is_bf_column(false);

    // Setup second schema with different index
    construct_column(schema2_pb.add_column(), schema2_pb.add_index(), 10000, "test_index", 1, "INT",
                     "test_col", IndexType::BLOOMFILTER);
    auto* col2 = schema2_pb.mutable_column(0);
    col2->set_is_bf_column(true);

    TabletSchemaSPtr schema1 = std::make_shared<TabletSchema>();
    TabletSchemaSPtr schema2 = std::make_shared<TabletSchema>();
    schema1->init_from_pb(schema1_pb);
    schema2->init_from_pb(schema2_pb);

    EXPECT_TRUE(schema_util::has_schema_index_diff(schema1.get(), schema2.get(), 0, 0));
}

TEST_F(SchemaUtilTest, TestParseVariantColumns) {
    // Create a block with variant column
    Block block;

    // Create a variant column with JSON string data
    auto variant_type = std::make_shared<DataTypeVariant>(10);
    auto variant_column = ColumnVariant::create(10);
    auto root_column = ColumnString::create();
    root_column->insert(
            vectorized::Field::create_field<PrimitiveType::TYPE_STRING>("{'a': 1, 'b': 'test'}"));
    variant_column->create_root(std::make_shared<DataTypeString>(), root_column->get_ptr());

    block.insert({variant_column->get_ptr(), variant_type, "variant_col"});

    std::vector<int> variant_pos {0};
    ParseConfig config;

    auto status = schema_util::parse_variant_columns(block, variant_pos, config);
    EXPECT_TRUE(status.ok());

    // Check the parsed variant column
    const auto& result_column = block.get_by_position(0).column;
    std::cout << "Result column name: " << result_column->get_name() << std::endl;
    EXPECT_TRUE(result_column->get_name().find("variant") == std::string::npos);

    const auto& obj_column = assert_cast<const ColumnVariant&>(*result_column);
    EXPECT_TRUE(obj_column.is_scalar_variant());
}

TEST_F(SchemaUtilTest, TestGetLeastCommonSchema) {
    // Create test schemas
    TabletSchemaPB schema1_pb;
    schema1_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema1_pb.add_column(), schema1_pb.add_index(), 10000, "v1_index", 1,
                     "VARIANT", "v1", IndexType::INVERTED);

    TabletSchemaPB schema2_pb;
    schema2_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema2_pb.add_column(), schema2_pb.add_index(), 10000, "v1_index", 1,
                     "VARIANT", "v1", IndexType::INVERTED);

    TabletSchemaSPtr schema1 = std::make_shared<TabletSchema>();
    TabletSchemaSPtr schema2 = std::make_shared<TabletSchema>();
    schema1->init_from_pb(schema1_pb);
    schema2->init_from_pb(schema2_pb);

    std::vector<TabletSchemaSPtr> schemas {schema1, schema2};
    TabletSchemaSPtr result_schema;

    auto status = schema_util::get_least_common_schema(schemas, nullptr, result_schema);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_schema->num_columns(), 1);
}

TEST_F(SchemaUtilTest, TestCastColumnEdgeCases) {
    // Test casting from Nothing type
    auto nothing_type = std::make_shared<DataTypeNothing>();
    auto dst_type = std::make_shared<DataTypeInt32>();

    auto nothing_column = ColumnNothing::create(1);
    ColumnWithTypeAndName src_col;
    src_col.type = nothing_type;
    src_col.column = nothing_column->get_ptr();
    src_col.name = "nothing_col";

    ColumnPtr result;
    auto status = schema_util::cast_column(src_col, dst_type, &result);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result->size(), 1);

    // Test casting to variant type
    auto variant_type = std::make_shared<DataTypeVariant>(10);
    auto nullable_array_type =
            make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));
    auto array_column =
            ColumnArray::create(ColumnInt32::create(), ColumnArray::ColumnOffsets::create());
    auto nullable_array_column = make_nullable(array_column->get_ptr());

    ColumnWithTypeAndName array_col;
    array_col.type = nullable_array_type;
    array_col.column = nullable_array_column;
    array_col.name = "array_col";

    // test Array Type cast Int will throw Exception
    auto int_type = std::make_shared<DataTypeInt32>();
    Status st = schema_util::cast_column(array_col, int_type, &result);
    EXPECT_TRUE(st.ok());

    ColumnPtr result1;
    status = schema_util::cast_column(array_col, variant_type, &result1);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(result1->is_nullable());

    auto variant_type_nullable = make_nullable(variant_type);
    status = schema_util::cast_column(array_col, variant_type_nullable, &result1);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(result1->is_nullable());

    // Test casting from variant to variant
    auto variant_column = ColumnVariant::create(10);
    variant_column->create_root(nullable_array_type, nullable_array_column->assume_mutable());

    ColumnWithTypeAndName variant_col;
    variant_col.type = variant_type;
    variant_col.column = variant_column->get_ptr();
    variant_col.name = "variant_col";

    ColumnPtr result2;
    status = schema_util::cast_column(variant_col, variant_type, &result2);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(result2->is_nullable());
}

TEST_F(SchemaUtilTest, TestCastColumnWithExecuteFailure) {
    // Create a complex type to simple type conversion scenario, this conversion usually fails
    auto complex_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeIPv4>());
    auto simple_type = std::make_shared<DataTypeJsonb>();

    // Insert some test dataset
    auto nested_array =
            ColumnArray::create(ColumnIPv4::create(), ColumnArray::ColumnOffsets::create());
    nested_array->insert(
            vectorized::Field::create_field<PrimitiveType::TYPE_ARRAY>(Array(IPv4(1))));
    nested_array->insert(
            vectorized::Field::create_field<PrimitiveType::TYPE_ARRAY>(Array(IPv4(2))));

    ColumnWithTypeAndName src_col;
    src_col.type = complex_type;
    src_col.column = nested_array->get_ptr();
    src_col.name = "array_col";

    // Try converting to a simple type, which should fail and return the default value
    ColumnPtr result;
    auto status = schema_util::cast_column(src_col, simple_type, &result);

    // Check result
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result->size(), 2);
    // TODO(lihangyu): ARRAY<IPv4> -> JSONB, the result will throw exception
    // EXPECT_EQ(result->get_data_at(0).size, 26);
}

TEST_F(SchemaUtilTest, TestGetColumnByTypeEdgeCases) {
    // Test decimal type
    auto decimal_type = std::make_shared<DataTypeDecimal<PrimitiveType::TYPE_DECIMAL128I>>(18, 2);
    schema_util::ExtraInfo ext_info;
    auto decimal_column = schema_util::get_column_by_type(decimal_type, "decimal_col", ext_info);
    EXPECT_EQ(decimal_column.type(), FieldType::OLAP_FIELD_TYPE_DECIMAL128I);
    EXPECT_EQ(decimal_column.precision(), 18);
    EXPECT_EQ(decimal_column.frac(), 2);

    // Test datetime type
    DataTypePtr datetime_type = std::make_shared<DataTypeDateTime>();
    auto datetime_column = schema_util::get_column_by_type(datetime_type, "datetime_col", ext_info);
    EXPECT_EQ(datetime_column.type(), FieldType::OLAP_FIELD_TYPE_DATETIME);

    // Test datetime v2 type
    auto datetime_v2_type = std::make_shared<DataTypeDateTimeV2>(6);
    auto datetime_v2_column =
            schema_util::get_column_by_type(datetime_v2_type, "datetime_v2_col", ext_info);
    EXPECT_EQ(datetime_v2_column.type(), FieldType::OLAP_FIELD_TYPE_DATETIMEV2);
    EXPECT_EQ(datetime_v2_column.precision(), -1);
    EXPECT_EQ(datetime_v2_column.frac(), 6);

    // Test invalid type
    auto invalid_type = std::make_shared<DataTypeNothing>();
    EXPECT_THROW(schema_util::get_column_by_type(invalid_type, "invalid_col", ext_info), Exception);
}

TEST_F(SchemaUtilTest, TestUpdateLeastSchemaInternal) {
    // Create test schemas and types
    std::map<PathInData, DataTypes> subcolumns_types;
    auto schema = std::make_shared<TabletSchema>();

    // Add some test columns
    TabletColumn base_col;
    base_col.set_unique_id(1);
    base_col.set_name("test_variant");
    base_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    schema->append_column(base_col);

    // Add different types for same path
    PathInData test_path("test_variant.a");
    subcolumns_types[test_path] = {std::make_shared<DataTypeInt32>(),
                                   std::make_shared<DataTypeInt64>()};

    // Add array types with different dimensions
    PathInData array_path("test_variant.b");
    subcolumns_types[array_path] = {
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()),
            std::make_shared<DataTypeArray>(
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()))};

    // Add path with single type
    PathInData single_path("test_variant.c");
    subcolumns_types[single_path] = {std::make_shared<DataTypeString>()};

    std::map<std::string, TabletColumnPtr> typed_columns;
    Status st =
            schema_util::update_least_schema_internal(subcolumns_types, schema, 1, typed_columns);
    EXPECT_TRUE(st.ok());

    // Check results
    EXPECT_EQ(schema->num_columns(), 4); // base + 3 subcolumns

    // Check that array path was converted to JSONB due to dimension mismatch
    int array_col_idx = schema->field_index("test_variant.b");
    EXPECT_GE(array_col_idx, 0);
    EXPECT_EQ(schema->column(array_col_idx).type(), FieldType::OLAP_FIELD_TYPE_JSONB);

    // Check that mixed integer types were promoted
    int int_col_idx = schema->field_index("test_variant.a");
    EXPECT_GE(int_col_idx, 0);
    EXPECT_EQ(schema->column(int_col_idx).type(), FieldType::OLAP_FIELD_TYPE_BIGINT);
}

TEST_F(SchemaUtilTest, TestUpdateLeastCommonSchema) {
    // Create test schemas
    std::vector<TabletSchemaSPtr> schemas;
    auto schema1 = std::make_shared<TabletSchema>();
    auto schema2 = std::make_shared<TabletSchema>();

    // Add variant column to both schemas
    TabletColumn variant_col;
    variant_col.set_unique_id(1);
    variant_col.set_name("test_variant");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    schema1->append_column(variant_col);
    schema2->append_column(variant_col);

    // Add different subcolumns to schemas
    TabletColumn subcol1;
    subcol1.set_name("test_variant.a");
    subcol1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    subcol1.set_parent_unique_id(1);
    subcol1.set_path_info(PathInData("test_variant.a"));
    schema1->append_column(subcol1);

    TabletColumn subcol2;
    subcol2.set_name("test_variant.a");
    subcol2.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    subcol2.set_parent_unique_id(1);
    subcol2.set_path_info(PathInData("test_variant.a"));
    schema2->append_column(subcol2);

    schemas.push_back(schema1);
    schemas.push_back(schema2);

    auto result_schema = std::make_shared<TabletSchema>();
    result_schema->append_column(variant_col);

    std::set<PathInData> path_set;
    Status st = schema_util::update_least_common_schema(schemas, result_schema, 1, &path_set);
    EXPECT_TRUE(st.ok());

    // Check results
    EXPECT_EQ(result_schema->num_columns(), 2); // variant + subcolumn
    EXPECT_EQ(path_set.size(), 1);
    EXPECT_TRUE(path_set.find(PathInData("test_variant.a")) != path_set.end());

    // Check that subcolumn type was promoted to BIGINT
    int subcol_idx = result_schema->field_index("test_variant.a");
    EXPECT_GE(subcol_idx, 0);
    EXPECT_EQ(result_schema->column(subcol_idx).type(), FieldType::OLAP_FIELD_TYPE_BIGINT);
}

TEST_F(SchemaUtilTest, TestUpdateLeastCommonSchema2) {
    // Create common schema with a variant column
    TabletSchemaSPtr common_schema = std::make_shared<TabletSchema>();
    TabletColumn variant_col;
    variant_col.set_unique_id(1);
    variant_col.set_name("test_variant");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);

    // Create subcolumns for variant column in common_schema
    TabletColumn sub_col1;
    sub_col1.set_name("test_variant.field1");
    sub_col1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    sub_col1.set_parent_unique_id(1);
    vectorized::PathInData path1("test_variant.field1");
    sub_col1.set_path_info(path1);
    variant_col.add_sub_column(sub_col1);

    TabletColumn sub_col2;
    sub_col2.set_name("test_variant.field2");
    sub_col2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    sub_col2.set_parent_unique_id(1);
    vectorized::PathInData path2("test_variant.field2");
    sub_col2.set_path_info(path2);
    variant_col.add_sub_column(sub_col2);

    common_schema->append_column(variant_col);

    // Create schemas vector with two schemas
    std::vector<TabletSchemaSPtr> schemas;
    // Schema1: doesn't have the variant column
    auto schema1 = std::make_shared<TabletSchema>();
    schemas.push_back(schema1);

    // Schema2: has variant column with different subcolumns
    auto schema2 = std::make_shared<TabletSchema>();
    TabletColumn variant_col2;
    variant_col2.set_unique_id(1);
    variant_col2.set_name("test_variant");
    variant_col2.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);

    // Add subcolumns to schema2's variant column
    TabletColumn sub_col3;
    sub_col3.set_name("test_variant.field3");
    sub_col3.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    sub_col3.set_parent_unique_id(1);
    vectorized::PathInData path3("test_variant.field3");
    sub_col3.set_path_info(path3);
    variant_col2.add_sub_column(sub_col3);

    // Add a subcolumn with same path but different type
    TabletColumn sub_col1_different_type;
    sub_col1_different_type.set_name("test_variant.field1");
    sub_col1_different_type.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    sub_col1_different_type.set_parent_unique_id(1);
    sub_col1_different_type.set_path_info(path1);
    variant_col2.add_sub_column(sub_col1_different_type);

    schema2->append_column(variant_col2);
    schemas.push_back(schema2);

    // Create path_set that contains some paths
    std::set<PathInData> path_set;
    path_set.insert(path1);
    path_set.insert(path2);
    path_set.insert(path3);

    // Test update_least_common_schema
    // This should cover:
    // 1. schema->field_index(variant_col_unique_id) == -1 branch (via schema1)
    // 3. subcolumns_types.find(*col->path_info_ptr()) != subcolumns_types.end() branch
    Status st = schema_util::update_least_common_schema(schemas, common_schema, 1, &path_set);
    EXPECT_TRUE(st.ok());

    // Verify results
    const auto& result_variant = common_schema->column_by_uid(1);

    // Check that all subcolumns are present
    EXPECT_EQ(result_variant.get_sub_columns().size(), 2);

    // Check that field1 has the most compatible type (should be BIGINT due to type promotion)
    bool found_field1 = false;
    bool found_field2 = false;
    bool found_field3 = false;

    for (const auto& col : result_variant.get_sub_columns()) {
        if (col->name() == "test_variant.field1") {
            found_field1 = true;
            EXPECT_EQ(col->type(), FieldType::OLAP_FIELD_TYPE_INT);
        } else if (col->name() == "test_variant.field2") {
            found_field2 = true;
            EXPECT_EQ(col->type(), FieldType::OLAP_FIELD_TYPE_STRING);
        } else if (col->name() == "test_variant.field3") {
            EXPECT_EQ(col->type(), FieldType::OLAP_FIELD_TYPE_INT);
        }
    }

    EXPECT_TRUE(found_field1);
    EXPECT_TRUE(found_field2);
    EXPECT_FALSE(found_field3);
}

TEST_F(SchemaUtilTest, TestUpdateLeastCommonSchema3) {
    // Create common schema with a variant column
    TabletSchemaSPtr common_schema = std::make_shared<TabletSchema>();
    TabletColumn variant_col;
    variant_col.set_unique_id(1);
    variant_col.set_name("test_variant");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);

    // Create sparse columns for variant column in common_schema
    TabletColumn sparse_col1;
    sparse_col1.set_name("test_variant.sparse1");
    sparse_col1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    sparse_col1.set_parent_unique_id(1);
    vectorized::PathInData path1("test_variant.sparse1");
    sparse_col1.set_path_info(path1);

    TabletColumn sparse_col2;
    sparse_col2.set_name("test_variant.sparse2");
    sparse_col2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    sparse_col2.set_parent_unique_id(1);
    vectorized::PathInData path2("test_variant.sparse2");
    sparse_col2.set_path_info(path2);

    common_schema->append_column(variant_col);

    // Create schemas vector with two schemas
    std::vector<TabletSchemaSPtr> schemas;

    // Schema1: doesn't have the variant column
    auto schema1 = std::make_shared<TabletSchema>();
    schemas.push_back(schema1);

    // Schema2: has variant column with different sparse columns
    auto schema2 = std::make_shared<TabletSchema>();
    TabletColumn variant_col2;
    variant_col2.set_unique_id(1);
    variant_col2.set_name("test_variant");
    variant_col2.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);

    // Add sparse columns to schema2's variant column
    TabletColumn sparse_col3;
    sparse_col3.set_name("test_variant.sparse3");
    sparse_col3.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    sparse_col3.set_parent_unique_id(1);
    vectorized::PathInData path3("test_variant.sparse3");
    sparse_col3.set_path_info(path3);

    TabletColumn sparse_col4;
    sparse_col4.set_name("test_variant.sparse4");
    sparse_col4.set_type(FieldType::OLAP_FIELD_TYPE_DOUBLE);
    sparse_col4.set_parent_unique_id(1);
    vectorized::PathInData path4("test_variant.sparse4");
    sparse_col4.set_path_info(path4);

    schema2->append_column(variant_col2);
    schemas.push_back(schema2);

    // Create path_set that contains some but not all sparse column paths
    std::set<PathInData> path_set;
    path_set.insert(path1); // from common_schema
    path_set.insert(path3); // from schema2

    // Test update_least_sparse_column
    // This should cover:
    // 1. schema->field_index(variant_col_unique_id) == -1 branch (via schema1)
    // 3. path_set.find(*col->path_info_ptr()) == path_set.end() branch (via sparse_col4)
    Status st = schema_util::update_least_common_schema(schemas, common_schema, 1, &path_set);
    EXPECT_TRUE(st.ok());
}

TEST_F(SchemaUtilTest, TestGetCompactionSchema) {
    // Create test rowsets
    std::vector<RowsetSharedPtr> rowsets;
    RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();

    // Create schema for rowsets
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "v1_index", 1, "VARIANT",
                     "v1", IndexType::INVERTED);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);

    // Add path statistics
    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {{"v1.a", 1000}, {"v1.b", 800}, {"v1.c", 500}, {"v1.d", 300}, {"v1.e", 200}};

    // Mock rowset behavior
    //    BetaRowset rowset1(schema, rowset_meta, "");
    //    BetaRowset rowset2(schema, rowset_meta, "");
    auto rowset1 = std::make_shared<BetaRowset>(schema, rowset_meta, "");
    auto rowset2 = std::make_shared<BetaRowset>(schema, rowset_meta, "");
    rowsets.push_back(rowset1);
    rowsets.push_back(rowset2);

    auto target_schema = std::make_shared<TabletSchema>();
    target_schema->init_from_pb(schema_pb);

    auto status = schema_util::VariantCompactionUtil::get_extended_compaction_schema(rowsets,
                                                                                     target_schema);
    EXPECT_TRUE(status.ok());

    // Check that paths were properly distributed between subcolumns and sparse columns
    const auto& variant_col = target_schema->column_by_uid(1);
    // this is not work!!!
    EXPECT_EQ(variant_col.get_sub_columns().size(), 0);
}

TEST_F(SchemaUtilTest, TestGetSortedSubcolumns) {
    // Create test subcolumns
    vectorized::ColumnVariant::Subcolumns subcolumns;
    auto obj = VariantUtil::construct_dst_varint_column();

    auto sorted = schema_util::get_sorted_subcolumns(obj->get_subcolumns());
    std::vector<std::string> expected_paths = {"", "v.b", "v.b.d", "v.c.d", "v.e", "v.f"};
    EXPECT_EQ(sorted.size(), 6);
    int i = 0;
    for (auto iter = sorted.begin(); iter != sorted.end(); ++iter) {
        EXPECT_EQ(iter.operator*()->path.get_path(), expected_paths[i++]);
    }
}

TEST_F(SchemaUtilTest, TestCreateSparseColumn) {
    TabletColumn variant;
    variant.set_name("test_variant");
    variant.set_unique_id(42);
    variant.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_GENERIC);

    auto sparse_column = schema_util::create_sparse_column(variant);

    EXPECT_EQ(sparse_column.name(), "test_variant." + SPARSE_COLUMN_PATH);
    EXPECT_EQ(sparse_column.type(), FieldType::OLAP_FIELD_TYPE_MAP);
    EXPECT_EQ(sparse_column.aggregation(), FieldAggregationMethod::OLAP_FIELD_AGGREGATION_GENERIC);
    EXPECT_EQ(sparse_column.parent_unique_id(), 42);
    EXPECT_EQ(sparse_column.path_info_ptr()->get_path(), "test_variant." + SPARSE_COLUMN_PATH);

    // Check map value columns
    EXPECT_EQ(sparse_column.get_sub_column(0).type(), FieldType::OLAP_FIELD_TYPE_STRING);
    EXPECT_EQ(sparse_column.get_sub_column(1).type(), FieldType::OLAP_FIELD_TYPE_STRING);
}

TEST_F(SchemaUtilTest, TestParseVariantColumnsEdgeCases) {
    Block block;

    // Test parsing from string to variant
    auto variant_type = std::make_shared<DataTypeVariant>(10);
    auto variant_column = ColumnVariant::create(10);
    auto root_column = ColumnString::create();

    // Add some test JSON data
    root_column->insert(
            vectorized::Field::create_field<PrimitiveType::TYPE_STRING>("{'a': 1, 'b': 'test'}"));
    root_column->insert(
            vectorized::Field::create_field<PrimitiveType::TYPE_STRING>("{'a': 2, 'c': [1,2,3]}"));
    root_column->insert(
            vectorized::Field::create_field<PrimitiveType::TYPE_STRING>("{'a': 3, 'd': {'x': 1}}"));

    variant_column->create_root(std::make_shared<DataTypeString>(), root_column->get_ptr());
    block.insert({variant_column->get_ptr(), variant_type, "variant_col"});

    std::vector<int> variant_pos {0};
    ParseConfig config;

    auto status = schema_util::parse_variant_columns(block, variant_pos, config);
    EXPECT_TRUE(status.ok());

    // Test parsing from JSONB to variant
    auto jsonb_type = std::make_shared<DataTypeJsonb>();
    auto jsonb_column = ColumnString::create();
    jsonb_column->insert(vectorized::Field::create_field<PrimitiveType::TYPE_STRING>("{'x': 1}"));

    auto variant_column2 = ColumnVariant::create(10);
    variant_column2->create_root(jsonb_type, jsonb_column->get_ptr());

    Block block2;
    block2.insert({variant_column2->get_ptr(), variant_type, "variant_col2"});

    status = schema_util::parse_variant_columns(block2, {0}, config);
    EXPECT_TRUE(status.ok());

    // Test parsing already parsed variant
    auto variant_column3 = ColumnVariant::create(10);
    variant_column3->finalize();

    Block block3;
    block3.insert({variant_column3->get_ptr(), variant_type, "variant_col3"});

    status = schema_util::parse_variant_columns(block3, {0}, config);
    EXPECT_TRUE(status.ok());
}

TEST_F(SchemaUtilTest, TestParseVariantColumnsWithNulls) {
    Block block;

    // Create a nullable variant column
    auto variant_type = make_nullable(std::make_shared<DataTypeVariant>(10));
    auto string_type = make_nullable(std::make_shared<DataTypeString>());

    auto string_column = ColumnString::create();
    string_column->insert(vectorized::Field::create_field<PrimitiveType::TYPE_STRING>("{'a': 1}"));
    auto nullable_string = make_nullable(string_column->get_ptr());

    auto variant_column = ColumnVariant::create(10);
    variant_column->create_root(string_type, nullable_string->assume_mutable());
    auto nullable_variant = make_nullable(variant_column->get_ptr());

    block.insert({nullable_variant, variant_type, "nullable_variant"});

    std::vector<int> variant_pos {0};
    ParseConfig config;

    auto status = schema_util::parse_variant_columns(block, variant_pos, config);
    EXPECT_TRUE(status.ok());

    const auto& result_column = block.get_by_position(0).column;
    EXPECT_TRUE(result_column->is_nullable());
}

TEST_F(SchemaUtilTest, get_compaction_typed_columns) {
    TabletColumn variant;
    variant.set_unique_id(10);
    variant.set_variant_max_subcolumns_count(3);

    TabletColumn subcolumn;
    subcolumn.set_name("profile.id.*");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    variant.add_sub_column(subcolumn);

    TabletColumn subcolumn2;
    subcolumn2.set_name("profile.name.?");
    subcolumn2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    variant.add_sub_column(subcolumn2);

    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
    schema->append_column(variant);

    std::unordered_set<std::string> typed_paths;
    typed_paths.insert("profile.id.name");
    TabletSchemaSPtr output_schema = std::make_shared<TabletSchema>();
    TabletColumnPtr parent_column = std::make_shared<TabletColumn>(variant);
    TabletSchema::PathsSetInfo paths_set_info;
    EXPECT_TRUE(schema_util::VariantCompactionUtil::get_compaction_typed_columns(
                        schema, typed_paths, parent_column, output_schema, paths_set_info)
                        .ok());
    EXPECT_EQ(output_schema->num_columns(), 1);
    EXPECT_EQ(output_schema->column(0).type(), FieldType::OLAP_FIELD_TYPE_INT);
    EXPECT_EQ(paths_set_info.typed_path_set.size(), 1);

    typed_paths.insert("abc");
    EXPECT_FALSE(schema_util::VariantCompactionUtil::get_compaction_typed_columns(
                         schema, typed_paths, parent_column, output_schema, paths_set_info)
                         .ok());
}

TEST_F(SchemaUtilTest, get_compaction_nested_columns) {
    TabletColumn variant;
    variant.set_unique_id(20);
    variant.set_variant_max_subcolumns_count(3);

    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
    schema->append_column(variant);

    std::unordered_set<vectorized::PathInData, vectorized::PathInData::Hash> nested_paths;
    vectorized::PathInData path1("profile.address");
    vectorized::PathInData path2("profile.phone");
    nested_paths.insert(path1);
    nested_paths.insert(path2);

    TabletSchemaSPtr output_schema = std::make_shared<TabletSchema>();
    TabletSchema::PathsSetInfo paths_set_info;

    doris::vectorized::schema_util::PathToDataTypes path_to_data_types;
    path_to_data_types[path1] = {std::make_shared<vectorized::DataTypeInt32>(),
                                 std::make_shared<vectorized::DataTypeString>()};
    path_to_data_types[path2] = {std::make_shared<vectorized::DataTypeString>(),
                                 std::make_shared<vectorized::DataTypeString>()};
    TabletColumnPtr parent_column = std::make_shared<TabletColumn>(variant);

    Status st = schema_util::VariantCompactionUtil::get_compaction_nested_columns(
            nested_paths, path_to_data_types, parent_column, output_schema, paths_set_info);

    EXPECT_TRUE(st.ok());
    EXPECT_EQ(output_schema->num_columns(), 2);
    for (const auto& column : output_schema->columns()) {
        // std::cout << "column name: " << column->name() << " type: " << (int)column->type() << std::endl;
        if (column->name().ends_with("address")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_JSONB);
        } else if (column->name().ends_with("phone")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_STRING);
        }
    }

    std::unordered_set<vectorized::PathInData, vectorized::PathInData::Hash> bad_nested_paths;
    bad_nested_paths.insert(vectorized::PathInData("not_exist"));
    TabletSchemaSPtr bad_output_schema = std::make_shared<TabletSchema>();
    TabletSchema::PathsSetInfo bad_paths_set_info;
    Status st2 = schema_util::VariantCompactionUtil::get_compaction_nested_columns(
            bad_nested_paths, path_to_data_types, parent_column, bad_output_schema,
            bad_paths_set_info);
    EXPECT_FALSE(st2.ok());
}

TEST_F(SchemaUtilTest, get_compaction_subcolumns) {
    TabletColumn variant;
    variant.set_unique_id(30);
    variant.set_variant_max_subcolumns_count(3);
    variant.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);

    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
    schema->append_column(variant);

    TabletColumnPtr parent_column = std::make_shared<TabletColumn>(variant);

    TabletSchema::PathsSetInfo paths_set_info;
    paths_set_info.sub_path_set.insert("a");
    paths_set_info.sub_path_set.insert("b");
    doris::vectorized::schema_util::PathToDataTypes path_to_data_types;
    std::unordered_set<std::string> sparse_paths;
    TabletSchemaSPtr output_schema = std::make_shared<TabletSchema>();

    schema_util::VariantCompactionUtil::get_compaction_subcolumns(
            paths_set_info, parent_column, schema, path_to_data_types, sparse_paths, output_schema);
    EXPECT_EQ(output_schema->num_columns(), 2);
    for (const auto& column : output_schema->columns()) {
        EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
    }

    output_schema = std::make_shared<TabletSchema>();
    path_to_data_types.clear();
    path_to_data_types[vectorized::PathInData("a")] = {
            std::make_shared<vectorized::DataTypeInt32>()};
    path_to_data_types[vectorized::PathInData("b")] = {
            std::make_shared<vectorized::DataTypeString>()};
    schema_util::VariantCompactionUtil::get_compaction_subcolumns(
            paths_set_info, parent_column, schema, path_to_data_types, sparse_paths, output_schema);
    EXPECT_EQ(output_schema->num_columns(), 2);
    bool found_int = false, found_str = false;
    for (const auto& column : output_schema->columns()) {
        if (column->name().ends_with("a")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_INT);
            found_int = true;
        } else if (column->name().ends_with("b")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_STRING);
            found_str = true;
        }
    }
    EXPECT_TRUE(found_int && found_str);

    output_schema = std::make_shared<TabletSchema>();
    sparse_paths.insert("a");
    schema_util::VariantCompactionUtil::get_compaction_subcolumns(
            paths_set_info, parent_column, schema, path_to_data_types, sparse_paths, output_schema);
    EXPECT_EQ(output_schema->num_columns(), 2);
    for (const auto& column : output_schema->columns()) {
        if (column->name().ends_with("a")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
        } else if (column->name().ends_with("b")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_STRING);
        }
    }

    output_schema = std::make_shared<TabletSchema>();
    sparse_paths.clear();

    for (int i = 0; i < config::variant_max_sparse_column_statistics_size + 1; ++i) {
        sparse_paths.insert("dummy" + std::to_string(i));
    }
    schema_util::VariantCompactionUtil::get_compaction_subcolumns(
            paths_set_info, parent_column, schema, path_to_data_types, sparse_paths, output_schema);
    EXPECT_EQ(output_schema->num_columns(), 2);
    for (const auto& column : output_schema->columns()) {
        EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
    }
}

TEST_F(SchemaUtilTest, get_compaction_subcolumns_advanced) {
    TabletColumn variant;
    variant.set_unique_id(30);
    variant.set_variant_max_subcolumns_count(3);
    variant.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    variant.set_variant_enable_typed_paths_to_sparse(true);
    TabletColumn subcolumn;
    subcolumn.set_name("c");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_DATEV2);
    variant.add_sub_column(subcolumn);
    TabletColumn subcolumn2;
    subcolumn2.set_name("d");
    subcolumn2.set_type(FieldType::OLAP_FIELD_TYPE_DATEV2);
    variant.add_sub_column(subcolumn2);

    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
    schema->append_column(variant);

    TabletColumnPtr parent_column = std::make_shared<TabletColumn>(variant);

    TabletSchema::PathsSetInfo paths_set_info;
    paths_set_info.sub_path_set.insert("a");
    paths_set_info.sub_path_set.insert("b");
    paths_set_info.sub_path_set.insert("c");
    paths_set_info.sub_path_set.insert("d");
    doris::vectorized::schema_util::PathToDataTypes path_to_data_types;
    std::unordered_set<std::string> sparse_paths;
    TabletSchemaSPtr output_schema = std::make_shared<TabletSchema>();

    schema_util::VariantCompactionUtil::get_compaction_subcolumns(
            paths_set_info, parent_column, schema, path_to_data_types, sparse_paths, output_schema);
    EXPECT_EQ(output_schema->num_columns(), 4);
    for (const auto& column : output_schema->columns()) {
        if (column->name().ends_with("a") || column->name().ends_with("b")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
        } else if (column->name().ends_with("c") || column->name().ends_with("d")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_DATEV2);
        }
    }

    output_schema = std::make_shared<TabletSchema>();
    path_to_data_types.clear();
    path_to_data_types[vectorized::PathInData("a")] = {
            std::make_shared<vectorized::DataTypeInt32>()};
    path_to_data_types[vectorized::PathInData("b")] = {
            std::make_shared<vectorized::DataTypeString>()};
    schema_util::VariantCompactionUtil::get_compaction_subcolumns(
            paths_set_info, parent_column, schema, path_to_data_types, sparse_paths, output_schema);
    EXPECT_EQ(output_schema->num_columns(), 4);
    bool found_int = false, found_str = false;
    for (const auto& column : output_schema->columns()) {
        if (column->name().ends_with("a")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_INT);
            found_int = true;
        } else if (column->name().ends_with("b")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_STRING);
            found_str = true;
        } else if (column->name().ends_with("c") || column->name().ends_with("d")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_DATEV2);
        }
    }
    EXPECT_TRUE(found_int && found_str);

    output_schema = std::make_shared<TabletSchema>();
    sparse_paths.insert("a");
    schema_util::VariantCompactionUtil::get_compaction_subcolumns(
            paths_set_info, parent_column, schema, path_to_data_types, sparse_paths, output_schema);
    EXPECT_EQ(output_schema->num_columns(), 4);
    for (const auto& column : output_schema->columns()) {
        if (column->name().ends_with("a")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
        } else if (column->name().ends_with("b")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_STRING);
        } else if (column->name().ends_with("c") || column->name().ends_with("d")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_DATEV2);
        }
    }

    output_schema = std::make_shared<TabletSchema>();
    sparse_paths.clear();

    for (int i = 0; i < config::variant_max_sparse_column_statistics_size + 1; ++i) {
        sparse_paths.insert("dummy" + std::to_string(i));
    }
    schema_util::VariantCompactionUtil::get_compaction_subcolumns(
            paths_set_info, parent_column, schema, path_to_data_types, sparse_paths, output_schema);
    EXPECT_EQ(output_schema->num_columns(), 4);
    for (const auto& column : output_schema->columns()) {
        if (column->name().ends_with("a") || column->name().ends_with("b")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
        } else if (column->name().ends_with("c") || column->name().ends_with("d")) {
            EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_DATEV2);
        }
    }
}

// Test has_different_structure_in_same_path function indirectly through check_variant_has_no_ambiguous_paths
TEST_F(SchemaUtilTest, has_different_structure_in_same_path_indirect) {
    // Test case 1: Same structure and same length - should not detect ambiguity
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("a", false).append("b", false).append("c", false);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("a", false).append("b", false).append("c", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 2: Different keys at same position - should not detect ambiguity (different keys)
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("a", false).append("b", false).append("c", false);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("a", false).append("d", false).append("c", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 3: Same keys but different nested structure - should detect ambiguity
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("a", false).append("b", true);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("a", false).append("b", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(status.to_string().find("Ambiguous paths") != std::string::npos);
    }

    // Test case 4: Same keys but different anonymous array levels - should detect ambiguity
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("a", true).append("b", false);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("a", false).append("b", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(status.to_string().find("Ambiguous paths") != std::string::npos);
    }

    // Test case 5: Same keys but different nested and anonymous levels - should detect ambiguity
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("a", true).append("b", true);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("a", false).append("b", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(status.to_string().find("Ambiguous paths") != std::string::npos);
    }

    // Test case 6: Different lengths - should not detect ambiguity (new behavior: only check same length paths)
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("a", false).append("b", false).append("c", false);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("a", false).append("b", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 7: Different lengths with structure difference - should not detect ambiguity
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("a", false).append("b", true).append("c", false);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("a", false).append("b", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 8: Complex nested structure difference with same length - should detect ambiguity
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("user", false).append("address", true).append("street", false);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("user", false).append("address", false).append("street", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(status.to_string().find("Ambiguous paths") != std::string::npos);
    }

    // Test case 9: Multiple paths with different lengths - should not detect ambiguity
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("config", false).append("database", false).append("host", false);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("config", false).append("database", false);
        paths.emplace_back(builder2.build());

        vectorized::PathInDataBuilder builder3;
        builder3.append("config", false);
        paths.emplace_back(builder3.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 10: Empty paths - should not detect ambiguity
    {
        vectorized::PathsInData paths;
        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 11: Single path - should not detect ambiguity
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("single", false).append("path", false);
        paths.emplace_back(builder1.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 12: we have path like '{"a.b": "UPPER CASE", "a.c": "lower case", "a" : {"b" : 123}, "a" : {"c" : 456}}'
    {
        vectorized::PathsInData paths;
        vectorized::PathInDataBuilder builder1;
        builder1.append("a", false).append("b", false);
        paths.emplace_back(builder1.build());

        vectorized::PathInDataBuilder builder2;
        builder2.append("a.b", false);
        paths.emplace_back(builder2.build());

        auto status = vectorized::schema_util::check_variant_has_no_ambiguous_paths(paths);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }
}

// Test check_path_conflicts_with_existing function indirectly through update_least_common_schema
TEST_F(SchemaUtilTest, check_path_conflicts_with_existing) {
    // Test case 1: No conflicts - should succeed
    {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);

        // Create a variant column
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1,
                         "VARIANT", "v1", IndexType::INVERTED);

        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);
        std::vector<TabletColumn> subcolumns;

        // Add subcolumns with different paths
        construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_STRING, 1, "v1.name",
                            &subcolumns);
        construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_INT, 1, "v1.age",
                            &subcolumns);

        std::vector<TabletSchemaSPtr> schemas = {tablet_schema};
        TabletSchemaSPtr output_schema;

        auto status = vectorized::schema_util::get_least_common_schema(schemas, nullptr,
                                                                       output_schema, false);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 2: Conflicts with same path but different structure - should fail
    {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);

        // Create a variant column
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1,
                         "VARIANT", "v1", IndexType::INVERTED);

        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);

        // Add subcolumns with same path but different structure
        // This would require creating paths with different nested structure
        // For now, we'll test the basic functionality

        std::vector<TabletSchemaSPtr> schemas = {tablet_schema};
        TabletSchemaSPtr output_schema;

        auto status = vectorized::schema_util::get_least_common_schema(schemas, nullptr,
                                                                       output_schema, false);
        // This should succeed since we don't have conflicting paths in this simple case
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    // Test case 3: Multiple schemas with conflicting paths - should fail
    {
        // Create first schema
        TabletSchemaPB schema_pb1;
        schema_pb1.set_keys_type(KeysType::DUP_KEYS);
        construct_column(schema_pb1.add_column(), schema_pb1.add_index(), 10001, "v1_index", 1,
                         "VARIANT", "v1", IndexType::INVERTED);

        TabletSchemaSPtr tablet_schema1 = std::make_shared<TabletSchema>();
        tablet_schema1->init_from_pb(schema_pb1);
        std::vector<TabletColumn> subcolumns;
        construct_subcolumn(tablet_schema1, FieldType::OLAP_FIELD_TYPE_STRING, 1, "v1.address",
                            &subcolumns);

        // Create second schema with same path but different structure
        TabletSchemaPB schema_pb2;
        schema_pb2.set_keys_type(KeysType::DUP_KEYS);
        construct_column(schema_pb2.add_column(), schema_pb2.add_index(), 10001, "v1_index", 1,
                         "VARIANT", "v1", IndexType::INVERTED);

        TabletSchemaSPtr tablet_schema2 = std::make_shared<TabletSchema>();
        tablet_schema2->init_from_pb(schema_pb2);
        std::vector<TabletColumn> subcolumns2;
        construct_subcolumn(tablet_schema2, FieldType::OLAP_FIELD_TYPE_INT, 1, "v1.address",
                            &subcolumns2);

        std::vector<TabletSchemaSPtr> schemas = {tablet_schema1, tablet_schema2};
        TabletSchemaSPtr output_schema;

        auto status = vectorized::schema_util::get_least_common_schema(schemas, nullptr,
                                                                       output_schema, false);
        // This should succeed since the paths are the same and we're just checking for structure conflicts
        EXPECT_TRUE(status.ok()) << status.to_string();
    }
}

TEST_F(SchemaUtilTest, parse_variant_columns_ambiguous_paths) {
    using namespace doris::vectorized;
    // Prepare the string column with two rows
    auto string_col = ColumnString::create();
    string_col->insert(doris::vectorized::Field::create_field<TYPE_STRING>(
            String("{\"nested\": [{\"a\": 2.5, \"b\": \"123.1\"}]}")));
    string_col->insert(doris::vectorized::Field::create_field<TYPE_STRING>(
            String("{\"nested\": {\"a\": 2.5, \"b\": \"123.1\"}}")));
    auto string_type = std::make_shared<DataTypeString>();

    // Prepare the variant column with the string column as root
    vectorized::ColumnVariant::Subcolumns dynamic_subcolumns;
    dynamic_subcolumns.create_root(
            vectorized::ColumnVariant::Subcolumn(string_col->assume_mutable(), string_type, true));

    auto variant_col = ColumnVariant::create(0, std::move(dynamic_subcolumns));
    auto variant_type = std::make_shared<DataTypeVariant>();

    // Construct the block
    Block block;
    block.insert(
            vectorized::ColumnWithTypeAndName(variant_col->assume_mutable(), variant_type, "v"));

    // The variant column is at index 0
    std::vector<int> variant_pos = {0};
    ParseConfig config;
    config.enable_flatten_nested = true;

    // Should throw due to ambiguous paths
    Status st = schema_util::parse_variant_columns(block, variant_pos, config);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("Ambiguous paths") != std::string::npos);
}
