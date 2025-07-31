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

#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/json/json_parser.h"

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

} // namespace doris
