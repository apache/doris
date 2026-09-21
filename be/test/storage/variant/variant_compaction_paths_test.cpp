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

#include "storage/segment/variant/variant_compaction_paths.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "storage/tablet/tablet_schema.h"
#include "util/json/path_in_data.h"

namespace doris {

namespace {

constexpr int32_t kVariantUid = 9001;

TabletIndexPtr make_inverted_index(int64_t index_id, const std::string& name) {
    auto index = std::make_shared<TabletIndex>();
    TabletIndexPB index_pb;
    index_pb.set_index_id(index_id);
    index_pb.set_index_name(name);
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kVariantUid);
    index->init_from_pb(index_pb);
    return index;
}

// An extracted column of the variant above. `typed` selects which half of the layout the lookup
// is expected to consult.
TabletColumn make_extracted_column(const std::string& path, bool typed,
                                   FieldType type = FieldType::OLAP_FIELD_TYPE_STRING,
                                   int32_t parent_uid = kVariantUid) {
    TabletColumn column;
    column.set_unique_id(-1);
    column.set_name("v." + path);
    column.set_type(type);
    column.set_parent_unique_id(parent_uid);
    column.set_path_info(PathInData("v." + path, typed));
    return column;
}

// typed_path_set["user.name"] -> index 1001, subcolumn_indexes["product.id"] -> index 2001.
VariantCompactionPathsMap make_layout() {
    VariantCompactionPaths paths;

    TabletSchema::SubColumnInfo typed;
    typed.indexes.push_back(make_inverted_index(1001, "typed_path_idx"));
    paths.typed_path_set["user.name"] = std::move(typed);

    paths.subcolumn_indexes["product.id"] = {make_inverted_index(2001, "subcolumn_idx")};

    VariantCompactionPathsMap layout;
    layout[kVariantUid] = std::move(paths);
    return layout;
}

} // namespace

TEST(VariantCompactionPathsTest, TypedPathTakesItsOwnIndexes) {
    const auto layout = make_layout();
    auto indexes = variant_subcolumn_indexes(&layout, make_extracted_column("user.name", true));
    ASSERT_EQ(1, indexes.size());
    EXPECT_EQ(1001, indexes[0]->index_id());
    EXPECT_EQ("typed_path_idx", indexes[0]->index_name());
}

TEST(VariantCompactionPathsTest, SubcolumnPathTakesItsOwnIndexes) {
    const auto layout = make_layout();
    auto indexes = variant_subcolumn_indexes(&layout, make_extracted_column("product.id", false));
    ASSERT_EQ(1, indexes.size());
    EXPECT_EQ(2001, indexes[0]->index_id());
    EXPECT_EQ("subcolumn_idx", indexes[0]->index_name());
}

// A typed path is looked up only in typed_path_set, and a plain one only in subcolumn_indexes,
// so asking for either under the wrong flavour finds nothing.
TEST(VariantCompactionPathsTest, PathIsNotFoundInTheOtherHalfOfTheLayout) {
    const auto layout = make_layout();
    EXPECT_TRUE(
            variant_subcolumn_indexes(&layout, make_extracted_column("user.name", false)).empty());
    EXPECT_TRUE(
            variant_subcolumn_indexes(&layout, make_extracted_column("product.id", true)).empty());
}

TEST(VariantCompactionPathsTest, PathAbsentFromTheLayout) {
    const auto layout = make_layout();
    EXPECT_TRUE(variant_subcolumn_indexes(&layout, make_extracted_column("non.existing", false))
                        .empty());
}

TEST(VariantCompactionPathsTest, ParentColumnAbsentFromTheLayout) {
    const auto layout = make_layout();
    auto column = make_extracted_column("user.name", true, FieldType::OLAP_FIELD_TYPE_STRING,
                                        /*parent_uid=*/9999);
    EXPECT_TRUE(variant_subcolumn_indexes(&layout, column).empty());
}

// Every write that is not a compaction passes no layout at all.
TEST(VariantCompactionPathsTest, NoLayout) {
    EXPECT_TRUE(
            variant_subcolumn_indexes(nullptr, make_extracted_column("user.name", true)).empty());
}

// A column that is not extracted never has a layout entry, whatever the layout holds.
// A plain column carries its indexes on the schema, never in the layout. STRING so that the
// extracted-column guard is the only thing that can refuse it.
TEST(VariantCompactionPathsTest, NonExtractedColumn) {
    const auto layout = make_layout();
    TabletColumn column;
    column.set_unique_id(kVariantUid);
    column.set_name("user.name");
    column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    ASSERT_FALSE(column.is_extracted_column());
    EXPECT_TRUE(variant_subcolumn_indexes(&layout, column).empty());
}

// Types that cannot carry an inverted index are refused before the layout is consulted, even
// though their path is in it: JSONB and variant are rejected outright, and an array only passes
// when its element type does (IndexColumnWriter::check_support_inverted_index).
TEST(VariantCompactionPathsTest, TypeThatCannotCarryAnInvertedIndex) {
    const auto layout = make_layout();
    for (auto type : {FieldType::OLAP_FIELD_TYPE_JSONB, FieldType::OLAP_FIELD_TYPE_VARIANT}) {
        auto column = make_extracted_column("user.name", true, type);
        EXPECT_TRUE(variant_subcolumn_indexes(&layout, column).empty())
                << "type=" << static_cast<int>(type);
    }

    // A scalar element type does pass, so the path is found.
    auto scalar = make_extracted_column("user.name", true, FieldType::OLAP_FIELD_TYPE_DOUBLE);
    EXPECT_EQ(1, variant_subcolumn_indexes(&layout, scalar).size());
}

} // namespace doris
