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

#include "exec/common/variant_util.h"
#include "storage/variant/index_storage_variant_test_base.h"

namespace doris::index_storage_test {

class IndexStorageVariantCompactionSchemaTest : public IndexStorageTestFixture {
protected:
};

TEST(IndexStorageVariantCompactionUtilTest, GetSubpathsHonorsZeroLimitAndTieOrdering) {
    variant_util::PathToNoneNullValues stats {
            {"alpha", 3},
            {"beta", 3},
            {"gamma", 1},
    };

    TabletSchema::PathsSetInfo unlimited;
    variant_util::VariantCompactionUtil::get_subpaths(0, stats, unlimited);
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("gamma")));
    EXPECT_TRUE(unlimited.sparse_path_set.empty());

    TabletSchema::PathsSetInfo top_one;
    variant_util::VariantCompactionUtil::get_subpaths(1, stats, top_one);
    EXPECT_TRUE(top_one.sub_path_set.contains(StringRef("beta")));
    EXPECT_FALSE(top_one.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(top_one.sparse_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(top_one.sparse_path_set.contains(StringRef("gamma")));
}

TEST(IndexStorageVariantCompactionUtilTest, GetSubpathsKeepsAllPathsAtLimitAndHandlesEmptyStats) {
    variant_util::PathToNoneNullValues exact_limit {
            {"alpha", 2},
            {"beta", 1},
    };

    TabletSchema::PathsSetInfo at_limit;
    variant_util::VariantCompactionUtil::get_subpaths(2, exact_limit, at_limit);
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(at_limit.sparse_path_set.empty());

    TabletSchema::PathsSetInfo empty;
    variant_util::VariantCompactionUtil::get_subpaths(1, {}, empty);
    EXPECT_TRUE(empty.sub_path_set.empty());
    EXPECT_TRUE(empty.sparse_path_set.empty());
}

TEST(IndexStorageVariantCompactionUtilTest, EmptyInputsKeepVariantSchemaWithoutPathStats) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";

    IndexTabletOptions options;
    options.tablet_id = 110054;
    options.variant_columns = {std::move(variant)};
    auto base_schema = build_tablet_schema(options);
    ASSERT_NE(base_schema, nullptr);

    EXPECT_EQ(
            variant_util::VariantCompactionUtil::calculate_variant_extended_schema({}, base_schema),
            nullptr);

    auto compaction_schema = std::make_shared<TabletSchema>(*base_schema);
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {}, compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(compaction_schema->has_column_unique_id(2));

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->typed_path_set.empty());
    EXPECT_TRUE(path_set_info->sub_path_set.empty());
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
}

TEST_F(IndexStorageVariantCompactionSchemaTest,
       VariantMaxSubcolumnsCountZeroMaterializesAllObservedPaths) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 0;

    IndexTabletOptions options;
    options.tablet_id = 110049;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"alpha": "a0", "beta": "b0"})", R"({"alpha": "a1", "gamma": "g1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("gamma")));
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "alpha"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "beta"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "gamma"));
}

TEST_F(IndexStorageVariantCompactionSchemaTest,
       VariantPathCountAtLimitMaterializesAllObservedPaths) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 2;

    IndexTabletOptions options;
    options.tablet_id = 110050;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"alpha": "a0", "beta": "b0"})", R"({"alpha": "a1", "beta": "b1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "alpha"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "beta"));
}

TEST_F(IndexStorageVariantCompactionSchemaTest,
       VariantTypedPathAndDynamicSubpathStaySeparatedInCompactionSchema) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 2;
    variant.predefined_paths = {
            VariantPathSpec {.path = "typed_i",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    IndexTabletOptions options;
    options.tablet_id = 110052;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"typed_i": 1, "hot": "h0"})", R"({"typed_i": 2, "cold": "c1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->typed_path_set.contains("typed_i"));
    EXPECT_FALSE(path_set_info->sub_path_set.contains(StringRef("typed_i")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("hot")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("cold")));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "typed_i"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "hot"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "cold"));
}

} // namespace doris::index_storage_test
