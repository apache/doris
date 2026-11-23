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

#include <bvar/variable.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <algorithm>

#include "cloud/pb_convert.h"
#include "cpp/sync_point.h"
#include "exec/common/variant_util.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/segment/variant/variant_desc_fingerprint.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "util/defer_op.h"
#include "util/json/path_in_data.h"

namespace doris::index_storage_test {

class IndexStorageVariantCompactionSchemaTest : public IndexStorageTestFixture {
protected:
    Status write_desc_fingerprint_rowsets(std::vector<RowsetSharedPtr>& rowsets);
    void expect_desc_reads_after_incomplete_representative(const char* point,
                                                           const Status& failure);
};

static constexpr auto desc_read_counter = "variant_desc_fingerprint_rowsets_read";
static constexpr auto desc_skip_counter = "variant_desc_fingerprint_rowsets_skipped";

static int64_t desc_metric(const char* name) {
    return std::stoll(bvar::Variable::describe_exposed(name));
}

TEST(IndexStorageVariantCompactionUtilTest, DescFingerprintIgnoresFooterColumnOrder) {
    IndexTabletOptions options;
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    options.variant_columns = {std::move(variant)};
    const auto schema = build_tablet_schema(options);

    segment_v2::SegmentFooterPB footer;
    auto* root = footer.add_columns();
    root->set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_VARIANT));
    root->set_unique_id(2);
    auto* amount = footer.add_columns();
    amount->set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_DECIMAL64));
    amount->set_precision(12);
    amount->set_frac(2);
    PathInData("v.amount").to_protobuf(amount->mutable_column_path_info(), 2);
    auto* name = footer.add_columns();
    name->set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));
    PathInData("v.name").to_protobuf(name->mutable_column_path_info(), 2);

    const auto digest = variant_util::build_segment_desc_fingerprint(footer, *schema);
    ASSERT_FALSE(digest.empty());
    footer.mutable_columns()->SwapElements(0, 2);
    amount->set_column_id(47);
    amount->set_num_rows(100);
    EXPECT_EQ(digest, variant_util::build_segment_desc_fingerprint(footer, *schema));

    amount->set_frac(3);
    const auto changed_type = variant_util::build_segment_desc_fingerprint(footer, *schema);
    ASSERT_FALSE(changed_type.empty());
    EXPECT_NE(digest, changed_type);
}

TEST(IndexStorageVariantCompactionUtilTest, DescFingerprintCoversPhysicalSegmentIds) {
    RowsetMeta rowset;
    RowsetId id;
    id.init(120001);
    rowset.set_rowset_id(id);
    rowset.set_segment_ids({7, 90});
    const std::string first(64, 'a');
    const std::string second(64, 'b');
    variant_util::set_rowset_desc_fingerprint(rowset, {{90, second}, {7, first}});
    const auto fingerprint = rowset.variant_desc_fingerprint();
    ASSERT_EQ(4, fingerprint.layout_sha256_words_size());

    variant_util::set_rowset_desc_fingerprint(rowset, {{7, first}, {90, second}});
    EXPECT_EQ(fingerprint.SerializeAsString(),
              rowset.variant_desc_fingerprint().SerializeAsString());

    std::string json;
    ASSERT_TRUE(rowset.json_rowset_meta(&json));
    RowsetMeta restored;
    ASSERT_TRUE(restored.init_from_json(json));
    EXPECT_EQ(fingerprint.SerializeAsString(),
              restored.variant_desc_fingerprint().SerializeAsString());

    variant_util::set_rowset_desc_fingerprint(rowset, {{0, first}, {1, second}});
    EXPECT_EQ(0, rowset.variant_desc_fingerprint().layout_sha256_words_size());
}

Status IndexStorageVariantCompactionSchemaTest::write_desc_fingerprint_rowsets(
        std::vector<RowsetSharedPtr>& rowsets) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 0;
    IndexTabletOptions options;
    options.tablet_id = 110055;
    options.external_segment_meta = true;
    options.variant_columns = {std::move(variant)};
    RETURN_IF_ERROR(create_tablet(options));

    const std::vector<std::vector<std::string>> documents {
            {R"({"same":1,"a":2})", R"({"same":2,"b":"x"})"},
            {R"({"b":"y","same":3})", R"({"a":4,"same":4})"},
            {R"({"same":"changed","extra":5})", R"({"extra":6,"same":"again"})"},
    };
    for (size_t i = 0; i < documents.size(); ++i) {
        IndexRowsetSpec spec;
        spec.version = i;
        for (const auto& document : documents[i]) {
            spec.batches.push_back(IndexBatch::single_variant({document}, 0));
        }
        auto rowset = write_rowset(spec);
        if (!rowset.has_value()) {
            return rowset.error();
        }
        if (rowset.value()->num_segments() != documents[i].size() ||
            rowset.value()->rowset_meta()->variant_desc_fingerprint().layout_sha256_words_size() !=
                    4) {
            return Status::InternalError("Missing segment coverage or fingerprint for rowset {}",
                                         i);
        }
        rowsets.push_back(std::move(rowset.value()));
    }
    return Status::OK();
}

TEST_F(IndexStorageVariantCompactionSchemaTest, DescFingerprintIgnoresJsonAndSegmentOrder) {
    std::vector<RowsetSharedPtr> rowsets;
    const auto status = write_desc_fingerprint_rowsets(rowsets);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& fingerprint = rowsets[0]->rowset_meta()->variant_desc_fingerprint();
    const auto& reordered = rowsets[1]->rowset_meta()->variant_desc_fingerprint();
    const auto& changed = rowsets[2]->rowset_meta()->variant_desc_fingerprint();
    EXPECT_TRUE(
            std::ranges::equal(fingerprint.layout_sha256_words(), reordered.layout_sha256_words()));
    EXPECT_FALSE(std::ranges::equal(fingerprint.segment_set_sha256_words(),
                                    reordered.segment_set_sha256_words()));
    EXPECT_FALSE(
            std::ranges::equal(fingerprint.layout_sha256_words(), changed.layout_sha256_words()));
}

static void expect_desc_deduplication(const std::vector<RowsetSharedPtr>& rowsets,
                                      const TabletSchemaSPtr& schema) {
    const auto before_read = desc_metric(desc_read_counter);
    const auto before_skipped = desc_metric(desc_skip_counter);
    auto result =
            variant_util::VariantCompactionUtil::calculate_variant_extended_schema(rowsets, schema);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(schema_has_extracted_variant_path(*result, 2, "extra"));
    EXPECT_EQ(2, desc_metric(desc_read_counter) - before_read);
    EXPECT_EQ(1, desc_metric(desc_skip_counter) - before_skipped);
}

void IndexStorageVariantCompactionSchemaTest::expect_desc_reads_after_incomplete_representative(
        const char* point, const Status& failure) {
    std::vector<RowsetSharedPtr> rowsets;
    const auto status = write_desc_fingerprint_rowsets(rowsets);
    ASSERT_TRUE(status.ok()) << status.to_string();

    auto remaining = rowsets.front()->num_segments();
    auto* sync = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync->set_call_back(
            point,
            [&](auto&& args) {
                if (remaining > 0) {
                    --remaining;
                    *try_any_cast<Status*>(args[0]) = failure;
                }
            },
            &guard);
    sync->enable_processing();
    Defer cleanup {[&] { sync->disable_processing(); }};

    const auto before_read = desc_metric(desc_read_counter);
    const auto before_skipped = desc_metric(desc_skip_counter);
    auto result = variant_util::VariantCompactionUtil::calculate_variant_extended_schema(
            rowsets, tablet_schema());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(0, remaining);
    EXPECT_TRUE(schema_has_extracted_variant_path(*result, 2, "a"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*result, 2, "b"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*result, 2, "extra"));
    EXPECT_EQ(3, desc_metric(desc_read_counter) - before_read);
    EXPECT_EQ(0, desc_metric(desc_skip_counter) - before_skipped);
}

TEST_F(IndexStorageVariantCompactionSchemaTest, DescReadsNextRepresentativeAfterMissingRoot) {
    expect_desc_reads_after_incomplete_representative("VariantDescFingerprint::root_reader_status",
                                                      Status::NotFound("missing variant root"));
}

TEST_F(IndexStorageVariantCompactionSchemaTest, DescReadsNextRepresentativeAfterExternalMetaError) {
    expect_desc_reads_after_incomplete_representative(
            "VariantDescFingerprint::external_meta_status",
            Status::IOError("external Variant metadata unavailable"));
}

TEST_F(IndexStorageVariantCompactionSchemaTest, DescFingerprintSkipsAfterMetadataRoundTrips) {
    std::vector<RowsetSharedPtr> rowsets;
    const auto status = write_desc_fingerprint_rowsets(rowsets);
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (const auto& rowset : rowsets) {
        auto meta = rowset->rowset_meta();
        std::string serialized;
        ASSERT_TRUE(meta->serialize(&serialized));
        ASSERT_TRUE(meta->init(serialized));
    }
    expect_desc_deduplication(rowsets, tablet_schema());

    for (const auto& rowset : rowsets) {
        auto meta = rowset->rowset_meta();
        auto cloud_meta = cloud::doris_rowset_meta_to_cloud(meta->get_rowset_pb());
        auto restored = cloud::cloud_rowset_meta_to_doris(std::move(cloud_meta));
        // The Cloud converter materializes resource_id. These files live in the local fixture.
        restored.clear_resource_id();
        ASSERT_TRUE(meta->init_from_pb(restored));
    }
    expect_desc_deduplication(rowsets, tablet_schema());
}

TEST_F(IndexStorageVariantCompactionSchemaTest, DescFingerprintMatchesLegacyReader) {
    std::vector<RowsetSharedPtr> rowsets;
    const auto status = write_desc_fingerprint_rowsets(rowsets);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& fingerprint = rowsets[0]->rowset_meta()->variant_desc_fingerprint();
    auto from_fingerprints = variant_util::VariantCompactionUtil::calculate_variant_extended_schema(
            rowsets, tablet_schema());
    ASSERT_NE(from_fingerprints, nullptr);
    EXPECT_TRUE(schema_has_extracted_variant_path(*from_fingerprints, 2, "extra"));

    rowsets[2]->rowset_meta()->mutable_variant_desc_fingerprint()->CopyFrom(fingerprint);
    auto with_stale_fingerprint =
            variant_util::VariantCompactionUtil::calculate_variant_extended_schema(rowsets,
                                                                                   tablet_schema());
    ASSERT_NE(with_stale_fingerprint, nullptr);
    EXPECT_EQ(from_fingerprints->to_key(), with_stale_fingerprint->to_key());

    for (const auto& rowset : rowsets) {
        rowset->rowset_meta()->clear_variant_desc_fingerprint();
    }
    auto from_segments = variant_util::VariantCompactionUtil::calculate_variant_extended_schema(
            rowsets, tablet_schema());
    ASSERT_NE(from_segments, nullptr);
    EXPECT_EQ(from_fingerprints->to_key(), from_segments->to_key());
}

TEST(IndexStorageVariantCompactionUtilTest, GetSubpathsHonorsZeroLimitAndTieOrdering) {
    variant_util::PathToNoneNullValues stats {
            {"alpha", 3},
            {"beta", 3},
            {"gamma", 1},
    };

    VariantCompactionPaths unlimited;
    variant_util::VariantCompactionUtil::get_subpaths(0, stats, unlimited);
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("gamma")));
    EXPECT_TRUE(unlimited.sparse_path_set.empty());

    VariantCompactionPaths top_one;
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

    VariantCompactionPaths at_limit;
    variant_util::VariantCompactionUtil::get_subpaths(2, exact_limit, at_limit);
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(at_limit.sparse_path_set.empty());

    VariantCompactionPaths empty;
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
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {}, compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(compaction_schema->has_column_unique_id(2));

    const auto* path_set_info = compaction_paths.contains(2) ? &compaction_paths.at(2) : nullptr;
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
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_paths.contains(2) ? &compaction_paths.at(2) : nullptr;
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
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_paths.contains(2) ? &compaction_paths.at(2) : nullptr;
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
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_paths.contains(2) ? &compaction_paths.at(2) : nullptr;
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
