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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "exec/common/variant_util.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/variant/index_storage_variant_test_base.h"

namespace doris::index_storage_test {

class IndexStorageVariantCompactionSchemaTest : public IndexStorageTestFixture {
protected:
};

namespace {

class ScopedLocalReadFailure {
public:
    explicit ScopedLocalReadFailure(
            Status error, std::function<bool()> ready = [] { return true; })
            : _sync_point(SyncPoint::get_instance()), _was_enabled(_sync_point->get_enable()) {
        _sync_point->set_call_back(
                "LocalFileReader::read_at_impl",
                [this, thread = std::this_thread::get_id(), error = std::move(error),
                 ready = std::move(ready)](auto&& args) {
                    if (std::this_thread::get_id() == thread && !_fired && ready()) {
                        auto* result = try_any_cast<std::pair<Status, bool>*>(args.back());
                        result->first = error;
                        result->second = true;
                        _fired = true;
                    }
                },
                &_guard);
        _sync_point->enable_processing();
    }

    ~ScopedLocalReadFailure() {
        if (!_was_enabled) {
            _sync_point->disable_processing();
        }
    }

    bool fired() const { return _fired; }

private:
    SyncPoint* _sync_point;
    bool _was_enabled;
    bool _fired = false;
    SyncPoint::CallbackGuard _guard;
};

} // namespace

class IndexStorageVariantExternalMetaTest : public IndexStorageTestFixture {
protected:
    static constexpr int32_t VARIANT_UID = 2;

    void SetUp() override {
        auto* env = ExecEnv::GetInstance();
        _previous_disable_segment_cache = std::exchange(config::disable_segment_cache, false);
        _previous_cache_manager = env->get_cache_manager();
        _previous_loader = env->segment_loader();
        _cache_manager.reset(CacheManager::create_global_instance());
        env->set_cache_manager(_cache_manager.get());
        _loader = std::make_unique<SegmentLoader>(64 * 1024 * 1024, 1024);
        env->set_segment_loader(_loader.get());
        IndexStorageTestFixture::SetUp();
    }

    Status write_v3_rows(int64_t version, std::vector<std::string> rows) {
        VariantColumnSpec variant;
        variant.unique_id = VARIANT_UID;
        variant.name = "v";
        variant.max_subcolumns_count = 4;
        IndexTabletOptions options;
        options.tablet_id = 110081;
        options.external_segment_meta = true;
        options.variant_columns = {std::move(variant)};
        RETURN_IF_ERROR(create_tablet(options));

        IndexRowsetSpec spec;
        spec.version = version;
        spec.add_to_tablet = false;
        spec.batches.push_back(IndexBatch::single_variant(std::move(rows)));
        auto rowset = write_rowset(spec);
        if (!rowset.has_value()) {
            return rowset.error();
        }
        _rowset = std::move(rowset).value();
        return Status::OK();
    }

    Status open_cached_variant_reader() {
        auto beta_rowset = std::static_pointer_cast<BetaRowset>(_rowset);
        auto* loader = SegmentLoader::instance();
        RETURN_IF_ERROR(loader->load_segments(beta_rowset, &_segments, true));
        SegmentCacheHandle cache_hit;
        RETURN_IF_ERROR(loader->load_segments(beta_rowset, &cache_hit));
        if (_segments.get_segments().size() != 1 ||
            _segments.get_segments() != cache_hit.get_segments()) {
            return Status::InternalError("Expected one cached V3 segment");
        }
        OlapReaderStatistics stats;
        StorageReadOptions options(stats);
        return _segments.get_segments().front()->get_variant_root_reader(
                tablet_schema()->column_by_uid(VARIANT_UID), options, &_reader);
    }

    void TearDown() override {
        if (_rowset != nullptr) {
            SegmentLoader::instance()->erase_segments(*_rowset->rowset_meta());
        }
        _reader.reset();
        _segments.get_segments().clear();
        _rowset.reset();
        IndexStorageTestFixture::TearDown();
        auto* env = ExecEnv::GetInstance();
        env->set_segment_loader(_previous_loader);
        _loader.reset();
        env->set_cache_manager(_previous_cache_manager);
        _cache_manager.reset();
        config::disable_segment_cache = _previous_disable_segment_cache;
    }

    RowsetSharedPtr _rowset;
    SegmentCacheHandle _segments;
    std::shared_ptr<segment_v2::VariantColumnReader> _reader;

private:
    bool _previous_disable_segment_cache = false;
    CacheManager* _previous_cache_manager = nullptr;
    SegmentLoader* _previous_loader = nullptr;
    std::unique_ptr<CacheManager> _cache_manager;
    std::unique_ptr<SegmentLoader> _loader;
};

TEST_F(IndexStorageVariantExternalMetaTest, V3PreservesOrdinaryMaterializedFieldWithSparseMarker) {
    const std::vector<std::optional<std::string>> expected = {
            R"({"control":1})", R"({"metric__DORIS_VARIANT_SPARSE__suffix":2})",
            R"({"metric__DORIS_VARIANT_DOC_VALUE__suffix":3})"};
    auto write_status = write_v3_rows(8101, {*expected[0], *expected[1], *expected[2]});
    ASSERT_TRUE(write_status.ok()) << write_status;

    IndexReadOptions options;
    options.need_ordered_result = true;
    options.collect_variant_values = true;
    auto read = read_rowsets({_rowset}, options);
    ASSERT_TRUE(read.has_value()) << read.error();
    ASSERT_TRUE(read->variant_values_by_uid.contains(VARIANT_UID));
    EXPECT_EQ(canonical_variant_values(read->variant_values_by_uid.at(VARIANT_UID)),
              canonical_variant_values(expected));
}

TEST_F(IndexStorageVariantExternalMetaTest, V3PrefixReadErrorDoesNotMeanNoChildren) {
    auto status = write_v3_rows(8102, {R"({"parent":{"child":1}})"});
    ASSERT_TRUE(status.ok()) << status;
    status = open_cached_variant_reader();
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(_reader, nullptr);

    const PathInData parent("parent");
    {
        ScopedLocalReadFailure failure(Status::IOError<false>("temporary V3 read failure"));
        auto result = _reader->has_prefix_path(parent);
        ASSERT_TRUE(failure.fired());
        ASSERT_FALSE(result.has_value());
        EXPECT_TRUE(result.error().is<ErrorCode::IO_ERROR>()) << result.error();
    }
    auto result = _reader->has_prefix_path(parent);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_TRUE(*result) << "Control read must find the on-disk child";

    auto missing = _reader->has_prefix_path(PathInData("missing"));
    ASSERT_TRUE(missing.has_value()) << missing.error();
    EXPECT_FALSE(*missing);
}

TEST_F(IndexStorageVariantExternalMetaTest, V3ExtendedSchemaDoesNotReportPartialSuccess) {
    auto status = write_v3_rows(8103, {R"({"present":1})"});
    ASSERT_TRUE(status.ok()) << status;
    status = open_cached_variant_reader();
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(_reader, nullptr);

    {
        ScopedLocalReadFailure failure(
                Status::MemoryLimitExceeded<false>("temporary V3 read limit"));
        auto schema = variant_util::VariantCompactionUtil::calculate_variant_extended_schema(
                {_rowset}, tablet_schema());
        ASSERT_TRUE(failure.fired());
        ASSERT_FALSE(schema.has_value());
        EXPECT_TRUE(schema.error().is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << schema.error();
    }
    auto recovered = variant_util::VariantCompactionUtil::calculate_variant_extended_schema(
            {_rowset}, tablet_schema());
    ASSERT_TRUE(recovered.has_value()) << recovered.error();
    ASSERT_NE(*recovered, nullptr);
    EXPECT_TRUE(schema_has_extracted_variant_path(**recovered, VARIANT_UID, "present"));
}

TEST_F(IndexStorageVariantExternalMetaTest, V3BulkLoadRetriesTransientReadError) {
    auto status = write_v3_rows(8104, {R"({"first":1,"second":2})"});
    ASSERT_TRUE(status.ok()) << status;
    status = open_cached_variant_reader();
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(_reader, nullptr);

    const PathInData first_path("first");
    const PathInData second_path("second");
    const auto* tree = _reader->get_subcolumns_meta_info();
    const auto initial_leaves = tree->size();
    {
        // Fail the next metadata read after the first path was published. The callback
        // runs on the loader thread while it holds the metadata lock.
        ScopedLocalReadFailure failure(Status::IOError<false>("temporary V3 read failure"),
                                       [&] { return tree->find_leaf(first_path) != nullptr; });
        const auto first = _reader->load_external_meta_once();
        ASSERT_TRUE(failure.fired());
        ASSERT_TRUE(first.is<ErrorCode::IO_ERROR>()) << first;
        EXPECT_NE(_reader->get_subcolumn_meta_by_path(first_path), nullptr);
        EXPECT_EQ(_reader->get_subcolumn_meta_by_path(second_path), nullptr);
    }
    const auto second = _reader->load_external_meta_once();
    ASSERT_TRUE(second.ok()) << second;
    EXPECT_NE(_reader->get_subcolumn_meta_by_path(first_path), nullptr);
    EXPECT_NE(_reader->get_subcolumn_meta_by_path(second_path), nullptr);
    EXPECT_EQ(tree->size(), initial_leaves + 2);
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

    auto result =
            variant_util::VariantCompactionUtil::calculate_variant_extended_schema({}, base_schema);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(*result, nullptr);

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

TEST(IndexStorageVariantCompactionUtilTest, InternalBucketPathsKeepDottedRootNameTogether) {
    TabletColumn root;
    root.set_name("v.with.dot");
    root.set_unique_id(2);

    const auto sparse = variant_util::create_sparse_column(root);
    const auto sparse_bucket = variant_util::create_sparse_shard_column(root, 0);
    const auto doc_bucket = variant_util::create_doc_value_column(root, 1);
    const auto doc_compaction = variant_util::create_doc_value_compaction_column(root, 1);

    EXPECT_EQ(sparse.path_info_ptr()->get_relative_path(), SPARSE_COLUMN_PATH);
    EXPECT_EQ(sparse_bucket.path_info_ptr()->get_relative_path(), SPARSE_COLUMN_PATH + ".b0");
    EXPECT_EQ(doc_bucket.path_info_ptr()->get_relative_path(), DOC_VALUE_COLUMN_PATH + ".b1");
    EXPECT_EQ(*doc_compaction.path_info_ptr(), *doc_bucket.path_info_ptr());
    for (const auto* column : {&sparse, &sparse_bucket, &doc_bucket, &doc_compaction}) {
        EXPECT_EQ(column->path_info_ptr()->get_parts().front().key, "v.with.dot");
    }
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
