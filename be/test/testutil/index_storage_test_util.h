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

#pragma once

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/descriptors.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/predicate/column_predicate.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_schema.h"
#include "util/json/json_parser.h"

namespace doris {

class DataDir;
struct DebugPoint;
class StorageEngine;

namespace segment_v2 {
class InvertedIndexQueryCache;
class InvertedIndexSearcherCache;
} // namespace segment_v2

namespace index_storage_test {

struct TextColumnSpec {
    int32_t unique_id = 2;
    std::string name = "title";
    FieldType type = FieldType::OLAP_FIELD_TYPE_STRING;
    bool nullable = false;
};

struct VariantPathSpec {
    std::string path;
    FieldType type = FieldType::OLAP_FIELD_TYPE_STRING;
    bool nullable = true;
    PatternTypePB pattern_type = PatternTypePB::MATCH_NAME;
    std::optional<FieldType> array_item_type;
    bool array_item_nullable = true;
};

struct VariantColumnSpec {
    int32_t unique_id = 2;
    std::string name = "v";
    bool nullable = false;
    int32_t max_subcolumns_count = 8;
    int32_t max_sparse_column_statistics_size = 10000;
    int32_t sparse_hash_shard_count = 0;
    bool enable_doc_mode = false;
    int64_t doc_materialization_min_rows = 0;
    int32_t doc_hash_shard_count = 0;
    bool enable_nested_group = false;
    std::vector<VariantPathSpec> predefined_paths;
};

struct IndexSpec {
    int64_t index_id = 10001;
    std::string name = "idx_v";
    int32_t column_uid = 2;
    std::string suffix_path;
    std::map<std::string, std::string> properties;

    static IndexSpec column_index(int64_t index_id, std::string name, int32_t column_uid,
                                  std::map<std::string, std::string> properties = {});
    static IndexSpec field_pattern_index(int64_t index_id, std::string name, int32_t column_uid,
                                         std::string field_pattern);
};

struct IndexTabletOptions {
    int64_t tablet_id = 100000;
    bool include_key_column = true;
    bool external_segment_meta = true;
    InvertedIndexStorageFormatPB index_storage_format = InvertedIndexStorageFormatPB::V2;
    std::vector<TextColumnSpec> text_columns;
    std::vector<VariantColumnSpec> variant_columns;
    std::vector<IndexSpec> inverted_indexes;
};

struct IndexBatch {
    std::vector<int32_t> keys;
    std::vector<std::vector<std::string>> text_values_by_column;
    std::vector<std::vector<std::string>> variant_jsons_by_column;
    std::vector<ColumnPtr> variant_columns_by_column;
    bool deprecated_enable_flatten_nested = false;
    bool check_duplicate_json_path = false;
    ParseConfig::ParseTo parse_to = ParseConfig::ParseTo::OnlySubcolumns;

    static IndexBatch single_text(std::vector<std::string> values, int32_t first_key = 0);
    static IndexBatch single_variant(std::vector<std::string> jsons, int32_t first_key = 0);
    static IndexBatch single_variant_column(ColumnPtr column, int32_t first_key = 0);
    size_t num_rows() const;
};

enum class IndexDataSourceKind {
    INLINE_TEXT_ROWS,
    INLINE_VARIANT_ROWS,
    VARIANT_JSONL_FILE,
};

struct IndexDataSourceSpec {
    IndexDataSourceKind kind = IndexDataSourceKind::INLINE_TEXT_ROWS;
    std::vector<std::string> rows;
    std::string file_path;
    int32_t first_key = 0;
    size_t batch_size = 4096;

    static IndexDataSourceSpec inline_text(std::vector<std::string> values, int32_t first_key = 0);
    static IndexDataSourceSpec inline_variant(std::vector<std::string> jsons,
                                              int32_t first_key = 0);
    static IndexDataSourceSpec variant_jsonl(std::string file_path, int32_t first_key = 0,
                                             size_t batch_size = 4096);
};

struct IndexRowsetSpec {
    int64_t version = 0;
    int64_t max_rows_per_segment = 200;
    DataWriteType write_type = DataWriteType::TYPE_DIRECT;
    bool add_to_tablet = true;
    std::vector<IndexDataSourceSpec> data_sources;
    std::vector<IndexBatch> batches;
};

struct IndexReadOptions {
    ReaderType reader_type = ReaderType::READER_QUERY;
    bool need_ordered_result = false;
    std::string index_probe_label;
    std::vector<uint32_t> return_columns;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
    std::map<std::string, DataTypePtr> target_cast_type_for_variants;
    std::map<int32_t, TColumnAccessPaths> all_access_paths;
    std::map<int32_t, TColumnAccessPaths> predicate_access_paths;
    VExprContextSPtrs common_expr_ctxs_push_down;
    bool collect_string_values = false;
    bool collect_variant_values = false;
    bool enable_inverted_index_query = true;
    bool enable_fallback_on_missing_inverted_index = true;
    bool enable_no_need_read_data_opt = true;
    bool enable_common_expr_pushdown = true;
    bool enable_inverted_index_query_cache = false;
    TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;
};

struct IndexProfileSnapshot {
    std::string label;
    int64_t rows_inverted_index_filtered = 0;
    int64_t inverted_index_filter_timer = 0;
    int64_t inverted_index_query_timer = 0;
    int64_t inverted_index_query_null_bitmap_timer = 0;
    int64_t inverted_index_query_bitmap_copy_timer = 0;
    int64_t inverted_index_searcher_open_timer = 0;
    int64_t inverted_index_searcher_search_timer = 0;
    int64_t inverted_index_searcher_search_init_timer = 0;
    int64_t inverted_index_searcher_search_exec_timer = 0;
    int64_t inverted_index_downgrade_count = 0;
    int64_t inverted_index_analyzer_timer = 0;
    int64_t inverted_index_lookup_timer = 0;
};

struct IndexProbeExpectation {
    std::optional<IndexProbeSource> source;
    std::optional<IndexProbeState> state;
    std::optional<IndexFallbackReason> reason;
    std::optional<int32_t> column_uid;
    std::optional<std::string> variant_path;
    std::optional<int64_t> index_id;
    std::optional<int64_t> filtered_rows;
};

struct IndexReadResult {
    int64_t rows_read = 0;
    OlapReaderStatistics stats;
    std::vector<IndexProfileSnapshot> profile_snapshots;
    std::map<int32_t, std::vector<std::optional<std::string>>> string_values_by_uid;
    std::map<int32_t, std::vector<std::optional<std::string>>> variant_values_by_uid;

    bool inverted_index_attempted() const;
    bool inverted_index_downgraded() const;
    bool inverted_index_used() const;
    bool inverted_index_effective_filter() const;
};

struct IndexColumnLayout {
    int32_t parent_unique_id = -1;
    std::string full_path;
    std::string relative_path;
    int64_t none_null_size = 0;
    bool is_sparse_column = false;
    bool is_doc_value_column = false;
    std::map<std::string, int64_t> sparse_non_null_size;
};

struct IndexSegmentLayout {
    int64_t segment_id = -1;
    int64_t num_rows = 0;
    std::vector<IndexColumnLayout> variant_columns;

    bool contains_relative_path(const std::string& path) const;
};

struct IndexFileProbe {
    int64_t expected_files = 0;
    int64_t existing_files = 0;
    int64_t rowset_meta_entries = 0;
    int64_t rowset_meta_index_size = 0;
    std::vector<std::string> missing_files;

    bool all_expected_files_exist() const { return expected_files == existing_files; }
};

struct IndexRowsetProbe {
    int64_t num_rows = 0;
    int64_t num_segments = 0;
    std::vector<IndexSegmentLayout> segments;
    IndexFileProbe index_files;

    bool contains_relative_path(const std::string& path) const;
};

enum class IndexCompactionKind {
    CUMULATIVE,
    FULL,
};

struct IndexSchemaPatch {
    std::vector<TextColumnSpec> add_text_columns;
    std::vector<VariantColumnSpec> add_variant_columns;
    std::set<int32_t> drop_column_uids;
    std::map<int32_t, VariantColumnSpec> modify_variant_columns;
    std::vector<IndexSpec> add_inverted_indexes;
    std::set<int64_t> drop_index_ids;
    std::set<std::string> drop_index_names;
};

struct IndexStorageCase {
    std::string name;
    IndexTabletOptions tablet_options;
    std::vector<IndexRowsetSpec> rowsets;
};

class IndexStorageCaseBuilder {
public:
    explicit IndexStorageCaseBuilder(std::string name);

    IndexStorageCaseBuilder& tablet_id(int64_t tablet_id);
    IndexStorageCaseBuilder& text_column(TextColumnSpec column);
    IndexStorageCaseBuilder& variant_column(VariantColumnSpec column);
    IndexStorageCaseBuilder& inverted_index(IndexSpec index);
    IndexStorageCaseBuilder& rowset(int64_t version, IndexDataSourceSpec data_source,
                                    int64_t max_rows_per_segment = 200);
    IndexStorageCase build() const;

private:
    IndexStorageCase _case;
};

TabletSchemaPB build_tablet_schema_pb(const IndexTabletOptions& options);
TabletSchemaSPtr build_tablet_schema(const IndexTabletOptions& options);
TabletSchemaPB apply_schema_patch(const TabletSchema& base_schema, const IndexSchemaPatch& patch);
TabletSchemaSPtr build_patched_tablet_schema(const TabletSchema& base_schema,
                                             const IndexSchemaPatch& patch);
TabletSchemaSPtr build_schema_with_variant_path_column(const TabletSchema& base_schema,
                                                       int32_t parent_unique_id,
                                                       std::string relative_path, FieldType type);

void expect_index_filter_stats(const IndexReadResult& result, int64_t expected_filtered_rows);
void expect_inverted_index_used(const IndexReadResult& result);
void expect_inverted_index_fallback(const IndexReadResult& result);
void expect_inverted_index_not_attempted(const IndexReadResult& result);
void expect_index_probe(const IndexReadResult& result, const IndexProbeExpectation& expectation);
void expect_no_index_probe(const IndexReadResult& result, const IndexProbeExpectation& expectation);
void expect_applied_variant_path_index(const IndexReadResult& result, std::string_view path,
                                       int64_t index_id, int64_t expected_filtered_rows,
                                       int32_t column_uid = 2);
void expect_index_not_applied(const IndexReadResult& result, int64_t index_id,
                              int32_t column_uid = 2);
void expect_index_files(const IndexRowsetProbe& probe, bool expected_present);
std::string dump_schema_paths(const TabletSchema& schema);

class ScopedDebugPoint {
public:
    explicit ScopedDebugPoint(std::string name, std::map<std::string, std::string> params = {});
    ~ScopedDebugPoint();

    int64_t execute_num() const;

private:
    std::string _name;
    bool _enable_debug_points = false;
    std::shared_ptr<DebugPoint> _debug_point;
};

class IndexStorageTestFixture : public testing::Test {
public:
    ~IndexStorageTestFixture() override;

protected:
    void SetUp() override;
    void TearDown() override;

    Status create_tablet(const IndexTabletOptions& options);
    Result<RowsetSharedPtr> write_rowset(const IndexRowsetSpec& spec);
    Result<std::vector<RowsetSharedPtr>> write_rowsets(const std::vector<IndexRowsetSpec>& specs);
    Result<IndexReadResult> read_rowsets(const std::vector<RowsetSharedPtr>& rowsets,
                                         IndexReadOptions options = {});
    Result<RowsetSharedPtr> compact_rowsets(IndexCompactionKind kind,
                                            const std::vector<RowsetSharedPtr>& rowsets);
    Result<RowsetSharedPtr> compact_rowsets_and_reload(IndexCompactionKind kind,
                                                       const std::vector<RowsetSharedPtr>& rowsets);
    Result<RowsetSharedPtr> reload_rowset(const RowsetSharedPtr& rowset);
    Result<std::vector<RowsetSharedPtr>> reload_rowsets(
            const std::vector<RowsetSharedPtr>& rowsets);
    // Synthetic reader-schema injection for storage UTs. This mutates fixture tablet metadata and
    // reloads rowsets with the supplied schema; it does not model a real schema-change job.
    Result<std::vector<RowsetSharedPtr>> inject_reader_schema_for_rowsets(
            const std::vector<RowsetSharedPtr>& rowsets, TabletSchemaSPtr schema);
    Result<std::vector<RowsetSharedPtr>> build_inverted_indexes(
            const std::vector<IndexSpec>& indexes);
    Result<std::vector<RowsetSharedPtr>> build_inverted_indexes_and_reload(
            const std::vector<IndexSpec>& indexes);
    Result<std::vector<RowsetSharedPtr>> drop_inverted_indexes(
            const std::vector<IndexSpec>& indexes);
    Result<std::vector<RowsetSharedPtr>> drop_inverted_indexes_and_reload(
            const std::vector<IndexSpec>& indexes);
    Result<IndexRowsetProbe> probe_rowset(const RowsetSharedPtr& rowset);
    void use_rowset_schema(const RowsetSharedPtr& rowset);
    Result<std::vector<RowsetSharedPtr>> rowsets_with_variant_extended_schema(
            const std::vector<RowsetSharedPtr>& rowsets);
    int32_t column_id_by_path(const std::string& path) const;

    const TabletSchemaSPtr& tablet_schema() const { return _tablet_schema; }
    const TabletSharedPtr& tablet() const { return _tablet; }
    StorageEngine* storage_engine() const { return _engine_ref; }
    DataDir* data_dir() const { return _data_dir.get(); }

private:
    std::string _test_dir;
    std::string _tmp_dir;
    TabletSchemaSPtr _tablet_schema;
    TabletSharedPtr _tablet;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    std::unique_ptr<segment_v2::InvertedIndexSearcherCache> _inverted_index_searcher_cache;
    std::unique_ptr<segment_v2::InvertedIndexQueryCache> _inverted_index_query_cache;
};

} // namespace index_storage_test
} // namespace doris
