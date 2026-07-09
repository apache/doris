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

#include "testutil/index_storage_test_util.h"

#include <cctz/time_zone.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <numeric>
#include <shared_mutex>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "exec/common/variant_util.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/compaction/cumulative_compaction.h"
#include "storage/compaction/full_compaction.h"
#include "storage/data_dir.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/options.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/task/index_builder.h"
#include "util/debug_points.h"

namespace doris::index_storage_test {
namespace {

constexpr uint32_t kMaxPathLen = 1024;

#define RETURN_RESULT_IF_ERROR(stmt)                 \
    do {                                             \
        Status _status_ = (stmt);                    \
        if (UNLIKELY(!_status_.ok())) {              \
            return ResultError(std::move(_status_)); \
        }                                            \
    } while (false)

std::string describe_filter_stats(const IndexReadResult& result) {
    std::ostringstream oss;
    oss << "\nfilter_stats:"
        << " rows_read=" << result.rows_read << " raw_rows_read=" << result.stats.raw_rows_read
        << " total_segments=" << result.stats.total_segment_number
        << " filtered_segments=" << result.stats.filtered_segment_number
        << " rows_inverted_index_filtered=" << result.stats.rows_inverted_index_filtered
        << " rows_stats_filtered=" << result.stats.rows_stats_filtered
        << " rows_stats_rp_filtered=" << result.stats.rows_stats_rp_filtered
        << " rows_bf_filtered=" << result.stats.rows_bf_filtered
        << " segment_dict_filtered=" << result.stats.segment_dict_filtered
        << " rows_conditions_filtered=" << result.stats.rows_conditions_filtered;
    return oss.str();
}

std::string escape_for_variant_index_suffix(const std::string& value) {
    auto hex_digit = [](int value) -> char {
        return value < 10 ? static_cast<char>('0' + value) : static_cast<char>('A' + value - 10);
    };

    std::string escaped;
    escaped.reserve(value.size());
    for (unsigned char ch : value) {
        if (ch == '.' || ch == '_') {
            escaped.push_back('%');
            escaped.push_back(hex_digit(ch / 16));
            escaped.push_back(hex_digit(ch % 16));
        } else {
            escaped.push_back(static_cast<char>(ch));
        }
    }
    return escaped;
}

std::string sanitize_test_name(std::string name) {
    for (auto& ch : name) {
        if (!std::isalnum(static_cast<unsigned char>(ch))) {
            ch = '_';
        }
    }
    return name;
}

std::string current_working_dir() {
    char buffer[kMaxPathLen];
    CHECK(getcwd(buffer, kMaxPathLen) != nullptr);
    return std::string(buffer);
}

void init_key_column(ColumnPB* column_pb) {
    column_pb->set_unique_id(1);
    column_pb->set_name("k");
    column_pb->set_type("INT");
    column_pb->set_is_key(true);
    column_pb->set_length(4);
    column_pb->set_index_length(4);
    column_pb->set_is_nullable(false);
    column_pb->set_is_bf_column(false);
}

void init_text_column(ColumnPB* column_pb, const TextColumnSpec& spec) {
    column_pb->set_unique_id(spec.unique_id);
    column_pb->set_name(spec.name);
    column_pb->set_type(TabletColumn::get_string_by_field_type(spec.type));
    column_pb->set_is_key(false);
    column_pb->set_is_nullable(spec.nullable);
    column_pb->set_is_bf_column(false);
}

void init_variant_column(ColumnPB* column_pb, const VariantColumnSpec& spec) {
    column_pb->set_unique_id(spec.unique_id);
    column_pb->set_name(spec.name);
    column_pb->set_type("VARIANT");
    column_pb->set_is_key(false);
    column_pb->set_is_nullable(spec.nullable);
    column_pb->set_is_bf_column(spec.is_bf_column);
    column_pb->set_variant_max_subcolumns_count(spec.max_subcolumns_count);
    column_pb->set_variant_max_sparse_column_statistics_size(
            spec.max_sparse_column_statistics_size);
    column_pb->set_variant_sparse_hash_shard_count(spec.sparse_hash_shard_count);
    column_pb->set_variant_enable_doc_mode(spec.enable_doc_mode);
    column_pb->set_variant_doc_materialization_min_rows(spec.doc_materialization_min_rows);
    if (spec.doc_hash_shard_count > 0) {
        column_pb->set_variant_doc_hash_shard_count(spec.doc_hash_shard_count);
    }
    column_pb->set_variant_enable_nested_group(spec.enable_nested_group);
}

bool is_text_field_type(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_STRING ||
           type == FieldType::OLAP_FIELD_TYPE_VARCHAR || type == FieldType::OLAP_FIELD_TYPE_CHAR;
}

std::string relative_variant_path(std::string full_path) {
    auto dot_pos = full_path.find('.');
    if (dot_pos == std::string::npos) {
        return full_path;
    }
    return full_path.substr(dot_pos + 1);
}

TabletColumn make_predefined_path_template(const std::string&, const VariantPathSpec& path_spec,
                                           int32_t parent_unique_id) {
    ColumnPB column_pb;
    column_pb.set_unique_id(-1);
    column_pb.set_name(path_spec.path);
    column_pb.set_type(TabletColumn::get_string_by_field_type(path_spec.type));
    column_pb.set_is_key(false);
    column_pb.set_is_nullable(path_spec.nullable);
    if (path_spec.type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2) {
        column_pb.set_frac(0);
    }
    column_pb.set_pattern_type(path_spec.pattern_type);
    if (path_spec.type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        auto* child_pb = column_pb.add_children_columns();
        child_pb->set_unique_id(-1);
        child_pb->set_name(path_spec.path + ".item");
        child_pb->set_type(TabletColumn::get_string_by_field_type(
                path_spec.array_item_type.value_or(FieldType::OLAP_FIELD_TYPE_STRING)));
        child_pb->set_is_key(false);
        child_pb->set_is_nullable(path_spec.array_item_nullable);
        child_pb->set_length(INT_MAX);
        child_pb->set_index_length(0);
    }

    TabletColumn subcolumn;
    subcolumn.init_from_pb(column_pb);
    subcolumn.set_parent_unique_id(parent_unique_id);
    return subcolumn;
}

void append_predefined_paths(TabletSchema* schema, const IndexTabletOptions& options) {
    for (const auto& column_spec : options.variant_columns) {
        if (column_spec.predefined_paths.empty()) {
            continue;
        }
        TabletColumn& variant = schema->mutable_column_by_uid(column_spec.unique_id);
        for (const auto& path_spec : column_spec.predefined_paths) {
            TabletColumn subcolumn = make_predefined_path_template(column_spec.name, path_spec,
                                                                   column_spec.unique_id);
            variant.add_sub_column(subcolumn);
        }
    }
}

bool index_references_any_column(const TabletIndexPB& index, const std::set<int32_t>& column_uids) {
    return std::any_of(index.col_unique_id().begin(), index.col_unique_id().end(),
                       [&](int32_t uid) { return column_uids.contains(uid); });
}

TQueryOptions make_read_query_options(const IndexReadOptions& options) {
    TQueryOptions query_options;
    query_options.__set_enable_inverted_index_query(options.enable_inverted_index_query);
    query_options.__set_enable_fallback_on_missing_inverted_index(
            options.enable_fallback_on_missing_inverted_index);
    query_options.__set_enable_no_need_read_data_opt(options.enable_no_need_read_data_opt);
    query_options.__set_enable_common_expr_pushdown(options.enable_common_expr_pushdown);
    query_options.__set_enable_inverted_index_query_cache(
            options.enable_inverted_index_query_cache);
    query_options.__set_enable_file_cache(false);
    query_options.__set_disable_file_cache(true);
    query_options.__set_enable_segment_cache(false);
    query_options.__set_batch_size(1024);
    if (options.inverted_index_skip_threshold.has_value()) {
        query_options.__set_inverted_index_skip_threshold(
                options.inverted_index_skip_threshold.value());
    }
    return query_options;
}

void append_index(TabletSchemaPB* schema_pb, const IndexSpec& spec) {
    auto* index_pb = schema_pb->add_index();
    index_pb->set_index_id(spec.index_id);
    index_pb->set_index_name(spec.name);
    index_pb->set_index_type(IndexType::INVERTED);
    index_pb->add_col_unique_id(spec.column_uid);
    if (!spec.suffix_path.empty()) {
        index_pb->set_index_suffix_name(escape_for_variant_index_suffix(spec.suffix_path));
    }
    auto* properties = index_pb->mutable_properties();
    for (const auto& [key, value] : spec.properties) {
        (*properties)[key] = value;
    }
}

TOlapTableIndex to_alter_index(const TabletSchema& schema, const IndexSpec& spec) {
    TOlapTableIndex index;
    index.__set_index_id(spec.index_id);
    index.__set_index_name(spec.name);
    index.__set_index_type(TIndexType::INVERTED);
    if (!spec.properties.empty()) {
        index.__set_properties(spec.properties);
    }

    std::string column_name;
    if (schema.has_column_unique_id(spec.column_uid)) {
        column_name = schema.column_by_uid(spec.column_uid).name();
    }
    index.__set_columns({column_name});
    index.__set_column_unique_ids({spec.column_uid});
    return index;
}

Result<std::vector<RowsetSharedPtr>> current_rowsets_for_versions(
        Tablet& tablet, const std::vector<Version>& versions) {
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(versions.size());
    std::shared_lock lock(tablet.get_header_lock());
    for (const auto& version : versions) {
        const auto& rowset_map = tablet.rowset_map();
        auto it = rowset_map.find(version);
        if (it == rowset_map.end()) {
            return ResultError(Status::InternalError(
                    "rowset version {} not found after index builder", version.to_string()));
        }
        rowsets.push_back(it->second);
    }
    std::sort(rowsets.begin(), rowsets.end(), Rowset::comparator);
    return rowsets;
}

std::filesystem::path repo_root_from_this_source_file() {
    std::filesystem::path source_path(__FILE__);
    if (source_path.is_relative()) {
        source_path = std::filesystem::absolute(source_path);
    }
    for (int i = 0; i < 4; ++i) {
        source_path = source_path.parent_path();
    }
    return source_path;
}

std::ifstream open_jsonl_file(const std::string& file_path, std::string* opened_path) {
    std::vector<std::filesystem::path> candidates;
    const std::filesystem::path input_path(file_path);
    candidates.push_back(input_path);
    if (!input_path.is_absolute()) {
        if (const char* doris_home = std::getenv("DORIS_HOME"); doris_home != nullptr) {
            candidates.push_back(std::filesystem::path(doris_home) / input_path);
        }
        candidates.push_back(repo_root_from_this_source_file() / input_path);
    }

    for (const auto& candidate : candidates) {
        std::ifstream input(candidate);
        if (input.is_open()) {
            *opened_path = candidate.string();
            return input;
        }
    }
    *opened_path = file_path;
    return {};
}

Result<std::vector<IndexBatch>> batches_from_data_source(const IndexDataSourceSpec& data_source) {
    switch (data_source.kind) {
    case IndexDataSourceKind::INLINE_TEXT_ROWS:
        return std::vector<IndexBatch> {
                IndexBatch::single_text(data_source.rows, data_source.first_key)};
    case IndexDataSourceKind::INLINE_VARIANT_ROWS:
        return std::vector<IndexBatch> {
                IndexBatch::single_variant(data_source.rows, data_source.first_key)};
    case IndexDataSourceKind::VARIANT_JSONL_FILE: {
        if (data_source.batch_size == 0) {
            return ResultError(Status::InvalidArgument("jsonl batch size must be positive"));
        }

        std::string opened_path;
        std::ifstream input = open_jsonl_file(data_source.file_path, &opened_path);
        if (!input.is_open()) {
            return ResultError(
                    Status::InvalidArgument("failed to open jsonl file {}", data_source.file_path));
        }

        std::vector<IndexBatch> batches;
        std::vector<std::string> rows;
        rows.reserve(data_source.batch_size);
        std::string line;
        int32_t next_key = data_source.first_key;
        while (std::getline(input, line)) {
            if (line.empty()) {
                continue;
            }
            rows.push_back(std::move(line));
            if (rows.size() == data_source.batch_size) {
                batches.push_back(IndexBatch::single_variant(std::move(rows), next_key));
                next_key += static_cast<int32_t>(batches.back().num_rows());
                rows.clear();
                rows.reserve(data_source.batch_size);
            }
        }
        if (!rows.empty()) {
            batches.push_back(IndexBatch::single_variant(std::move(rows), next_key));
        }
        if (batches.empty()) {
            return ResultError(Status::InvalidArgument("jsonl file {} has no rows", opened_path));
        }
        return batches;
    }
    }

    return ResultError(Status::InvalidArgument("unknown index data source kind"));
}

Result<std::vector<IndexBatch>> materialize_batches(const IndexRowsetSpec& spec) {
    std::vector<IndexBatch> batches = spec.batches;
    for (const auto& data_source : spec.data_sources) {
        auto materialized = batches_from_data_source(data_source);
        if (!materialized.has_value()) {
            return ResultError(materialized.error());
        }
        batches.insert(batches.end(), std::make_move_iterator(materialized->begin()),
                       std::make_move_iterator(materialized->end()));
    }
    return batches;
}

void ensure_variant_json_shape(const IndexBatch& batch, size_t column_pos) {
    CHECK(!batch.variant_jsons_by_column.empty());
    CHECK_LT(column_pos, batch.variant_jsons_by_column.size());
    const size_t rows = batch.num_rows();
    for (const auto& jsons : batch.variant_jsons_by_column) {
        CHECK_EQ(jsons.size(), rows);
    }
}

Status fill_variant_column(const VariantColumnSpec& column_spec, const IndexBatch& batch,
                           size_t column_pos, MutableColumnPtr* output) {
    ensure_variant_json_shape(batch, column_pos);
    auto variant_column =
            ColumnVariant::create(column_spec.max_subcolumns_count, column_spec.enable_doc_mode);
    auto json_column = ColumnString::create();
    for (const auto& json : batch.variant_jsons_by_column[column_pos]) {
        json_column->insert_data(json.data(), json.size());
    }

    ParseConfig config;
    config.deprecated_enable_flatten_nested = batch.deprecated_enable_flatten_nested;
    config.check_duplicate_json_path = batch.check_duplicate_json_path;
    config.parse_to =
            column_spec.enable_doc_mode ? ParseConfig::ParseTo::OnlyDocValueColumn : batch.parse_to;
    variant_util::parse_json_to_variant(*variant_column, *json_column, config);
    if (column_spec.nullable) {
        auto null_map = ColumnUInt8::create();
        null_map->insert_many_defaults(variant_column->size());
        *output = ColumnNullable::create(std::move(variant_column), std::move(null_map));
    } else {
        *output = std::move(variant_column);
    }
    return Status::OK();
}

Status validate_direct_variant_column(const VariantColumnSpec& column_spec, const ColumnPtr& column,
                                      size_t expected_rows, size_t variant_pos) {
    if (!column) {
        return Status::InvalidArgument("direct variant column {} is null", variant_pos);
    }
    if (column->size() != expected_rows) {
        return Status::InvalidArgument(
                "direct variant column {} row count mismatch: expected={}, actual={}",
                column_spec.name, expected_rows, column->size());
    }

    const IColumn* nested_column = column.get();
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(nested_column)) {
        if (!column_spec.nullable) {
            return Status::InvalidArgument(
                    "non-nullable variant column {} cannot use nullable direct column",
                    column_spec.name);
        }
        nested_column = &nullable_column->get_nested_column();
    }
    if (check_and_get_column<ColumnVariant>(nested_column) == nullptr) {
        return Status::InvalidArgument(
                "direct variant column {} must be ColumnVariant or ColumnNullable(ColumnVariant), "
                "actual={}",
                column_spec.name, column->get_name());
    }
    return Status::OK();
}

Status fill_text_column(const IndexBatch& batch, size_t column_pos, MutableColumnPtr* output) {
    if (column_pos >= batch.text_values_by_column.size()) {
        return Status::InvalidArgument("text batch column count mismatch: column_pos={}",
                                       column_pos);
    }
    const auto& values = batch.text_values_by_column[column_pos];
    auto text_column = ColumnString::create();
    for (const auto& value : values) {
        text_column->insert_data(value.data(), value.size());
    }
    *output = std::move(text_column);
    return Status::OK();
}

Status fill_block(const TabletSchema& schema, const IndexTabletOptions& tablet_options,
                  const IndexBatch& batch, int32_t* next_key, Block* block) {
    auto columns = std::move(*block).mutate_columns();
    size_t column_pos = 0;
    const size_t rows = batch.num_rows();

    if (tablet_options.include_key_column) {
        for (size_t row = 0; row < rows; ++row) {
            int32_t key = batch.keys.empty() ? (*next_key)++ : batch.keys[row];
            columns[column_pos]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
        }
        ++column_pos;
    }

    for (size_t text_pos = 0; text_pos < tablet_options.text_columns.size();
         ++text_pos, ++column_pos) {
        MutableColumnPtr text_column;
        RETURN_IF_ERROR(fill_text_column(batch, text_pos, &text_column));
        columns[column_pos] = std::move(text_column);
    }

    for (size_t variant_pos = 0; variant_pos < tablet_options.variant_columns.size();
         ++variant_pos, ++column_pos) {
        if (!batch.variant_columns_by_column.empty()) {
            RETURN_IF_ERROR(validate_direct_variant_column(
                    tablet_options.variant_columns[variant_pos],
                    batch.variant_columns_by_column[variant_pos], rows, variant_pos));
            auto variant_column = batch.variant_columns_by_column[variant_pos]->clone_resized(rows);
            if (tablet_options.variant_columns[variant_pos].nullable &&
                !variant_column->is_nullable()) {
                auto null_map = ColumnUInt8::create();
                null_map->insert_many_defaults(rows);
                variant_column =
                        ColumnNullable::create(std::move(variant_column), std::move(null_map));
            }
            columns[column_pos] = std::move(variant_column);
            continue;
        }

        MutableColumnPtr variant_column;
        RETURN_IF_ERROR(fill_variant_column(tablet_options.variant_columns[variant_pos], batch,
                                            variant_pos, &variant_column));
        columns[column_pos] = std::move(variant_column);
    }

    CHECK_EQ(column_pos, schema.num_columns());
    block->set_columns(std::move(columns));
    return Status::OK();
}

void collect_string_values_from_block(const TabletSchema& schema,
                                      const std::vector<uint32_t>& return_columns,
                                      const Block& block, IndexReadResult* result) {
    for (size_t pos = 0; pos < return_columns.size() && pos < block.columns(); ++pos) {
        const auto cid = return_columns[pos];
        if (cid >= schema.num_columns()) {
            continue;
        }
        const auto& tablet_column = schema.column(cid);
        if (!is_text_field_type(tablet_column.type())) {
            continue;
        }
        auto full_column = block.get_by_position(pos).column->convert_to_full_column_if_const();
        auto& values = result->string_values_by_uid[tablet_column.unique_id()];
        for (size_t row = 0; row < full_column->size(); ++row) {
            if (full_column->is_null_at(row)) {
                values.push_back(std::nullopt);
            } else {
                values.push_back(full_column->get_data_at(row).to_string());
            }
        }
    }
}

void collect_variant_values_from_block(const TabletSchema& schema,
                                       const std::vector<uint32_t>& return_columns,
                                       const Block& block, IndexReadResult* result) {
    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;

    for (size_t pos = 0; pos < return_columns.size() && pos < block.columns(); ++pos) {
        const auto cid = return_columns[pos];
        if (cid >= schema.num_columns()) {
            continue;
        }
        const auto& tablet_column = schema.column(cid);
        if (!tablet_column.is_variant_type()) {
            continue;
        }

        auto full_column = block.get_by_position(pos).column->convert_to_full_column_if_const();
        const auto* nullable_column = check_and_get_column<ColumnNullable>(*full_column);
        const auto* variant_column =
                nullable_column != nullptr
                        ? assert_cast<const ColumnVariant*>(&nullable_column->get_nested_column())
                        : assert_cast<const ColumnVariant*>(full_column.get());
        auto& values = result->variant_values_by_uid[tablet_column.unique_id()];
        for (size_t row = 0; row < full_column->size(); ++row) {
            if (full_column->is_null_at(row)) {
                values.push_back(std::nullopt);
                continue;
            }
            std::string value;
            variant_column->serialize_one_row_to_string(row, &value, options);
            values.emplace_back(std::move(value));
        }
    }
}

void collect_variant_column_layout(const ColumnMetaPB& column_meta, IndexSegmentLayout* layout) {
    if (column_meta.has_column_path_info()) {
        PathInData path;
        path.from_protobuf(column_meta.column_path_info());

        IndexColumnLayout column_layout;
        column_layout.parent_unique_id = column_meta.column_path_info().parrent_column_unique_id();
        column_layout.full_path = path.get_path();
        column_layout.relative_path = relative_variant_path(column_layout.full_path);
        column_layout.none_null_size = column_meta.none_null_size();
        column_layout.is_sparse_column =
                column_layout.full_path.find("__DORIS_VARIANT_SPARSE__") != std::string::npos;
        column_layout.is_doc_value_column =
                column_layout.full_path.find("__DORIS_VARIANT_DOC_VALUE__") != std::string::npos;
        if (column_meta.has_variant_statistics()) {
            for (const auto& [sub_path, non_null_size] :
                 column_meta.variant_statistics().sparse_column_non_null_size()) {
                column_layout.sparse_non_null_size[sub_path] = non_null_size;
            }
        }
        layout->variant_columns.push_back(std::move(column_layout));
    }

    for (const auto& child_meta : column_meta.children_columns()) {
        collect_variant_column_layout(child_meta, layout);
    }
}

Result<IndexSegmentLayout> probe_segment(const RowsetSharedPtr& rowset, int64_t segment_id) {
    auto seg = rowset->segment(rowset->rowset_meta()->position_of(segment_id));
    auto segment_path = seg.path();
    if (!segment_path.has_value()) {
        return ResultError(segment_path.error());
    }

    OlapReaderStatistics stats;
    std::shared_ptr<segment_v2::Segment> segment;
    auto status = segment_v2::Segment::open(
            rowset->rowset_meta()->fs(), segment_path.value(), rowset->rowset_meta()->tablet_id(),
            static_cast<uint32_t>(segment_id), rowset->rowset_id(), rowset->tablet_schema(),
            io::FileReaderOptions {}, &segment, seg.inverted_index_file_info(), &stats);
    if (!status.ok()) {
        return ResultError(status);
    }

    IndexSegmentLayout layout;
    layout.segment_id = segment_id;
    layout.num_rows = segment->num_rows();

    status = segment->traverse_column_meta_pbs([&](const ColumnMetaPB& column_meta) {
        collect_variant_column_layout(column_meta, &layout);
    });
    if (!status.ok()) {
        return ResultError(status);
    }
    return layout;
}

Result<IndexFileProbe> probe_index_files(const RowsetSharedPtr& rowset) {
    IndexFileProbe probe;
    probe.rowset_meta_entries = rowset->rowset_meta()->inverted_index_file_info().size();

    for (const auto& info : rowset->rowset_meta()->inverted_index_file_info()) {
        if (info.has_index_size()) {
            probe.rowset_meta_index_size += info.index_size();
        }
    }

    if (!rowset->tablet_schema()->has_inverted_index()) {
        return probe;
    }

    const auto file_names = rowset->get_index_file_names();
    probe.expected_files = file_names.size();
    for (const auto& file_name : file_names) {
        const std::string path = rowset->tablet_path() + "/" + file_name;
        bool exists = false;
        auto status = rowset->rowset_meta()->fs()->exists(path, &exists);
        if (!status.ok()) {
            return ResultError(status);
        }
        if (exists) {
            ++probe.existing_files;
        } else {
            probe.missing_files.push_back(path);
        }
    }
    return probe;
}

} // namespace

IndexSpec IndexSpec::column_index(int64_t index_id, std::string name, int32_t column_uid,
                                  std::map<std::string, std::string> properties) {
    IndexSpec spec;
    spec.index_id = index_id;
    spec.name = std::move(name);
    spec.column_uid = column_uid;
    spec.properties = std::move(properties);
    return spec;
}

IndexSpec IndexSpec::field_pattern_index(int64_t index_id, std::string name, int32_t column_uid,
                                         std::string field_pattern) {
    IndexSpec spec;
    spec.index_id = index_id;
    spec.name = std::move(name);
    spec.column_uid = column_uid;
    spec.properties["field_pattern"] = std::move(field_pattern);
    return spec;
}

IndexBatch IndexBatch::single_text(std::vector<std::string> values, int32_t first_key) {
    IndexBatch batch;
    batch.keys.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        batch.keys.push_back(first_key + static_cast<int32_t>(i));
    }
    batch.text_values_by_column.emplace_back(std::move(values));
    return batch;
}

IndexBatch IndexBatch::single_variant(std::vector<std::string> jsons, int32_t first_key) {
    IndexBatch batch;
    batch.keys.reserve(jsons.size());
    for (size_t i = 0; i < jsons.size(); ++i) {
        batch.keys.push_back(first_key + static_cast<int32_t>(i));
    }
    batch.variant_jsons_by_column.emplace_back(std::move(jsons));
    return batch;
}

IndexBatch IndexBatch::single_variant_column(ColumnPtr column, int32_t first_key) {
    IndexBatch batch;
    const auto rows = column->size();
    batch.keys.reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        batch.keys.push_back(first_key + static_cast<int32_t>(i));
    }
    batch.variant_columns_by_column.emplace_back(std::move(column));
    return batch;
}

size_t IndexBatch::num_rows() const {
    if (!keys.empty()) {
        return keys.size();
    }
    if (!text_values_by_column.empty()) {
        return text_values_by_column.front().size();
    }
    if (!variant_jsons_by_column.empty()) {
        return variant_jsons_by_column.front().size();
    }
    if (!variant_columns_by_column.empty()) {
        return variant_columns_by_column.front() ? variant_columns_by_column.front()->size() : 0;
    }
    return 0;
}

IndexDataSourceSpec IndexDataSourceSpec::inline_text(std::vector<std::string> values,
                                                     int32_t first_key) {
    IndexDataSourceSpec spec;
    spec.kind = IndexDataSourceKind::INLINE_TEXT_ROWS;
    spec.rows = std::move(values);
    spec.first_key = first_key;
    return spec;
}

IndexDataSourceSpec IndexDataSourceSpec::inline_variant(std::vector<std::string> jsons,
                                                        int32_t first_key) {
    IndexDataSourceSpec spec;
    spec.kind = IndexDataSourceKind::INLINE_VARIANT_ROWS;
    spec.rows = std::move(jsons);
    spec.first_key = first_key;
    return spec;
}

IndexDataSourceSpec IndexDataSourceSpec::variant_jsonl(std::string file_path, int32_t first_key,
                                                       size_t batch_size) {
    IndexDataSourceSpec spec;
    spec.kind = IndexDataSourceKind::VARIANT_JSONL_FILE;
    spec.file_path = std::move(file_path);
    spec.first_key = first_key;
    spec.batch_size = batch_size;
    return spec;
}

IndexStorageCaseBuilder::IndexStorageCaseBuilder(std::string name) {
    _case.name = std::move(name);
}

IndexStorageCaseBuilder& IndexStorageCaseBuilder::tablet_id(int64_t tablet_id) {
    _case.tablet_options.tablet_id = tablet_id;
    return *this;
}

IndexStorageCaseBuilder& IndexStorageCaseBuilder::text_column(TextColumnSpec column) {
    _case.tablet_options.text_columns.push_back(std::move(column));
    return *this;
}

IndexStorageCaseBuilder& IndexStorageCaseBuilder::variant_column(VariantColumnSpec column) {
    _case.tablet_options.variant_columns.push_back(std::move(column));
    return *this;
}

IndexStorageCaseBuilder& IndexStorageCaseBuilder::inverted_index(IndexSpec index) {
    _case.tablet_options.inverted_indexes.push_back(std::move(index));
    return *this;
}

IndexStorageCaseBuilder& IndexStorageCaseBuilder::rowset(int64_t version,
                                                         IndexDataSourceSpec data_source,
                                                         int64_t max_rows_per_segment) {
    IndexRowsetSpec rowset;
    rowset.version = version;
    rowset.max_rows_per_segment = max_rows_per_segment;
    rowset.data_sources.push_back(std::move(data_source));
    _case.rowsets.push_back(std::move(rowset));
    return *this;
}

IndexStorageCase IndexStorageCaseBuilder::build() const {
    return _case;
}

bool IndexSegmentLayout::contains_relative_path(const std::string& path) const {
    return std::any_of(variant_columns.begin(), variant_columns.end(),
                       [&](const auto& column) { return column.relative_path == path; });
}

bool IndexRowsetProbe::contains_relative_path(const std::string& path) const {
    return std::any_of(segments.begin(), segments.end(),
                       [&](const auto& segment) { return segment.contains_relative_path(path); });
}

bool IndexReadResult::inverted_index_used() const {
    return stats.rows_inverted_index_filtered > 0;
}

TabletSchemaPB build_tablet_schema_pb(const IndexTabletOptions& options) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_num_short_key_columns(options.include_key_column ? 1 : 0);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_compress_kind(COMPRESS_NONE);
    schema_pb.set_inverted_index_storage_format(options.index_storage_format);
    if (options.storage_page_size > 0) {
        schema_pb.set_storage_page_size(options.storage_page_size);
    }

    int32_t max_unique_id = 0;
    if (options.include_key_column) {
        init_key_column(schema_pb.add_column());
        max_unique_id = 1;
    }

    for (const auto& column : options.text_columns) {
        init_text_column(schema_pb.add_column(), column);
        max_unique_id = std::max(max_unique_id, column.unique_id);
    }

    for (const auto& column : options.variant_columns) {
        init_variant_column(schema_pb.add_column(), column);
        max_unique_id = std::max(max_unique_id, column.unique_id);
    }

    for (const auto& index : options.inverted_indexes) {
        append_index(&schema_pb, index);
    }

    schema_pb.set_next_column_unique_id(max_unique_id + 1);
    return schema_pb;
}

TabletSchemaSPtr build_tablet_schema(const IndexTabletOptions& options) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    auto schema_pb = build_tablet_schema_pb(options);
    tablet_schema->init_from_pb(schema_pb);
    append_predefined_paths(tablet_schema.get(), options);
    tablet_schema->set_storage_format(options.external_segment_meta
                                              ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                              : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    return tablet_schema;
}

TabletSchemaPB apply_schema_patch(const TabletSchema& base_schema, const IndexSchemaPatch& patch) {
    TabletSchemaPB schema_pb;
    base_schema.to_schema_pb(&schema_pb);

    auto existing_columns = schema_pb.column();
    schema_pb.clear_column();
    int32_t max_unique_id = 0;
    for (const auto& column : existing_columns) {
        if (patch.drop_column_uids.contains(column.unique_id())) {
            continue;
        }
        auto* dst = schema_pb.add_column();
        dst->CopyFrom(column);
        if (auto it = patch.modify_variant_columns.find(column.unique_id());
            it != patch.modify_variant_columns.end()) {
            init_variant_column(dst, it->second);
        }
        max_unique_id = std::max(max_unique_id, dst->unique_id());
    }

    for (const auto& column : patch.add_text_columns) {
        init_text_column(schema_pb.add_column(), column);
        max_unique_id = std::max(max_unique_id, column.unique_id);
    }

    for (const auto& column : patch.add_variant_columns) {
        init_variant_column(schema_pb.add_column(), column);
        max_unique_id = std::max(max_unique_id, column.unique_id);
    }

    auto existing_indexes = schema_pb.index();
    schema_pb.clear_index();
    for (const auto& index : existing_indexes) {
        if (patch.drop_index_ids.contains(index.index_id()) ||
            patch.drop_index_names.contains(index.index_name()) ||
            index_references_any_column(index, patch.drop_column_uids)) {
            continue;
        }
        schema_pb.add_index()->CopyFrom(index);
    }
    for (const auto& index : patch.add_inverted_indexes) {
        append_index(&schema_pb, index);
    }

    schema_pb.set_next_column_unique_id(
            std::max<int32_t>(schema_pb.next_column_unique_id(), max_unique_id + 1));
    return schema_pb;
}

TabletSchemaSPtr build_patched_tablet_schema(const TabletSchema& base_schema,
                                             const IndexSchemaPatch& patch) {
    auto schema = std::make_shared<TabletSchema>();
    auto schema_pb = apply_schema_patch(base_schema, patch);
    schema->init_from_pb(schema_pb);
    schema->set_storage_format(base_schema.storage_format());

    IndexTabletOptions path_options;
    path_options.variant_columns.clear();
    for (const auto& column : patch.add_variant_columns) {
        if (!column.predefined_paths.empty()) {
            path_options.variant_columns.push_back(column);
        }
    }
    for (const auto& [_, column] : patch.modify_variant_columns) {
        if (!column.predefined_paths.empty()) {
            path_options.variant_columns.push_back(column);
        }
    }
    append_predefined_paths(schema.get(), path_options);
    return schema;
}

TabletSchemaSPtr build_schema_with_variant_path_column(const TabletSchema& base_schema,
                                                       int32_t parent_unique_id,
                                                       std::string relative_path, FieldType type) {
    auto schema = std::make_shared<TabletSchema>();
    TabletSchemaPB schema_pb;
    base_schema.to_schema_pb(&schema_pb);
    schema->init_from_pb(schema_pb);
    schema->set_storage_format(base_schema.storage_format());

    const auto& parent_column = schema->column_by_uid(parent_unique_id);
    const std::string full_path = relative_path.find('.') == std::string::npos
                                          ? parent_column.name_lower_case() + "." + relative_path
                                          : std::move(relative_path);

    TabletColumn path_column;
    path_column.set_unique_id(-1);
    path_column.set_name(full_path);
    path_column.set_type(type);
    path_column.set_parent_unique_id(parent_column.unique_id());
    path_column.set_path_info(PathInData(full_path));
    path_column.set_aggregation_method(parent_column.aggregation());
    path_column.set_variant_max_subcolumns_count(parent_column.variant_max_subcolumns_count());
    path_column.set_variant_max_sparse_column_statistics_size(
            parent_column.variant_max_sparse_column_statistics_size());
    path_column.set_variant_sparse_hash_shard_count(
            parent_column.variant_sparse_hash_shard_count());
    path_column.set_variant_enable_doc_mode(parent_column.variant_enable_doc_mode());
    path_column.set_variant_doc_materialization_min_rows(
            parent_column.variant_doc_materialization_min_rows());
    path_column.set_variant_doc_hash_shard_count(parent_column.variant_doc_hash_shard_count());
    path_column.set_variant_enable_nested_group(parent_column.variant_enable_nested_group());
    path_column.set_is_nullable(true);
    if (type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2) {
        path_column.set_frac(0);
    }
    schema->append_column(std::move(path_column));
    return schema;
}

void expect_index_filter_stats(const IndexReadResult& result, int64_t expected_filtered_rows) {
    EXPECT_EQ(result.stats.rows_inverted_index_filtered, expected_filtered_rows)
            << describe_filter_stats(result);
}

void expect_raw_rows_read(const IndexReadResult& result, int64_t expected_raw_rows_read) {
    EXPECT_EQ(result.stats.raw_rows_read, expected_raw_rows_read) << describe_filter_stats(result);
}

void expect_segment_pruned(const IndexReadResult& result, int64_t expected_filtered_segments) {
    EXPECT_EQ(result.stats.filtered_segment_number, expected_filtered_segments)
            << describe_filter_stats(result);
}

void expect_zone_map_filtered(const IndexReadResult& result, int64_t expected_filtered_rows) {
    EXPECT_EQ(result.stats.rows_stats_filtered, expected_filtered_rows)
            << describe_filter_stats(result);
}

void expect_bloom_filter_filtered(const IndexReadResult& result, int64_t expected_filtered_rows) {
    EXPECT_EQ(result.stats.rows_bf_filtered, expected_filtered_rows)
            << describe_filter_stats(result);
}

void expect_inverted_index_used(const IndexReadResult& result) {
    EXPECT_TRUE(result.inverted_index_used())
            << "expected inverted index to filter rows, rows_inverted_index_filtered="
            << result.stats.rows_inverted_index_filtered << describe_filter_stats(result);
}

void expect_index_files(const IndexRowsetProbe& probe, bool expected_present) {
    if (expected_present) {
        EXPECT_GT(probe.index_files.expected_files, 0);
        EXPECT_EQ(probe.index_files.expected_files, probe.index_files.existing_files);
        EXPECT_TRUE(probe.index_files.missing_files.empty());
    } else {
        EXPECT_EQ(probe.index_files.expected_files, 0);
        EXPECT_TRUE(probe.index_files.missing_files.empty());
    }
}

std::string dump_schema_paths(const TabletSchema& schema) {
    std::ostringstream out;
    for (int32_t i = 0; i < schema.num_columns(); ++i) {
        const auto& column = schema.column(i);
        out << i << ": uid=" << column.unique_id() << " name=" << column.name()
            << " type=" << TabletColumn::get_string_by_field_type(column.type());
        if (column.has_path_info()) {
            out << " path=" << column.path_info_ptr()->get_path();
        }
        out << '\n';
    }
    return out.str();
}

ScopedDebugPoint::ScopedDebugPoint(std::string name, std::map<std::string, std::string> params)
        : _name(std::move(name)), _enable_debug_points(config::enable_debug_points) {
    config::enable_debug_points = true;
    _debug_point = std::make_shared<DebugPoint>();
    _debug_point->params = std::move(params);
    _debug_point->execute_limit = std::numeric_limits<int64_t>::max();
    DebugPoints::instance()->remove(_name);
    DebugPoints::instance()->add(_name, _debug_point);
}

ScopedDebugPoint::~ScopedDebugPoint() {
    DebugPoints::instance()->remove(_name);
    config::enable_debug_points = _enable_debug_points;
}

int64_t ScopedDebugPoint::execute_num() const {
    return _debug_point == nullptr ? 0 : _debug_point->execute_num.load();
}

IndexStorageTestFixture::~IndexStorageTestFixture() = default;

void IndexStorageTestFixture::SetUp() {
    const auto* test_info = testing::UnitTest::GetInstance()->current_test_info();
    const std::string test_name =
            sanitize_test_name(std::string(test_info->test_suite_name()) + "_" + test_info->name());
    const std::string cwd = current_working_dir();
    _test_dir = cwd + "/ut_dir/" + test_name;
    _tmp_dir = _test_dir + "/tmp";

    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_test_dir).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_test_dir).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tmp_dir).ok());

    std::vector<StorePath> tmp_paths;
    tmp_paths.emplace_back(_tmp_dir, 1024000000);
    auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(tmp_paths);
    auto status = tmp_file_dirs->init();
    ASSERT_TRUE(status.ok()) << status.to_json();
    ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

    EngineOptions options;
    auto engine = std::make_unique<StorageEngine>(options);
    _engine_ref = engine.get();
    _data_dir = std::make_unique<DataDir>(*_engine_ref, _test_dir);
    status = _data_dir->init(true);
    ASSERT_TRUE(status.ok()) << status.to_json();
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

    constexpr int64_t inverted_index_cache_limit = 1024 * 1024 * 1024;
    _inverted_index_searcher_cache = std::unique_ptr<segment_v2::InvertedIndexSearcherCache>(
            segment_v2::InvertedIndexSearcherCache::create_global_instance(
                    inverted_index_cache_limit, 1));
    _inverted_index_query_cache = std::unique_ptr<segment_v2::InvertedIndexQueryCache>(
            segment_v2::InvertedIndexQueryCache::create_global_cache(inverted_index_cache_limit,
                                                                     1));
    ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_inverted_index_searcher_cache.get());
    ExecEnv::GetInstance()->set_inverted_index_query_cache(_inverted_index_query_cache.get());
}

void IndexStorageTestFixture::TearDown() {
    _tablet.reset();
    _tablet_schema.reset();
    _data_dir.reset();
    _engine_ref = nullptr;
    ExecEnv::GetInstance()->set_storage_engine(nullptr);
    ExecEnv::GetInstance()->set_inverted_index_searcher_cache(nullptr);
    ExecEnv::GetInstance()->set_inverted_index_query_cache(nullptr);
    _inverted_index_searcher_cache.reset();
    _inverted_index_query_cache.reset();
    static_cast<void>(io::global_local_filesystem()->delete_directory(_test_dir));
}

Status IndexStorageTestFixture::create_tablet(const IndexTabletOptions& options) {
    _tablet_schema = build_tablet_schema(options);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = options.tablet_id;
    tablet_meta->set_tablet_uid(TabletUid(options.tablet_id, options.tablet_id + 1));
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    RETURN_IF_ERROR(_tablet->init());
    static_cast<void>(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()));
    return io::global_local_filesystem()->create_directory(_tablet->tablet_path());
}

Result<RowsetSharedPtr> IndexStorageTestFixture::write_rowset(const IndexRowsetSpec& spec) {
    if (_tablet == nullptr || _tablet_schema == nullptr) {
        return ResultError(Status::InvalidArgument("tablet is not initialized"));
    }
    auto batches_result = materialize_batches(spec);
    if (!batches_result.has_value()) {
        return ResultError(batches_result.error());
    }
    auto batches = std::move(batches_result).value();
    if (batches.empty()) {
        return ResultError(Status::InvalidArgument("rowset spec has no batches"));
    }

    RowsetWriterContext context;
    RowsetId rowset_id;
    rowset_id.init(spec.version + 1000);
    context.rowset_id = rowset_id;
    context.rowset_type = BETA_ROWSET;
    context.data_dir = _data_dir.get();
    context.rowset_state = VISIBLE;
    context.tablet_schema = _tablet_schema;
    context.tablet_path = _tablet->tablet_path();
    context.tablet_id = _tablet->tablet_id();
    context.tablet_uid = _tablet->tablet_uid();
    context.tablet = _tablet;
    context.version = Version(spec.version, spec.version);
    context.segments_overlap = NONOVERLAPPING;
    context.max_rows_per_segment = spec.max_rows_per_segment;
    context.write_type = spec.write_type;

    auto writer_result = RowsetFactory::create_rowset_writer(*_engine_ref, context, false);
    if (!writer_result.has_value()) {
        return ResultError(writer_result.error());
    }
    auto rowset_writer = std::move(writer_result).value();

    int32_t next_key = 0;
    IndexTabletOptions tablet_options;
    tablet_options.include_key_column = _tablet_schema->num_key_columns() > 0;
    tablet_options.text_columns.clear();
    tablet_options.variant_columns.clear();
    for (int i = 0; i < _tablet_schema->num_columns(); ++i) {
        const auto& column = _tablet_schema->column(i);
        if (column.is_key()) {
            continue;
        }
        if (is_text_field_type(column.type())) {
            TextColumnSpec column_spec;
            column_spec.unique_id = column.unique_id();
            column_spec.name = column.name();
            column_spec.type = column.type();
            column_spec.nullable = column.is_nullable();
            tablet_options.text_columns.push_back(std::move(column_spec));
            continue;
        }
        if (!column.is_variant_type()) {
            continue;
        }
        VariantColumnSpec column_spec;
        column_spec.unique_id = column.unique_id();
        column_spec.name = column.name();
        column_spec.nullable = column.is_nullable();
        column_spec.is_bf_column = column.is_bf_column();
        column_spec.max_subcolumns_count = column.variant_max_subcolumns_count();
        column_spec.max_sparse_column_statistics_size =
                column.variant_max_sparse_column_statistics_size();
        column_spec.sparse_hash_shard_count = column.variant_sparse_hash_shard_count();
        column_spec.enable_doc_mode = column.variant_enable_doc_mode();
        tablet_options.variant_columns.push_back(std::move(column_spec));
    }

    for (const auto& batch : batches) {
        if (batch.num_rows() == 0) {
            return ResultError(Status::InvalidArgument("variant json batch is empty"));
        }
        if (!batch.keys.empty() && batch.keys.size() != batch.num_rows()) {
            return ResultError(Status::InvalidArgument("variant json batch key count mismatch"));
        }
        if (batch.text_values_by_column.size() != tablet_options.text_columns.size()) {
            return ResultError(Status::InvalidArgument(
                    "text batch column count mismatch: expected={}, actual={}",
                    tablet_options.text_columns.size(), batch.text_values_by_column.size()));
        }
        if (!batch.variant_jsons_by_column.empty() && !batch.variant_columns_by_column.empty()) {
            return ResultError(Status::InvalidArgument(
                    "variant batch cannot mix json values and direct columns"));
        }
        if (batch.variant_jsons_by_column.empty() && batch.variant_columns_by_column.empty() &&
            !tablet_options.variant_columns.empty()) {
            return ResultError(
                    Status::InvalidArgument("variant batch does not contain variant values"));
        }
        if (!batch.variant_jsons_by_column.empty() &&
            batch.variant_jsons_by_column.size() != tablet_options.variant_columns.size()) {
            return ResultError(Status::InvalidArgument(
                    "variant json batch column count mismatch: expected={}, actual={}",
                    tablet_options.variant_columns.size(), batch.variant_jsons_by_column.size()));
        }
        if (!batch.variant_columns_by_column.empty() &&
            batch.variant_columns_by_column.size() != tablet_options.variant_columns.size()) {
            return ResultError(Status::InvalidArgument(
                    "variant direct column count mismatch: expected={}, actual={}",
                    tablet_options.variant_columns.size(), batch.variant_columns_by_column.size()));
        }

        Block block = _tablet_schema->create_block();
        RETURN_RESULT_IF_ERROR(
                fill_block(*_tablet_schema, tablet_options, batch, &next_key, &block));
        RETURN_RESULT_IF_ERROR(rowset_writer->add_block(&block));
        RETURN_RESULT_IF_ERROR(rowset_writer->flush());
    }

    RowsetSharedPtr rowset;
    RETURN_RESULT_IF_ERROR(rowset_writer->build(rowset));
    if (spec.add_to_tablet) {
        RETURN_RESULT_IF_ERROR(_tablet->add_rowset(rowset));
    }
    return rowset;
}

Result<std::vector<RowsetSharedPtr>> IndexStorageTestFixture::write_rowsets(
        const std::vector<IndexRowsetSpec>& specs) {
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(specs.size());
    for (const auto& spec : specs) {
        auto rowset = write_rowset(spec);
        if (!rowset.has_value()) {
            return ResultError(rowset.error());
        }
        rowsets.push_back(std::move(rowset).value());
    }
    return rowsets;
}

Result<IndexReadResult> IndexStorageTestFixture::read_rowsets(
        const std::vector<RowsetSharedPtr>& rowsets, IndexReadOptions options) {
    if (_tablet_schema == nullptr) {
        return ResultError(Status::InvalidArgument("tablet schema is not initialized"));
    }

    IndexReadResult result;
    std::vector<uint32_t> return_columns = std::move(options.return_columns);
    if (return_columns.empty()) {
        return_columns.resize(_tablet_schema->num_columns());
        std::iota(return_columns.begin(), return_columns.end(), 0);
    }

    RuntimeState runtime_state;
    runtime_state.set_exec_env(ExecEnv::GetInstance());
    runtime_state.set_query_options(make_read_query_options(options));
    TupleDescriptor tuple_desc;
    RowDescriptor row_desc(&tuple_desc);
    for (const auto& expr_ctx : options.common_expr_ctxs_push_down) {
        RETURN_RESULT_IF_ERROR(expr_ctx->prepare(&runtime_state, row_desc));
        RETURN_RESULT_IF_ERROR(expr_ctx->open(&runtime_state));
    }

    for (const auto& rowset : rowsets) {
        RowsetReaderSharedPtr reader;
        RETURN_RESULT_IF_ERROR(rowset->create_reader(&reader));

        RowsetReaderContext context;
        context.reader_type = options.reader_type;
        context.tablet_schema = _tablet_schema;
        context.need_ordered_result = options.need_ordered_result;
        context.return_columns = &return_columns;
        context.predicates = &options.predicates;
        context.stats = &result.stats;
        context.target_cast_type_for_variants = options.target_cast_type_for_variants;
        context.all_access_paths = options.all_access_paths;
        context.predicate_access_paths = options.predicate_access_paths;
        context.common_expr_ctxs_push_down = options.common_expr_ctxs_push_down;
        context.push_down_agg_type_opt = options.push_down_agg_type_opt;
        context.runtime_state = &runtime_state;
        RETURN_RESULT_IF_ERROR(reader->init(&context));

        while (true) {
            Block block = _tablet_schema->create_block_by_cids(return_columns);
            auto status = reader->next_batch(&block);
            if (status.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            RETURN_RESULT_IF_ERROR(status);
            if (options.collect_string_values) {
                collect_string_values_from_block(*_tablet_schema, return_columns, block, &result);
            }
            if (options.collect_variant_values) {
                collect_variant_values_from_block(*_tablet_schema, return_columns, block, &result);
            }
            result.rows_read += block.rows();
        }
    }

    return result;
}

Result<RowsetSharedPtr> IndexStorageTestFixture::compact_rowsets(
        IndexCompactionKind kind, const std::vector<RowsetSharedPtr>& rowsets) {
    if (_tablet == nullptr) {
        return ResultError(Status::InvalidArgument("tablet is not initialized"));
    }

    switch (kind) {
    case IndexCompactionKind::CUMULATIVE: {
        CumulativeCompaction compaction(*_engine_ref, _tablet);
        compaction._input_rowsets = rowsets;
        RETURN_RESULT_IF_ERROR(compaction.CompactionMixin::execute_compact());
        return compaction._output_rowset;
    }
    case IndexCompactionKind::FULL: {
        FullCompaction compaction(*_engine_ref, _tablet);
        compaction._input_rowsets = rowsets;
        RETURN_RESULT_IF_ERROR(compaction.CompactionMixin::execute_compact());
        return compaction._output_rowset;
    }
    }

    return ResultError(Status::InvalidArgument("unknown variant compaction kind"));
}

Result<RowsetSharedPtr> IndexStorageTestFixture::compact_rowsets_and_reload(
        IndexCompactionKind kind, const std::vector<RowsetSharedPtr>& rowsets) {
    auto compacted = compact_rowsets(kind, rowsets);
    if (!compacted.has_value()) {
        return ResultError(compacted.error());
    }
    return reload_rowset(compacted.value());
}

Result<RowsetSharedPtr> IndexStorageTestFixture::reload_rowset(const RowsetSharedPtr& rowset) {
    if (rowset == nullptr) {
        return ResultError(Status::InvalidArgument("rowset is null"));
    }

    RowsetSharedPtr reloaded;
    RETURN_RESULT_IF_ERROR(RowsetFactory::create_rowset(
            rowset->tablet_schema(), rowset->tablet_path(), rowset->rowset_meta(), &reloaded));
    return reloaded;
}

Result<std::vector<RowsetSharedPtr>> IndexStorageTestFixture::reload_rowsets(
        const std::vector<RowsetSharedPtr>& rowsets) {
    std::vector<RowsetSharedPtr> reloaded_rowsets;
    reloaded_rowsets.reserve(rowsets.size());
    for (const auto& rowset : rowsets) {
        auto reloaded = reload_rowset(rowset);
        if (!reloaded.has_value()) {
            return ResultError(reloaded.error());
        }
        reloaded_rowsets.push_back(std::move(reloaded).value());
    }
    if (!reloaded_rowsets.empty()) {
        _tablet_schema = reloaded_rowsets.front()->tablet_schema();
    }
    return reloaded_rowsets;
}

Result<std::vector<RowsetSharedPtr>> IndexStorageTestFixture::inject_reader_schema_for_rowsets(
        const std::vector<RowsetSharedPtr>& rowsets, TabletSchemaSPtr schema) {
    if (rowsets.empty()) {
        return ResultError(Status::InvalidArgument("rowsets are empty"));
    }
    if (schema == nullptr) {
        return ResultError(Status::InvalidArgument("tablet schema is null"));
    }

    if (_tablet != nullptr) {
        _tablet->tablet_meta()->mutable_tablet_schema()->copy_from(*schema);
        _tablet_schema = _tablet->tablet_meta()->tablet_schema();
    } else {
        _tablet_schema = std::move(schema);
    }

    std::vector<RowsetSharedPtr> reloaded_rowsets;
    reloaded_rowsets.reserve(rowsets.size());
    for (const auto& rowset : rowsets) {
        if (rowset == nullptr) {
            return ResultError(Status::InvalidArgument("rowset is null"));
        }
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        if (!rowset_meta->init(rowset->rowset_meta().get())) {
            return ResultError(Status::InternalError("failed to clone rowset meta"));
        }
        rowset_meta->set_tablet_schema(_tablet_schema);

        RowsetSharedPtr reloaded;
        RETURN_RESULT_IF_ERROR(RowsetFactory::create_rowset(_tablet_schema, rowset->tablet_path(),
                                                            rowset_meta, &reloaded));
        reloaded_rowsets.push_back(std::move(reloaded));
    }
    if (_tablet != nullptr) {
        std::vector<RowsetSharedPtr> to_add = reloaded_rowsets;
        std::vector<RowsetSharedPtr> to_delete = rowsets;
        RETURN_RESULT_IF_ERROR(_tablet->modify_rowsets(to_add, to_delete, true));
    }
    return reloaded_rowsets;
}

Result<std::vector<RowsetSharedPtr>> IndexStorageTestFixture::build_inverted_indexes(
        const std::vector<IndexSpec>& indexes) {
    if (_tablet == nullptr || _tablet_schema == nullptr) {
        return ResultError(Status::InvalidArgument("tablet is not initialized"));
    }
    if (indexes.empty()) {
        return ResultError(Status::InvalidArgument("index builder requires at least one index"));
    }

    std::set<int64_t> index_ids;
    std::vector<TOlapTableIndex> alter_indexes;
    alter_indexes.reserve(indexes.size());
    for (const auto& index : indexes) {
        index_ids.insert(index.index_id);
        alter_indexes.push_back(to_alter_index(*_tablet_schema, index));
    }

    auto input_rowsets = _tablet->pick_candidate_rowsets_to_build_inverted_index(index_ids, false);
    if (input_rowsets.empty()) {
        return std::vector<RowsetSharedPtr> {};
    }

    std::vector<Version> versions;
    versions.reserve(input_rowsets.size());
    for (const auto& rowset : input_rowsets) {
        versions.push_back(rowset->version());
    }

    IndexBuilder builder(*_engine_ref, _tablet, {}, alter_indexes, false);
    RETURN_RESULT_IF_ERROR(builder.init());
    RETURN_RESULT_IF_ERROR(builder.do_build_inverted_index());

    auto rowsets = current_rowsets_for_versions(*_tablet, versions);
    if (rowsets.has_value() && !rowsets->empty()) {
        _tablet_schema = rowsets->front()->tablet_schema();
    }
    return rowsets;
}

Result<std::vector<RowsetSharedPtr>> IndexStorageTestFixture::build_inverted_indexes_and_reload(
        const std::vector<IndexSpec>& indexes) {
    auto rowsets = build_inverted_indexes(indexes);
    if (!rowsets.has_value()) {
        return ResultError(rowsets.error());
    }
    return reload_rowsets(rowsets.value());
}

Result<std::vector<RowsetSharedPtr>> IndexStorageTestFixture::drop_inverted_indexes(
        const std::vector<IndexSpec>& indexes) {
    if (_tablet == nullptr || _tablet_schema == nullptr) {
        return ResultError(Status::InvalidArgument("tablet is not initialized"));
    }
    if (indexes.empty()) {
        return ResultError(Status::InvalidArgument("index builder requires at least one index"));
    }

    std::set<int64_t> index_ids;
    std::vector<TOlapTableIndex> alter_indexes;
    alter_indexes.reserve(indexes.size());
    for (const auto& index : indexes) {
        index_ids.insert(index.index_id);
        alter_indexes.push_back(to_alter_index(*_tablet_schema, index));
    }

    auto input_rowsets = _tablet->pick_candidate_rowsets_to_build_inverted_index(index_ids, true);
    if (input_rowsets.empty()) {
        return std::vector<RowsetSharedPtr> {};
    }

    std::vector<Version> versions;
    versions.reserve(input_rowsets.size());
    for (const auto& rowset : input_rowsets) {
        versions.push_back(rowset->version());
    }

    IndexBuilder builder(*_engine_ref, _tablet, {}, alter_indexes, true);
    RETURN_RESULT_IF_ERROR(builder.init());
    RETURN_RESULT_IF_ERROR(builder.do_build_inverted_index());

    auto rowsets = current_rowsets_for_versions(*_tablet, versions);
    if (rowsets.has_value() && !rowsets->empty()) {
        _tablet_schema = rowsets->front()->tablet_schema();
    }
    return rowsets;
}

Result<std::vector<RowsetSharedPtr>> IndexStorageTestFixture::drop_inverted_indexes_and_reload(
        const std::vector<IndexSpec>& indexes) {
    auto rowsets = drop_inverted_indexes(indexes);
    if (!rowsets.has_value()) {
        return ResultError(rowsets.error());
    }
    return reload_rowsets(rowsets.value());
}

Result<IndexRowsetProbe> IndexStorageTestFixture::probe_rowset(const RowsetSharedPtr& rowset) {
    IndexRowsetProbe probe;
    probe.num_rows = rowset->num_rows();
    probe.num_segments = rowset->num_segments();

    auto index_files = probe_index_files(rowset);
    if (!index_files.has_value()) {
        return ResultError(index_files.error());
    }
    probe.index_files = std::move(index_files).value();

    for (auto seg : rowset->segments()) {
        auto segment_probe = probe_segment(rowset, seg.id());
        if (!segment_probe.has_value()) {
            return ResultError(segment_probe.error());
        }
        probe.segments.push_back(std::move(segment_probe).value());
    }
    return probe;
}

void IndexStorageTestFixture::use_rowset_schema(const RowsetSharedPtr& rowset) {
    CHECK(rowset != nullptr);
    _tablet_schema = rowset->tablet_schema();
}

Result<std::vector<RowsetSharedPtr>> IndexStorageTestFixture::rowsets_with_variant_extended_schema(
        const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return ResultError(Status::InvalidArgument("rowsets are empty"));
    }
    if (rowsets.front() == nullptr || rowsets.front()->tablet_schema() == nullptr) {
        return ResultError(Status::InvalidArgument("rowset schema is not initialized"));
    }

    auto schema = variant_util::VariantCompactionUtil::calculate_variant_extended_schema(
            rowsets, rowsets.front()->tablet_schema());
    if (schema == nullptr) {
        return ResultError(Status::InternalError("failed to calculate variant extended schema"));
    }
    return inject_reader_schema_for_rowsets(rowsets, std::move(schema));
}

int32_t IndexStorageTestFixture::column_id_by_path(const std::string& path) const {
    if (_tablet_schema == nullptr) {
        return -1;
    }
    const int32_t column_id = _tablet_schema->field_index(PathInData(path));
    if (column_id >= 0) {
        return column_id;
    }

    const std::string relative_query_path = relative_variant_path(path);
    for (int32_t i = 0; i < _tablet_schema->num_columns(); ++i) {
        const auto& column = _tablet_schema->column(i);
        if (!column.has_path_info()) {
            continue;
        }
        auto relative_path = column.path_info_ptr()->copy_pop_front().get_path();
        if (relative_path == path || relative_path == relative_query_path) {
            return i;
        }
    }
    return -1;
}

#undef RETURN_RESULT_IF_ERROR

} // namespace doris::index_storage_test
