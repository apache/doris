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

#include <benchmark/benchmark.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "storage/cache/page_cache.h"
#include "storage/compaction/cumulative_compaction.h"
#include "storage/data_dir.h"
#include "storage/index/index_writer.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_column_object_pool.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_cache.h"

namespace doris::variant_segment_benchmark {
namespace {

// P0 workload: 200 top-level BIGINT paths, with one hot path and 29 additional paths per row.
// Ingest cases parse the same canonical JSON before VerticalSegmentWriter. Read cases use the same
// V1-written physical segment and only switch the requested V1/V2 output representation. Input
// generation, buffer destruction, warmup, checksum, physical-layout validation, and route
// validation are paused. Scan-and-rewrite cases query key plus whole Variant from that prevalidated
// source, then time scan/writer initialization, read, append, and destination finalize; destination
// validation is paused. Ingest rotates VerticalSegmentWriter at
// DORIS_VARIANT_BENCHMARK_ROWS_PER_SEGMENT; read cases retain their historical single-segment
// semantics.
constexpr uint32_t DEFAULT_ROWS = 1'000'000;
constexpr uint32_t DEFAULT_ROWS_PER_SEGMENT = 1'000'000;
constexpr uint32_t BATCH_ROWS = 4'096;
constexpr uint32_t WHOLE_VALIDATION_STRIDE = BATCH_ROWS;
constexpr uint32_t COMPACTION_INPUT_ROWSETS = 10;
constexpr uint32_t CANDIDATE_PATHS = 200;
constexpr uint32_t COLD_PATHS = CANDIDATE_PATHS - 2;
constexpr uint32_t FIELDS_PER_ROW = 30;
constexpr uint32_t SPARSE_HIT_PERIOD = 17;
constexpr int32_t KEY_UID = 0;
constexpr int32_t ROOT_UID = 1;
constexpr uint32_t BUCKETS = 16;
constexpr std::string_view ROOT_NAME = "v";
constexpr std::string_view HOT_PATH = "hot";
constexpr std::string_view SPARSE_PATH = "sparse_target";
constexpr std::string_view MISSING_PATH = "global_missing";
constexpr uint64_t FNV_OFFSET = 1469598103934665603ULL;
constexpr uint64_t FNV_PRIME = 1099511628211ULL;

enum class VariantVersion : uint8_t { V1, V2 };
enum class VariantLayout : uint8_t { SPARSE16, DOC16, FULL };
enum class ReadTarget : uint8_t { WHOLE, MATERIALIZED, SPARSE, MISSING };

struct LayoutConfig {
    std::string_view name;
    int32_t max_subcolumns;
    bool doc_mode;
};

struct LayoutCounts {
    uint32_t materialized = 0;
    uint32_t sparse = 0;
    uint32_t doc = 0;
};

void count_layout_columns(const ColumnMetaPB& meta, LayoutCounts* counts) {
    DORIS_CHECK(counts != nullptr);
    if (meta.has_column_path_info()) {
        PathInData path;
        path.from_protobuf(meta.column_path_info());
        const std::string relative = path.copy_pop_front().get_path();
        if (!relative.empty()) {
            if (relative.find(SPARSE_COLUMN_PATH) != std::string::npos) {
                ++counts->sparse;
            } else if (relative.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) {
                ++counts->doc;
            } else {
                ++counts->materialized;
            }
        }
    }
    for (const ColumnMetaPB& child : meta.children_columns()) {
        count_layout_columns(child, counts);
    }
}

struct WriteResult {
    uint64_t segment_bytes = 0;
    uint64_t index_bytes = 0;
    int64_t init_ns = 0;
    int64_t parse_ns = 0;
    int64_t append_ns = 0;
    int64_t finalize_ns = 0;
};

struct PreparedSegment {
    TabletSchemaSPtr schema;
    segment_v2::SegmentSharedPtr segment;
    std::string path;
    uint64_t segment_bytes = 0;
    LayoutCounts counts;
};

struct PreparedScan {
    PreparedSegment* fixture = nullptr;
    TabletSchemaSPtr query_schema;
    ReadSchemaSPtr scan_schema;
    ColumnId output_column_id = 0;
    ReadTarget target = ReadTarget::WHOLE;
};

struct ScanResult {
    uint64_t checksum = FNV_OFFSET;
    uint64_t output_bytes = 0;
    uint32_t rows = 0;
    uint32_t hits = 0;
    OlapReaderStatistics statistics;
};

struct RewriteResult {
    uint64_t source_segment_bytes = 0;
    uint64_t read_output_bytes = 0;
    uint64_t destination_segment_bytes = 0;
    uint64_t destination_index_bytes = 0;
    uint32_t rows = 0;
    int64_t scan_init_ns = 0;
    int64_t writer_init_ns = 0;
    int64_t read_ns = 0;
    int64_t append_ns = 0;
    int64_t finalize_ns = 0;
    OlapReaderStatistics statistics;
};

struct CompactionResult {
    uint64_t input_json_bytes = 0;
    uint64_t input_disk_bytes = 0;
    uint64_t output_disk_bytes = 0;
    uint32_t output_segments = 0;
    LayoutCounts output_layout;
};

std::string_view version_name(VariantVersion version) {
    return version == VariantVersion::V1 ? "V1" : "V2";
}

LayoutConfig layout_config(VariantLayout layout) {
    switch (layout) {
    case VariantLayout::SPARSE16:
        return {.name = "Sparse16", .max_subcolumns = 1, .doc_mode = false};
    case VariantLayout::DOC16:
        return {.name = "Doc16", .max_subcolumns = 1, .doc_mode = true};
    case VariantLayout::FULL:
        return {.name = "Full", .max_subcolumns = 0, .doc_mode = false};
    }
    __builtin_unreachable();
}

Status validate_sparse16_layout(const LayoutCounts& counts) {
    if (counts.materialized != 1 || counts.sparse != BUCKETS || counts.doc != 0) {
        return Status::InternalError("Sparse16 layout is materialized={}, sparse={}, doc={}",
                                     counts.materialized, counts.sparse, counts.doc);
    }
    return Status::OK();
}

void ensure_variant_compaction_runtime() {
    ExecEnv* env = ExecEnv::GetInstance();
    if (env->get_cache_manager() == nullptr) {
        env->set_cache_manager(CacheManager::create_global_instance());
    }
    if (env->get_storage_page_cache() == nullptr) {
        constexpr size_t CACHE_CAPACITY = 512UL << 20;
        env->set_storage_page_cache(StoragePageCache::create_global_cache(CACHE_CAPACITY, 10, 0));
    }
    if (env->segment_loader() == nullptr) {
        static const std::unique_ptr<SegmentLoader> loader =
                std::make_unique<SegmentLoader>(512UL << 20, 4'096);
        env->set_segment_loader(loader.get());
    }
    if (env->get_tablet_schema_cache() == nullptr) {
        env->set_tablet_schema_cache(TabletSchemaCache::create_global_schema_cache(
                config::tablet_schema_cache_capacity));
    }
    if (env->get_tablet_column_object_pool() == nullptr) {
        env->set_tablet_column_object_pool(TabletColumnObjectPool::create_global_column_cache(
                config::tablet_schema_cache_capacity));
    }
}

std::string_view target_name(ReadTarget target) {
    switch (target) {
    case ReadTarget::WHOLE:
        return "Whole";
    case ReadTarget::MATERIALIZED:
        return "Materialized100pct";
    case ReadTarget::SPARSE:
        return "Sparse6pct";
    case ReadTarget::MISSING:
        return "GlobalMiss";
    }
    __builtin_unreachable();
}

uint32_t configured_rows() {
    static const uint32_t rows = [] {
        const char* value = std::getenv("DORIS_VARIANT_BENCHMARK_ROWS");
        if (value == nullptr) {
            return DEFAULT_ROWS;
        }
        uint64_t parsed = 0;
        const std::string_view text(value);
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), parsed);
        DORIS_CHECK(error == std::errc {} && end == text.data() + text.size());
        DORIS_CHECK_GT(parsed, 0);
        DORIS_CHECK_LE(parsed, std::numeric_limits<uint32_t>::max());
        return static_cast<uint32_t>(parsed);
    }();
    return rows;
}

uint32_t configured_rows_per_segment() {
    static const uint32_t rows = [] {
        const char* value = std::getenv("DORIS_VARIANT_BENCHMARK_ROWS_PER_SEGMENT");
        if (value == nullptr) {
            return DEFAULT_ROWS_PER_SEGMENT;
        }
        uint64_t parsed = 0;
        const std::string_view text(value);
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), parsed);
        DORIS_CHECK(error == std::errc {} && end == text.data() + text.size());
        DORIS_CHECK_GT(parsed, 0);
        DORIS_CHECK_LE(parsed, std::numeric_limits<uint32_t>::max());
        return static_cast<uint32_t>(parsed);
    }();
    return rows;
}

std::string benchmark_root() {
    const char* value = std::getenv("DORIS_VARIANT_BENCHMARK_ROOT");
    return value == nullptr ? "/tmp" : value;
}

uint64_t update_checksum(uint64_t checksum, std::string_view value) {
    for (const unsigned char byte : value) {
        checksum ^= byte;
        checksum *= FNV_PRIME;
    }
    checksum ^= 0xff;
    checksum *= FNV_PRIME;
    return checksum;
}

int64_t elapsed_ns(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() -
                                                                start)
            .count();
}

class VariantSegmentBenchmarkData {
public:
    static VariantSegmentBenchmarkData& instance() {
        static VariantSegmentBenchmarkData data;
        return data;
    }

    const Status& status() const { return _status; }
    uint32_t rows() const { return _rows; }
    uint32_t rows_per_segment() const { return _rows_per_segment; }
    uint32_t expected_hits(ReadTarget target) const {
        if (target == ReadTarget::MATERIALIZED) {
            return _rows;
        }
        if (target == ReadTarget::SPARSE) {
            return (_rows + SPARSE_HIT_PERIOD - 1) / SPARSE_HIT_PERIOD;
        }
        return 0;
    }
    uint32_t segment_count() const {
        return static_cast<uint32_t>((static_cast<uint64_t>(_rows) + _rows_per_segment - 1) /
                                     _rows_per_segment);
    }
    uint32_t whole_validation_sample_rows() const {
        uint64_t samples = 0;
        for (uint32_t segment_id = 0; segment_id < segment_count(); ++segment_id) {
            const uint32_t rows = rows_in_segment(segment_id);
            samples += (static_cast<uint64_t>(rows) + WHOLE_VALIDATION_STRIDE - 1) /
                       WHOLE_VALIDATION_STRIDE;
            if ((rows - 1) % WHOLE_VALIDATION_STRIDE != 0) {
                ++samples;
            }
        }
        DORIS_CHECK_LE(samples, std::numeric_limits<uint32_t>::max());
        return static_cast<uint32_t>(samples);
    }
    uint64_t input_bytes() const { return _input_bytes; }
    std::string measured_segment_path(VariantLayout layout, VariantVersion version,
                                      std::string benchmark_name) const {
        std::replace(benchmark_name.begin(), benchmark_name.end(), '/', '_');
        return _directory + "/measured_" + std::string(layout_config(layout).name) + "_" +
               std::string(version_name(version)) + "_" + benchmark_name + ".dat";
    }
    Status delete_segment_files(const std::string& base_path) const {
        for (uint32_t segment_id = 0; segment_id < segment_count(); ++segment_id) {
            RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(
                    segment_path(base_path, segment_id)));
        }
        return Status::OK();
    }
    ParseConfig v1_parse_config(VariantLayout layout) const {
        const TabletSchema& schema = *_schemas[static_cast<size_t>(layout)];
        const int32_t root_index = schema.field_index(ROOT_UID);
        DORIS_CHECK_GE(root_index, 0);
        ParseConfig parse_config;
        parse_config.deprecated_enable_flatten_nested = schema.deprecated_variant_flatten_nested();
        parse_config.check_duplicate_json_path = config::variant_enable_duplicate_json_path_check;
        parse_config.parse_to = variant_util::select_storage_variant_parse_target(
                schema.column(root_index), parse_config);
        return parse_config;
    }
    Status ensure_writer_warmup(VariantLayout layout, VariantVersion version,
                                PreparedSegment** prepared) {
        if (prepared != nullptr && segment_count() != 1) {
            return Status::InternalError(
                    "Variant read benchmark requires one segment, but rows={} and "
                    "rows_per_segment={} produce {} segments",
                    _rows, _rows_per_segment, segment_count());
        }
        const auto layout_index = static_cast<size_t>(layout);
        const auto version_index = static_cast<size_t>(version);
        if (_writer_attempted[layout_index][version_index]) {
            if (!_writer_errors[layout_index][version_index].empty()) {
                return Status::InternalError(_writer_errors[layout_index][version_index]);
            }
            if (prepared != nullptr) {
                *prepared = &_writer_segments[layout_index][version_index];
            }
            return Status::OK();
        }
        _writer_attempted[layout_index][version_index] = true;

        PreparedSegment result;
        const std::string path = _directory + "/warm_" + std::string(layout_config(layout).name) +
                                 "_" + std::string(version_name(version)) + ".dat";
        RETURN_IF_ERROR(delete_segment_files(path));
        WriteResult write_result;
        std::vector<uint64_t> segment_bytes;
        Status status =
                write_segments(layout, version, path, nullptr, &write_result, &segment_bytes);
        if (status.ok()) {
            LayoutCounts counts;
            status = validate_written_segments(layout, version, path, segment_bytes, &counts,
                                               segment_count() == 1 ? &result : nullptr);
        }
        if (!status.ok()) {
            _writer_errors[layout_index][version_index] = status.to_string();
            return status;
        }
        if (segment_count() == 1) {
            _writer_segments[layout_index][version_index] = std::move(result);
        }
        if (prepared != nullptr) {
            *prepared = &_writer_segments[layout_index][version_index];
        }
        return Status::OK();
    }

    Status ensure_read_validation(VariantLayout layout, ReadTarget target,
                                  PreparedSegment** fixture) {
        const auto layout_index = static_cast<size_t>(layout);
        const auto target_index = static_cast<size_t>(target);
        PreparedSegment* segment = nullptr;
        RETURN_IF_ERROR(ensure_writer_warmup(layout, VariantVersion::V1, &segment));

        if (_read_attempted[layout_index][target_index]) {
            if (!_read_errors[layout_index][target_index].empty()) {
                return Status::InternalError(_read_errors[layout_index][target_index]);
            }
            *fixture = segment;
            return Status::OK();
        }
        _read_attempted[layout_index][target_index] = true;

        PreparedScan v1_scan;
        PreparedScan v2_scan;
        Status status = prepare_scan(segment, target, VariantVersion::V1, &v1_scan);
        if (status.ok()) {
            status = prepare_scan(segment, target, VariantVersion::V2, &v2_scan);
        }
        ScanResult v1_result;
        ScanResult v2_result;
        const bool sample_whole = target == ReadTarget::WHOLE;
        if (status.ok()) {
            status = scan_segment(v1_scan, true, _rows, &v1_result, sample_whole);
        }
        if (status.ok()) {
            status = scan_segment(v2_scan, true, _rows, &v2_result, sample_whole);
        }
        if (status.ok() && (v1_result.rows != v2_result.rows || v1_result.hits != v2_result.hits)) {
            status = Status::InternalError("{} {} V1/V2 mismatch: rows {}/{}, hits {}/{}",
                                           layout_config(layout).name, target_name(target),
                                           v1_result.rows, v2_result.rows, v1_result.hits,
                                           v2_result.hits);
        }
        const auto expected = expected_checksum(target);
        if (status.ok() && !sample_whole &&
            (v1_result.checksum != expected || v2_result.checksum != expected)) {
            status = Status::InternalError("{} {} checksum mismatch: V1 {}, V2 {}, input {}",
                                           layout_config(layout).name, target_name(target),
                                           v1_result.checksum, v2_result.checksum, expected);
        }
        if (status.ok()) {
            status = validate_hit_count(target, v1_result.hits);
        }
        if (status.ok()) {
            status = validate_route(target, v1_result.statistics);
        }
        if (status.ok()) {
            status = validate_route(target, v2_result.statistics);
        }
        if (!status.ok()) {
            _read_errors[layout_index][target_index] = status.to_string();
            return status;
        }
        *fixture = segment;
        return Status::OK();
    }

    Status prepare_scan(PreparedSegment* fixture, ReadTarget target, VariantVersion version,
                        PreparedScan* prepared) const {
        DORIS_CHECK(fixture != nullptr);
        DORIS_CHECK(prepared != nullptr);
        TabletSchemaPB schema_pb;
        fixture->schema->to_schema_pb(&schema_pb);
        auto query_schema = std::make_shared<TabletSchema>();
        query_schema->init_from_pb(schema_pb);
        query_schema->set_storage_format(fixture->schema->storage_format());

        const int32_t root_id = query_schema->field_index(ROOT_UID);
        if (root_id < 0) {
            return Status::InternalError("Variant benchmark root column is missing");
        }
        const TabletColumn& root = query_schema->column(root_id);
        auto output_id = static_cast<ColumnId>(root_id);
        if (target == ReadTarget::WHOLE) {
            query_schema->mutable_column(root_id).set_variant_is_v2(version == VariantVersion::V2);
        } else {
            std::string_view relative_path;
            switch (target) {
            case ReadTarget::MATERIALIZED:
                relative_path = HOT_PATH;
                break;
            case ReadTarget::SPARSE:
                relative_path = SPARSE_PATH;
                break;
            case ReadTarget::MISSING:
                relative_path = MISSING_PATH;
                break;
            case ReadTarget::WHOLE:
                __builtin_unreachable();
            }
            const std::string full_path = root.name_lower_case() + "." + std::string(relative_path);
            DataTypePtr path_type = std::make_shared<DataTypeVariant>(
                    root.variant_max_subcolumns_count(), root.variant_enable_doc_mode());
            if (version == VariantVersion::V2) {
                path_type = std::make_shared<DataTypeVariantV2>(root.variant_max_subcolumns_count(),
                                                                root.variant_enable_doc_mode());
            }
            TabletColumn path_column = variant_util::get_column_by_type(
                    path_type, full_path,
                    variant_util::ExtraInfo {.parent_unique_id = root.unique_id(),
                                             .path_info = PathInData(full_path)});
            path_column.set_is_nullable(true);
            variant_util::inherit_column_attributes(root, path_column);
            query_schema->append_column(path_column, TabletSchema::ColumnType::VARIANT);
            output_id = static_cast<ColumnId>(query_schema->num_columns() - 1);
        }

        prepared->fixture = fixture;
        prepared->query_schema = std::move(query_schema);
        prepared->scan_schema = std::make_shared<ReadSchema>(project_columns_by_ordinal(
                prepared->query_schema->columns(), std::vector<ColumnId> {output_id}));
        prepared->output_column_id = output_id;
        prepared->target = target;
        return Status::OK();
    }

    Status prepare_rewrite_scan(PreparedSegment* fixture, VariantVersion version,
                                PreparedScan* prepared) const {
        RETURN_IF_ERROR(prepare_scan(fixture, ReadTarget::WHOLE, version, prepared));
        const int32_t key_id = prepared->query_schema->field_index(KEY_UID);
        const int32_t root_id = prepared->query_schema->field_index(ROOT_UID);
        if (key_id < 0 || root_id < 0) {
            return Status::InternalError("Variant rewrite benchmark columns are missing");
        }
        prepared->scan_schema = std::make_shared<ReadSchema>(
                project_columns_by_ordinal(prepared->query_schema->columns(),
                                           std::vector<ColumnId> {static_cast<ColumnId>(key_id),
                                                                  static_cast<ColumnId>(root_id)}));
        return Status::OK();
    }

    Status rewrite_segment(const PreparedScan& prepared, VariantLayout layout,
                           VariantVersion version, const std::string& destination_path,
                           RewriteResult* result) const {
        DORIS_CHECK(prepared.fixture != nullptr);
        DORIS_CHECK(result != nullptr);
        if (segment_count() != 1) {
            return Status::InternalError(
                    "Variant rewrite benchmark requires one source segment, got {}",
                    segment_count());
        }

        const auto scan_init_start = std::chrono::steady_clock::now();
        StorageReadOptions read_options;
        read_options.stats = &result->statistics;
        read_options.tablet_schema = prepared.query_schema;
        read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
        read_options.use_page_cache = true;
        read_options.block_row_max = BATCH_ROWS;
        read_options.preferred_block_size_bytes = 0;
        RowwiseIteratorUPtr iterator;
        RETURN_IF_ERROR(prepared.fixture->segment->new_iterator(prepared.scan_schema, read_options,
                                                                &iterator));
        result->scan_init_ns += elapsed_ns(scan_init_start);

        const TabletSchemaSPtr& destination_schema = _schemas[static_cast<size_t>(layout)];
        const auto writer_init_start = std::chrono::steady_clock::now();
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(destination_path, &file_writer));
        RowsetWriterContext rowset_context;
        rowset_context.write_type = DataWriteType::TYPE_DIRECT;
        rowset_context.tablet_schema = destination_schema;
        rowset_context.tablet_path = _directory;

        segment_v2::VerticalSegmentWriterOptions writer_options;
        writer_options.num_rows_per_block = BATCH_ROWS;
        writer_options.max_rows_per_segment = _rows_per_segment;
        writer_options.compression_type = CompressionTypePB::LZ4;
        writer_options.rowset_ctx = &rowset_context;
        writer_options.write_type = DataWriteType::TYPE_DIRECT;
        segment_v2::VerticalSegmentWriter writer(file_writer.get(), 0, destination_schema, nullptr,
                                                 nullptr, writer_options, nullptr);
        RETURN_IF_ERROR(writer.init());
        result->writer_init_ns += elapsed_ns(writer_init_start);

        Block block = prepared.scan_schema->create_read_block();
        bool checked_representation = false;
        while (true) {
            const auto read_start = std::chrono::steady_clock::now();
            Status status = iterator->next_batch(&block);
            result->read_ns += elapsed_ns(read_start);
            if (status.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            RETURN_IF_ERROR(status);
            if (!checked_representation) {
                const IColumn& root = *block.get_by_position(1).column;
                const bool is_v2 = check_and_get_column<ColumnVariantV2>(root) != nullptr;
                const bool is_v1 = check_and_get_column<ColumnVariant>(root) != nullptr;
                if ((version == VariantVersion::V2 && !is_v2) ||
                    (version == VariantVersion::V1 && (!is_v1 || is_v2))) {
                    return Status::InternalError("Variant rewrite query returned the wrong column");
                }
                checked_representation = true;
            }
            const uint32_t batch_rows = static_cast<uint32_t>(block.rows());
            result->rows += batch_rows;
            result->read_output_bytes += block.bytes();
            const auto append_start = std::chrono::steady_clock::now();
            RETURN_IF_ERROR(writer.append_block(&block, 0, batch_rows));
            result->append_ns += elapsed_ns(append_start);
            block.clear_column_data();
        }
        if (result->rows != _rows) {
            return Status::InternalError("Variant rewrite read {} rows, expected {}", result->rows,
                                         _rows);
        }

        const auto finalize_start = std::chrono::steady_clock::now();
        RETURN_IF_ERROR(writer.finalize_columns(&result->destination_index_bytes));
        RETURN_IF_ERROR(writer.finalize_footer(&result->destination_segment_bytes));
        result->finalize_ns += elapsed_ns(finalize_start);
        result->source_segment_bytes = prepared.fixture->segment_bytes;
        return Status::OK();
    }

    Status write_segments(VariantLayout layout, VariantVersion version,
                          const std::string& base_path, benchmark::State* state,
                          WriteResult* result, std::vector<uint64_t>* segment_bytes) const {
        DORIS_CHECK(result != nullptr);
        DORIS_CHECK(segment_bytes != nullptr);
        const TabletSchemaSPtr& schema = _schemas[static_cast<size_t>(layout)];
        const auto key_type = std::make_shared<DataTypeInt64>();
        segment_bytes->clear();
        segment_bytes->reserve(segment_count());

        for (uint32_t segment_id = 0; segment_id < segment_count(); ++segment_id) {
            const uint32_t segment_begin = segment_id * _rows_per_segment;
            const uint32_t segment_rows = rows_in_segment(segment_id);
            const auto init_start = std::chrono::steady_clock::now();
            {
                io::FileWriterPtr file_writer;
                RETURN_IF_ERROR(io::global_local_filesystem()->create_file(
                        segment_path(base_path, segment_id), &file_writer));
                RowsetWriterContext rowset_context;
                rowset_context.write_type = DataWriteType::TYPE_DIRECT;
                rowset_context.tablet_schema = schema;
                rowset_context.tablet_path = _directory;

                segment_v2::VerticalSegmentWriterOptions options;
                options.num_rows_per_block = BATCH_ROWS;
                options.max_rows_per_segment = _rows_per_segment;
                options.compression_type = CompressionTypePB::LZ4;
                options.rowset_ctx = &rowset_context;
                options.write_type = DataWriteType::TYPE_DIRECT;

                segment_v2::VerticalSegmentWriter writer(file_writer.get(), segment_id, schema,
                                                         nullptr, nullptr, options, nullptr);
                RETURN_IF_ERROR(writer.init());
                result->init_ns += elapsed_ns(init_start);

                for (uint32_t offset = 0; offset < segment_rows; offset += BATCH_ROWS) {
                    const uint32_t batch_rows = std::min(BATCH_ROWS, segment_rows - offset);
                    if (state != nullptr) {
                        state->PauseTiming();
                    }
                    {
                        ColumnString::MutablePtr raw_json;
                        ColumnPtr key_batch;
                        build_input_batch(segment_begin + offset, batch_rows, &raw_json,
                                          &key_batch);
                        if (state != nullptr) {
                            state->ResumeTiming();
                        }

                        ColumnPtr variant_column;
                        DataTypePtr variant_type;
                        const auto parse_start = std::chrono::steady_clock::now();
                        RETURN_IF_ERROR(parse_batch(layout, version, *raw_json, batch_rows,
                                                    &variant_column, &variant_type));
                        result->parse_ns += elapsed_ns(parse_start);

                        Block block;
                        block.insert({std::move(key_batch), key_type, "k"});
                        block.insert({std::move(variant_column), std::move(variant_type),
                                      std::string(ROOT_NAME)});
                        const auto append_start = std::chrono::steady_clock::now();
                        RETURN_IF_ERROR(writer.append_block(&block, 0, batch_rows));
                        result->append_ns += elapsed_ns(append_start);
                        if (state != nullptr) {
                            state->PauseTiming();
                        }
                    }
                    if (state != nullptr) {
                        state->ResumeTiming();
                    }
                }

                uint64_t bytes = 0;
                uint64_t index_bytes = 0;
                const auto finalize_start = std::chrono::steady_clock::now();
                RETURN_IF_ERROR(writer.finalize_columns(&index_bytes));
                RETURN_IF_ERROR(writer.finalize_footer(&bytes));
                result->finalize_ns += elapsed_ns(finalize_start);
                result->segment_bytes += bytes;
                result->index_bytes += index_bytes;
                if (state != nullptr) {
                    state->PauseTiming();
                }
                segment_bytes->push_back(bytes);
            }
            if (state != nullptr) {
                state->ResumeTiming();
            }
        }
        return Status::OK();
    }

    Status validate_written_segments(VariantLayout layout, VariantVersion version,
                                     const std::string& base_path,
                                     const std::vector<uint64_t>& segment_bytes,
                                     LayoutCounts* counts, PreparedSegment* retained) const {
        DORIS_CHECK(counts != nullptr);
        if (segment_bytes.size() != segment_count()) {
            return Status::InternalError("Variant benchmark wrote {} segments, expected {}",
                                         segment_bytes.size(), segment_count());
        }
        if (retained != nullptr && segment_count() != 1) {
            return Status::InternalError("Only a single segment can be retained for read cases");
        }

        *counts = LayoutCounts {};
        uint64_t total_rows = 0;
        for (uint32_t segment_id = 0; segment_id < segment_count(); ++segment_id) {
            PreparedSegment measured;
            measured.schema = _schemas[static_cast<size_t>(layout)];
            measured.path = segment_path(base_path, segment_id);
            measured.segment_bytes = segment_bytes[segment_id];
            const uint32_t expected_rows = rows_in_segment(segment_id);
            RETURN_IF_ERROR(
                    open_and_validate_segment(layout, segment_id, expected_rows, &measured));

            PreparedScan scan;
            RETURN_IF_ERROR(prepare_scan(&measured, ReadTarget::WHOLE, version, &scan));
            ScanResult scan_result;
            const uint32_t global_row_offset = segment_id * _rows_per_segment;
            RETURN_IF_ERROR(
                    scan_segment(scan, true, expected_rows, &scan_result, true, global_row_offset));
            RETURN_IF_ERROR(validate_route(ReadTarget::WHOLE, scan_result.statistics));
            total_rows += scan_result.rows;

            if (segment_id == 0) {
                *counts = measured.counts;
            } else if (measured.counts.materialized != counts->materialized ||
                       measured.counts.sparse != counts->sparse ||
                       measured.counts.doc != counts->doc) {
                return Status::InternalError(
                        "{} segment {} layout differs: materialized={}, sparse={}, doc={}",
                        layout_config(layout).name, segment_id, measured.counts.materialized,
                        measured.counts.sparse, measured.counts.doc);
            }

            if (retained != nullptr) {
                *retained = std::move(measured);
            } else {
                measured.segment.reset();
                RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(measured.path));
            }
        }
        if (total_rows != _rows) {
            return Status::InternalError(
                    "Variant benchmark read {} rows from {} segments, expected {}", total_rows,
                    segment_count(), _rows);
        }
        return Status::OK();
    }

    Status scan_segment(const PreparedScan& prepared, bool checksum, uint32_t expected_rows,
                        ScanResult* result, bool sample_whole = false,
                        uint32_t global_row_offset = 0) const {
        DORIS_CHECK(prepared.fixture != nullptr);
        DORIS_CHECK(result != nullptr);
        DORIS_CHECK(!sample_whole || (checksum && prepared.target == ReadTarget::WHOLE));
        StorageReadOptions options;
        options.stats = &result->statistics;
        options.tablet_schema = prepared.query_schema;
        options.io_ctx.reader_type = ReaderType::READER_QUERY;
        options.use_page_cache = true;
        options.block_row_max = BATCH_ROWS;
        options.preferred_block_size_bytes = 0;

        RowwiseIteratorUPtr iterator;
        RETURN_IF_ERROR(
                prepared.fixture->segment->new_iterator(prepared.scan_schema, options, &iterator));
        Block block = prepared.scan_schema->create_read_block();
        while (true) {
            Status status = iterator->next_batch(&block);
            if (status.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            RETURN_IF_ERROR(status);
            const auto& output = block.get_by_position(0);
            const uint32_t block_begin = result->rows;
            result->rows += static_cast<uint32_t>(block.rows());
            result->output_bytes += block.bytes();
            if (checksum && prepared.target != ReadTarget::WHOLE) {
                for (size_t row = 0; row < block.rows(); ++row) {
                    result->hits += !output.column->is_null_at(row);
                }
            }
            if (checksum) {
                if (sample_whole) {
                    for (size_t row = 0; row < block.rows(); ++row) {
                        const uint32_t local_row = block_begin + static_cast<uint32_t>(row);
                        if (local_row % WHOLE_VALIDATION_STRIDE != 0 &&
                            local_row + 1 != expected_rows) {
                            continue;
                        }

                        const std::string actual = output.type->to_string(*output.column, row);
                        const uint32_t global_row = global_row_offset + local_row;
                        const std::string expected = make_json(global_row);
                        if (actual != expected) {
                            return Status::InternalError(
                                    "Variant whole-column sample differs at global row {}",
                                    global_row);
                        }
                    }
                } else {
                    for (size_t row = 0; row < block.rows(); ++row) {
                        result->checksum = update_checksum(
                                result->checksum, output.type->to_string(*output.column, row));
                    }
                }
            } else {
                benchmark::DoNotOptimize(result->output_bytes);
            }
            block.clear_column_data();
        }
        if (result->rows != expected_rows) {
            return Status::InternalError("Variant benchmark read {} rows, expected {}",
                                         result->rows, expected_rows);
        }
        return Status::OK();
    }

private:
    VariantSegmentBenchmarkData()
            : _rows(configured_rows()),
              _rows_per_segment(std::min(configured_rows_per_segment(), _rows)),
              _directory(benchmark_root() + "/doris_variant_segment_benchmark_" +
                         std::to_string(getpid())) {
        _status = initialize();
    }

    ~VariantSegmentBenchmarkData() {
        for (auto& layouts : _writer_segments) {
            for (auto& segment : layouts) {
                segment.segment.reset();
            }
        }
        WARN_IF_ERROR(io::global_local_filesystem()->delete_directory(_directory),
                      "Failed to clean Variant segment benchmark directory");
    }

    Status initialize() {
        if (ExecEnv::GetInstance()->get_cache_manager() == nullptr) {
            ExecEnv::GetInstance()->set_cache_manager(CacheManager::create_global_instance());
        }
        if (ExecEnv::GetInstance()->get_storage_page_cache() == nullptr) {
            constexpr size_t CACHE_CAPACITY = 512UL << 20;
            ExecEnv::GetInstance()->set_storage_page_cache(
                    StoragePageCache::create_global_cache(CACHE_CAPACITY, 10, 0));
        }
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(_directory));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_directory));
        _schemas[static_cast<size_t>(VariantLayout::SPARSE16)] =
                make_schema(VariantLayout::SPARSE16);
        _schemas[static_cast<size_t>(VariantLayout::DOC16)] = make_schema(VariantLayout::DOC16);
        _schemas[static_cast<size_t>(VariantLayout::FULL)] = make_schema(VariantLayout::FULL);
        build_input_oracle();
        return Status::OK();
    }

    uint64_t expected_checksum(ReadTarget target) const {
        return _expected_checksums[static_cast<size_t>(target)];
    }

    uint32_t rows_in_segment(uint32_t segment_id) const {
        const uint64_t begin = static_cast<uint64_t>(segment_id) * _rows_per_segment;
        return static_cast<uint32_t>(
                std::min<uint64_t>(_rows_per_segment, static_cast<uint64_t>(_rows) - begin));
    }

    std::string segment_path(const std::string& base_path, uint32_t segment_id) const {
        if (segment_count() == 1) {
            return base_path;
        }
        return base_path + "." + std::to_string(segment_id);
    }

public:
    static TabletSchemaSPtr make_schema(VariantLayout layout) {
        const LayoutConfig config = layout_config(layout);
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);

        auto* key = schema_pb.add_column();
        key->set_unique_id(KEY_UID);
        key->set_name("k");
        key->set_type("BIGINT");
        key->set_is_key(true);
        key->set_is_nullable(false);

        auto* variant = schema_pb.add_column();
        variant->set_unique_id(ROOT_UID);
        variant->set_name(std::string(ROOT_NAME));
        variant->set_type("VARIANT");
        variant->set_is_key(false);
        variant->set_is_nullable(false);
        variant->set_variant_max_subcolumns_count(config.max_subcolumns);
        variant->set_variant_max_sparse_column_statistics_size(10'000);
        variant->set_variant_sparse_hash_shard_count(BUCKETS);
        variant->set_variant_enable_doc_mode(config.doc_mode);
        variant->set_variant_doc_materialization_min_rows(std::numeric_limits<int64_t>::max());
        variant->set_variant_doc_hash_shard_count(BUCKETS);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);
        return schema;
    }

    static std::string make_json(uint32_t row) {
        static const std::array<std::string, COLD_PATHS> cold_keys = [] {
            std::array<std::string, COLD_PATHS> keys;
            for (uint32_t cold = 0; cold < COLD_PATHS; ++cold) {
                std::string suffix = std::to_string(cold);
                suffix.insert(0, 3 - suffix.size(), '0');
                keys[cold] = "cold_" + suffix;
            }
            return keys;
        }();
        std::array<uint16_t, FIELDS_PER_ROW - 1> cold_ids {};
        const bool include_sparse = row % SPARSE_HIT_PERIOD == 0;
        const size_t cold_count = FIELDS_PER_ROW - 1 - include_sparse;
        const uint32_t start = (static_cast<uint64_t>(row) * 37) % COLD_PATHS;
        for (size_t index = 0; index < cold_count; ++index) {
            cold_ids[index] = static_cast<uint16_t>((start + index) % COLD_PATHS);
        }
        std::sort(cold_ids.begin(), cold_ids.begin() + cold_count);

        std::string json;
        json.reserve(640);
        json.push_back('{');
        bool first = true;
        const auto append_field = [&](std::string_view key, uint64_t value) {
            if (!first) {
                json.push_back(',');
            }
            first = false;
            json.push_back('"');
            json.append(key);
            json += "\":";
            json += std::to_string(value);
        };
        for (size_t index = 0; index < cold_count; ++index) {
            const uint32_t cold = cold_ids[index];
            append_field(cold_keys[cold], static_cast<uint64_t>(row) * COLD_PATHS + cold);
        }
        append_field(HOT_PATH, row);
        if (include_sparse) {
            append_field(SPARSE_PATH, static_cast<uint64_t>(row) * 17 + 3);
        }
        json.push_back('}');
        DORIS_CHECK_EQ(std::count(json.begin(), json.end(), ':'), FIELDS_PER_ROW);
        return json;
    }

private:
    static void build_input_batch(uint32_t begin, uint32_t rows, ColumnString::MutablePtr* raw_json,
                                  ColumnPtr* keys) {
        DORIS_CHECK(raw_json != nullptr);
        DORIS_CHECK(keys != nullptr);
        auto json_batch = ColumnString::create();
        json_batch->reserve(rows);
        auto key_batch = ColumnInt64::create();
        key_batch->reserve(rows);
        for (uint32_t offset = 0; offset < rows; ++offset) {
            const uint32_t row = begin + offset;
            const std::string json = make_json(row);
            json_batch->insert_data(json.data(), json.size());
            key_batch->insert_value(row);
        }
        *raw_json = std::move(json_batch);
        *keys = std::move(key_batch);
    }

    void build_input_oracle() {
        _segment_expected_checksums.resize(segment_count());
        for (auto& checksums : _segment_expected_checksums) {
            checksums.fill(FNV_OFFSET);
        }
        for (uint32_t segment_id = 0; segment_id < segment_count(); ++segment_id) {
            const uint32_t segment_begin = segment_id * _rows_per_segment;
            const uint32_t segment_rows = rows_in_segment(segment_id);
            for (uint32_t offset = 0; offset < segment_rows; ++offset) {
                const uint32_t row = segment_begin + offset;
                const std::string json = make_json(row);
                const bool include_sparse = row % SPARSE_HIT_PERIOD == 0;
                const std::string materialized = std::to_string(row);
                const std::string sparse =
                        include_sparse ? std::to_string(static_cast<uint64_t>(row) * 17 + 3)
                                       : "NULL";
                _input_bytes += json.size();
                const std::array<std::string_view, 4> values {json, materialized, sparse, "NULL"};
                for (size_t target = 0; target < values.size(); ++target) {
                    _expected_checksums[target] =
                            update_checksum(_expected_checksums[target], values[target]);
                    _segment_expected_checksums[segment_id][target] = update_checksum(
                            _segment_expected_checksums[segment_id][target], values[target]);
                }
            }
        }
    }

    Status parse_batch(VariantLayout layout, VariantVersion version, const ColumnString& raw_json,
                       uint32_t rows, ColumnPtr* column, DataTypePtr* type) const {
        DORIS_CHECK(column != nullptr);
        DORIS_CHECK(type != nullptr);
        const LayoutConfig config = layout_config(layout);
        if (version == VariantVersion::V1) {
            auto values = ColumnVariant::create(config.max_subcolumns, config.doc_mode);
            const ParseConfig parse_config = v1_parse_config(layout);
            JsonParser parser;
            RETURN_IF_CATCH_EXCEPTION({
                for (size_t row = 0; row < rows; ++row) {
                    variant_util::parse_json_to_variant(*values, raw_json.get_data_at(row), &parser,
                                                        parse_config);
                }
                values->finalize();
            });
            *column = std::move(values);
            *type = std::make_shared<DataTypeVariant>(config.max_subcolumns, config.doc_mode);
        } else {
            auto values = ColumnVariantV2::create();
            RETURN_IF_CATCH_EXCEPTION({
                JsonStringToVariantEncoder encoder(JsonToVariantOptions::current_config());
                for (size_t row = 0; row < rows; ++row) {
                    encoder.add_json(raw_json.get_data_at(row));
                }
                VariantBatchBuilder encoded = encoder.finish_batch();
                values->insert_encoded_batch(encoded);
            });
            *column = std::move(values);
            *type = std::make_shared<DataTypeVariantV2>(config.max_subcolumns, config.doc_mode);
        }
        DORIS_CHECK_EQ((*column)->size(), rows);
        return Status::OK();
    }

    Status open_and_validate_segment(VariantLayout layout, uint32_t segment_id,
                                     uint32_t expected_rows, PreparedSegment* prepared) const {
        DORIS_CHECK(prepared != nullptr);
        RowsetId rowset_id;
        rowset_id.init(10'000 + static_cast<int64_t>(layout));
        RETURN_IF_ERROR(segment_v2::Segment::open(io::global_local_filesystem(), prepared->path,
                                                  20'000 + static_cast<int64_t>(layout), segment_id,
                                                  rowset_id, prepared->schema,
                                                  io::FileReaderOptions {}, &prepared->segment));
        if (prepared->segment->num_rows() != expected_rows) {
            return Status::InternalError("{} segment has {} rows, expected {}",
                                         layout_config(layout).name, prepared->segment->num_rows(),
                                         expected_rows);
        }

        std::shared_ptr<SegmentFooterPB> footer;
        OlapReaderStatistics statistics;
        io::IOContext io_context;
        io_context.reader_type = ReaderType::READER_QUERY;
        RETURN_IF_ERROR(prepared->segment->_get_segment_footer(footer, &statistics, &io_context));
        segment_v2::ColumnMetaAccessor accessor;
        RETURN_IF_ERROR(accessor.init(*footer, prepared->segment->_file_reader));
        RETURN_IF_ERROR(accessor.traverse_metas(
                *footer,
                [&](const ColumnMetaPB& meta) { count_layout_columns(meta, &prepared->counts); },
                &statistics, &io_context));

        const LayoutCounts& counts = prepared->counts;
        switch (layout) {
        case VariantLayout::SPARSE16:
            if (counts.materialized != 1 || counts.sparse != BUCKETS || counts.doc != 0) {
                return Status::InternalError(
                        "Sparse16 layout is materialized={}, sparse={}, doc={}",
                        counts.materialized, counts.sparse, counts.doc);
            }
            break;
        case VariantLayout::DOC16:
            if (counts.materialized != 0 || counts.sparse != 0 || counts.doc != BUCKETS) {
                return Status::InternalError("Doc16 layout is materialized={}, sparse={}, doc={}",
                                             counts.materialized, counts.sparse, counts.doc);
            }
            break;
        case VariantLayout::FULL:
            if (counts.materialized != CANDIDATE_PATHS || counts.sparse != BUCKETS ||
                counts.doc != 0) {
                return Status::InternalError("Full layout is materialized={}, sparse={}, doc={}",
                                             counts.materialized, counts.sparse, counts.doc);
            }
            break;
        }
        return Status::OK();
    }

    Status validate_hit_count(ReadTarget target, uint32_t hits) const {
        if (target == ReadTarget::WHOLE) {
            return Status::OK();
        }
        const uint32_t expected = expected_hits(target);
        if (hits != expected) {
            return Status::InternalError("{} hit count is {}, expected {}", target_name(target),
                                         hits, expected);
        }
        return Status::OK();
    }

    static Status validate_route(ReadTarget target, const OlapReaderStatistics& statistics) {
        switch (target) {
        case ReadTarget::WHOLE:
            if (statistics.variant_subtree_hierarchical_iter_count <= 0) {
                return Status::InternalError("Whole-column scan did not use HIERARCHICAL");
            }
            break;
        case ReadTarget::MATERIALIZED:
            if (statistics.variant_subtree_leaf_iter_count <= 0) {
                return Status::InternalError("Materialized scan did not use LEAF");
            }
            break;
        case ReadTarget::SPARSE:
            if (statistics.variant_subtree_sparse_iter_count <= 0) {
                return Status::InternalError("Sparse scan did not use BINARY_EXTRACT");
            }
            break;
        case ReadTarget::MISSING:
            if (statistics.variant_subtree_default_iter_count <= 0) {
                return Status::InternalError("Global-miss scan did not use DEFAULT_FILL");
            }
            break;
        }
        return Status::OK();
    }

    uint32_t _rows;
    uint32_t _rows_per_segment;
    std::string _directory;
    Status _status = Status::OK();
    uint64_t _input_bytes = 0;
    std::array<uint64_t, 4> _expected_checksums {FNV_OFFSET, FNV_OFFSET, FNV_OFFSET, FNV_OFFSET};
    std::vector<std::array<uint64_t, 4>> _segment_expected_checksums;
    std::array<TabletSchemaSPtr, 3> _schemas;
    std::array<std::array<PreparedSegment, 2>, 3> _writer_segments;
    std::array<std::array<bool, 2>, 3> _writer_attempted {};
    std::array<std::array<std::string, 2>, 3> _writer_errors;
    std::array<std::array<bool, 4>, 3> _read_attempted {};
    std::array<std::array<std::string, 4>, 3> _read_errors;
};

uint64_t next_compaction_fixture_id() {
    static uint64_t next_id = 0;
    return ++next_id;
}

// Measures the production cumulative-compaction entry point. Input rowsets are
// persisted by the V2 writer, while the current compactor intentionally reads
// and writes ColumnVariant; there is no V1-input parser in this fixture.
class VariantCompactionBenchmarkFixture {
public:
    VariantCompactionBenchmarkFixture()
            : _total_rows(configured_rows()),
              _rows_per_rowset(_total_rows / COMPACTION_INPUT_ROWSETS),
              _fixture_id(next_compaction_fixture_id()),
              _directory(benchmark_root() + "/doris_variant_compaction_benchmark_" +
                         std::to_string(getpid()) + "_" + std::to_string(_fixture_id)),
              _tmp_directory(_directory + "/tmp"),
              _previous_ordered_compaction(config::enable_ordered_data_compaction),
              _previous_compaction_checksum(config::enable_compaction_checksum),
              _previous_vertical_compaction(config::enable_vertical_compaction),
              _previous_vertical_variant_compaction(
                      config::enable_vertical_compact_variant_subcolumns) {}

    ~VariantCompactionBenchmarkFixture() {
        _compaction.reset();
        _input_rowsets.clear();
        _tablet.reset();
        _schema.reset();
        _data_dir.reset();
        _engine = nullptr;
        if (_installed_runtime) {
            ExecEnv* env = ExecEnv::GetInstance();
            env->set_storage_engine(std::move(_previous_storage_engine));
            env->set_tmp_file_dir(std::move(_previous_tmp_file_dirs));
        }
        WARN_IF_ERROR(io::global_local_filesystem()->delete_directory(_directory),
                      "Failed to clean Variant compaction benchmark directory");
        config::enable_ordered_data_compaction = _previous_ordered_compaction;
        config::enable_compaction_checksum = _previous_compaction_checksum;
        config::enable_vertical_compaction = _previous_vertical_compaction;
        config::enable_vertical_compact_variant_subcolumns = _previous_vertical_variant_compaction;
    }

    uint32_t total_rows() const { return _total_rows; }
    uint32_t rows_per_rowset() const { return _rows_per_rowset; }
    uint32_t input_segments() const { return _input_segments; }
    CumulativeCompaction* compaction() const { return _compaction.get(); }

    Status prepare() {
        if (config::variant_storage_parse_mode != 0) {
            return Status::InvalidArgument(
                    "Variant cumulative compaction benchmark requires "
                    "variant_storage_parse_mode=0, actual={}",
                    config::variant_storage_parse_mode);
        }
        if (_total_rows % COMPACTION_INPUT_ROWSETS != 0) {
            return Status::InvalidArgument("Variant compaction rows {} must be divisible by {}",
                                           _total_rows, COMPACTION_INPUT_ROWSETS);
        }

        ensure_variant_compaction_runtime();
        config::enable_ordered_data_compaction = false;
        config::enable_compaction_checksum = false;
        config::enable_vertical_compaction = true;
        config::enable_vertical_compact_variant_subcolumns = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(_directory));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_directory));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_tmp_directory));

        ExecEnv* env = ExecEnv::GetInstance();
        _previous_storage_engine = std::move(env->_storage_engine);
        _previous_tmp_file_dirs = std::move(env->_tmp_file_dirs);
        _installed_runtime = true;

        std::vector<StorePath> tmp_paths;
        tmp_paths.emplace_back(_tmp_directory, 100ULL << 30);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(tmp_paths);
        RETURN_IF_ERROR(tmp_file_dirs->init());
        env->set_tmp_file_dir(std::move(tmp_file_dirs));

        EngineOptions engine_options;
        auto engine = std::make_unique<StorageEngine>(engine_options);
        _engine = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine, _directory);
        RETURN_IF_ERROR(_data_dir->init(true));
        env->set_storage_engine(std::move(engine));

        _schema = VariantSegmentBenchmarkData::make_schema(VariantLayout::SPARSE16);
        auto tablet_meta = std::make_shared<TabletMeta>(_schema);
        const int64_t tablet_id = 100'000 + static_cast<int64_t>(_fixture_id);
        tablet_meta->_tablet_id = tablet_id;
        tablet_meta->set_tablet_uid(TabletUid(tablet_id, tablet_id + 1));
        _tablet = std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get());
        RETURN_IF_ERROR(_tablet->init());
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_tablet->tablet_path()));

        build_oracle();
        _input_rowsets.reserve(COMPACTION_INPUT_ROWSETS);
        for (uint32_t index = 0; index < COMPACTION_INPUT_ROWSETS; ++index) {
            RowsetSharedPtr rowset;
            RETURN_IF_ERROR(write_input_rowset(index, &rowset));
            LayoutCounts input_layout;
            RETURN_IF_ERROR(validate_rowset_layout(rowset, _rows_per_rowset, &input_layout));
            _input_segments += cast_set<uint32_t>(rowset->num_segments());
            RETURN_IF_ERROR(record_input_inodes(rowset));
            RETURN_IF_ERROR(_tablet->add_rowset(rowset));
            _input_disk_bytes += rowset->total_disk_size();
            _input_rowsets.emplace_back(std::move(rowset));
        }

        _compaction = std::make_unique<CumulativeCompaction>(*_engine, _tablet);
        _compaction->_input_rowsets = _input_rowsets;
        return Status::OK();
    }

    Status validate(CompactionResult* result) {
        DORIS_CHECK(result != nullptr);
        if (_compaction == nullptr || _compaction->_output_rowset == nullptr) {
            return Status::InternalError("Variant compaction did not produce an output rowset");
        }
        if (_compaction->_is_ordered_data_compaction) {
            return Status::InternalError("Variant compaction used ordered link-file compaction");
        }

        const RowsetSharedPtr& output = _compaction->_output_rowset;
        if (output->start_version() != 0 || output->end_version() != COMPACTION_INPUT_ROWSETS - 1) {
            return Status::InternalError("Compaction output version is [{},{}], expected [0,{}]",
                                         output->start_version(), output->end_version(),
                                         COMPACTION_INPUT_ROWSETS - 1);
        }
        if (output->num_rows() != _total_rows) {
            return Status::InternalError("Compaction output has {} rows, expected {}",
                                         output->num_rows(), _total_rows);
        }
        const int32_t root_index = output->tablet_schema()->field_index(ROOT_UID);
        if (root_index < 0 || output->tablet_schema()->column(root_index).variant_is_v2()) {
            return Status::InternalError(
                    "ProductionCurrent compaction output unexpectedly uses Variant V2 schema");
        }
        RETURN_IF_ERROR(validate_physical_rewrite(output));
        RETURN_IF_ERROR(validate_rowset_layout(output, _total_rows, &result->output_layout));
        RETURN_IF_ERROR(validate_output_oracle(output));
        result->input_json_bytes = _input_json_bytes;
        result->input_disk_bytes = _input_disk_bytes;
        result->output_disk_bytes = output->total_disk_size();
        result->output_segments = cast_set<uint32_t>(output->num_segments());
        return Status::OK();
    }

private:
    void build_oracle() {
        for (uint32_t row = 0; row < _total_rows; ++row) {
            const std::string json = VariantSegmentBenchmarkData::make_json(row);
            const std::string key = std::to_string(row);
            _expected_whole_checksum = update_checksum(_expected_whole_checksum, json);
            _expected_key_checksum = update_checksum(_expected_key_checksum, key);
            _expected_hot_checksum = update_checksum(_expected_hot_checksum, key);
            const bool has_sparse = row % SPARSE_HIT_PERIOD == 0;
            _expected_sparse_checksum = update_checksum(
                    _expected_sparse_checksum,
                    has_sparse ? std::to_string(static_cast<uint64_t>(row) * 17 + 3) : "NULL");
            _expected_sparse_hits += has_sparse;
        }
    }

    Status make_v2_input_block(uint32_t rowset_index, uint32_t first_local_row, uint32_t rows,
                               Block* block) {
        auto keys = ColumnInt64::create();
        auto raw_json = ColumnString::create();
        keys->reserve(rows);
        raw_json->reserve(rows);
        for (uint32_t local = 0; local < rows; ++local) {
            const uint32_t global_row =
                    (first_local_row + local) * COMPACTION_INPUT_ROWSETS + rowset_index;
            const std::string json = VariantSegmentBenchmarkData::make_json(global_row);
            keys->insert_value(global_row);
            raw_json->insert_data(json.data(), json.size());
            _input_json_bytes += json.size();
        }

        auto values = ColumnVariantV2::create();
        RETURN_IF_CATCH_EXCEPTION({
            JsonStringToVariantEncoder encoder(JsonToVariantOptions::current_config());
            for (uint32_t row = 0; row < rows; ++row) {
                encoder.add_json(raw_json->get_data_at(row));
            }
            VariantBatchBuilder encoded = encoder.finish_batch();
            values->insert_encoded_batch(encoded);
        });
        block->insert({std::move(keys), std::make_shared<DataTypeInt64>(), "k"});
        block->insert({std::move(values), std::make_shared<DataTypeVariantV2>(1, false),
                       std::string(ROOT_NAME)});
        return Status::OK();
    }

    Status write_input_rowset(uint32_t rowset_index, RowsetSharedPtr* rowset) {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(static_cast<int64_t>(_fixture_id) * 100 + rowset_index + 1);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = _data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _schema;
        context.tablet_path = _tablet->tablet_path();
        context.tablet_id = _tablet->tablet_id();
        context.tablet_uid = _tablet->tablet_uid();
        context.tablet = _tablet;
        context.version = Version(rowset_index, rowset_index);
        context.segments_overlap = NONOVERLAPPING;
        context.max_rows_per_segment = _rows_per_rowset;
        context.write_type = DataWriteType::TYPE_DIRECT;

        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, false);
        if (!writer_result.has_value()) {
            return writer_result.error();
        }
        auto writer = std::move(writer_result).value();
        for (uint32_t first = 0; first < _rows_per_rowset; first += BATCH_ROWS) {
            Block block;
            RETURN_IF_ERROR(make_v2_input_block(
                    rowset_index, first, std::min(BATCH_ROWS, _rows_per_rowset - first), &block));
            RETURN_IF_ERROR(writer->add_block(&block));
        }
        RETURN_IF_ERROR(writer->flush());
        RETURN_IF_ERROR(writer->build(*rowset));
        return Status::OK();
    }

    Status validate_rowset_layout(const RowsetSharedPtr& rowset, uint32_t expected_rows,
                                  LayoutCounts* result) const {
        auto beta_rowset = std::static_pointer_cast<BetaRowset>(rowset);
        std::vector<segment_v2::SegmentSharedPtr> segments;
        RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
        if (segments.empty()) {
            return Status::InternalError("Rowset has no segments");
        }
        uint64_t rows = 0;
        bool first = true;
        for (const auto& segment : segments) {
            LayoutCounts counts;
            RETURN_IF_ERROR(segment->traverse_column_meta_pbs(
                    [&](const ColumnMetaPB& meta) { count_layout_columns(meta, &counts); }));
            RETURN_IF_ERROR(validate_sparse16_layout(counts));
            if (first) {
                *result = counts;
                first = false;
            } else if (counts.materialized != result->materialized ||
                       counts.sparse != result->sparse || counts.doc != result->doc) {
                return Status::InternalError("Compaction output segment layouts differ");
            }
            rows += segment->num_rows();
        }
        if (rows != expected_rows) {
            return Status::InternalError("Rowset segments contain {} rows, expected {}", rows,
                                         expected_rows);
        }
        return Status::OK();
    }

    Status record_input_inodes(const RowsetSharedPtr& rowset) {
        for (uint32_t segment = 0; segment < rowset->num_segments(); ++segment) {
            struct stat file_stat {};
            const std::string path = local_segment_path(_tablet->tablet_path(),
                                                        rowset->rowset_id().to_string(), segment);
            if (::stat(path.c_str(), &file_stat) != 0) {
                return Status::IOError("stat {} failed: {}", path, std::strerror(errno));
            }
            _input_inodes.push_back(file_stat.st_ino);
        }
        return Status::OK();
    }

    Status validate_physical_rewrite(const RowsetSharedPtr& output) const {
        for (uint32_t segment = 0; segment < output->num_segments(); ++segment) {
            struct stat file_stat {};
            const std::string path = local_segment_path(_tablet->tablet_path(),
                                                        output->rowset_id().to_string(), segment);
            if (::stat(path.c_str(), &file_stat) != 0) {
                return Status::IOError("stat {} failed: {}", path, std::strerror(errno));
            }
            if (std::find(_input_inodes.begin(), _input_inodes.end(), file_stat.st_ino) !=
                _input_inodes.end()) {
                return Status::InternalError("Compaction output segment {} reused an input inode",
                                             segment);
            }
        }
        return Status::OK();
    }

    Status validate_output_oracle(const RowsetSharedPtr& output) const {
        TabletSchemaPB schema_pb;
        _schema->to_schema_pb(&schema_pb);
        auto query_schema = std::make_shared<TabletSchema>();
        query_schema->init_from_pb(schema_pb);
        query_schema->set_storage_format(_schema->storage_format());
        const int32_t root_index = query_schema->field_index(ROOT_UID);
        if (root_index < 0) {
            return Status::InternalError("Variant root is missing from compaction read schema");
        }
        query_schema->mutable_column(root_index).set_variant_is_v2(false);
        const TabletColumn root = query_schema->column(root_index);
        const auto append_path = [&](std::string_view path) {
            const std::string full_path = root.name_lower_case() + "." + std::string(path);
            TabletColumn path_column = variant_util::get_column_by_type(
                    std::make_shared<DataTypeVariant>(1, false), full_path,
                    variant_util::ExtraInfo {.parent_unique_id = root.unique_id(),
                                             .path_info = PathInData(full_path)});
            path_column.set_is_nullable(true);
            variant_util::inherit_column_attributes(root, path_column);
            query_schema->append_column(path_column, TabletSchema::ColumnType::VARIANT);
            return static_cast<uint32_t>(query_schema->num_columns() - 1);
        };
        const uint32_t hot_id = append_path(HOT_PATH);
        const uint32_t sparse_id = append_path(SPARSE_PATH);
        auto read_schema = std::make_shared<ReadSchema>(project_columns_by_ordinal(
                query_schema->columns(),
                std::vector<ColumnId> {0, static_cast<ColumnId>(root_index), hot_id, sparse_id}));

        RowsetReaderSharedPtr reader;
        RETURN_IF_ERROR(output->create_reader(&reader));
        OlapReaderStatistics statistics;
        RowsetReaderContext context;
        context.reader_type = ReaderType::READER_QUERY;
        context.tablet_schema = query_schema;
        context.need_ordered_result = true;
        context.read_schema = read_schema;
        context.stats = &statistics;
        RETURN_IF_ERROR(reader->init(&context));

        uint64_t whole_checksum = FNV_OFFSET;
        uint64_t key_checksum = FNV_OFFSET;
        uint64_t hot_checksum = FNV_OFFSET;
        uint64_t sparse_checksum = FNV_OFFSET;
        uint32_t rows = 0;
        uint32_t hot_hits = 0;
        uint32_t sparse_hits = 0;
        bool saw_current_compactor = false;
        while (true) {
            Block block = read_schema->create_read_block();
            Status status = reader->next_batch(&block);
            if (status.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            RETURN_IF_ERROR(status);
            const auto& key = block.get_by_position(0);
            const auto& whole = block.get_by_position(1);
            const auto& hot = block.get_by_position(2);
            const auto& sparse = block.get_by_position(3);
            if (block.rows() > 0 && !saw_current_compactor) {
                const auto& nested =
                        assert_cast<const ColumnNullable&>(*hot.column).get_nested_column();
                if (check_and_get_column<ColumnVariant>(nested) == nullptr ||
                    check_and_get_column<ColumnVariantV2>(nested) != nullptr) {
                    return Status::InternalError(
                            "ProductionCurrent compaction did not return ColumnVariant");
                }
                saw_current_compactor = true;
            }
            for (size_t row = 0; row < block.rows(); ++row) {
                whole_checksum =
                        update_checksum(whole_checksum, whole.type->to_string(*whole.column, row));
                key_checksum = update_checksum(key_checksum, key.type->to_string(*key.column, row));
                hot_checksum = update_checksum(hot_checksum, hot.type->to_string(*hot.column, row));
                sparse_checksum = update_checksum(sparse_checksum,
                                                  sparse.type->to_string(*sparse.column, row));
                hot_hits += !hot.column->is_null_at(row);
                sparse_hits += !sparse.column->is_null_at(row);
            }
            rows += static_cast<uint32_t>(block.rows());
        }
        if (!saw_current_compactor || rows != _total_rows || hot_hits != _total_rows ||
            sparse_hits != _expected_sparse_hits || whole_checksum != _expected_whole_checksum ||
            key_checksum != _expected_key_checksum || hot_checksum != _expected_hot_checksum ||
            sparse_checksum != _expected_sparse_checksum) {
            return Status::InternalError(
                    "Compaction oracle mismatch: rows={}/{}, hot_hits={}/{}, sparse_hits={}/{}, "
                    "whole_checksum={}/{}, key_checksum={}/{}, hot_checksum={}/{}, "
                    "sparse_checksum={}/{}",
                    rows, _total_rows, hot_hits, _total_rows, sparse_hits, _expected_sparse_hits,
                    whole_checksum, _expected_whole_checksum, key_checksum, _expected_key_checksum,
                    hot_checksum, _expected_hot_checksum, sparse_checksum,
                    _expected_sparse_checksum);
        }
        return Status::OK();
    }

    uint32_t _total_rows;
    uint32_t _rows_per_rowset;
    uint32_t _input_segments = 0;
    uint64_t _fixture_id;
    std::string _directory;
    std::string _tmp_directory;
    bool _previous_ordered_compaction;
    bool _previous_compaction_checksum;
    bool _previous_vertical_compaction;
    bool _previous_vertical_variant_compaction;
    bool _installed_runtime = false;
    std::unique_ptr<BaseStorageEngine> _previous_storage_engine;
    std::unique_ptr<segment_v2::TmpFileDirs> _previous_tmp_file_dirs;
    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSchemaSPtr _schema;
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _input_rowsets;
    std::vector<ino_t> _input_inodes;
    std::unique_ptr<CumulativeCompaction> _compaction;
    uint64_t _input_json_bytes = 0;
    uint64_t _input_disk_bytes = 0;
    uint64_t _expected_whole_checksum = FNV_OFFSET;
    uint64_t _expected_key_checksum = FNV_OFFSET;
    uint64_t _expected_hot_checksum = FNV_OFFSET;
    uint64_t _expected_sparse_checksum = FNV_OFFSET;
    uint32_t _expected_sparse_hits = 0;
};

bool benchmark_status(benchmark::State& state, const Status& status) {
    if (status.ok()) {
        return true;
    }
    const std::string message = status.to_string();
    state.SkipWithError(message);
    return false;
}

void add_common_counters(benchmark::State& state, const VariantSegmentBenchmarkData& data) {
    state.counters["batch_rows"] =
            benchmark::Counter(BATCH_ROWS, benchmark::Counter::kIsIterationInvariant);
    state.counters["rows_per_run"] =
            benchmark::Counter(data.rows(), benchmark::Counter::kIsIterationInvariant);
    state.counters["rows_per_segment"] =
            benchmark::Counter(data.rows_per_segment(), benchmark::Counter::kIsIterationInvariant);
    state.counters["segment_count"] =
            benchmark::Counter(data.segment_count(), benchmark::Counter::kIsIterationInvariant);
    state.counters["input_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(data.input_bytes()) / data.rows(),
                               benchmark::Counter::kIsIterationInvariant);
}

void BM_VariantIngestToSegment(benchmark::State& state, VariantLayout layout,
                               VariantVersion version) {
    VariantSegmentBenchmarkData* data = nullptr;
    std::string path;
    WriteResult result;
    std::vector<uint64_t> segment_bytes;
    LayoutCounts measured_counts;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        state.PauseTiming();
        data = &VariantSegmentBenchmarkData::instance();
        Status status = data->status();
        if (status.ok()) {
            status = data->ensure_writer_warmup(layout, version, nullptr);
        }
        if (status.ok()) {
            path = data->measured_segment_path(layout, version, state.name());
            status = data->delete_segment_files(path);
        }
        state.ResumeTiming();
        if (!benchmark_status(state, status)) {
            break;
        }
        result = WriteResult {};
        status = data->write_segments(layout, version, path, &state, &result, &segment_bytes);
        if (!benchmark_status(state, status)) {
            break;
        }
        benchmark::DoNotOptimize(result.segment_bytes);
        state.PauseTiming();
        status = data->validate_written_segments(layout, version, path, segment_bytes,
                                                 &measured_counts, nullptr);
        state.ResumeTiming();
        if (!benchmark_status(state, status)) {
            break;
        }
        completed = true;
    }
    if (!completed) {
        return;
    }
    add_common_counters(state, *data);
    state.counters["whole_validation_sample_rows"] = benchmark::Counter(
            data->whole_validation_sample_rows(), benchmark::Counter::kIsIterationInvariant);
    state.counters["whole_validation_sample_stride"] =
            benchmark::Counter(WHOLE_VALIDATION_STRIDE, benchmark::Counter::kIsIterationInvariant);
    state.counters["whole_validation_row_count_full"] =
            benchmark::Counter(1, benchmark::Counter::kIsIterationInvariant);
    state.counters["whole_validation_footer_layout_full"] =
            benchmark::Counter(1, benchmark::Counter::kIsIterationInvariant);
    state.counters["whole_validation_route_full_scan"] =
            benchmark::Counter(1, benchmark::Counter::kIsIterationInvariant);
    state.counters["whole_validation_canonical_sampled"] =
            benchmark::Counter(1, benchmark::Counter::kIsIterationInvariant);
    state.SetItemsProcessed(static_cast<int64_t>(data->rows()) * state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(data->input_bytes()) * state.iterations());
    state.counters["segment_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(result.segment_bytes) / data->rows(),
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["index_bytes"] = benchmark::Counter(static_cast<double>(result.index_bytes),
                                                       benchmark::Counter::kIsIterationInvariant);
    state.counters["init_ms"] = result.init_ns / 1e6;
    state.counters["parse_ms"] = result.parse_ns / 1e6;
    state.counters["append_ms"] = result.append_ns / 1e6;
    state.counters["finalize_ms"] = result.finalize_ns / 1e6;
    state.counters["materialized_columns"] = measured_counts.materialized;
    state.counters["sparse_columns"] = measured_counts.sparse;
    state.counters["doc_columns"] = measured_counts.doc;
    const ParseConfig parse_config = data->v1_parse_config(layout);
    state.counters["variant_storage_parse_mode"] = config::variant_storage_parse_mode;
    state.counters["v1_parse_to_doc_value"] =
            parse_config.parse_to == ParseConfig::ParseTo::OnlyDocValueColumn;
}

void BM_VariantRead(benchmark::State& state, VariantLayout layout, ReadTarget target,
                    VariantVersion version) {
    VariantSegmentBenchmarkData* data = nullptr;
    PreparedSegment* fixture = nullptr;
    PreparedScan prepared;
    ScanResult result;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        state.PauseTiming();
        data = &VariantSegmentBenchmarkData::instance();
        Status status = data->status();
        if (status.ok()) {
            status = data->ensure_read_validation(layout, target, &fixture);
        }
        if (status.ok()) {
            status = data->prepare_scan(fixture, target, version, &prepared);
        }
        state.ResumeTiming();
        if (!benchmark_status(state, status)) {
            break;
        }
        result = ScanResult {};
        status = data->scan_segment(prepared, false, data->rows(), &result);
        if (!benchmark_status(state, status)) {
            break;
        }
        completed = true;
    }
    if (!completed) {
        return;
    }
    add_common_counters(state, *data);
    state.counters["prevalidation_whole_sample_rows_per_representation"] = benchmark::Counter(
            target == ReadTarget::WHOLE ? data->whole_validation_sample_rows() : 0,
            benchmark::Counter::kIsIterationInvariant);
    state.counters["prevalidation_whole_sample_stride"] =
            target == ReadTarget::WHOLE ? WHOLE_VALIDATION_STRIDE : 0;
    state.counters["prevalidation_exact_oracle_full_rows_per_representation"] =
            benchmark::Counter(target == ReadTarget::WHOLE ? 0 : data->rows(),
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["prevalidation_representation_scans"] = 2;
    state.counters["prevalidation_row_count_full"] = 1;
    state.counters["prevalidation_footer_layout_full"] = 1;
    state.counters["prevalidation_route_full_scan"] = 1;
    state.counters["prevalidation_canonical_sampled"] = target == ReadTarget::WHOLE ? 1 : 0;
    state.counters["prevalidation_checksum_full"] = target == ReadTarget::WHOLE ? 0 : 1;
    state.SetItemsProcessed(static_cast<int64_t>(result.rows));
    if (target == ReadTarget::WHOLE) {
        state.SetBytesProcessed(static_cast<int64_t>(data->input_bytes()));
    }
    state.counters["output_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(result.output_bytes) / result.rows,
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["segment_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(fixture->segment_bytes) / data->rows(),
                               benchmark::Counter::kIsIterationInvariant);
    const uint32_t expected_hits = data->expected_hits(target);
    state.counters["hit_count"] = expected_hits;
    state.counters["hit_rate"] = static_cast<double>(expected_hits) / result.rows;
    state.counters["variant_storage_parse_mode"] = config::variant_storage_parse_mode;
    state.counters["route_leaf"] = result.statistics.variant_subtree_leaf_iter_count;
    state.counters["route_binary_extract"] = result.statistics.variant_subtree_sparse_iter_count;
    state.counters["route_default_fill"] = result.statistics.variant_subtree_default_iter_count;
    state.counters["route_hierarchical"] =
            result.statistics.variant_subtree_hierarchical_iter_count;
    state.counters["materialized_columns"] = fixture->counts.materialized;
    state.counters["sparse_columns"] = fixture->counts.sparse;
    state.counters["doc_columns"] = fixture->counts.doc;
}

void BM_VariantScanAndRewriteSegment(benchmark::State& state, VariantLayout layout,
                                     VariantVersion version) {
    VariantSegmentBenchmarkData* data = nullptr;
    PreparedSegment* source = nullptr;
    PreparedScan prepared;
    RewriteResult result;
    LayoutCounts destination_counts;
    std::string destination_path;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        state.PauseTiming();
        data = &VariantSegmentBenchmarkData::instance();
        Status status = data->status();
        if (status.ok()) {
            status = data->ensure_read_validation(layout, ReadTarget::WHOLE, &source);
        }
        if (status.ok()) {
            status = data->prepare_rewrite_scan(source, version, &prepared);
        }
        if (status.ok()) {
            destination_path = data->measured_segment_path(layout, version, state.name());
            status = data->delete_segment_files(destination_path);
        }
        state.ResumeTiming();
        if (!benchmark_status(state, status)) {
            break;
        }

        result = RewriteResult {};
        status = data->rewrite_segment(prepared, layout, version, destination_path, &result);
        if (!benchmark_status(state, status)) {
            break;
        }

        state.PauseTiming();
        std::vector<uint64_t> destination_bytes {result.destination_segment_bytes};
        status = data->validate_written_segments(layout, version, destination_path,
                                                 destination_bytes, &destination_counts, nullptr);
        state.ResumeTiming();
        if (!benchmark_status(state, status)) {
            break;
        }
        completed = true;
    }
    if (!completed) {
        return;
    }

    add_common_counters(state, *data);
    state.SetItemsProcessed(static_cast<int64_t>(result.rows) * state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(result.source_segment_bytes) * state.iterations());
    state.counters["scan_init_ms"] = result.scan_init_ns / 1e6;
    state.counters["writer_init_ms"] = result.writer_init_ns / 1e6;
    state.counters["read_ms"] = result.read_ns / 1e6;
    state.counters["append_ms"] = result.append_ns / 1e6;
    state.counters["finalize_ms"] = result.finalize_ns / 1e6;
    state.counters["stage_sum_ms"] = (result.scan_init_ns + result.writer_init_ns + result.read_ns +
                                      result.append_ns + result.finalize_ns) /
                                     1e6;
    state.counters["read_output_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(result.read_output_bytes) / result.rows,
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["source_segment_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(result.source_segment_bytes) / result.rows,
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["destination_segment_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(result.destination_segment_bytes) / result.rows,
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["destination_index_bytes"] = result.destination_index_bytes;
    state.counters["source_writer_variant_v1"] = 1;
    state.counters["query_output_variant_v2"] = version == VariantVersion::V2;
    state.counters["destination_input_variant_v2"] = version == VariantVersion::V2;
    state.counters["destination_validation_row_count_full"] = 1;
    state.counters["destination_validation_footer_layout_full"] = 1;
    state.counters["destination_validation_canonical_sampled"] = 1;
    state.counters["destination_validation_route_full_scan"] = 1;
    state.counters["source_prevalidated"] = 1;
    state.counters["source_page_cache"] = 1;
    state.counters["variant_storage_parse_mode"] = config::variant_storage_parse_mode;
    state.counters["route_hierarchical"] =
            result.statistics.variant_subtree_hierarchical_iter_count;
    state.counters["materialized_columns"] = destination_counts.materialized;
    state.counters["sparse_columns"] = destination_counts.sparse;
    state.counters["doc_columns"] = destination_counts.doc;
}

void BM_VariantCumulativeCompaction(benchmark::State& state) {
    CompactionResult result;
    uint32_t total_rows = 0;
    uint32_t rows_per_rowset = 0;
    uint32_t input_segments = 0;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        state.PauseTiming();
        auto fixture = std::make_unique<VariantCompactionBenchmarkFixture>();
        Status status = fixture->prepare();
        if (status.ok()) {
            total_rows = fixture->total_rows();
            rows_per_rowset = fixture->rows_per_rowset();
            input_segments = fixture->input_segments();
        }
        CumulativeCompaction* compaction = status.ok() ? fixture->compaction() : nullptr;
        state.ResumeTiming();
        if (!benchmark_status(state, status)) {
            break;
        }

        status = compaction->execute_compact();

        state.PauseTiming();
        if (status.ok()) {
            status = fixture->validate(&result);
        }
        const bool ok = benchmark_status(state, status);
        fixture.reset();
        state.ResumeTiming();
        if (!ok) {
            break;
        }
        completed = true;
    }
    if (!completed) {
        return;
    }

    state.SetItemsProcessed(static_cast<int64_t>(total_rows) * state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(result.input_disk_bytes) * state.iterations());
    state.counters["rows_per_run"] =
            benchmark::Counter(total_rows, benchmark::Counter::kIsIterationInvariant);
    state.counters["input_rowsets"] =
            benchmark::Counter(COMPACTION_INPUT_ROWSETS, benchmark::Counter::kIsIterationInvariant);
    state.counters["input_segments"] =
            benchmark::Counter(input_segments, benchmark::Counter::kIsIterationInvariant);
    state.counters["rows_per_input_rowset"] =
            benchmark::Counter(rows_per_rowset, benchmark::Counter::kIsIterationInvariant);
    state.counters["merge_ways"] =
            benchmark::Counter(COMPACTION_INPUT_ROWSETS, benchmark::Counter::kIsIterationInvariant);
    state.counters["input_key_ranges_overlap"] = 1;
    state.counters["input_json_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(result.input_json_bytes) / total_rows,
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["input_disk_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(result.input_disk_bytes) / total_rows,
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["output_disk_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(result.output_disk_bytes) / total_rows,
                               benchmark::Counter::kIsIterationInvariant);
    state.counters["output_segments"] = result.output_segments;
    state.counters["materialized_columns"] = result.output_layout.materialized;
    state.counters["sparse_columns"] = result.output_layout.sparse;
    state.counters["doc_columns"] = result.output_layout.doc;
    state.counters["input_writer_variant_v2"] = 1;
    state.counters["persisted_compaction_variant_v2"] = 0;
    state.counters["ordered_link_fast_path"] = 0;
    state.counters["vertical_compaction"] = 1;
    state.counters["vertical_variant_subcolumns"] = 1;
    state.counters["compaction_batch_size"] = config::compaction_batch_size;
    state.counters["vertical_columns_per_group"] =
            config::vertical_compaction_num_columns_per_group;
    state.counters["fresh_input_fixture"] = 1;
    state.counters["input_footers_prevalidated"] = 1;
    state.counters["variant_storage_parse_mode"] = config::variant_storage_parse_mode;
}

constexpr std::array<VariantVersion, 10> ABBA_ORDER {
        VariantVersion::V1, VariantVersion::V2, VariantVersion::V2, VariantVersion::V1,
        VariantVersion::V2, VariantVersion::V1, VariantVersion::V1, VariantVersion::V2,
        VariantVersion::V1, VariantVersion::V2,
};

template <typename Register>
void register_abba_pair(std::string_view prefix, bool reverse, Register&& register_one) {
    std::array<int, 2> sample {};
    for (VariantVersion ordered : ABBA_ORDER) {
        VariantVersion version = ordered;
        if (reverse) {
            version = ordered == VariantVersion::V1 ? VariantVersion::V2 : VariantVersion::V1;
        }
        const auto version_index = static_cast<size_t>(version);
        const std::string name = std::string(prefix) + "/sample" +
                                 std::to_string(++sample[version_index]) + "_" +
                                 std::string(version_name(version));
        register_one(name, version)->Unit(benchmark::kMillisecond)->Iterations(1)->UseRealTime();
    }
}

bool register_variant_segment_benchmarks() {
    const std::array<VariantLayout, 3> layouts {VariantLayout::SPARSE16, VariantLayout::DOC16,
                                                VariantLayout::FULL};
    size_t pair_index = 0;
    for (VariantLayout layout : layouts) {
        const std::string prefix =
                "BM_VariantIngestToSegment/" + std::string(layout_config(layout).name);
        register_abba_pair(prefix, pair_index++ % 2 != 0,
                           [layout](const std::string& name, VariantVersion version) {
                               return benchmark::RegisterBenchmark(
                                       name, [layout, version](benchmark::State& state) {
                                           BM_VariantIngestToSegment(state, layout, version);
                                       });
                           });
    }
    for (VariantLayout layout : layouts) {
        const std::string prefix =
                "BM_VariantReadWholeColumn/" + std::string(layout_config(layout).name);
        register_abba_pair(prefix, pair_index++ % 2 != 0,
                           [layout](const std::string& name, VariantVersion version) {
                               return benchmark::RegisterBenchmark(
                                       name, [layout, version](benchmark::State& state) {
                                           BM_VariantRead(state, layout, ReadTarget::WHOLE,
                                                          version);
                                       });
                           });
    }
    const std::array<ReadTarget, 3> exact_targets {ReadTarget::MATERIALIZED, ReadTarget::SPARSE,
                                                   ReadTarget::MISSING};
    for (ReadTarget target : exact_targets) {
        const std::string prefix =
                "BM_VariantReadExactPath/Sparse16/" + std::string(target_name(target));
        register_abba_pair(prefix, pair_index++ % 2 != 0,
                           [target](const std::string& name, VariantVersion version) {
                               return benchmark::RegisterBenchmark(
                                       name, [target, version](benchmark::State& state) {
                                           BM_VariantRead(state, VariantLayout::SPARSE16, target,
                                                          version);
                                       });
                           });
    }
    for (VariantLayout layout : layouts) {
        const std::string prefix = "BM_VariantScanAndRewriteSegment/" +
                                   std::string(layout_config(layout).name) + "/V1WrittenSource";
        register_abba_pair(prefix, pair_index++ % 2 != 0,
                           [layout](const std::string& name, VariantVersion version) {
                               return benchmark::RegisterBenchmark(
                                       name, [layout, version](benchmark::State& state) {
                                           BM_VariantScanAndRewriteSegment(state, layout, version);
                                       });
                           });
    }
    constexpr uint32_t COMPACTION_SAMPLES = 5;
    for (uint32_t sample = 1; sample <= COMPACTION_SAMPLES; ++sample) {
        const std::string name =
                "BM_VariantCumulativeCompaction/Overlap10Way/FreshInput/"
                "Sparse16/InputWrittenByV2/ProductionCurrent/sample" +
                std::to_string(sample);
        benchmark::RegisterBenchmark(name, BM_VariantCumulativeCompaction)
                ->Unit(benchmark::kMillisecond)
                ->Iterations(1)
                ->UseRealTime();
    }
    return true;
}

inline const bool VARIANT_SEGMENT_BENCHMARKS_REGISTERED = register_variant_segment_benchmarks();

} // namespace
} // namespace doris::variant_segment_benchmark
