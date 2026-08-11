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
#include <unistd.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
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
#include "storage/olap_common.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::variant_segment_benchmark {
namespace {

// P0 workload: 200 top-level BIGINT paths, with one hot path and 29 additional paths per row.
// Ingest cases parse the same canonical JSON before SegmentWriter. Read cases use the same
// V1-written physical segment and only switch the requested V1/V2 output representation. Input
// generation, warmup, checksum, physical-layout validation, and route validation are paused.
constexpr uint32_t DEFAULT_ROWS = 1'000'000;
constexpr uint32_t BATCH_ROWS = 4'096;
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
    SchemaSPtr scan_schema;
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
    uint64_t input_bytes() const { return _input_bytes; }
    std::string measured_segment_path(VariantLayout layout, VariantVersion version,
                                      std::string benchmark_name) const {
        std::replace(benchmark_name.begin(), benchmark_name.end(), '/', '_');
        return _directory + "/measured_" + std::string(layout_config(layout).name) + "_" +
               std::string(version_name(version)) + "_" + benchmark_name + ".dat";
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
        const auto layout_index = static_cast<size_t>(layout);
        const auto version_index = static_cast<size_t>(version);
        if (_writer_attempted[layout_index][version_index]) {
            if (!_writer_errors[layout_index][version_index].empty()) {
                return Status::InternalError(_writer_errors[layout_index][version_index]);
            }
            *prepared = &_writer_segments[layout_index][version_index];
            return Status::OK();
        }
        _writer_attempted[layout_index][version_index] = true;

        PreparedSegment result;
        result.schema = _schemas[layout_index];
        result.path = _directory + "/warm_" + std::string(layout_config(layout).name) + "_" +
                      std::string(version_name(version)) + ".dat";
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(result.path));
        WriteResult write_result;
        Status status = write_segment(layout, version, result.path, &write_result);
        if (status.ok()) {
            result.segment_bytes = write_result.segment_bytes;
            status = open_and_validate_segment(layout, &result);
        }
        if (status.ok()) {
            PreparedScan scan;
            status = prepare_scan(&result, ReadTarget::WHOLE, version, &scan);
            ScanResult scan_result;
            if (status.ok()) {
                status = scan_segment(scan, true, &scan_result);
            }
            const auto expected = expected_checksum(ReadTarget::WHOLE);
            if (status.ok() && scan_result.checksum != expected) {
                status = Status::InternalError(
                        "{} {} root checksum {} differs from input checksum {}",
                        layout_config(layout).name, version_name(version), scan_result.checksum,
                        expected);
            }
        }
        if (!status.ok()) {
            _writer_errors[layout_index][version_index] = status.to_string();
            return status;
        }
        _writer_segments[layout_index][version_index] = std::move(result);
        *prepared = &_writer_segments[layout_index][version_index];
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
        if (status.ok()) {
            status = scan_segment(v1_scan, true, &v1_result);
        }
        if (status.ok()) {
            status = scan_segment(v2_scan, true, &v2_result);
        }
        if (status.ok() && (v1_result.rows != v2_result.rows || v1_result.hits != v2_result.hits)) {
            status = Status::InternalError("{} {} V1/V2 mismatch: rows {}/{}, hits {}/{}",
                                           layout_config(layout).name, target_name(target),
                                           v1_result.rows, v2_result.rows, v1_result.hits,
                                           v2_result.hits);
        }
        const auto expected = expected_checksum(target);
        if (status.ok() && (v1_result.checksum != expected || v2_result.checksum != expected)) {
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
        prepared->scan_schema = std::make_shared<Schema>(prepared->query_schema->columns(),
                                                         std::vector<ColumnId> {output_id});
        prepared->output_column_id = output_id;
        prepared->target = target;
        return Status::OK();
    }

    Status write_segment(VariantLayout layout, VariantVersion version, const std::string& path,
                         WriteResult* result) const {
        DORIS_CHECK(result != nullptr);
        const TabletSchemaSPtr& schema = _schemas[static_cast<size_t>(layout)];

        const auto init_start = std::chrono::steady_clock::now();
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &file_writer));
        RowsetWriterContext rowset_context;
        rowset_context.write_type = DataWriteType::TYPE_DIRECT;
        rowset_context.tablet_schema = schema;
        rowset_context.tablet_path = _directory;

        segment_v2::SegmentWriterOptions options;
        options.num_rows_per_block = BATCH_ROWS;
        options.max_rows_per_segment = _rows;
        options.compression_type = CompressionTypePB::LZ4;
        options.rowset_ctx = &rowset_context;
        options.write_type = DataWriteType::TYPE_DIRECT;

        segment_v2::SegmentWriter writer(file_writer.get(), 0, schema, nullptr, nullptr, options,
                                         nullptr);
        RETURN_IF_ERROR(writer.init());
        result->init_ns = elapsed_ns(init_start);

        const auto key_type = std::make_shared<DataTypeInt64>();
        for (size_t batch = 0; batch < _key_batches.size(); ++batch) {
            ColumnPtr variant_column;
            DataTypePtr variant_type;
            const auto parse_start = std::chrono::steady_clock::now();
            RETURN_IF_ERROR(parse_batch(layout, version, batch, &variant_column, &variant_type));
            result->parse_ns += elapsed_ns(parse_start);

            Block block;
            block.insert({_key_batches[batch], key_type, "k"});
            block.insert(
                    {std::move(variant_column), std::move(variant_type), std::string(ROOT_NAME)});
            const auto append_start = std::chrono::steady_clock::now();
            RETURN_IF_ERROR(writer.append_block(&block, 0, _batch_sizes[batch]));
            result->append_ns += elapsed_ns(append_start);
        }

        const auto finalize_start = std::chrono::steady_clock::now();
        RETURN_IF_ERROR(writer.finalize(&result->segment_bytes, &result->index_bytes));
        result->finalize_ns = elapsed_ns(finalize_start);
        return Status::OK();
    }

    Status validate_measured_segment(VariantLayout layout, VariantVersion version,
                                     const std::string& path, uint64_t segment_bytes,
                                     LayoutCounts* counts) const {
        DORIS_CHECK(counts != nullptr);
        PreparedSegment measured;
        measured.schema = _schemas[static_cast<size_t>(layout)];
        measured.path = path;
        measured.segment_bytes = segment_bytes;
        RETURN_IF_ERROR(open_and_validate_segment(layout, &measured));

        PreparedScan scan;
        RETURN_IF_ERROR(prepare_scan(&measured, ReadTarget::WHOLE, version, &scan));
        ScanResult scan_result;
        RETURN_IF_ERROR(scan_segment(scan, true, &scan_result));
        const uint64_t expected = expected_checksum(ReadTarget::WHOLE);
        if (scan_result.checksum != expected) {
            return Status::InternalError(
                    "{} {} measured checksum {} differs from input checksum {}",
                    layout_config(layout).name, version_name(version), scan_result.checksum,
                    expected);
        }

        *counts = measured.counts;
        measured.segment.reset();
        return io::global_local_filesystem()->delete_file(path);
    }

    Status scan_segment(const PreparedScan& prepared, bool checksum, ScanResult* result) const {
        DORIS_CHECK(prepared.fixture != nullptr);
        DORIS_CHECK(result != nullptr);
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
        Block block = prepared.query_schema->create_block_by_cids(
                {static_cast<uint32_t>(prepared.output_column_id)});
        while (true) {
            Status status = iterator->next_batch(&block);
            if (status.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            RETURN_IF_ERROR(status);
            const auto& output = block.get_by_position(0);
            result->rows += static_cast<uint32_t>(block.rows());
            result->output_bytes += block.bytes();
            if (prepared.target != ReadTarget::WHOLE) {
                for (size_t row = 0; row < block.rows(); ++row) {
                    result->hits += !output.column->is_null_at(row);
                }
            }
            if (checksum) {
                for (size_t row = 0; row < block.rows(); ++row) {
                    result->checksum = update_checksum(result->checksum,
                                                       output.type->to_string(*output.column, row));
                }
            } else {
                benchmark::DoNotOptimize(result->output_bytes);
            }
            block.clear_column_data();
        }
        if (result->rows != _rows) {
            return Status::InternalError("Variant benchmark read {} rows, expected {}",
                                         result->rows, _rows);
        }
        return Status::OK();
    }

private:
    VariantSegmentBenchmarkData()
            : _rows(configured_rows()),
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
        build_input();
        return Status::OK();
    }

    uint64_t expected_checksum(ReadTarget target) const {
        return _expected_checksums[static_cast<size_t>(target)];
    }

    TabletSchemaSPtr make_schema(VariantLayout layout) const {
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

    void build_input() {
        _raw_json = ColumnString::create();
        _raw_json->reserve(_rows);
        std::array<std::string, COLD_PATHS> cold_keys;
        for (uint32_t cold = 0; cold < COLD_PATHS; ++cold) {
            std::string suffix = std::to_string(cold);
            suffix.insert(0, 3 - suffix.size(), '0');
            cold_keys[cold] = "cold_" + suffix;
        }

        auto key_batch = ColumnInt64::create();
        key_batch->reserve(BATCH_ROWS);
        std::array<uint16_t, FIELDS_PER_ROW - 1> cold_ids {};
        for (uint32_t row = 0; row < _rows; ++row) {
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
            _input_bytes += json.size();
            _expected_checksums[static_cast<size_t>(ReadTarget::WHOLE)] =
                    update_checksum(expected_checksum(ReadTarget::WHOLE), json);
            _expected_checksums[static_cast<size_t>(ReadTarget::MATERIALIZED)] = update_checksum(
                    expected_checksum(ReadTarget::MATERIALIZED), std::to_string(row));
            _expected_checksums[static_cast<size_t>(ReadTarget::SPARSE)] = update_checksum(
                    expected_checksum(ReadTarget::SPARSE),
                    include_sparse ? std::to_string(static_cast<uint64_t>(row) * 17 + 3) : "NULL");
            _expected_checksums[static_cast<size_t>(ReadTarget::MISSING)] =
                    update_checksum(expected_checksum(ReadTarget::MISSING), "NULL");
            _raw_json->insert_data(json.data(), json.size());

            key_batch->insert_value(row);
            if (key_batch->size() == BATCH_ROWS || row + 1 == _rows) {
                _batch_sizes.push_back(static_cast<uint32_t>(key_batch->size()));
                _key_batches.emplace_back(std::move(key_batch));
                key_batch = ColumnInt64::create();
                key_batch->reserve(BATCH_ROWS);
            }
        }
        DORIS_CHECK_EQ(_raw_json->size(), _rows);
        DORIS_CHECK_EQ(_key_batches.size(), _batch_sizes.size());
    }

    Status parse_batch(VariantLayout layout, VariantVersion version, size_t batch,
                       ColumnPtr* column, DataTypePtr* type) const {
        DORIS_CHECK(column != nullptr);
        DORIS_CHECK(type != nullptr);
        const LayoutConfig config = layout_config(layout);
        const size_t begin = batch * BATCH_ROWS;
        const size_t rows = _batch_sizes[batch];
        if (version == VariantVersion::V1) {
            auto values = ColumnVariant::create(config.max_subcolumns, config.doc_mode);
            const ParseConfig parse_config = v1_parse_config(layout);
            JsonParser parser;
            RETURN_IF_CATCH_EXCEPTION({
                for (size_t row = 0; row < rows; ++row) {
                    variant_util::parse_json_to_variant(
                            *values, _raw_json->get_data_at(begin + row), &parser, parse_config);
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
                    encoder.add_json(_raw_json->get_data_at(begin + row));
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

    Status open_and_validate_segment(VariantLayout layout, PreparedSegment* prepared) const {
        DORIS_CHECK(prepared != nullptr);
        RowsetId rowset_id;
        rowset_id.init(10'000 + static_cast<int64_t>(layout));
        RETURN_IF_ERROR(segment_v2::Segment::open(io::global_local_filesystem(), prepared->path,
                                                  20'000 + static_cast<int64_t>(layout), 0,
                                                  rowset_id, prepared->schema,
                                                  io::FileReaderOptions {}, &prepared->segment));
        if (prepared->segment->num_rows() != _rows) {
            return Status::InternalError("{} segment has {} rows, expected {}",
                                         layout_config(layout).name, prepared->segment->num_rows(),
                                         _rows);
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
        uint32_t expected = _rows;
        if (target == ReadTarget::SPARSE) {
            expected = (_rows + SPARSE_HIT_PERIOD - 1) / SPARSE_HIT_PERIOD;
        } else if (target == ReadTarget::MISSING) {
            expected = 0;
        }
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
    std::string _directory;
    Status _status = Status::OK();
    ColumnString::MutablePtr _raw_json;
    std::vector<ColumnPtr> _key_batches;
    std::vector<uint32_t> _batch_sizes;
    uint64_t _input_bytes = 0;
    std::array<uint64_t, 4> _expected_checksums {FNV_OFFSET, FNV_OFFSET, FNV_OFFSET, FNV_OFFSET};
    std::array<TabletSchemaSPtr, 3> _schemas;
    std::array<std::array<PreparedSegment, 2>, 3> _writer_segments;
    std::array<std::array<bool, 2>, 3> _writer_attempted {};
    std::array<std::array<std::string, 2>, 3> _writer_errors;
    std::array<std::array<bool, 4>, 3> _read_attempted {};
    std::array<std::array<std::string, 4>, 3> _read_errors;
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
    state.counters["input_bytes_per_row"] =
            benchmark::Counter(static_cast<double>(data.input_bytes()) / data.rows(),
                               benchmark::Counter::kIsIterationInvariant);
}

void BM_VariantIngestToSegment(benchmark::State& state, VariantLayout layout,
                               VariantVersion version) {
    VariantSegmentBenchmarkData* data = nullptr;
    PreparedSegment* warmup = nullptr;
    std::string path;
    WriteResult result;
    LayoutCounts measured_counts;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        state.PauseTiming();
        data = &VariantSegmentBenchmarkData::instance();
        Status status = data->status();
        if (status.ok()) {
            status = data->ensure_writer_warmup(layout, version, &warmup);
        }
        if (status.ok()) {
            path = data->measured_segment_path(layout, version, state.name());
            status = io::global_local_filesystem()->delete_file(path);
        }
        state.ResumeTiming();
        if (!benchmark_status(state, status)) {
            break;
        }
        result = WriteResult {};
        status = data->write_segment(layout, version, path, &result);
        if (!benchmark_status(state, status)) {
            break;
        }
        benchmark::DoNotOptimize(result.segment_bytes);
        state.PauseTiming();
        status = data->validate_measured_segment(layout, version, path, result.segment_bytes,
                                                 &measured_counts);
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
        status = data->scan_segment(prepared, false, &result);
        if (!benchmark_status(state, status)) {
            break;
        }
        completed = true;
    }
    if (!completed) {
        return;
    }
    add_common_counters(state, *data);
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
    state.counters["hit_count"] = result.hits;
    state.counters["hit_rate"] = static_cast<double>(result.hits) / result.rows;
    state.counters["route_leaf"] = result.statistics.variant_subtree_leaf_iter_count;
    state.counters["route_binary_extract"] = result.statistics.variant_subtree_sparse_iter_count;
    state.counters["route_default_fill"] = result.statistics.variant_subtree_default_iter_count;
    state.counters["route_hierarchical"] =
            result.statistics.variant_subtree_hierarchical_iter_count;
    state.counters["materialized_columns"] = fixture->counts.materialized;
    state.counters["sparse_columns"] = fixture->counts.sparse;
    state.counters["doc_columns"] = fixture->counts.doc;
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
    return true;
}

inline const bool VARIANT_SEGMENT_BENCHMARKS_REGISTERED = register_variant_segment_benchmarks();

} // namespace
} // namespace doris::variant_segment_benchmark
