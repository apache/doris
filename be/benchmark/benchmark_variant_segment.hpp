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
#include <rapidjson/document.h>
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
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "exec/common/variant_util.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
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
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_column_object_pool.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_cache.h"

namespace doris::variant_segment_benchmark {
namespace {

// Run this single workload on separate Git revisions. It deliberately has no Variant-version
// selector or physical-column assertion.
constexpr uint32_t WORKLOAD_REVISION = 3;
constexpr uint32_t DEFAULT_ROWS = 1'000'000;
constexpr uint32_t BATCH_ROWS = 4'096;
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
constexpr std::string_view ALTERNATE_HOT_PATH = "hot_alternate";
constexpr std::string_view SPARSE_PATH = "sparse_target";
constexpr uint64_t FNV_OFFSET = 1469598103934665603ULL;
constexpr uint64_t FNV_PRIME = 1099511628211ULL;

enum class VariantLayout : uint8_t { SPARSE16, DOC16, FULL };
enum class VariantWorkload : uint8_t { FLAT, MIXED_PLACEMENT };

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

struct CompactionScenario {
    VariantLayout layout;
    VariantWorkload workload;
    std::string_view name;
    bool compact_variant_subcolumns;
    bool materialize_doc = false;
};

struct CompactionResult {
    uint64_t input_json_bytes = 0;
    uint64_t input_disk_bytes = 0;
    uint64_t output_disk_bytes = 0;
    uint32_t output_segments = 0;
    LayoutCounts output_layout;
    OlapReaderStatistics validation_statistics;
};

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

Status validate_layout(const CompactionScenario& scenario, const LayoutCounts& counts) {
    switch (scenario.layout) {
    case VariantLayout::SPARSE16:
        if (counts.materialized != 1 || counts.sparse != BUCKETS || counts.doc != 0) {
            return Status::InternalError("Sparse16 layout is materialized={}, sparse={}, doc={}",
                                         counts.materialized, counts.sparse, counts.doc);
        }
        break;
    case VariantLayout::DOC16: {
        const uint32_t expected_materialized = scenario.materialize_doc ? CANDIDATE_PATHS : 0;
        if (counts.materialized != expected_materialized || counts.sparse != 0 ||
            counts.doc != BUCKETS) {
            return Status::InternalError("Doc16 layout is materialized={}, sparse={}, doc={}",
                                         counts.materialized, counts.sparse, counts.doc);
        }
        break;
    }
    case VariantLayout::FULL:
        if (counts.materialized != CANDIDATE_PATHS || counts.sparse != BUCKETS || counts.doc != 0) {
            return Status::InternalError("Full layout is materialized={}, sparse={}, doc={}",
                                         counts.materialized, counts.sparse, counts.doc);
        }
        break;
    }
    return Status::OK();
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
        DORIS_CHECK_GE(parsed, COMPACTION_INPUT_ROWSETS);
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

Status update_flat_object_checksum(uint64_t* checksum, std::string_view json) {
    DORIS_CHECK(checksum != nullptr);
    rapidjson::Document document;
    document.Parse(json.data(), json.size());
    if (document.HasParseError() || !document.IsObject()) {
        return Status::InternalError("Invalid flat-object JSON at offset {}",
                                     document.GetErrorOffset());
    }
    std::vector<std::pair<std::string_view, uint64_t>> fields;
    fields.reserve(document.MemberCount());
    for (const auto& member : document.GetObject()) {
        if (!member.name.IsString() ||
            (!member.value.IsUint64() &&
             !(member.value.IsInt64() && member.value.GetInt64() >= 0))) {
            return Status::InternalError("Flat-object golden only accepts unsigned integers");
        }
        fields.emplace_back(
                std::string_view(member.name.GetString(), member.name.GetStringLength()),
                member.value.IsUint64() ? member.value.GetUint64()
                                        : static_cast<uint64_t>(member.value.GetInt64()));
    }
    if (fields.size() != FIELDS_PER_ROW) {
        return Status::InternalError("Flat-object JSON has {} fields, expected {}: {}",
                                     fields.size(), FIELDS_PER_ROW, json);
    }
    std::sort(fields.begin(), fields.end());
    for (const auto& [key, value] : fields) {
        *checksum = update_checksum(*checksum, key);
        *checksum = update_checksum(*checksum, std::to_string(value));
    }
    *checksum = update_checksum(*checksum, "row");
    return Status::OK();
}

std::string make_json(VariantWorkload workload, uint32_t row) {
    static const std::array<std::string, COLD_PATHS> cold_keys = [] {
        std::array<std::string, COLD_PATHS> keys;
        for (uint32_t cold = 0; cold < COLD_PATHS; ++cold) {
            std::string suffix = std::to_string(cold);
            suffix.insert(0, 3 - suffix.size(), '0');
            keys[cold] = "cold_" + suffix;
        }
        return keys;
    }();
    std::array<uint16_t, FIELDS_PER_ROW> cold_ids {};
    const bool include_sparse = row % SPARSE_HIT_PERIOD == 0;
    bool include_alternate_hot = false;
    if (workload == VariantWorkload::MIXED_PLACEMENT) {
        const uint32_t rowset_index = row % COMPACTION_INPUT_ROWSETS;
        const uint32_t local_row = row / COMPACTION_INPUT_ROWSETS;
        if (rowset_index % 2 == 0) {
            include_alternate_hot = local_row % 2 == 0;
        } else {
            include_alternate_hot = true;
        }
    }
    const size_t cold_count = FIELDS_PER_ROW - 1 - include_alternate_hot - include_sparse;
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
    if (include_alternate_hot) {
        append_field(ALTERNATE_HOT_PATH, static_cast<uint64_t>(row) * 19 + 7);
    }
    if (include_sparse) {
        append_field(SPARSE_PATH, static_cast<uint64_t>(row) * 17 + 3);
    }
    json.push_back('}');
    DORIS_CHECK_EQ(std::count(json.begin(), json.end(), ':'), FIELDS_PER_ROW);
    return json;
}

TabletSchemaSPtr make_schema(const CompactionScenario& scenario) {
    const LayoutConfig config = layout_config(scenario.layout);
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
    variant->set_variant_doc_materialization_min_rows(
            scenario.materialize_doc ? 0 : std::numeric_limits<int64_t>::max());
    variant->set_variant_doc_hash_shard_count(BUCKETS);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);
    return schema;
}

Status parse_json_batch(ColumnPtr json, const LayoutConfig& config, uint32_t rows,
                        ColumnPtr* output, DataTypePtr* output_type) {
    DORIS_CHECK(output != nullptr);
    DORIS_CHECK(output_type != nullptr);
    const auto input_type = std::make_shared<DataTypeString>();
    auto result_type = std::make_shared<DataTypeVariant>(config.max_subcolumns, config.doc_mode);
    Block block;
    block.insert({std::move(json), input_type, "json"});
    FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
            "parse_to_variant", block.get_columns_with_type_and_name(), result_type);
    DORIS_CHECK(function != nullptr);
    block.insert({nullptr, result_type, "result"});
    auto context = FunctionContext::create_context(nullptr, result_type, {input_type});
    RETURN_IF_ERROR(function->open(context.get(), FunctionContext::FRAGMENT_LOCAL));
    RETURN_IF_ERROR(function->open(context.get(), FunctionContext::THREAD_LOCAL));
    Status status = function->execute(context.get(), block, {0}, 1, rows);
    Status close_thread = function->close(context.get(), FunctionContext::THREAD_LOCAL);
    Status close_fragment = function->close(context.get(), FunctionContext::FRAGMENT_LOCAL);
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(close_thread);
    RETURN_IF_ERROR(close_fragment);
    *output = block.get_by_position(1).column;
    *output_type = std::move(result_type);
    DORIS_CHECK_EQ((*output)->size(), rows);
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

uint64_t next_compaction_fixture_id() {
    static uint64_t next_id = 0;
    return ++next_id;
}

class VariantCompactionBenchmarkFixture {
public:
    explicit VariantCompactionBenchmarkFixture(CompactionScenario scenario)
            : _scenario(scenario),
              _total_rows(configured_rows()),
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
        if (_installed_runtime) {
            SegmentLoader* loader = ExecEnv::GetInstance()->segment_loader();
            DORIS_CHECK(loader != nullptr);
            if (_compaction != nullptr && _compaction->_output_rowset != nullptr) {
                loader->erase_segments(*_compaction->_output_rowset->rowset_meta());
            }
            for (const RowsetSharedPtr& rowset : _input_rowsets) {
                loader->erase_segments(*rowset->rowset_meta());
            }
        }
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
    uint32_t variant_subcolumn_writer_paths() const { return _variant_subcolumn_writer_paths; }
    CumulativeCompaction* compaction() const { return _compaction.get(); }

    Status prepare() {
        if (_total_rows % COMPACTION_INPUT_ROWSETS != 0) {
            return Status::InvalidArgument("Variant compaction rows {} must be divisible by {}",
                                           _total_rows, COMPACTION_INPUT_ROWSETS);
        }

        ensure_variant_compaction_runtime();
        config::enable_ordered_data_compaction = false;
        config::enable_compaction_checksum = false;
        config::enable_vertical_compaction = true;
        config::enable_vertical_compact_variant_subcolumns = _scenario.compact_variant_subcolumns;
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

        _schema = make_schema(_scenario);
        auto tablet_meta = std::make_shared<TabletMeta>(_schema);
        const int64_t tablet_id = 100'000 + static_cast<int64_t>(_fixture_id);
        tablet_meta->_tablet_id = tablet_id;
        tablet_meta->set_tablet_uid(TabletUid(tablet_id, tablet_id + 1));
        _tablet = std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get());
        RETURN_IF_ERROR(_tablet->init());
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_tablet->tablet_path()));

        RETURN_IF_ERROR(build_oracle());
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
        return initialize_compaction();
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
        RETURN_IF_ERROR(validate_physical_rewrite(output));
        RETURN_IF_ERROR(validate_rowset_layout(output, _total_rows, &result->output_layout));
        RETURN_IF_ERROR(validate_output_oracle(output, &result->validation_statistics));
        result->input_json_bytes = _input_json_bytes;
        result->input_disk_bytes = _input_disk_bytes;
        result->output_disk_bytes = output->total_disk_size();
        result->output_segments = cast_set<uint32_t>(output->num_segments());
        return Status::OK();
    }

private:
    Status initialize_compaction() {
        RETURN_IF_ERROR(validate_variant_subcolumn_writer_route());
        _compaction = std::make_unique<CumulativeCompaction>(*_engine, _tablet);
        _compaction->_input_rowsets = _input_rowsets;
        return Status::OK();
    }

    Status validate_variant_subcolumn_writer_route() {
        if (!_scenario.compact_variant_subcolumns) {
            return Status::OK();
        }
        auto compaction_schema = std::make_shared<TabletSchema>(*_schema);
        RETURN_IF_ERROR(variant_util::VariantCompactionUtil::get_extended_compaction_schema(
                _input_rowsets, compaction_schema));
        for (const TabletColumnPtr& column : compaction_schema->columns()) {
            if (column->is_extracted_column() && column->parent_unique_id() == ROOT_UID &&
                column->is_variant_type() &&
                column->name().find(DOC_VALUE_COLUMN_PATH) == std::string::npos) {
                ++_variant_subcolumn_writer_paths;
            }
        }
        const uint32_t expected = _scenario.workload == VariantWorkload::MIXED_PLACEMENT ? 1 : 0;
        if (_variant_subcolumn_writer_paths != expected) {
            return Status::InternalError(
                    "Compaction schema has {} Variant subcolumn writer paths, expected {}",
                    _variant_subcolumn_writer_paths, expected);
        }
        return Status::OK();
    }

    Status build_oracle() {
        for (uint32_t row = 0; row < _total_rows; ++row) {
            const std::string json = make_json(_scenario.workload, row);
            const std::string key = std::to_string(row);
            RETURN_IF_ERROR(update_flat_object_checksum(&_expected_whole_checksum, json));
            _expected_key_checksum = update_checksum(_expected_key_checksum, key);
            _expected_hot_checksum = update_checksum(_expected_hot_checksum, key);
            const bool has_sparse = row % SPARSE_HIT_PERIOD == 0;
            _expected_sparse_checksum = update_checksum(
                    _expected_sparse_checksum,
                    has_sparse ? std::to_string(static_cast<uint64_t>(row) * 17 + 3) : "NULL");
            _expected_sparse_hits += has_sparse;
        }
        return Status::OK();
    }

    Status make_input_block(uint32_t rowset_index, uint32_t first_local_row, uint32_t rows,
                            Block* block) {
        auto keys = ColumnInt64::create();
        auto raw_json = ColumnString::create();
        keys->reserve(rows);
        raw_json->reserve(rows);
        for (uint32_t local = 0; local < rows; ++local) {
            const uint32_t global_row =
                    (first_local_row + local) * COMPACTION_INPUT_ROWSETS + rowset_index;
            const std::string json = make_json(_scenario.workload, global_row);
            keys->insert_value(global_row);
            raw_json->insert_data(json.data(), json.size());
            _input_json_bytes += json.size();
        }

        ColumnPtr values;
        DataTypePtr value_type;
        RETURN_IF_ERROR(parse_json_batch(std::move(raw_json), layout_config(_scenario.layout), rows,
                                         &values, &value_type));
        block->insert({std::move(keys), std::make_shared<DataTypeInt64>(), "k"});
        block->insert({std::move(values), std::move(value_type), std::string(ROOT_NAME)});
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
            RETURN_IF_ERROR(make_input_block(
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
            RETURN_IF_ERROR(validate_layout(_scenario, counts));
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

    Status validate_output_oracle(const RowsetSharedPtr& output,
                                  OlapReaderStatistics* statistics) const {
        DORIS_CHECK(statistics != nullptr);
        TabletSchemaPB schema_pb;
        _schema->to_schema_pb(&schema_pb);
        auto query_schema = std::make_shared<TabletSchema>();
        query_schema->init_from_pb(schema_pb);
        query_schema->set_storage_format(_schema->storage_format());
        const int32_t root_index = query_schema->field_index(ROOT_UID);
        if (root_index < 0) {
            return Status::InternalError("Variant root is missing from compaction read schema");
        }
        const TabletColumn root = query_schema->column(root_index);
        const auto append_path = [&](std::string_view path) {
            const std::string full_path = root.name_lower_case() + "." + std::string(path);
            TabletColumn path_column = variant_util::get_column_by_type(
                    std::make_shared<DataTypeVariant>(root.variant_max_subcolumns_count(),
                                                      root.variant_enable_doc_mode()),
                    full_path,
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
        RowsetReaderContext context;
        context.reader_type = ReaderType::READER_QUERY;
        context.tablet_schema = query_schema;
        context.need_ordered_result = true;
        context.read_schema = read_schema;
        context.stats = statistics;
        RETURN_IF_ERROR(reader->init(&context));

        uint64_t whole_checksum = FNV_OFFSET;
        uint64_t key_checksum = FNV_OFFSET;
        uint64_t hot_checksum = FNV_OFFSET;
        uint64_t sparse_checksum = FNV_OFFSET;
        uint32_t rows = 0;
        uint32_t hot_hits = 0;
        uint32_t sparse_hits = 0;
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
            for (size_t row = 0; row < block.rows(); ++row) {
                Status whole_status = update_flat_object_checksum(
                        &whole_checksum, whole.type->to_string(*whole.column, row));
                if (!whole_status.ok()) {
                    return Status::InternalError("Whole Variant mismatch at row {}: {}", rows + row,
                                                 whole_status.to_string());
                }
                key_checksum = update_checksum(key_checksum, key.type->to_string(*key.column, row));
                hot_checksum = update_checksum(hot_checksum, hot.type->to_string(*hot.column, row));
                sparse_checksum = update_checksum(sparse_checksum,
                                                  sparse.type->to_string(*sparse.column, row));
                hot_hits += !hot.column->is_null_at(row);
                sparse_hits += !sparse.column->is_null_at(row);
            }
            rows += static_cast<uint32_t>(block.rows());
        }
        if (rows != _total_rows || hot_hits != _total_rows ||
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

    CompactionScenario _scenario;
    uint32_t _total_rows;
    uint32_t _rows_per_rowset;
    uint32_t _input_segments = 0;
    uint32_t _variant_subcolumn_writer_paths = 0;
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

void BM_VariantCumulativeCompaction(benchmark::State& state, CompactionScenario scenario) {
    CompactionResult result;
    uint32_t total_rows = 0;
    uint32_t rows_per_rowset = 0;
    uint32_t input_segments = 0;
    uint32_t variant_subcolumn_writer_paths = 0;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        auto fixture = std::make_unique<VariantCompactionBenchmarkFixture>(scenario);
        Status status = fixture->prepare();
        if (status.ok()) {
            total_rows = fixture->total_rows();
            rows_per_rowset = fixture->rows_per_rowset();
            input_segments = fixture->input_segments();
            variant_subcolumn_writer_paths = fixture->variant_subcolumn_writer_paths();
        }
        CumulativeCompaction* compaction = status.ok() ? fixture->compaction() : nullptr;
        if (!benchmark_status(state, status)) {
            break;
        }

        const auto start = std::chrono::steady_clock::now();
        status = compaction->execute_compact();
        const auto finish = std::chrono::steady_clock::now();
        state.SetIterationTime(std::chrono::duration<double>(finish - start).count());

        if (status.ok()) {
            status = fixture->validate(&result);
        }
        const bool ok = benchmark_status(state, status);
        fixture.reset();
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
    state.counters["workload_revision"] = WORKLOAD_REVISION;
    state.counters["batch_rows"] = BATCH_ROWS;
    state.counters["rows_per_run"] = total_rows;
    state.counters["input_rowsets"] = COMPACTION_INPUT_ROWSETS;
    state.counters["input_segments"] = input_segments;
    state.counters["rows_per_input_rowset"] = rows_per_rowset;
    state.counters["merge_ways"] = COMPACTION_INPUT_ROWSETS;
    state.counters["input_key_ranges_overlap"] = 1;
    state.counters["input_json_bytes_per_row"] =
            static_cast<double>(result.input_json_bytes) / total_rows;
    state.counters["input_disk_bytes_per_row"] =
            static_cast<double>(result.input_disk_bytes) / total_rows;
    state.counters["output_disk_bytes_per_row"] =
            static_cast<double>(result.output_disk_bytes) / total_rows;
    state.counters["output_segments"] = result.output_segments;
    state.counters["materialized_columns"] = result.output_layout.materialized;
    state.counters["sparse_columns"] = result.output_layout.sparse;
    state.counters["doc_columns"] = result.output_layout.doc;
    state.counters["ordered_link_fast_path"] = 0;
    state.counters["vertical_compaction"] = 1;
    state.counters["compact_variant_subcolumns"] = scenario.compact_variant_subcolumns;
    state.counters["doc_materialization_enabled"] = scenario.materialize_doc;
    state.counters["mixed_physical_placement"] =
            scenario.workload == VariantWorkload::MIXED_PLACEMENT;
    state.counters["variant_subcolumn_writer_paths"] = variant_subcolumn_writer_paths;
    state.counters["compaction_batch_size"] = config::compaction_batch_size;
    state.counters["vertical_columns_per_group"] =
            config::vertical_compaction_num_columns_per_group;
    state.counters["fresh_input_fixture"] = 1;
    state.counters["input_footers_prevalidated"] = 1;
    state.counters["validation_route_leaf"] =
            result.validation_statistics.variant_subtree_leaf_iter_count;
    state.counters["validation_route_sparse"] =
            result.validation_statistics.variant_subtree_sparse_iter_count;
    state.counters["validation_route_hierarchical"] =
            result.validation_statistics.variant_subtree_hierarchical_iter_count;
}

bool register_variant_compaction_benchmarks() {
    constexpr std::array<CompactionScenario, 7> scenarios {{
            {.layout = VariantLayout::SPARSE16,
             .workload = VariantWorkload::FLAT,
             .name = "Sparse16/FlatPhysicalColumns",
             .compact_variant_subcolumns = true},
            {.layout = VariantLayout::SPARSE16,
             .workload = VariantWorkload::MIXED_PLACEMENT,
             .name = "Sparse16/MixedPhysicalPlacement",
             .compact_variant_subcolumns = true},
            {.layout = VariantLayout::SPARSE16,
             .workload = VariantWorkload::FLAT,
             .name = "Sparse16/WholeVariant",
             .compact_variant_subcolumns = false},
            {.layout = VariantLayout::FULL,
             .workload = VariantWorkload::FLAT,
             .name = "Full/WholeVariant",
             .compact_variant_subcolumns = false},
            {.layout = VariantLayout::DOC16,
             .workload = VariantWorkload::FLAT,
             .name = "Doc16/DocBuckets",
             .compact_variant_subcolumns = true},
            {.layout = VariantLayout::DOC16,
             .workload = VariantWorkload::FLAT,
             .name = "Doc16/DocBucketsMaterialized",
             .compact_variant_subcolumns = true,
             .materialize_doc = true},
            {.layout = VariantLayout::DOC16,
             .workload = VariantWorkload::FLAT,
             .name = "Doc16/WholeVariant",
             .compact_variant_subcolumns = false},
    }};
    constexpr uint32_t SAMPLES = 5;
    for (const CompactionScenario scenario : scenarios) {
        for (uint32_t sample = 1; sample <= SAMPLES; ++sample) {
            const std::string name = "BM_VariantCumulativeCompaction/" +
                                     std::string(scenario.name) +
                                     "/Overlap10Way/FreshInput/sample" + std::to_string(sample);
            benchmark::RegisterBenchmark(name,
                                         [scenario](benchmark::State& state) {
                                             BM_VariantCumulativeCompaction(state, scenario);
                                         })
                    ->Unit(benchmark::kMillisecond)
                    ->Iterations(1)
                    ->UseManualTime();
        }
    }
    return true;
}

inline const bool VARIANT_COMPACTION_BENCHMARKS_REGISTERED =
        register_variant_compaction_benchmarks();

} // namespace
} // namespace doris::variant_segment_benchmark
