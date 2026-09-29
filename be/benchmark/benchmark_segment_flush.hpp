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
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/value/vdatetime_value.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_creator.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris::segment_flush_benchmark {
namespace {

// Writes generated rows through the production segment writers; building the input blocks is
// never timed.
//   BM_SegmentFlushSingleBlock: a load's path. A memtable flush hands
//     SegmentFlusher::flush_single_block one key-sorted block per segment. One run per table
//     shape below.
//   BM_SegmentFlushDupKeys: SegmentCreator::add_block and flush, the path of push load,
//     horizontal compaction and schema change.
// Knobs:
//   DORIS_SEGMENT_FLUSH_BENCHMARK_ROWS              rows per iteration       (default 20000000)
//   DORIS_SEGMENT_FLUSH_BENCHMARK_ROWS_PER_SEGMENT  rows per flushed block   (default 1000000)
//   DORIS_SEGMENT_FLUSH_BENCHMARK_ROWS_PER_BLOCK    rows per add_block block (default 8192)
//   DORIS_SEGMENT_FLUSH_BENCHMARK_ROOT              directory for segments   (default /tmp)
// Every iteration writes the same rows, so binaries built from different revisions compare the
// same work. Use --benchmark_repetitions for several samples.
int64_t env_int(const char* name, int64_t fallback) {
    const char* raw = std::getenv(name);
    return raw == nullptr || *raw == '\0' ? fallback : std::strtoll(raw, nullptr, 10);
}

std::string env_str(const char* name, std::string fallback) {
    const char* raw = std::getenv(name);
    return raw == nullptr || *raw == '\0' ? std::move(fallback) : std::string(raw);
}

// A table shape.
struct Scenario {
    KeysType keys_type;
    // Merge-on-write: the segment carries a primary key index instead of a short key index.
    bool merge_on_write;
    // k_name as CHAR, which the key encodings pad, instead of VARCHAR.
    bool char_key;
    // An ARRAY<VARCHAR> value column with an inverted index.
    bool indexed_array;
};

constexpr Scenario kDup {KeysType::DUP_KEYS, false, false, false};
constexpr Scenario kMergeOnWrite {KeysType::UNIQUE_KEYS, true, false, false};
constexpr Scenario kMergeOnWriteCharKey {KeysType::UNIQUE_KEYS, true, true, false};
constexpr Scenario kDupIndexedArray {KeysType::DUP_KEYS, false, false, true};

// A fact table of the types a table created today uses: no V1 DATE, DATETIME or DECIMALV2, which
// only tables older than DATEV2 write.
struct BenchColumn {
    const char* name;
    const char* storage_type;
    bool is_key;
    bool is_nullable;
    int32_t length;
    int32_t index_length;
    int32_t precision;
    int32_t scale;
};

constexpr BenchColumn kColumns[] = {
        // an id and a name as keys, so the short key index and the key encoding both work
        {"k_id", "BIGINT", true, false, 8, 8, 0, 0},
        {"k_name", "VARCHAR", true, false, 66, 20, 0, 0},
        {"v_count", "INT", false, false, 4, 4, 0, 0},
        {"v_amount", "BIGINT", false, false, 8, 8, 0, 0},
        {"v_ratio", "DOUBLE", false, false, 8, 8, 0, 0},
        {"v_day", "DATEV2", false, false, 4, 4, 0, 0},
        {"v_ts", "DATETIMEV2", false, false, 8, 8, 0, 6},
        {"v_price", "DECIMAL128I", false, false, 16, 16, 38, 9},
        // a low-cardinality string, which stays dictionary encoded
        {"v_tag", "VARCHAR", false, true, 66, 20, 0, 0},
        // a high-cardinality string, which overflows the dictionary and falls back to plain
        {"v_payload", "STRING", false, true, std::numeric_limits<int32_t>::max(), 20, 0, 0},
};
// k_name's values are 19 characters, which CHAR pads to this length in the key encodings.
constexpr int32_t kCharKeyLength = 24;
constexpr BenchColumn kLabelsColumn {
        "v_labels", "ARRAY", false, false, OLAP_ARRAY_MAX_LENGTH, OLAP_ARRAY_MAX_LENGTH, 0, 0};
constexpr int32_t kLabelsItemUniqueId = 100;

ColumnPB* add_column(TabletSchemaPB* schema_pb, const BenchColumn& spec) {
    auto* column = schema_pb->add_column();
    column->set_unique_id(schema_pb->column_size() - 1);
    column->set_name(spec.name);
    column->set_type(spec.storage_type);
    column->set_is_key(spec.is_key);
    column->set_is_nullable(spec.is_nullable);
    column->set_aggregation("NONE");
    column->set_length(spec.length);
    column->set_index_length(spec.index_length);
    column->set_precision(spec.precision);
    column->set_frac(spec.scale);
    return column;
}

void add_hidden_column(TabletSchemaPB* schema_pb, const std::string& name, const char* type,
                       int32_t length) {
    auto* column = add_column(schema_pb, {"", type, false, false, length, length, 0, 0});
    column->set_name(name);
    column->set_visible(false);
    column->set_default_value("0");
}

TabletSchemaSPtr make_schema(const Scenario& scenario) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(scenario.keys_type);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_compress_kind(COMPRESS_LZ4);
    for (const auto& spec : kColumns) {
        auto* column = add_column(&schema_pb, spec);
        if (scenario.char_key && std::string_view(spec.name) == "k_name") {
            column->set_type("CHAR");
            column->set_length(kCharKeyLength);
            column->set_index_length(kCharKeyLength);
        }
    }
    if (scenario.indexed_array) {
        auto* labels = add_column(&schema_pb, kLabelsColumn);
        auto* item = labels->add_children_columns();
        item->set_unique_id(kLabelsItemUniqueId);
        item->set_name("item");
        item->set_type("VARCHAR");
        item->set_is_key(false);
        item->set_is_nullable(true);
        item->set_aggregation("NONE");
        item->set_length(66);
        item->set_index_length(20);

        auto* index = schema_pb.add_index();
        index->set_index_id(10001);
        index->set_index_name("v_labels_idx");
        index->set_index_type(IndexType::INVERTED);
        index->add_col_unique_id(labels->unique_id());
        (*index->mutable_properties())[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_NONE;
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    }
    if (scenario.keys_type == KeysType::UNIQUE_KEYS) {
        // The hidden columns every unique-keys table has.
        schema_pb.set_delete_sign_idx(schema_pb.column_size());
        add_hidden_column(&schema_pb, DELETE_SIGN, "TINYINT", 1);
        schema_pb.set_version_col_idx(schema_pb.column_size());
        add_hidden_column(&schema_pb, VERSION_COL, "BIGINT", 8);
    }
    schema_pb.set_next_column_unique_id(schema_pb.column_size());
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

// splitmix64: the same rows from any build, unlike the engines of <random>.
uint64_t mix(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

// Few enough distinct tags that the dictionary never fills up.
constexpr size_t kTagCardinality = 64;
// The words v_labels picks up to three of per row.
constexpr size_t kLabelCardinality = 1000;
// Uncompressed bytes of one BM_SegmentFlushDupKeys row: the fixed widths plus the two strings'
// average payloads.
constexpr size_t kRowBytes = 8 + 20 + 4 + 8 + 8 + 4 + 8 + 16 + 6 + 41;

class RowBuilder {
public:
    RowBuilder(const TabletSchemaSPtr& schema, const Scenario& scenario)
            : _schema(schema), _scenario(scenario) {
        for (size_t cid = 0; cid < schema->num_columns(); ++cid) {
            const auto& column = schema->column(cid);
            _types.push_back(
                    DataTypeFactory::instance().create_data_type(column, column.is_nullable()));
        }
        for (size_t i = 0; i < kTagCardinality; ++i) {
            _tags.push_back(fmt::format("tag-{:02d}", i));
        }
        for (size_t i = 0; i < kLabelCardinality; ++i) {
            _labels.push_back(fmt::format("label-{:03d}", i));
        }
    }

    // Rows [first_row, first_row + num_rows) in key order: k_id is the row number, so the block
    // is sorted the way a memtable hands it over, with no key repeated.
    Block build(int64_t first_row, size_t num_rows) const {
        MutableColumns columns;
        for (const auto& type : _types) {
            auto column = type->create_column();
            column->reserve(num_rows);
            columns.push_back(std::move(column));
        }

        std::string scratch;
        for (size_t i = 0; i < num_rows; ++i) {
            const int64_t row = first_row + static_cast<int64_t>(i);
            const uint64_t noise = mix(static_cast<uint64_t>(row));

            append_fixed(*columns[0], row);
            scratch = fmt::format("user-{:014d}", row);
            columns[1]->insert_data(scratch.data(), scratch.size());
            append_fixed(*columns[2], static_cast<int32_t>(noise % 100000));
            append_fixed(*columns[3], static_cast<int64_t>(noise >> 8));
            append_fixed(*columns[4], static_cast<double>(noise % 1000000) / 1000.0);

            DateV2Value<DateV2ValueType> day;
            day.unchecked_set_time(2020 + static_cast<uint16_t>(noise % 5),
                                   1 + static_cast<uint8_t>((noise >> 3) % 12),
                                   1 + static_cast<uint8_t>((noise >> 7) % 28), 0, 0, 0);
            append_fixed(*columns[5], day);

            DateV2Value<DateTimeV2ValueType> ts;
            ts.unchecked_set_time(2024, 1 + static_cast<uint8_t>((noise >> 11) % 12),
                                  1 + static_cast<uint8_t>((noise >> 15) % 28),
                                  static_cast<uint8_t>((noise >> 19) % 24),
                                  static_cast<uint8_t>((noise >> 24) % 60),
                                  static_cast<uint16_t>((noise >> 30) % 60),
                                  static_cast<uint32_t>((noise >> 36) % 1000000));
            append_fixed(*columns[6], ts);

            const __int128 price = static_cast<__int128>(noise % 1000000000000ULL) - 500000000000LL;
            append_fixed(*columns[7], price);

            // One row in 32 is NULL, so the writer walks null runs.
            if (noise % 32 == 0) {
                columns[8]->insert_data(nullptr, 0);
            } else {
                const auto& tag = _tags[(noise >> 40) % kTagCardinality];
                columns[8]->insert_data(tag.data(), tag.size());
            }
            if (noise % 41 == 0) {
                columns[9]->insert_data(nullptr, 0);
            } else {
                scratch = fmt::format("{:016x}-{:016x}-payload", noise, mix(noise));
                columns[9]->insert_data(scratch.data(), scratch.size());
            }

            size_t next = std::size(kColumns);
            if (_scenario.indexed_array) {
                append_labels(*columns[next++], mix(noise));
            }
            if (_scenario.keys_type == KeysType::UNIQUE_KEYS) {
                append_fixed(*columns[next++], int8_t {0});
                append_fixed(*columns[next++], int64_t {0});
            }
        }

        Block block;
        for (size_t cid = 0; cid < _types.size(); ++cid) {
            block.insert(ColumnWithTypeAndName(std::move(columns[cid]), _types[cid],
                                               _schema->column(cid).name()));
        }
        return block;
    }

private:
    template <typename T>
    static void append_fixed(IColumn& column, const T& value) {
        column.insert_data(reinterpret_cast<const char*>(&value), sizeof(T));
    }

    // Up to three labels.
    void append_labels(IColumn& column, uint64_t noise) const {
        auto& labels = assert_cast<ColumnArray&>(column);
        auto& items = labels.get_data();
        for (size_t i = 0; i < noise % 4; ++i) {
            const auto& label = _labels[(noise >> (8 + 16 * i)) % kLabelCardinality];
            items.insert_data(label.data(), label.size());
        }
        labels.get_offsets().push_back(items.size());
    }

    TabletSchemaSPtr _schema;
    Scenario _scenario;
    DataTypes _types;
    std::vector<std::string> _tags;
    std::vector<std::string> _labels;
};

class BenchFileWriterCreator final : public FileWriterCreator {
public:
    BenchFileWriterCreator(std::string directory, TabletSchemaSPtr schema)
            : _directory(std::move(directory)), _schema(std::move(schema)) {}

    Status create(uint32_t segment_id, io::FileWriterPtr& file_writer,
                  FileType /*file_type*/) override {
        return io::global_local_filesystem()->create_file(path(segment_id), &file_writer);
    }

    Status create(uint32_t segment_id, IndexFileWriterPtr* file_writer) override {
        const auto storage_format = _schema->get_inverted_index_storage_format();
        std::string prefix {
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(path(segment_id))};
        // Every index of a segment goes into one file, except in the V1 format.
        io::FileWriterPtr index_file;
        if (storage_format != InvertedIndexStorageFormatPB::V1) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_file(
                    segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                    &index_file));
        }
        *file_writer = std::make_unique<segment_v2::IndexFileWriter>(
                io::global_local_filesystem(), std::move(prefix), "10002", segment_id,
                storage_format, std::move(index_file), true, 10001);
        return Status::OK();
    }

    std::string path(uint32_t segment_id) const {
        return fmt::format("{}/segment_{}.dat", _directory, segment_id);
    }

private:
    std::string _directory;
    TabletSchemaSPtr _schema;
};

class BenchSegmentCollector final : public SegmentCollector {
public:
    Status add(uint32_t segment_id, SegmentStatistics& statistics) override {
        ++segments;
        data_size += statistics.data_size;
        index_size += statistics.index_size;
        return Status::OK();
    }

    size_t segments = 0;
    int64_t data_size = 0;
    int64_t index_size = 0;
};

struct FlushResult {
    double seconds = 0;
    size_t segments = 0;
    int64_t segment_bytes = 0;
    int64_t index_bytes = 0;
};

using Clock = std::chrono::steady_clock;

// Starts a fresh `directory` and sets `context` to write there, its segments counted by
// `collector`.
Status prepare_context(const TabletSchemaSPtr& schema, const std::string& directory,
                       const std::shared_ptr<BenchSegmentCollector>& collector,
                       RowsetWriterContext* context) {
    RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(directory));
    RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(directory));
    context->tablet_schema = schema;
    context->tablet_path = directory;
    context->tablet_id = 10001;
    context->rowset_id.init(10002);
    context->write_type = DataWriteType::TYPE_DIRECT;
    context->file_writer_creator = std::make_shared<BenchFileWriterCreator>(directory, schema);
    context->segment_collector = collector;
    return Status::OK();
}

void fill_result(const BenchSegmentCollector& collector, Clock::duration elapsed,
                 FlushResult* result) {
    result->seconds = std::chrono::duration<double>(elapsed).count();
    result->segments = collector.segments;
    result->segment_bytes = collector.data_size;
    result->index_bytes = collector.index_size;
}

// Writes `total_rows` rows through a SegmentCreator; only add_block and flush count.
Status flush_rows(const TabletSchemaSPtr& schema, const RowBuilder& rows,
                  const std::string& directory, int64_t total_rows, size_t rows_per_block,
                  FlushResult* result) {
    auto collector = std::make_shared<BenchSegmentCollector>();
    RowsetWriterContext context;
    RETURN_IF_ERROR(prepare_context(schema, directory, collector, &context));
    SegmentFileCollection segment_files;
    InvertedIndexFileCollection index_files;
    SegmentCreator creator(context, segment_files, index_files);

    Clock::duration elapsed {};
    for (int64_t first = 0; first < total_rows; first += static_cast<int64_t>(rows_per_block)) {
        Block block = rows.build(
                first, static_cast<size_t>(std::min<int64_t>(static_cast<int64_t>(rows_per_block),
                                                             total_rows - first)));
        const auto start = Clock::now();
        RETURN_IF_ERROR(creator.add_block(&block));
        elapsed += Clock::now() - start;
    }
    const auto start = Clock::now();
    RETURN_IF_ERROR(creator.flush());
    RETURN_IF_ERROR(segment_files.close());
    RETURN_IF_ERROR(index_files.begin_close());
    RETURN_IF_ERROR(index_files.finish_close());
    elapsed += Clock::now() - start;
    fill_result(*collector, elapsed, result);
    return Status::OK();
}

// Writes `total_rows` rows as one segment per `rows_per_segment` rows through
// SegmentFlusher::flush_single_block; only the flushes count.
Status flush_segments(const TabletSchemaSPtr& schema, const Scenario& scenario,
                      const RowBuilder& rows, const std::string& directory, int64_t total_rows,
                      size_t rows_per_segment, FlushResult* result) {
    auto collector = std::make_shared<BenchSegmentCollector>();
    RowsetWriterContext context;
    RETURN_IF_ERROR(prepare_context(schema, directory, collector, &context));
    context.enable_unique_key_merge_on_write = scenario.merge_on_write;
    SegmentFileCollection segment_files;
    InvertedIndexFileCollection index_files;
    SegmentFlusher flusher(context, segment_files, index_files);

    Clock::duration elapsed {};
    int32_t segment_id = 0;
    for (int64_t first = 0; first < total_rows; first += static_cast<int64_t>(rows_per_segment)) {
        Block block = rows.build(
                first, static_cast<size_t>(std::min<int64_t>(static_cast<int64_t>(rows_per_segment),
                                                             total_rows - first)));
        const auto start = Clock::now();
        RETURN_IF_ERROR(flusher.flush_single_block(&block, segment_id++));
        elapsed += Clock::now() - start;
    }
    // Closes the segment files and finishes the index files, whose segments began closing them.
    const auto start = Clock::now();
    RETURN_IF_ERROR(flusher.close());
    elapsed += Clock::now() - start;
    fill_result(*collector, elapsed, result);
    return Status::OK();
}

// Runs `flush` once per iteration under a fresh root, which the segment writers also spill to.
template <typename Flush>
void run(benchmark::State& state, int64_t total_rows, Flush flush) {
    const std::string root =
            env_str("DORIS_SEGMENT_FLUSH_BENCHMARK_ROOT", "/tmp") + "/segment_flush_benchmark";
    Status status = io::global_local_filesystem()->delete_directory(root);
    if (status.ok()) {
        status = io::global_local_filesystem()->create_directory(root);
    }
    std::vector<StorePath> store_paths;
    store_paths.emplace_back(root, -1);
    auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(store_paths);
    if (status.ok()) {
        status = tmp_file_dirs->init();
    }
    if (!status.ok()) {
        state.SkipWithError(status.to_string());
        return;
    }
    ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

    FlushResult result;
    for (auto _ : state) {
        status = flush(root + "/segments", &result);
        if (!status.ok()) {
            state.SkipWithError(status.to_string());
            break;
        }
        state.SetIterationTime(result.seconds);
    }
    ExecEnv::GetInstance()->set_tmp_file_dir(nullptr);
    static_cast<void>(io::global_local_filesystem()->delete_directory(root));
    if (!status.ok()) {
        return;
    }

    state.SetItemsProcessed(total_rows * state.iterations());
    state.counters["rows"] = static_cast<double>(total_rows);
    state.counters["segments"] = static_cast<double>(result.segments);
    state.counters["segment_MB"] = static_cast<double>(result.segment_bytes) / (1024.0 * 1024.0);
    state.counters["index_MB"] = static_cast<double>(result.index_bytes) / (1024.0 * 1024.0);
}

void BM_SegmentFlushSingleBlock(benchmark::State& state, Scenario scenario) {
    const int64_t total_rows = env_int("DORIS_SEGMENT_FLUSH_BENCHMARK_ROWS", 20'000'000);
    const auto rows_per_segment = static_cast<size_t>(std::max<int64_t>(
            1, env_int("DORIS_SEGMENT_FLUSH_BENCHMARK_ROWS_PER_SEGMENT", 1'000'000)));
    const auto schema = make_schema(scenario);
    const RowBuilder rows(schema, scenario);
    run(state, total_rows, [&](const std::string& directory, FlushResult* result) {
        return flush_segments(schema, scenario, rows, directory, total_rows, rows_per_segment,
                              result);
    });
}

void BM_SegmentFlushDupKeys(benchmark::State& state) {
    const int64_t total_rows = env_int("DORIS_SEGMENT_FLUSH_BENCHMARK_ROWS", 20'000'000);
    const auto rows_per_block = static_cast<size_t>(
            std::max<int64_t>(1, env_int("DORIS_SEGMENT_FLUSH_BENCHMARK_ROWS_PER_BLOCK", 8192)));
    const auto schema = make_schema(kDup);
    const RowBuilder rows(schema, kDup);
    run(state, total_rows, [&](const std::string& directory, FlushResult* result) {
        return flush_rows(schema, rows, directory, total_rows, rows_per_block, result);
    });
    state.SetBytesProcessed(total_rows * static_cast<int64_t>(kRowBytes) * state.iterations());
}

BENCHMARK_CAPTURE(BM_SegmentFlushSingleBlock, dup, kDup)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(1)
        ->UseManualTime();
BENCHMARK_CAPTURE(BM_SegmentFlushSingleBlock, mow, kMergeOnWrite)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(1)
        ->UseManualTime();
BENCHMARK_CAPTURE(BM_SegmentFlushSingleBlock, mow_char_key, kMergeOnWriteCharKey)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(1)
        ->UseManualTime();
BENCHMARK_CAPTURE(BM_SegmentFlushSingleBlock, dup_indexed_array, kDupIndexedArray)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(1)
        ->UseManualTime();
BENCHMARK(BM_SegmentFlushDupKeys)->Unit(benchmark::kMillisecond)->Iterations(1)->UseManualTime();

} // namespace
} // namespace doris::segment_flush_benchmark
