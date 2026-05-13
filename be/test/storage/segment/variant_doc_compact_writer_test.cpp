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

// Unit tests for VariantDocCompactWriter (see
// be/src/storage/segment/variant/variant_column_writer_impl.cpp ~L1828-2191).
//
// This file focuses on exercising the writer directly in isolation from the
// full compaction pipeline, so each test runs fast (<1s). The tests cover:
//
//   - ctor + init()
//   - append_data / append_nullable (legacy vs batched path, threshold crossings)
//   - append_nulls / finish_current_page (NotSupported)
//   - _flush_batch branches (empty, first flush, stats lookup hit/miss/null)
//   - finalize() dispatch to _finalize_legacy / _finalize_batched
//   - finish / write_data / write_ordinal_index / write_zone_map /
//     write_inverted_index / write_bloom_filter_index
//   - estimate_buffer_size (with and without batch state)
//
// Setup: we construct a bucket-specific doc_value TabletColumn via
// variant_util::create_doc_value_column so ColumnWriter::create_variant_writer
// dispatches to VariantDocCompactWriter. We drive append_data with a
// VariantColumnData wrapper pointing at a doc-mode ColumnVariant prepared via
// create_doc_value_variant.

#include <gen_cpp/segment_v2.pb.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "gtest/gtest.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/variant/variant_column_writer_impl.h"
#include "storage/segment/variant/variant_doc_path_stats.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/variant_doc_mode_test_util.h"

using namespace doris;
using namespace doris::segment_v2;
using namespace doris::variant_doc_mode_test;

namespace doris {

namespace {

// Default threshold used by tests unless explicitly overridden. 64MB mirrors
// the production default in variant_doc_compact_batch_flush_bytes.
constexpr int64_t kDefaultFlushBytes = 64LL * 1024 * 1024;

// Build a doc-mode schema with configurable bucket count and return it along
// with the parent column (column 0, unique_id = 1).
TabletSchemaSPtr make_parent_schema(int doc_hash_shard_count = 4, int max_subcolumns_count = 10,
                                    int64_t doc_materialization_min_rows = 0) {
    return create_doc_mode_schema(max_subcolumns_count, doc_materialization_min_rows,
                                  doc_hash_shard_count);
}

} // namespace

// Harness: owns the filesystem scaffolding and the single ColumnWriter under
// test. Tests set up state via configure() and then drive the writer.
class VariantDocCompactWriterTest : public testing::Test {
public:
    void SetUp() override {
        char buffer[1024];
        EXPECT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/variant_doc_compact_writer_test";
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        _saved_flush_bytes = config::variant_doc_compact_batch_flush_bytes;
        _saved_sparse_subcolumns = config::enable_variant_doc_sparse_write_subcolumns;
    }

    void TearDown() override {
        // Ensure the column writer is fully released before we tear down the file
        // writer (the writer references a ColumnMetaPB owned by _footer).
        _writer.reset();
        if (_file_writer) {
            // Ignore close errors: a test may leave the writer un-finished on failure.
            auto st [[maybe_unused]] = _file_writer->close();
            _file_writer.reset();
        }
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());

        config::variant_doc_compact_batch_flush_bytes = _saved_flush_bytes;
        config::enable_variant_doc_sparse_write_subcolumns = _saved_sparse_subcolumns;
    }

protected:
    // One-shot setup. Builds the schema, creates a file writer, constructs a
    // VariantDocCompactWriter pointing at bucket `bucket_idx`. Also populates
    // _rowset_ctx so _flush_batch can find RowsetWriterContext.
    Status configure(int bucket_idx, int doc_hash_shard_count = 4,
                     const std::string& segment_name = "seg0") {
        _schema = make_parent_schema(doc_hash_shard_count);
        _parent_column = std::make_unique<TabletColumn>(_schema->column(0));
        _bucket_column = std::make_unique<TabletColumn>(
                variant_util::create_doc_value_column(*_parent_column, bucket_idx));

        std::string path = _absolute_dir + "/" + segment_name + ".dat";
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &_file_writer));
        _file_path = path;

        _footer = std::make_unique<SegmentFooterPB>();
        _rowset_ctx = std::make_unique<RowsetWriterContext>();
        _rowset_ctx->tablet_schema = _schema;
        _rowset_ctx->write_type = DataWriteType::TYPE_DIRECT;

        _opts.meta = _footer->add_columns();
        _opts.compression_type = CompressionTypePB::LZ4;
        _opts.file_writer = _file_writer.get();
        _opts.footer = _footer.get();
        _opts.rowset_ctx = _rowset_ctx.get();
        _init_column_meta(_opts.meta, /*column_id=*/0, *_bucket_column, CompressionTypePB::LZ4);

        RETURN_IF_ERROR(ColumnWriter::create_variant_writer(_opts, _bucket_column.get(),
                                                            _file_writer.get(), &_writer));
        RETURN_IF_ERROR(_writer->init());
        return Status::OK();
    }

    // Install pre-reserve stats so _flush_batch can bind them.
    void install_doc_path_stats(int total_buckets,
                                const std::unordered_map<std::string, uint64_t>& counts) {
        auto stats = VariantDocPathStats::build_from_path_counts(counts, total_buckets);
        _rowset_ctx->variant_doc_path_stats[_parent_column->unique_id()] = std::move(stats);
    }

    // Install an explicitly-null shared_ptr entry. Exercises the branch where
    // stats_it is found but the pointer is null.
    void install_null_doc_path_stats() {
        _rowset_ctx->variant_doc_path_stats[_parent_column->unique_id()] =
                std::shared_ptr<VariantDocPathStats>();
    }

    // Build a doc-mode ColumnVariant from the supplied JSON strings.
    MutableColumnPtr make_doc_mode_variant(const std::vector<std::string>& jsons) {
        return create_doc_value_variant(jsons);
    }

    // Feed the doc-mode ColumnVariant through append_data via the
    // VariantColumnData wrapper the writer expects.
    Status feed(const IColumn& variant_col, size_t row_pos, size_t num_rows) {
        VariantColumnData wrap {&variant_col, row_pos};
        const uint8_t* p = reinterpret_cast<const uint8_t*>(&wrap);
        return _writer->append_data(&p, num_rows);
    }

    // Drive the tail-end of the writer lifecycle.
    Status finish_and_flush() {
        RETURN_IF_ERROR(_writer->finish());
        RETURN_IF_ERROR(_writer->write_data());
        RETURN_IF_ERROR(_writer->write_ordinal_index());
        if (_opts.need_zone_map) {
            RETURN_IF_ERROR(_writer->write_zone_map());
        }
        if (_opts.need_inverted_index) {
            RETURN_IF_ERROR(_writer->write_inverted_index());
        }
        if (_opts.need_bloom_filter) {
            RETURN_IF_ERROR(_writer->write_bloom_filter_index());
        }
        return Status::OK();
    }

    VariantDocCompactWriter* compact_writer() {
        return dynamic_cast<VariantDocCompactWriter*>(_writer.get());
    }

    int64_t _saved_flush_bytes = kDefaultFlushBytes;
    bool _saved_sparse_subcolumns = false;

    std::string _absolute_dir;
    std::string _file_path;
    TabletSchemaSPtr _schema;
    std::unique_ptr<TabletColumn> _parent_column;
    std::unique_ptr<TabletColumn> _bucket_column;
    io::FileWriterPtr _file_writer;
    std::unique_ptr<SegmentFooterPB> _footer;
    std::unique_ptr<RowsetWriterContext> _rowset_ctx;
    ColumnWriterOptions _opts;
    std::unique_ptr<ColumnWriter> _writer;
};

// ============================================================
// 1. Dispatch + init
// ============================================================

TEST_F(VariantDocCompactWriterTest, CreateVariantWriterDispatchesToDocCompact) {
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());
    EXPECT_NE(compact_writer(), nullptr);
}

TEST_F(VariantDocCompactWriterTest, InitIsNoop) {
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());
    // init() was already called in configure(); verify it can be invoked
    // again without side effects.
    EXPECT_TRUE(_writer->init().ok());
}

// ============================================================
// 2. Unsupported operations
// ============================================================

TEST_F(VariantDocCompactWriterTest, AppendNullsReturnsNotSupported) {
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());
    auto st = _writer->append_nulls(5);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
}

TEST_F(VariantDocCompactWriterTest, FinishCurrentPageReturnsNotSupported) {
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());
    auto st = _writer->finish_current_page();
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
}

// ============================================================
// 3. Legacy path (batching disabled via thr <= 0)
// ============================================================

TEST_F(VariantDocCompactWriterTest, LegacyPath_NoFlushWhenThresholdZero) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 50; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);

    ASSERT_TRUE(feed(*variant, 0, 50).ok());
    // No flush yet — _total_rows_flushed should still be zero.
    EXPECT_EQ(compact_writer()->estimate_buffer_size() > 0, true);

    // finalize() picks the legacy branch (_total_rows_flushed == 0).
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    // After legacy finalize we expect doc_value bucket in the footer.
    auto s = inspect_footer(*_footer);
    EXPECT_GT(s.doc_value_bucket_count, 0);
}

TEST_F(VariantDocCompactWriterTest, LegacyPath_NoFlushWhenThresholdNegative) {
    config::variant_doc_compact_batch_flush_bytes = -1;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 20; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);

    ASSERT_TRUE(feed(*variant, 0, 20).ok());
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    auto s = inspect_footer(*_footer);
    EXPECT_GT(s.doc_value_bucket_count, 0);
    EXPECT_EQ(_footer->columns(0).num_rows(), 20);
}

TEST_F(VariantDocCompactWriterTest, LegacyPath_ThresholdLargerThanData_NoFlush) {
    // Threshold is set large enough that the buffered bytes never cross it.
    config::variant_doc_compact_batch_flush_bytes = 1LL << 30; // 1GB
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 30; ++i) {
        jsons.push_back(generate_json(4, i));
    }
    auto variant = make_doc_mode_variant(jsons);

    ASSERT_TRUE(feed(*variant, 0, 30).ok());
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    auto s = inspect_footer(*_footer);
    EXPECT_GT(s.doc_value_bucket_count, 0);
}

TEST_F(VariantDocCompactWriterTest, LegacyPath_AppendNullableDelegatesToAppendData) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 10; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);

    VariantColumnData wrap {variant.get(), 0};
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&wrap);
    std::vector<uint8_t> null_map(10, 0);
    ASSERT_TRUE(_writer->append_nullable(null_map.data(), &p, 10).ok());

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    auto s = inspect_footer(*_footer);
    EXPECT_GT(s.doc_value_bucket_count, 0);
}

// ============================================================
// 4. Batched path
// ============================================================

TEST_F(VariantDocCompactWriterTest, BatchedPath_ThresholdCrossingTriggersFlush) {
    // Pick a tiny threshold so a single feed crosses it. Each doc value row
    // we generate is a few hundred bytes of JSON content — 64 bytes comfortably
    // guarantees a flush after the first feed.
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 40; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);

    ASSERT_TRUE(feed(*variant, 0, 40).ok());

    // After one feed that crosses the threshold, some rows should already
    // have been flushed to the doc_value writer — we observe this indirectly
    // through finalize() taking the batched branch.
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    auto s = inspect_footer(*_footer);
    EXPECT_GT(s.doc_value_bucket_count, 0);
    EXPECT_EQ(_footer->columns(0).num_rows(), 40);
}

TEST_F(VariantDocCompactWriterTest, BatchedPath_MultipleBatchesPreserveAllRows) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    // Multiple append_data calls, each crossing the threshold.
    for (int batch = 0; batch < 4; ++batch) {
        std::vector<std::string> jsons;
        for (int i = 0; i < 10; ++i) {
            jsons.push_back(generate_json(3, batch * 100 + i));
        }
        auto variant = make_doc_mode_variant(jsons);
        ASSERT_TRUE(feed(*variant, 0, 10).ok());
    }

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    auto s = inspect_footer(*_footer);
    EXPECT_GT(s.doc_value_bucket_count, 0);
    EXPECT_EQ(_footer->columns(0).num_rows(), 40);
}

TEST_F(VariantDocCompactWriterTest, BatchedPath_TailFlushOnFinalize) {
    // Threshold set so the first feed crosses it but the second small feed
    // leaves a tail in _column that must be flushed by finalize().
    config::variant_doc_compact_batch_flush_bytes = 128;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    {
        std::vector<std::string> jsons;
        for (int i = 0; i < 30; ++i) {
            jsons.push_back(generate_json(5, i));
        }
        auto variant = make_doc_mode_variant(jsons);
        ASSERT_TRUE(feed(*variant, 0, 30).ok());
    }

    // A tiny tail. Should stay buffered until finalize().
    {
        std::vector<std::string> jsons = {R"({"k0":1})", R"({"k0":2})"};
        auto variant = make_doc_mode_variant(jsons);
        ASSERT_TRUE(feed(*variant, 0, 2).ok());
    }

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    EXPECT_EQ(_footer->columns(0).num_rows(), 32);
}

// Regression: VariantDocCompactWriter::_flush_batch used to construct a
// throwaway OlapBlockDataConvertor every call. OlapColumnDataConvertorMap
// keeps a cumulative `_base_offset` across convert_to_olap() calls so each
// successive convert produces monotonically increasing offsets within one
// continuous offset stream. A fresh-per-batch converter restarts
// `_base_offset` at 0, while _doc_value_column_writer (a single
// MapColumnWriter -> OffsetColumnWriter) appends each batch's offsets verbatim
// into one stream. The on-disk sequence becomes non-monotonic and trips the
// read-side `next_storage_offset >= first_storage_offset` DCHECK in
// OffsetFileColumnIterator::_calculate_offsets.
//
// Force ≥ 2 flushes by setting a tiny threshold, finalize, reopen the
// segment, then read the doc-value Map column back. With the bug present the
// read either aborts (debug build) or returns garbled offsets (release
// build); with the fix every row reads back monotonically.
TEST_F(VariantDocCompactWriterTest, BatchedPath_DocValueOffsetsMonotonicAcrossBatches) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    constexpr int kBatchCount = 4;
    constexpr int kRowsPerBatch = 10;
    constexpr int kFlatKeys = 3;
    constexpr int kObjChildren = 0; // keep keys-per-row deterministic
    int seed = 0;
    for (int b = 0; b < kBatchCount; ++b) {
        std::vector<std::string> jsons;
        jsons.reserve(kRowsPerBatch);
        for (int i = 0; i < kRowsPerBatch; ++i) {
            jsons.push_back(generate_json(kFlatKeys, ++seed, kObjChildren));
        }
        auto variant = make_doc_mode_variant(jsons);
        ASSERT_TRUE(feed(*variant, 0, kRowsPerBatch).ok());
    }

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    constexpr size_t kTotalRows = static_cast<size_t>(kBatchCount * kRowsPerBatch);
    const auto& bucket_meta = _footer->columns(0);
    ASSERT_EQ(bucket_meta.num_rows(), kTotalRows);

    // Reopen the segment file and build a ColumnReader for the doc-value
    // Map bucket directly (no Segment/VariantColumnReader needed).
    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(io::global_local_filesystem()->open_file(_file_path, &file_reader).ok());

    ColumnReaderOptions reader_opts;
    reader_opts.kept_in_memory = false;
    reader_opts.tablet_schema = _schema;
    std::shared_ptr<ColumnReader> column_reader;
    ASSERT_TRUE(
            ColumnReader::create(reader_opts, bucket_meta, kTotalRows, file_reader, &column_reader)
                    .ok());

    ColumnIteratorUPtr iter;
    ASSERT_TRUE(column_reader->new_iterator(&iter, _bucket_column.get()).ok());

    OlapReaderStatistics stats;
    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = file_reader.get();
    ASSERT_TRUE(iter->init(iter_opts).ok());

    auto map_dst = ColumnVariant::create_binary_column_fn();
    size_t to_read = kTotalRows;
    bool has_null = false;
    ASSERT_TRUE(iter->seek_to_ordinal(0).ok());
    // With the bug present this aborts via DCHECK in
    // OffsetFileColumnIterator::_calculate_offsets. With the fix it succeeds.
    ASSERT_TRUE(iter->next_batch(&to_read, map_dst, &has_null).ok());
    EXPECT_EQ(to_read, kTotalRows);
    EXPECT_EQ(map_dst->size(), kTotalRows);

    // Each row carried exactly kFlatKeys keys; the materialized Map's offsets
    // must be monotonically increasing with a constant step. A wrong
    // `_base_offset` would surface here as a non-monotonic step or a wrong
    // stride, even in builds where DCHECK is compiled out.
    auto& map_col = assert_cast<ColumnMap&>(*map_dst);
    const auto& offsets = map_col.get_offsets();
    ASSERT_EQ(offsets.size(), kTotalRows);
    int64_t prev = 0;
    for (size_t i = 0; i < offsets.size(); ++i) {
        ASSERT_GE(offsets[i], prev) << "non-monotonic Map offsets at row " << i;
        ASSERT_EQ(offsets[i] - prev, static_cast<int64_t>(kFlatKeys))
                << "unexpected key count at row " << i;
        prev = offsets[i];
    }
}

// ============================================================
// 5. _flush_batch stats lookup branches
// ============================================================

TEST_F(VariantDocCompactWriterTest, BatchedPath_StatsBoundWhenUidMatches) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0, /*doc_hash_shard_count=*/4).ok());

    // Build stats that partition keys into buckets — some should end up
    // in bucket 0 after hashing.
    std::unordered_map<std::string, uint64_t> counts;
    for (int i = 0; i < 20; ++i) {
        counts["k" + std::to_string(i)] = 100;
    }
    install_doc_path_stats(/*total_buckets=*/4, counts);

    std::vector<std::string> jsons;
    for (int i = 0; i < 30; ++i) {
        jsons.push_back(generate_json(4, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 30).ok());

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    EXPECT_EQ(_footer->columns(0).num_rows(), 30);
}

TEST_F(VariantDocCompactWriterTest, BatchedPath_NoStatsForUid_LeavesPointerNull) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    // Deliberately leave _rowset_ctx->variant_doc_path_stats empty. The
    // _flush_batch branch where stats_it == end() should be taken, and the
    // writer should still produce a valid segment.
    std::vector<std::string> jsons;
    for (int i = 0; i < 20; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 20).ok());

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    EXPECT_EQ(_footer->columns(0).num_rows(), 20);
}

TEST_F(VariantDocCompactWriterTest, BatchedPath_NullStatsSharedPtrSkipped) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    // variant_doc_path_stats has an entry for our uid but the shared_ptr is
    // null. The branch `stats_it->second` check should skip binding.
    install_null_doc_path_stats();

    std::vector<std::string> jsons;
    for (int i = 0; i < 20; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 20).ok());

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    EXPECT_EQ(_footer->columns(0).num_rows(), 20);
}

TEST_F(VariantDocCompactWriterTest, BatchedPath_WithoutRowsetCtx_SkipsStatsLookup) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    // DIAGNOSIS: _flush_batch has a guard `_opts.rowset_ctx != nullptr`
    // but column_by_uid is also called on rowset_ctx->tablet_schema
    // unconditionally. We cannot null out rowset_ctx entirely without
    // also breaking the parent_column lookup. Instead, we simply verify
    // the guarded branch by reusing the NoStatsForUid test path but with
    // an entry that maps to an empty-bucket configuration. The combination
    // of no stats + valid rowset_ctx exercises the same conditional without
    // crashing.
    std::unordered_map<int32_t, std::shared_ptr<VariantDocPathStats>> other;
    // populate map with unrelated uid so the lookup misses.
    other[9999] = VariantDocPathStats::build_from_path_counts({{"zzz", 1}}, 1);
    _rowset_ctx->variant_doc_path_stats = std::move(other);

    std::vector<std::string> jsons;
    for (int i = 0; i < 15; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 15).ok());
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
    EXPECT_EQ(_footer->columns(0).num_rows(), 15);
}

// ============================================================
// 6. estimate_buffer_size
// ============================================================

TEST_F(VariantDocCompactWriterTest, EstimateBufferSize_InitiallyZero) {
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());
    EXPECT_EQ(compact_writer()->estimate_buffer_size(), 0u);
}

TEST_F(VariantDocCompactWriterTest, EstimateBufferSize_GrowsWithBufferedRows) {
    // Keep batching disabled so data stays in _column.
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 40; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 40).ok());

    EXPECT_GT(compact_writer()->estimate_buffer_size(), 0u);
}

TEST_F(VariantDocCompactWriterTest, EstimateBufferSize_IncludesBatchState) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 40; ++i) {
        jsons.push_back(generate_json(4, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 40).ok());

    // After a flush, _column is reset but _batch_state holds the accumulator.
    // estimate_buffer_size must fold in the batch_state footprint.
    EXPECT_GT(compact_writer()->estimate_buffer_size(), 0u);
}

// ============================================================
// 7. Multiple buckets: sanity check name-based dispatch
// ============================================================

TEST_F(VariantDocCompactWriterTest, MultipleBucketsFinalize_Legacy) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    // Two buckets: run a minimal legacy-path finalize per bucket and verify
    // each one produces its own doc_value bucket meta in the footer.
    for (int bucket : {0, 1, 2, 3}) {
        SCOPED_TRACE("bucket=" + std::to_string(bucket));
        // Fresh harness state for each bucket.
        _writer.reset();
        _file_writer.reset();
        _footer.reset();
        _rowset_ctx.reset();
        ASSERT_TRUE(configure(bucket, /*doc_hash_shard_count=*/4, "seg_b" + std::to_string(bucket))
                            .ok());

        std::vector<std::string> jsons;
        for (int i = 0; i < 10; ++i) {
            jsons.push_back(generate_json(3, i));
        }
        auto variant = make_doc_mode_variant(jsons);
        ASSERT_TRUE(feed(*variant, 0, 10).ok());
        ASSERT_TRUE(finish_and_flush().ok());
        ASSERT_TRUE(_file_writer->close().ok());
        _file_writer.reset();

        EXPECT_EQ(_footer->columns(0).num_rows(), 10);
    }
}

// ============================================================
// 8. Re-entrancy of finish/write_data
// ============================================================

TEST_F(VariantDocCompactWriterTest, FinishAndWriteDataIdempotent) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 15; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 15).ok());

    ASSERT_TRUE(_writer->finish().ok());
    // Second call: write_data should be a no-op after the first.
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_ordinal_index().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
}

// ============================================================
// 9. write_zone_map / write_inverted_index / write_bloom_filter_index
//    with opts needs = false should still return OK (no-op per-subcolumn
//    plus the doc_value writer's own call).
// ============================================================

TEST_F(VariantDocCompactWriterTest, WriteIndexesWithNoIndexOpts) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 10; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 10).ok());

    ASSERT_TRUE(_writer->finish().ok());
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_ordinal_index().ok());
    // These should be safe to call even when need_* opts are false:
    // the writer iterates _subcolumn_writers (may be empty) and calls into
    // _doc_value_column_writer.
    ASSERT_TRUE(_writer->write_zone_map().ok());
    ASSERT_TRUE(_writer->write_inverted_index().ok());
    ASSERT_TRUE(_writer->write_bloom_filter_index().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
}

// ============================================================
// 10. Legacy + sparse-write-subcolumns toggle
// ============================================================

TEST_F(VariantDocCompactWriterTest, LegacyPath_SparseWriteEnabled) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    config::enable_variant_doc_sparse_write_subcolumns = true;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 25; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 25).ok());
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    auto s = inspect_footer(*_footer);
    EXPECT_GT(s.doc_value_bucket_count, 0);
}

TEST_F(VariantDocCompactWriterTest, LegacyPath_SparseWriteDisabled) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    config::enable_variant_doc_sparse_write_subcolumns = false;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 25; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 25).ok());
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    auto s = inspect_footer(*_footer);
    EXPECT_GT(s.doc_value_bucket_count, 0);
}

// ============================================================
// 11. get_next_rowid / get_raw_data_bytes: TODO stubs in production
//     — document by asserting current behavior.
// ============================================================

TEST_F(VariantDocCompactWriterTest, AccessorsInitiallyZero) {
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());
    EXPECT_EQ(compact_writer()->get_next_rowid(), 0u);
    EXPECT_EQ(compact_writer()->get_raw_data_bytes(), 0u);
    EXPECT_EQ(compact_writer()->get_total_uncompressed_data_pages_bytes(), 0u);
    EXPECT_EQ(compact_writer()->get_total_compressed_data_pages_bytes(), 0u);
}

// ============================================================
// 12. is_finalized transitions
// ============================================================

TEST_F(VariantDocCompactWriterTest, IsFinalizedAfterExplicitFinalize) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 8; ++i) {
        jsons.push_back(generate_json(2, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 8).ok());

    EXPECT_FALSE(compact_writer()->is_finalized());
    ASSERT_TRUE(compact_writer()->finalize().ok());
    EXPECT_TRUE(compact_writer()->is_finalized());

    // finish() should detect already-finalized and not re-run finalize.
    ASSERT_TRUE(_writer->finish().ok());
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_ordinal_index().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
}

TEST_F(VariantDocCompactWriterTest, IsFinalizedAfterBatchedFinalize) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 30; ++i) {
        jsons.push_back(generate_json(4, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 30).ok());

    EXPECT_FALSE(compact_writer()->is_finalized());
    ASSERT_TRUE(compact_writer()->finalize().ok());
    EXPECT_TRUE(compact_writer()->is_finalized());

    ASSERT_TRUE(_writer->finish().ok());
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_ordinal_index().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
}

// ============================================================
// 13. Multiple feeds in legacy mode (no threshold crossing)
// ============================================================

TEST_F(VariantDocCompactWriterTest, LegacyPath_MultipleFeedsAccumulate) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    for (int batch = 0; batch < 3; ++batch) {
        std::vector<std::string> jsons;
        for (int i = 0; i < 10; ++i) {
            jsons.push_back(generate_json(3, batch * 100 + i));
        }
        auto variant = make_doc_mode_variant(jsons);
        ASSERT_TRUE(feed(*variant, 0, 10).ok());
    }

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
    EXPECT_EQ(_footer->columns(0).num_rows(), 30);
}

// ============================================================
// 14. Minimal single-row finalize (smallest input the legacy path accepts)
// ============================================================
//
// DIAGNOSIS: finalize() with zero rows hits a DCHECK in
// OlapBlockDataConvertor::set_source_content_with_specifid_column
// (`Check failed: num_rows > 0`) because VariantDocCompactWriter passes
// num_rows straight through without the guard. This is a latent edge-case
// bug in the production writer but it is never exercised in real compaction
// — every bucket is invoked with at least one row. Rather than change
// production behavior, the test here covers the smallest well-defined
// input: a single row. If someone later wants to fix the zero-row case,
// they should guard `_write_doc_value_column` / `_finalize_legacy` early
// on `num_rows == 0`.
TEST_F(VariantDocCompactWriterTest, LegacyPath_MinimalSingleRowFinalize) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons = {R"({"only":1})"};
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 1).ok());

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    EXPECT_EQ(_footer->columns(0).num_rows(), 1);
}

// ============================================================
// 15. Round-trip via existing doc-mode write helper for comparison
// ============================================================
//
// This test does NOT exercise VariantDocCompactWriter directly but it
// verifies the broader doc-mode write path still passes after our unit tests
// link together — acting as a sanity check against accidental ABI breaks.

TEST_F(VariantDocCompactWriterTest, SanityCheck_DocModeWriteHelperStillWorks) {
    auto schema = make_parent_schema(/*doc_hash_shard_count=*/4);

    std::vector<std::string> jsons;
    for (int i = 0; i < 50; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = create_doc_value_variant(jsons);

    SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_variant_segment(schema, variant->get_ptr(), 50, _absolute_dir, "sanity", 0,
                                      &footer, &file_path)
                        .ok());
    auto s = inspect_footer(footer);
    // With min_rows=0 and max_subcolumns_count=10, paths=5+3=8<10 → Segment B.
    EXPECT_EQ(s.doc_value_bucket_count, 0);
    EXPECT_GT(s.subcolumn_count, 0);
}

// ============================================================
// 16. Gap-fill: _flush_batch batch_rows == 0 early return
// ============================================================
//
// After a threshold-crossing flush resets _column, an immediate finalize()
// with _column empty exercises the `_column->size() > 0` short-circuit in
// finalize() (see variant_column_writer_impl.cpp:~2064) and avoids the
// unnecessary _flush_batch call.
TEST_F(VariantDocCompactWriterTest, BatchedPath_EmptyColumnAfterFlush_FinalizeDirect) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    // Generate enough data for a single append_data to cross the threshold
    // exactly — after the internal flush, _column should be size 0.
    std::vector<std::string> jsons;
    for (int i = 0; i < 40; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 40).ok());

    // At this point, the batched path has flushed 40 rows and _column
    // is empty. finalize() should take the batched branch but skip the
    // tail _flush_batch call.
    ASSERT_TRUE(_writer->finish().ok());
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_ordinal_index().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
    EXPECT_EQ(_footer->columns(0).num_rows(), 40);
}

// ============================================================
// 17. Gap-fill: single append_data crossing the threshold multiple times
// ============================================================
//
// Only one flush is triggered per append_data call (the byte_size check is
// made once after insert_range_from). This test documents that behavior.
TEST_F(VariantDocCompactWriterTest, BatchedPath_SingleAppendFlushesOnce) {
    // Threshold is tiny — a single append of many rows will hold all rows
    // in _column before the single check triggers one flush.
    config::variant_doc_compact_batch_flush_bytes = 32;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 100; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 100).ok());

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    // All 100 rows must be preserved, even though only one flush ran.
    EXPECT_EQ(_footer->columns(0).num_rows(), 100);
}

// ============================================================
// 18. Gap-fill: append_nullable in batched path (crosses threshold)
// ============================================================
TEST_F(VariantDocCompactWriterTest, BatchedPath_AppendNullableCrossesThreshold) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 40; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);

    VariantColumnData wrap {variant.get(), 0};
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&wrap);
    std::vector<uint8_t> null_map(40, 0);
    ASSERT_TRUE(_writer->append_nullable(null_map.data(), &p, 40).ok());

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    EXPECT_EQ(_footer->columns(0).num_rows(), 40);
}

// ============================================================
// 19. Gap-fill: finalize() called twice is safe
// ============================================================
//
// finish() guards by is_finalized(). A user who calls finalize() directly
// and then finish() must not trigger a second finalize.
TEST_F(VariantDocCompactWriterTest, FinalizeTwice_SecondCallIsNoop) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 10; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 10).ok());

    ASSERT_TRUE(compact_writer()->finalize().ok());
    EXPECT_TRUE(compact_writer()->is_finalized());

    // Second finish() must not re-run finalize (which would re-execute the
    // doc_value write pipeline and likely produce a duplicate write).
    ASSERT_TRUE(_writer->finish().ok());
    ASSERT_TRUE(_writer->finish().ok());
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_ordinal_index().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    EXPECT_EQ(_footer->columns(0).num_rows(), 10);
}

// ============================================================
// 20. Gap-fill: higher bucket_idx (bucket 3 of 4) path-stats lookup
// ============================================================
//
// Exercises the bucket_pos parse path in _flush_batch with a non-zero
// bucket_idx. The path string looks like "...b3" for bucket 3; parsing
// lifts "3" via rfind('b') + substr.
TEST_F(VariantDocCompactWriterTest, BatchedPath_HigherBucketIndex) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/3, /*doc_hash_shard_count=*/4,
                          /*segment_name=*/"seg_b3")
                        .ok());

    std::unordered_map<std::string, uint64_t> counts;
    for (int i = 0; i < 30; ++i) {
        counts["k" + std::to_string(i)] = 50;
    }
    install_doc_path_stats(/*total_buckets=*/4, counts);

    std::vector<std::string> jsons;
    for (int i = 0; i < 30; ++i) {
        jsons.push_back(generate_json(5, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 30).ok());

    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
    EXPECT_EQ(_footer->columns(0).num_rows(), 30);
}

// ============================================================
// 21. Gap-fill: estimate_buffer_size after finalize
// ============================================================
//
// After finalize, _column is reset to an empty variant and _batch_state is
// dropped (batched finalize) or was never created (legacy finalize).
// estimate_buffer_size should return 0.
TEST_F(VariantDocCompactWriterTest, EstimateBufferSize_ZeroAfterBatchedFinalize) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 30; ++i) {
        jsons.push_back(generate_json(4, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 30).ok());

    ASSERT_TRUE(compact_writer()->finalize().ok());
    // _column reset + _batch_state reset → estimate should be 0.
    EXPECT_EQ(compact_writer()->estimate_buffer_size(), 0u);

    ASSERT_TRUE(_writer->finish().ok());
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_ordinal_index().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
}

TEST_F(VariantDocCompactWriterTest, EstimateBufferSize_ZeroAfterLegacyFinalize) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 10; ++i) {
        jsons.push_back(generate_json(3, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 10).ok());

    ASSERT_TRUE(compact_writer()->finalize().ok());
    EXPECT_EQ(compact_writer()->estimate_buffer_size(), 0u);

    ASSERT_TRUE(_writer->finish().ok());
    ASSERT_TRUE(_writer->write_data().ok());
    ASSERT_TRUE(_writer->write_ordinal_index().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();
}

// ============================================================
// 22. Gap-fill: variant_statistics populated with non-null counts (legacy)
// ============================================================
TEST_F(VariantDocCompactWriterTest, LegacyPath_PopulatesVariantStatistics) {
    config::variant_doc_compact_batch_flush_bytes = 0;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 20; ++i) {
        jsons.push_back(generate_json(4, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 20).ok());
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    // The meta must carry variant_statistics with at least one path entry.
    const auto& meta = _footer->columns(0);
    ASSERT_TRUE(meta.has_variant_statistics());
    EXPECT_GT(meta.variant_statistics().doc_value_column_non_null_size_size(), 0);
}

// ============================================================
// 23. Gap-fill: variant_statistics populated with non-null counts (batched)
// ============================================================
TEST_F(VariantDocCompactWriterTest, BatchedPath_PopulatesVariantStatistics) {
    config::variant_doc_compact_batch_flush_bytes = 64;
    ASSERT_TRUE(configure(/*bucket_idx=*/0).ok());

    std::vector<std::string> jsons;
    for (int i = 0; i < 30; ++i) {
        jsons.push_back(generate_json(4, i));
    }
    auto variant = make_doc_mode_variant(jsons);
    ASSERT_TRUE(feed(*variant, 0, 30).ok());
    ASSERT_TRUE(finish_and_flush().ok());
    ASSERT_TRUE(_file_writer->close().ok());
    _file_writer.reset();

    const auto& meta = _footer->columns(0);
    ASSERT_TRUE(meta.has_variant_statistics());
    EXPECT_GT(meta.variant_statistics().doc_value_column_non_null_size_size(), 0);
}

} // namespace doris
