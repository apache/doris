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
//
// SNII vs V3 baseline over the same wikipedia corpus, covering the three phases that matter for the
// format: building the index while loading, compacting it, and querying it across MATCH_ANY,
// MATCH_ALL, MATCH_PHRASE and MATCH_PHRASE_PREFIX.
//
// Why this lives in a unit test rather than a cluster benchmark: a UT iterates in minutes instead
// of a deploy cycle, and it measures one process we control end to end.
//
// Why CPU time is the headline number: this machine is shared, so wall clock moves with whatever
// else is running. Process CPU time barely does. Wall time is still reported -- a large wall/CPU
// gap means the run was IO bound or descheduled and the comparison should be rerun -- but the
// SNII/V3 verdict is taken from CPU.
//
// Corpus is not committed (~41 MB). Point SNII_BENCH_CORPUS_DIR at a directory of wikipedia_*.json
// with {"title","content"} per line:
//
//   SNII_BENCH_CORPUS_DIR=/path/to/corpus \
//     SNII_BENCH_QUERY_ITERATIONS=30 \
//     ./run-be-ut.sh --run --filter='*SniiVsV3Benchmark*' -j <N>
//
// SNII_BENCH_QUERY_ITERATIONS defaults to 30. The benchmark reports nearest-rank
// p50/p99 query CPU and wall time from the sorted per-iteration samples.
//
// SNII_BENCH_QUERY_INPUT_ROWSETS=1 points the query phase at the input rowsets instead of the
// compacted output. With one corpus file per rowset that is the shape of a tablet still ingesting:
// N indexes open at once and N rounds of remote reads per cold query, rather than one merged index.
// It also skips compaction, whose only purpose here is to produce the rowset the query reads --
// compaction is benchmarked by the default compacted-output mode, where it is on the measured path.
//
// SNII_BENCH_ONLY_CASE=<label> restricts the pass to one query case. A write-back cold pass shares
// one cache across every case, so whichever case first touches a block pays for it and the rest
// read it locally; isolating a case is the only way its cold IO describes that query alone.
//
// SNII_BENCH_ROWS_PER_SEGMENT=<n> sets segment size (default 200, the prepared corpus's per-file
// count). Per-segment open cost grows with a segment's vocabulary, so a 200-row segment sits at
// the bottom of that curve and a production-sized one does not; use 25000 for anything meant to
// describe production.
//
// Iteration-speed knobs, none of which change what is measured:
//   SNII_BENCH_ONLY_FORMAT=V3|SNII  run one format (the other's numbers do not move while
//                                   iterating on the other side). Ratios are then absent.
//   SNII_BENCH_REUSE_ROWSETS=1      keep the imported rowsets on S3 and rebuild them from a
//                                   serialized RowsetMeta manifest instead of re-importing.
//                                   Import is ~88% of a short run. Invalidated automatically by
//                                   corpus/schema/segment-size/policy changes, NOT by writer or
//                                   format changes -- re-import once after touching the write
//                                   path. Leaves s3://<bucket>/snii_bench_reuse behind.
//   SNII_BENCH_REUSE_DIR=<dir>      where the manifest lives (default: beside the corpus).
//
// The benchmark fixture and percentile tests are DISABLED_ so CI never runs them;
// --gtest_also_run_disabled_tests or the filter above opts in. They are still compiled into
// doris_be_test on every build.

#include <fcntl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "core/data_type/data_type_string.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_iterator.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/compaction/util/index_compaction_utils.cpp"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/storage_policy.h"
#include "storage/tablet/tablet.h"
#include "util/defer_op.h"
#include "util/threadpool.h"
#include "util/time.h"

namespace doris {

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view kDestDir = "./ut_dir/snii_bench";
constexpr static std::string_view kTmpDir = "./ut_dir/snii_bench_tmp";

// Which indexed column a case queries. Index into the schema's columns: 0 = title, 1 = content.
enum class QueryColumn : int { kTitle = 0, kContent = 1 };

struct QueryCase {
    const char* label;
    InvertedIndexQueryType type;
    const char* text;
    QueryColumn column = QueryColumn::kContent;
};

// Spread across the shapes the two formats plan differently: single common term, single rare term,
// two/three-word phrases, stopword-led phrases (where CommonGrams matters), and prefixes of varying
// length. A single query shape would only exercise one code path.
const std::vector<QueryCase> kQueryCases = {
        {"any_common", InvertedIndexQueryType::MATCH_ANY_QUERY, "anarchism"},
        {"any_rare", InvertedIndexQueryType::MATCH_ANY_QUERY, "syndicalism"},
        {"any_multi", InvertedIndexQueryType::MATCH_ANY_QUERY, "philosophy movement state"},
        // A real article title, punctuation and all. The english analyzer splits it into
        // the/idol/tv/series, so MATCH_ANY unions a stopword-grade term with three ordinary ones --
        // a much wider posting union than the hand-picked cases above, and the shape a title-match
        // query actually has in production.
        {"any_title", InvertedIndexQueryType::MATCH_ANY_QUERY, "The Idol (TV series)",
         QueryColumn::kTitle},
        {"all_multi", InvertedIndexQueryType::MATCH_ALL_QUERY, "political philosophy"},
        {"phrase_2", InvertedIndexQueryType::MATCH_PHRASE_QUERY, "anarchism is"},
        {"phrase_3", InvertedIndexQueryType::MATCH_PHRASE_QUERY, "political philosophy and"},
        {"phrase_stop", InvertedIndexQueryType::MATCH_PHRASE_QUERY, "of the"},
        {"prefix_1", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, "anarch"},
        {"prefix_2", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, "anarchism is"},
        {"prefix_3", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, "philosophy and mov"},
        {"prefix_stop", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, "the united sta"},
        {"prefix_long", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
         "in the early twentieth cen"},
};

// Which storage the rowset lives on. Local is the fast iteration loop; RemoteS3 is the shape a
// real deployment has -- S3FileSystem -> CachedRemoteFileReader -> BlockFileCache -> query -- and is
// the only mode where the read-amplification counters mean anything.
enum class IoMode { kLocal, kRemoteS3 };

// How the run treats the block file cache. Applied uniformly to load, compaction and query so the
// three phases describe one consistent deployment instead of contradicting each other.
//
//   kDirect    - file cache off end to end. Load writes straight to S3, compaction re-reads from
//                S3, the query reads from S3. This is the read-amplification measurement: every
//                byte and every GET is real.
//   kWriteBack - file cache on, load populates it (cloud sets RowsetWriterContext::write_file_cache
//                from the load request). Compaction then reads local SSD, which is CPU bound and is
//                what cloud E2E actually does. Two query numbers come out of this mode: a cold one
//                (cache emptied first, so the query fetches from S3 and repopulates) and a hot one
//                (same query again against the cache it just filled).
//
// Mixing them is what made earlier runs incoherent: a warm-cache compaction paired with a
// cold-cache query described no real deployment.
enum class BenchCachePolicy { kDirect, kWriteBack };

struct Measurement {
    double wall_s = 0;
    double cpu_s = 0;
};

double nearest_rank_percentile(const std::vector<double>& sorted_samples, size_t percentile) {
    DORIS_CHECK(!sorted_samples.empty());
    DORIS_CHECK(percentile > 0);
    DORIS_CHECK(percentile <= 100);
    const size_t whole_hundreds = sorted_samples.size() / 100;
    const size_t remainder = sorted_samples.size() % 100;
    const size_t percentile_rank =
            whole_hundreds * percentile + (remainder * percentile + 99) / 100;
    const size_t percentile_index = percentile_rank - 1;
    return sorted_samples[percentile_index];
}

int parse_query_iterations(std::string_view value) {
    int query_iterations = 0;
    const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), query_iterations);
    DORIS_CHECK(error == std::errc());
    DORIS_CHECK(end == value.data() + value.size());
    DORIS_CHECK(query_iterations > 0);
    return query_iterations;
}

// Prevent a future timing-report change from silently using the upper median for
// even-sized samples or from treating p99 as a maximum by convention.
TEST(DISABLED_SniiBenchmarkPercentile, SelectsNearestRankForOddAndEvenSamples) {
    const std::vector<double> odd {1.0, 2.0, 3.0, 4.0, 5.0};
    EXPECT_DOUBLE_EQ(nearest_rank_percentile(odd, 50), 3.0);
    EXPECT_DOUBLE_EQ(nearest_rank_percentile(odd, 99), 5.0);

    const std::vector<double> even {1.0, 2.0, 3.0, 4.0};
    EXPECT_DOUBLE_EQ(nearest_rank_percentile(even, 50), 2.0);
    EXPECT_DOUBLE_EQ(nearest_rank_percentile(even, 99), 4.0);

    std::vector<double> more_than_one_hundred_samples;
    for (double sample = 0; sample <= 100; ++sample) {
        more_than_one_hundred_samples.push_back(sample);
    }
    EXPECT_DOUBLE_EQ(nearest_rank_percentile(more_than_one_hundred_samples, 99), 99.0);
    EXPECT_NE(nearest_rank_percentile(more_than_one_hundred_samples, 99),
              more_than_one_hundred_samples.back());
}

TEST(DISABLED_SniiBenchmarkPercentile, RejectsInvalidPercentiles) {
    const std::vector<double> samples {1.0};
    EXPECT_DEATH({ static_cast<void>(nearest_rank_percentile(samples, 0)); }, "");
    EXPECT_DEATH({ static_cast<void>(nearest_rank_percentile(samples, 101)); }, "");
}

TEST(DISABLED_SniiBenchmarkConfig, ParsesPositiveQueryIterations) {
    EXPECT_EQ(parse_query_iterations("30"), 30);
}

TEST(DISABLED_SniiBenchmarkConfig, RejectsInvalidQueryIterations) {
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations("0")); }, "");
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations("-1")); }, "");
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations("30junk")); }, "");
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations("abc")); }, "");

    const std::string overflow =
            std::to_string(static_cast<int64_t>(std::numeric_limits<int>::max()) + 1);
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations(overflow)); }, "");
}

// What a scan node's RuntimeProfile would show for one query, minus the plumbing. The harness
// already builds a full OlapReaderStatistics per segment but only ever read file_cache_stats off
// it, so every timer and every SNII counter was being discarded. Captured per query case by
// diffing the monotonic counters around each read_from_index(), then summed over segments,
// rowsets and iterations -- attribution the aggregate IO totals cannot give.
struct CaseProfile {
    // Timers, ns. index_query is the whole read_from_index(); the rest are its named parts.
    int64_t index_query_ns = 0;
    int64_t searcher_open_ns = 0;
    int64_t searcher_search_ns = 0;
    int64_t analyzer_ns = 0;
    int64_t lookup_ns = 0;
    // Index IO. remote_io/local_io are counts; the physical bytes are what actually crossed the
    // network, as opposed to the logical bytes the reader asked for.
    int64_t remote_io = 0;
    int64_t local_io = 0;
    int64_t bytes_remote = 0;
    int64_t bytes_local = 0;
    int64_t physical_bytes = 0;
    // Logical bytes the index layer asked for, vs physical bytes that crossed the network.
    // physical/request is read amplification. Caveat for V3: FSIndexInput::readInternal charges
    // request_bytes the 4096-byte buffer refill (config::inverted_index_read_buffer_size), not the
    // caller's true need, so V3's request_bytes is itself buffer-granular. SNII passes its real
    // request size into _record_read_stats. The pair is therefore only strictly comparable on the
    // cached path, where physical comes from CachedRemoteFileReader for both.
    int64_t request_bytes = 0;
    int64_t read_bytes = 0;
    int64_t range_reads = 0;
    int64_t serial_rounds = 0;
    int64_t remote_io_ns = 0;
    int64_t local_io_ns = 0;
    // SNII-only; stays all-zero for V3, which is itself the signal that the path was not taken.
    snii::SniiQueryStats snii;
    int64_t hits = 0;

    void add_delta(const OlapReaderStatistics& before, const OlapReaderStatistics& after) {
        index_query_ns += after.inverted_index_query_timer - before.inverted_index_query_timer;
        searcher_open_ns += after.inverted_index_searcher_open_timer -
                            before.inverted_index_searcher_open_timer;
        searcher_search_ns += after.inverted_index_searcher_search_timer -
                              before.inverted_index_searcher_search_timer;
        analyzer_ns += after.inverted_index_analyzer_timer - before.inverted_index_analyzer_timer;
        lookup_ns += after.inverted_index_lookup_timer - before.inverted_index_lookup_timer;

        const auto& a = after.file_cache_stats;
        const auto& b = before.file_cache_stats;
        remote_io += a.inverted_index_num_remote_io_total - b.inverted_index_num_remote_io_total;
        local_io += a.inverted_index_num_local_io_total - b.inverted_index_num_local_io_total;
        bytes_remote +=
                a.inverted_index_bytes_read_from_remote - b.inverted_index_bytes_read_from_remote;
        bytes_local +=
                a.inverted_index_bytes_read_from_local - b.inverted_index_bytes_read_from_local;
        physical_bytes += a.inverted_index_remote_physical_read_bytes -
                          b.inverted_index_remote_physical_read_bytes;
        request_bytes += a.inverted_index_request_bytes - b.inverted_index_request_bytes;
        read_bytes += a.inverted_index_read_bytes - b.inverted_index_read_bytes;
        range_reads += a.inverted_index_range_read_count - b.inverted_index_range_read_count;
        serial_rounds += a.inverted_index_serial_read_rounds - b.inverted_index_serial_read_rounds;
        remote_io_ns += a.inverted_index_remote_io_timer - b.inverted_index_remote_io_timer;
        local_io_ns += a.inverted_index_local_io_timer - b.inverted_index_local_io_timer;

        add_snii_delta(after.snii_stats, before.snii_stats);
    }

private:
    void add_snii_delta(const snii::SniiQueryStats& a, const snii::SniiQueryStats& b) {
        snii.prx_raw_frames += a.prx_raw_frames - b.prx_raw_frames;
        snii.prx_zstd_frames += a.prx_zstd_frames - b.prx_zstd_frames;
        snii.prx_pfor_frames += a.prx_pfor_frames - b.prx_pfor_frames;
        snii.prx_plaintext_bytes += a.prx_plaintext_bytes - b.prx_plaintext_bytes;
        snii.prx_total_docs += a.prx_total_docs - b.prx_total_docs;
        snii.prx_selected_docs += a.prx_selected_docs - b.prx_selected_docs;
        snii.prx_total_positions += a.prx_total_positions - b.prx_total_positions;
        snii.prx_selected_positions += a.prx_selected_positions - b.prx_selected_positions;
        snii.prx_fetch_ns += a.prx_fetch_ns - b.prx_fetch_ns;
        snii.prx_decode_ns += a.prx_decode_ns - b.prx_decode_ns;
        snii.prx_phrase_verify_ns += a.prx_phrase_verify_ns - b.prx_phrase_verify_ns;
        snii.phrase_candidate_docs += a.phrase_candidate_docs - b.phrase_candidate_docs;
        snii.phrase_candidate_visits += a.phrase_candidate_visits - b.phrase_candidate_visits;
        snii.phrase_prefix_leading_candidate_docs +=
                a.phrase_prefix_leading_candidate_docs - b.phrase_prefix_leading_candidate_docs;
        snii.phrase_prefix_tail_candidate_visits +=
                a.phrase_prefix_tail_candidate_visits - b.phrase_prefix_tail_candidate_visits;
        snii.common_grams_candidate_queries +=
                a.common_grams_candidate_queries - b.common_grams_candidate_queries;
        snii.common_grams_plain_plans += a.common_grams_plain_plans - b.common_grams_plain_plans;
        snii.common_grams_gram_plans += a.common_grams_gram_plans - b.common_grams_gram_plans;
        snii.common_grams_planning_ns += a.common_grams_planning_ns - b.common_grams_planning_ns;
    }
};

struct PhaseResult {
    Measurement import;
    Measurement compaction;
    Measurement cold_query; // p50 over query iterations
    Measurement cold_query_p99;
    // Only filled for BenchCachePolicy::kWriteBack: the same queries run again without emptying the
    // cache, so they are served by the blocks the cold pass just wrote back.
    Measurement hot_query;
    Measurement hot_query_p99;
    bool has_hot = false;
    int64_t hot_remote_read_bytes = 0;
    int64_t hot_range_read_count = 0;
    int64_t index_bytes = -1; // -1 = not measured (remote rowsets expose S3 keys, not paths)
    // Deterministic remote-IO comparables. Unlike latency these do not move with machine or network
    // load, so they carry the SNII-vs-V3 verdict in remote mode.
    int64_t remote_physical_read_bytes = 0;
    int64_t request_bytes = 0;
    int64_t read_bytes = 0;
    // Remote GET / local-hit counts (CachedRemoteFileReader's num_*_io_total). GETs x per-GET
    // cost is what a block-fetch-bound cold pass actually pays, and the number the E2E profile's
    // InvertedIndexNumRemoteIOTotal compares against.
    int64_t num_remote_io = 0;
    int64_t num_local_io = 0;
    int64_t range_read_count = 0;
    int64_t serial_read_rounds = 0;
    // How many columns actually went through index compaction. Zero means the index was rebuilt
    // from raw data instead, which is a completely different cost profile -- comparing a format
    // that compacted against one that rebuilt would be meaningless.
    int64_t index_compaction_columns = -1;
    int64_t matched_docs = 0;
    double cold_wall_min = 0;
    double cold_wall_max = 0;
    std::vector<double> cold_cpu_samples;
    std::vector<double> cold_wall_samples;
    std::vector<double> hot_cpu_samples;
    std::vector<double> hot_wall_samples;
    std::vector<int64_t> per_case;
    // One entry per kQueryCases, summed over every iteration. The report divides by
    // profile_iterations, so a per-case number reads as "what one pass costs" while still being
    // averaged rather than taken from whichever single iteration happened to be last.
    std::vector<CaseProfile> cold_profile;
    std::vector<CaseProfile> hot_profile;
    int profile_iterations = 1;
    // False when the query reads the input rowsets: compaction is skipped, and its row must read
    // as "not run" rather than 0.00s, which would look like a free merge.
    bool compaction_ran = true;
    // False for the format that SNII_BENCH_ONLY_FORMAT skipped. Everything in this struct is then
    // default-constructed, and printing it as a measurement would be a lie.
    bool ran = false;
    // True when the import phase was skipped by rowset reuse; its Measurement is then zero and
    // must not be printed as if the import were free.
    bool import_reused = false;
};

class DISABLED_SniiVsV3BenchmarkTest : public ::testing::Test {
protected:
    static constexpr int kDefaultQueryIterations = 30;
    // build_rowsets() asserts num_segments == num_rows / max_rows_per_segment, so this must divide
    // the per-file document count exactly. The default matches the prepared corpus's 200 documents
    // per file. SNII_BENCH_ROWS_PER_SEGMENT overrides it: per-segment open cost is a function of
    // the segment's vocabulary (SNII resident-reads the whole dict region and BSBF at open when
    // they fit their 256 KB caps), so a 200-row segment sits at the bottom of that curve and a
    // production-sized one does not -- comparing the two is the point of the knob.
    static constexpr int64_t kRowsPerSegment = 200;
    static int64_t _rows_per_segment() {
        const char* const env = std::getenv("SNII_BENCH_ROWS_PER_SEGMENT");
        return env != nullptr ? parse_query_iterations(env) : kRowsPerSegment;
    }
    static constexpr int64_t kBenchTabletId = 990001;
    // One tablet id per format. A segment's remote path is <prefix>/data/<tablet_id>/<rowset_id>_*,
    // and rowset ids restart from _inc_id every run, so two formats sharing a tablet id also share
    // every object path. With a per-pid prefix that was invisible; under SNII_BENCH_REUSE_ROWSETS
    // the prefix is stable, and the second format's import silently overwrote the first's index
    // files -- the reusing run then read the wrong format's bytes and failed with
    // "tail_pointer: tail_checksum mismatch".
    static int64_t _bench_tablet_id(InvertedIndexStorageFormatPB format) {
        return kBenchTabletId + (format == InvertedIndexStorageFormatPB::SNII ? 1 : 0);
    }
    // What the cache directory still weighs once it is empty of data: the RocksDB meta store and
    // the LRU dump files, which are never removed. ~37 KB on a run that caches everything, but it
    // grows with the number of blocks ever inserted, so a small-capacity run that churns 120 MB
    // through a 3 MB queue leaves ~330 KB of metadata behind. 2 MB separates that from any
    // meaningful amount of retained data (a cold pass fetches 12-16 MB).
    static constexpr int64_t kColdCacheResidueBytes = 2 * 1024 * 1024;
    // 400 x 25 ms = 10 s for the GC thread to finish removing what clear_file_caches() marked.
    static constexpr int kCacheDrainSpins = 400;
    static constexpr int kCacheDrainSleepMs = 25;
    // 600 x 50 ms = 30 s for BlockFileCache to finish opening asynchronously.
    static constexpr int kCacheOpenSpins = 600;
    static constexpr int kCacheOpenSleepMs = 50;
    static constexpr const char* kCacheDirPrefix = "snii_bench_file_cache_";
    static constexpr const char* kS3Prefix = "snii_bench_";

    // Drops every block from the file cache and returns what the directory still weighs.
    // clear_file_caches() only marks referenced blocks for recycling, so it can return with the
    // cache still full (num_cells_wait_recycle); BlockFileCache's background GC thread does the
    // actual removal. Poll until it has, otherwise a "cold" query is served from the cache it was
    // supposed to have dropped -- which showed up as a wall spread of [0.010, 0.234] across
    // iterations that were all supposed to be cold.
    int64_t _empty_file_cache() {
        int64_t remaining = _cache_dir_bytes();
        for (int spin = 0; spin < kCacheDrainSpins && remaining > kColdCacheResidueBytes; ++spin) {
            static_cast<void>(io::FileCacheFactory::instance()->clear_file_caches(true));
            std::this_thread::sleep_for(std::chrono::milliseconds(kCacheDrainSleepMs));
            remaining = _cache_dir_bytes();
        }
        return remaining;
    }

    // Bytes the block file cache is actually holding, read off the filesystem. The queue metrics
    // are not usable for this -- they report zero while the block files are still on disk.
    int64_t _cache_dir_bytes() const {
        if (_file_cache_dir.empty()) {
            return 0;
        }
        int64_t total = 0;
        std::error_code ec;
        for (auto it = std::filesystem::recursive_directory_iterator(_file_cache_dir, ec);
             !ec && it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
            if (it->is_regular_file(ec)) {
                total += static_cast<int64_t>(it->file_size(ec));
            }
        }
        return total;
    }

    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + std::string(kDestDir);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTmpDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTmpDir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(kTmpDir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        // Match the existing index-compaction perf test so the two are comparable.
        config::enable_segcompaction = false;
        config::enable_ordered_data_compaction = false;
        config::total_permits_for_compaction_score = 200000;
        config::inverted_index_ram_dir_enable = true;
        config::string_type_length_soft_limit_bytes = 10485760;

        // The query path goes through InvertedIndexQueryCache::instance() unconditionally, and
        // ExecEnv hands out a null pointer unless something installs one. Own them here and put the
        // previous globals back in TearDown so a later test never sees a dangling pointer.
        constexpr int64_t kIndexCacheBytes = 1024L * 1024L * 1024L;
        _searcher_cache.reset(segment_v2::InvertedIndexSearcherCache::create_global_instance(
                kIndexCacheBytes, 1));
        _query_cache.reset(
                segment_v2::InvertedIndexQueryCache::create_global_cache(kIndexCacheBytes, 1));
        _previous_searcher_cache = ExecEnv::GetInstance()->get_inverted_index_searcher_cache();
        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_searcher_cache.get());
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());

        _corpus_files = _discover_corpus();
    }

    void TearDown() override {
        if (_tablet != nullptr) {
            static_cast<void>(
                    io::global_local_filesystem()->delete_directory(_tablet->tablet_path()));
        }
        static_cast<void>(io::global_local_filesystem()->delete_directory(_absolute_dir));
        static_cast<void>(io::global_local_filesystem()->delete_directory(kTmpDir));
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);

        config::enable_segcompaction = true;
        config::enable_ordered_data_compaction = true;
        config::total_permits_for_compaction_score = 1000000;
        config::string_type_length_soft_limit_bytes = 1048576;
        // do_compaction() raises this; the sibling DISABLED_IndexCompactionPerformanceTest
        // restores it the same way. Leaving it set changes later tests in the same binary.
        config::compaction_batch_size = _origin_compaction_batch_size;
        config::inverted_index_compaction_enable = _origin_index_compaction_enable;
        config::inverted_index_ram_dir_enable = _origin_ram_dir_enable;

        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_previous_searcher_cache);
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);

        // Unconditionally, not inside _teardown_remote(): _setup_remote() installs the fixture's
        // factory before any of its five later failure paths, and each of those makes the caller
        // GTEST_SKIP(), which skips _teardown_remote() entirely. ExecEnv would then keep a pointer
        // to a factory this fixture is about to destroy, and every later test in doris_be_test that
        // touches the file cache would read freed memory.
        _restore_file_cache_globals();
    }

    // Puts back everything _setup_remote() replaced. Safe to call when setup never ran.
    void _restore_file_cache_globals() {
        if (_origin_file_cache_factory_saved) {
            ExecEnv::GetInstance()->_file_cache_factory = _origin_file_cache_factory;
            _origin_file_cache_factory_saved = false;
            // Deliberately leaked, never destroyed. FileWriter::init_cache_builder hands the S3
            // write path a raw BlockFileCache* (file_writer.h), and S3FileWriter fills the cache
            // from its upload threads -- its own comment notes the writer may already be gone by
            // then. The cache's own GC/monitor threads are likewise only stopped by ~BlockFileCache.
            // In a real BE the factory is a process-lifetime singleton so none of that can dangle;
            // destroying it here frees the caches out from under those threads.
            static_cast<void>(_owned_file_cache_factory.release());
        }
        config::enable_file_cache = _origin_enable_file_cache;
    }

    // Process CPU time, which is what the verdict is based on; wall time is kept only to detect a
    // run that was descheduled or IO bound.
    static Measurement measure(const std::function<void()>& body) {
        timespec cpu_start {};
        timespec cpu_end {};
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
        const int64_t wall_start = MonotonicNanos();
        body();
        const int64_t wall_end = MonotonicNanos();
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_end);

        Measurement m;
        m.wall_s = static_cast<double>(wall_end - wall_start) / 1e9;
        m.cpu_s = static_cast<double>(cpu_end.tv_sec - cpu_start.tv_sec) +
                  static_cast<double>(cpu_end.tv_nsec - cpu_start.tv_nsec) / 1e9;
        return m;
    }

    static int _query_iterations() {
        const char* const env_iterations = std::getenv("SNII_BENCH_QUERY_ITERATIONS");
        return env_iterations != nullptr ? parse_query_iterations(env_iterations)
                                         : kDefaultQueryIterations;
    }

    // Restricts the pass to one case by label. A write-back cold pass clears the cache once and
    // then runs every case against a shared cache, so whichever case first touches a 1 MB block
    // pays for it and later cases needing that block read it locally -- per-case remote IO there
    // measures ordering, not the query. Isolating a case removes the other cases' prewarming and
    // makes its cold cost its own.
    static const char* _only_case() { return std::getenv("SNII_BENCH_ONLY_CASE"); }

    static bool _case_enabled(const QueryCase& qc) {
        const char* only = _only_case();
        return only == nullptr || *only == '\0' || std::string_view(only) == qc.label;
    }

    // SNII_BENCH_REUSE_ROWSETS=1 keeps the imported rowsets on S3 between runs and rebuilds them
    // from a serialized RowsetMeta manifest instead of re-importing. Import is ~88% of a short
    // run's wall clock and its output is byte-identical for the same corpus and writer, so a
    // reader-side iteration loop pays it for nothing.
    //
    // Automatic invalidation covers everything cheap to detect and easy to get wrong -- corpus
    // contents, rows/segment, schema (so adding an index re-imports), storage format, cache
    // policy. It CANNOT detect a changed writer or on-disk format: only the caller knows that,
    // which is why reuse is opt-in rather than the default. Delete the manifest, or run without
    // the knob once, after touching anything on the write path.
    static bool _reuse_rowsets() {
        const char* const v = std::getenv("SNII_BENCH_REUSE_ROWSETS");
        return v != nullptr && *v != '\0' && std::string_view(v) != "0";
    }

    std::string _reuse_dir() const {
        const char* const env = std::getenv("SNII_BENCH_REUSE_DIR");
        if (env != nullptr && *env != '\0') {
            return env;
        }
        // Beside the corpus: a manifest is only ever valid for the corpus it was built from.
        return _corpus_files.empty()
                       ? std::string(kTmpDir) + "/rowset_reuse"
                       : std::filesystem::path(_corpus_files.front()).parent_path().string() +
                                 "/.rowset_reuse";
    }

    std::string _reuse_tag(InvertedIndexStorageFormatPB format, bool write_back) const {
        return std::string(format == InvertedIndexStorageFormatPB::SNII ? "snii" : "v3") +
               (write_back ? "_wb" : "_direct");
    }

    // Verbatim-compared text, not a hash: a fingerprint collision would silently serve stale
    // rowsets, which is far worse than an occasional needless re-import.
    std::string _reuse_fingerprint(InvertedIndexStorageFormatPB format, bool write_back) const {
        std::string fp;
        fp += "rows_per_segment=" + std::to_string(_rows_per_segment()) + "\n";
        fp += "format=" + std::to_string(static_cast<int>(format)) + "\n";
        fp += "write_back=" + std::to_string(write_back ? 1 : 0) + "\n";
        fp += "bucket=" + _reuse_bucket + "\n";
        fp += "tablet_id=" + std::to_string(_bench_tablet_id(format)) + "\n";
        fp += "prefix=" + _reuse_prefix + "\n";
        std::string schema_pb;
        if (_tablet_schema != nullptr) {
            TabletSchemaPB pb;
            _tablet_schema->to_schema_pb(&pb);
            // Deterministic on purpose: TabletIndexPB carries the analyzer settings in a
            // protobuf map, and plain SerializeToString leaves map order unspecified, so the
            // same schema hashes differently run to run and reuse never hits.
            google::protobuf::io::StringOutputStream stream(&schema_pb);
            google::protobuf::io::CodedOutputStream coded(&stream);
            coded.SetSerializationDeterministic(true);
            if (!pb.SerializeToCodedStream(&coded)) {
                schema_pb.clear();
            }
        }
        fp += "schema_bytes=" + std::to_string(schema_pb.size()) +
              " h=" + std::to_string(std::hash<std::string> {}(schema_pb)) + "\n";
        std::error_code ec;
        for (const auto& f : _corpus_files) {
            fp += "file=" + std::filesystem::path(f).filename().string() + " size=" +
                  std::to_string(static_cast<int64_t>(std::filesystem::file_size(f, ec))) + "\n";
        }
        return fp;
    }

    bool _load_rowset_manifest(InvertedIndexStorageFormatPB format, bool write_back,
                               const std::optional<StorageResource>& storage_resource,
                               std::vector<RowsetSharedPtr>* rowsets) {
        const std::string base = _reuse_dir() + "/" + _reuse_tag(format, write_back);
        std::ifstream fp_in(base + ".fp");
        if (!fp_in) {
            return false;
        }
        const std::string got((std::istreambuf_iterator<char>(fp_in)),
                              std::istreambuf_iterator<char>());
        const std::string want = _reuse_fingerprint(format, write_back);
        if (got != want) {
            std::cout << "  reuse: fingerprint changed, re-importing" << std::endl;
            // Which line moved -- otherwise a silent re-import looks like the feature is broken.
            auto split = [](const std::string& text) {
                std::vector<std::string> lines;
                std::istringstream in(text);
                std::string line;
                while (std::getline(in, line)) {
                    lines.push_back(line);
                }
                return lines;
            };
            const std::vector<std::string> saved = split(got);
            const std::vector<std::string> current = split(want);
            for (size_t i = 0; i < std::max(saved.size(), current.size()); ++i) {
                const std::string& a = i < saved.size() ? saved[i] : std::string();
                const std::string& b = i < current.size() ? current[i] : std::string();
                if (a != b) {
                    std::cout << "    - saved:   " << a << "\n    + current: " << b << std::endl;
                }
            }
            return false;
        }
        std::ifstream bin(base + ".bin", std::ios::binary);
        if (!bin) {
            return false;
        }
        std::vector<RowsetSharedPtr> loaded;
        while (true) {
            uint32_t len = 0;
            if (!bin.read(reinterpret_cast<char*>(&len), sizeof(len))) {
                break;
            }
            std::string blob(len, '\0');
            if (!bin.read(blob.data(), len)) {
                return false;
            }
            auto meta = std::make_shared<RowsetMeta>();
            if (!meta->init(blob)) {
                std::cout << "  reuse: RowsetMeta::init failed, re-importing" << std::endl;
                return false;
            }
            // The S3FileSystem object is rebuilt every run; the meta only carries a resource id,
            // so rebind it or every read would go through a null filesystem.
            if (storage_resource.has_value()) {
                meta->set_remote_storage_resource(*storage_resource);
            }
            RowsetSharedPtr rs;
            const Status st =
                    RowsetFactory::create_rowset(_tablet_schema, _tablet->tablet_path(), meta, &rs);
            if (!st.ok()) {
                std::cout << "  reuse: create_rowset failed (" << st.to_string()
                          << "), re-importing" << std::endl;
                return false;
            }
            static_cast<void>(_tablet->add_rowset(rs));
            loaded.push_back(std::move(rs));
        }
        if (loaded.size() != _corpus_files.size()) {
            std::cout << "  reuse: manifest has " << loaded.size() << " rowsets, corpus has "
                      << _corpus_files.size() << "; re-importing" << std::endl;
            return false;
        }
        *rowsets = std::move(loaded);
        return true;
    }

    void _save_rowset_manifest(InvertedIndexStorageFormatPB format, bool write_back,
                               const std::vector<RowsetSharedPtr>& rowsets) {
        const std::string dir = _reuse_dir();
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        const std::string base = dir + "/" + _reuse_tag(format, write_back);
        std::ofstream bin(base + ".bin", std::ios::binary | std::ios::trunc);
        if (!bin) {
            std::cout << "  reuse: cannot write " << base << ".bin" << std::endl;
            return;
        }
        for (const auto& rs : rowsets) {
            std::string blob;
            if (rs == nullptr || !rs->rowset_meta()->serialize(&blob)) {
                std::cout << "  reuse: serialize failed; manifest not written" << std::endl;
                return;
            }
            const auto len = static_cast<uint32_t>(blob.size());
            bin.write(reinterpret_cast<const char*>(&len), sizeof(len));
            bin.write(blob.data(), static_cast<std::streamsize>(blob.size()));
        }
        bin.close();
        // Fingerprint last: a half-written .bin with no .fp is simply ignored next run.
        std::ofstream fp_out(base + ".fp", std::ios::trunc);
        fp_out << _reuse_fingerprint(format, write_back);
        std::cout << "  reuse: manifest saved to " << base << ".{bin,fp}" << std::endl;
    }

    // Which rowsets the query phase reads. Compaction merges every input rowset into one, so the
    // default measures a single merged index -- the post-compaction steady state. A tablet that is
    // still ingesting has many rowsets live at once, and a cold query there opens one index per
    // rowset and pays a separate round of remote reads for each; SNII_BENCH_QUERY_INPUT_ROWSETS=1
    // measures that shape instead. Both are real, and they answer different questions, so the
    // benchmark reports which one it ran rather than silently picking.
    static bool _query_input_rowsets() {
        return std::getenv("SNII_BENCH_QUERY_INPUT_ROWSETS") != nullptr;
    }

    std::vector<std::string> _discover_corpus() const {
        const char* env_dir = std::getenv("SNII_BENCH_CORPUS_DIR");
        const std::string dir =
                env_dir != nullptr
                        ? std::string(env_dir)
                        : _current_dir + "/be/test/storage/index/inverted/data/performance";
        std::vector<std::string> files;
        if (!std::filesystem::exists(dir)) {
            return files;
        }
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            const std::string name = entry.path().filename().string();
            if (entry.is_regular_file() && name.starts_with("wikipedia") &&
                name.ends_with(".json")) {
                files.push_back(entry.path().string());
            }
        }
        // Deterministic load order: the segment layout must not depend on readdir order, or the two
        // formats would not be indexing the same thing in the same sequence.
        std::sort(files.begin(), files.end());
        return files;
    }

    void _build_tablet(InvertedIndexStorageFormatPB storage_format) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(storage_format);

        std::map<std::string, std::string> properties;
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);

        // title carries its own inverted index, not just content. Its postings are tiny next to
        // content's, which is a different regime for both formats -- per-term overhead and block
        // layout dominate where there is little data to amortise them over.
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000,
                                               "idx_title", 0, "STRING", "title", properties);
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001,
                                               "idx_content", 1, "STRING", "content", properties);
        IndexCompactionUtils::construct_column(schema_pb.add_column(), 2, "STRING", "redirect");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), 3, "STRING", "namespace");

        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);

        // Default-construct rather than TabletMeta(schema): only the default ctor initialises
        // _delete_bitmap, and init_from_pb writes through it. Going via the PB is the only way to
        // set tablet_id, which CachedRemoteFileReader requires to be > 0 for Doris tables.
        TabletMetaPB meta_pb;
        meta_pb.set_tablet_id(_bench_tablet_id(storage_format));
        meta_pb.set_schema_hash(1);
        meta_pb.set_table_id(1);
        meta_pb.set_partition_id(1);
        meta_pb.set_replica_id(1);
        *meta_pb.mutable_schema() = schema_pb;
        TabletMetaSharedPtr tablet_meta(new TabletMeta());
        tablet_meta->init_from_pb(meta_pb);
        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
        ASSERT_TRUE(_tablet->init().ok());
        std::cout << "  tablet_id=" << _tablet->tablet_id() << std::endl;

        static_cast<void>(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()));
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    // Stand up the real deployment chain: S3FileSystem -> CachedRemoteFileReader -> BlockFileCache.
    // Credentials come from the environment so they never live in the source tree; the endpoint is
    // a real S3 service, so this measures genuine remote behaviour rather than a local stand-in.
    // Returns false (and skips) when the environment is not configured.
    bool _setup_remote() {
        auto env = [](const char* name) -> std::string {
            const char* v = std::getenv(name);
            return v != nullptr ? std::string(v) : std::string();
        };
        const std::string ak = env("SNII_BENCH_S3_AK");
        const std::string sk = env("SNII_BENCH_S3_SK");
        const std::string endpoint = env("SNII_BENCH_S3_ENDPOINT");
        const std::string region = env("SNII_BENCH_S3_REGION");
        const std::string bucket = env("SNII_BENCH_S3_BUCKET");
        if (ak.empty() || sk.empty() || endpoint.empty() || bucket.empty()) {
            return false;
        }

        _origin_enable_file_cache = config::enable_file_cache;
        config::enable_file_cache = true;

        // Disk-backed, not memory-backed: production caches to disk, and a cold query has to be
        // able to evict the cache's own backing pages as well.
        // Must live on a disk with real free space. BlockFileCache watches the filesystem holding
        // this path and, above config::file_cache_enter_disk_resource_limit_mode_percent (90%),
        // enters disk resource limit mode: its background thread then evicts everything as fast as
        // the write path caches it, so the cache stays empty no matter what write_file_cache says.
        // The repo checkout sits on a 98%-full disk, which is what silently disabled caching here.
        const char* cache_root = std::getenv("SNII_BENCH_CACHE_DIR");
        _file_cache_dir = (cache_root != nullptr && *cache_root != '\0')
                                  ? std::string(cache_root) + "/" + kCacheDirPrefix +
                                            std::to_string(::getpid())
                                  : _absolute_dir + "/file_cache";
        // The cache dir is never deleted on teardown (the factory is leaked, see _teardown_remote),
        // so sweep leftovers from earlier runs here instead.
        if (cache_root != nullptr && *cache_root != '\0') {
            std::error_code sec;
            for (auto it = std::filesystem::directory_iterator(cache_root, sec);
                 !sec && it != std::filesystem::directory_iterator(); it.increment(sec)) {
                const std::string name = it->path().filename().string();
                if (!name.starts_with(kCacheDirPrefix)) {
                    continue;
                }
                // Only reap directories whose owning process is gone. A second benchmark running
                // concurrently has a live pid, and deleting its cache out from under it would
                // corrupt that run rather than this one.
                const std::string pid_part = name.substr(std::strlen(kCacheDirPrefix));
                const int64_t owner = std::atoll(pid_part.c_str());
                if (owner > 0 && ::kill(static_cast<pid_t>(owner), 0) == 0) {
                    continue;
                }
                std::error_code rec;
                std::filesystem::remove_all(it->path(), rec);
            }
        }
        static_cast<void>(io::global_local_filesystem()->delete_directory(_file_cache_dir));
        static_cast<void>(io::global_local_filesystem()->create_directory(_file_cache_dir));
        // Reading a block back out of the cache goes FSFileCacheStorage::get_or_open_file_reader ->
        // FDCache::instance(), which is just ExecEnv::file_cache_open_fd_cache(). Only
        // exec_env_init.cpp:318 ever creates it, and this fixture does not run that, so the pointer
        // is null and the first cache hit dereferences it. Never seen until the cache actually
        // retained data.
        if (ExecEnv::GetInstance()->file_cache_open_fd_cache() == nullptr) {
            ExecEnv::GetInstance()->set_file_cache_open_fd_cache(std::make_unique<io::FDCache>());
        }
        _origin_file_cache_factory = ExecEnv::GetInstance()->_file_cache_factory;
        _origin_file_cache_factory_saved = true;
        _owned_file_cache_factory = std::make_unique<io::FileCacheFactory>();
        ExecEnv::GetInstance()->_file_cache_factory = _owned_file_cache_factory.get();
        // Built the way a real BE builds it. Hand-filling FileCacheSettings leaves ttl_queue_*
        // at zero and `storage` empty, and the queue sizes then do not add up to capacity, so
        // reservations for the queue the write path uses can never be satisfied.
        // Shrinking this is how the benchmark models a working set larger than the cache: index
        // blocks land in the normal queue, which get_file_cache_settings sizes at 40% of capacity,
        // so a 16 MB index stops fitting once capacity drops below ~40 MB and the LRU starts
        // evicting between queries. Default is large enough that nothing is ever evicted.
        int64_t capacity_mb = 4096;
        if (const char* cap = std::getenv("SNII_BENCH_CACHE_CAPACITY_MB");
            cap != nullptr && *cap != '\0') {
            capacity_mb = std::max<int64_t>(1, std::atoll(cap));
        }
        io::FileCacheSettings settings =
                io::get_file_cache_settings(/*capacity=*/capacity_mb * 1024L * 1024L,
                                            /*max_query_cache_size=*/0);
        settings.max_file_block_size = 1024 * 1024;
        std::cout << "  file cache capacity " << capacity_mb << " MB (normal queue "
                  << settings.query_queue_size / (1024 * 1024) << " MB)" << std::endl;
        if (!io::FileCacheFactory::instance()->create_file_cache(_file_cache_dir, settings).ok()) {
            return false;
        }
        // create_file_cache() returns before the cache finishes opening. Until then try_reserve()
        // diverts to try_reserve_during_async_load(), which throttles reservations, so a load that
        // starts immediately writes almost nothing into the cache -- the whole point of
        // write_file_cache. A real BE has been up long enough for this to be done.
        {
            io::BlockFileCache* cache =
                    io::FileCacheFactory::instance()->get_by_path(_file_cache_dir);
            if (cache == nullptr) {
                return false;
            }
            for (int i = 0; i < kCacheOpenSpins && !cache->get_async_open_success(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(kCacheOpenSleepMs));
            }
            if (!cache->get_async_open_success()) {
                std::cerr << "file cache did not finish async open" << std::endl;
                return false;
            }
        }

        if (ExecEnv::GetInstance()->s3_file_upload_thread_pool() == nullptr) {
            std::unique_ptr<ThreadPool> pool;
            if (!ThreadPoolBuilder("snii_bench_s3_upload")
                         .set_min_threads(1)
                         .set_max_threads(8)
                         .build(&pool)
                         .ok()) {
                return false;
            }
            ExecEnv::GetInstance()->_s3_file_upload_thread_pool = std::move(pool);
        }

        S3Conf s3_conf;
        s3_conf.client_conf.ak = ak;
        s3_conf.client_conf.sk = sk;
        s3_conf.client_conf.endpoint = endpoint;
        s3_conf.client_conf.region = region;
        s3_conf.bucket = bucket;
        // A per-pid prefix cannot be reused across runs, so reuse mode pins a stable one. It is
        // also why teardown must not delete it -- see _teardown_remote.
        s3_conf.prefix = _reuse_rowsets() ? std::string(kS3Prefix) + "reuse"
                                          : kS3Prefix + std::to_string(::getpid());
        _reuse_bucket = bucket;
        _reuse_prefix = s3_conf.prefix;
        auto fs = io::S3FileSystem::create(std::move(s3_conf), "snii-bench-s3-fs");
        if (!fs.has_value()) {
            std::cout << "  S3FileSystem::create failed: " << fs.error() << std::endl;
            return false;
        }
        _remote_fs = fs.value();
        std::cout << "  remote: s3://" << bucket << "/" << kS3Prefix << ::getpid() << " via "
                  << endpoint << std::endl;
        return true;
    }

    void _teardown_remote() {
        if (_remote_fs == nullptr) {
            return;
        }
        if (_reuse_rowsets()) {
            // The whole point: the objects have to outlive the process for the next run to skip
            // importing. Cleaning up is then the caller's job (delete the manifest dir and the
            // s3://<bucket>/snii_bench_reuse prefix).
            std::cout << "  reuse: leaving s3://.../" << _reuse_prefix << " in place" << std::endl;
            _remote_fs.reset();
            _restore_file_cache_globals();
            return;
        }
        // Otherwise every run leaves a full corpus (~40 MB per format, plus compaction output)
        // in the user's bucket forever.
        const Status st = _remote_fs->delete_directory("");
        if (!st.ok()) {
            std::cout << "  warning: could not remove s3://.../" << kS3Prefix << ::getpid() << ": "
                      << st.to_string() << std::endl;
        }
        _remote_fs.reset();
        _restore_file_cache_globals();
    }

    // Pages of `path` still resident in the OS page cache. Used to prove the eviction below
    // actually worked instead of assuming it did.
    static std::pair<size_t, size_t> _resident_pages(const std::string& path) {
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            return {0, 0};
        }
        struct stat st {};
        if (::fstat(fd, &st) != 0 || st.st_size == 0) {
            ::close(fd);
            return {0, 0};
        }
        void* addr = ::mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ, MAP_SHARED, fd, 0);
        if (addr == MAP_FAILED) {
            ::close(fd);
            return {0, 0};
        }
        const size_t page_size = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
        const size_t pages = (static_cast<size_t>(st.st_size) + page_size - 1) / page_size;
        std::vector<unsigned char> vec(pages, 0);
        size_t resident = 0;
        if (::mincore(addr, static_cast<size_t>(st.st_size), vec.data()) == 0) {
            for (unsigned char v : vec) {
                resident += (v & 1u);
            }
        }
        ::munmap(addr, static_cast<size_t>(st.st_size));
        ::close(fd);
        return {resident, pages};
    }

    // A real cold query means three things, and the first two are not enough on their own:
    //   1. Doris' own caches (query/searcher/page/segment) emptied;
    //   2. the OS page cache emptied for the data and index files, otherwise every "cold" read is
    //      served from RAM and measures nothing;
    //   3. no Doris file cache in the way -- enable_file_cache is false here and the UT reads local
    //      files through LocalFileReader. In remote write-back mode BlockFileCache does
    //      participate, which is why the caller also empties it via _empty_file_cache().
    // fsync first: POSIX_FADV_DONTNEED only evicts clean pages, and compaction has just written
    // these files.
    static void _drop_caches(const std::string& tablet_dir) {
        // Not a null check: if the manager were missing, nothing would be pruned and every
        // "cold" number below would silently be a warm one. Fail instead of measuring garbage.
        auto* manager = ExecEnv::GetInstance()->get_cache_manager();
        DORIS_CHECK(manager != nullptr);
        static_cast<void>(manager->for_each_cache_prune_all(nullptr, /*force=*/true));
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(tablet_dir, ec)) {
            if (!entry.is_regular_file(ec)) {
                continue;
            }
            const int fd = ::open(entry.path().c_str(), O_RDONLY);
            if (fd < 0) {
                continue;
            }
            ::fsync(fd);
            ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
            ::close(fd);
        }
    }

    static int64_t _index_bytes(const RowsetSharedPtr& rowset) {
        // Only this rowset's own index files. The tablet directory also holds the source rowsets
        // that compaction consumed, and summing the whole directory silently reported
        // sources + output as if it were the compacted index.
        int64_t total = 0;
        const auto& seg_path = rowset->segment_path(0);
        if (!seg_path.has_value()) {
            return total;
        }
        const std::string prefix = rowset->rowset_id().to_string();
        const auto dir = std::filesystem::path(seg_path.value()).parent_path();
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
            const std::string name = entry.path().filename().string();
            if (name.starts_with(prefix) && (name.ends_with(".idx") || name.ends_with(".snii"))) {
                total += static_cast<int64_t>(entry.file_size(ec));
            }
        }
        return total;
    }

    // Runs both formats and returns {v3, snii}. Order matters and is not neutral: whichever runs
    // first pays Aws::InitAPI, the TLS handshake and connection pool, the CLucene/analyzer and
    // codec-pool lazy init, and faults in the tcmalloc arena the second run then reuses warm.
    // SNII_BENCH_REVERSE_ORDER=1 runs SNII first; comparing the two orders quantifies that bias
    // instead of assuming it away.
    // SNII_BENCH_ONLY_FORMAT=V3|SNII runs one format and leaves the other's PhaseResult empty
    // (`ran == false`). During reader-side iteration the other format's numbers do not move, so
    // re-importing and re-querying it every round is pure cost; the report prints the one that
    // ran and says the comparison is absent rather than inventing a ratio against zeros.
    static bool _format_enabled(InvertedIndexStorageFormatPB format) {
        const char* const only = std::getenv("SNII_BENCH_ONLY_FORMAT");
        if (only == nullptr || *only == '\0') {
            return true;
        }
        const std::string_view want(only);
        return format == InvertedIndexStorageFormatPB::SNII ? want == "SNII" : want == "V3";
    }

    std::pair<PhaseResult, PhaseResult> _run_both(IoMode io_mode, BenchCachePolicy policy) {
        const bool reverse = std::getenv("SNII_BENCH_REVERSE_ORDER") != nullptr;
        const bool v3_on = _format_enabled(InvertedIndexStorageFormatPB::V3);
        const bool snii_on = _format_enabled(InvertedIndexStorageFormatPB::SNII);
        if (!v3_on || !snii_on) {
            std::cout << "  [single-format] only " << (v3_on ? "V3" : "SNII")
                      << " ran; cross-format ratios are absent, not 0" << std::endl;
            PhaseResult only = _run_format(
                    v3_on ? InvertedIndexStorageFormatPB::V3 : InvertedIndexStorageFormatPB::SNII,
                    io_mode, policy);
            PhaseResult absent;
            return v3_on ? std::pair {std::move(only), std::move(absent)}
                         : std::pair {std::move(absent), std::move(only)};
        }
        std::cout << "  format order: " << (reverse ? "SNII first" : "V3 first") << std::endl;
        if (reverse) {
            PhaseResult snii = _run_format(InvertedIndexStorageFormatPB::SNII, io_mode, policy);
            PhaseResult v3 = _run_format(InvertedIndexStorageFormatPB::V3, io_mode, policy);
            return {std::move(v3), std::move(snii)};
        }
        PhaseResult v3 = _run_format(InvertedIndexStorageFormatPB::V3, io_mode, policy);
        PhaseResult snii = _run_format(InvertedIndexStorageFormatPB::SNII, io_mode, policy);
        return {std::move(v3), std::move(snii)};
    }

    PhaseResult _run_format(InvertedIndexStorageFormatPB storage_format, IoMode io_mode,
                            BenchCachePolicy policy) {
        PhaseResult result;
        result.ran = true;
        const int query_iterations = _query_iterations();
        const bool write_back =
                io_mode == IoMode::kRemoteS3 && policy == BenchCachePolicy::kWriteBack;
        const bool local_warm = io_mode == IoMode::kLocal;
        // Held for the whole run so load, compaction and query all see the same policy. With the
        // cache off, S3FileSystem hands out a plain reader instead of a CachedRemoteFileReader, so
        // every read is a real GET.
        const bool saved_enable_file_cache = config::enable_file_cache;
        config::enable_file_cache = write_back;
        Defer restore_cache_cfg {[&] { config::enable_file_cache = saved_enable_file_cache; }};
        _build_tablet(storage_format);

        // Both formats must start from the same cache state. _setup_remote() runs once per test and
        // _run_format() is then called for V3 and SNII against the same BlockFileCache, so without
        // this the second format would load into an LRU the first one left full -- exactly the
        // regime SNII_BENCH_CACHE_CAPACITY_MB exists to probe, where residency decides the result.
        int64_t cache_bytes_before_load = 0;
        if (write_back) {
            _empty_file_cache();
            cache_bytes_before_load = _cache_dir_bytes();
        }

        std::cout << "  rows per segment: " << _rows_per_segment() << std::endl;
        // --- Phase 1: load + build the index ---
        std::optional<StorageResource> storage_resource;
        if (io_mode == IoMode::kRemoteS3) {
            storage_resource = StorageResource(_remote_fs);
        }
        std::vector<RowsetSharedPtr> rowsets(_corpus_files.size());
        const bool reused =
                _reuse_rowsets() && io_mode == IoMode::kRemoteS3 &&
                _load_rowset_manifest(storage_format, write_back, storage_resource, &rowsets);
        result.import_reused = reused;
        if (reused) {
            std::cout << "  import skipped: reused " << rowsets.size()
                      << " rowsets from manifest (import timing not measured)" << std::endl;
        } else {
            result.import = measure([&] {
                IndexCompactionUtils::build_rowsets<IndexCompactionUtils::WikiDataRow>(
                        _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, _corpus_files,
                        _inc_id, nullptr, /*is_performance=*/true, _rows_per_segment(),
                        storage_resource,
                        /*write_file_cache=*/write_back);
            });
            if (_reuse_rowsets() && io_mode == IoMode::kRemoteS3) {
                _save_rowset_manifest(storage_format, write_back, rowsets);
            }
        }

        // --- Phase 2: index compaction ---
        // Cloud load leaves the file cache warm for the data it just wrote (write_file_cache above),
        // so compaction there reads local SSD rather than S3. The S3 upload path fills the cache
        // asynchronously, so drain it before timing compaction, otherwise the first compaction read
        // races the upload and still goes to S3.
        // Reuse skips the load, so there is nothing to populate the cache with -- this checks a
        // property of an import that did not run. The cold query clears the cache before every
        // iteration regardless, so its measurement is unaffected (proven: every deterministic
        // counter is identical between an importing run and a reusing one).
        if (write_back && !reused) {
            const int64_t cached = _cache_dir_bytes();
            std::cout << "  load populated file cache with " << cached - cache_bytes_before_load
                      << " B (dir now " << cached << " B)" << std::endl;
            // An empty cache dir still weighs kColdCacheResidueBytes of RocksDB metadata, so
            // "> 0" would pass even if nothing was cached. If the load did not fill the cache,
            // compaction below reads S3 and the timing is not comparable to cloud.
            EXPECT_GT(cached - cache_bytes_before_load, kColdCacheResidueBytes)
                    << "write_file_cache did not populate " << _file_cache_dir;
        } else if (write_back) {
            std::cout << "  cache-population check skipped (import was reused)" << std::endl;
        }

        // Compaction only exists here to produce the rowset the query phase reads. When the query
        // reads the input rowsets instead, the merged output is provably never touched, so running
        // it would burn an 80-way merge per format per test and report a phase nothing depends on.
        // Compaction gets benchmarked by the default (compacted-output) mode, where it is on the
        // measured path.
        RowsetSharedPtr output_rowset;
        result.compaction_ran = !_query_input_rowsets();
        if (result.compaction_ran) {
            Status compaction_status;
            result.compaction = measure([&] {
                compaction_status = IndexCompactionUtils::do_compaction(
                        rowsets, _engine_ref, _tablet, /*is_index_compaction=*/true, output_rowset,
                        [&result](const BaseCompaction&, const RowsetWriterContext& cctx) {
                            result.index_compaction_columns = static_cast<int64_t>(
                                    cctx.columns_to_do_index_compaction.size());
                        },
                        10000000, storage_resource);
            });
            EXPECT_TRUE(compaction_status.ok()) << compaction_status.to_string();
        } else {
            std::cout << "  compaction skipped (query reads the input rowsets, so the merged "
                         "output would never be read)"
                      << std::endl;
        }

        // do_compaction() only builds the output rowset; it never calls modify_rowsets, so the
        // inputs stay readable and either target can be queried below.
        const std::vector<RowsetSharedPtr> query_rowsets =
                _query_input_rowsets() ? rowsets : std::vector<RowsetSharedPtr> {output_rowset};
        std::cout << "  query target: " << query_rowsets.size() << " rowset(s) ("
                  << (_query_input_rowsets() ? "input, pre-compaction" : "compacted output") << ")"
                  << std::endl;

        // Only meaningful for a local rowset: for a remote one segment_path() returns an S3 key,
        // so the directory walk in _index_bytes() would find nothing and report 0 -- which the
        // report would print as if the index were empty. Leave it unset (-1 = not measured).
        if (io_mode == IoMode::kLocal) {
            result.index_bytes = 0;
            for (const auto& rs : query_rowsets) {
                result.index_bytes += _index_bytes(rs);
            }
        } else {
            result.index_bytes = -1;
        }

        // --- Phase 3: cold match_phrase_prefix ---
        std::vector<double> cpu_samples;
        std::vector<double> wall_samples;
        result.profile_iterations = query_iterations;
        result.cold_profile.assign(kQueryCases.size(), CaseProfile {});
        cpu_samples.reserve(query_iterations);
        wall_samples.reserve(query_iterations);
        const std::string tablet_dir = _tablet->tablet_path();
        // Same reason: an S3 key is not a path mincore() can open, and "0/0 pages resident"
        // would be indistinguishable from a successful eviction.
        std::string probe_file;
        if (io_mode == IoMode::kLocal) {
            const auto& sp = query_rowsets.front()->segment_path(0);
            DORIS_CHECK(sp.has_value());
            probe_file = std::filesystem::path(sp.value()).replace_extension(".idx").string();
        }
        for (int i = 0; i < query_iterations; ++i) {
            _drop_caches(tablet_dir);
            if (_remote_fs != nullptr) {
                // Evicting the cache directory's OS pages is not enough: BlockFileCache still
                // believes it holds the blocks and serves them from local disk, so the query never
                // goes back to S3 and the read counters stay at zero. Invalidate the cache itself
                // so a cold query is a genuine remote fetch. The rowsets' own segments pin blocks,
                // so release them first or nothing becomes evictable -- every rowset about to be
                // queried, not just one, or the rest stay pinned and warm.
                for (const auto& rs : query_rowsets) {
                    rs->clear_cache();
                }
                const int64_t before = _cache_dir_bytes();
                const int64_t after = _empty_file_cache();
                if (i == 0) {
                    std::cout << "  cache before/after clear: " << before << " -> " << after << " B"
                              << std::endl;
                }
                // A "cold" query served from a cache that never emptied is not a measurement.
                EXPECT_LE(after, kColdCacheResidueBytes)
                        << "file cache still holds " << after << " B; cold query is not cold";
            }
            if (!_file_cache_dir.empty()) {
                // The block file cache is the layer a real deployment actually hits; leaving it
                // warm would make every "cold" query a cache hit and measure nothing remote.
                _drop_caches(_file_cache_dir);
            }
            if (i == 0 && !probe_file.empty()) {
                const auto [resident, pages] = _resident_pages(probe_file);
                std::cout << "  cold check: " << resident << "/" << pages
                          << " index pages resident after evict" << std::endl;
            }
            int64_t matched = 0;
            io::FileCacheStatistics io_stats;
            const Measurement m = measure([&] {
                matched = _run_query_cases(query_rowsets, &result.per_case, &io_stats,
                                           &result.cold_profile);
            });
            // These are deterministic per iteration; the report pairs them with a median
            // timing, so assert they really are identical rather than quietly publishing
            // iteration N's IO next to iteration N/2's time.
            if (i == 0) {
                result.remote_physical_read_bytes =
                        io_stats.inverted_index_remote_physical_read_bytes;
                result.request_bytes = io_stats.inverted_index_request_bytes;
                result.read_bytes = io_stats.inverted_index_read_bytes;
                result.num_remote_io = io_stats.inverted_index_num_remote_io_total;
                result.num_local_io = io_stats.inverted_index_num_local_io_total;
                result.range_read_count = io_stats.inverted_index_range_read_count;
                result.serial_read_rounds = io_stats.inverted_index_serial_read_rounds;
            } else {
                EXPECT_EQ(result.range_read_count, io_stats.inverted_index_range_read_count)
                        << "range_reads moved between iterations; the IO counters are not "
                           "comparable to a median timing";
            }
            cpu_samples.push_back(m.cpu_s);
            wall_samples.push_back(m.wall_s);
            result.matched_docs = matched;
        }
        std::sort(cpu_samples.begin(), cpu_samples.end());
        std::sort(wall_samples.begin(), wall_samples.end());
        result.cold_cpu_samples = std::move(cpu_samples);
        result.cold_wall_samples = std::move(wall_samples);
        result.cold_query.cpu_s = nearest_rank_percentile(result.cold_cpu_samples, 50);
        result.cold_query.wall_s = nearest_rank_percentile(result.cold_wall_samples, 50);
        result.cold_query_p99.cpu_s = nearest_rank_percentile(result.cold_cpu_samples, 99);
        result.cold_query_p99.wall_s = nearest_rank_percentile(result.cold_wall_samples, 99);
        // A cold query is dominated by IO, so wall spread is the honest signal-to-noise indicator.
        result.cold_wall_min = result.cold_wall_samples.front();
        result.cold_wall_max = result.cold_wall_samples.back();

        // --- Phase 3b: hot query ---
        // The cold pass above ended by pulling everything it touched into the active cache layer.
        // Repeating the same queries without dropping anything measures the steady state: the
        // block cache for remote write-back, or the OS page cache for a local run.
        if (write_back || local_warm) {
            std::vector<double> hot_cpu;
            std::vector<double> hot_wall;
            result.hot_profile.assign(kQueryCases.size(), CaseProfile {});
            hot_cpu.reserve(query_iterations);
            hot_wall.reserve(query_iterations);
            for (int i = 0; i < query_iterations; ++i) {
                io::FileCacheStatistics hot_stats;
                std::vector<int64_t> hot_hits;
                const Measurement m = measure([&] {
                    static_cast<void>(_run_query_cases(query_rowsets, &hot_hits, &hot_stats,
                                                       &result.hot_profile));
                });
                result.hot_remote_read_bytes = hot_stats.inverted_index_remote_physical_read_bytes;
                result.hot_range_read_count = hot_stats.inverted_index_range_read_count;
                hot_cpu.push_back(m.cpu_s);
                hot_wall.push_back(m.wall_s);
            }
            std::sort(hot_cpu.begin(), hot_cpu.end());
            std::sort(hot_wall.begin(), hot_wall.end());
            result.hot_cpu_samples = std::move(hot_cpu);
            result.hot_wall_samples = std::move(hot_wall);
            result.hot_query.cpu_s = nearest_rank_percentile(result.hot_cpu_samples, 50);
            result.hot_query.wall_s = nearest_rank_percentile(result.hot_wall_samples, 50);
            result.hot_query_p99.cpu_s = nearest_rank_percentile(result.hot_cpu_samples, 99);
            result.hot_query_p99.wall_s = nearest_rank_percentile(result.hot_wall_samples, 99);
            result.has_hot = true;
        }
        return result;
    }

    // Runs the 12 query cases against every segment of every rowset. One rowset for the compacted
    // target, all of them for SNII_BENCH_QUERY_INPUT_ROWSETS.
    int64_t _run_query_cases(const std::vector<RowsetSharedPtr>& rowsets,
                             std::vector<int64_t>* per_case = nullptr,
                             io::FileCacheStatistics* io_out = nullptr,
                             std::vector<CaseProfile>* profile_out = nullptr) {
        // Reachable only when an earlier phase failed. Returning 0 quietly would let the run print
        // a full report in which every query "matched" nothing.
        if (rowsets.empty()) {
            ADD_FAILURE() << "no rowsets to query; an earlier phase must have failed";
            return 0;
        }
        int64_t matched = 0;
        io::FileCacheStatistics collected_io;
        if (per_case != nullptr) {
            per_case->assign(kQueryCases.size(), 0);
        }
        // Deliberately not reset here: the caller sizes it once and every iteration adds into it,
        // so the report can divide by the iteration count. A single iteration is too noisy to
        // compare two formats on a sub-millisecond query.
        if (profile_out != nullptr && profile_out->size() != kQueryCases.size()) {
            profile_out->assign(kQueryCases.size(), CaseProfile {});
        }
        // enable_inverted_index_query_cache defaults to true (PaloInternalService.thrift:370), and
        // InvertedIndexReader::handle_query_cache then answers a repeated query straight from the
        // cached bitmap without touching the searcher or any file reader. Every phase here reruns
        // the same 12 queries against the same segments, so leaving it on would measure a bitmap
        // lookup instead of the index read path this benchmark exists to compare -- the hot phase
        // in particular would never reach the block cache it claims to be measuring.
        TQueryOptions query_options;
        query_options.enable_inverted_index_query_cache = false;
        RuntimeState runtime_state(query_options, TQueryGlobals());
        for (const auto& rowset : rowsets) {
            if (rowset == nullptr) {
                ADD_FAILURE() << "null rowset; an earlier phase must have failed";
                return 0;
            }
            matched +=
                    _query_one_rowset(rowset, runtime_state, per_case, &collected_io, profile_out);
        }
        if (io_out != nullptr) {
            *io_out = collected_io;
        }
        return matched;
    }

    int64_t _query_one_rowset(const RowsetSharedPtr& rowset, RuntimeState& runtime_state,
                              std::vector<int64_t>* per_case, io::FileCacheStatistics* collected_io,
                              std::vector<CaseProfile>* profile_out) {
        SegmentCacheHandle segment_cache;
        const Status load_st = SegmentLoader::instance()->load_segments(
                std::static_pointer_cast<BetaRowset>(rowset), &segment_cache);
        if (!load_st.ok()) {
            ADD_FAILURE() << "load_segments failed: " << load_st.to_string();
            return 0;
        }
        int64_t matched = 0;
        for (const auto& segment : segment_cache.get_segments()) {
            const auto& indexes = _tablet_schema->inverted_indexes();
            if (indexes.empty()) {
                continue;
            }
            OlapReaderStatistics stats;
            StorageReadOptions read_options;
            read_options.stats = &stats;
            auto query_context = std::make_shared<IndexQueryContext>();
            query_context->stats = &stats;
            // Without this the inverted-index IO counters stay at zero and the remote comparison
            // has nothing to compare.
            read_options.io_ctx.file_cache_stats = &stats.file_cache_stats;
            query_context->io_ctx = &read_options.io_ctx;
            // FullTextIndexReader reads query_options() off it unconditionally (max_expansions for
            // phrase prefix), so a default-constructed state is required, not optional.
            query_context->runtime_state = &runtime_state;
            // One iterator per indexed column. Matched by col_unique_ids rather than by position
            // in inverted_indexes(), so adding or reordering an index cannot silently point a case
            // at the wrong column -- which would return an empty bitmap indistinguishable from
            // "no matches".
            constexpr size_t kIndexedColumns = 2; // title, content
            std::array<std::unique_ptr<IndexIterator>, kIndexedColumns> iters;
            std::array<std::string, kIndexedColumns> field_names;
            bool all_iters_ready = true;
            for (size_t ci = 0; ci < kIndexedColumns; ++ci) {
                const auto& column = _tablet_schema->column(static_cast<int32_t>(ci));
                const TabletIndex* index = nullptr;
                for (const auto* candidate : indexes) {
                    const auto& ids = candidate->col_unique_ids();
                    if (!ids.empty() && ids[0] == column.unique_id()) {
                        index = candidate;
                        break;
                    }
                }
                if (index == nullptr) {
                    ADD_FAILURE() << "no inverted index on column " << column.name();
                    all_iters_ready = false;
                    break;
                }
                const Status iter_st =
                        segment->new_index_iterator(column, index, read_options, &iters[ci]);
                if (!iter_st.ok() || iters[ci] == nullptr) {
                    // Every segment must have both indexes; skipping one would compare a format
                    // that read N segments against one that read N-1.
                    ADD_FAILURE() << "new_index_iterator(" << column.name()
                                  << ") failed: " << iter_st.to_string();
                    all_iters_ready = false;
                    break;
                }
                // read_from_index() dereferences the context unconditionally; SegmentIterator
                // normally supplies it, so a hand-built iterator has to as well.
                iters[ci]->set_context(query_context);
                // The index stores the field under the column's unique id, not its name
                // (index_writer.cpp: field_name = std::to_string(column->unique_id())). Querying by
                // "content" silently matched no field and returned OK with an empty bitmap.
                field_names[ci] = std::to_string(column.unique_id());
            }
            if (!all_iters_ready) {
                continue;
            }
            // Only now: Segment::new_index_iterator is what lazily opens _index_file_reader
            // (segment.cpp, _index_file_reader_open.call), so before the loop above it is still
            // null and this dereference segfaults. Initialising it is still required -- without it
            // every query returns OK with an empty bitmap, indistinguishable from "no matches".
            // SegmentIterator does this as part of its own setup.
            const Status reader_init = segment->_index_file_reader->init(
                    config::inverted_index_read_buffer_size, &read_options.io_ctx);
            if (!reader_init.ok()) {
                ADD_FAILURE() << "index file reader init failed: " << reader_init.to_string();
                continue;
            }

            for (size_t pi = 0; pi < kQueryCases.size(); ++pi) {
                const auto& qc = kQueryCases[pi];
                if (!_case_enabled(qc)) {
                    continue;
                }
                const auto ci = static_cast<size_t>(qc.column);
                InvertedIndexParam param;
                param.column_name = field_names[ci];
                // Required: the reader DCHECKs on it. The indexed column is STRING.
                param.column_type = std::make_shared<DataTypeString>();
                param.query_value = Field::create_field<TYPE_STRING>(std::string(qc.text));
                param.query_type = qc.type;
                param.num_rows = segment->num_rows();
                param.roaring = std::make_shared<roaring::Roaring>();
                // The counters are monotonic and shared by every case on this segment, so the only
                // way to attribute them to one case is to bracket that case's own call.
                const OlapReaderStatistics before = stats;
                const Status qs = iters[ci]->read_from_index(&param);
                if (profile_out != nullptr) {
                    (*profile_out)[pi].add_delta(before, stats);
                }
                if (!qs.ok()) {
                    // Never swallow this: a format that errors out looks identical to a format that
                    // is simply fast, and the timings would be comparing work against no work.
                    ADD_FAILURE() << qc.label << " ('" << qc.text
                                  << "') failed: " << qs.to_string();
                    continue;
                }
                {
                    const auto hits = static_cast<int64_t>(param.roaring->cardinality());
                    matched += hits;
                    if (per_case != nullptr) {
                        (*per_case)[pi] += hits;
                    }
                    if (profile_out != nullptr) {
                        (*profile_out)[pi].hits += hits;
                    }
                }
            }
            collected_io->merge_from(stats.file_cache_stats);
        }
        return matched;
    }

    static const char* _column_label(QueryColumn column) {
        return column == QueryColumn::kTitle ? "title" : "content";
    }

    // Prints the one format that ran. Ratios need both, so in single-format mode the paired
    // report would divide by a default-constructed zero; print absolute numbers instead.
    static void _report_single(const PhaseResult& r, const char* which) {
        auto ms = [](int64_t ns) { return static_cast<double>(ns) / 1e6; };
        std::cout << "\n=== " << which
                  << " only (no comparison: SNII_BENCH_ONLY_FORMAT) ===" << std::endl;
        auto line = [&](const char* phase, const Measurement& m) {
            std::cout << std::left << std::setw(20) << phase << std::right << std::fixed
                      << std::setprecision(6) << std::setw(12) << m.cpu_s << " cpu" << std::setw(12)
                      << m.wall_s << " wall" << std::endl;
        };
        if (r.import_reused) {
            std::cout << std::left << std::setw(20) << "import"
                      << "  (skipped -- rowsets reused)" << std::endl;
        } else {
            line("import", r.import);
        }
        if (r.compaction_ran) {
            line("compaction", r.compaction);
        }
        line("cold_query_p50", r.cold_query);
        line("cold_query_p99", r.cold_query_p99);
        if (r.has_hot) {
            line("hot_query_p50", r.hot_query);
            line("hot_query_p99", r.hot_query_p99);
        }
        auto io = [&](const char* name, int64_t v) {
            std::cout << std::left << std::setw(20) << name << std::right << std::setw(14) << v
                      << std::endl;
        };
        io("matched_docs", r.matched_docs);
        io("request_B(logic)", r.request_bytes);
        io("remote_read_B", r.remote_physical_read_bytes);
        io("range_reads", r.range_read_count);
        io("remote_GETs", r.num_remote_io);
        io("local_hits", r.num_local_io);
        // The acceptance metrics for the resident-read fix are per-segment, so print them that
        // way -- 2 GETs/segment is the defect, ~1 is V3 parity.
        std::cout << "\nper-case (one pass, mean of iterations; times ms):" << std::endl;
        std::cout << std::left << std::setw(14) << "case" << std::setw(9) << "col" << std::right
                  << std::setw(10) << "qry" << std::setw(12) << "req_B" << std::setw(12) << "phys_B"
                  << std::setw(10) << "rem_io" << std::setw(10) << "loc_io" << std::endl;
        const double n = r.profile_iterations > 0 ? r.profile_iterations : 1;
        for (size_t i = 0; i < kQueryCases.size() && i < r.cold_profile.size(); ++i) {
            const CaseProfile& p = r.cold_profile[i];
            if (p.hits == 0 && p.index_query_ns == 0) {
                continue;
            }
            std::cout << std::left << std::setw(14) << kQueryCases[i].label << std::setw(9)
                      << _column_label(kQueryCases[i].column) << std::right << std::fixed
                      << std::setprecision(2) << std::setw(10) << ms(p.index_query_ns) / n
                      << std::setprecision(0) << std::setw(12) << p.request_bytes / n
                      << std::setw(12) << p.physical_bytes / n << std::setw(10) << p.remote_io / n
                      << std::setw(10) << p.local_io / n << std::endl;
        }
    }

    static void _report(const PhaseResult& v3, const PhaseResult& snii) {
        if (!v3.ran || !snii.ran) {
            _report_single(v3.ran ? v3 : snii, v3.ran ? "V3" : "SNII");
            return;
        }
        auto ratio = [](double snii_v, double v3_v) { return v3_v > 0 ? snii_v / v3_v : 0.0; };
        auto line = [&](const char* phase, const Measurement& a, const Measurement& b) {
            std::cout << std::left << std::setw(18) << phase << std::right << std::fixed
                      << std::setprecision(6) << std::setw(12) << a.cpu_s << std::setw(12)
                      << b.cpu_s << std::setw(9) << ratio(b.cpu_s, a.cpu_s) << "x"
                      << "      " << std::setw(12) << a.wall_s << std::setw(12) << b.wall_s
                      << std::endl;
        };

        std::cout << "\n=== SNII vs V3 (CPU seconds; ratio <1 means SNII is cheaper) ===\n"
                  << std::left << std::setw(18) << "phase" << std::right << std::setw(12)
                  << "V3 cpu" << std::setw(12) << "SNII cpu" << std::setw(10) << "ratio"
                  << "      " << std::setw(12) << "V3 wall" << std::setw(12) << "SNII wall"
                  << std::endl;
        if (v3.import_reused || snii.import_reused) {
            std::cout << std::left << std::setw(18) << "import"
                      << "  (skipped -- rowsets reused from manifest)" << std::endl;
        } else {
            line("import", v3.import, snii.import);
        }
        const bool compaction_ran = v3.compaction_ran || snii.compaction_ran;
        if (compaction_ran) {
            line("compaction", v3.compaction, snii.compaction);
        } else {
            std::cout << std::left << std::setw(18) << "compaction"
                      << "  (skipped -- query reads the input rowsets; use the default "
                         "compacted-output mode to benchmark compaction)"
                      << std::endl;
        }
        line("cold_query_p50", v3.cold_query, snii.cold_query);
        line("cold_query_p99", v3.cold_query_p99, snii.cold_query_p99);
        // Only the cases that actually ran; under SNII_BENCH_ONLY_CASE the rest are skipped and
        // dividing by the full list would understate per-query cost.
        size_t executed = 0;
        for (const auto& qc : kQueryCases) {
            executed += _case_enabled(qc) ? 1 : 0;
        }
        const double n = static_cast<double>(executed);
        auto per_query = [&](const char* label, const Measurement& a, const Measurement& b) {
            std::cout << std::left << std::setw(18) << label << std::right << std::fixed
                      << std::setprecision(4) << std::setw(10) << a.wall_s / n << std::setw(10)
                      << b.wall_s / n << std::setw(9) << (a.wall_s > 0 ? b.wall_s / a.wall_s : 0.0)
                      << "x   (" << executed << " queries)" << std::endl;
        };
        per_query("  per-query wall", v3.cold_query, snii.cold_query);
        std::cout << "  cold wall spread  V3 [" << std::fixed << std::setprecision(6)
                  << v3.cold_wall_min << ", " << v3.cold_wall_max << "]  SNII ["
                  << snii.cold_wall_min << ", " << snii.cold_wall_max << "]" << std::endl;
        if (v3.has_hot && snii.has_hot) {
            line("hot_query_p50", v3.hot_query, snii.hot_query);
            line("hot_query_p99", v3.hot_query_p99, snii.hot_query_p99);
            per_query("  per-query wall", v3.hot_query, snii.hot_query);
        }
        std::cout << "\nper-query hits (V3 / SNII):" << std::endl;
        for (size_t i = 0; i < kQueryCases.size(); ++i) {
            const int64_t a = i < v3.per_case.size() ? v3.per_case[i] : -1;
            const int64_t b = i < snii.per_case.size() ? snii.per_case[i] : -1;
            std::cout << "  " << std::left << std::setw(14) << kQueryCases[i].label << std::setw(9)
                      << _column_label(kQueryCases[i].column) << std::right << std::setw(8) << a
                      << std::setw(8) << b << (a == b ? "" : "   <-- differs") << std::endl;
        }
        auto io_line = [&](const char* name, int64_t a, int64_t b) {
            std::cout << std::left << std::setw(18) << name << std::right << std::setw(10) << a
                      << std::setw(10) << b << std::setw(9);
            // A zero denominator means "nothing was measured"; printing 0.00x would read as
            // "SNII is infinitely cheaper".
            if (a > 0) {
                std::cout << static_cast<double>(b) / static_cast<double>(a) << "x";
            } else {
                std::cout << "n/a";
            }
            std::cout << std::endl;
        };
        if (compaction_ran) {
            io_line("idx_compact_cols", v3.index_compaction_columns, snii.index_compaction_columns);
        }
        io_line("request_B(logic)", v3.request_bytes, snii.request_bytes);
        io_line("read_B", v3.read_bytes, snii.read_bytes);
        io_line("remote_read_B", v3.remote_physical_read_bytes, snii.remote_physical_read_bytes);
        // How many physical bytes each logical byte cost. On the cached path physical comes from
        // CachedRemoteFileReader for both formats, so this is the comparable read-amplification
        // number; with the cache off both degenerate to 1.00 by construction.
        auto amp = [](int64_t phys, int64_t req) {
            return req > 0 ? static_cast<double>(phys) / static_cast<double>(req) : 0.0;
        };
        std::cout << std::left << std::setw(18) << "amplification" << std::right << std::fixed
                  << std::setprecision(2) << std::setw(10)
                  << amp(v3.remote_physical_read_bytes, v3.request_bytes) << std::setw(10)
                  << amp(snii.remote_physical_read_bytes, snii.request_bytes)
                  << "   (remote_read_B / request_B)" << std::endl;
        io_line("range_reads", v3.range_read_count, snii.range_read_count);
        io_line("remote_GETs", v3.num_remote_io, snii.num_remote_io);
        io_line("local_hits", v3.num_local_io, snii.num_local_io);
        io_line("serial_rounds", v3.serial_read_rounds, snii.serial_read_rounds);
        if (v3.has_hot && snii.has_hot) {
            // Should be ~0 remote bytes: if the hot pass still fetches from S3 the cold pass failed
            // to write back and the hot number is not a cache-hit measurement.
            io_line("hot_remote_read_B", v3.hot_remote_read_bytes, snii.hot_remote_read_bytes);
            io_line("hot_range_reads", v3.hot_range_read_count, snii.hot_range_read_count);
        }
        if (v3.index_bytes < 0 || snii.index_bytes < 0) {
            std::cout << std::left << std::setw(18) << "index_bytes" << std::right << std::setw(10)
                      << "n/a"
                      << "   (remote rowsets expose S3 keys, not local paths)" << std::endl;
        } else {
            std::cout << std::left << std::setw(18) << "index_bytes" << std::right << std::setw(10)
                      << v3.index_bytes << std::setw(10) << snii.index_bytes << std::setw(9)
                      << ratio(static_cast<double>(snii.index_bytes),
                               static_cast<double>(v3.index_bytes))
                      << "x" << std::endl;
        }

        // A run where wall clock ran far ahead of CPU was competing for the machine; the ratios
        // above are still CPU-based and usable, but say so rather than let it pass silently.
        auto flag = [](const char* phase, const Measurement& m) {
            if (m.cpu_s > 0.05 && m.wall_s > m.cpu_s * 2.0) {
                std::cout << "  [warn] " << phase << " wall " << m.wall_s << "s vs cpu " << m.cpu_s
                          << "s -- machine was busy or the phase was IO bound" << std::endl;
            }
        };
        flag("V3 import", v3.import);
        flag("SNII import", snii.import);
        if (compaction_ran) {
            flag("V3 compaction", v3.compaction);
            flag("SNII compaction", snii.compaction);
        }

        // A filtered run reports only that case; say so, or the table reads as if the other
        // cases had cost nothing.
        if (const char* only = _only_case(); only != nullptr && *only != '\0') {
            std::cout << "\n[filtered] only query case '" << only
                      << "' ran; other cases did not execute and did not prewarm any cache"
                      << std::endl;
        }
        _report_profile("cold", v3.cold_profile, snii.cold_profile, v3.profile_iterations,
                        snii.profile_iterations);
        if (v3.has_hot || snii.has_hot) {
            _report_profile("hot", v3.hot_profile, snii.hot_profile, v3.profile_iterations,
                            snii.profile_iterations);
        }
    }

    // The per-case profile: what a scan node's RuntimeProfile would show, per query shape. The
    // aggregate IO totals say SNII reads less; this says which query shape that came from and
    // where the time inside it went.
    static void _report_profile(const char* phase, const std::vector<CaseProfile>& v3,
                                const std::vector<CaseProfile>& snii, int v3_iters,
                                int snii_iters) {
        if (v3.size() != kQueryCases.size() || snii.size() != kQueryCases.size()) {
            return;
        }
        const double v3_n = v3_iters > 0 ? static_cast<double>(v3_iters) : 1.0;
        const double snii_n = snii_iters > 0 ? static_cast<double>(snii_iters) : 1.0;
        auto ms = [](int64_t ns, double n) { return static_cast<double>(ns) / 1e6 / n; };
        auto per = [](int64_t v, double n) { return static_cast<double>(v) / n; };
        std::cout << "\n=== " << phase << " query profile (mean of one pass over every rowset, "
                  << v3_iters << " iterations; times ms) ===" << std::endl;
        std::cout << std::left << std::setw(14) << "case" << std::setw(9) << "col" << std::right
                  << std::setw(10) << "V3 qry" << std::setw(10) << "SNII qry" << std::setw(11)
                  << "V3 open" << std::setw(11) << "SNII open" << std::setw(11) << "V3 srch"
                  << std::setw(11) << "SNII srch" << std::setw(11) << "V3 ioms" << std::setw(11)
                  << "SNII ioms" << std::setw(10) << "V3 rng" << std::setw(10) << "SNII rng"
                  << std::endl;
        for (size_t i = 0; i < kQueryCases.size(); ++i) {
            const CaseProfile& a = v3[i];
            const CaseProfile& b = snii[i];
            std::cout << std::left << std::setw(14) << kQueryCases[i].label << std::setw(9)
                      << _column_label(kQueryCases[i].column) << std::right << std::fixed
                      << std::setprecision(2) << std::setw(10) << ms(a.index_query_ns, v3_n)
                      << std::setw(10) << ms(b.index_query_ns, snii_n) << std::setw(11)
                      << ms(a.searcher_open_ns, v3_n) << std::setw(11)
                      << ms(b.searcher_open_ns, snii_n) << std::setw(11)
                      << ms(a.searcher_search_ns, v3_n) << std::setw(11)
                      << ms(b.searcher_search_ns, snii_n) << std::setw(11)
                      << ms(a.remote_io_ns + a.local_io_ns, v3_n) << std::setw(11)
                      << ms(b.remote_io_ns + b.local_io_ns, snii_n) << std::setprecision(0)
                      << std::setw(10) << per(a.range_reads, v3_n) << std::setw(10)
                      << per(b.range_reads, snii_n) << std::endl;
        }

        std::cout << "\n" << phase << " index IO by case (bytes / io counts):" << std::endl;
        std::cout << std::left << std::setw(14) << "case" << std::setw(9) << "col" << std::right
                  << std::setw(13) << "V3 req_B" << std::setw(13) << "SNII req_B" << std::setw(13)
                  << "V3 phys_B" << std::setw(13) << "SNII phys_B" << std::setw(11) << "V3 rem_io"
                  << std::setw(11) << "SNII r_io" << std::setw(11) << "V3 loc_io" << std::setw(11)
                  << "SNII l_io" << std::endl;
        for (size_t i = 0; i < kQueryCases.size(); ++i) {
            std::cout << std::left << std::setw(14) << kQueryCases[i].label << std::setw(9)
                      << _column_label(kQueryCases[i].column) << std::right << std::fixed
                      << std::setprecision(0) << std::setw(13) << per(v3[i].request_bytes, v3_n)
                      << std::setw(13) << per(snii[i].request_bytes, snii_n) << std::setw(13)
                      << per(v3[i].physical_bytes, v3_n) << std::setw(13)
                      << per(snii[i].physical_bytes, snii_n) << std::setw(11)
                      << per(v3[i].remote_io, v3_n) << std::setw(11)
                      << per(snii[i].remote_io, snii_n) << std::setw(11)
                      << per(v3[i].local_io, v3_n) << std::setw(11) << per(snii[i].local_io, snii_n)
                      << std::endl;
        }

        // Only SNII populates these. Printing them for V3 would be a column of zeros pretending to
        // be a comparison, so this block is SNII-only and says so.
        int64_t any = 0;
        for (const auto& p : snii) {
            any += p.snii.prx_fetch_ns + p.snii.prx_decode_ns + p.snii.prx_total_docs +
                   p.snii.phrase_candidate_docs + p.snii.common_grams_candidate_queries;
        }
        if (any == 0) {
            std::cout << "\n(SNII " << phase
                      << " internals: all zero -- the prx/phrase/CommonGrams paths were not "
                         "instrumented on this build or not taken)"
                      << std::endl;
            return;
        }
        std::cout << "\nSNII " << phase << " internals (V3 has no equivalent):" << std::endl;
        std::cout << std::left << std::setw(14) << "case" << std::setw(9) << "col" << std::right
                  << std::setw(10) << "fetchms" << std::setw(10) << "decodms" << std::setw(10)
                  << "verifms" << std::setw(12) << "docs_tot" << std::setw(12) << "docs_sel"
                  << std::setw(12) << "pos_tot" << std::setw(12) << "pos_sel" << std::setw(10)
                  << "cand_dc" << std::setw(10) << "cg_gram" << std::endl;
        for (size_t i = 0; i < kQueryCases.size(); ++i) {
            const snii::SniiQueryStats& s = snii[i].snii;
            std::cout << std::left << std::setw(14) << kQueryCases[i].label << std::setw(9)
                      << _column_label(kQueryCases[i].column) << std::right << std::fixed
                      << std::setprecision(2) << std::setw(10) << ms(s.prx_fetch_ns, snii_n)
                      << std::setw(10) << ms(s.prx_decode_ns, snii_n) << std::setw(10)
                      << ms(s.prx_phrase_verify_ns, snii_n) << std::setprecision(0) << std::setw(12)
                      << per(s.prx_total_docs, snii_n) << std::setw(12)
                      << per(s.prx_selected_docs, snii_n) << std::setw(12)
                      << per(s.prx_total_positions, snii_n) << std::setw(12)
                      << per(s.prx_selected_positions, snii_n) << std::setw(10)
                      << per(s.phrase_candidate_docs, snii_n) << std::setw(10)
                      << per(s.common_grams_gram_plans, snii_n) << std::endl;
        }
    }

    TabletSchemaSPtr _tablet_schema;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSharedPtr _tablet;
    std::string _absolute_dir;
    std::string _current_dir;
    std::string _reuse_bucket;
    std::string _reuse_prefix;
    std::unique_ptr<segment_v2::InvertedIndexSearcherCache> _searcher_cache;
    std::unique_ptr<segment_v2::InvertedIndexQueryCache> _query_cache;
    segment_v2::InvertedIndexSearcherCache* _previous_searcher_cache = nullptr;
    segment_v2::InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::shared_ptr<io::S3FileSystem> _remote_fs;
    std::unique_ptr<io::FileCacheFactory> _owned_file_cache_factory;
    io::FileCacheFactory* _origin_file_cache_factory = nullptr;
    bool _origin_file_cache_factory_saved = false;
    int64_t _origin_compaction_batch_size = config::compaction_batch_size;
    bool _origin_index_compaction_enable = config::inverted_index_compaction_enable;
    bool _origin_ram_dir_enable = config::inverted_index_ram_dir_enable;
    std::string _file_cache_dir;
    bool _origin_enable_file_cache = false;
    std::vector<std::string> _corpus_files;
    int64_t _inc_id = 1000;
};

TEST_F(DISABLED_SniiVsV3BenchmarkTest, wikipedia_english_local) {
    ASSERT_FALSE(_corpus_files.empty())
            << "no wikipedia_*.json found; set SNII_BENCH_CORPUS_DIR to a corpus directory";
    std::cout << "corpus: " << _corpus_files.size() << " files, io=local" << std::endl;

    // Local rowsets never touch the block file cache, so the policy is irrelevant here.
    const auto [v3, snii] = _run_both(IoMode::kLocal, BenchCachePolicy::kDirect);
    // Cross-format checks need both formats; SNII_BENCH_ONLY_FORMAT leaves one default-constructed
    // and 0 == 0 would pass vacuously while 0 > 0 would fail spuriously.
    const bool both_ran = v3.ran && snii.ran;
    if (both_ran) {
        EXPECT_EQ(v3.matched_docs, snii.matched_docs) << "V3 and SNII matched a different number "
                                                         "of documents, timings are not comparable";
    }
    // A run where compaction or segment loading failed silently produces zeros everywhere and
    // still satisfies the equality above as 0 == 0, printing a clean but meaningless report.
    EXPECT_GT((v3.ran ? v3 : snii).matched_docs, 0)
            << "no documents matched; the run produced no measurement";
    // Zero means the index was rebuilt from raw data instead of compacted (compaction.cpp:1520
    // rejects ineligible SNII postings and falls back), which is a different cost profile
    // entirely -- comparing a format that compacted against one that rebuilt measures nothing.
    // Only when compaction ran at all: the input-rowsets query mode skips it and leaves -1.
    if (both_ran && (v3.compaction_ran || snii.compaction_ran)) {
        EXPECT_GT(v3.index_compaction_columns, 0) << "V3 did not run index compaction";
        EXPECT_EQ(v3.index_compaction_columns, snii.index_compaction_columns)
                << "formats compacted a different number of index columns";
    }
    _report(v3, snii);
}

// File cache off end to end: load writes straight to S3, compaction re-reads from S3, the query
// reads from S3. Every byte in remote_read_B and every GET in range_reads is physical, which makes
// this the read-amplification comparison.
TEST_F(DISABLED_SniiVsV3BenchmarkTest, wikipedia_english_remote_s3_direct) {
    ASSERT_FALSE(_corpus_files.empty())
            << "no wikipedia_*.json found; set SNII_BENCH_CORPUS_DIR to a corpus directory";
    if (!_setup_remote()) {
        GTEST_SKIP() << "remote S3 not configured; set SNII_BENCH_S3_{AK,SK,ENDPOINT,REGION,BUCKET}"
                     << " and make sure HTTP(S)_PROXY is unset";
    }
    std::cout << "corpus: " << _corpus_files.size() << " files, io=remote-s3, cache=direct"
              << std::endl;

    const auto [v3, snii] = _run_both(IoMode::kRemoteS3, BenchCachePolicy::kDirect);
    // Cross-format checks need both formats; SNII_BENCH_ONLY_FORMAT leaves one default-constructed
    // and 0 == 0 would pass vacuously while 0 > 0 would fail spuriously.
    const bool both_ran = v3.ran && snii.ran;
    if (both_ran) {
        EXPECT_EQ(v3.matched_docs, snii.matched_docs) << "V3 and SNII matched a different number "
                                                         "of documents, timings are not comparable";
    }
    // A run where compaction or segment loading failed silently produces zeros everywhere and
    // still satisfies the equality above as 0 == 0, printing a clean but meaningless report.
    EXPECT_GT((v3.ran ? v3 : snii).matched_docs, 0)
            << "no documents matched; the run produced no measurement";
    // Zero means the index was rebuilt from raw data instead of compacted (compaction.cpp:1520
    // rejects ineligible SNII postings and falls back), which is a different cost profile
    // entirely -- comparing a format that compacted against one that rebuilt measures nothing.
    // Only when compaction ran at all: the input-rowsets query mode skips it and leaves -1.
    if (both_ran && (v3.compaction_ran || snii.compaction_ran)) {
        EXPECT_GT(v3.index_compaction_columns, 0) << "V3 did not run index compaction";
        EXPECT_EQ(v3.index_compaction_columns, snii.index_compaction_columns)
                << "formats compacted a different number of index columns";
    }
    // The whole point of this mode: if nothing was fetched from S3 the cache was not actually off.
    EXPECT_GT((v3.ran ? v3 : snii).remote_physical_read_bytes, 0)
            << "direct mode read nothing from S3";
    _report(v3, snii);
    _teardown_remote();
}

// File cache on and populated by the load, the way cloud does it (write_file_cache comes from the
// load request). Compaction then reads local SSD, and two query numbers come out: cold (cache
// emptied, query refetches and writes back) and hot (same query against what it just cached).
TEST_F(DISABLED_SniiVsV3BenchmarkTest, wikipedia_english_remote_s3_writeback) {
    ASSERT_FALSE(_corpus_files.empty())
            << "no wikipedia_*.json found; set SNII_BENCH_CORPUS_DIR to a corpus directory";
    if (!_setup_remote()) {
        GTEST_SKIP() << "remote S3 not configured; set SNII_BENCH_S3_{AK,SK,ENDPOINT,REGION,BUCKET}"
                     << " and make sure HTTP(S)_PROXY is unset";
    }
    std::cout << "corpus: " << _corpus_files.size() << " files, io=remote-s3, cache=write-back"
              << std::endl;

    const auto [v3, snii] = _run_both(IoMode::kRemoteS3, BenchCachePolicy::kWriteBack);
    // Cross-format checks need both formats; SNII_BENCH_ONLY_FORMAT leaves one default-constructed
    // and 0 == 0 would pass vacuously while 0 > 0 would fail spuriously.
    const bool both_ran = v3.ran && snii.ran;
    if (both_ran) {
        EXPECT_EQ(v3.matched_docs, snii.matched_docs) << "V3 and SNII matched a different number "
                                                         "of documents, timings are not comparable";
    }
    // A run where compaction or segment loading failed silently produces zeros everywhere and
    // still satisfies the equality above as 0 == 0, printing a clean but meaningless report.
    EXPECT_GT((v3.ran ? v3 : snii).matched_docs, 0)
            << "no documents matched; the run produced no measurement";
    // Zero means the index was rebuilt from raw data instead of compacted (compaction.cpp:1520
    // rejects ineligible SNII postings and falls back), which is a different cost profile
    // entirely -- comparing a format that compacted against one that rebuilt measures nothing.
    // Only when compaction ran at all: the input-rowsets query mode skips it and leaves -1.
    if (both_ran && (v3.compaction_ran || snii.compaction_ran)) {
        EXPECT_GT(v3.index_compaction_columns, 0) << "V3 did not run index compaction";
        EXPECT_EQ(v3.index_compaction_columns, snii.index_compaction_columns)
                << "formats compacted a different number of index columns";
    }
    _report(v3, snii);
    _teardown_remote();
}

} // namespace doris
