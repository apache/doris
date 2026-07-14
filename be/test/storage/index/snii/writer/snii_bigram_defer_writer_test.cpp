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

// SNII bigram-defer-to-compaction writer tests (see
// config::snii_bigram_defer_build_to_compaction). Full-stack through the
// PRODUCTION glue -- SniiIndexColumnWriter (init -> set_direct_load ->
// add_values -> finish) into IndexFileWriter::add_snii_index -- then read back
// with the raw SNII segment reader and phrase-queried:
//   1. A deferred (direct-load + config on) segment materializes NO bigram
//      sentinel and NO hidden pair terms, so every phrase takes the
//      positions-verification fallback -- and its phrase / phrase-prefix
//      results are IDENTICAL to a normal segment built from the same rows.
//      Covered for both reader fallback flavors: df-prune meta declared
//      (default config) and legacy no-prune meta (sentinel gate).
//   2. Any non-deferring combination (config off, or not a direct load, or the
//      hint never delivered) leaves the output BYTE-IDENTICAL to the baseline
//      -- the hint alone must not perturb a single byte.
//   3. The defer decision is CAPTURED ONCE in set_direct_load(): a live mBool
//      config flip landing after the hint -- before any row, through finish()
//      -- changes nothing in either direction (on->off keeps deferring,
//      off->on never arms), byte-for-byte against the constant-config twins.
//   4. set_direct_load is a no-op on the IndexColumnWriter base class (what a
//      V3 InvertedIndexColumnWriter inherits) and safely gated by
//      _has_positions on a docs-only SNII writer.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/count_query.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

namespace qinternal = ::doris::snii::query::internal;

constexpr int64_t kIndexId = 1;
constexpr const char* kTestDir = "./ut_dir/snii_bigram_defer_writer_test";

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

// English-parser fulltext index with phrase support: _has_positions == true and
// the hidden bigram build active -- the production shape the deferral targets.
void init_phrase_index_meta(TabletIndex* meta) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("defer_test_idx");
    pb.add_col_unique_id(0);
    pb.mutable_properties()->insert({"parser", "english"});
    pb.mutable_properties()->insert({"lower_case", "true"});
    pb.mutable_properties()->insert({"support_phrase", "true"});
    meta->init_from_pb(pb);
}

// How the segment writer delivers (or does not deliver) the direct-load hint.
enum class DirectLoadHint {
    kNone,      // set_direct_load never called (index build / default paths)
    kDirect,    // set_direct_load(true): stream/broker load
    kNotDirect, // set_direct_load(false): compaction / schema change
};

// Writes one single-index SNII segment file through the production writer
// stack, mirroring the production call order exactly: create()'s init() runs
// BEFORE the segment writer's set_direct_load, which precedes every row.
void write_segment(const std::string& path, const TabletIndex& meta,
                   const std::vector<std::string>& rows, DirectLoadHint hint) {
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(path, &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), path, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/200);

    SniiIndexColumnWriter writer(&index_file_writer, &meta, /*single_field=*/true);
    assert_ok(writer.init());
    if (hint == DirectLoadHint::kDirect) {
        writer.set_direct_load(true);
    } else if (hint == DirectLoadHint::kNotDirect) {
        writer.set_direct_load(false);
    }

    std::vector<Slice> slices;
    slices.reserve(rows.size());
    for (const std::string& row : rows) {
        slices.emplace_back(row);
    }
    assert_ok(writer.add_values("c1", slices.data(), slices.size()));
    assert_ok(writer.finish());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());
}

// Spec-G capture-once pin. Same production call order as write_segment, but
// hot-flips config::snii_bigram_defer_build_to_compaction to `config_after_hint`
// right AFTER set_direct_load(true) captured the defer decision -- the earliest
// instant a live mBool flip can land -- and keeps the flipped value through
// add_values AND finish(). If _add_value_tokens or finish() ever re-read the
// config instead of the captured _phrase_bigrams_deferred, the pair feed and the sentinel
// would follow the flipped value and the segment would diverge from its
// constant-config twin (worst case: pair postings whose vouching sentinel went
// missing, or a sentinel vouching for pairs that were never fed -- the exact
// half-fed-segment hazard the capture exists to prevent).
void write_segment_flipping_config_after_hint(const std::string& path, const TabletIndex& meta,
                                              const std::vector<std::string>& rows,
                                              bool config_after_hint) {
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(path, &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), path, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/200);

    SniiIndexColumnWriter writer(&index_file_writer, &meta, /*single_field=*/true);
    assert_ok(writer.init());
    writer.set_direct_load(true); // the capture happens HERE, under the pre-flip config
    config::snii_bigram_defer_build_to_compaction = config_after_hint;

    std::vector<Slice> slices;
    slices.reserve(rows.size());
    for (const std::string& row : rows) {
        slices.emplace_back(row);
    }
    assert_ok(writer.add_values("c1", slices.data(), slices.size()));
    assert_ok(writer.finish());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());
}

// 80 rows with the adjacent (hello, world) pair: df 80 sits inside the default
// prune survival window [auto floor 64, 2 x floor 128], so any regression that
// re-feeds pairs on a should-be-deferred segment MATERIALIZES the pair -- the
// dict probes below catch it independently of the byte comparison.
std::vector<std::string> eighty_pair_rows() {
    std::vector<std::string> rows;
    rows.reserve(80);
    for (uint32_t i = 0; i < 80; ++i) {
        rows.push_back("hello world item" + std::to_string(i));
    }
    return rows;
}

// Opens the single logical index of a segment file written by write_segment.
// The reader pair must outlive every query against *idx.
void open_index(const std::string& path, ::doris::snii::io::LocalFileReader* file,
                ::doris::snii::reader::SniiSegmentReader* segment,
                ::doris::snii::reader::LogicalIndexReader* idx) {
    assert_ok(file->open(path));
    assert_ok(::doris::snii::reader::SniiSegmentReader::open(file, segment));
    assert_ok(segment->open_index(static_cast<uint64_t>(kIndexId), /*index_suffix=*/"", idx));
}

bool lookup_found(const ::doris::snii::reader::LogicalIndexReader& idx, const std::string& term) {
    bool found = false;
    ::doris::snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    EXPECT_TRUE(idx.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    return found;
}

std::vector<uint32_t> run_phrase(const ::doris::snii::reader::LogicalIndexReader& idx,
                                 const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    EXPECT_TRUE(::doris::snii::query::phrase_query(idx, terms, &docids).ok());
    return docids;
}

std::vector<uint32_t> run_phrase_prefix(const ::doris::snii::reader::LogicalIndexReader& idx,
                                        const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    EXPECT_TRUE(
            ::doris::snii::query::phrase_prefix_query(idx, terms, &docids, /*max_expansions=*/50)
                    .ok());
    return docids;
}

std::string read_file_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << path;
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

class SniiBigramDeferWriterTest : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        _saved_defer = config::snii_bigram_defer_build_to_compaction;
        _saved_prune_min_df = config::snii_bigram_prune_min_df;
        _saved_prune_max_ratio = config::snii_bigram_prune_max_df_ratio;
        init_phrase_index_meta(&_meta);
    }

    void TearDown() override {
        config::snii_bigram_defer_build_to_compaction = _saved_defer;
        config::snii_bigram_prune_min_df = _saved_prune_min_df;
        config::snii_bigram_prune_max_df_ratio = _saved_prune_max_ratio;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    std::string test_path(const std::string& name) const {
        return std::string(kTestDir) + "/" + name + ".idx";
    }

    TabletIndex _meta;

private:
    bool _saved_defer = false;
    int32_t _saved_prune_min_df = 0;
    double _saved_prune_max_ratio = 0.0;
};

// Deferral flavor 1 -- default config (df-pruning armed, so the reader's
// dict-miss fallback is meta-declared): the deferred segment carries neither
// the sentinel nor any pair term, unigrams are untouched, and phrase /
// phrase-prefix answers equal the normal segment's exactly.
TEST_F(SniiBigramDeferWriterTest, DeferredSegmentOmitsBigramsAndPhraseFallsBack) {
    // 80 rows with the adjacent pair (hello, world): df 80 clears the auto
    // prune floor of 64, so the NORMAL segment provably materializes the pair
    // (a below-floor pair would be dict-absent on BOTH segments and the
    // presence assertions below would go vacuous). Plus non-matching shapes:
    // reversed order, non-adjacent, and a second "wor"-prefixed tail for the
    // multi-expansion phrase-prefix path.
    std::vector<std::string> rows;
    std::vector<uint32_t> expect_phrase;
    for (uint32_t i = 0; i < 80; ++i) {
        rows.push_back("hello world item" + std::to_string(i));
        expect_phrase.push_back(i);
    }
    rows.emplace_back("world hello reversed"); // docid 80: not "hello world"
    rows.emplace_back("hello alpha world");    // docid 81: not adjacent
    std::vector<uint32_t> expect_prefix = expect_phrase;
    for (uint32_t i = 82; i < 85; ++i) {
        rows.push_back("hello worm data" + std::to_string(i)); // "hello wor*" only
        expect_prefix.push_back(i);
    }

    config::snii_bigram_defer_build_to_compaction = true;
    const std::string normal_path = test_path("normal");
    const std::string defer_path = test_path("defer");
    // Config on but the hint never delivered (index build shape): full build.
    write_segment(normal_path, _meta, rows, DirectLoadHint::kNone);
    write_segment(defer_path, _meta, rows, DirectLoadHint::kDirect);

    ::doris::snii::io::LocalFileReader normal_file;
    ::doris::snii::reader::SniiSegmentReader normal_segment;
    ::doris::snii::reader::LogicalIndexReader normal_idx;
    open_index(normal_path, &normal_file, &normal_segment, &normal_idx);
    ::doris::snii::io::LocalFileReader defer_file;
    ::doris::snii::reader::SniiSegmentReader defer_segment;
    ::doris::snii::reader::LogicalIndexReader defer_idx;
    open_index(defer_path, &defer_file, &defer_segment, &defer_idx);

    const std::string sentinel = ::doris::snii::format::make_phrase_bigram_sentinel_term();
    const std::string pair = ::doris::snii::format::make_phrase_bigram_term("hello", "world");
    // Normal segment: sentinel + the high-df pair are materialized.
    EXPECT_TRUE(lookup_found(normal_idx, sentinel));
    EXPECT_TRUE(lookup_found(normal_idx, pair));
    // Deferred segment: no sentinel or pair term, while the unigrams stay
    // identical. Its resident meta flag avoids probing an impossible pair.
    EXPECT_FALSE(lookup_found(defer_idx, sentinel));
    EXPECT_FALSE(lookup_found(defer_idx, pair));
    EXPECT_TRUE(lookup_found(defer_idx, "hello"));
    EXPECT_TRUE(lookup_found(defer_idx, "world"));
    // Deferral is independent of the prune declaration: the meta still records
    // the flush-resolved thresholds, plus the resident deferred capability that
    // bypasses the otherwise unavoidable missing-pair probe.
    EXPECT_GT(defer_idx.bigram_prune_min_df(), 0U);
    EXPECT_FALSE(normal_idx.phrase_bigrams_deferred());
    EXPECT_TRUE(defer_idx.phrase_bigrams_deferred());

    // POSITIVE CONTROL first: the normal segment's 2-term fast path actually
    // probes the pair dict (and hits) -- proving bigram_probe_attempts is a
    // live instrument, so the ==0 assertions on the deferred segment below
    // cannot go vacuous if the counting site is ever moved or removed.
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    EXPECT_EQ(run_phrase(normal_idx, {"hello", "world"}), expect_phrase);
    EXPECT_EQ(qinternal::query_test_counters().bigram_probe_attempts, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    // Same rows -> same answers; the deferred segment just takes the
    // positions-verification route without a single pair probe.
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    EXPECT_EQ(run_phrase(defer_idx, {"hello", "world"}), expect_phrase);
    EXPECT_EQ(qinternal::query_test_counters().bigram_probe_attempts, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);

    // Count-only first probes the hidden pair before deciding whether it can
    // fabricate a count. The same resident gate prevents that redundant probe
    // and lets the normal phrase path own verification.
    bool count_handled = true;
    uint64_t count = 1;
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    assert_ok(::doris::snii::query::count_only_two_term_phrase_bigram_df(
            defer_idx, "hello", "world", &count_handled, &count));
    EXPECT_FALSE(count_handled);
    EXPECT_EQ(count, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_probe_attempts, 0U);
    // Positive control for the count path's instrument: the normal segment
    // probes once and answers from the pair's df alone (df 80).
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    assert_ok(::doris::snii::query::count_only_two_term_phrase_bigram_df(
            normal_idx, "hello", "world", &count_handled, &count));
    EXPECT_TRUE(count_handled);
    EXPECT_EQ(count, 80U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_probe_attempts, 1U);

    EXPECT_EQ(run_phrase(normal_idx, {"world", "hello"}),
              run_phrase(defer_idx, {"world", "hello"}));
    // Multi-expansion phrase-prefix ("wor" -> world + worm) crosses the
    // single-leading bigram fast path on the normal segment (one probe per
    // tail: world hits, worm misses into the prune-ambiguous fallback) and the
    // pure verification path on the deferred one: identical answers.
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    EXPECT_EQ(run_phrase_prefix(normal_idx, {"hello", "wor"}), expect_prefix);
    EXPECT_EQ(qinternal::query_test_counters().bigram_probe_attempts, 2U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);
    // Deferred segment: the phrase_prefix_query guard skips the per-tail
    // TryTwoTermPhraseBigram loop OUTRIGHT -- zero probes AND zero fallbacks.
    // (Without the guard each tail would early-return inside
    // TryTwoTermPhraseBigram and count one fallback per tail, so the
    // fallbacks==0 assertion is what pins the guard itself.)
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    EXPECT_EQ(run_phrase_prefix(defer_idx, {"hello", "wor"}), expect_prefix);
    EXPECT_EQ(qinternal::query_test_counters().bigram_probe_attempts, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);
}

// Deferral flavor 2 -- both prune gates disarmed. Newly written deferred
// segments still use their resident metadata capability gate; older segments
// without that bit retain the sentinel fallback covered by the no-bigram query
// fixtures.
TEST_F(SniiBigramDeferWriterTest, DeferredSegmentFallsBackUnderLegacyNoPruneConfig) {
    config::snii_bigram_prune_min_df = 0;         // legacy: materialize every pair
    config::snii_bigram_prune_max_df_ratio = 0.0; // and disarm the upper gate
    config::snii_bigram_defer_build_to_compaction = true;

    std::vector<std::string> rows;
    std::vector<uint32_t> expect_phrase;
    for (uint32_t i = 0; i < 5; ++i) {
        rows.push_back("quick fox item" + std::to_string(i));
        expect_phrase.push_back(i);
    }
    rows.emplace_back("fox quick reversed"); // docid 5: adjacency only as (fox, quick)

    const std::string normal_path = test_path("legacy_normal");
    const std::string defer_path = test_path("legacy_defer");
    write_segment(normal_path, _meta, rows, DirectLoadHint::kNone);
    write_segment(defer_path, _meta, rows, DirectLoadHint::kDirect);

    ::doris::snii::io::LocalFileReader normal_file;
    ::doris::snii::reader::SniiSegmentReader normal_segment;
    ::doris::snii::reader::LogicalIndexReader normal_idx;
    open_index(normal_path, &normal_file, &normal_segment, &normal_idx);
    ::doris::snii::io::LocalFileReader defer_file;
    ::doris::snii::reader::SniiSegmentReader defer_segment;
    ::doris::snii::reader::LogicalIndexReader defer_idx;
    open_index(defer_path, &defer_file, &defer_segment, &defer_idx);

    const std::string sentinel = ::doris::snii::format::make_phrase_bigram_sentinel_term();
    // Legacy normal segment: every pair materialized, sentinel present, nothing
    // declared in the meta.
    EXPECT_TRUE(lookup_found(normal_idx, sentinel));
    EXPECT_TRUE(lookup_found(normal_idx,
                             ::doris::snii::format::make_phrase_bigram_term("quick", "fox")));
    EXPECT_EQ(normal_idx.bigram_prune_min_df(), 0U);
    // Deferred segment: no sentinel or pairs. The metadata capability gate
    // routes it to positions verification even with no prune declaration.
    EXPECT_FALSE(lookup_found(defer_idx, sentinel));
    EXPECT_FALSE(lookup_found(defer_idx,
                              ::doris::snii::format::make_phrase_bigram_term("quick", "fox")));
    EXPECT_EQ(defer_idx.bigram_prune_min_df(), 0U);
    EXPECT_EQ(defer_idx.bigram_prune_max_df(), 0U);
    EXPECT_TRUE(defer_idx.phrase_bigrams_deferred());

    EXPECT_EQ(run_phrase(normal_idx, {"quick", "fox"}), expect_phrase);
    EXPECT_EQ(run_phrase(defer_idx, {"quick", "fox"}), expect_phrase);
    // Legacy fast "dict miss == empty" on the normal segment vs the deferred
    // segment's positions verification: a bigram-indexable pair whose terms
    // both exist but are never ADJACENT ("fox"@0 / "reversed"@2 in docid 5)
    // must come back empty through BOTH routes.
    EXPECT_TRUE(run_phrase(normal_idx, {"fox", "reversed"}).empty());
    EXPECT_TRUE(run_phrase(defer_idx, {"fox", "reversed"}).empty());
}

// Every non-deferring combination must leave the segment BYTE-IDENTICAL to the
// untouched baseline: the hint or the config alone (or delivering an explicit
// "not direct load") may not perturb the output.
TEST_F(SniiBigramDeferWriterTest, NonDeferringCombinationsStayByteIdentical) {
    const std::vector<std::string> rows = {"hello world one", "hello world two",
                                           "gamma delta three", "world hello four"};

    config::snii_bigram_defer_build_to_compaction = false;
    const std::string baseline_path = test_path("baseline");
    write_segment(baseline_path, _meta, rows, DirectLoadHint::kNone);
    const std::string baseline_bytes = read_file_bytes(baseline_path);
    ASSERT_FALSE(baseline_bytes.empty());

    // Direct load but config off.
    const std::string direct_off_path = test_path("direct_config_off");
    write_segment(direct_off_path, _meta, rows, DirectLoadHint::kDirect);

    // Config on but not a direct load (compaction / schema change shape).
    config::snii_bigram_defer_build_to_compaction = true;
    const std::string not_direct_path = test_path("not_direct_config_on");
    write_segment(not_direct_path, _meta, rows, DirectLoadHint::kNotDirect);

    // Config on but the hint never delivered (ADD INDEX build shape).
    const std::string no_hint_path = test_path("no_hint_config_on");
    write_segment(no_hint_path, _meta, rows, DirectLoadHint::kNone);

    EXPECT_EQ(read_file_bytes(direct_off_path), baseline_bytes);
    EXPECT_EQ(read_file_bytes(not_direct_path), baseline_bytes);
    EXPECT_EQ(read_file_bytes(no_hint_path), baseline_bytes);

    // And the deferring combination DOES change bytes (the negative control
    // proving the three assertions above are not vacuous).
    const std::string defer_path = test_path("defer_control");
    write_segment(defer_path, _meta, rows, DirectLoadHint::kDirect);
    EXPECT_NE(read_file_bytes(defer_path), baseline_bytes);
}

// Spec-G capture pin, flip direction on -> off: the defer decision is captured
// ONCE in set_direct_load(); a live-config flip landing one instruction later
// (config::snii_bigram_defer_build_to_compaction is an mBool, hot-changeable
// mid-load) must not leak into the pair feed or the finish()-time sentinel.
// If _add_value_tokens re-read the config it would feed all 80 pairs under
// config=off (df 80 -> the pair MATERIALIZES: dict probe goes red); if finish()
// re-read it it would add the sentinel; either way the bytes diverge from the
// constant-config deferred twin.
TEST_F(SniiBigramDeferWriterTest, CapturedDeferralIgnoresMidLoadConfigFlipToOff) {
    const std::vector<std::string> rows = eighty_pair_rows();

    // Reference twin: config=on held constant across the whole deferred write.
    config::snii_bigram_defer_build_to_compaction = true;
    const std::string defer_ref_path = test_path("flip_off_ref");
    write_segment(defer_ref_path, _meta, rows, DirectLoadHint::kDirect);

    // Flip scenario: capture under config=on, then config=off through every
    // add_values call and through finish().
    config::snii_bigram_defer_build_to_compaction = true;
    const std::string flipped_path = test_path("flip_off");
    write_segment_flipping_config_after_hint(flipped_path, _meta, rows,
                                             /*config_after_hint=*/false);

    // The mid-load flip must be invisible: byte-identical to the constant twin.
    EXPECT_EQ(read_file_bytes(flipped_path), read_file_bytes(defer_ref_path));

    // Belt and braces, pinpointing WHICH re-read regressed on a failure: a
    // finish()-side config re-read would have written the sentinel, a
    // feed-side one the (materialized, df 80) pair term.
    ::doris::snii::io::LocalFileReader flip_file;
    ::doris::snii::reader::SniiSegmentReader flip_segment;
    ::doris::snii::reader::LogicalIndexReader flip_idx;
    open_index(flipped_path, &flip_file, &flip_segment, &flip_idx);
    EXPECT_FALSE(lookup_found(flip_idx, ::doris::snii::format::make_phrase_bigram_sentinel_term()));
    EXPECT_FALSE(lookup_found(flip_idx,
                              ::doris::snii::format::make_phrase_bigram_term("hello", "world")));
    EXPECT_TRUE(lookup_found(flip_idx, "hello")); // unigrams untouched by the flip
}

// Spec-G capture pin, flip direction off -> on: set_direct_load(true) under
// config=off captures NO deferral; turning the config on before the first row
// must not arm it retroactively. A config re-read anywhere downstream would
// skip pair feeds and/or the sentinel and the bytes would leave the baseline.
TEST_F(SniiBigramDeferWriterTest, CapturedNonDeferralIgnoresMidLoadConfigFlipToOn) {
    const std::vector<std::string> rows = eighty_pair_rows();

    // Reference twin: config=off held constant -- the full build (proven
    // byte-identical to every other non-deferring shape by
    // NonDeferringCombinationsStayByteIdentical above).
    config::snii_bigram_defer_build_to_compaction = false;
    const std::string baseline_path = test_path("flip_on_ref");
    write_segment(baseline_path, _meta, rows, DirectLoadHint::kDirect);

    // Flip scenario: capture under config=off, then config=on through feed and
    // finish. Deferral must stay disarmed.
    config::snii_bigram_defer_build_to_compaction = false;
    const std::string flipped_path = test_path("flip_on");
    write_segment_flipping_config_after_hint(flipped_path, _meta, rows,
                                             /*config_after_hint=*/true);

    EXPECT_EQ(read_file_bytes(flipped_path), read_file_bytes(baseline_path));

    // Explicit full-build evidence on the flipped segment: sentinel and the
    // df-80 pair are both materialized.
    ::doris::snii::io::LocalFileReader flip_file;
    ::doris::snii::reader::SniiSegmentReader flip_segment;
    ::doris::snii::reader::LogicalIndexReader flip_idx;
    open_index(flipped_path, &flip_file, &flip_segment, &flip_idx);
    EXPECT_TRUE(lookup_found(flip_idx, ::doris::snii::format::make_phrase_bigram_sentinel_term()));
    EXPECT_TRUE(lookup_found(flip_idx,
                             ::doris::snii::format::make_phrase_bigram_term("hello", "world")));
}

// Docs-only index (no support_phrase -> no positions -> no bigram build to
// defer): a direct load with the config ON must neither publish the resident
// kPhraseBigramsDeferred flag nor perturb a single byte vs the config-off
// baseline. Pins the writer-side _has_positions gate through the REAL persisted
// meta -- the scaffold-free BaseHook test below can only show the writer stays
// usable, not what lands on disk.
TEST_F(SniiBigramDeferWriterTest, DocsOnlyIndexNeverPublishesDeferFlag) {
    TabletIndex docs_meta;
    {
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(kIndexId);
        pb.set_index_name("defer_docs_only_idx");
        pb.add_col_unique_id(0);
        pb.mutable_properties()->insert({"parser", "english"});
        pb.mutable_properties()->insert({"lower_case", "true"});
        // No support_phrase: the index resolves docs-only, _has_positions false.
        docs_meta.init_from_pb(pb);
    }
    const std::vector<std::string> rows = {"hello world one", "hello world two"};

    config::snii_bigram_defer_build_to_compaction = false;
    const std::string baseline_path = test_path("docs_only_baseline");
    write_segment(baseline_path, docs_meta, rows, DirectLoadHint::kNone);

    config::snii_bigram_defer_build_to_compaction = true;
    const std::string direct_path = test_path("docs_only_direct");
    write_segment(direct_path, docs_meta, rows, DirectLoadHint::kDirect);

    EXPECT_EQ(read_file_bytes(direct_path), read_file_bytes(baseline_path));

    ::doris::snii::io::LocalFileReader file;
    ::doris::snii::reader::SniiSegmentReader segment;
    ::doris::snii::reader::LogicalIndexReader idx;
    open_index(direct_path, &file, &segment, &idx);
    EXPECT_FALSE(idx.has_positions());
    EXPECT_FALSE(idx.phrase_bigrams_deferred());
}

// The base-class hook is what a V3 InvertedIndexColumnWriter inherits: it must
// be a callable no-op on a writer that never overrides it.
TEST(SniiBigramDeferBaseHook, SetDirectLoadDefaultIsNoOp) {
    // Minimal non-overriding IndexColumnWriter, mirroring the DBUG empty writer
    // in column_writer.cpp: exercises the exact inherited base method a V3
    // writer would dispatch to.
    class NoOverrideWriter final : public IndexColumnWriter {
    public:
        Status init() override { return Status::OK(); }
        Status add_values(const std::string, const void*, size_t) override { return Status::OK(); }
        Status add_array_values(size_t, const void*, const uint8_t*, const uint8_t*,
                                size_t) override {
            return Status::OK();
        }
        Status add_nulls(uint32_t) override { return Status::OK(); }
        Status add_array_nulls(const uint8_t*, size_t) override { return Status::OK(); }
        Status finish() override { return Status::OK(); }
        int64_t size() const override { return 0; }
        void close_on_error() override {}
    };

    NoOverrideWriter writer;
    IndexColumnWriter* base = &writer;
    base->set_direct_load(true); // inherited default: must be a harmless no-op
    base->set_direct_load(false);
    EXPECT_TRUE(base->finish().ok());
}

// The SNII override's _has_positions gate: on a writer whose index has no
// positions (scaffold-free, init() never run -> _has_positions == false, the
// same value a docs-only index resolves), a direct-load marking with the
// config on must NOT arm deferral -- there is no bigram build to defer -- and
// the writer stays fully usable.
TEST(SniiBigramDeferBaseHook, SniiWriterWithoutPositionsNeverDefers) {
    const bool saved = config::snii_bigram_defer_build_to_compaction;
    config::snii_bigram_defer_build_to_compaction = true;
    {
        SniiIndexColumnWriter writer(nullptr, nullptr, /*single_field=*/true);
        writer.set_direct_load(true);
        EXPECT_TRUE(writer.add_nulls(3).ok());
        EXPECT_EQ(writer.null_docids_for_test().size(), 3U);
    }
    config::snii_bigram_defer_build_to_compaction = saved;
}

} // namespace
} // namespace doris::segment_v2
