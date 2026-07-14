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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

// G06 verification gate at the QUERY level: phrase / phrase_prefix RESULTS
// from a build whose low-df pair-keyed bigrams were dropped by the DRAIN-SIDE
// df gate (sunk from process_term; plumbed automatically by
// LogicalIndexWriter::build_blocks from input.bigram_prune_min_df) are
// identical to a G01 STRING-KEYED control build over the same logical token
// stream -- whose bigrams only process_term's flush gate prunes -- across the
// two routings the gate can influence:
//   hot    : df >= threshold survivor -> answered DIRECTLY from its bigram
//            posting in both builds (bigram_hits seam);
//   pruned : df < threshold pair -> dropped at the DRAIN in the pair build,
//            by process_term in the control -> no dict entry in either, both
//            take the generic positions FALLBACK with equal results --
//            including an out-of-order docid REVISIT pair, which the drain
//            gate must exempt (inexact counter) and hand to process_term.
// The two segments' BYTES are also asserted identical: at query time the gate
// is entirely invisible.
using namespace doris::snii;
using namespace doris::snii::snii_test;
using doris::snii::query::phrase_prefix_query;
using doris::snii::query::phrase_query;
namespace qinternal = doris::snii::query::internal;
namespace spimi_testing = doris::snii::writer::testing;

namespace {

constexpr uint32_t kDocCount = 400;
constexpr uint32_t kThreshold = 3; // (gone,away) df 2 and (twist,back) df 2 drop;
                                   // (hot,pair) df 13 survives

struct Fixture {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
};

// STRING-KEYED control feed (the G01 writer behavior): these bigrams never
// enter the pair map, so the G06 drain gate cannot touch them -- process_term
// is their only pruner.
void feed_doc_strings(writer::SpimiTermBuffer* buf, uint32_t docid, std::string_view l,
                      std::string_view r, uint32_t pos_base = 0) {
    buf->add_token(l, docid, pos_base);
    buf->add_token(r, docid, pos_base + 1);
    buf->add_bigram_token(l, r, docid, pos_base);
}

// PAIR-KEYED feed, mirroring the production _add_value_tokens +
// _add_phrase_bigram_tokens split: intern the unigrams (capturing ids), then
// add the adjacent pair by id.
void feed_doc_ids(writer::SpimiTermBuffer* buf, uint32_t docid, std::string_view l,
                  std::string_view r, uint32_t pos_base = 0) {
    const uint32_t lid = buf->add_token_returning_id(l, docid, pos_base);
    const uint32_t rid = buf->add_token_returning_id(r, docid, pos_base + 1);
    buf->add_bigram_token(lid, rid, docid, pos_base);
}

// The shared logical token stream (identical in both builds).
template <class FeedPair>
void feed_stream(writer::SpimiTermBuffer* buf, FeedPair&& feed_pair) {
    // Hot pair: df 13 >= threshold, answered directly from its bigram posting.
    feed_pair(buf, 0, "hot", "pair", 0);
    feed_pair(buf, 1, "hot", "pair", 0);
    feed_pair(buf, 2, "hot", "pair", 0);
    // Pruned pair: df 2 < threshold -> the G06 drain drop (pair build) /
    // process_term df prune (control). Query falls back to unigram positions.
    feed_pair(buf, 3, "gone", "away", 0);
    feed_pair(buf, 4, "gone", "away", 0);
    // Out-of-order REVISIT pair (docids 20, 8, 20): Term::ndocs overcounts
    // (3 groups, coalesced df 2), so the drain gate must exempt it; both
    // builds prune it at process_term on the exact coalesced df.
    feed_pair(buf, 20, "twist", "back", 0);
    feed_pair(buf, 8, "twist", "back", 0);
    feed_pair(buf, 20, "twist", "back", 10);
    // More hot occurrences (docids keep ascending past the revisit block).
    for (uint32_t d = 50; d < 60; ++d) {
        feed_pair(buf, d, "hot", "pair", 0);
    }
    // The sentinel, as the production writer feeds it at finish().
    buf->add_token(format::make_phrase_bigram_sentinel_term(), 0, 0);
}

Status build_fixture(Fixture* f, writer::SpimiTermBuffer* buf) {
    writer::SniiIndexInput input;
    input.index_id = 23;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = kDocCount;
    input.bigram_prune_min_df = kThreshold;
    input.term_source = buf;
    input.bigram_ever_dropped = buf->bigram_dropped_filter(); // null: no evictions here

    writer::SniiCompoundWriter writer(&f->file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(&f->file, &f->segment_reader));
    return f->segment_reader.open_index(input.index_id, input.index_suffix, &f->index_reader);
}

std::vector<uint32_t> run_phrase(const reader::LogicalIndexReader& idx,
                                 const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    assert_ok(phrase_query(idx, terms, &docids));
    return docids;
}

std::vector<uint32_t> run_phrase_prefix(const reader::LogicalIndexReader& idx,
                                        const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(idx, terms, &docids));
    return docids;
}

void reset_query_counters() {
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
}

} // namespace

TEST(SniiPhraseBigramDrainDfGateQuery, ResultsEqualStringKeyedControlHotAndPruned) {
    writer::SpimiTermBuffer pair_buf(/*has_positions=*/true);
    writer::SpimiTermBuffer control_buf(/*has_positions=*/true);
    pair_buf.configure_bigram_diet(0);
    control_buf.configure_bigram_diet(0);

    feed_stream(&pair_buf, feed_doc_ids);
    feed_stream(&control_buf, feed_doc_strings);
    ASSERT_TRUE(pair_buf.status().ok());
    ASSERT_TRUE(control_buf.status().ok());

    // The pair build's below-threshold pairs die at the DRAIN gate (seam: only
    // (gone,away); the out-of-order (twist,back) is exempt and reaches
    // process_term); no bloom is ever created by those drops.
    spimi_testing::reset_bigram_drain_df_drops();
    Fixture pair_fix;
    assert_ok(build_fixture(&pair_fix, &pair_buf));
    EXPECT_EQ(spimi_testing::bigram_drain_df_drops(), 1U);
    EXPECT_EQ(pair_buf.bigram_dropped_filter(), nullptr);

    Fixture control_fix;
    assert_ok(build_fixture(&control_fix, &control_buf));
    // String-keyed bigrams never enter the drain gate.
    EXPECT_EQ(spimi_testing::bigram_drain_df_drops(), 1U);

    // The gate is invisible on disk: identical segment bytes.
    ASSERT_EQ(pair_fix.file.size(), control_fix.file.size());
    std::vector<uint8_t> pair_bytes;
    std::vector<uint8_t> control_bytes;
    assert_ok(pair_fix.file.read_at(0, pair_fix.file.size(), &pair_bytes));
    assert_ok(control_fix.file.read_at(0, control_fix.file.size(), &control_bytes));
    EXPECT_EQ(pair_bytes, control_bytes);

    EXPECT_EQ(pair_fix.index_reader.bigram_prune_min_df(), kThreshold);
    EXPECT_EQ(control_fix.index_reader.bigram_prune_min_df(), kThreshold);

    // HOT routing: direct bigram hit in BOTH builds, equal results.
    const std::vector<std::string> hot {"hot", "pair"};
    std::vector<uint32_t> want_hot {0, 1, 2};
    for (uint32_t d = 50; d < 60; ++d) {
        want_hot.push_back(d);
    }
    reset_query_counters();
    EXPECT_EQ(run_phrase(control_fix.index_reader, hot), want_hot);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);
    reset_query_counters();
    EXPECT_EQ(run_phrase(pair_fix.index_reader, hot), want_hot);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);

    // PRUNED routing (drain-dropped in the pair build): dict miss on a
    // pruning-declared segment -> generic positions fallback, equal results.
    const std::vector<std::string> pruned {"gone", "away"};
    const std::vector<uint32_t> want_pruned {3, 4};
    reset_query_counters();
    EXPECT_EQ(run_phrase(control_fix.index_reader, pruned), want_pruned);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);
    reset_query_counters();
    EXPECT_EQ(run_phrase(pair_fix.index_reader, pruned), want_pruned);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);

    // PRUNED routing, out-of-order revisit shape: exempt from the drain gate,
    // pruned by process_term on the exact coalesced df in BOTH builds; the
    // fallback recovers the coalesced doc set.
    const std::vector<std::string> revisit {"twist", "back"};
    const std::vector<uint32_t> want_revisit {8, 20};
    reset_query_counters();
    EXPECT_EQ(run_phrase(control_fix.index_reader, revisit), want_revisit);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);
    reset_query_counters();
    EXPECT_EQ(run_phrase(pair_fix.index_reader, revisit), want_revisit);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);

    // PHRASE_PREFIX over the same segments: identical results for a hot head
    // ("hot pai" -> "hot pair") and a drain-dropped head ("gone aw" ->
    // "gone away").
    const std::vector<std::string> hot_prefix {"hot", "pai"};
    EXPECT_EQ(run_phrase_prefix(control_fix.index_reader, hot_prefix), want_hot);
    EXPECT_EQ(run_phrase_prefix(pair_fix.index_reader, hot_prefix), want_hot);
    const std::vector<std::string> pruned_prefix {"gone", "aw"};
    EXPECT_EQ(run_phrase_prefix(control_fix.index_reader, pruned_prefix), want_pruned);
    EXPECT_EQ(run_phrase_prefix(pair_fix.index_reader, pruned_prefix), want_pruned);
}
