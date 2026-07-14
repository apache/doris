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
#include <utility>
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

// G05 verification gate: phrase / phrase_prefix query RESULTS from a build fed
// through the PAIR-KEYED bigram path (add_token_returning_id +
// add_bigram_token(left_id, right_id) -- what the production column writer now
// does) are identical to a G01 STRING-KEYED control build over the same logical
// token stream, across every routing:
//   hot    : df>=2 survivor -> answered DIRECTLY from its bigram posting in
//            both builds (bigram_hits seam);
//   pruned : df==1 pair, below the df threshold in BOTH builds -> no dict
//            entry, generic positions FALLBACK, same results;
//   evicted-reappearing (pair build only capped): the G04 vocab cap evicted the
//            pair at df==1 under PAIR KEYING, the bloom (inserted via the
//            piecewise content hash) drops the reappeared term at flush, and
//            the cold fallback recovers the identical doc set the uncapped
//            string-keyed control answers directly;
//   phrase_prefix : the tail-expansion path over the same segments agrees.
using namespace doris::snii;
using namespace doris::snii::snii_test;
using doris::snii::query::phrase_prefix_query;
using doris::snii::query::phrase_query;
namespace qinternal = doris::snii::query::internal;
namespace wtesting = doris::snii::writer::testing;

namespace {

constexpr uint32_t kDocCount = 400;
constexpr uint32_t kThreshold = 2; // df==1 bigrams pruned in BOTH builds (the
                                   // pruned-fallback routing); the reappeared
                                   // victim's df is far above it, isolating the
                                   // BLOOM as its only dropper in the pair build

const std::string kHotTerm = format::make_phrase_bigram_term("hot", "pair");
const std::string kVictimTerm = format::make_phrase_bigram_term("gone", "away");

struct Fixture {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
};

// STRING-KEYED control feed (the G01/HEAD writer behavior).
void feed_doc_strings(writer::SpimiTermBuffer* buf, uint32_t docid, std::string_view l,
                      std::string_view r) {
    buf->add_token(l, docid, 0);
    buf->add_token(r, docid, 1);
    buf->add_bigram_token(l, r, docid, 0);
}

// PAIR-KEYED feed, mirroring the production _add_value_tokens +
// _add_phrase_bigram_tokens split: intern the unigrams (capturing ids), then
// add the adjacent pair by id.
void feed_doc_ids(writer::SpimiTermBuffer* buf, uint32_t docid, std::string_view l,
                  std::string_view r) {
    const uint32_t lid = buf->add_token_returning_id(l, docid, 0);
    const uint32_t rid = buf->add_token_returning_id(r, docid, 1);
    buf->add_bigram_token(lid, rid, docid, 0);
}

std::pair<std::string, std::string> filler_pair(uint32_t i) {
    std::string l = "u";
    l += static_cast<char>('a' + (i / 26) % 26);
    l += static_cast<char>('a' + i % 26);
    l += static_cast<char>('a' + (i / 676) % 26);
    std::string r = "v";
    r += static_cast<char>('a' + i % 26);
    r += static_cast<char>('a' + (i / 26) % 26);
    r += static_cast<char>('a' + (i / 676) % 26);
    return {std::move(l), std::move(r)};
}

// Feeds the IDENTICAL logical token stream into the (capped) pair-keyed buffer
// and the (uncapped) string-keyed control. The filler phase keeps feeding
// unique tail pairs until the capped pair build provably evicted the victim
// (probed by the COMPOSED string -- the pair eviction must have bloomed the
// identical content hash).
void feed_stream(writer::SpimiTermBuffer* pair_capped, writer::SpimiTermBuffer* str_control) {
    auto both = [&](uint32_t docid, std::string_view l, std::string_view r) {
        feed_doc_ids(pair_capped, docid, l, r);
        feed_doc_strings(str_control, docid, l, r);
    };
    // Hot pair reaches df==2 before any cap pressure -> never evictable.
    both(0, "hot", "pair");
    both(1, "hot", "pair");
    // A df==1 pair that stays df==1: pruned by the df threshold in BOTH builds.
    both(2, "rare", "once");
    // The victim: one doc -> df==1 when the tail pressure arrives. Fed
    // explicitly so the pair build's unigram ids are captured ONCE here and
    // reused by the reappearance below (identical token streams in both builds).
    const uint32_t gone = pair_capped->add_token_returning_id("gone", 4, 0);
    const uint32_t away = pair_capped->add_token_returning_id("away", 4, 1);
    pair_capped->add_bigram_token(gone, away, 4, 0);
    feed_doc_strings(str_control, 4, "gone", "away");
    // Unique tail pairs until the victim is evicted from the capped pair build.
    uint32_t docid = 5;
    for (uint32_t i = 0; i < 2000 && docid < 190; ++i) {
        if (pair_capped->bigram_dropped_filter() != nullptr &&
            pair_capped->bigram_dropped_filter()->maybe_contains(kVictimTerm)) {
            break;
        }
        const auto [l, r] = filler_pair(i);
        both(docid++, l, r);
    }
    // The victim REAPPEARS: bigram tokens back-to-back first (the second add
    // re-establishes df==2 immunity deterministically -- a term is never
    // evicted by its own add's sweep), then the unigrams for those docs. The
    // pair build reuses the unigram ids captured at doc 4 (unigram ids are
    // stable for the buffer's lifetime).
    for (uint32_t d = 200; d < 220; ++d) {
        pair_capped->add_bigram_token(gone, away, d, 0);
        str_control->add_bigram_token("gone", "away", d, 0);
    }
    for (uint32_t d = 200; d < 220; ++d) {
        pair_capped->add_token("gone", d, 0);
        pair_capped->add_token("away", d, 1);
        str_control->add_token("gone", d, 0);
        str_control->add_token("away", d, 1);
    }
    // More hot occurrences after the pressure (docids keep ascending).
    for (uint32_t d = 300; d < 310; ++d) {
        both(d, "hot", "pair");
    }
    // The sentinel, as the production writer feeds it at finish().
    pair_capped->add_token(format::make_phrase_bigram_sentinel_term(), 0, 0);
    str_control->add_token(format::make_phrase_bigram_sentinel_term(), 0, 0);
}

Status build_fixture(Fixture* f, writer::SpimiTermBuffer* buf,
                     const writer::BigramDropFilter* dropped) {
    writer::SniiIndexInput input;
    input.index_id = 22;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = kDocCount;
    input.bigram_prune_min_df = kThreshold;
    input.term_source = buf;
    input.bigram_ever_dropped = dropped;

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

TEST(SniiPhraseBigramPairKeyQuery, ResultsEqualStringKeyedControlAcrossRoutings) {
    writer::SpimiTermBuffer pair_buf(/*has_positions=*/true);
    writer::SpimiTermBuffer control_buf(/*has_positions=*/true);
    pair_buf.configure_bigram_diet(/*vocab_cap_bytes=*/2 * 1024);
    // Control: same diet mode (docs-only bigrams) but UNCAPPED string-keyed
    // interning -- no eviction, complete bigram vocabulary (ground truth).
    control_buf.configure_bigram_diet(/*vocab_cap_bytes=*/0);

    feed_stream(&pair_buf, &control_buf);
    ASSERT_TRUE(pair_buf.status().ok());
    ASSERT_TRUE(control_buf.status().ok());
    // The victim was evicted from the PAIR-KEYED build (bloomed via the
    // piecewise content hash -- probed here by the composed string); the hot
    // pair was not.
    ASSERT_NE(pair_buf.bigram_dropped_filter(), nullptr);
    ASSERT_TRUE(pair_buf.bigram_dropped_filter()->maybe_contains(kVictimTerm));
    EXPECT_FALSE(pair_buf.bigram_dropped_filter()->maybe_contains(kHotTerm));

    wtesting::reset_bigram_prune_counters();
    Fixture pair_fix;
    assert_ok(build_fixture(&pair_fix, &pair_buf, pair_buf.bigram_dropped_filter()));
    // Exactly ONE df-surviving bigram was dropped by the BLOOM in the pair
    // build (the reappeared victim, df 20 >= threshold 2).
    EXPECT_EQ(wtesting::bigram_bloom_dropped(), 1U);

    Fixture control_fix;
    assert_ok(build_fixture(&control_fix, &control_buf, control_buf.bigram_dropped_filter()));
    EXPECT_EQ(pair_fix.index_reader.bigram_prune_min_df(), kThreshold);
    EXPECT_EQ(control_fix.index_reader.bigram_prune_min_df(), kThreshold);

    // HOT pair: direct bigram hit in BOTH builds, equal results.
    const std::vector<std::string> hot {"hot", "pair"};
    std::vector<uint32_t> want_hot {0, 1};
    for (uint32_t d = 300; d < 310; ++d) {
        want_hot.push_back(d);
    }
    EXPECT_EQ(run_phrase(control_fix.index_reader, hot), want_hot);
    reset_query_counters();
    EXPECT_EQ(run_phrase(pair_fix.index_reader, hot), want_hot);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);

    // PRUNED df==1 pair: no dict entry in EITHER build (df gate), fallback
    // routing, equal results.
    const std::vector<std::string> rare {"rare", "once"};
    const std::vector<uint32_t> want_rare {2};
    EXPECT_EQ(run_phrase(control_fix.index_reader, rare), want_rare);
    reset_query_counters();
    EXPECT_EQ(run_phrase(pair_fix.index_reader, rare), want_rare);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);

    // EVICTED-REAPPEARING pair: the uncapped string-keyed control answers
    // DIRECTLY from its complete bigram posting {4, 200..219}; the capped pair
    // build bloom-dropped the (incomplete) reappeared term and takes the COLD
    // FALLBACK to unigram positions -- recovering the identical doc set,
    // including doc 4, whose posting the eviction lost.
    const std::vector<std::string> victim {"gone", "away"};
    std::vector<uint32_t> want_victim {4};
    for (uint32_t d = 200; d < 220; ++d) {
        want_victim.push_back(d);
    }
    reset_query_counters();
    EXPECT_EQ(run_phrase(control_fix.index_reader, victim), want_victim);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);

    reset_query_counters();
    EXPECT_EQ(run_phrase(pair_fix.index_reader, victim), want_victim);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);

    // PHRASE_PREFIX over the same segments: identical results between the
    // builds for a hot head ("hot pai" -> "hot pair") and the evicted victim
    // head ("gone aw" -> "gone away").
    const std::vector<std::string> hot_prefix {"hot", "pai"};
    EXPECT_EQ(run_phrase_prefix(control_fix.index_reader, hot_prefix), want_hot);
    EXPECT_EQ(run_phrase_prefix(pair_fix.index_reader, hot_prefix), want_hot);
    const std::vector<std::string> victim_prefix {"gone", "aw"};
    EXPECT_EQ(run_phrase_prefix(control_fix.index_reader, victim_prefix), want_victim);
    EXPECT_EQ(run_phrase_prefix(pair_fix.index_reader, victim_prefix), want_victim);
}
