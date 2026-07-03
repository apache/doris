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
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

// G06: the flush-time phrase-bigram df prune (process_term's
// `df < bigram_prune_min_df` gate) is SUNK into the SPIMI drain for PAIR-KEYED
// bigram terms: a pair term whose EXACT df (Term::ndocs, valid while
// Term::sorted holds) is below the threshold is dropped at
// prepare_pair_terms_for_drain WITHOUT composing its term string, decoding its
// postings, or leaving any trace -- the same disposition process_term would
// give it after paying for all of that (on wikipedia, the low-df tail's
// materialize+emit was ~1/3 of build CPU). These tests pin:
//   (1) the final-drain drop set equals process_term's df gate exactly
//       (equality vs a pair-keyed-without-G06 control), drops never bloom,
//       and the strings of dropped terms are never materialized;
//   (2) the threshold boundary through the REAL flush plumbing
//       (LogicalIndexWriter::build_blocks -> set_bigram_drain_min_df):
//       df == threshold survives, df == threshold-1 drops, and the segment
//       BYTES equal a string-keyed control's (whose bigrams only process_term
//       gates -- G06 never touches non-pair-map terms);
//   (3) EXACTNESS: an out-of-order docid revisit feed (5,1,5) can make ndocs
//       overcount the coalesced df, so such terms are NEVER dropped at drain --
//       they materialize and process_term gates the exact coalesced df;
//   (4) the mid-feed spill + final drain mix: df-below-threshold pair terms
//       are dropped+BLOOMED at mid-feed spills (a reappearance must die at
//       flush) but dropped UNbloomed at the final drain, and the emitted
//       stream equals a no-spill control's.
using doris::Status;
using doris::snii::format::make_phrase_bigram_sentinel_term;
using doris::snii::format::make_phrase_bigram_term;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;
using namespace doris::snii;
using namespace doris::snii::snii_test;
namespace spimi_testing = doris::snii::writer::testing;

namespace {

// Feeds one "document" of two adjacent words into `buf` through the G05 pair
// path exactly as the production column writer does: intern the unigrams first
// (capturing their ids), then add the pair by id.
void feed_pair_doc_ids(SpimiTermBuffer* buf, uint32_t docid, std::string_view l, std::string_view r,
                       uint32_t pos_base = 0) {
    const uint32_t lid = buf->add_token_returning_id(l, docid, pos_base);
    const uint32_t rid = buf->add_token_returning_id(r, docid, pos_base + 1);
    ASSERT_NE(lid, SpimiTermBuffer::kInvalidTermId);
    ASSERT_NE(rid, SpimiTermBuffer::kInvalidTermId);
    buf->add_bigram_token(lid, rid, docid, pos_base);
}

// The G01 string-keyed control feed for the identical logical stream. These
// terms live in the content-keyed intern set, NOT the pair map, so the G06
// drain gate never sees them -- process_term's df gate is their only pruner.
void feed_pair_doc_strings(SpimiTermBuffer* buf, uint32_t docid, std::string_view l,
                           std::string_view r, uint32_t pos_base = 0) {
    buf->add_token(l, docid, pos_base);
    buf->add_token(r, docid, pos_base + 1);
    buf->add_bigram_token(l, r, docid, pos_base);
}

std::vector<TermPostings> drain_ordered(SpimiTermBuffer* buf) {
    std::vector<TermPostings> out = buf->finalize_sorted();
    EXPECT_TRUE(buf->status().ok()) << buf->status().to_string();
    return out;
}

void expect_same_stream(const std::vector<TermPostings>& got, const std::vector<TermPostings>& want,
                        const char* label) {
    ASSERT_EQ(got.size(), want.size()) << label;
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i].term, want[i].term) << label << " term order diverged at " << i;
        EXPECT_EQ(got[i].docids, want[i].docids) << label << " " << got[i].term;
        EXPECT_EQ(got[i].freqs, want[i].freqs) << label << " " << got[i].term;
        EXPECT_EQ(got[i].positions_flat, want[i].positions_flat) << label << " " << got[i].term;
    }
}

bool stream_has_term(const std::vector<TermPostings>& terms, const std::string& term) {
    for (const TermPostings& tp : terms) {
        if (tp.term == term) {
            return true;
        }
    }
    return false;
}

// Threshold-straddling fixture at kGateThreshold == 3: a df==3 survivor (the
// == threshold boundary), a df==2 boundary victim (== threshold - 1), a df==1
// tail victim, a pair-less unigram and the sentinel (fed last, as the
// production writer's finish() does).
constexpr uint32_t kGateThreshold = 3;

template <class FeedPair>
void feed_gate_stream(SpimiTermBuffer* buf, FeedPair&& feed_pair) {
    feed_pair(buf, 0, "hot", "pair", 0);
    feed_pair(buf, 1, "hot", "pair", 0);
    feed_pair(buf, 2, "hot", "pair", 0); // df == 3 == threshold: SURVIVES
    feed_pair(buf, 3, "mid", "tier", 0);
    feed_pair(buf, 4, "mid", "tier", 0);  // df == 2 == threshold-1: DROPS
    feed_pair(buf, 5, "rare", "once", 0); // df == 1: DROPS
    buf->add_token("zonly", 6, 7);        // unigram with no pair
    buf->add_token(make_phrase_bigram_sentinel_term(), 0, 0);
}

Status build_segment(MemoryFile* file, SpimiTermBuffer* buf, uint32_t threshold,
                     uint32_t doc_count) {
    writer::SniiIndexInput input;
    input.index_id = 61;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = doc_count;
    input.bigram_prune_min_df = threshold;
    input.term_source = buf;
    input.bigram_ever_dropped = buf->bigram_dropped_filter();
    writer::SniiCompoundWriter writer(file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    return writer.finish();
}

std::vector<uint8_t> file_bytes(MemoryFile* file) {
    std::vector<uint8_t> bytes;
    assert_ok(file->read_at(0, file->size(), &bytes));
    return bytes;
}

bool lookup_term(const reader::LogicalIndexReader& idx, const std::string& term,
                 format::DictEntry* entry) {
    bool found = false;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(idx.lookup(term, &found, entry, &frq_base, &prx_base));
    return found;
}

} // namespace

// (1) BUFFER-LEVEL final-drain gate: the drop set is exactly {df < threshold},
// drops never materialize a string, never create/insert into the drop bloom,
// and the surviving stream equals a pair-keyed control WITHOUT the gate minus
// the below-threshold bigram terms.
TEST(SniiSpimiBigramDrainDfGate, FinalDrainDropsExactLowDfPairsNoBloomNoStrings) {
    SpimiTermBuffer gated(/*has_positions=*/true);
    SpimiTermBuffer control(/*has_positions=*/true); // pair-keyed, NO G06 gate
    gated.configure_bigram_diet(0);
    control.configure_bigram_diet(0);

    feed_gate_stream(&gated, feed_pair_doc_ids);
    feed_gate_stream(&control, feed_pair_doc_ids);
    ASSERT_TRUE(gated.status().ok());
    ASSERT_TRUE(control.status().ok());

    // The flush plumbs the EXACT threshold right before the drain; mimic it.
    gated.set_bigram_drain_min_df(kGateThreshold);

    spimi_testing::reset_bigram_drain_df_drops();
    spimi_testing::reset_vocab_string_materialization_count();
    const std::vector<TermPostings> got = drain_ordered(&gated);
    // Exactly ONE pair term survived the gate, so the drain composed exactly
    // ONE bigram string ("hot pair"); the two dropped pairs composed NOTHING.
    EXPECT_EQ(spimi_testing::vocab_string_materialization_count(), 1U);
    EXPECT_EQ(spimi_testing::bigram_drain_df_drops(), 2U); // (mid,tier) + (rare,once)
    // Final-drain drops must leave NO bloom trace: the filter is created on
    // first (bloomed) eviction only, and none happened.
    EXPECT_EQ(gated.bigram_dropped_filter(), nullptr);

    const std::vector<TermPostings> full = drain_ordered(&control);
    // want = the ungated control stream minus the below-threshold bigrams.
    std::vector<TermPostings> want;
    for (const TermPostings& tp : full) {
        const bool prunable_bigram = format::is_phrase_bigram_term(tp.term) &&
                                     !format::is_phrase_bigram_sentinel_term(tp.term) &&
                                     tp.docids.size() < kGateThreshold;
        if (!prunable_bigram) {
            want.push_back(tp);
        }
    }
    ASSERT_LT(want.size(), full.size()); // the fixture really exercised the gate
    expect_same_stream(got, want, "final-drain gate");

    // Boundary pins: df==threshold survived, df==threshold-1 and df==1 dropped.
    EXPECT_TRUE(stream_has_term(got, make_phrase_bigram_term("hot", "pair")));
    EXPECT_FALSE(stream_has_term(got, make_phrase_bigram_term("mid", "tier")));
    EXPECT_FALSE(stream_has_term(got, make_phrase_bigram_term("rare", "once")));
    EXPECT_TRUE(stream_has_term(got, make_phrase_bigram_sentinel_term()));
}

// (2) SEGMENT-LEVEL boundary through the REAL plumbing (build_blocks calls
// set_bigram_drain_min_df with the effective threshold): the pair-keyed build's
// BYTES equal the string-keyed control's, df==threshold got its dict entry,
// df==threshold-1 did not.
TEST(SniiSpimiBigramDrainDfGate, SegmentBytesEqualStringControlAndBoundaryHolds) {
    for (const uint32_t threshold : {kGateThreshold, kGateThreshold + 1}) {
        SpimiTermBuffer pair_buf(/*has_positions=*/true);
        SpimiTermBuffer str_buf(/*has_positions=*/true);
        pair_buf.configure_bigram_diet(0);
        str_buf.configure_bigram_diet(0);
        feed_gate_stream(&pair_buf, feed_pair_doc_ids);
        feed_gate_stream(&str_buf, feed_pair_doc_strings);
        ASSERT_TRUE(pair_buf.status().ok());
        ASSERT_TRUE(str_buf.status().ok());

        spimi_testing::reset_bigram_drain_df_drops();
        MemoryFile pair_file;
        assert_ok(build_segment(&pair_file, &pair_buf, threshold, /*doc_count=*/7));
        // threshold 3 drops (mid,tier)+(rare,once); threshold 4 also drops
        // (hot,pair) -- the every-pair-dropped edge.
        EXPECT_EQ(spimi_testing::bigram_drain_df_drops(), threshold == kGateThreshold ? 2U : 3U);

        MemoryFile str_file;
        assert_ok(build_segment(&str_file, &str_buf, threshold, /*doc_count=*/7));
        // String-keyed bigrams are not pair-map entries: the G06 gate never
        // fires for them (process_term pruned them instead)...
        EXPECT_EQ(spimi_testing::bigram_drain_df_drops(), threshold == kGateThreshold ? 2U : 3U);
        // ...yet the two builds' bytes are identical: the drain gate's drop set
        // is exactly process_term's.
        EXPECT_EQ(file_bytes(&pair_file), file_bytes(&str_file))
                << "threshold " << threshold << " bytes diverged";

        reader::SniiSegmentReader segment_reader;
        reader::LogicalIndexReader index_reader;
        assert_ok(reader::SniiSegmentReader::open(&pair_file, &segment_reader));
        assert_ok(segment_reader.open_index(61, "Body", &index_reader));
        format::DictEntry entry;
        if (threshold == kGateThreshold) {
            ASSERT_TRUE(lookup_term(index_reader, make_phrase_bigram_term("hot", "pair"), &entry));
            EXPECT_EQ(entry.df, 3U); // df == threshold SURVIVES, postings intact
        } else {
            EXPECT_FALSE(lookup_term(index_reader, make_phrase_bigram_term("hot", "pair"), &entry));
        }
        EXPECT_FALSE(lookup_term(index_reader, make_phrase_bigram_term("mid", "tier"), &entry));
        EXPECT_FALSE(lookup_term(index_reader, make_phrase_bigram_term("rare", "once"), &entry));
        ASSERT_TRUE(lookup_term(index_reader, make_phrase_bigram_sentinel_term(), &entry));
    }
}

// (3) EXACTNESS: an out-of-order docid REVISIT feed (5,1,5) makes Term::ndocs
// overcount the coalesced df (3 groups, 2 distinct docs), so the drain gate
// must NOT drop such terms -- they materialize and process_term gates the
// exact coalesced df. Both revisit shapes are pinned: one whose coalesced df
// is below the threshold (process_term prunes it) and one at/above it (it
// survives with coalesced postings) -- and the segment bytes still equal the
// string-keyed control's.
TEST(SniiSpimiBigramDrainDfGate, OutOfOrderRevisitFeedNeverDroppedAtDrain) {
    auto feed = [](SpimiTermBuffer* buf, auto&& feed_pair) {
        // Revisit-low: docids 5,1,5 -> ndocs 3, coalesced df 2 (< threshold 3).
        feed_pair(buf, 5, "twist", "back", 0);
        feed_pair(buf, 1, "twist", "back", 0);
        feed_pair(buf, 5, "twist", "back", 10); // revisit: pos past the doc-5 first visit
        // Revisit-high: docids 9,8,9,10 -> ndocs 4, coalesced df 3 (== threshold).
        feed_pair(buf, 9, "loop", "guard", 0);
        feed_pair(buf, 8, "loop", "guard", 0);
        feed_pair(buf, 9, "loop", "guard", 10);
        feed_pair(buf, 10, "loop", "guard", 0);
        buf->add_token(make_phrase_bigram_sentinel_term(), 0, 0);
    };

    // Buffer-level: with the gate armed, NEITHER revisit term is dropped at
    // the drain (unsorted terms are exempt), and the drop seam stays 0.
    {
        SpimiTermBuffer buf(/*has_positions=*/true);
        buf.configure_bigram_diet(0);
        feed(&buf, feed_pair_doc_ids);
        ASSERT_TRUE(buf.status().ok());
        buf.set_bigram_drain_min_df(kGateThreshold);
        spimi_testing::reset_bigram_drain_df_drops();
        const std::vector<TermPostings> got = drain_ordered(&buf);
        EXPECT_EQ(spimi_testing::bigram_drain_df_drops(), 0U);
        EXPECT_TRUE(stream_has_term(got, make_phrase_bigram_term("twist", "back")));
        EXPECT_TRUE(stream_has_term(got, make_phrase_bigram_term("loop", "guard")));
        for (const TermPostings& tp : got) {
            if (tp.term == make_phrase_bigram_term("twist", "back")) {
                // Coalesced exactly as process_term will see it: df 2.
                EXPECT_EQ(tp.docids, (std::vector<uint32_t> {1, 5}));
                EXPECT_EQ(tp.freqs, (std::vector<uint32_t> {1, 2}));
            }
        }
    }

    // Segment-level: process_term prunes revisit-low (exact df 2 < 3) and
    // keeps revisit-high (exact df 3); bytes equal the string-keyed control.
    SpimiTermBuffer pair_buf(/*has_positions=*/true);
    SpimiTermBuffer str_buf(/*has_positions=*/true);
    pair_buf.configure_bigram_diet(0);
    str_buf.configure_bigram_diet(0);
    feed(&pair_buf, feed_pair_doc_ids);
    feed(&str_buf, feed_pair_doc_strings);
    ASSERT_TRUE(pair_buf.status().ok());
    ASSERT_TRUE(str_buf.status().ok());

    MemoryFile pair_file;
    MemoryFile str_file;
    assert_ok(build_segment(&pair_file, &pair_buf, kGateThreshold, /*doc_count=*/11));
    assert_ok(build_segment(&str_file, &str_buf, kGateThreshold, /*doc_count=*/11));
    EXPECT_EQ(file_bytes(&pair_file), file_bytes(&str_file));

    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(reader::SniiSegmentReader::open(&pair_file, &segment_reader));
    assert_ok(segment_reader.open_index(61, "Body", &index_reader));
    format::DictEntry entry;
    EXPECT_FALSE(lookup_term(index_reader, make_phrase_bigram_term("twist", "back"), &entry));
    ASSERT_TRUE(lookup_term(index_reader, make_phrase_bigram_term("loop", "guard"), &entry));
    EXPECT_EQ(entry.df, 3U);
}

// (4) MID-FEED SPILL + FINAL DRAIN MIX: a df-in-[2,threshold) pair term is
// dropped AND BLOOMED at a mid-feed spill (a reappearance must die at flush);
// a df==1 pair term takes the unchanged G04 eviction; the survivor is
// materialized, written, and boundary-coalesced across runs; the residual
// (final) drain drops nothing bloomed; and the emitted stream equals a
// no-spill control that dropped the same terms at ITS final drain (unbloomed).
TEST(SniiSpimiBigramDrainDfGate, MidFeedSpillDropsBloomAndFinalDrainDropsDoNot) {
    spimi_testing::reset_bigram_vocab_cap_counters();
    // Spilled build: gate threshold delivered UP FRONT through the new
    // configure_bigram_diet parameter (what the production column writer does),
    // so mid-feed spill drains can gate before the flush value exists.
    SpimiTermBuffer spilled(/*has_positions=*/true, /*spill_threshold_bytes=*/40 * 1024);
    spilled.configure_bigram_diet(/*vocab_cap_bytes=*/64 * 1024, kGateThreshold);
    // Control: no spilling; same gate applied only at its final drain. The diet
    // must be ON (huge cap = never evicts) so bigram position suppression matches
    // the spilled buffer -- with the diet off the control keeps accumulating
    // bigram positions and the drained streams differ in positions_flat even
    // though the on-disk bytes (docs-only either way) agree.
    SpimiTermBuffer control(/*has_positions=*/true);
    control.configure_bigram_diet(/*vocab_cap_bytes=*/uint64_t(1) << 40);
    control.set_bigram_drain_min_df(kGateThreshold);

    spimi_testing::reset_bigram_drain_df_drops();
    for (SpimiTermBuffer* buf : {&spilled, &control}) {
        // Survivor: df 3 == threshold BEFORE the filler can trigger any spill.
        feed_pair_doc_ids(buf, 0, "hot", "pair");
        feed_pair_doc_ids(buf, 1, "hot", "pair");
        feed_pair_doc_ids(buf, 2, "hot", "pair");
        // df==2 victim: below the threshold -> the NEW mid-feed df-gate drop.
        feed_pair_doc_ids(buf, 3, "gone", "away");
        feed_pair_doc_ids(buf, 4, "gone", "away");
        // df==1 victim: the unchanged G04 eviction rule.
        feed_pair_doc_ids(buf, 5, "solo", "once");
        // ~40+ KiB of unigram chain bytes force at least one gate-2 spill in
        // the spilled build (established spill-test pattern).
        for (uint32_t d = 6; d < 10000; ++d) {
            buf->add_token("word", d, 0);
            buf->add_token("more", d, 1);
        }
        // Post-spill survivor occurrences: cross-run postings must coalesce.
        for (uint32_t d = 10000; d < 10005; ++d) {
            feed_pair_doc_ids(buf, d, "hot", "pair");
        }
        buf->add_token(make_phrase_bigram_sentinel_term(), 0, 0);
        ASSERT_TRUE(buf->status().ok());
    }
    ASSERT_GE(spilled.run_count_for_test(), 1U);
    ASSERT_EQ(control.run_count_for_test(), 0U);

    // Mid-feed drops BLOOMED both victims in the spilled build: the df==2 one
    // via the G06 gate (seam == 1: the df==1 one took the plain G04 eviction,
    // which the gate seam deliberately excludes).
    EXPECT_EQ(spimi_testing::bigram_drain_df_drops(), 1U);
    ASSERT_NE(spilled.bigram_dropped_filter(), nullptr);
    EXPECT_TRUE(spilled.bigram_dropped_filter()->maybe_contains(
            make_phrase_bigram_term("gone", "away")));
    EXPECT_TRUE(spilled.bigram_dropped_filter()->maybe_contains(
            make_phrase_bigram_term("solo", "once")));
    EXPECT_FALSE(spilled.bigram_dropped_filter()->maybe_contains(
            make_phrase_bigram_term("hot", "pair")));

    // Drain both; the spilled stream (k-way merged) equals the no-spill one.
    // for_each_term_sorted streams positions: a WIDE term (df >= kSlimDfThreshold;
    // the ~10k-doc filler unigrams here) arrives with positions_flat EMPTY and its
    // positions delivered ONLY through pos_pump, valid solely inside fn() (the
    // synchronous-consume-once contract on TermPostings::pos_pump). A retaining
    // collector must therefore materialize the pump INSIDE fn() -- which also pins
    // that the streamed positions equal the control's materialized ones.
    std::vector<TermPostings> got;
    assert_ok(spilled.for_each_term_sorted([&](TermPostings&& tp) {
        if (tp.pos_pump) {
            tp.positions_flat.resize(tp.pos_total);
            tp.pos_pump(tp.positions_flat.data(), tp.positions_flat.size());
            tp.pos_pump = nullptr;
            tp.pos_total = 0;
        }
        got.push_back(std::move(tp));
    }));
    ASSERT_TRUE(spilled.status().ok());
    spimi_testing::reset_bigram_drain_df_drops();
    const std::vector<TermPostings> want = drain_ordered(&control);
    // The control dropped BOTH victims at its FINAL drain through the gate
    // (the df==1 term is not "evicted" there -- eviction is mid-feed-only)...
    EXPECT_EQ(spimi_testing::bigram_drain_df_drops(), 2U);
    // ...and final-drain drops never bloom: the control never created a filter.
    EXPECT_EQ(control.bigram_dropped_filter(), nullptr);

    expect_same_stream(got, want, "spill mix");
    bool saw_hot = false;
    for (const TermPostings& tp : got) {
        EXPECT_NE(tp.term, make_phrase_bigram_term("gone", "away"));
        EXPECT_NE(tp.term, make_phrase_bigram_term("solo", "once"));
        if (tp.term == make_phrase_bigram_term("hot", "pair")) {
            saw_hot = true;
            std::vector<uint32_t> want_docs {0, 1, 2};
            for (uint32_t d = 10000; d < 10005; ++d) {
                want_docs.push_back(d);
            }
            EXPECT_EQ(tp.docids, want_docs); // pre- and post-spill runs coalesced
        }
    }
    EXPECT_TRUE(saw_hot);
}
