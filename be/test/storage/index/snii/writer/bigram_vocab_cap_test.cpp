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
#include <map>
#include <string>
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

// G04 "bigram diet" phase 2, buffer/writer level:
//   (a) the vocab cap evicts ONLY current-df==1 bigram terms (hot pairs and the
//       sentinel are untouchable) via the incremental sweep, keeping
//       bigram_intern_bytes() bounded by the cap;
//   (b) every survivor's postings EQUAL an uncapped control build;
//   (c) an evicted-then-reappearing pair re-interns fine but lands in the
//       ever-dropped bloom, so the flush drops it no matter how large its
//       re-accumulated df grows (the completeness invariant);
//   (d) position suppression: bigram tokens stop buffering positions entirely
//       (in-memory and across the spill/merge round trip) while unigrams keep
//       theirs.
using doris::Status;
using doris::snii::format::make_phrase_bigram_sentinel_term;
using doris::snii::format::make_phrase_bigram_term;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;
using namespace doris::snii;
using namespace doris::snii::snii_test;
namespace spimi_testing = doris::snii::writer::testing;

namespace {

// Deterministic distinct alpha-only pair for filler bigrams.
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

std::map<std::string, TermPostings> drain_to_map(SpimiTermBuffer* buf) {
    std::map<std::string, TermPostings> out;
    for (TermPostings& tp : buf->finalize_sorted()) {
        std::string key = tp.term;
        out.emplace(std::move(key), std::move(tp));
    }
    return out;
}

} // namespace

// (a)+(b): eviction fires once the cap is crossed, takes ONLY df==1 bigrams,
// keeps the intern storage bounded, and every survivor equals the uncapped
// control byte-for-byte.
TEST(SniiBigramVocabCap, EvictsOnlyDfOneTailAndSurvivorsEqualControl) {
    constexpr uint64_t kCap = 8 * 1024;
    spimi_testing::reset_bigram_vocab_cap_counters();

    SpimiTermBuffer capped(/*has_positions=*/true);
    SpimiTermBuffer control(/*has_positions=*/true);
    capped.configure_bigram_diet(kCap);
    control.configure_bigram_diet(0); // diet (docs-only bigrams) but NO cap
    ASSERT_TRUE(capped.status().ok());
    ASSERT_TRUE(control.status().ok());

    auto feed_both = [&](std::string_view l, std::string_view r, uint32_t docid) {
        capped.add_bigram_token(l, r, docid, 0);
        control.add_bigram_token(l, r, docid, 0);
    };

    // Hot pair reaches df==2 before any cap pressure exists -> never evictable.
    feed_both("hot", "pair", 0);
    feed_both("hot", "pair", 1);
    // 400 unique df==1 tail pairs blow the cap several times over.
    for (uint32_t i = 0; i < 400; ++i) {
        const auto [l, r] = filler_pair(i);
        feed_both(l, r, 2 + i);
        // The incremental sweep runs inside the add: the intern storage must
        // stay bounded by the cap plus a small hover margin at every step.
        EXPECT_LE(capped.bigram_intern_bytes(), kCap + 2048);
    }
    ASSERT_TRUE(capped.status().ok());

    EXPECT_GT(spimi_testing::bigram_evictions(), 0U);
    EXPECT_GT(spimi_testing::vocab_cap_sweeps(), 0U);
    EXPECT_LE(capped.bigram_intern_bytes(), kCap + 2048);
    // The uncapped control kept the whole tail resident.
    EXPECT_GT(control.bigram_intern_bytes(), 4 * kCap);

    const std::string hot_term = make_phrase_bigram_term("hot", "pair");
    // The hot pair was never evicted: not in the bloom (the filter exists,
    // evictions fired).
    ASSERT_NE(capped.bigram_dropped_filter(), nullptr);
    EXPECT_FALSE(capped.bigram_dropped_filter()->maybe_contains(hot_term));

    std::map<std::string, TermPostings> capped_terms = drain_to_map(&capped);
    std::map<std::string, TermPostings> control_terms = drain_to_map(&control);
    ASSERT_TRUE(capped.status().ok());
    ASSERT_TRUE(control.status().ok());

    // Eviction removed terms; it must never have touched a survivor's postings.
    EXPECT_LT(capped_terms.size(), control_terms.size());
    ASSERT_TRUE(capped_terms.contains(hot_term));
    EXPECT_EQ(capped_terms.at(hot_term).docids, (std::vector<uint32_t> {0, 1}));
    for (const auto& [term, tp] : capped_terms) {
        ASSERT_TRUE(control_terms.contains(term)) << term;
        const TermPostings& ct = control_terms.at(term);
        EXPECT_EQ(tp.docids, ct.docids) << term;
        EXPECT_EQ(tp.freqs, ct.freqs) << term;
        EXPECT_EQ(tp.positions_flat, ct.positions_flat) << term;
        // df==1-only rule: every EVICTED term had df 1, so anything with df >= 2
        // must still be here. Equivalently: every control term with df >= 2
        // survives in the capped build.
    }
    for (const auto& [term, tp] : control_terms) {
        if (tp.docids.size() >= 2) {
            EXPECT_TRUE(capped_terms.contains(term)) << "df>=2 term evicted: " << term;
        }
    }
}

// The sentinel (df==1 forever by construction) is never evicted regardless of
// cap pressure -- it gates reader semantics.
TEST(SniiBigramVocabCap, SentinelNeverEvicted) {
    constexpr uint64_t kCap = 2 * 1024;
    spimi_testing::reset_bigram_vocab_cap_counters();
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.configure_bigram_diet(kCap);

    const std::string sentinel = make_phrase_bigram_sentinel_term();
    buf.add_token(sentinel, 0, 0);
    for (uint32_t i = 0; i < 300; ++i) {
        const auto [l, r] = filler_pair(i);
        buf.add_bigram_token(l, r, 1 + i, 0);
    }
    ASSERT_TRUE(buf.status().ok());
    ASSERT_GT(spimi_testing::bigram_evictions(), 0U); // pressure definitely fired

    if (buf.bigram_dropped_filter() != nullptr) {
        EXPECT_FALSE(buf.bigram_dropped_filter()->maybe_contains(sentinel));
    }
    std::map<std::string, TermPostings> terms = drain_to_map(&buf);
    ASSERT_TRUE(terms.contains(sentinel));
    EXPECT_EQ(terms.at(sentinel).docids, (std::vector<uint32_t> {0}));
}

// (c): an evicted pair that REAPPEARS re-interns as a fresh term (its id space
// stays sane), but it is in the ever-dropped bloom, so the flush drops it even
// when its re-accumulated df is far above the threshold. Asserted end-to-end:
// the built segment has no dict entry for it, and the drop is attributed to
// the bloom seam (not the df threshold).
TEST(SniiBigramVocabCap, ReappearingEvictedPairDroppedAtFlushViaBloom) {
    constexpr uint64_t kCap = 2 * 1024;
    constexpr uint32_t kThreshold = 4;
    spimi_testing::reset_bigram_vocab_cap_counters();
    doris::snii::writer::testing::reset_bigram_prune_counters();

    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.configure_bigram_diet(kCap);

    // Hot survivor for sanity, df >= threshold before any pressure.
    for (uint32_t d = 0; d < 8; ++d) {
        buf.add_bigram_token("hot", "pair", d, 0);
    }
    // The victim pair: one occurrence -> df==1.
    buf.add_bigram_token("gone", "away", 10, 0);
    const std::string victim = make_phrase_bigram_term("gone", "away");

    // Blow the cap until the victim is provably evicted (the incremental sweep
    // walks the id space in cursor order; keep feeding unique tail pairs until
    // its cursor has consumed the victim). Bounded: fails loudly if it never
    // fires.
    uint32_t docid = 20;
    for (uint32_t i = 0; i < 4000; ++i) {
        if (buf.bigram_dropped_filter() != nullptr &&
            buf.bigram_dropped_filter()->maybe_contains(victim)) {
            break;
        }
        const auto [l, r] = filler_pair(i);
        buf.add_bigram_token(l, r, docid++, 0);
    }
    ASSERT_NE(buf.bigram_dropped_filter(), nullptr);
    ASSERT_TRUE(buf.bigram_dropped_filter()->maybe_contains(victim));
    const uint64_t evictions_after_pressure = spimi_testing::bigram_evictions();
    EXPECT_GE(evictions_after_pressure, 1U);

    // The pair REAPPEARS across many docs: re-interned (fresh term), df grows
    // far past the threshold. Churn (the sweep may evict early re-incarnations
    // again) is fine -- each re-eviction re-blooms it; the FINAL incarnation
    // reaches the flush with a large df.
    for (uint32_t d = 5000; d < 5200; ++d) {
        buf.add_bigram_token("gone", "away", d, 0);
    }
    buf.add_token(make_phrase_bigram_sentinel_term(), 0, 0);
    ASSERT_TRUE(buf.status().ok());

    // Flush through the real writer with the bloom wired (as production does).
    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 11;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 5200;
    input.bigram_prune_min_df = kThreshold;
    input.term_source = &buf;
    input.bigram_ever_dropped = buf.bigram_dropped_filter();

    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());

    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &index_reader));

    auto lookup = [&](const std::string& term, format::DictEntry* entry) {
        bool found = false;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(index_reader.lookup(term, &found, entry, &frq_base, &prx_base));
        return found;
    };

    format::DictEntry entry;
    // The victim's final df (~200) is far above the threshold, yet the bloom
    // dropped it: no dict entry, and the bloom seam (not the df seam) counted it.
    EXPECT_FALSE(lookup(victim, &entry));
    EXPECT_EQ(doris::snii::writer::testing::bigram_bloom_dropped(), 1U);
    // The hot pair materialized untouched, docs-only (G01 layout).
    ASSERT_TRUE(lookup(make_phrase_bigram_term("hot", "pair"), &entry));
    EXPECT_EQ(entry.df, 8U);
    EXPECT_EQ(entry.prx_len, 0U);
    EXPECT_TRUE(entry.prx_bytes.empty());
    ASSERT_TRUE(lookup(make_phrase_bigram_sentinel_term(), &entry));
    // The meta still declares the df threshold (the bloom rides its contract).
    EXPECT_EQ(index_reader.bigram_prune_min_df(), kThreshold);
}

// RSS proxy: a synthetic long-document feed (many unique pairs per doc) keeps
// the intern storage bounded by the cap at EVERY probe point.
TEST(SniiBigramVocabCap, InternBytesBoundedByCapOnSyntheticLongDocs) {
    constexpr uint64_t kCap = 16 * 1024;
    SpimiTermBuffer capped(/*has_positions=*/true);
    capped.configure_bigram_diet(kCap);

    uint32_t serial = 0;
    for (uint32_t d = 0; d < 100; ++d) {
        for (uint32_t k = 0; k < 50; ++k) {
            const auto [l, r] = filler_pair(serial++);
            capped.add_bigram_token(l, r, d, k);
        }
        // Probe once per "document": bounded by the cap plus the hover margin
        // (the sweep runs inside each add; one term's footprint of slack).
        ASSERT_LE(capped.bigram_intern_bytes(), kCap + 1024) << "doc " << d;
    }
    ASSERT_TRUE(capped.status().ok());
    EXPECT_GT(spimi_testing::bigram_evictions(), 0U);
    // 5000 uniques at ~100 B apiece would be ~500 KiB uncapped; the cap held.
    EXPECT_LE(capped.bigram_intern_bytes(), kCap + 1024);
}

// (d) in-memory: with the diet on, bigram tokens stop buffering positions
// entirely (empty positions_flat, freqs intact); unigrams are untouched.
// Legacy (no diet) keeps bigram positions byte-for-byte.
TEST(SniiBigramVocabCap, DietStopsBufferingBigramPositions) {
    SpimiTermBuffer diet(/*has_positions=*/true);
    diet.configure_bigram_diet(0); // suppression without any cap
    SpimiTermBuffer legacy(/*has_positions=*/true);

    for (uint32_t d = 0; d < 3; ++d) {
        for (SpimiTermBuffer* buf : {&diet, &legacy}) {
            buf->add_token("alpha", d, 5);
            buf->add_bigram_token("alpha", "beta", d, 6);
        }
    }
    ASSERT_TRUE(diet.status().ok());
    ASSERT_TRUE(legacy.status().ok());
    EXPECT_EQ(diet.total_tokens(), legacy.total_tokens());

    std::map<std::string, TermPostings> diet_terms = drain_to_map(&diet);
    std::map<std::string, TermPostings> legacy_terms = drain_to_map(&legacy);

    const std::string bigram = make_phrase_bigram_term("alpha", "beta");
    ASSERT_TRUE(diet_terms.contains(bigram));
    ASSERT_TRUE(legacy_terms.contains(bigram));
    // Same docids/freqs; the diet dropped ONLY the (never-emitted) positions.
    EXPECT_EQ(diet_terms.at(bigram).docids, legacy_terms.at(bigram).docids);
    EXPECT_EQ(diet_terms.at(bigram).freqs, legacy_terms.at(bigram).freqs);
    EXPECT_TRUE(diet_terms.at(bigram).positions_flat.empty());
    EXPECT_EQ(legacy_terms.at(bigram).positions_flat, (std::vector<uint32_t> {6, 6, 6}));
    // Unigrams keep positions under the diet.
    ASSERT_TRUE(diet_terms.contains("alpha"));
    EXPECT_EQ(diet_terms.at("alpha").positions_flat, (std::vector<uint32_t> {5, 5, 5}));
    EXPECT_EQ(diet_terms.at("alpha").docids, legacy_terms.at("alpha").docids);
}

// (d) out-of-core: suppressed bigram terms survive the spill/merge round trip
// -- runs serialize an empty position block per record, the k-way merge
// coalesces docids/freqs across runs (including the same-docid boundary case
// that would index an empty positions_flat without the per-term guard) and the
// merged term still carries no positions. Unigram positions are intact.
TEST(SniiBigramVocabCap, SuppressedBigramSurvivesSpillMergeRoundTrip) {
    // A spill threshold below the 32 KiB arena block size forces a spill on
    // (nearly) every token -- the established pattern from the spill tests --
    // so one doc's two bigram tokens land in DIFFERENT runs: the merge must
    // boundary-coalesce the same docid with EMPTY position blocks.
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/4096);
    buf.configure_bigram_diet(0);

    for (uint32_t d = 0; d < 10; ++d) {
        buf.add_token("word", d, 0);
        buf.add_bigram_token("aa", "bb", d, 0);
        buf.add_token("word", d, 1);
        buf.add_bigram_token("aa", "bb", d, 5);
    }
    ASSERT_TRUE(buf.status().ok());
    EXPECT_GE(buf.run_count_for_test(), 2U);

    // Retaining the TermPostings past the callback is safe here: every term is
    // tiny (ntok 20 and merged df 10), far below the pos_pump streaming gates,
    // so positions are always fully materialized.
    std::map<std::string, TermPostings> terms;
    assert_ok(buf.for_each_term_sorted([&](TermPostings&& tp) {
        std::string key = tp.term;
        terms.emplace(std::move(key), std::move(tp));
    }));

    const std::string bigram = make_phrase_bigram_term("aa", "bb");
    ASSERT_TRUE(terms.contains(bigram));
    const TermPostings& bt = terms.at(bigram);
    std::vector<uint32_t> want_docs(10);
    for (uint32_t d = 0; d < 10; ++d) {
        want_docs[d] = d;
    }
    EXPECT_EQ(bt.docids, want_docs);
    EXPECT_EQ(bt.freqs, std::vector<uint32_t>(10, 2)); // boundary-coalesced
    EXPECT_TRUE(bt.positions_flat.empty());

    ASSERT_TRUE(terms.contains("word"));
    const TermPostings& wt = terms.at("word");
    EXPECT_EQ(wt.docids, want_docs);
    EXPECT_EQ(wt.freqs, std::vector<uint32_t>(10, 2));
    std::vector<uint32_t> want_pos;
    for (uint32_t d = 0; d < 10; ++d) {
        want_pos.push_back(0);
        want_pos.push_back(1);
    }
    EXPECT_EQ(wt.positions_flat, want_pos);
}
