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

// G05: pair-keyed bigram interning. The production writer interns each unigram
// once (capturing its term-id) and feeds adjacent pairs to
// add_bigram_token(left_id, right_id, ...), which keys the hidden bigram term
// by the uint64 (left_id << 32 | right_id) pair key -- no composed-string
// hashing, comparing, or storage on the accumulation path. The composed
// on-disk term string materializes inside the buffer only at spill/flush.
// These tests pin:
//   (1) the pair path's drained stream (terms, ORDER, postings) and its on-disk
//       segment BYTES are identical to the G01 string-keyed path's;
//   (2) the pair-map hit/miss seams and the deferred-materialization seam;
//   (3) G04 cap/eviction/bloom semantics are preserved under pair keying,
//       including the bloom KEY contract (pair evictions are probe-able by the
//       composed term string at flush);
//   (4) the spill paths: df>=2 pair survivors are materialized at spill time
//       and round-trip through the k-way merge in lexicographic order; df==1
//       pair terms are evicted-not-spilled on mid-feed spills (G04 rule).
using doris::Status;
using doris::snii::format::make_phrase_bigram_sentinel_term;
using doris::snii::format::make_phrase_bigram_term;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;
using namespace doris::snii;
using namespace doris::snii::snii_test;
namespace spimi_testing = doris::snii::writer::testing;

namespace {

// Deterministic distinct alpha-only pair for filler bigrams (same generator as
// the G04 cap tests).
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

// The G01 string-keyed control feed for the identical logical stream.
void feed_pair_doc_strings(SpimiTermBuffer* buf, uint32_t docid, std::string_view l,
                           std::string_view r, uint32_t pos_base = 0) {
    buf->add_token(l, docid, pos_base);
    buf->add_token(r, docid, pos_base + 1);
    buf->add_bigram_token(l, r, docid, pos_base);
}

// Drains preserving EMISSION ORDER (the property under test: pair terms must
// interleave with unigrams exactly where their composed bytes sort).
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

// One shared logical stream: two unigram-only fillers, a hot df==3 pair, an
// ambiguous-concatenation pair set ("ab c" vs "a bc"), a repeat-in-doc pair
// (freq 2), a df==1 rare pair, and the sentinel fed LAST at docid 0 (as the
// production writer's finish() does).
template <class FeedPair>
void feed_shared_stream(SpimiTermBuffer* buf, FeedPair&& feed_pair) {
    feed_pair(buf, 0, "hot", "pair", 0);
    feed_pair(buf, 1, "hot", "pair", 0);
    buf->add_token("zonly", 1, 7); // unigram with no pair, positions exercised
    feed_pair(buf, 2, "ab", "c", 0);
    feed_pair(buf, 3, "a", "bc", 0);
    feed_pair(buf, 4, "hot", "pair", 0);
    // Same pair twice in one doc at ASCENDING positions -> freq 2, coalesced.
    feed_pair(buf, 5, "twin", "beat", 0);
    feed_pair(buf, 5, "twin", "beat", 2);
    feed_pair(buf, 6, "rare", "once", 0); // df==1: pruned at flush by any threshold >= 2
    buf->add_token(make_phrase_bigram_sentinel_term(), 0, 0);
}

} // namespace

// (1a) In-memory drained stream identical to the string-keyed control, in diet
// mode (docs-only bigrams, as production prune mode implies).
TEST(SniiSpimiBigramPairKey, DrainedStreamEqualsStringPathControlDietMode) {
    SpimiTermBuffer pair_buf(/*has_positions=*/true);
    SpimiTermBuffer str_buf(/*has_positions=*/true);
    pair_buf.configure_bigram_diet(0);
    str_buf.configure_bigram_diet(0);

    feed_shared_stream(&pair_buf, feed_pair_doc_ids);
    feed_shared_stream(&str_buf, feed_pair_doc_strings);
    ASSERT_TRUE(pair_buf.status().ok());
    ASSERT_TRUE(str_buf.status().ok());
    EXPECT_EQ(pair_buf.total_tokens(), str_buf.total_tokens());

    const std::vector<TermPostings> got = drain_ordered(&pair_buf);
    const std::vector<TermPostings> want = drain_ordered(&str_buf);
    expect_same_stream(got, want, "diet");
    // The pair terms really are in the stream (docs+freq only under the diet).
    bool saw_hot = false;
    for (const TermPostings& tp : got) {
        if (tp.term == make_phrase_bigram_term("hot", "pair")) {
            saw_hot = true;
            EXPECT_EQ(tp.docids, (std::vector<uint32_t> {0, 1, 4}));
            EXPECT_TRUE(tp.positions_flat.empty());
        }
    }
    EXPECT_TRUE(saw_hot);
}

// (1b) Legacy mode (no diet): bigram positions are preserved and still equal
// the string-keyed control byte for byte.
TEST(SniiSpimiBigramPairKey, DrainedStreamEqualsStringPathControlLegacyMode) {
    SpimiTermBuffer pair_buf(/*has_positions=*/true);
    SpimiTermBuffer str_buf(/*has_positions=*/true);

    feed_shared_stream(&pair_buf, feed_pair_doc_ids);
    feed_shared_stream(&str_buf, feed_pair_doc_strings);
    ASSERT_TRUE(pair_buf.status().ok());
    ASSERT_TRUE(str_buf.status().ok());

    const std::vector<TermPostings> got = drain_ordered(&pair_buf);
    const std::vector<TermPostings> want = drain_ordered(&str_buf);
    expect_same_stream(got, want, "legacy");
    for (const TermPostings& tp : got) {
        if (tp.term == make_phrase_bigram_term("twin", "beat")) {
            EXPECT_EQ(tp.freqs, (std::vector<uint32_t> {2}));
            EXPECT_FALSE(tp.positions_flat.empty()); // legacy keeps bigram positions
        }
    }
}

// (1c) The BUILT SEGMENT BYTES are identical between the two keying schemes for
// a fixed fixture -- the pair keying is purely an in-memory representation
// change; dict/posting bytes must not move.
TEST(SniiSpimiBigramPairKey, OnDiskBytesIdenticalToStringPathControl) {
    constexpr uint32_t kThreshold = 2; // prunes the df==1 "rare once" pair at flush
    auto build = [&](SpimiTermBuffer* buf, MemoryFile* file) {
        writer::SniiIndexInput input;
        input.index_id = 31;
        input.index_suffix = "Body";
        input.config = format::IndexConfig::kDocsPositions;
        input.doc_count = 7;
        input.bigram_prune_min_df = kThreshold;
        input.term_source = buf;
        input.bigram_ever_dropped = buf->bigram_dropped_filter(); // null: no evictions
        writer::SniiCompoundWriter writer(file);
        assert_ok(writer.add_logical_index(input));
        assert_ok(writer.finish());
    };

    SpimiTermBuffer pair_buf(/*has_positions=*/true);
    SpimiTermBuffer str_buf(/*has_positions=*/true);
    pair_buf.configure_bigram_diet(0);
    str_buf.configure_bigram_diet(0);
    feed_shared_stream(&pair_buf, feed_pair_doc_ids);
    feed_shared_stream(&str_buf, feed_pair_doc_strings);
    ASSERT_TRUE(pair_buf.status().ok());
    ASSERT_TRUE(str_buf.status().ok());

    MemoryFile pair_file;
    MemoryFile str_file;
    build(&pair_buf, &pair_file);
    build(&str_buf, &str_file);

    ASSERT_EQ(pair_file.size(), str_file.size());
    std::vector<uint8_t> pair_bytes;
    std::vector<uint8_t> str_bytes;
    assert_ok(pair_file.read_at(0, pair_file.size(), &pair_bytes));
    assert_ok(str_file.read_at(0, str_file.size(), &str_bytes));
    EXPECT_EQ(pair_bytes, str_bytes);
}

// (2a) Pair-map seams: one miss per DISTINCT pair, one hit per repeat.
TEST(SniiSpimiBigramPairKey, PairMapSeamsCountHitsAndMisses) {
    spimi_testing::reset_bigram_pair_map_counters();
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.configure_bigram_diet(0);

    const uint32_t a = buf.add_token_returning_id("alpha", 0, 0);
    const uint32_t b = buf.add_token_returning_id("beta", 0, 1);
    const uint32_t c = buf.add_token_returning_id("gamma", 0, 2);
    // Repeat unigram: same id back (unigram ids are stable).
    EXPECT_EQ(buf.add_token_returning_id("alpha", 1, 0), a);

    for (uint32_t d = 0; d < 5; ++d) {
        buf.add_bigram_token(a, b, d, 0);
    }
    buf.add_bigram_token(b, c, 9, 0);
    ASSERT_TRUE(buf.status().ok());

    EXPECT_EQ(spimi_testing::bigram_pair_map_misses(), 2U); // (a,b) and (b,c)
    EXPECT_EQ(spimi_testing::bigram_pair_map_hits(), 4U);   // 4 repeats of (a,b)
    EXPECT_EQ(buf.bigram_pair_terms_for_test(), 2U);
}

// (2b) NO bigram string is composed during accumulation; the composed strings
// materialize exactly once per live pair term, at drain.
TEST(SniiSpimiBigramPairKey, BigramStringsMaterializeOnlyAtDrain) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.configure_bigram_diet(0);

    spimi_testing::reset_vocab_string_materialization_count();
    const uint32_t l = buf.add_token_returning_id("left", 0, 0);
    const uint32_t r = buf.add_token_returning_id("right", 0, 1);
    EXPECT_EQ(spimi_testing::vocab_string_materialization_count(), 2U); // the unigrams

    for (uint32_t d = 0; d < 100; ++d) {
        buf.add_bigram_token(l, r, d, 0);
    }
    const auto [l2s, r2s] = filler_pair(7);
    const uint32_t l2 = buf.add_token_returning_id(l2s, 100, 0);
    const uint32_t r2 = buf.add_token_returning_id(r2s, 100, 1);
    buf.add_bigram_token(l2, r2, 100, 0);
    ASSERT_TRUE(buf.status().ok());
    // 100 pair adds of 2 distinct pairs composed NOTHING (only the 2 extra
    // unigram interns count).
    EXPECT_EQ(spimi_testing::vocab_string_materialization_count(), 4U);

    const std::vector<TermPostings> terms = drain_ordered(&buf);
    // Drain materialized exactly the 2 live pair terms' composed strings.
    EXPECT_EQ(spimi_testing::vocab_string_materialization_count(), 6U);
    bool saw = false;
    for (const TermPostings& tp : terms) {
        saw |= tp.term == make_phrase_bigram_term("left", "right");
    }
    EXPECT_TRUE(saw);
}

// (3) G04 cap/eviction/bloom preserved under pair keying, end-to-end through
// the real flush: only the df==1 tail is evicted (bounded intern bytes), the
// bloom is PROBE-ABLE BY THE COMPOSED TERM STRING (the pair eviction inserted
// the piecewise content hash -- the key contract), an evicted-then-reappearing
// pair is dropped at flush by the bloom, and hot/df>=2 pairs plus the sentinel
// are untouched.
TEST(SniiSpimiBigramPairKey, CapEvictionAndBloomPreservedUnderPairKeying) {
    constexpr uint64_t kCap = 2 * 1024;
    constexpr uint32_t kThreshold = 4;
    spimi_testing::reset_bigram_vocab_cap_counters();
    doris::snii::writer::testing::reset_bigram_prune_counters();

    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.configure_bigram_diet(kCap);

    // Hot survivor: df well past the threshold before any cap pressure.
    const uint32_t hot_l = buf.add_token_returning_id("hot", 0, 0);
    const uint32_t hot_r = buf.add_token_returning_id("pair", 0, 1);
    for (uint32_t d = 0; d < 8; ++d) {
        buf.add_bigram_token(hot_l, hot_r, d, 0);
    }
    // The victim pair: one occurrence -> df==1.
    const uint32_t gone = buf.add_token_returning_id("gone", 10, 0);
    const uint32_t away = buf.add_token_returning_id("away", 10, 1);
    buf.add_bigram_token(gone, away, 10, 0);
    const std::string victim = make_phrase_bigram_term("gone", "away");

    // Blow the cap with unique df==1 tail pairs until the victim is provably
    // evicted; the bloom probe uses the COMPOSED STRING, which must agree with
    // the piecewise pair-key insertion.
    uint32_t docid = 20;
    for (uint32_t i = 0; i < 4000; ++i) {
        if (buf.bigram_dropped_filter() != nullptr &&
            buf.bigram_dropped_filter()->maybe_contains(victim)) {
            break;
        }
        const auto [l, r] = filler_pair(i);
        feed_pair_doc_ids(&buf, docid++, l, r);
    }
    ASSERT_NE(buf.bigram_dropped_filter(), nullptr);
    ASSERT_TRUE(buf.bigram_dropped_filter()->maybe_contains(victim));
    EXPECT_GT(spimi_testing::bigram_evictions(), 0U);
    EXPECT_GT(spimi_testing::vocab_cap_sweeps(), 0U);
    // The cap bounded the pair-map intern accounting (fixed bytes/entry).
    EXPECT_LE(buf.bigram_intern_bytes(), kCap + 2048);
    // The hot pair was never evicted.
    EXPECT_FALSE(
            buf.bigram_dropped_filter()->maybe_contains(make_phrase_bigram_term("hot", "pair")));

    // The victim REAPPEARS far past the threshold; the bloom must still drop it
    // at flush (its earlier posting for doc 10 is gone -- incomplete).
    for (uint32_t d = 5000; d < 5200; ++d) {
        buf.add_bigram_token(gone, away, d, 0);
    }
    buf.add_token(make_phrase_bigram_sentinel_term(), 0, 0);
    ASSERT_TRUE(buf.status().ok());

    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 12;
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
    EXPECT_FALSE(lookup(victim, &entry));
    EXPECT_EQ(doris::snii::writer::testing::bigram_bloom_dropped(), 1U);
    ASSERT_TRUE(lookup(make_phrase_bigram_term("hot", "pair"), &entry));
    EXPECT_EQ(entry.df, 8U);
    EXPECT_EQ(entry.prx_len, 0U); // docs-only bigram (G01 layout)
    ASSERT_TRUE(lookup(make_phrase_bigram_sentinel_term(), &entry));
}

// (4a) Spill round trip with a pair SURVIVOR: df>=2 pair terms are materialized
// at spill time, written to runs by id, and the k-way merge re-emits the whole
// stream in lexicographic composed-string order with coalesced postings equal
// to a no-spill control.
TEST(SniiSpimiBigramPairKey, SpillWithPairSurvivorRoundTripMatchesNoSpillControl) {
    // Threshold below the 32 KiB arena block size -> a spill on (nearly) every
    // token (the established spill-test pattern), so the survivor's occurrences
    // land in DIFFERENT runs and must boundary-coalesce at merge.
    SpimiTermBuffer spilled(/*has_positions=*/true, /*spill_threshold_bytes=*/4096);
    SpimiTermBuffer control(/*has_positions=*/true);
    spilled.configure_bigram_diet(0); // suppression, NO cap: nothing is evicted
    control.configure_bigram_diet(0);

    for (SpimiTermBuffer* buf : {&spilled, &control}) {
        for (uint32_t d = 0; d < 10; ++d) {
            const uint32_t w = buf->add_token_returning_id("word", d, 0);
            ASSERT_NE(w, SpimiTermBuffer::kInvalidTermId);
            const uint32_t aa = buf->add_token_returning_id("aa", d, 1);
            const uint32_t bb = buf->add_token_returning_id("bb", d, 2);
            buf->add_bigram_token(aa, bb, d, 1);
            buf->add_bigram_token(aa, bb, d, 5); // same doc twice: freq 2
        }
        // One df==1 pair; with NO cap it must survive the spill unharmed.
        const uint32_t r1 = buf->add_token_returning_id("rare", 10, 0);
        const uint32_t r2 = buf->add_token_returning_id("once", 10, 1);
        buf->add_bigram_token(r1, r2, 10, 0);
        ASSERT_TRUE(buf->status().ok());
    }
    EXPECT_GE(spilled.run_count_for_test(), 2U);
    EXPECT_EQ(control.run_count_for_test(), 0U);

    std::vector<TermPostings> got;
    assert_ok(
            spilled.for_each_term_sorted([&](TermPostings&& tp) { got.push_back(std::move(tp)); }));
    const std::vector<TermPostings> want = drain_ordered(&control);
    expect_same_stream(got, want, "spill");

    // Explicit order pin: the emission is sorted by the FINAL composed bytes
    // (marker byte 0x1F sorts the hidden bigrams before the ASCII unigrams).
    for (size_t i = 1; i < got.size(); ++i) {
        EXPECT_LT(got[i - 1].term, got[i].term) << "emission order not lexicographic at " << i;
    }
    bool saw_pair = false;
    for (const TermPostings& tp : got) {
        if (tp.term == make_phrase_bigram_term("aa", "bb")) {
            saw_pair = true;
            std::vector<uint32_t> want_docs(10);
            for (uint32_t d = 0; d < 10; ++d) {
                want_docs[d] = d;
            }
            EXPECT_EQ(tp.docids, want_docs);
            EXPECT_EQ(tp.freqs, std::vector<uint32_t>(10, 2)); // boundary-coalesced
            EXPECT_TRUE(tp.positions_flat.empty());            // diet suppression
        }
    }
    EXPECT_TRUE(saw_pair);
}

// (4b) Mid-feed spills EVICT df==1 pair terms instead of spilling them (they
// would pin their id + a composed string in the run), recording them in the
// bloom; df>=2 pair terms are materialized and written. Same G04 rule as the
// string path.
TEST(SniiSpimiBigramPairKey, MidFeedSpillEvictsDfOnePairsInsteadOfSpilling) {
    spimi_testing::reset_bigram_vocab_cap_counters();
    // Threshold ABOVE one 32 KiB arena block but below two: the first spill
    // fires only once the chain bytes claim a second block, i.e. AFTER the hot
    // pair below has reached df==2 (df==1 at spill time would evict it).
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/40 * 1024);
    buf.configure_bigram_diet(64 * 1024); // eviction enabled; cap never binding

    // Survivor first: df==2 long before the first spill can consider it.
    const uint32_t hl = buf.add_token_returning_id("hot", 0, 0);
    const uint32_t hr = buf.add_token_returning_id("pair", 0, 1);
    buf.add_bigram_token(hl, hr, 0, 0);
    buf.add_bigram_token(hl, hr, 1, 0);
    // df==1 victim, then enough unigram tokens (~40 KiB of chain bytes) to
    // force at least one gate-2 spill past it.
    const uint32_t vl = buf.add_token_returning_id("gone", 2, 0);
    const uint32_t vr = buf.add_token_returning_id("away", 2, 1);
    buf.add_bigram_token(vl, vr, 2, 0);
    for (uint32_t d = 3; d < 10000; ++d) {
        buf.add_token("word", d, 0);
        buf.add_token("more", d, 1);
    }
    ASSERT_TRUE(buf.status().ok());
    ASSERT_GE(buf.run_count_for_test(), 1U);

    const std::string victim = make_phrase_bigram_term("gone", "away");
    ASSERT_NE(buf.bigram_dropped_filter(), nullptr);
    EXPECT_TRUE(buf.bigram_dropped_filter()->maybe_contains(victim));
    EXPECT_GE(spimi_testing::bigram_evictions(), 1U);

    std::vector<TermPostings> got;
    assert_ok(buf.for_each_term_sorted([&](TermPostings&& tp) { got.push_back(std::move(tp)); }));
    bool saw_hot = false;
    for (size_t i = 0; i < got.size(); ++i) {
        if (i > 0) {
            EXPECT_LT(got[i - 1].term, got[i].term);
        }
        EXPECT_NE(got[i].term, victim); // evicted, never spilled
        if (got[i].term == make_phrase_bigram_term("hot", "pair")) {
            saw_hot = true;
            EXPECT_EQ(got[i].docids, (std::vector<uint32_t> {0, 1}));
        }
    }
    EXPECT_TRUE(saw_hot);
}

// (5) Contract violations latch InvalidArgument and drop the token: borrowed
// vocab mode, out-of-range ids, and a pair-term id as a constituent.
TEST(SniiSpimiBigramPairKey, InvalidInputsLatchStatus) {
    {
        const std::vector<std::string> vocab {"a", "b"};
        SpimiTermBuffer borrowed(&vocab, /*has_positions=*/true);
        borrowed.add_bigram_token(0U, 1U, 0, 0);
        EXPECT_FALSE(borrowed.status().ok());
        EXPECT_EQ(borrowed.total_tokens(), 0U);
    }
    {
        SpimiTermBuffer buf(/*has_positions=*/true);
        const uint32_t a = buf.add_token_returning_id("a", 0, 0);
        buf.add_bigram_token(a, a + 100, 0, 0); // right id never interned
        EXPECT_FALSE(buf.status().ok());
    }
    {
        SpimiTermBuffer buf(/*has_positions=*/true);
        const uint32_t a = buf.add_token_returning_id("aa", 0, 0);
        const uint32_t b = buf.add_token_returning_id("bb", 0, 1);
        buf.add_bigram_token(a, b, 0, 0);
        ASSERT_TRUE(buf.status().ok());
        // The pair term's own id is the next fresh id (a, b, pair): using it as
        // a constituent must be rejected.
        const uint32_t pair_id = b + 1;
        buf.add_bigram_token(pair_id, a, 1, 0);
        EXPECT_FALSE(buf.status().ok());
    }
    {
        // add_token_returning_id on a borrowed-vocab buffer: kInvalidTermId.
        const std::vector<std::string> vocab {"a"};
        SpimiTermBuffer borrowed(&vocab, /*has_positions=*/true);
        EXPECT_EQ(borrowed.add_token_returning_id("a", 0, 0), SpimiTermBuffer::kInvalidTermId);
        EXPECT_FALSE(borrowed.status().ok());
    }
}
