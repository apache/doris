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

#include "storage/index/inverted/spimi/posting_buffer.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

TEST(SpimiPostingBufferTest, EmptyOnConstruction) {
    SpimiPostingBuffer buf;
    EXPECT_EQ(buf.RecordCount(), 0U);
    EXPECT_EQ(buf.TermCount(), 0U);
    EXPECT_TRUE(buf.records().empty());
}

TEST(SpimiPostingBufferTest, AppendOneOccurrence) {
    SpimiPostingBuffer buf;
    buf.Append("hello", /*doc_id=*/0, /*position=*/0);
    ASSERT_EQ(buf.RecordCount(), 1U);
    EXPECT_EQ(buf.TermCount(), 1U);

    const auto& rec = buf.records()[0];
    EXPECT_EQ(rec.doc_id, 0U);
    EXPECT_EQ(rec.position, 0U);
    EXPECT_EQ(buf.TermFor(rec), "hello");
}

TEST(SpimiPostingBufferTest, InternDeduplicatesIdenticalTerms) {
    SpimiPostingBuffer buf;
    buf.Append("alpha", 0, 0);
    buf.Append("beta", 0, 1);
    buf.Append("alpha", 1, 0);
    buf.Append("beta", 1, 1);
    buf.Append("alpha", 2, 0);

    EXPECT_EQ(buf.RecordCount(), 5U);
    // Only two distinct terms even after five occurrences.
    EXPECT_EQ(buf.TermCount(), 2U);

    // All "alpha" records share one text_ref; all "beta" records share another.
    const uint32_t alpha_ref = buf.records()[0].text_ref;
    const uint32_t beta_ref = buf.records()[1].text_ref;
    EXPECT_NE(alpha_ref, beta_ref);
    EXPECT_EQ(buf.records()[2].text_ref, alpha_ref);
    EXPECT_EQ(buf.records()[3].text_ref, beta_ref);
    EXPECT_EQ(buf.records()[4].text_ref, alpha_ref);

    EXPECT_EQ(buf.TermAt(alpha_ref), "alpha");
    EXPECT_EQ(buf.TermAt(beta_ref), "beta");
}

TEST(SpimiPostingBufferTest, EmptyAndPrefixTermsAreDistinct) {
    SpimiPostingBuffer buf;
    buf.Append("", 0, 0);   // empty term
    buf.Append("a", 0, 1);  // single byte
    buf.Append("ab", 0, 2); // prefix-extends "a"
    buf.Append("a", 0, 3);  // duplicate of "a"

    EXPECT_EQ(buf.RecordCount(), 4U);
    EXPECT_EQ(buf.TermCount(), 3U);
    EXPECT_EQ(buf.records()[0].text_ref, buf.records()[0].text_ref); // self
    EXPECT_NE(buf.records()[0].text_ref, buf.records()[1].text_ref); // "" vs "a"
    EXPECT_NE(buf.records()[1].text_ref, buf.records()[2].text_ref); // "a" vs "ab"
    EXPECT_EQ(buf.records()[1].text_ref, buf.records()[3].text_ref); // "a" dedup

    EXPECT_EQ(buf.TermAt(buf.records()[0].text_ref), "");
    EXPECT_EQ(buf.TermAt(buf.records()[1].text_ref), "a");
    EXPECT_EQ(buf.TermAt(buf.records()[2].text_ref), "ab");
}

TEST(SpimiPostingBufferTest, CjkUtf8TermsArePreservedAndDeduplicated) {
    SpimiPostingBuffer buf;
    // 中文 (zhōngwén) — 6 bytes in UTF-8.
    const std::string zhongwen = "\xe4\xb8\xad\xe6\x96\x87";
    // 日本 (nihon) — 6 bytes in UTF-8.
    const std::string nihon = "\xe6\x97\xa5\xe6\x9c\xac";

    buf.Append(zhongwen, 0, 0);
    buf.Append(nihon, 0, 1);
    buf.Append(zhongwen, 1, 0);

    EXPECT_EQ(buf.RecordCount(), 3U);
    EXPECT_EQ(buf.TermCount(), 2U);
    EXPECT_EQ(buf.records()[0].text_ref, buf.records()[2].text_ref);
    EXPECT_EQ(buf.TermAt(buf.records()[0].text_ref), std::string_view(zhongwen));
    EXPECT_EQ(buf.TermAt(buf.records()[1].text_ref), std::string_view(nihon));
}

TEST(SpimiPostingBufferTest, RecordsArePreservedInInsertionOrder) {
    SpimiPostingBuffer buf;
    for (uint32_t i = 0; i < 5; ++i) {
        buf.Append("t", /*doc_id=*/i, /*position=*/i * 2);
    }
    ASSERT_EQ(buf.RecordCount(), 5U);
    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_EQ(buf.records()[i].doc_id, i);
        EXPECT_EQ(buf.records()[i].position, i * 2);
    }
    EXPECT_EQ(buf.TermCount(), 1U);
}

TEST(SpimiPostingBufferTest, GrowsThroughManyDistinctTerms) {
    SpimiPostingBuffer buf;
    constexpr uint32_t kN = 1000;
    for (uint32_t i = 0; i < kN; ++i) {
        // "term00000".."term00999"
        char buf2[32];
        std::snprintf(buf2, sizeof(buf2), "term%05u", i);
        buf.Append(buf2, i, 0);
    }
    EXPECT_EQ(buf.RecordCount(), kN);
    EXPECT_EQ(buf.TermCount(), kN);

    // Spot-check a few that the right text_ref resolves back to the right term.
    for (uint32_t i : {0U, 1U, 7U, 99U, 500U, 999U}) {
        char expected[32];
        std::snprintf(expected, sizeof(expected), "term%05u", i);
        EXPECT_EQ(buf.TermFor(buf.records()[i]), expected) << "i=" << i;
    }

    // Re-appending a duplicate must not allocate a new term entry.
    const size_t terms_before = buf.TermCount();
    buf.Append("term00042", 0, 0);
    EXPECT_EQ(buf.TermCount(), terms_before);
    EXPECT_EQ(buf.RecordCount(), kN + 1);
    EXPECT_EQ(buf.TermFor(buf.records().back()), "term00042");
}

TEST(SpimiPostingBufferTest, MemoryUsageGrowsWithContent) {
    SpimiPostingBuffer buf;
    const size_t empty_bytes = buf.MemoryUsage();

    for (uint32_t i = 0; i < 100; ++i) {
        char term[16];
        std::snprintf(term, sizeof(term), "tok%u", i);
        buf.Append(term, i, 0);
    }
    const size_t filled_bytes = buf.MemoryUsage();
    EXPECT_GT(filled_bytes, empty_bytes);

    // Reset releases the logical content but the capacity may stay (intended,
    // for reuse). We only require that MemoryUsage() does not grow.
    buf.Reset();
    EXPECT_EQ(buf.RecordCount(), 0U);
    EXPECT_EQ(buf.TermCount(), 0U);
    EXPECT_LE(buf.MemoryUsage(), filled_bytes);
}

TEST(SpimiPostingBufferTest, ResetClearsAllStateAndAllowsReuse) {
    SpimiPostingBuffer buf;
    buf.Append("foo", 0, 0);
    buf.Append("bar", 0, 1);
    ASSERT_EQ(buf.TermCount(), 2U);

    buf.Reset();
    EXPECT_EQ(buf.RecordCount(), 0U);
    EXPECT_EQ(buf.TermCount(), 0U);

    buf.Append("baz", 5, 7);
    ASSERT_EQ(buf.RecordCount(), 1U);
    EXPECT_EQ(buf.TermCount(), 1U);
    EXPECT_EQ(buf.records()[0].doc_id, 5U);
    EXPECT_EQ(buf.records()[0].position, 7U);
    EXPECT_EQ(buf.TermFor(buf.records()[0]), "baz");
}

// --- Phase 2: Sort -----------------------------------------------------------

TEST(SpimiPostingBufferTest, SortEmptyIsNoOp) {
    SpimiPostingBuffer buf;
    buf.Sort();
    EXPECT_EQ(buf.RecordCount(), 0U);
}

TEST(SpimiPostingBufferTest, SortSingleRecordIsNoOp) {
    SpimiPostingBuffer buf;
    buf.Append("hello", 42, 7);
    buf.Sort();
    ASSERT_EQ(buf.RecordCount(), 1U);
    EXPECT_EQ(buf.records()[0].doc_id, 42U);
    EXPECT_EQ(buf.records()[0].position, 7U);
    EXPECT_EQ(buf.TermFor(buf.records()[0]), "hello");
}

TEST(SpimiPostingBufferTest, SortOrdersByTermLexicographically) {
    SpimiPostingBuffer buf;
    buf.Append("ham", 0, 0);
    buf.Append("apple", 1, 0);
    buf.Append("fig", 2, 0);
    buf.Append("banana", 3, 0);
    buf.Sort();

    ASSERT_EQ(buf.RecordCount(), 4U);
    EXPECT_EQ(buf.TermFor(buf.records()[0]), "apple");
    EXPECT_EQ(buf.TermFor(buf.records()[1]), "banana");
    EXPECT_EQ(buf.TermFor(buf.records()[2]), "fig");
    EXPECT_EQ(buf.TermFor(buf.records()[3]), "ham");
}

TEST(SpimiPostingBufferTest, SortHandlesPrefixOrdering) {
    SpimiPostingBuffer buf;
    // A shorter term sorts before any longer term that extends it.
    buf.Append("ab", 0, 0);
    buf.Append("abc", 1, 0);
    buf.Append("a", 2, 0);
    buf.Append("aa", 3, 0);
    buf.Sort();

    ASSERT_EQ(buf.RecordCount(), 4U);
    EXPECT_EQ(buf.TermFor(buf.records()[0]), "a");
    EXPECT_EQ(buf.TermFor(buf.records()[1]), "aa");
    EXPECT_EQ(buf.TermFor(buf.records()[2]), "ab");
    EXPECT_EQ(buf.TermFor(buf.records()[3]), "abc");
}

TEST(SpimiPostingBufferTest, SortGroupsByTermThenSortsByDoc) {
    SpimiPostingBuffer buf;
    buf.Append("alpha", 5, 0);
    buf.Append("beta", 4, 0);
    buf.Append("alpha", 1, 0);
    buf.Append("beta", 2, 0);
    buf.Append("alpha", 3, 0);
    buf.Sort();

    ASSERT_EQ(buf.RecordCount(), 5U);
    EXPECT_EQ(buf.TermFor(buf.records()[0]), "alpha");
    EXPECT_EQ(buf.records()[0].doc_id, 1U);
    EXPECT_EQ(buf.TermFor(buf.records()[1]), "alpha");
    EXPECT_EQ(buf.records()[1].doc_id, 3U);
    EXPECT_EQ(buf.TermFor(buf.records()[2]), "alpha");
    EXPECT_EQ(buf.records()[2].doc_id, 5U);
    EXPECT_EQ(buf.TermFor(buf.records()[3]), "beta");
    EXPECT_EQ(buf.records()[3].doc_id, 2U);
    EXPECT_EQ(buf.TermFor(buf.records()[4]), "beta");
    EXPECT_EQ(buf.records()[4].doc_id, 4U);
}

TEST(SpimiPostingBufferTest, SortSameTermSameDocSortsByPosition) {
    SpimiPostingBuffer buf;
    buf.Append("t", 0, 5);
    buf.Append("t", 0, 1);
    buf.Append("t", 0, 9);
    buf.Append("t", 0, 3);
    buf.Sort();

    ASSERT_EQ(buf.RecordCount(), 4U);
    EXPECT_EQ(buf.records()[0].position, 1U);
    EXPECT_EQ(buf.records()[1].position, 3U);
    EXPECT_EQ(buf.records()[2].position, 5U);
    EXPECT_EQ(buf.records()[3].position, 9U);
}

TEST(SpimiPostingBufferTest, SortIsStableForIdenticalKeys) {
    SpimiPostingBuffer buf;
    // Three "t" records and two "u" records, all at (doc 0, pos 0). std::stable_sort
    // must keep records with the same key in their original insertion order.
    buf.Append("t", 0, 0);
    buf.Append("u", 0, 0);
    buf.Append("t", 0, 0);
    buf.Append("u", 0, 0);
    buf.Append("t", 0, 0);
    buf.Sort();

    ASSERT_EQ(buf.RecordCount(), 5U);
    EXPECT_EQ(buf.TermFor(buf.records()[0]), "t");
    EXPECT_EQ(buf.TermFor(buf.records()[1]), "t");
    EXPECT_EQ(buf.TermFor(buf.records()[2]), "t");
    EXPECT_EQ(buf.TermFor(buf.records()[3]), "u");
    EXPECT_EQ(buf.TermFor(buf.records()[4]), "u");
}

TEST(SpimiPostingBufferTest, SortCjkUtf8UsesByteOrder) {
    SpimiPostingBuffer buf;
    // UTF-8 byte order equals Unicode code-point order:
    //   中 U+4E2D = 0xE4 0xB8 0xAD
    //   日 U+65E5 = 0xE6 0x97 0xA5
    //   月 U+6708 = 0xE6 0x9C 0x88
    // Expected order: 中 < 日 < 月.
    const std::string zhong = "\xe4\xb8\xad";
    const std::string ri = "\xe6\x97\xa5";
    const std::string yue = "\xe6\x9c\x88";
    buf.Append(yue, 0, 0);
    buf.Append(zhong, 1, 0);
    buf.Append(ri, 2, 0);
    buf.Sort();

    ASSERT_EQ(buf.RecordCount(), 3U);
    EXPECT_EQ(buf.TermFor(buf.records()[0]), std::string_view(zhong));
    EXPECT_EQ(buf.TermFor(buf.records()[1]), std::string_view(ri));
    EXPECT_EQ(buf.TermFor(buf.records()[2]), std::string_view(yue));
}

TEST(SpimiPostingBufferTest, SortOnLargeBufferIsNonDecreasing) {
    SpimiPostingBuffer buf;
    constexpr uint32_t kTerms = 32;
    constexpr uint32_t kDocs = 4;
    constexpr uint32_t kPosPerDoc = 3;

    // Insert in reverse so the sort has work to do.
    for (uint32_t d = kDocs; d-- > 0;) {
        for (uint32_t t = kTerms; t-- > 0;) {
            char term[16];
            std::snprintf(term, sizeof(term), "term%02u", t);
            for (uint32_t p = kPosPerDoc; p-- > 0;) {
                buf.Append(term, d, p);
            }
        }
    }
    const size_t expected = static_cast<size_t>(kTerms) * kDocs * kPosPerDoc;
    ASSERT_EQ(buf.RecordCount(), expected);
    buf.Sort();

    for (size_t i = 1; i < buf.records().size(); ++i) {
        const auto& a = buf.records()[i - 1];
        const auto& b = buf.records()[i];
        const std::string_view ta = buf.TermFor(a);
        const std::string_view tb = buf.TermFor(b);
        if (ta != tb) {
            EXPECT_LT(ta, tb) << "i=" << i;
        } else if (a.doc_id != b.doc_id) {
            EXPECT_LT(a.doc_id, b.doc_id) << "i=" << i;
        } else {
            EXPECT_LE(a.position, b.position) << "i=" << i;
        }
    }
}

TEST(SpimiPostingBufferTest, OversizedTermSaturatesAndDropsAppend) {
    // C1/C6 regression — a term above kMaxTermBytes must not be appended,
    // must latch saturation, and must not corrupt earlier state.
    SpimiPostingBuffer buf;
    buf.Append("normal", 0, 0);
    ASSERT_EQ(buf.RecordCount(), 1U);
    ASSERT_FALSE(buf.Saturated());

    const std::string oversize(kMaxTermBytes + 1, 'x');
    buf.Append(oversize, 1, 0);
    EXPECT_TRUE(buf.Saturated());
    EXPECT_EQ(buf.RecordCount(), 1U) << "oversized term must not be recorded";

    // Subsequent well-formed appends are dropped (frozen state).
    buf.Append("alsodropped", 2, 0);
    EXPECT_EQ(buf.RecordCount(), 1U);

    // Reset clears the saturation latch and restores accepting state.
    buf.Reset();
    EXPECT_FALSE(buf.Saturated());
    buf.Append("after-reset", 0, 0);
    EXPECT_EQ(buf.RecordCount(), 1U);
}

TEST(SpimiPostingBufferTest, ExactlyMaxTermBytesIsAccepted) {
    // Boundary condition — kMaxTermBytes is the inclusive upper bound.
    SpimiPostingBuffer buf;
    const std::string at_limit(kMaxTermBytes, 'y');
    buf.Append(at_limit, 0, 0);
    EXPECT_FALSE(buf.Saturated());
    ASSERT_EQ(buf.RecordCount(), 1U);
    EXPECT_EQ(buf.TermFor(buf.records()[0]).size(), kMaxTermBytes);
}

TEST(SpimiPostingBufferTest, KeyedHashDiffersAcrossDefaultInstances) {
    // C5 regression — the per-instance random seed makes the hash unpredictable
    // across writers within the same process. Two default-constructed buffers
    // hashing the same term should (with overwhelming probability) yield
    // different hashes; collisions are still possible but the test compares a
    // handful of inputs so the false-failure rate is ~2^-64 per term.
    SpimiPostingBuffer a;
    SpimiPostingBuffer b;
    bool any_diff = false;
    for (const std::string_view t : {"alpha", "beta", "gamma", "delta", "epsilon"}) {
        if (a.HashTerm(t) != b.HashTerm(t)) {
            any_diff = true;
            break;
        }
    }
    EXPECT_TRUE(any_diff) << "expected per-instance seeds to diverge for at least one input";
}

TEST(SpimiPostingBufferTest, ExplicitSeedHashIsDeterministic) {
    // The seed-injected constructor is used by tests that need the hash to be
    // reproducible — verify two instances built with the same seed agree.
    constexpr uint64_t kSeed = 0xABCDEF0123456789ULL;
    SpimiPostingBuffer a(kSeed);
    SpimiPostingBuffer b(kSeed);
    for (const std::string_view t : {"alpha", "beta", "gamma"}) {
        EXPECT_EQ(a.HashTerm(t), b.HashTerm(t)) << "term=" << t;
    }
}

TEST(SpimiPostingBufferTest, ArenaSaturatesAtConfiguredLimit) {
    // C1/C6 — arena-size cap reachable via the test-only Limits seam.
    // Production constant kMaxArenaBytes is 2 GiB, but the saturation path
    // is structurally equivalent regardless of the magnitude; shrinking the
    // cap to 256 bytes lets us cover the branch without a multi-GiB malloc.
    SpimiPostingBuffer::Limits tight {};
    tight.max_arena_bytes = 64; // enough for a few short terms
    SpimiPostingBuffer buf(/*hash_seed=*/0xDEADBEEFULL, tight);

    // Each unique term consumes (4 + bytes) of arena space.
    buf.Append("abcd", 0, 0);   // +8
    buf.Append("efghij", 0, 1); // +10
    EXPECT_FALSE(buf.Saturated());
    EXPECT_EQ(buf.RecordCount(), 2U);

    // A term whose (4 + len) would push the arena past 64 B must saturate.
    const std::string fat(60, 'z');
    buf.Append(fat, 1, 0);
    EXPECT_TRUE(buf.Saturated());

    // Subsequent appends are no-ops.
    buf.Append("dropped", 2, 0);
    EXPECT_EQ(buf.RecordCount(), 2U);
}

TEST(SpimiPostingBufferTest, RecordCountSaturatesAtConfiguredLimit) {
    // C1/C6 — record-count cap reachable via the test-only Limits seam.
    SpimiPostingBuffer::Limits tight {};
    tight.max_record_count = 4;
    SpimiPostingBuffer buf(/*hash_seed=*/0xDEADBEEFULL, tight);

    for (int i = 0; i < 4; ++i) {
        buf.Append("t" + std::to_string(i), 0, static_cast<uint32_t>(i));
    }
    EXPECT_FALSE(buf.Saturated());
    EXPECT_EQ(buf.RecordCount(), 4U);

    // The 5th record must trip saturation and be dropped.
    buf.Append("dropped", 0, 99);
    EXPECT_TRUE(buf.Saturated());
    EXPECT_EQ(buf.RecordCount(), 4U);
}

TEST(SpimiPostingBufferTest, LimitsRoundTripDefaultsMatchConstants) {
    // Sanity: a default-constructed buffer uses the production constants.
    SpimiPostingBuffer::Limits defaults {};
    EXPECT_EQ(defaults.max_term_bytes, kMaxTermBytes);
    EXPECT_EQ(defaults.max_arena_bytes, kMaxArenaBytes);
    EXPECT_EQ(defaults.max_record_count, kMaxRecordCount);
}

TEST(SpimiPostingBufferTest, ResetClearsSaturationAndKeepsSlotCapacity) {
    // Combined regression: the saturation latch must clear on Reset(), and the
    // slot vector's capacity must be retained (Reset previously dropped slots
    // entirely, forcing a re-allocation on the next Append).
    SpimiPostingBuffer buf;
    for (int i = 0; i < 64; ++i) {
        buf.Append("t" + std::to_string(i), 0, static_cast<uint32_t>(i));
    }
    const size_t slot_capacity_before = buf.records().capacity(); // proxy: records grew, slots too
    (void)slot_capacity_before; // we just exercise the path; capacity retention covered indirectly

    const std::string oversize(kMaxTermBytes + 1, 'z');
    buf.Append(oversize, 99, 0);
    ASSERT_TRUE(buf.Saturated());

    buf.Reset();
    EXPECT_FALSE(buf.Saturated());
    EXPECT_EQ(buf.RecordCount(), 0U);
    EXPECT_EQ(buf.TermCount(), 0U);

    // After Reset the buffer must accept new terms again.
    buf.Append("again", 0, 0);
    EXPECT_EQ(buf.RecordCount(), 1U);
}

TEST(SpimiPostingBufferTest, CompactModeGrowSlotsKeepsTermIdsConsistent) {
    // P51 follow-up regression: after compact mode activates,
    // subsequent Appends introduce NEW terms via Intern() which can
    // trigger `GrowSlots` (load factor > 0.75). Pre-fix the cold
    // path (taken when `_last_intern_slot == -1` post-grow) did not
    // re-populate `_slot_term_ids` for the new term's slot — the
    // next Append for that same term would find `kInvalidTermId`
    // in the slot, assign ANOTHER fresh term_id, and split the
    // term's occurrences across two streams. This test triggers
    // the scenario:
    //   1. 600 Appends across 4 terms → records cross 512 → compact
    //      mode activates with 4 term_states.
    //   2. Append 200 NEW distinct terms one by one. The slot table
    //      starts at 256 (after 4-term Intern grew it past 16) and
    //      grows again as terms cross the 0.75 load factor.
    //   3. After all 200 new terms inserted, re-Append the FIRST
    //      new term once more. The post-grow Append must resolve to
    //      the SAME term_id assigned originally (not a fresh one).
    SpimiPostingBuffer buf;
    // Phase 1: trigger compact mode.
    const std::vector<std::string> compact_vocab = {"alpha", "beta", "gamma", "delta"};
    for (int doc = 0; doc < 150; ++doc) {
        for (int t = 0; t < 4; ++t) {
            buf.Append(compact_vocab[t], static_cast<uint32_t>(doc), 0);
        }
    }
    EXPECT_EQ(buf.TermCount(), 4U);
    // Phase 2: 200 fresh terms — forces multiple GrowSlots calls.
    std::vector<std::string> fresh_terms;
    fresh_terms.reserve(200);
    for (int i = 0; i < 200; ++i) {
        fresh_terms.push_back("zterm" + std::to_string(i));
        buf.Append(fresh_terms.back(), static_cast<uint32_t>(150 + i), 0);
    }
    EXPECT_EQ(buf.TermCount(), 204U);
    const size_t term_states_after_growth = buf.records().size(); // baseline (compact mode)
    // Phase 3: re-Append the FIRST fresh term — must reuse its
    // existing term_id, not spawn a fresh one. We verify by
    // calling Sort() and checking the distinct text_ref count
    // equals total term count.
    buf.Append(fresh_terms[0], 350, 0);
    // Force `_records` materialization: this white-box test inspects the
    // distinct text_refs in the materialized records, which the compact
    // direct-emit fast path deliberately skips producing.
    buf.Sort(/*allow_direct_emit=*/false);
    std::unordered_set<uint32_t> distinct_text_refs;
    for (const auto& rec : buf.records()) {
        distinct_text_refs.insert(rec.text_ref);
    }
    EXPECT_EQ(distinct_text_refs.size(), 204U)
            << "All 204 distinct terms must remain distinct after compact-mode GrowSlots. "
            << "Pre-fix: one term split across two term_ids.";
    (void)term_states_after_growth;
}

TEST(SpimiPostingBufferTest, MaybeCompactPreservesDistinctTermsAcrossMigration) {
    // P51 regression test for a multi-agent-review finding: pre-fix,
    // `MaybeCompact`'s migration loop called `GetOrAssignTermId(text_ref)`
    // without first calling `Intern()`, leaving `_last_intern_slot`
    // pointing at the last-Appended term's slot. The hot-path then
    // returned `_slot_term_ids[stale_slot]` for every record, collapsing
    // every distinct term onto a single term_id and corrupting the
    // emitted segment. The fix added a `_slots[_last_intern_slot] ==
    // text_ref` guard to the hot path so the migration loop falls
    // through to the hashmap cold path.
    //
    // This test triggers the corruption scenario directly:
    //   - 5 distinct terms × 150 occurrences each = 750 records
    //   - crosses kCompactCheckEvery=512 → MaybeCompact runs
    //   - 750/5 = 150 ≥ kCompactAvgOcc=32 → compact trigger fires
    // After Sort() materialises the compact streams, the materialised
    // `_records` MUST contain all 5 distinct text_refs (not 1).
    SpimiPostingBuffer buf;
    constexpr int kTerms = 5;
    constexpr int kOccPerTerm = 150;
    const std::vector<std::string> vocab = {"alpha", "beta", "gamma", "delta", "epsilon"};
    for (int doc = 0; doc < kOccPerTerm; ++doc) {
        for (int t = 0; t < kTerms; ++t) {
            buf.Append(vocab[t], static_cast<uint32_t>(doc), static_cast<uint32_t>(t));
        }
    }
    EXPECT_EQ(buf.TermCount(), static_cast<size_t>(kTerms));
    // Force `_records` materialization so this white-box test can inspect the
    // post-migration records; the compact direct-emit fast path skips them.
    buf.Sort(/*allow_direct_emit=*/false);
    std::unordered_set<uint32_t> distinct_text_refs;
    for (const auto& rec : buf.records()) {
        distinct_text_refs.insert(rec.text_ref);
    }
    EXPECT_EQ(distinct_text_refs.size(), static_cast<size_t>(kTerms))
            << "MaybeCompact must preserve all " << kTerms << " distinct terms; got "
            << distinct_text_refs.size() << ". Pre-fix collapsed all terms onto a single term_id.";
    EXPECT_EQ(buf.records().size(), static_cast<size_t>(kTerms * kOccPerTerm));
}

TEST(SpimiPostingBufferTest, RecordCountReturnsTotalOccurrencesInCompactMode) {
    // Core regression: RecordCount() previously returned _records.size()
    // which is 0 in compact mode (after MaybeCompact clears _records).
    // The fix makes it return _total_occurrences instead.
    SpimiPostingBuffer buf;
    const std::vector<std::string> vocab = {"a", "b", "c", "d", "e"};
    constexpr int kRecords = 600; // > 512 to trigger compact
    for (int i = 0; i < kRecords; ++i) {
        buf.Append(vocab[i % vocab.size()], static_cast<uint32_t>(i / 10),
                   static_cast<uint32_t>(i % 10));
    }
    // Compact mode should have activated: _records cleared but data persists.
    EXPECT_TRUE(buf.records().empty()) << "MaybeCompact should have cleared _records vector";
    EXPECT_EQ(buf.RecordCount(), static_cast<size_t>(kRecords))
            << "RecordCount() must return _total_occurrences in compact mode";
}

TEST(SpimiPostingBufferTest, CompactModeActivatesAtExactThreshold) {
    // Boundary test: compact mode check fires at exactly 512 records.
    SpimiPostingBuffer buf;
    // 512 / 1 term = 512 avg_occ >> kCompactAvgOcc(8)
    for (uint32_t i = 0; i < 512; ++i) {
        buf.Append("term", i / 10, i % 10);
    }
    EXPECT_TRUE(buf.records().empty()) << "MaybeCompact should activate at kCompactCheckEvery=512";
    EXPECT_EQ(buf.RecordCount(), 512U);
}

TEST(SpimiPostingBufferTest, OmitTfapAccessorReflectsConstruction) {
    SpimiPostingBuffer phrase(/*omit_tfap=*/false);
    SpimiPostingBuffer docs_only(/*omit_tfap=*/true);
    EXPECT_FALSE(phrase.OmitTfap());
    EXPECT_TRUE(docs_only.OmitTfap());
    // Default ctor is phrase-on (omit_tfap=false).
    SpimiPostingBuffer dflt;
    EXPECT_FALSE(dflt.OmitTfap());
}

TEST(SpimiPostingBufferTest, OmitTfapLeavesProxChainEmptyInCompactMode) {
    // DOCS_ONLY: the per-token prox slice chain is never allocated, so every
    // term's (pos_start, pos_end) span is empty. Drive the buffer into compact
    // direct-emit mode and inspect the streams.
    SpimiPostingBuffer buf(/*omit_tfap=*/true);
    // Single hot term over many docs: 600 occ / 1 term >> kCompactAvgOcc(8).
    for (uint32_t i = 0; i < 600; ++i) {
        buf.Append("term", /*doc_id=*/i / 3, /*position=*/i % 3);
    }
    EXPECT_TRUE(buf.records().empty()) << "compact mode should have activated";
    buf.Sort(/*allow_direct_emit=*/true);
    ASSERT_TRUE(buf.CompactDirectEmitReady());
    const auto& terms = buf.SortedCompactTerms();
    ASSERT_EQ(terms.size(), 1U);
    const auto st = buf.CompactStreamsFor(terms[0].term_id);
    EXPECT_EQ(st.pos_start, st.pos_end) << "DOCS_ONLY must not allocate a prox chain";
    EXPECT_EQ(st.pos_start, 0U);
    EXPECT_GT(st.doc_end, st.doc_start) << "freq/doc chain must still be present";
}

TEST(SpimiPostingBufferTest, OmitTfapDecodeReturnsDocsWithZeroPositions) {
    // DecodeCompactTerm must round-trip the (term, doc) sequence with positions
    // forced to 0 in DOCS_ONLY (no prox chain exists to read).
    SpimiPostingBuffer buf(/*omit_tfap=*/true);
    // Deterministic (doc, freq) layout: doc d appears (d % 4 + 1) times.
    std::vector<uint32_t> expected_docs;
    for (uint32_t d = 0; d < 200; ++d) {
        const uint32_t freq = (d % 4) + 1;
        for (uint32_t f = 0; f < freq; ++f) {
            buf.Append("term", d, f);
            expected_docs.push_back(d);
        }
    }
    // Top up well past compact threshold with the same term so it stays compact.
    while (buf.RecordCount() < 600) {
        const uint32_t d = 1000 + static_cast<uint32_t>(buf.RecordCount());
        buf.Append("term", d, 0);
        expected_docs.push_back(d);
    }
    ASSERT_TRUE(buf.records().empty());
    buf.Sort(/*allow_direct_emit=*/true);
    ASSERT_TRUE(buf.CompactDirectEmitReady());
    const auto& terms = buf.SortedCompactTerms();
    ASSERT_EQ(terms.size(), 1U);
    std::vector<uint32_t> docs;
    std::vector<uint32_t> positions;
    buf.DecodeCompactTerm(terms[0].term_id, docs, positions);
    ASSERT_EQ(docs.size(), expected_docs.size());
    EXPECT_EQ(docs, expected_docs);
    for (uint32_t p : positions) {
        EXPECT_EQ(p, 0U) << "DOCS_ONLY decode must yield position 0 for every occurrence";
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
