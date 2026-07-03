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
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// G08: gate-2 resident accounting. Before G08, SpimiTermBuffer::resident_bytes()
// summed ONLY the posting arena + the vocab-sized slot index, so the entire G05
// pair-key machinery (bigram_pair_map_, pair_of_), the owned vocabulary /
// intern set, the Term slot pool and the rank/bookkeeping arrays were INVISIBLE
// to both the gate-2 spill trigger and the MemoryReporter -- on wikipedia each
// of the 16 writers held those ~750 MiB ON TOP of the 512 MiB cap the gate
// believed it was enforcing (~20 GiB total). These tests pin:
//   (1) COVERAGE + MONOTONICITY: after N unique pair interns, resident_bytes()
//       is at least the externally-measured pair-map + reverse-slot footprint
//       (the pre-G08 sum sits BELOW that floor at this shape, so the assertion
//       fails without the G08 charges), and it never decreases while feeding
//       with eviction off and no spills;
//   (2) GATE TRIGGER ON THE BIGRAM PATH: a tiny spill threshold is crossed by
//       the newly-charged pair structures alone (the arena stays inside its
//       first 32 KiB block), and the spill fires from add_bigram_token --
//       run_count_for_test() > 0 where the pre-G08 accounting never spilled;
//   (3) EQUALITY VS NO-SPILL CONTROL: the structure-triggered spill timing
//       changes nothing about the emitted stream (terms, order, postings);
//   (4) REPORTER NET-ZERO (credit/debit symmetry): every byte the intern /
//       materialize paths credit is debited by eviction + the terminal drains,
//       on both the in-memory and the spilled (+ diet eviction) paths.
using doris::snii::writer::MemoryReporter;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;

namespace {

// Distinct short unigrams ("uaa", "uab", ...): SSO-sized, alpha-only, never
// bigram-marker-prefixed.
std::string unigram(uint32_t i) {
    std::string s = "u";
    s += static_cast<char>('a' + (i / 26) % 26);
    s += static_cast<char>('a' + i % 26);
    s += static_cast<char>('a' + (i / 676) % 26);
    return s;
}

// Interns `n` distinct unigrams at docid 0 (position i) and returns their ids.
std::vector<uint32_t> intern_unigrams(SpimiTermBuffer* buf, uint32_t n) {
    std::vector<uint32_t> ids;
    ids.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t id = buf->add_token_returning_id(unigram(i), /*docid=*/0, /*pos=*/i);
        EXPECT_NE(id, SpimiTermBuffer::kInvalidTermId);
        ids.push_back(id);
    }
    return ids;
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

// (1) Coverage + monotonicity. 200k unique pairs, one token each, keeps the
// arena small (~1.8 MiB of 9 B single-token chains) while the pair structures
// alone measure ~5 MiB -- the pre-G08 arena + slot-index sum (~2.9 MiB) sits
// BELOW the asserted floor, so this test FAILS without the G08 charges.
TEST(SniiSpimiResidentAccounting, ResidentCoversPairStructuresAndIsMonotone) {
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    const std::vector<uint32_t> ids = intern_unigrams(&buf, 600);

    constexpr uint32_t kPairs = 200000;
    uint64_t prev = buf.resident_bytes_for_test();
    for (uint32_t k = 0; k < kPairs; ++k) {
        // Distinct ordered pairs (i, j); docids globally ascending.
        buf.add_bigram_token(ids[k / 600], ids[k % 600], /*docid=*/k, /*pos=*/0);
        const uint64_t now = buf.resident_bytes_for_test();
        // Monotone while nothing spills and nothing evicts (no diet): capacities
        // never shrink, the intern set and heap payloads only grow.
        ASSERT_GE(now, prev) << "resident_bytes decreased at pair " << k;
        prev = now;
    }
    ASSERT_TRUE(buf.status().ok());
    ASSERT_EQ(buf.bigram_pair_terms_for_test(), kPairs);

    // Externally-measured floor of the G05 pair machinery ALONE: one flat-map
    // slot (16 B pair + 1 control byte; size <= capacity) plus one reverse
    // pair-key slot (8 B) per live pair. resident_bytes() must cover at least
    // this -- it actually also charges the owned-vocab headers, the Term slot
    // pool and the rank arrays on top.
    const uint64_t pair_slot_bytes = sizeof(std::pair<const uint64_t, uint32_t>) + 1;
    const uint64_t measured_floor = static_cast<uint64_t>(buf.bigram_pair_terms_for_test()) *
                                    (pair_slot_bytes + sizeof(uint64_t));
    EXPECT_GE(buf.resident_bytes_for_test(), measured_floor);
}

// (2) Gate trigger on the bigram path: threshold 64 KiB, no reporter (the local
// fallback gate). After 100 unigrams the resident is one 32 KiB arena block +
// ~13 KiB of structures -- no spill. Unique pair interns then push the CHARGED
// structures (map slots, reverse slots, vocab headers, Term slots) over the
// threshold while the chains still fit the FIRST arena block, so the spill that
// fires is attributable to the G08 charges on the add_bigram_token path. The
// pre-G08 accounting (arena + slot index < 49 KiB for this whole feed) never
// spilled here.
TEST(SniiSpimiResidentAccounting, TinyThresholdSpillsOnBigramPathFromChargedStructures) {
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/64 * 1024);
    const std::vector<uint32_t> ids = intern_unigrams(&buf, 100);
    ASSERT_TRUE(buf.status().ok());
    ASSERT_EQ(buf.run_count_for_test(), 0U) << "unigram feed alone must not spill";

    uint32_t pairs_fed = 0;
    for (uint32_t k = 0; k < 2000 && buf.run_count_for_test() == 0; ++k) {
        buf.add_bigram_token(ids[(k / 100) % 100], ids[k % 100], /*docid=*/1 + k, /*pos=*/0);
        ++pairs_fed;
    }
    ASSERT_TRUE(buf.status().ok());
    // run_count_for_test() > 0: the gate-2 spill fired from the bigram add path.
    ASSERT_GT(buf.run_count_for_test(), 0U)
            << "pair-structure growth must trip the gate (pre-G08 accounting never did)";

    // The spilled buffer still drains cleanly: every unigram and every fed pair
    // term is emitted exactly once (no diet -> nothing evicted or dropped).
    size_t seen = 0;
    ASSERT_TRUE(buf.for_each_term_sorted([&seen](TermPostings&&) { ++seen; }).ok());
    EXPECT_EQ(seen, 100U + pairs_fed);
    EXPECT_TRUE(buf.status().ok());
}

// (3) Equality vs no-spill control: the earlier, structure-driven spills change
// WHEN runs are cut, never WHAT is emitted. Two rounds per pair (docids
// globally ascending across rounds) force cross-run boundary coalescing for
// pair terms materialized+pinned by the mid-feed spill.
TEST(SniiSpimiResidentAccounting, StructureDrivenSpillEqualsNoSpillControl) {
    SpimiTermBuffer spilled(/*has_positions=*/true, /*spill_threshold_bytes=*/64 * 1024);
    SpimiTermBuffer control(/*has_positions=*/true, /*spill_threshold_bytes=*/0);

    constexpr uint32_t kPairs = 400;
    for (SpimiTermBuffer* buf : {&spilled, &control}) {
        const std::vector<uint32_t> ids = intern_unigrams(buf, 100);
        for (uint32_t k = 0; k < kPairs; ++k) { // round 1: docids 1..400
            buf->add_bigram_token(ids[(k / 100) % 100], ids[k % 100], /*docid=*/1 + k, /*pos=*/0);
        }
        for (uint32_t k = 0; k < kPairs; ++k) { // round 2: docids 2001..2400
            buf->add_bigram_token(ids[(k / 100) % 100], ids[k % 100], /*docid=*/2001 + k,
                                  /*pos=*/0);
        }
        ASSERT_TRUE(buf->status().ok());
    }
    ASSERT_GE(spilled.run_count_for_test(), 1U);
    ASSERT_EQ(control.run_count_for_test(), 0U);

    const std::vector<TermPostings> got = spilled.finalize_sorted();
    const std::vector<TermPostings> want = control.finalize_sorted();
    ASSERT_TRUE(spilled.status().ok()) << spilled.status().to_string();
    ASSERT_TRUE(control.status().ok()) << control.status().to_string();
    ASSERT_EQ(want.size(), 100U + kPairs);
    expect_same_stream(got, want, "structure-driven spill");
}

// (4a) Reporter net-zero, in-memory drain: what the intern paths credit (pair
// map, reverse slots, vocab headers + heap payloads, intern nodes) the terminal
// drain debits in full -- current_bytes() returns to exactly 0.
TEST(SniiSpimiResidentAccounting, OwnedModeReporterNetsToZeroAfterInMemoryDrain) {
    MemoryReporter rep;
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0, &rep);
    const std::vector<uint32_t> ids = intern_unigrams(&buf, 50);
    for (uint32_t k = 0; k < 200; ++k) {
        buf.add_bigram_token(ids[k % 50], ids[(k + 1) % 50], /*docid=*/1 + k, /*pos=*/0);
    }
    EXPECT_GT(rep.current_bytes(), 0);
    size_t seen = 0;
    ASSERT_TRUE(buf.for_each_term_sorted([&seen](TermPostings&&) { ++seen; }).ok());
    EXPECT_GT(seen, 0U);
    EXPECT_EQ(rep.current_bytes(), 0) << "terminal drain must refund every charged byte";
}

// (4b) Reporter net-zero, spilled path WITH diet evictions: mid-feed spills
// materialize survivors, evict df==1 pair terms (debiting their charges), and
// the post-merge release returns the vocab-side remainder -- 0 at the end.
TEST(SniiSpimiResidentAccounting, OwnedModeReporterNetsToZeroAfterSpilledDrain) {
    MemoryReporter rep(/*consume_release=*/nullptr, /*cap_bytes=*/64 * 1024);
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0, &rep);
    buf.configure_bigram_diet(/*vocab_cap_bytes=*/4096); // binding: evictions fire
    const std::vector<uint32_t> ids = intern_unigrams(&buf, 100);
    for (uint32_t k = 0; k < 1200 && buf.run_count_for_test() < 2; ++k) {
        buf.add_bigram_token(ids[(k / 100) % 100], ids[k % 100], /*docid=*/1 + k, /*pos=*/0);
    }
    ASSERT_TRUE(buf.status().ok());
    ASSERT_GE(buf.run_count_for_test(), 1U);
    EXPECT_GT(rep.current_bytes(), 0);
    size_t seen = 0;
    ASSERT_TRUE(buf.for_each_term_sorted([&seen](TermPostings&&) { ++seen; }).ok());
    EXPECT_GT(seen, 0U);
    EXPECT_EQ(rep.current_bytes(), 0)
            << "spilled drain + diet evictions must refund every charged byte";
}

} // namespace
