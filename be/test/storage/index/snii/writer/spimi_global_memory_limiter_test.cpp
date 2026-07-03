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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/writer/compact_posting_pool.h"
#include "storage/index/snii/writer/global_memory_limiter.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// G09: process-wide build-RAM limiter. The per-writer gate-2 cap bounds ONE
// SPIMI accumulator; a concurrent load keeps (tablets x concurrency) writers
// alive at once, none of which may ever reach its own cap (wikipedia at
// concurrency 16: 100+ writers x 300-500 MB, ~41 GiB, zero per-writer spills).
// These tests pin the limiter's contract:
//   (1) REGISTRY: register / absolute-report / unregister maintain the exact
//       total and entry count; reports for unregistered flags are ignored;
//   (2) SELECTION: over budget flags the largest-ARENA eligible buffers
//       (arena >= the victim floor -- only the arena is reclaimable by a
//       forced spill) until the flagged arena covers the overage -- counting
//       already-pending flags -- and under budget (or budget 0 = off) never
//       flags anything;
//   (3) HONOR: the owner's next add_token observes a pending request, spills
//       (run_count increments; global_forced_spills seam bumps) BYPASSING the
//       G08 anti-churn floor but respecting the FORCED-SPILL FLOOR, and
//       clears the flag; requests are advisory;
//   (4) LIFETIME: attach registers the current resident bytes, the debounced
//       report path keeps the registry equal to resident_bytes(), and the
//       destructor un-registers;
//   (5) THREADS: concurrent register / report / unregister (with flags dying
//       right after unregister) is race-free -- the TSAN canary;
//   (6) ANTI-STORM (the conc=16 wikipedia field failure): the floor makes a
//       below-floor request a pending NO-OP, the cooldown exempts a
//       just-spilled buffer until its arena regrows past the floor, and the
//       budget-sanity check degrades to log-once-and-stop when the per-writer
//       budget share cannot be met by spilling persistent-dominated buffers.
using doris::snii::writer::CompactPostingPool;
using doris::snii::writer::GlobalMemoryLimiter;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;

namespace snii_testing = doris::snii::writer::testing;

namespace {

constexpr int64_t kMiB = 1LL << 20;

// Distinct short unigrams ("uaa", "uab", ...): SSO-sized, alpha-only, never
// bigram-marker-prefixed.
std::string unigram(uint32_t i) {
    std::string s = "u";
    s += static_cast<char>('a' + i % 26);
    s += static_cast<char>('a' + (i / 26) % 26);
    s += static_cast<char>('a' + (i / 676) % 26);
    return s;
}

// ---- (1) registry bookkeeping ---------------------------------------------

TEST(SniiGlobalMemoryLimiter, RegistryAddRemoveTotal) {
    GlobalMemoryLimiter lim; // budget defaults to 0 (off): pure tracking
    std::atomic<bool> a {false};
    std::atomic<bool> b {false};
    EXPECT_EQ(lim.total_bytes(), 0);
    EXPECT_EQ(lim.registered_count(), 0U);

    lim.register_buffer(&a, 100, 40);
    lim.register_buffer(&b, 60, 10);
    EXPECT_EQ(lim.total_bytes(), 160);
    EXPECT_EQ(lim.registered_count(), 2U);

    lim.report(&a, 150, 90); // ABSOLUTE totals, not deltas
    EXPECT_EQ(lim.total_bytes(), 210);

    lim.register_buffer(&a, 40, 5); // re-register updates in place, no duplicate
    EXPECT_EQ(lim.total_bytes(), 100);
    EXPECT_EQ(lim.registered_count(), 2U);

    lim.unregister_buffer(&a);
    EXPECT_EQ(lim.total_bytes(), 60);
    EXPECT_EQ(lim.registered_count(), 1U);

    lim.unregister_buffer(&a); // double-unregister is harmless
    lim.report(&a, 999, 999);  // a report for an unregistered flag is ignored
    EXPECT_EQ(lim.total_bytes(), 60);
    EXPECT_EQ(lim.registered_count(), 1U);

    lim.unregister_buffer(&b);
    EXPECT_EQ(lim.total_bytes(), 0);
    EXPECT_EQ(lim.registered_count(), 0U);
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
}

// ---- (2) victim selection ---------------------------------------------------

// MiB-scale entries with arena == resident keep the pre-floor selection shape
// visible while every entry clears the default 64 MiB victim floor AND every
// budget keeps budget/count above the 96 MiB/writer sanity minimum.
TEST(SniiGlobalMemoryLimiter, OverBudgetFlagsLargestUntilOverageCovered) {
    GlobalMemoryLimiter lim;
    lim.set_budget_bytes(1000 * kMiB);
    std::atomic<bool> a {false}; // the largest
    std::atomic<bool> b {false};
    std::atomic<bool> c {false};
    lim.register_buffer(&a, 900 * kMiB, 900 * kMiB);
    lim.register_buffer(&b, 500 * kMiB, 500 * kMiB);
    lim.register_buffer(&c, 100 * kMiB, 100 * kMiB); // total 1500 MiB > 1000 MiB
    // Registration alone never flags: reacting belongs to the report path.
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // Overage 500 MiB: the largest (a, 900 MiB of arena) alone covers it; b
    // and c spared.
    lim.report(&c, 100 * kMiB, 100 * kMiB);
    EXPECT_TRUE(a.load());
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // a's request is still pending -- its arena counts toward coverage, so a
    // re-report while still over budget must NOT widen the victim set.
    lim.report(&c, 100 * kMiB, 100 * kMiB);
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // Deeper overage (budget 300 MiB -> overage 1200 MiB): a (900) is no
    // longer enough, b joins (900 + 500 >= 1200), c is still spared.
    lim.set_budget_bytes(300 * kMiB);
    lim.report(&c, 100 * kMiB, 100 * kMiB);
    EXPECT_TRUE(a.load());
    EXPECT_TRUE(b.load());
    EXPECT_FALSE(c.load());
}

TEST(SniiGlobalMemoryLimiter, UnderBudgetOrDisabledNeverFlags) {
    GlobalMemoryLimiter lim;
    lim.set_budget_bytes(1000 * kMiB);
    std::atomic<bool> a {false};
    std::atomic<bool> b {false};
    lim.register_buffer(&a, 600 * kMiB, 600 * kMiB);
    lim.register_buffer(&b, 300 * kMiB, 300 * kMiB);
    lim.report(&a, 700 * kMiB, 700 * kMiB); // total == budget: at (not over) budget
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());

    lim.set_budget_bytes(0); // 0 = limiter off, no matter how large the total
    lim.report(&a, 100000 * kMiB, 100000 * kMiB);
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
}

// The field storm's root selection bug: victims were ranked by RESIDENT total,
// which is dominated by PERSISTENT (non-spillable) vocab/pair structures.
// Victims must be ranked by their reclaimable ARENA, and a buffer below the
// victim floor is not a victim at all.
TEST(SniiGlobalMemoryLimiter, VictimsSelectedByArenaNotResident) {
    GlobalMemoryLimiter lim;
    lim.set_budget_bytes(600 * kMiB); // 2 writers -> 300 MiB/writer: sane budget
    std::atomic<bool> persistent_heavy {false};
    std::atomic<bool> arena_heavy {false};
    // Larger RESIDENT, tiny arena (8 MiB < the 64 MiB default floor).
    lim.register_buffer(&persistent_heavy, 1000 * kMiB, 8 * kMiB);
    // Smaller resident, large reclaimable arena.
    lim.register_buffer(&arena_heavy, 500 * kMiB, 300 * kMiB);

    lim.report(&persistent_heavy, 1000 * kMiB, 8 * kMiB); // total 1500 > 600
    EXPECT_TRUE(arena_heavy.load()) << "the reclaimable-arena holder is the victim";
    EXPECT_FALSE(persistent_heavy.load())
            << "a below-floor arena must never be flagged, however large its resident total";
}

// PER-BUFFER COOLDOWN: after a victim's forced spill its arena is ~0; further
// over-budget reports must NOT re-flag it until the arena regrows past the
// floor. (No timer: eligibility IS the cooldown.)
TEST(SniiGlobalMemoryLimiter, CooldownSkipsJustSpilledBufferUntilArenaRegrows) {
    GlobalMemoryLimiter lim;
    lim.set_budget_bytes(200 * kMiB); // 1 writer -> 200 MiB/writer: sane budget
    std::atomic<bool> flag {false};
    lim.register_buffer(&flag, 400 * kMiB, 200 * kMiB);

    lim.report(&flag, 400 * kMiB, 200 * kMiB); // over budget, arena >= floor
    EXPECT_TRUE(flag.load());
    flag.store(false); // the owner honors: spill, clear the flag...
    // ...and its next report shows the arena reclaimed but the PERSISTENT
    // remainder still over budget (the field shape).
    lim.report(&flag, 210 * kMiB, 0);
    EXPECT_FALSE(flag.load()) << "cooldown: a spilled (empty-arena) buffer is exempt";
    lim.report(&flag, 240 * kMiB, 32 * kMiB); // regrown, still below the floor
    EXPECT_FALSE(flag.load()) << "still exempt below the floor";
    lim.report(&flag, 280 * kMiB, 72 * kMiB); // regrown PAST the 64 MiB floor
    EXPECT_TRUE(flag.load()) << "eligible again once a floor's worth is reclaimable";
}

// BUDGET SANITY: when budget / registered_count < the per-writer useful
// minimum, spilling cannot meet the budget (persistent structures dominate) --
// the limiter must degrade (log once, flag nothing) and recover when the
// ratio does.
TEST(SniiGlobalMemoryLimiter, BudgetDegradationStopsFlaggingAndRecovers) {
    GlobalMemoryLimiter lim;
    // 3 writers on a 240 MiB budget: 80 MiB/writer < the 96 MiB minimum.
    lim.set_budget_bytes(240 * kMiB);
    std::atomic<bool> a {false};
    std::atomic<bool> b {false};
    std::atomic<bool> c {false};
    lim.register_buffer(&a, 200 * kMiB, 150 * kMiB);
    lim.register_buffer(&b, 200 * kMiB, 150 * kMiB);
    lim.register_buffer(&c, 200 * kMiB, 150 * kMiB);
    EXPECT_FALSE(lim.budget_degraded());

    lim.report(&a, 200 * kMiB, 150 * kMiB); // total 600 > 240: over budget...
    EXPECT_TRUE(lim.budget_degraded()) << "80 MiB/writer < 96 MiB: degrade";
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());
    lim.report(&b, 200 * kMiB, 150 * kMiB); // sustained episode: still no flags
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // One writer drains: 240 MiB / 2 = 120 MiB/writer >= 96 MiB -- recovered.
    lim.unregister_buffer(&c);
    lim.report(&a, 200 * kMiB, 150 * kMiB); // total 400 > 240, sane ratio
    EXPECT_FALSE(lim.budget_degraded());
    EXPECT_TRUE(a.load() || b.load()) << "flagging must resume after recovery";
}

// ---- (3) the owner honors a pending request ---------------------------------

TEST(SniiSpimiGlobalSpill, OwnerHonorsRequestAtNextTokenAndClears) {
    snii_testing::reset_global_forced_spills();
    // Unlimited local threshold: the per-writer gate can never fire, so any
    // spill below is attributable to the global request alone. The G08
    // anti-churn floor (arena >= cap/4) is bypassed by construction here --
    // the arena holds a single 32 KiB block, far below any production cap/4.
    // The forced-spill floor is dropped to its one-block minimum: THIS test
    // pins the honor mechanics, not the floor (see the floor tests below).
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    buf.set_forced_spill_min_arena_bytes(1);
    for (uint32_t d = 0; d < 200; ++d) {
        buf.add_token("hot", /*docid=*/d, /*pos=*/0);
    }
    ASSERT_TRUE(buf.status().ok());
    ASSERT_EQ(buf.run_count_for_test(), 0U);

    buf.request_global_spill_for_test(); // what the limiter does cross-thread
    EXPECT_TRUE(buf.global_spill_requested_for_test());
    EXPECT_EQ(snii_testing::global_forced_spills(), 0U);

    buf.add_token("hot", /*docid=*/200, /*pos=*/0); // next token observes it
    EXPECT_EQ(buf.run_count_for_test(), 1U) << "forced spill must cut a run";
    EXPECT_FALSE(buf.global_spill_requested_for_test()) << "honor must clear";
    EXPECT_EQ(snii_testing::global_forced_spills(), 1U);

    // Once cleared, later tokens spill nothing further (advisory, one-shot).
    buf.add_token("hot", /*docid=*/201, /*pos=*/0);
    EXPECT_EQ(buf.run_count_for_test(), 1U);

    // The forced spill changes WHEN a run was cut, never WHAT is emitted.
    std::vector<TermPostings> out = buf.finalize_sorted();
    ASSERT_TRUE(buf.status().ok()) << buf.status().to_string();
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].term, "hot");
    ASSERT_EQ(out[0].docids.size(), 202U);
    EXPECT_EQ(out[0].docids.front(), 0U);
    EXPECT_EQ(out[0].docids.back(), 201U);
}

TEST(SniiSpimiGlobalSpill, RequestOnEmptyArenaIsPendingUntilARunIsWritable) {
    snii_testing::reset_global_forced_spills();
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    buf.set_forced_spill_min_arena_bytes(1); // one-block minimum still applies
    // A request on an empty buffer has nothing to write: purely advisory, no
    // run, flag stays pending (a drained owner would simply never observe it).
    buf.request_global_spill_for_test();
    EXPECT_EQ(buf.run_count_for_test(), 0U);
    EXPECT_TRUE(buf.global_spill_requested_for_test());
    // The first token claims the first 32 KiB arena block -- the "arena >= one
    // block" floor is now met, so the pending request is honored right there.
    buf.add_token("t", /*docid=*/0, /*pos=*/0);
    EXPECT_EQ(buf.run_count_for_test(), 1U);
    EXPECT_FALSE(buf.global_spill_requested_for_test());
    EXPECT_EQ(snii_testing::global_forced_spills(), 1U);
    ASSERT_TRUE(buf.status().ok());
}

// ---- (6) forced-spill floor: below-floor requests are pending no-ops --------

// The field storm's honor-side bug: a request was honored with a single 32 KiB
// arena block, cutting a near-empty run. With the (default 64 MiB) floor, a
// request planted while the arena is small must spill NOTHING -- not per
// token, not once -- and stay pending.
TEST(SniiSpimiGlobalSpill, RequestBelowForcedSpillFloorIsNotHonored) {
    snii_testing::reset_global_forced_spills();
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    // Production-default floor (64 MiB) -- far above this feed's arena.
    ASSERT_EQ(buf.forced_spill_min_arena_bytes(),
              SpimiTermBuffer::kDefaultForcedSpillMinArenaBytes);
    buf.request_global_spill_for_test();
    for (uint32_t d = 0; d < 5000; ++d) { // a few arena blocks, << 64 MiB
        buf.add_token(unigram(d % 300), /*docid=*/d, /*pos=*/0);
    }
    ASSERT_TRUE(buf.status().ok());
    EXPECT_EQ(buf.run_count_for_test(), 0U) << "below-floor request must be a no-op";
    EXPECT_EQ(snii_testing::global_forced_spills(), 0U);
    EXPECT_TRUE(buf.global_spill_requested_for_test()) << "...that stays pending";
}

// Deferred honor: with a small floor (3 arena blocks), a pending request is a
// no-op until the arena regrows past the floor, then fires exactly once.
TEST(SniiSpimiGlobalSpill, RequestHonoredOnceArenaRegrowsPastFloor) {
    snii_testing::reset_global_forced_spills();
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    const uint64_t kFloor = 3ULL * CompactPostingPool::kBlockSize;
    buf.set_forced_spill_min_arena_bytes(kFloor);
    buf.request_global_spill_for_test();
    uint32_t docid = 0;
    // One block is not enough: feed until the arena holds one block and check
    // the request is still pending, then keep feeding until the floor is met.
    while (buf.status().ok() && buf.run_count_for_test() == 0 && docid < 200000) {
        buf.add_token("hot", docid, /*pos=*/docid % 7);
        ++docid;
    }
    ASSERT_TRUE(buf.status().ok());
    ASSERT_EQ(buf.run_count_for_test(), 1U) << "must fire once the floor is met";
    EXPECT_FALSE(buf.global_spill_requested_for_test());
    EXPECT_EQ(snii_testing::global_forced_spills(), 1U);
    // The run was cut with at least a floor's worth of arena accumulated: the
    // feed needed more than 2 blocks' worth of tokens (each token appends
    // >= 2 bytes, so 2 blocks < 64 KiB of payload < the token count here).
    EXPECT_GT(docid, (2ULL * CompactPostingPool::kBlockSize) / 4)
            << "honor must not fire before the floor's worth of arena existed";
}

// ---- (6) the storm scenario end-to-end --------------------------------------

// The conc=16 wikipedia field failure in miniature, with PRODUCTION-DEFAULT
// anti-storm settings: a budget far below the writers' (persistent-dominated)
// resident sum and far below (96 MiB x writers). The old limiter flagged every
// buffer on every report and each honored with one 32 KiB block -- a storm of
// tiny runs. Now: the budget-sanity check degrades (log once, stop flagging),
// the victim floor exempts every small-arena buffer anyway, so ZERO forced
// spills and ZERO runs result.
TEST(SniiSpimiGlobalSpill, StormScenarioTinyBudgetSmallArenasProducesZeroForcedSpills) {
    snii_testing::reset_global_forced_spills();
    GlobalMemoryLimiter lim; // production defaults: 64 MiB floor, 96 MiB/writer sanity
    lim.set_budget_bytes(256 * 1024);

    constexpr size_t kBuffers = 6;
    std::vector<std::unique_ptr<SpimiTermBuffer>> buffers;
    buffers.reserve(kBuffers);
    for (size_t i = 0; i < kBuffers; ++i) {
        auto buf = std::make_unique<SpimiTermBuffer>(/*has_positions=*/true,
                                                     /*spill_threshold_bytes=*/0);
        buf->attach_global_limiter(&lim);
        buffers.push_back(std::move(buf));
    }
    // Interleaved feed: every buffer's resident grows well past its budget
    // share (the registry total exceeds 1 MiB almost immediately), reports
    // fire constantly, yet nothing may be flagged or spilled.
    for (uint32_t round = 0; round < 400; ++round) {
        for (auto& buf : buffers) {
            for (uint32_t k = 0; k < 8; ++k) {
                buf->add_token(unigram((round * 8 + k) % 2000), /*docid=*/round * 8 + k,
                               /*pos=*/0);
            }
        }
    }
    ASSERT_GT(lim.total_bytes(), lim.budget_bytes()) << "the scenario must be over budget";
    EXPECT_TRUE(lim.budget_degraded()) << "256 KiB / 6 writers is an unmeetable budget";
    EXPECT_EQ(snii_testing::global_forced_spills(), 0U) << "no forced-spill storm";
    for (auto& buf : buffers) {
        ASSERT_TRUE(buf->status().ok());
        EXPECT_EQ(buf->run_count_for_test(), 0U) << "no runs were cut";
        EXPECT_FALSE(buf->global_spill_requested_for_test()) << "no flags were planted";
    }
}

// ---- (4) attach / report / detach lifetime ----------------------------------

TEST(SniiSpimiGlobalSpill, AttachRegistersReportsTrackResidentAndDtorUnregisters) {
    GlobalMemoryLimiter lim; // budget 0: tracking only, no flags
    {
        SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
        buf.attach_global_limiter(&lim);
        EXPECT_EQ(lim.registered_count(), 1U);
        EXPECT_EQ(lim.total_bytes(), static_cast<int64_t>(buf.resident_bytes_for_test()));

        for (uint32_t i = 0; i < 300; ++i) {
            buf.add_token(unigram(i), /*docid=*/i, /*pos=*/0);
        }
        ASSERT_TRUE(buf.status().ok());
        // The debounced report path forwards ABSOLUTE totals: at rest the
        // registry equals the buffer's real resident bytes exactly.
        EXPECT_EQ(lim.total_bytes(), static_cast<int64_t>(buf.resident_bytes_for_test()));

        buf.attach_global_limiter(&lim); // at-most-once: ignored
        EXPECT_EQ(lim.registered_count(), 1U);
    }
    // Destruction un-registers: nothing leaks into the process-wide total.
    EXPECT_EQ(lim.registered_count(), 0U);
    EXPECT_EQ(lim.total_bytes(), 0);
}

// End-to-end: two attached buffers, one small and one that grows past the
// budget. The limiter must flag the larger-ARENA grower once its arena clears
// the victim floor (the small buffer's single arena block never does); the
// grower's own next token honors the request (its local threshold is
// unlimited, the G08 floor bypassed); the small buffer is never flagged and
// never spills. Degradation is disabled: the KiB-scale budget is intentional.
TEST(SniiSpimiGlobalSpill, LimiterFlagsLargestOwnerSpillsSmallBufferSpared) {
    snii_testing::reset_global_forced_spills();
    GlobalMemoryLimiter lim; // declared BEFORE the buffers: outlives them
    lim.set_budget_bytes(256 * 1024);
    const int64_t kFloor = 2LL * CompactPostingPool::kBlockSize; // 64 KiB
    lim.set_min_victim_arena_bytes(kFloor);
    lim.set_min_useful_budget_per_writer_bytes(0); // KiB-scale test budget

    SpimiTermBuffer small(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    small.set_forced_spill_min_arena_bytes(static_cast<uint64_t>(kFloor));
    small.attach_global_limiter(&lim);
    SpimiTermBuffer big(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    big.set_forced_spill_min_arena_bytes(static_cast<uint64_t>(kFloor));
    big.attach_global_limiter(&lim);

    for (uint32_t i = 0; i < 50; ++i) { // ~1 arena block: forever below the floor
        small.add_token(unigram(i), /*docid=*/i, /*pos=*/0);
    }
    ASSERT_TRUE(small.status().ok());

    // Distinct terms grow big's resident (vocab + intern + slots + arena) past
    // the budget and its ARENA past the floor; the over-budget report then
    // flags big (the largest eligible arena), and big's own add path honors
    // the request. Bounded feed with an early exit. The bound stays below
    // unigram()'s 17576 distinct strings so every fed term is DISTINCT (the
    // drain-count assertion relies on that); three tokens per term grow the
    // arena ~3x faster than the vocab, so the floor is met well within it.
    uint32_t fed = 0;
    uint32_t docid = 0;
    for (uint32_t k = 0; k < 16000 && big.run_count_for_test() == 0; ++k, ++fed) {
        big.add_token(unigram(k), docid++, /*pos=*/0);
        big.add_token(unigram(k), docid++, /*pos=*/1);
        big.add_token(unigram(k), docid++, /*pos=*/2);
    }
    ASSERT_TRUE(big.status().ok());
    ASSERT_GT(big.run_count_for_test(), 0U) << "grower must be forced to spill";
    EXPECT_FALSE(big.global_spill_requested_for_test()) << "honor must clear";
    EXPECT_GE(snii_testing::global_forced_spills(), 1U);
    EXPECT_EQ(small.run_count_for_test(), 0U) << "small buffer must be spared";
    EXPECT_FALSE(small.global_spill_requested_for_test());

    // The forced spill released the grower's arena and reported the drop: the
    // registry total is back to the exact resident sum of both buffers.
    EXPECT_EQ(lim.total_bytes(), static_cast<int64_t>(small.resident_bytes_for_test()) +
                                         static_cast<int64_t>(big.resident_bytes_for_test()));

    // Both buffers still drain cleanly (the small one in memory, the big one
    // through its forced run + k-way merge).
    size_t small_terms = 0;
    ASSERT_TRUE(small.for_each_term_sorted([&small_terms](TermPostings&&) { ++small_terms; }).ok());
    EXPECT_EQ(small_terms, 50U);
    size_t big_terms = 0;
    ASSERT_TRUE(big.for_each_term_sorted([&big_terms](TermPostings&&) { ++big_terms; }).ok());
    EXPECT_EQ(big_terms, fed);
}

// ---- (5) thread-safety canary ------------------------------------------------

// Concurrent register / report / unregister with a tiny budget so cross-thread
// flagging constantly targets flags that die right after their unregister --
// the exact lifetime the mutex must protect. Run under TSAN this is the G09
// race canary; under ASAN it still catches any touch-after-unregister. The
// floor and the sanity minimum are dropped so the byte-scale entries keep
// producing flags (the point is flag traffic, not selection policy).
TEST(SniiGlobalMemoryLimiter, ConcurrentRegisterReportUnregisterIsClean) {
    GlobalMemoryLimiter lim;
    lim.set_budget_bytes(1); // everything is over budget: flags fly
    lim.set_min_victim_arena_bytes(1);
    lim.set_min_useful_budget_per_writer_bytes(0);
    constexpr int kThreads = 8;
    constexpr int kIters = 400;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&lim, t] {
            for (int i = 0; i < kIters; ++i) {
                std::atomic<bool> flag {false};
                lim.register_buffer(&flag, 1000 + t, 1000 + t);
                for (int r = 0; r < 4; ++r) {
                    lim.report(&flag, 1000 + t + r, 1000 + t + r);
                    if (flag.load(std::memory_order_relaxed)) {
                        // The owner honoring: observe and clear on its thread.
                        flag.store(false, std::memory_order_relaxed);
                    }
                }
                lim.unregister_buffer(&flag);
                // `flag` is destroyed here -- after unregister_buffer returned,
                // the limiter must never touch it again.
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(lim.total_bytes(), 0);
    EXPECT_EQ(lim.registered_count(), 0U);
}

} // namespace
