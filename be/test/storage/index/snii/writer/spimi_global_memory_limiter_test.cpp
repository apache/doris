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
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/writer/global_memory_limiter.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// G09: process-wide build-RAM limiter. The per-writer gate-2 cap bounds ONE
// SPIMI accumulator; a concurrent load keeps (tablets x concurrency) writers
// alive at once, none of which may ever reach its own cap (wikipedia at
// concurrency 16: 100+ writers x 300-500 MB, ~41 GiB, zero per-writer spills).
// These tests pin the limiter's contract:
//   (1) REGISTRY: register / absolute-report / unregister maintain the exact
//       total and entry count; reports for unregistered flags are ignored;
//   (2) SELECTION: over budget flags the LARGEST registered buffers until the
//       flagged sum covers the overage -- counting already-pending flags --
//       and under budget (or budget 0 = off) never flags anything;
//   (3) HONOR: the owner's next add_token observes a pending request, spills
//       (run_count increments; global_forced_spills seam bumps) BYPASSING the
//       G08 anti-churn floor, and clears the flag; requests are advisory;
//   (4) LIFETIME: attach registers the current resident bytes, the debounced
//       report path keeps the registry equal to resident_bytes(), and the
//       destructor un-registers;
//   (5) THREADS: concurrent register / report / unregister (with flags dying
//       right after unregister) is race-free -- the TSAN canary.
using doris::snii::writer::GlobalMemoryLimiter;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;

namespace snii_testing = doris::snii::writer::testing;

namespace {

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

    lim.register_buffer(&a, 100);
    lim.register_buffer(&b, 60);
    EXPECT_EQ(lim.total_bytes(), 160);
    EXPECT_EQ(lim.registered_count(), 2U);

    lim.report(&a, 150); // ABSOLUTE totals, not deltas
    EXPECT_EQ(lim.total_bytes(), 210);

    lim.register_buffer(&a, 40); // re-register updates in place, no duplicate
    EXPECT_EQ(lim.total_bytes(), 100);
    EXPECT_EQ(lim.registered_count(), 2U);

    lim.unregister_buffer(&a);
    EXPECT_EQ(lim.total_bytes(), 60);
    EXPECT_EQ(lim.registered_count(), 1U);

    lim.unregister_buffer(&a); // double-unregister is harmless
    lim.report(&a, 999);       // a report for an unregistered flag is ignored
    EXPECT_EQ(lim.total_bytes(), 60);
    EXPECT_EQ(lim.registered_count(), 1U);

    lim.unregister_buffer(&b);
    EXPECT_EQ(lim.total_bytes(), 0);
    EXPECT_EQ(lim.registered_count(), 0U);
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
}

// ---- (2) victim selection ---------------------------------------------------

TEST(SniiGlobalMemoryLimiter, OverBudgetFlagsLargestUntilOverageCovered) {
    GlobalMemoryLimiter lim;
    lim.set_budget_bytes(100);
    std::atomic<bool> a {false}; // the largest
    std::atomic<bool> b {false};
    std::atomic<bool> c {false};
    lim.register_buffer(&a, 90);
    lim.register_buffer(&b, 50);
    lim.register_buffer(&c, 10); // total 150 > 100
    // Registration alone never flags: reacting belongs to the report path.
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // Overage 50: the largest (a, 90 bytes) alone covers it; b and c spared.
    lim.report(&c, 10);
    EXPECT_TRUE(a.load());
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // a's request is still pending -- its bytes count toward coverage, so a
    // re-report while still over budget must NOT widen the victim set.
    lim.report(&c, 10);
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // Deeper overage (budget 20 -> overage 130): a (90) is no longer enough,
    // b joins (90 + 50 >= 130), c is still spared.
    lim.set_budget_bytes(20);
    lim.report(&c, 10);
    EXPECT_TRUE(a.load());
    EXPECT_TRUE(b.load());
    EXPECT_FALSE(c.load());
}

TEST(SniiGlobalMemoryLimiter, UnderBudgetOrDisabledNeverFlags) {
    GlobalMemoryLimiter lim;
    lim.set_budget_bytes(1000);
    std::atomic<bool> a {false};
    std::atomic<bool> b {false};
    lim.register_buffer(&a, 600);
    lim.register_buffer(&b, 300);
    lim.report(&a, 700); // total 1000 == budget: at (not over) budget
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());

    lim.set_budget_bytes(0); // 0 = limiter off, no matter how large the total
    lim.report(&a, 100000);
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
}

// ---- (3) the owner honors a pending request ---------------------------------

TEST(SniiSpimiGlobalSpill, OwnerHonorsRequestAtNextTokenAndClears) {
    snii_testing::reset_global_forced_spills();
    // Unlimited local threshold: the per-writer gate can never fire, so any
    // spill below is attributable to the global request alone. The G08
    // anti-churn floor (arena >= cap/4) is bypassed by construction here --
    // the arena holds a single 32 KiB block, far below any production cap/4.
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
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
// budget. The limiter must flag the LARGEST (the grower), whose own next token
// honors the request (bypassing the G08 floor -- its local threshold is
// unlimited); the small buffer is never flagged and never spills.
TEST(SniiSpimiGlobalSpill, LimiterFlagsLargestOwnerSpillsSmallBufferSpared) {
    snii_testing::reset_global_forced_spills();
    GlobalMemoryLimiter lim; // declared BEFORE the buffers: outlives them
    lim.set_budget_bytes(256 * 1024);

    SpimiTermBuffer small(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    small.attach_global_limiter(&lim);
    SpimiTermBuffer big(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
    big.attach_global_limiter(&lim);

    for (uint32_t i = 0; i < 50; ++i) { // ~40 KiB resident: never the largest
        small.add_token(unigram(i), /*docid=*/i, /*pos=*/0);
    }
    ASSERT_TRUE(small.status().ok());

    // Distinct terms grow big's resident (vocab + intern + slots + arena) past
    // the budget; the over-budget report flags big (the largest), and big's
    // own add path honors the request. Bounded feed with an early exit.
    uint32_t fed = 0;
    // Bound stays below unigram()'s 17576 distinct strings so every fed token
    // is a DISTINCT term (the drain-count assertion below relies on that); the
    // spill is expected after ~2-3k terms, so the headroom is ~6x.
    for (uint32_t k = 0; k < 16000 && big.run_count_for_test() == 0; ++k, ++fed) {
        big.add_token(unigram(k), /*docid=*/k, /*pos=*/0);
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
// race canary; under ASAN it still catches any touch-after-unregister.
TEST(SniiGlobalMemoryLimiter, ConcurrentRegisterReportUnregisterIsClean) {
    GlobalMemoryLimiter lim;
    lim.set_budget_bytes(1); // everything is over budget: flags fly
    constexpr int kThreads = 8;
    constexpr int kIters = 400;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&lim, t] {
            for (int i = 0; i < kIters; ++i) {
                std::atomic<bool> flag {false};
                lim.register_buffer(&flag, 1000 + t);
                for (int r = 0; r < 4; ++r) {
                    lim.report(&flag, 1000 + t + r);
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
