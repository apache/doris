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
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "runtime/memory/mem_tracker.h"
#include "storage/index/snii/writer/compact_posting_pool.h"
#include "storage/index/snii/writer/global_memory_limiter.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/snii_build_memory_tracker.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii/writer/term_posting_test_utils.h"
#include "util/mem_info.h"

// G09: process-wide build-RAM limiter. The per-writer gate-2 cap bounds ONE
// SPIMI accumulator; a concurrent load keeps (tablets x concurrency) writers
// alive at once, none of which may ever reach its own cap (wikipedia at
// concurrency 16: 100+ writers x 300-500 MB, ~41 GiB, zero per-writer spills).
// These tests pin the limiter's contract:
//   (1) REGISTRY: register / absolute-report / unregister maintain the exact
//       eligible-arena sum and entry count; reports for unregistered flags are
//       ignored;
//   (2) TRIGGER: the decision is judged against SNII's share of the process
//       limit (from the observation tracker's consumption) plus the two
//       process-level backstops; a zero share disables SNII's own trigger even
//       for writers that registered while it was enabled;
//   (3) SELECTION: over the share, the largest-ARENA eligible buffers
//       (arena >= the victim floor -- only the arena is reclaimable by a
//       forced spill) are flagged until the flagged arena covers the overage --
//       counting already-pending flags -- and under it nothing is flagged;
//   (4) HONOR: the owner's next add_token observes a pending request, spills
//       (run_count increments; global_forced_spills seam bumps) BYPASSING the
//       G08 anti-churn floor but respecting the FORCED-SPILL FLOOR, and
//       clears the flag; requests are advisory;
//   (5) LIFETIME: attach registers the current arena bytes, the debounced
//       report path keeps the registry equal to arena_bytes(), and the
//       destructor un-registers;
//   (6) THREADS: concurrent register / report / unregister (with flags dying
//       right after unregister) is race-free -- the TSAN canary;
//   (7) ANTI-STORM (the conc=16 wikipedia field failure): the floor makes a
//       below-floor request a pending NO-OP, the cooldown exempts a
//       just-spilled buffer until its arena regrows past the floor, and
//       flagging is suspended whenever the reclaimable arena summed over the
//       eligible victims cannot cover the overage.
using doris::snii::writer::BuildMemorySignals;
using doris::snii::writer::calc_process_max_snii_build_memory;
using doris::snii::writer::MemoryReporter;
using doris::snii::writer::read_build_memory_signals;
using doris::snii::writer::CompactPostingPool;
using doris::snii::writer::GlobalMemoryLimiter;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::StreamedTermPostings;
using doris::Status;
using doris::snii::writer::TermPostings;

namespace snii_testing = doris::snii::writer::testing;

namespace {

constexpr int64_t kMiB = 1LL << 20;

// Distinct short ordinary terms ("uaa", "uab", ...).
std::string unigram(uint32_t i) {
    std::string s = "u";
    s += static_cast<char>('a' + i % 26);
    s += static_cast<char>('a' + (i / 26) % 26);
    s += static_cast<char>('a' + (i / 676) % 26);
    return s;
}

// A tunable stand-in for the process memory state the limiter judges against.
// Production reads these from Doris (read_build_memory_signals); tests drive
// them directly so the decision layer is deterministic. Declare one BEFORE the
// limiter it feeds: the provider borrows it.
struct FakeSignals {
    std::atomic<int64_t> build_consumption {0};
    std::atomic<int64_t> build_share_bytes {0};
    std::atomic<int64_t> sys_avail_below_warning_water_mark {0};
    std::atomic<int64_t> process_above_soft_mem_limit {0};

    GlobalMemoryLimiter::SignalsFn provider() {
        // Atomics throughout: the limiter invokes this from whichever thread
        // happens to be reporting.
        return [this] {
            BuildMemorySignals signals;
            signals.build_consumption = build_consumption.load();
            signals.build_share_bytes = build_share_bytes.load();
            signals.sys_avail_below_warning_water_mark = sys_avail_below_warning_water_mark.load();
            signals.process_above_soft_mem_limit = process_above_soft_mem_limit.load();
            return signals;
        };
    }

    // Puts SNII exactly `overage` bytes above its share, with the two
    // process-level backstops quiet.
    void set_overage(int64_t overage, int64_t share = 1LL << 30) {
        build_share_bytes.store(share);
        build_consumption.store(share + overage);
    }
};

// Restores the mutable share percent when a test that moves it finishes.
struct SharePercentRestore {
    int32_t saved = doris::config::snii_index_build_max_memory_limit_percent;
    ~SharePercentRestore() { doris::config::snii_index_build_max_memory_limit_percent = saved; }
};

// ---- (1) registry bookkeeping ---------------------------------------------

TEST(SniiGlobalMemoryLimiter, RegistryAddRemoveEligibleArena) {
    FakeSignals signals; // no pressure: pure tracking
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    lim.set_min_victim_arena_bytes(1); // byte-scale entries are all eligible
    std::atomic<bool> a {false};
    std::atomic<bool> b {false};
    EXPECT_EQ(lim.eligible_arena_bytes(), 0);
    EXPECT_EQ(lim.registered_count(), 0U);

    lim.register_buffer(&a, 40);
    lim.register_buffer(&b, 10);
    EXPECT_EQ(lim.eligible_arena_bytes(), 50);
    EXPECT_EQ(lim.registered_count(), 2U);

    lim.report(&a, 90); // ABSOLUTE arena bytes, not deltas
    EXPECT_EQ(lim.eligible_arena_bytes(), 100);

    lim.register_buffer(&a, 5); // re-register updates in place, no duplicate
    EXPECT_EQ(lim.eligible_arena_bytes(), 15);
    EXPECT_EQ(lim.registered_count(), 2U);

    lim.unregister_buffer(&a);
    EXPECT_EQ(lim.eligible_arena_bytes(), 10);
    EXPECT_EQ(lim.registered_count(), 1U);

    lim.unregister_buffer(&a); // double-unregister is harmless
    lim.report(&a, 999);       // a report for an unregistered flag is ignored
    EXPECT_EQ(lim.eligible_arena_bytes(), 10);
    EXPECT_EQ(lim.registered_count(), 1U);

    lim.unregister_buffer(&b);
    EXPECT_EQ(lim.eligible_arena_bytes(), 0);
    EXPECT_EQ(lim.registered_count(), 0U);
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
}

// ---- (2) the trigger: SNII's share, then the process backstops --------------

TEST(SniiGlobalMemoryLimiter, ShareIsDerivedFromTheProcessLimit) {
    SharePercentRestore restore;
    doris::config::snii_index_build_max_memory_limit_percent = 10;
    EXPECT_EQ(calc_process_max_snii_build_memory(100LL * 1024 * kMiB), 10LL * 1024 * kMiB)
            << "the share scales with the BE's process limit instead of being a "
               "fixed budget";
    EXPECT_EQ(calc_process_max_snii_build_memory(200LL * 1024 * kMiB), 20LL * 1024 * kMiB);
    EXPECT_EQ(calc_process_max_snii_build_memory(-1), -1)
            << "an unlimited process yields no derivable share";

    doris::config::snii_index_build_max_memory_limit_percent = 0;
    EXPECT_EQ(calc_process_max_snii_build_memory(100LL * 1024 * kMiB), 0)
            << "0 percent disables SNII's own share trigger";
}

// THE TRAP the share exists to avoid: the process soft limit is a global valve
// that trips late. If SNII's own share were not a small minority of the process
// limit it would fire no earlier than the backstops, and the mechanism would be
// worth nothing.
TEST(SniiGlobalMemoryLimiter, DefaultShareIsAMinorityOfTheProcessLimit) {
    const int32_t percent = doris::config::snii_index_build_max_memory_limit_percent;
    EXPECT_GT(percent, 0) << "the share must be enabled by default";
    EXPECT_LT(percent, doris::config::load_process_max_memory_limit_percent)
            << "index build must shed memory well before the process-level valves";
}

TEST(SniiGlobalMemoryLimiter, UnlimitedProcessYieldsNoShare) {
    SharePercentRestore restore;
    doris::config::snii_index_build_max_memory_limit_percent = 10;
    // Doris's "no limit" convention, and the unconfigured-cgroup value that
    // would overflow a percent multiplication.
    EXPECT_EQ(calc_process_max_snii_build_memory(-1), -1);
    EXPECT_EQ(calc_process_max_snii_build_memory(0), -1);
    EXPECT_EQ(calc_process_max_snii_build_memory(std::numeric_limits<int64_t>::max()), -1);
}

// I3: the share is floored against the per-writer spill threshold. Without the
// floor a small BE starts permanently over its share -- back-pressure that can
// never be relieved, because one writer alone may hold the threshold.
TEST(SniiGlobalMemoryLimiter, ShareIsFlooredAgainstThePerWriterSpillThreshold) {
    SharePercentRestore restore;
    doris::config::snii_index_build_max_memory_limit_percent = 10;
    const auto per_writer_cap =
            static_cast<int64_t>(doris::config::inverted_index_ram_buffer_size * 1024 * 1024);
    // An 8 GiB BE: 10% is 819 MiB, under two writers' own threshold.
    const int64_t small_be = 8LL * 1024 * kMiB;
    EXPECT_EQ(calc_process_max_snii_build_memory(small_be), 4 * per_writer_cap);
    EXPECT_GE(calc_process_max_snii_build_memory(small_be), 4 * per_writer_cap)
            << "a share below a few writers' worth of the per-writer threshold is unrelievable";
    // A large BE is above the floor and keeps the derived percentage.
    EXPECT_EQ(calc_process_max_snii_build_memory(100LL * 1024 * kMiB), 10LL * 1024 * kMiB);
}

// THE REVIEW FINDING, through the PRODUCTION signal reader: the old limiter
// latched an absolute budget into the process singleton and only refreshed it
// when a NEW writer registered, so lowering the mutable config could not
// disable it for writers already running. read_build_memory_signals() re-derives
// the share from the live process limit and the live percent at every decision.
//
// This drives the real singleton with the real reader, so the two process-level
// backstop terms come from the HOST. They would mask the share term, so the
// test skips when the host is itself under memory pressure.
TEST(SniiGlobalMemoryLimiter, ProductionReaderReDerivesTheShareAndZeroDisablesIt) {
    SharePercentRestore percent_restore;
    const int64_t saved_mem_limit = doris::MemInfo::mem_limit();
    // Small enough that a modest charge exceeds the share, but the floor tracks
    // the per-writer threshold, so shrink that too for the duration.
    const double saved_ram_buffer = doris::config::inverted_index_ram_buffer_size;
    doris::config::inverted_index_ram_buffer_size = 1; // 1 MiB -> 4 MiB floor
    doris::config::snii_index_build_max_memory_limit_percent = 10;
    doris::MemInfo::set_mem_limit_for_test(80 * kMiB); // 10% = 8 MiB > the 4 MiB floor

    struct Restore {
        int64_t mem_limit;
        double ram_buffer;
        ~Restore() {
            doris::MemInfo::set_mem_limit_for_test(mem_limit);
            doris::config::inverted_index_ram_buffer_size = ram_buffer;
        }
    } restore {saved_mem_limit, saved_ram_buffer};

    // The share really is re-derived from the live process limit and percent.
    ASSERT_EQ(read_build_memory_signals().build_share_bytes, 8 * kMiB);
    doris::MemInfo::set_mem_limit_for_test(160 * kMiB);
    ASSERT_EQ(read_build_memory_signals().build_share_bytes, 16 * kMiB);
    doris::MemInfo::set_mem_limit_for_test(80 * kMiB);

    if (read_build_memory_signals().sys_avail_below_warning_water_mark > 0 ||
        read_build_memory_signals().process_above_soft_mem_limit > 0) {
        GTEST_SKIP() << "host is under memory pressure; the backstop terms would mask the share";
    }

    // Charge the real observation tracker past the share, then let the real
    // reader drive a real limiter.
    const int64_t baseline = read_build_memory_signals().build_consumption;
    MemoryReporter reporter(doris::snii::writer::snii_build_consume_release(
            doris::snii::writer::BuildMemoryPopulation::kRegistered));
    reporter.report(20 * kMiB); // 20 MiB of reclaimable build memory vs an 8 MiB share
    ASSERT_EQ(read_build_memory_signals().build_consumption - baseline, 20 * kMiB);

    GlobalMemoryLimiter lim;
    lim.set_signals_provider(&read_build_memory_signals);
    lim.set_min_victim_arena_bytes(1);
    std::atomic<bool> victim {false};
    lim.register_buffer(&victim, 20 * kMiB);
    lim.report(&victim, 20 * kMiB);
    ASSERT_TRUE(victim.load()) << "with a share configured, the writer is asked to spill";
    victim.store(false);

    // The admin disables SNII's share mid-load. Nothing re-registers.
    doris::config::snii_index_build_max_memory_limit_percent = 0;
    ASSERT_EQ(read_build_memory_signals().build_share_bytes, 0);
    lim.report(&victim, 20 * kMiB);
    EXPECT_FALSE(victim.load())
            << "a zero share must stop flagging for writers that are already registered";
    reporter.report(-20 * kMiB);
}

// C2 (round 2): the property that makes the single-counter form safe. A thread
// hammering the UNREGISTERED population must not perturb the decision's input
// AT ALL -- not transiently, not by any interleaving. Under the old derived
// form (tracker - unregistered) this fails: a read landing between the
// tracker update and the subset update sees the compaction's bytes as
// reclaimable while it grows, and sees a deficit (possibly negative) while it
// releases. Reading one counter admits no such window.
TEST(SniiGlobalMemoryLimiter, UnregisteredChurnNeverPerturbsTheDecisionInput) {
    const int64_t registered_baseline = doris::snii::writer::snii_registered_build_bytes();
    MemoryReporter ingestion(doris::snii::writer::snii_build_consume_release(
            doris::snii::writer::BuildMemoryPopulation::kRegistered));
    ingestion.report(700); // the only registered bytes in flight

    std::atomic<bool> stop {false};
    std::thread churn([&stop] {
        MemoryReporter merge(doris::snii::writer::snii_build_consume_release(
                doris::snii::writer::BuildMemoryPopulation::kUnregistered));
        while (!stop.load(std::memory_order_relaxed)) {
            merge.report(512 * 1024 * 1024); // a large, realistic merge reservation
            merge.report(-512 * 1024 * 1024);
        }
    });

    // Sample the decision's actual input while the churn runs. Every sample must
    // be exactly the registered net -- never inflated by the merge's bytes,
    // never understated, never negative.
    //
    // saw_churn keeps this from passing VACUOUSLY. The observation tracker is
    // the operand the old derived form subtracted from; watching it actually
    // move proves the churn thread was running and that the sampling loop had
    // real opportunity to catch a torn read. Without it, a churn thread that
    // never got scheduled would make the invariant hold trivially.
    doris::MemTracker* tracker = doris::snii::writer::snii_build_mem_tracker();
    const int64_t tracker_baseline = tracker->consumption();
    bool saw_churn = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (int i = 0; i < 200000 || !saw_churn; ++i) {
        const int64_t observed =
                doris::snii::writer::snii_registered_build_bytes() - registered_baseline;
        ASSERT_EQ(observed, 700) << "unregistered churn leaked into the decision's input";
        ASSERT_GE(doris::snii::writer::snii_registered_build_bytes(), 0)
                << "decision input went negative";
        if (tracker->consumption() != tracker_baseline) {
            saw_churn = true; // the shared observation line is visibly moving
        }
        if (i >= 200000 && std::chrono::steady_clock::now() > deadline) {
            break;
        }
    }
    stop.store(true, std::memory_order_relaxed);
    churn.join();
    EXPECT_TRUE(saw_churn) << "churn thread never moved the observation tracker; the invariant "
                              "above was never actually exercised";

    ingestion.report(-700);
    EXPECT_EQ(doris::snii::writer::snii_registered_build_bytes(), registered_baseline);
}

// C2: index-merge compaction charges the same observation line but registers no
// spillable writer, so its bytes must NOT push ingestion writers over the share.
TEST(SniiGlobalMemoryLimiter, UnregisteredBuildMemoryIsExcludedFromTheDecision) {
    const int64_t before = read_build_memory_signals().build_consumption;
    MemoryReporter merge(doris::snii::writer::snii_build_consume_release(
            doris::snii::writer::BuildMemoryPopulation::kUnregistered));
    merge.report(512 * kMiB);
    EXPECT_EQ(read_build_memory_signals().build_consumption, before)
            << "unreclaimable merge scratch must not count toward the reclaimable population";
    merge.report(-512 * kMiB);
}

TEST(SniiGlobalMemoryLimiter, ProcessBackstopsTriggerEvenWithTheShareDisabled) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> a {false};
    lim.register_buffer(&a, 300 * kMiB);

    // Share disabled and SNII tiny: its own trigger says nothing.
    signals.build_share_bytes.store(0);
    signals.build_consumption.store(10 * kMiB);
    lim.report(&a, 300 * kMiB);
    ASSERT_FALSE(a.load());

    // System available memory dips below its warning water mark: SNII gives
    // back what it can even though its own share is not configured.
    signals.sys_avail_below_warning_water_mark.store(100 * kMiB);
    lim.report(&a, 300 * kMiB);
    EXPECT_TRUE(a.load()) << "the system water mark is a backstop trigger";
    a.store(false);
    signals.sys_avail_below_warning_water_mark.store(0);

    // Same for the process soft limit.
    signals.process_above_soft_mem_limit.store(100 * kMiB);
    lim.report(&a, 300 * kMiB);
    EXPECT_TRUE(a.load()) << "the process soft limit is a backstop trigger";
}

// ---- (3) victim selection ---------------------------------------------------

// MiB-scale arenas keep the selection shape visible while every entry clears
// the default 64 MiB victim floor.
TEST(SniiGlobalMemoryLimiter, OverShareFlagsLargestUntilOverageCovered) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> a {false}; // the largest arena
    std::atomic<bool> b {false};
    std::atomic<bool> c {false};
    lim.register_buffer(&a, 900 * kMiB);
    lim.register_buffer(&b, 500 * kMiB);
    lim.register_buffer(&c, 100 * kMiB);
    signals.set_overage(500 * kMiB);
    // Registration alone never flags: reacting belongs to the report path.
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // Overage 500 MiB: the largest (a, 900 MiB of arena) alone covers it; b
    // and c spared.
    lim.report(&c, 100 * kMiB);
    EXPECT_TRUE(a.load());
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // a's request is still pending -- its arena counts toward coverage, so a
    // re-report while still over the share must NOT widen the victim set.
    lim.report(&c, 100 * kMiB);
    EXPECT_FALSE(b.load());
    EXPECT_FALSE(c.load());

    // Deeper overage (1200 MiB): a (900) is no longer enough, b joins
    // (900 + 500 >= 1200), c is still spared.
    signals.set_overage(1200 * kMiB);
    lim.report(&c, 100 * kMiB);
    EXPECT_TRUE(a.load());
    EXPECT_TRUE(b.load());
    EXPECT_FALSE(c.load());
}

TEST(SniiGlobalMemoryLimiter, UnderShareNeverFlags) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> a {false};
    std::atomic<bool> b {false};
    lim.register_buffer(&a, 600 * kMiB);
    lim.register_buffer(&b, 300 * kMiB);
    signals.build_share_bytes.store(1000 * kMiB);
    signals.build_consumption.store(1000 * kMiB); // at, not over, the share
    lim.report(&a, 700 * kMiB);
    EXPECT_FALSE(a.load());
    EXPECT_FALSE(b.load());
}

// The field storm's root selection bug: victims were ranked by a RESIDENT total
// dominated by PERSISTENT (non-spillable) vocabulary and slot structures.
// Victims must be ranked by their reclaimable ARENA, and a buffer below the
// victim floor is not a victim at all.
TEST(SniiGlobalMemoryLimiter, VictimsSelectedByReclaimableArenaNotTotalMemory) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> persistent_heavy {false};
    std::atomic<bool> arena_heavy {false};
    // A huge persistent footprint with a tiny arena (8 MiB < the 64 MiB floor).
    lim.register_buffer(&persistent_heavy, 8 * kMiB);
    // Smaller overall, but with a large reclaimable arena.
    lim.register_buffer(&arena_heavy, 300 * kMiB);
    EXPECT_EQ(lim.eligible_arena_bytes(), 300 * kMiB)
            << "a below-floor arena offers nothing a forced spill could reclaim";

    signals.set_overage(200 * kMiB);
    lim.report(&persistent_heavy, 8 * kMiB);
    EXPECT_TRUE(arena_heavy.load()) << "the reclaimable-arena holder is the victim";
    EXPECT_FALSE(persistent_heavy.load())
            << "a below-floor arena must never be flagged, however large its total "
               "footprint";
}

// PER-BUFFER COOLDOWN: after a victim's forced spill its arena is ~0; further
// over-share reports must NOT re-flag it until the arena regrows past the
// floor. (No timer: eligibility IS the cooldown.)
TEST(SniiGlobalMemoryLimiter, CooldownSkipsJustSpilledBufferUntilArenaRegrows) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> flag {false};
    lim.register_buffer(&flag, 200 * kMiB);

    signals.set_overage(200 * kMiB);
    lim.report(&flag, 200 * kMiB); // over the share, arena covers the overage
    EXPECT_TRUE(flag.load());
    flag.store(false); // the owner honors: spill, clear the flag...
    // ...and its next report shows the arena reclaimed while the PERSISTENT
    // remainder keeps SNII a little over its share (the field shape).
    signals.set_overage(10 * kMiB);
    lim.report(&flag, 0);
    EXPECT_FALSE(flag.load()) << "cooldown: a spilled (empty-arena) buffer is exempt";
    signals.set_overage(40 * kMiB);
    lim.report(&flag, 32 * kMiB); // regrown, still below the floor
    EXPECT_FALSE(flag.load()) << "still exempt below the floor";
    signals.set_overage(60 * kMiB);
    lim.report(&flag, 72 * kMiB); // regrown PAST the 64 MiB floor
    EXPECT_TRUE(flag.load()) << "eligible again once a floor's worth is reclaimable";
}

// SHORTFALL IS NOT A REASON TO DO NOTHING. When the reclaimable arena cannot
// cover the overage, the limiter must still flag every eligible victim: the
// overage often exceeds anything SNII holds (two of its three terms measure
// whole-process pressure), and refusing to act there would make SNII least
// willing to give memory back exactly when the system is most short of it --
// a control loop with no exit but writers finishing naturally. The victim
// FLOOR, not a reachability judgement, is what bounds the work.
TEST(SniiGlobalMemoryLimiter, ShortfallStillFlagsBestEffortAndIsReported) {
    FakeSignals signals;
    GlobalMemoryLimiter lim; // production default 64 MiB victim floor
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> a {false};
    // 70 MiB of reclaimable arena (above the floor) against a ~10 GiB overage.
    lim.register_buffer(&a, 70 * kMiB);
    signals.set_overage(10LL * 1024 * kMiB, /*share=*/100 * kMiB);
    lim.report(&a, 70 * kMiB);
    EXPECT_TRUE(a.load()) << "70 MiB that can come back must come back, even against 10 GiB";
    EXPECT_TRUE(lim.reclaim_shortfall()) << "and the shortfall must be visible";
}

// The wikipedia@16 shape: build memory far over the share, arena a minority of
// it. This is the workload the feature exists for, so it must produce flags.
TEST(SniiGlobalMemoryLimiter, DeepShortfallFlagsEveryEligibleVictim) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> a {false};
    std::atomic<bool> b {false};
    std::atomic<bool> c {false};
    lim.register_buffer(&a, 150 * kMiB);
    lim.register_buffer(&b, 150 * kMiB);
    lim.register_buffer(&c, 150 * kMiB);
    ASSERT_EQ(lim.eligible_arena_bytes(), 450 * kMiB);
    EXPECT_FALSE(lim.reclaim_shortfall());

    // 35 GiB of overage against 450 MiB of reclaimable arena.
    signals.set_overage(35LL * 1024 * kMiB, /*share=*/6LL * 1024 * kMiB);
    lim.report(&a, 150 * kMiB);
    EXPECT_TRUE(lim.reclaim_shortfall());
    EXPECT_TRUE(a.load());
    EXPECT_TRUE(b.load());
    EXPECT_TRUE(c.load()) << "every eligible victim must be asked, not none of them";
}

// reclaim_shortfall() states whether the CURRENT eligible arena falls short, so
// pressure going away has to clear it. The recovery branch only runs while an
// overage still exists, which leaves the commonest ending -- writers drain, the
// query finishes, RSS falls back -- unhandled. Left latched, the flag would
// outlive the episode and, because the warning is log-once-per-episode, silence
// every later episode for the life of the process.
TEST(SniiGlobalMemoryLimiter, PressureGoingAwayClearsTheShortfallLatch) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> a {false};
    lim.register_buffer(&a, 100 * kMiB);

    signals.set_overage(20LL * 1024 * kMiB, /*share=*/1LL * 1024 * kMiB);
    lim.report(&a, 100 * kMiB);
    ASSERT_TRUE(lim.reclaim_shortfall()) << "20 GiB against 100 MiB of arena is a shortfall";

    // Pressure gone: consumption back under the share, so overage <= 0.
    signals.set_overage(-1LL * 1024 * kMiB, /*share=*/1LL * 1024 * kMiB);
    lim.report(&a, 100 * kMiB);
    EXPECT_FALSE(lim.reclaim_shortfall()) << "the episode is over, so the flag must not survive it";

    // And a fresh episode must be observable as one rather than hidden by the
    // stale latch -- this is what the log-once contract depends on.
    signals.set_overage(20LL * 1024 * kMiB, /*share=*/1LL * 1024 * kMiB);
    lim.report(&a, 100 * kMiB);
    EXPECT_TRUE(lim.reclaim_shortfall()) << "a relapse must be visible again";
}

// A whole-process backstop can demand more than SNII holds; SNII must still
// return what it has rather than declaring the request unmeetable.
TEST(SniiGlobalMemoryLimiter, ProcessPressureBeyondSniiStillReclaimsWhatSniiHolds) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    std::atomic<bool> a {false};
    lim.register_buffer(&a, 100 * kMiB);
    // SNII is comfortably inside its share; a query pushed the PROCESS over its
    // soft limit by more than SNII's entire arena.
    signals.build_share_bytes.store(6LL * 1024 * kMiB);
    signals.build_consumption.store(1LL * 1024 * kMiB);
    signals.process_above_soft_mem_limit.store(2400 * kMiB);
    lim.report(&a, 100 * kMiB);
    EXPECT_TRUE(a.load()) << "100 MiB that can come back in seconds must come back";
    EXPECT_TRUE(lim.reclaim_shortfall());

    // Once the pressure is within reach, the shortfall latch clears.
    signals.process_above_soft_mem_limit.store(50 * kMiB);
    a.store(false);
    lim.report(&a, 100 * kMiB);
    EXPECT_FALSE(lim.reclaim_shortfall());
    EXPECT_TRUE(a.load());
}

// ---- (4) the owner honors a pending request ---------------------------------

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

// ---- (7a) forced-spill floor: below-floor requests are pending no-ops -------

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

// ---- (7b) the storm scenario end-to-end -------------------------------------

// The conc=16 wikipedia field failure in miniature, with PRODUCTION-DEFAULT
// anti-storm settings: SNII far over a tiny share while every writer's
// reclaimable arena is small. The old limiter flagged every buffer on every
// report and each honored with one 32 KiB block -- a storm of tiny runs. Now
// the victim FLOOR makes every small-arena buffer ineligible, so no victim
// exists at all and ZERO forced spills and ZERO runs result.
//
// HONEST SCOPE: this test pins the floor, and only the floor. It says nothing
// about how the limiter behaves when the arena is short of the overage -- with
// no eligible victim the selection loop is empty either way. See
// ShortfallStillFlagsBestEffortAndIsReported for that property.
TEST(SniiSpimiGlobalSpill, StormScenarioTinySharesSmallArenasProducesZeroForcedSpills) {
    snii_testing::reset_global_forced_spills();
    FakeSignals signals;
    GlobalMemoryLimiter lim; // production defaults: 64 MiB victim floor
    lim.set_signals_provider(signals.provider());
    signals.set_overage(8 * kMiB, /*share=*/256 * 1024);

    constexpr size_t kBuffers = 6;
    std::vector<std::unique_ptr<SpimiTermBuffer>> buffers;
    buffers.reserve(kBuffers);
    for (size_t i = 0; i < kBuffers; ++i) {
        auto buf = std::make_unique<SpimiTermBuffer>(/*has_positions=*/true,
                                                     /*spill_threshold_bytes=*/0);
        buf->attach_global_limiter(&lim);
        buffers.push_back(std::move(buf));
    }
    // Interleaved feed: reports fire constantly while SNII sits far over its
    // share, yet nothing may be flagged or spilled.
    for (uint32_t round = 0; round < 400; ++round) {
        for (auto& buf : buffers) {
            for (uint32_t k = 0; k < 8; ++k) {
                buf->add_token(unigram((round * 8 + k) % 2000), /*docid=*/round * 8 + k,
                               /*pos=*/0);
            }
        }
    }
    EXPECT_EQ(lim.eligible_arena_bytes(), 0) << "every arena is below the victim floor";
    EXPECT_EQ(snii_testing::global_forced_spills(), 0U) << "no forced-spill storm";
    for (auto& buf : buffers) {
        ASSERT_TRUE(buf->status().ok());
        EXPECT_EQ(buf->run_count_for_test(), 0U) << "no runs were cut";
        EXPECT_FALSE(buf->global_spill_requested_for_test()) << "no flags were planted";
    }
}

// ---- (5) attach / report / detach lifetime ----------------------------------

TEST(SniiSpimiGlobalSpill, AttachRegistersReportsTrackArenaAndDtorUnregisters) {
    FakeSignals signals; // no pressure: tracking only, no flags
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    lim.set_min_victim_arena_bytes(1); // every non-empty arena counts
    {
        SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);
        buf.attach_global_limiter(&lim);
        EXPECT_EQ(lim.registered_count(), 1U);
        EXPECT_EQ(lim.eligible_arena_bytes(), static_cast<int64_t>(buf.arena_bytes_for_test()));

        for (uint32_t i = 0; i < 300; ++i) {
            buf.add_token(unigram(i), /*docid=*/i, /*pos=*/0);
        }
        ASSERT_TRUE(buf.status().ok());
        // The debounced report path forwards ABSOLUTE arena bytes: at rest the
        // registry equals the buffer's real reclaimable arena exactly.
        EXPECT_EQ(lim.eligible_arena_bytes(), static_cast<int64_t>(buf.arena_bytes_for_test()));

        buf.attach_global_limiter(&lim); // at-most-once: ignored
        EXPECT_EQ(lim.registered_count(), 1U);
    }
    // Destruction un-registers: nothing leaks into the process-wide registry.
    EXPECT_EQ(lim.registered_count(), 0U);
    EXPECT_EQ(lim.eligible_arena_bytes(), 0);
}

// End-to-end: two attached buffers, one small and one that grows. The limiter
// must flag the larger-ARENA grower once its arena clears the victim floor (the
// small buffer's single arena block never does); the grower's own next token
// honors the request (its local threshold is unlimited, the G08 floor
// bypassed); the small buffer is never flagged and never spills.
TEST(SniiSpimiGlobalSpill, LimiterFlagsLargestOwnerSpillsSmallBufferSpared) {
    snii_testing::reset_global_forced_spills();
    FakeSignals signals;
    GlobalMemoryLimiter lim; // declared BEFORE the buffers: outlives them
    lim.set_signals_provider(signals.provider());
    const int64_t kFloor = 2LL * CompactPostingPool::kBlockSize; // 64 KiB
    lim.set_min_victim_arena_bytes(kFloor);
    // A modest, fixed overage: one floor-sized arena covers it, so exactly one
    // victim is expected as soon as any buffer's arena clears the floor.
    signals.set_overage(kFloor);

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

    // Distinct terms grow big's ARENA past the floor; the next report then
    // flags big (the largest eligible arena), and big's own add path honors the
    // request. Bounded feed with an early exit. The bound stays below
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

    // One more token flushes the post-spill report; the registry is then back
    // in step with the live reclaimable arenas (the forced spill released
    // big's, so it drops below the floor and stops being a victim).
    big.add_token(unigram(0), docid++, /*pos=*/0);
    int64_t expected_eligible = 0;
    if (static_cast<int64_t>(small.arena_bytes_for_test()) >= kFloor) {
        expected_eligible += static_cast<int64_t>(small.arena_bytes_for_test());
    }
    if (static_cast<int64_t>(big.arena_bytes_for_test()) >= kFloor) {
        expected_eligible += static_cast<int64_t>(big.arena_bytes_for_test());
    }
    EXPECT_EQ(lim.eligible_arena_bytes(), expected_eligible);

    // Both buffers still drain cleanly (the small one in memory, the big one
    // through its forced run + k-way merge).
    size_t small_terms = 0;
    ASSERT_TRUE(small.for_each_term_sorted([&small_terms](StreamedTermPostings&& source) {
                         RETURN_IF_ERROR(
                                 doris::snii::writer::consume_streamed_term(std::move(source)));
                         ++small_terms;
                         return Status::OK();
                     }).ok());
    EXPECT_EQ(small_terms, 50U);
    size_t big_terms = 0;
    ASSERT_TRUE(big.for_each_term_sorted([&big_terms](StreamedTermPostings&& source) {
                       RETURN_IF_ERROR(
                               doris::snii::writer::consume_streamed_term(std::move(source)));
                       ++big_terms;
                       return Status::OK();
                   }).ok());
    EXPECT_EQ(big_terms, fed);
}

// ---- (6) thread-safety canary
// ------------------------------------------------

// Concurrent register / report / unregister with a tiny overage so cross-thread
// flagging constantly targets flags that die right after their unregister --
// the exact lifetime the mutex must protect. Run under TSAN this is the G09
// race canary; under ASAN it still catches any touch-after-unregister. The
// floor is dropped so the byte-scale entries keep producing flags (the point is
// flag traffic, not selection policy).
TEST(SniiGlobalMemoryLimiter, ConcurrentRegisterReportUnregisterIsClean) {
    FakeSignals signals;
    GlobalMemoryLimiter lim;
    lim.set_signals_provider(signals.provider());
    lim.set_min_victim_arena_bytes(1);
    signals.set_overage(1, /*share=*/1); // always over: flags fly
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
    EXPECT_EQ(lim.eligible_arena_bytes(), 0);
    EXPECT_EQ(lim.registered_count(), 0U);
}

} // namespace
