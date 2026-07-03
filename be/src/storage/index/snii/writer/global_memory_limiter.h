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

#pragma once

#include <parallel_hashmap/phmap.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>

namespace doris::snii::writer {

// Process-wide SNII build-RAM limiter (G09) -- the index-build analogue of
// Doris's MemTableMemoryLimiter. Every live SPIMI accumulator registers here
// and forwards its ACCURATE resident-byte total (G08) plus its SPILLABLE
// arena bytes through its existing debounced report path; when the resident
// sum across ALL writers of the process (tablets x segments x concurrent
// loads) exceeds the configured budget, the limiter requests spills from the
// largest-ARENA eligible buffers until the flagged (reclaimable) arena sum
// covers the overage.
//
// WHY: the per-writer gate-2 cap (e.g. 512 MiB) bounds ONE writer, but a load
// keeps (tablets x concurrency) writers alive at once -- wikipedia at
// concurrency 16 held 100+ writers at 300-500 MB each (~41 GiB), none of which
// ever reached its own cap, so per-writer spilling never fired. This registry
// bounds the SUM.
//
// ASYNC-SAFE REQUESTS: the SPIMI structures are single-threaded, so the
// limiter must never spill on the reporting thread. A request is a relaxed
// atomic FLAG on the target buffer (SpimiTermBuffer::global_spill_requested_)
// that the OWNER's next add_token / maybe_spill_after_token observes and
// honors on its own thread (bypassing the G08 per-writer anti-churn floor but
// still requiring the FORCED-SPILL FLOOR of reclaimable arena -- see below --
// so every forced run is worth its fixed costs). Flags are ADVISORY: the
// owner may have just spilled or drained -- the flag is then a (harmless)
// no-op or one extra floor-sized run. The limiter itself only ever takes its
// registry mutex and flips atomics; it never blocks a reporting thread beyond
// that mutex and never calls back into a buffer.
//
// LIFETIME: buffers un-register in their destructor. register / report /
// unregister all serialize on the registry mutex, and flags are only ever set
// UNDER that mutex, so once unregister_buffer returns no thread can touch the
// (about-to-die) flag again. The limiter must outlive every attached buffer
// (trivial for the process singleton; test-local instances are declared before
// the buffers they serve).
//
// THE BUDGET BOUNDS SPILLABLE MEMORY, NOT PERSISTENT MEMORY: a forced spill
// releases only the buffer's posting ARENA; the persistent vocab / pair-map /
// slot structures (~100-500 MB per wikipedia writer) survive it. If the
// persistent bytes of all writers alone exceed the budget, NO amount of
// spilling can reach the budget -- the budget is a back-pressure valve over
// the reclaimable arenas, not a hard cap on resident RSS. Three defenses keep
// an unreachable budget from degenerating into a forced-spill storm (the
// conc=16 wikipedia field failure: every report re-flagged every buffer, each
// honoring with one 32 KiB arena block -> thousands of tiny runs per buffer ->
// EMFILE re-opening them for the k-way merge -> failed loads):
//   * VICTIMS BY ARENA: victims are selected by their reported SPILLABLE arena
//     bytes -- the only bytes a forced spill can actually reclaim -- never by
//     the persistent-dominated resident total, and only buffers whose arena is
//     at least min_victim_arena_bytes (config snii_forced_spill_min_arena_bytes)
//     are eligible. Every forced run is therefore at least floor-sized.
//   * PER-BUFFER COOLDOWN: right after a buffer honors a forced spill its
//     arena is ~0, below the victim floor, so it is EXEMPT from new flags
//     until the arena regrows past the floor. No timer state: the eligibility
//     rule IS the cooldown.
//   * BUDGET SANITY: when budget / registered_count falls below a per-writer
//     minimum useful share (kMinUsefulBudgetPerWriterBytes), persistent
//     structures alone dominate and the budget is provably unreachable by
//     spilling; the limiter LOGS ONCE (per degradation episode) and stops
//     flagging until the ratio recovers (writers finishing / budget raised).
class GlobalMemoryLimiter {
public:
    // Victim floor default (mirrors config snii_forced_spill_min_arena_bytes):
    // a buffer is only ever asked to force-spill once its reclaimable arena
    // holds at least this much, so no forced run is smaller than this.
    static constexpr int64_t kDefaultMinVictimArenaBytes = 64LL << 20; // 64 MiB
    // Minimum useful per-writer budget share: below budget/registered_count ==
    // this, spilling cannot meet the budget (persistent per-writer structures
    // alone exceed the share) -- the limiter degrades to log-once-and-stop.
    static constexpr int64_t kMinUsefulBudgetPerWriterBytes = 96LL << 20; // 96 MiB

    // Local instances are constructible for unit tests; production code uses
    // the process singleton below.
    GlobalMemoryLimiter() = default;
    GlobalMemoryLimiter(const GlobalMemoryLimiter&) = delete;
    GlobalMemoryLimiter& operator=(const GlobalMemoryLimiter&) = delete;

    // Process singleton (never destroyed before the writers that use it).
    static GlobalMemoryLimiter* instance();

    // Budget in bytes across every registered buffer; <= 0 disables the
    // limiter (registration still tracked, but no flag is ever set). Refreshed
    // from the mutable BE config at each writer init, so a config change takes
    // effect for the whole registry at the next writer creation.
    void set_budget_bytes(int64_t budget_bytes) {
        budget_bytes_.store(budget_bytes, std::memory_order_relaxed);
    }
    int64_t budget_bytes() const { return budget_bytes_.load(std::memory_order_relaxed); }

    // Victim-eligibility floor over a buffer's reported SPILLABLE arena bytes
    // (see the class comment). Refreshed from the mutable BE config alongside
    // the budget at each writer init. Values < 1 behave as 1 (an empty arena
    // is never a victim -- there would be nothing to write to the run).
    void set_min_victim_arena_bytes(int64_t bytes) {
        min_victim_arena_bytes_.store(bytes, std::memory_order_relaxed);
    }
    int64_t min_victim_arena_bytes() const {
        return min_victim_arena_bytes_.load(std::memory_order_relaxed);
    }

    // Per-writer minimum useful budget share for the degradation check.
    // Production keeps the default (kMinUsefulBudgetPerWriterBytes); tests
    // lower it to exercise selection with synthetic byte scales.
    void set_min_useful_budget_per_writer_bytes(int64_t bytes) {
        min_useful_budget_per_writer_bytes_.store(bytes, std::memory_order_relaxed);
    }

    // True while the limiter is in the degraded log-once-and-stop state (the
    // per-writer budget share fell below the useful minimum at the last
    // over-budget report). Observability / tests.
    bool budget_degraded() const;

    // Adds `spill_flag` (the owning buffer's advisory request flag; also the
    // entry's identity) with its current resident bytes and its SPILLABLE
    // arena bytes (<= resident; what a forced spill can reclaim, the victim
    // selection key). Re-registering an already-registered flag just updates
    // its bytes. Never sets flags itself: a single registration cannot create
    // NEW overage worth reacting to before the buffer's first report.
    void register_buffer(std::atomic<bool>* spill_flag, int64_t resident_bytes,
                         int64_t arena_bytes);

    // Updates the entry's resident and spillable-arena bytes (ABSOLUTE totals,
    // not deltas -- self-healing across any missed report). When the
    // registered RESIDENT sum exceeds the budget, sets the request flags of
    // the largest-ARENA eligible entries (arena >= the victim floor; see the
    // class comment) -- counting entries whose flag is ALREADY pending toward
    // the covered sum, so an in-flight request is not amplified -- until the
    // flagged ARENA bytes cover the overage or eligible entries run out. A
    // report for a flag that is not registered is ignored.
    void report(std::atomic<bool>* spill_flag, int64_t resident_bytes, int64_t arena_bytes);

    // Removes the entry (subtracting its bytes from the total). After this
    // returns, the limiter never touches `spill_flag` again -- safe to destroy
    // the owning buffer.
    void unregister_buffer(std::atomic<bool>* spill_flag);

    // Registered sum / entry count, for tests and observability.
    int64_t total_bytes() const;
    size_t registered_count() const;

private:
    // One registered buffer's last reported byte totals. `resident` feeds the
    // over-budget decision (the sum the budget bounds); `arena` -- the
    // SPILLABLE subset a forced spill can actually reclaim -- feeds victim
    // selection and eligibility.
    struct Entry {
        int64_t resident = 0;
        int64_t arena = 0;
    };

    // Called with mutex_ held whenever the total may exceed the budget: after
    // the budget-sanity check (degrade to log-once-and-stop when the
    // per-writer share is below the useful minimum), sorts the ELIGIBLE
    // entries (arena >= victim floor) by ARENA descending and flags from the
    // top until the flagged arena sum covers (total - budget) or eligible
    // entries run out. O(n log n) over the live writer count (at most a few
    // hundred) -- bounded work under the mutex, no I/O, no callbacks.
    void request_spills_locked();

    mutable std::mutex mutex_;
    std::atomic<int64_t> budget_bytes_ {0};
    std::atomic<int64_t> min_victim_arena_bytes_ {kDefaultMinVictimArenaBytes};
    std::atomic<int64_t> min_useful_budget_per_writer_bytes_ {kMinUsefulBudgetPerWriterBytes};
    // All below guarded by mutex_. total_bytes_locked_ is the maintained sum
    // of entries_ resident values (avoids an O(n) walk per report).
    int64_t total_bytes_locked_ = 0;
    // Degradation episode latch: set (with ONE warning log) when the budget
    // sanity check fails, cleared when a later over-budget report finds the
    // ratio recovered -- so a relapse logs again, but a sustained episode
    // logs exactly once.
    bool degraded_ = false;
    phmap::flat_hash_map<std::atomic<bool>*, Entry> entries_;
};

} // namespace doris::snii::writer
