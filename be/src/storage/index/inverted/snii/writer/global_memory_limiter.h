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
#include <functional>
#include <mutex>

namespace doris::snii::writer {

// Byte-valued memory-pressure signals the forced-spill decision is judged
// against. Production fills these from Doris's own global memory state; unit
// tests inject deterministic values.
//
// The three pressure sources mirror MemTableMemoryLimiter's hard-limit test:
// the subsystem's own share first, then the two process-level backstops. The
// ORDER matters -- SNII's own share is set well below the backstops so SNII
// sheds its own memory before the global valve, which trips late by design.
struct BuildMemorySignals {
    // Live SNII index-build bytes -- the SniiIndexBuild observation tracker's
    // consumption, covering ingestion writers and index-merge compaction alike.
    int64_t build_consumption = 0;
    // SNII's share of the process memory limit. <= 0 disables the share
    // trigger; the backstops below still apply.
    int64_t build_share_bytes = 0;
    // Bytes by which system available memory sits BELOW its warning water mark
    // (<= 0 when the system is not short).
    int64_t sys_avail_below_warning_water_mark = 0;
    // Bytes by which process memory usage sits ABOVE the process soft limit
    // (<= 0 when the process is not over it).
    int64_t process_above_soft_mem_limit = 0;

    // Bytes that must be reclaimed to clear every active pressure source; 0
    // when nothing is under pressure. The maximum, not the sum: satisfying the
    // deepest deficit satisfies the others.
    int64_t overage() const;
};

// SNII's index-build share of `process_mem_limit`, from the mutable percent
// config (snii_index_build_max_memory_limit_percent). Returns 0 when the share
// trigger is disabled, and -1 for an unlimited process (mirroring
// MemInfo::mem_limit()'s -1 convention), which also disables the share trigger.
int64_t calc_process_max_snii_build_memory(int64_t process_mem_limit);

// Production signal reader: the SniiIndexBuild tracker, the share above, and
// Doris's system/process memory state.
BuildMemorySignals read_build_memory_signals();

// Requests spills from the largest reclaimable SNII posting arenas when index-build memory exceeds its share or the process is under pressure. Requests are atomic flags handled by each buffer's owner thread; registration protects flag lifetime, and a minimum arena size prevents tiny forced runs.
class GlobalMemoryLimiter {
public:
    // Signals provider; swapped out by unit tests. Invoked under the registry
    // mutex, so it must be cheap and must not call back into the limiter.
    using SignalsFn = std::function<BuildMemorySignals()>;

    // Victim floor default (mirrors config snii_forced_spill_min_arena_bytes):
    // a buffer is only ever asked to force-spill once its reclaimable arena
    // holds at least this much, so no forced run is smaller than this.
    static constexpr int64_t kDefaultMinVictimArenaBytes = 64LL << 20; // 64 MiB

    // Local instances are constructible for unit tests; production code uses
    // the process singleton below.
    GlobalMemoryLimiter();
    GlobalMemoryLimiter(const GlobalMemoryLimiter&) = delete;
    GlobalMemoryLimiter& operator=(const GlobalMemoryLimiter&) = delete;

    // Process singleton (never destroyed before the writers that use it).
    static GlobalMemoryLimiter* instance();

    // Replaces the memory-pressure signals the decision is judged against.
    // Production keeps the default (read_build_memory_signals); tests inject
    // deterministic values.
    void set_signals_provider(SignalsFn signals);

    // Victim-eligibility floor over a buffer's reported SPILLABLE arena bytes
    // (see the class comment). Refreshed from the mutable BE config at each
    // writer init. Values < 1 behave as 1 (an empty arena is never a victim --
    // there would be nothing to write to the run).
    void set_min_victim_arena_bytes(int64_t bytes) {
        min_victim_arena_bytes_.store(bytes, std::memory_order_relaxed);
    }
    int64_t min_victim_arena_bytes() const {
        return min_victim_arena_bytes_.load(std::memory_order_relaxed);
    }

    // True while the eligible reclaimable arena is short of the overage: every
    // eligible victim is being flagged and it still will not be enough. Purely
    // observability -- it never gates flagging.
    bool reclaim_shortfall() const;

    // Adds `spill_flag` (the owning buffer's advisory request flag; also the
    // entry's identity) with its SPILLABLE arena bytes -- what a forced spill
    // can reclaim, and the victim selection key. Re-registering an
    // already-registered flag just updates its bytes. Never sets flags itself:
    // a single registration cannot create NEW pressure worth reacting to
    // before the buffer's first report.
    void register_buffer(std::atomic<bool>* spill_flag, int64_t arena_bytes);

    // Updates the entry's spillable-arena bytes (an ABSOLUTE total, not a delta
    // -- self-healing across any missed report) and re-decides. When SNII's
    // build memory is over its share (or the process is under pressure), sets
    // the request flags of the largest-ARENA eligible entries (arena >= the
    // victim floor; see the class comment) -- counting entries whose flag is
    // ALREADY pending toward the covered sum, so an in-flight request is not
    // amplified -- until the flagged ARENA bytes cover the overage or the
    // eligible victims run out. A report for a flag that is not registered is
    // ignored.
    void report(std::atomic<bool>* spill_flag, int64_t arena_bytes);

    // Removes the entry. After this returns, the limiter never touches
    // `spill_flag` again -- safe to destroy the owning buffer.
    void unregister_buffer(std::atomic<bool>* spill_flag);

    // Reclaimable arena summed over the ELIGIBLE entries (arena >= the victim
    // floor) -- the exact quantity a round of forced spills could free, and
    // the left-hand side of the reachability test. Tests and observability.
    int64_t eligible_arena_bytes() const;
    size_t registered_count() const;

private:
    // Called with mutex_ held on every report: reads the pressure signals,
    // and if there is an overage, sorts the ELIGIBLE entries (arena >= victim
    // floor) by ARENA descending and flags from the top until the flagged arena
    // sum covers it or the victims run out. O(n log n) over the live writer
    // count (at most a few hundred) -- bounded work under the mutex, no I/O,
    // no callbacks.
    void request_spills_locked();

    mutable std::mutex mutex_;
    std::atomic<int64_t> min_victim_arena_bytes_ {kDefaultMinVictimArenaBytes};
    // All below guarded by mutex_.
    SignalsFn signals_;
    // Shortfall episode latch: set (with ONE warning log) when the eligible
    // arena is short of the overage, cleared when a later report finds it
    // sufficient again -- so a relapse logs again, but a sustained episode logs
    // exactly once. Observability only; flagging proceeds either way.
    bool reclaim_shortfall_ = false;
    // Live writers, keyed by their advisory flag: value is the buffer's last
    // reported SPILLABLE arena bytes (the victim selection key and the only
    // bytes a forced spill can reclaim).
    phmap::flat_hash_map<std::atomic<bool>*, int64_t> entries_;
};

} // namespace doris::snii::writer
