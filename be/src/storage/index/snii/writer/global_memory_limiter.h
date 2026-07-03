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
// and forwards its ACCURATE resident-byte total (G08) through its existing
// debounced report path; when the sum across ALL writers of the process
// (tablets x segments x concurrent loads) exceeds the configured budget, the
// limiter requests spills from the LARGEST registered buffers until the
// flagged sum covers the overage.
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
// still requiring one allocated arena block so a run is writable). Flags are
// ADVISORY: the owner may have just spilled or drained -- the flag is then a
// (harmless) no-op or one extra small run. The limiter itself only ever takes
// its registry mutex and flips atomics; it never blocks a reporting thread
// beyond that mutex and never calls back into a buffer.
//
// LIFETIME: buffers un-register in their destructor. register / report /
// unregister all serialize on the registry mutex, and flags are only ever set
// UNDER that mutex, so once unregister_buffer returns no thread can touch the
// (about-to-die) flag again. The limiter must outlive every attached buffer
// (trivial for the process singleton; test-local instances are declared before
// the buffers they serve).
//
// COVERAGE CAVEAT: a forced spill releases only the buffer's posting ARENA;
// the persistent vocab / pair-map structures survive it (the reason the G08
// floor exists per-writer). If the persistent bytes of all writers alone
// exceed the budget, forced spills converge to small runs without ever getting
// under it -- the budget is a back-pressure valve, not a hard cap. Size it
// above (writers x persistent-share); the default 8 GiB is ~16x the observed
// per-writer persistent footprint.
class GlobalMemoryLimiter {
public:
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

    // Adds `spill_flag` (the owning buffer's advisory request flag; also the
    // entry's identity) with its current resident bytes. Re-registering an
    // already-registered flag just updates its bytes. Never sets flags itself:
    // a single registration cannot create NEW overage worth reacting to before
    // the buffer's first report.
    void register_buffer(std::atomic<bool>* spill_flag, int64_t resident_bytes);

    // Updates the entry's resident bytes (an ABSOLUTE total, not a delta --
    // self-healing across any missed report). When the registered sum exceeds
    // the budget, sets the request flags of the largest entries -- counting
    // entries whose flag is ALREADY pending toward the covered sum, so an
    // in-flight request is not amplified -- until the flagged bytes cover the
    // overage. A report for a flag that is not registered is ignored.
    void report(std::atomic<bool>* spill_flag, int64_t resident_bytes);

    // Removes the entry (subtracting its bytes from the total). After this
    // returns, the limiter never touches `spill_flag` again -- safe to destroy
    // the owning buffer.
    void unregister_buffer(std::atomic<bool>* spill_flag);

    // Registered sum / entry count, for tests and observability.
    int64_t total_bytes() const;
    size_t registered_count() const;

private:
    // Called with mutex_ held whenever the total may exceed the budget: sorts
    // the entries by bytes descending and flags from the top until the flagged
    // sum covers (total - budget). O(n log n) over the live writer count (at
    // most a few hundred) -- bounded work under the mutex, no I/O, no
    // callbacks.
    void request_spills_locked();

    mutable std::mutex mutex_;
    std::atomic<int64_t> budget_bytes_ {0};
    // Both guarded by mutex_. total_bytes_locked_ is the maintained sum of
    // entries_ values (avoids an O(n) walk per report).
    int64_t total_bytes_locked_ = 0;
    phmap::flat_hash_map<std::atomic<bool>*, int64_t> entries_;
};

} // namespace doris::snii::writer
