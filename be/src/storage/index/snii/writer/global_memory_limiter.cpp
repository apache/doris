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

#include "storage/index/snii/writer/global_memory_limiter.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "common/logging.h"

namespace doris::snii::writer {

GlobalMemoryLimiter* GlobalMemoryLimiter::instance() {
    // Intentionally leaked (never destroyed): buffers un-register from their
    // destructors, which may run during static teardown at process exit -- a
    // destroyed registry there would be use-after-free. A leaked singleton
    // makes the "limiter outlives every attached buffer" contract
    // unconditional.
    static auto* g_instance = new GlobalMemoryLimiter();
    return g_instance;
}

void GlobalMemoryLimiter::register_buffer(std::atomic<bool>* spill_flag, int64_t resident_bytes,
                                          int64_t arena_bytes) {
    std::lock_guard<std::mutex> guard(mutex_);
    Entry& slot = entries_[spill_flag]; // zero-initialized on first insert
    total_bytes_locked_ += resident_bytes - slot.resident;
    slot.resident = resident_bytes;
    slot.arena = arena_bytes;
}

void GlobalMemoryLimiter::report(std::atomic<bool>* spill_flag, int64_t resident_bytes,
                                 int64_t arena_bytes) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = entries_.find(spill_flag);
    if (it == entries_.end()) {
        // Not registered (or already unregistered): ignore rather than
        // resurrect an entry nobody will remove.
        return;
    }
    total_bytes_locked_ += resident_bytes - it->second.resident;
    it->second.resident = resident_bytes;
    it->second.arena = arena_bytes;
    request_spills_locked();
}

void GlobalMemoryLimiter::unregister_buffer(std::atomic<bool>* spill_flag) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = entries_.find(spill_flag);
    if (it == entries_.end()) {
        return;
    }
    total_bytes_locked_ -= it->second.resident;
    entries_.erase(it);
}

int64_t GlobalMemoryLimiter::total_bytes() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return total_bytes_locked_;
}

size_t GlobalMemoryLimiter::registered_count() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return entries_.size();
}

bool GlobalMemoryLimiter::budget_degraded() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return degraded_;
}

void GlobalMemoryLimiter::request_spills_locked() {
    const int64_t budget = budget_bytes_.load(std::memory_order_relaxed);
    if (budget <= 0 || total_bytes_locked_ <= budget) {
        return;
    }
    // BUDGET SANITY (graceful degradation): the budget bounds SPILLABLE
    // memory only -- a forced spill reclaims a buffer's posting arena, never
    // its persistent vocab / pair-map structures. When the per-writer budget
    // share falls below the minimum useful working set, those persistent
    // bytes alone exceed the budget and no amount of forced spilling can get
    // under it; flagging would only manufacture a storm of floor-sized runs
    // for zero net relief (the conc=16 wikipedia field failure). Log ONCE per
    // degradation episode and stop flagging until the ratio recovers (writers
    // draining/unregistering, or a raised budget).
    const auto writer_count = static_cast<int64_t>(entries_.size());
    const int64_t min_useful = min_useful_budget_per_writer_bytes_.load(std::memory_order_relaxed);
    if (writer_count > 0 && min_useful > 0 && budget / writer_count < min_useful) {
        if (!degraded_) {
            degraded_ = true;
            LOG(WARNING) << "SNII global index-writer memory budget cannot be met by spilling: "
                         << "budget=" << budget << " B across " << writer_count
                         << " registered writers is below the minimum useful " << min_useful
                         << " B/writer (persistent per-writer structures dominate; the budget "
                         << "bounds SPILLABLE memory, not persistent memory). Forced spilling is "
                         << "suspended until writers drain or the budget "
                         << "(snii_index_writer_global_memory_bytes) is raised.";
        }
        return;
    }
    if (degraded_) {
        degraded_ = false; // episode over; a relapse will log once again
        LOG(INFO) << "SNII global index-writer memory budget recovered (" << budget << " B / "
                  << writer_count << " writers); forced spilling resumes.";
    }
    const int64_t overage = total_bytes_locked_ - budget;
    // FORCED-SPILL FLOOR + PER-BUFFER COOLDOWN: only buffers holding at least
    // the floor of RECLAIMABLE arena are eligible victims -- flagging a
    // smaller arena would cut a tiny run and reclaim next to nothing, and a
    // buffer that just honored a forced spill (arena ~0) stays exempt until
    // its arena regrows past the floor. Never below one byte: an empty arena
    // has nothing to write to a run.
    const int64_t victim_floor =
            std::max<int64_t>(min_victim_arena_bytes_.load(std::memory_order_relaxed), 1);
    // Largest RECLAIMABLE consumers first: victims are ranked by their
    // spillable ARENA (what the forced spill frees), not the
    // persistent-dominated resident total. n is the live writer count of the
    // process (at most a few hundred), and this only runs while over budget,
    // so the sort under the mutex is bounded, allocation-light work.
    std::vector<std::pair<int64_t, std::atomic<bool>*>> by_arena;
    by_arena.reserve(entries_.size());
    for (const auto& [flag, entry] : entries_) {
        if (entry.arena >= victim_floor) {
            by_arena.emplace_back(entry.arena, flag);
        }
    }
    std::sort(by_arena.begin(), by_arena.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    int64_t covered = 0;
    for (const auto& [arena, flag] : by_arena) {
        if (covered >= overage) {
            break;
        }
        // An ALREADY-pending flag (set by an earlier report, owner not yet at
        // its next token) counts toward the covered sum without a fresh store:
        // re-flagging it would be a no-op, and skipping the store avoids
        // dirtying the owner's cache line every over-budget report.
        if (!flag->load(std::memory_order_relaxed)) {
            flag->store(true, std::memory_order_relaxed);
        }
        // Count the ARENA toward coverage: it is all a forced spill of this
        // victim can actually reclaim. When the persistent remainder keeps the
        // sum over budget even after every eligible arena is flagged, the loop
        // simply flags them all -- each still cuts a >= floor-sized run, and
        // the cooldown above keeps any one buffer from being re-victimized
        // before it has a floor's worth of arena again.
        covered += arena;
    }
}

} // namespace doris::snii::writer
