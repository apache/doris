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
#include <limits>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker.h"
#include "storage/index/snii/writer/snii_build_memory_tracker.h"
#include "util/mem_info.h"

namespace doris::snii::writer {

int64_t BuildMemorySignals::overage() const {
    const int64_t over_share = build_share_bytes > 0 ? build_consumption - build_share_bytes : 0;
    // The maximum, NOT the sum -- and this is a correctness requirement, not a
    // stylistic one. The terms overlap: SNII's own bytes are inside
    // build_consumption AND inside the process_memory_usage() that produces
    // process_above_soft_mem_limit. Summing would count them twice and demand
    // reclaiming memory that does not exist. Each term is the bytes needed to
    // clear ONE pressure source; reclaiming the deepest clears the shallower.
    return std::max<int64_t>(
            {over_share, sys_avail_below_warning_water_mark, process_above_soft_mem_limit, 0});
}

int64_t calc_process_max_snii_build_memory(int64_t process_mem_limit) {
    // <= 0 is Doris's "no limit" convention (-1), and INT64_MAX appears on
    // unconfigured/unlimited cgroups: neither yields a meaningful share, and
    // multiplying either would overflow.
    if (process_mem_limit <= 0 || process_mem_limit == std::numeric_limits<int64_t>::max()) {
        return -1;
    }
    const int32_t percent = std::clamp(config::snii_index_build_max_memory_limit_percent, 0, 100);
    if (percent <= 0) {
        return 0; // share trigger explicitly disabled
    }
    // Multiply first where it is provably safe (percent <= 100, so the product
    // fits whenever the limit is under INT64_MAX/100 -- true of any real BE),
    // which keeps the share exact. Above that, divide first to stay in range;
    // the sub-100-byte rounding is irrelevant at that scale.
    const int64_t share = process_mem_limit <= std::numeric_limits<int64_t>::max() / 100
                                  ? process_mem_limit * percent / 100
                                  : process_mem_limit / 100 * percent;
    // FLOOR AGAINST THE PER-WRITER CAP: inverted_index_ram_buffer_size (512 MiB
    // by default) is what ONE writer may hold before it spills on its own. A
    // share below a few writers' worth of that would put small BEs permanently
    // over the share the moment two writers exist -- back-pressure that can
    // never be relieved rather than a limit. Four writers' worth is the
    // smallest share at which the mechanism can express "some writers are fine,
    // this one is not".
    const auto per_writer_cap =
            static_cast<int64_t>(config::inverted_index_ram_buffer_size * 1024 * 1024);
    return std::max(share, 4 * per_writer_cap);
}

BuildMemorySignals read_build_memory_signals() {
    BuildMemorySignals signals;
    // The RECLAIMABLE population only. Index-merge compaction charges the same
    // observation tracker but registers no writer and holds no posting arena,
    // so including it would charge ingestion writers for memory they do not
    // hold -- picking the wrong victim, or demanding an overage no arena can
    // cover. Its own kHardLimit reservation policy bounds it instead.
    //
    // ONE atomic load, and no arithmetic across populations. The registered
    // bytes are maintained directly for exactly this reason: deriving them as
    // (tracker - unregistered) would sample two independently-updated atomics
    // that cannot be read as one snapshot, and a read landing mid-update would
    // both invent overages and hide real ones. See snii_registered_build_bytes.
    signals.build_consumption = snii_registered_build_bytes();
    signals.build_share_bytes = calc_process_max_snii_build_memory(MemInfo::mem_limit());
    // The byte-valued form of the two conditions is_exceed_soft_mem_limit()
    // tests, computed directly (as MemTableMemoryLimiter does) because the
    // decision needs HOW MANY bytes are missing, not just whether. Reading them
    // directly also keeps this off the logging side effect that the boolean
    // helper performs, which would fire on every writer report.
    signals.sys_avail_below_warning_water_mark = MemInfo::sys_mem_available_warning_water_mark() -
                                                 GlobalMemoryArbitrator::sys_mem_available();
    signals.process_above_soft_mem_limit =
            GlobalMemoryArbitrator::process_memory_usage() - MemInfo::soft_mem_limit();
    return signals;
}

namespace {
// Names the term that produced overage(), so the shortfall warning cannot
// misattribute process-wide pressure to SNII's own persistent structures.
const char* deepest_pressure_source(const BuildMemorySignals& signals) {
    const int64_t over_share = signals.build_share_bytes > 0
                                       ? signals.build_consumption - signals.build_share_bytes
                                       : 0;
    if (over_share >= signals.sys_avail_below_warning_water_mark &&
        over_share >= signals.process_above_soft_mem_limit) {
        return "SNII index-build is over its own share";
    }
    if (signals.sys_avail_below_warning_water_mark >= signals.process_above_soft_mem_limit) {
        return "system available memory is below its warning water mark (not SNII's own memory)";
    }
    return "process memory usage is above the soft limit (not SNII's own memory)";
}
} // namespace

GlobalMemoryLimiter::GlobalMemoryLimiter() : signals_(&read_build_memory_signals) {}

GlobalMemoryLimiter* GlobalMemoryLimiter::instance() {
    // Intentionally leaked (never destroyed): buffers un-register from their
    // destructors, which may run during static teardown at process exit -- a
    // destroyed registry there would be use-after-free. A leaked singleton
    // makes the "limiter outlives every attached buffer" contract
    // unconditional.
    static auto* g_instance = [] {
        auto* limiter = new GlobalMemoryLimiter();
#ifdef BE_TEST
        // TEST ISOLATION: registration is unconditional, so every unit test
        // that builds a SniiIndexColumnWriter joins THIS singleton. With the
        // production reader that would import the HOST's memory state -- an
        // ASAN container already above the process soft limit would fire forced
        // spills inside tests that never asked for one, including the
        // golden-bytes tests. Default the singleton to "no pressure"; a test
        // that wants the real reader installs it explicitly.
        limiter->set_signals_provider([] { return BuildMemorySignals {}; });
#endif
        return limiter;
    }();
    return g_instance;
}

void GlobalMemoryLimiter::set_signals_provider(SignalsFn signals) {
    std::lock_guard<std::mutex> guard(mutex_);
    signals_ = std::move(signals);
}

void GlobalMemoryLimiter::register_buffer(std::atomic<bool>* spill_flag, int64_t arena_bytes) {
    std::lock_guard<std::mutex> guard(mutex_);
    entries_[spill_flag] = arena_bytes;
}

void GlobalMemoryLimiter::report(std::atomic<bool>* spill_flag, int64_t arena_bytes) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = entries_.find(spill_flag);
    if (it == entries_.end()) {
        // Not registered (or already unregistered): ignore rather than
        // resurrect an entry nobody will remove.
        return;
    }
    it->second = arena_bytes;
    request_spills_locked();
}

void GlobalMemoryLimiter::unregister_buffer(std::atomic<bool>* spill_flag) {
    std::lock_guard<std::mutex> guard(mutex_);
    entries_.erase(spill_flag);
}

size_t GlobalMemoryLimiter::registered_count() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return entries_.size();
}

bool GlobalMemoryLimiter::reclaim_shortfall() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return reclaim_shortfall_;
}

int64_t GlobalMemoryLimiter::eligible_arena_bytes() const {
    std::lock_guard<std::mutex> guard(mutex_);
    const int64_t victim_floor =
            std::max<int64_t>(min_victim_arena_bytes_.load(std::memory_order_relaxed), 1);
    int64_t total = 0;
    for (const auto& [flag, arena] : entries_) {
        if (arena >= victim_floor) {
            total += arena;
        }
    }
    return total;
}

void GlobalMemoryLimiter::request_spills_locked() {
    const BuildMemorySignals signals = signals_();
    const int64_t overage = signals.overage();
    if (overage <= 0) {
        // Pressure gone is the ordinary way an episode ends -- writers drain, a query
        // finishes, RSS falls back. Clearing here is what keeps reclaim_shortfall() a
        // statement about the CURRENT arena, and what lets the next episode log again:
        // the recovery branch below only runs while an overage still exists, so without
        // this the latch would survive for the life of the process and silence every
        // later episode.
        if (reclaim_shortfall_) {
            reclaim_shortfall_ = false;
            LOG(INFO) << "SNII index-build memory pressure cleared; reclaim shortfall episode "
                         "over.";
        }
        return;
    }
    // FORCED-SPILL FLOOR + PER-BUFFER COOLDOWN: only buffers holding at least
    // the floor of RECLAIMABLE arena are eligible victims -- flagging a
    // smaller arena would cut a tiny run and reclaim next to nothing, and a
    // buffer that just honored a forced spill (arena ~0) stays exempt until
    // its arena regrows past the floor. Never below one byte: an empty arena
    // has nothing to write to a run.
    const int64_t victim_floor =
            std::max<int64_t>(min_victim_arena_bytes_.load(std::memory_order_relaxed), 1);
    // Largest RECLAIMABLE consumers first: victims are ranked by their
    // spillable ARENA (what the forced spill frees), not by any
    // persistent-dominated resident total. n is the live writer count of the
    // process (at most a few hundred), and this only runs while over the
    // target, so the sort under the mutex is bounded, allocation-light work.
    std::vector<std::pair<int64_t, std::atomic<bool>*>> by_arena;
    by_arena.reserve(entries_.size());
    int64_t reclaimable = 0;
    for (const auto& [flag, arena] : entries_) {
        if (arena >= victim_floor) {
            by_arena.emplace_back(arena, flag);
            reclaimable += arena;
        }
    }
    // SHORTFALL IS NOT A REASON TO DO NOTHING. The overage may be larger than
    // everything SNII could free -- most obviously when it comes from the
    // process-level terms, which measure memory SNII does not hold at all.
    // Refusing to flag in that state would make SNII least willing to give
    // memory back exactly when the system needs it most, and it is a control
    // loop that cannot recover: nothing is reclaimed, consumption keeps
    // growing, the ratio only worsens. So flag BEST EFFORT and record the
    // shortfall for observability.
    //
    // What actually prevents the conc=16 storm is the victim FLOOR, not any
    // reachability judgement: every eligible victim holds >= the floor of
    // arena, so every forced run is at least floor-sized; after honoring, the
    // victim's arena is ~0 and the cooldown keeps it ineligible until it
    // regrows a full floor; and the run-file cap merge-compacts what survives.
    // Flagging is therefore bounded to one >= floor-sized run per floor of
    // arena growth per buffer -- the intended back-pressure.
    const bool shortfall = reclaimable < overage;
    if (shortfall && !reclaim_shortfall_) {
        reclaim_shortfall_ = true;
        LOG(WARNING) << "SNII index-build cannot fully relieve memory pressure: reclaimable "
                     << "posting arena across " << by_arena.size() << " eligible writers ("
                     << reclaimable << " B of " << entries_.size() << " registered) is short of "
                     << "the " << overage << " B overage; spilling every eligible arena anyway. "
                     << "Deepest pressure source: " << deepest_pressure_source(signals)
                     << " (over_share="
                     << (signals.build_share_bytes > 0
                                 ? signals.build_consumption - signals.build_share_bytes
                                 : 0)
                     << " B, sys_avail_below_water_mark="
                     << signals.sys_avail_below_warning_water_mark
                     << " B, process_above_soft_limit=" << signals.process_above_soft_mem_limit
                     << " B; reclaimable build_consumption=" << signals.build_consumption
                     << " B, share=" << signals.build_share_bytes << " B).";
    } else if (!shortfall && reclaim_shortfall_) {
        reclaim_shortfall_ = false; // episode over; a relapse will log once again
        LOG(INFO) << "SNII index-build can cover the current overage again: " << reclaimable
                  << " B of reclaimable arena vs a " << overage << " B overage.";
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
        // dirtying the owner's cache line every over-share report.
        if (!flag->load(std::memory_order_relaxed)) {
            flag->store(true, std::memory_order_relaxed);
        }
        // Count the ARENA toward coverage: it is all a forced spill of this
        // victim can actually reclaim. When the eligible arenas fall short the
        // loop simply exhausts them -- every flagged victim still cuts a
        // >= floor-sized run, and the cooldown keeps any one buffer from being
        // re-victimized before it has a floor's worth of arena again.
        covered += arena;
    }
}

} // namespace doris::snii::writer
