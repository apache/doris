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

#include "runtime/query_cache/query_cache.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "cpp/sync_point.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "util/countdown_latch.h"
#include "util/cpu_info.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris {

std::vector<int>* QueryCacheHandle::get_cache_slot_orders() {
    DCHECK(_handle);
    auto result_ptr = reinterpret_cast<LRUHandle*>(_handle)->value;
    return &((QueryCache::CacheValue*)(result_ptr))->slot_orders;
}

CacheResult* QueryCacheHandle::get_cache_result() {
    DCHECK(_handle);
    auto result_ptr = reinterpret_cast<LRUHandle*>(_handle)->value;
    return &((QueryCache::CacheValue*)(result_ptr))->result;
}

int64_t QueryCacheHandle::get_cache_version() {
    DCHECK(_handle);
    auto result_ptr = reinterpret_cast<LRUHandle*>(_handle)->value;
    return ((QueryCache::CacheValue*)(result_ptr))->version;
}

int64_t QueryCacheHandle::get_cache_delta_count() {
    DCHECK(_handle);
    auto result_ptr = reinterpret_cast<LRUHandle*>(_handle)->value;
    return ((QueryCache::CacheValue*)(result_ptr))->delta_count;
}

int64_t QueryCacheHandle::get_cache_total_bytes() {
    DCHECK(_handle);
    auto result_ptr = reinterpret_cast<LRUHandle*>(_handle)->value;
    return ((QueryCache::CacheValue*)(result_ptr))->total_bytes;
}

int64_t QueryCacheHandle::get_cache_total_rows() {
    DCHECK(_handle);
    auto result_ptr = reinterpret_cast<LRUHandle*>(_handle)->value;
    return ((QueryCache::CacheValue*)(result_ptr))->total_rows;
}

void QueryCache::insert(const CacheKey& key, int64_t version, CacheResult& res,
                        const std::vector<int>& slot_orders, int64_t cache_size,
                        int64_t delta_count) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->query_cache_mem_tracker());
    CacheResult cache_result;
    for (auto& block_data : res) {
        cache_result.emplace_back(Block::create_unique())->swap(block_data->clone_empty());
        ScopedMutableBlock scoped_mutable_block(cache_result.back().get());
        auto& mutable_block = scoped_mutable_block.mutable_block();
        auto st = mutable_block.merge(*block_data);
        DORIS_CHECK(st.ok());
    }
    auto cache_value_ptr = std::make_unique<QueryCache::CacheValue>(
            version, std::move(cache_result), slot_orders, delta_count, cache_size);

    QueryCacheHandle(this, LRUCachePolicy::insert(key, (void*)cache_value_ptr.release(), cache_size,
                                                  cache_size, CachePriority::NORMAL));
    DorisMetrics::instance()->query_cache_write_back_total->increment(1);
}

bool QueryCache::lookup(const CacheKey& key, int64_t version, doris::QueryCacheHandle* handle) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->query_cache_mem_tracker());
    auto* lru_handle = LRUCachePolicy::lookup(key);
    if (lru_handle) {
        QueryCacheHandle tmp_handle(this, lru_handle);
        if (tmp_handle.get_cache_version() == version) {
            *handle = std::move(tmp_handle);
            return true;
        }
    }
    return false;
}

bool QueryCache::lookup_any_version(const CacheKey& key, QueryCacheHandle* handle) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->query_cache_mem_tracker());
    auto* lru_handle = LRUCachePolicy::lookup(key);
    if (lru_handle) {
        *handle = QueryCacheHandle(this, lru_handle);
        return true;
    }
    return false;
}

QueryCacheInstanceDecision::~QueryCacheInstanceDecision() = default;

std::unique_ptr<TabletReadSource> QueryCacheInstanceDecision::take_delta_read_source(
        int64_t tablet_id) {
    std::lock_guard<std::mutex> lock(_take_lock);
    auto it = _delta_read_sources.find(tablet_id);
    if (it == _delta_read_sources.end()) {
        return nullptr;
    }
    auto res = std::move(it->second);
    _delta_read_sources.erase(it);
    return res;
}

std::shared_ptr<QueryCacheInstanceDecision> QueryCacheRuntime::get_or_make_decision(
        const std::vector<TScanRangeParams>& scan_ranges) {
    std::string cache_key;
    int64_t version = 0;
    Status st = QueryCache::build_cache_key(scan_ranges, _param, &cache_key, &version);
    if (!st.ok()) {
        // No reliable cache key for this instance (e.g. FE could not align the
        // instance to a single partition so tablets carry different versions).
        // Degrade to an uncached scan instead of failing the query: both the
        // scan operator and the cache source operator observe the shared MISS
        // decision with key_valid=false, so nothing is looked up and nothing
        // is written back. This is an expected plan shape, so log only once
        // per fragment instead of twice per instance.
        std::lock_guard<std::mutex> lock(_lock);
        if (_invalid_decision == nullptr) {
            LOG(WARNING) << "query cache degrades to uncached scan, node_id=" << _param.node_id
                         << ", reason: " << st.to_string();
            _invalid_decision = std::make_shared<QueryCacheInstanceDecision>();
        }
        return _invalid_decision;
    }

    {
        std::lock_guard<std::mutex> lock(_lock);
        auto it = _decisions.find(cache_key);
        if (it != _decisions.end()) {
            return it->second;
        }
    }

    // Build the candidate outside the lock: a stale-entry decision walks
    // tablet metadata under the header lock, on merge-on-write tables scans
    // the delete bitmap, and on cloud tablets may first sync the tablet view
    // from the meta service (an RPC), so its cost grows with the tablets per
    // instance, the bitmap size and the sync latency. This runtime is
    // fragment-wide, so a single lock-scoped build would serialize even
    // unrelated instances' keys behind one slow decision. Racing callers of the same instance
    // build duplicate candidates; the loser adopts the winner's decision
    // below and its own candidate releases with the shared_ptr (the entry
    // pin and any captured delta read sources go with it), which only costs
    // a redundant metadata pass. The metrics are settled once, by the
    // winner, after publication.
    auto decision = std::make_shared<QueryCacheInstanceDecision>();
    decision->key_valid = true;
    decision->cache_key = cache_key;
    decision->current_version = version;
    _make_decision(scan_ranges, decision.get());
    TEST_SYNC_POINT("QueryCacheRuntime::get_or_make_decision.before_publish");

    std::lock_guard<std::mutex> lock(_lock);
    auto [it, inserted] = _decisions.emplace(std::move(cache_key), decision);
    if (inserted) {
        if (decision->mode == QueryCacheInstanceDecision::Mode::INCREMENTAL) {
            DorisMetrics::instance()->query_cache_stale_hit_total->increment(1);
        } else if (decision->mode == QueryCacheInstanceDecision::Mode::MISS &&
                   !decision->incremental_fallback_reason.empty()) {
            // Only count real fallbacks: an empty reason means incremental
            // merge was not enabled for this query in the first place.
            DorisMetrics::instance()->query_cache_incremental_fallback_total->increment(1);
        }
    }
    return it->second;
}

void QueryCacheRuntime::_make_decision(const std::vector<TScanRangeParams>& scan_ranges,
                                       QueryCacheInstanceDecision* decision) {
    decision->mode = QueryCacheInstanceDecision::Mode::MISS;
    if (_binlog_scan) {
        // A row-binlog scan reads a different data stream under the same plan
        // digest: it must neither serve cached blocks nor write its output
        // back (that would poison the cache for normal queries).
        decision->key_valid = false;
        return;
    }
    if (_param.force_refresh_query_cache) {
        return;
    }

    QueryCacheHandle handle;
    if (!_cache->lookup_any_version(decision->cache_key, &handle)) {
        return;
    }
    // Pin the entry: as long as this decision object lives, the entry cannot be
    // evicted, so every operator consuming this decision sees the same blocks.
    decision->handle = std::move(handle);

    if (decision->handle.get_cache_version() == decision->current_version) {
        decision->mode = QueryCacheInstanceDecision::Mode::HIT;
        decision->cached_delta_count = decision->handle.get_cache_delta_count();
        return;
    }

    if (_try_prepare_incremental(scan_ranges, decision)) {
        decision->mode = QueryCacheInstanceDecision::Mode::INCREMENTAL;
        return;
    }

    // Stale entry not reusable: drop the pin and fall back to a full scan.
    // The stale-hit/fallback metrics are settled by the caller once the
    // winning candidate is published, so a losing racer cannot double-count.
    decision->_delta_read_sources.clear();
    decision->handle = QueryCacheHandle();
    decision->mode = QueryCacheInstanceDecision::Mode::MISS;
}

bool QueryCacheRuntime::_try_prepare_incremental(const std::vector<TScanRangeParams>& scan_ranges,
                                                 QueryCacheInstanceDecision* decision) {
    if (!(_param.__isset.allow_incremental && _param.allow_incremental)) {
        // Not a fallback: incremental merge is simply not enabled for this
        // query, so leave incremental_fallback_reason empty.
        return false;
    }
    int64_t cached_version = decision->handle.get_cache_version();
    if (cached_version >= decision->current_version) {
        // The entry is newer than what this replica is asked to read (e.g. the
        // entry was filled from another replica with a newer visible version).
        // A full scan of the requested version is the only safe answer.
        decision->incremental_fallback_reason = "cached entry is newer";
        return false;
    }
    int64_t cached_delta_count = decision->handle.get_cache_delta_count();
    if (cached_delta_count >= config::query_cache_max_incremental_merge_count) {
        // Force a full recompute to compact the entry: every incremental merge
        // appends the delta blocks to the entry, so both the entry and the
        // upstream merge get more fragmented as deltas accumulate.
        decision->incremental_fallback_reason = "delta count reached compaction threshold";
        return false;
    }

    // Cloud only: fan the per-tablet meta-service syncs out in parallel before
    // the serial capture below, so an instance holding many stale tablets does
    // not stall the shared prepare thread issuing those RPCs one at a time.
    // Empty (and untouched) in shared-nothing mode, where no sync is needed.
    std::unordered_map<int64_t, std::string> presync_reasons;
    if (config::is_cloud_mode()) {
        presync_reasons = _presync_cloud_delta_tablets(_cache, decision->cache_key, scan_ranges,
                                                       decision->current_version);
    }

    for (const auto& scan_range : scan_ranges) {
        int64_t tablet_id = scan_range.scan_range.palo_scan_range.tablet_id;
        if (!_capture_tablet_delta(tablet_id, cached_version, presync_reasons, decision)) {
            return false;
        }
    }

    // If the cached entry alone already exceeds the entry limits, the merged
    // entry (which can only be larger) could never be written back. Still scan
    // only the delta, but tell the cache source upfront so it does not clone
    // blocks for a write back that would be discarded anyway. Such an entry
    // stays stale until compaction merges its delta away (then the capture
    // above fails and a full recompute takes over).
    if (decision->handle.get_cache_total_bytes() > _param.entry_max_bytes ||
        decision->handle.get_cache_total_rows() > _param.entry_max_rows) {
        decision->write_back_feasible = false;
    }

    decision->cached_version = cached_version;
    decision->cached_delta_count = cached_delta_count;
    return true;
}

// BE-global soft cap counter for how many decision-sync waits may block the
// query-admission light pool at once; see _presync_cloud_delta_tablets and
// config::query_cache_max_concurrent_decision_sync. A soft cap: a brief boundary
// overshoot under contention is harmless for a pool guard, and each call reads the
// (mutable) config so the bound is tunable online. Returns to 0 after every call
// (the release Defer runs on every admitted exit path).
std::atomic<int> QueryCacheRuntime::_presync_active_waiters {0};

// Refund the flight's one-lump memory charge on its final teardown. release() is a
// plain atomic subtract, so this destructor never allocates and never throws (safe on
// any thread that drops the last ref -- a pool worker, a waiter, or a losing candidate
// being discarded). Skipped when the flight was never armed (mem_tracker null).
QueryCache::PresyncFlight::~PresyncFlight() {
    if (mem_tracker != nullptr) {
        mem_tracker->release(tracked_bytes);
    }
}

std::unordered_map<int64_t, std::string> QueryCacheRuntime::_presync_cloud_delta_tablets(
        QueryCache* cache, const std::string& cache_key,
        const std::vector<TScanRangeParams>& scan_ranges, int64_t current_version) {
    std::unordered_map<int64_t, std::string> fallback_reasons;

    // (c) A non-positive budget disables cloud incremental merge: return WITHOUT
    // launching any work. The previous code launched the fan-out and abandoned
    // it on an instantly expired wait, spawning tasks whose only effect was to
    // be orphaned. Fall every scanned tablet back so the whole instance
    // recomputes in full. Checked before touching the engine so the semantics
    // (and its unit test) do not depend on engine state.
    if (config::query_cache_decision_sync_timeout_ms <= 0) {
        for (const auto& scan_range : scan_ranges) {
            fallback_reasons[scan_range.scan_range.palo_scan_range.tablet_id] =
                    "cloud incremental sync disabled";
        }
        return fallback_reasons;
    }

    // Reached only under config::is_cloud_mode() (the caller gates on it), but
    // cloud_unique_id is a mutable config, so is_cloud_mode() can flip true on a
    // live LOCAL deployment whose engine stays a local StorageEngine. Degrade
    // gracefully rather than aborting in the hard CHECK inside to_cloud(): fall
    // every tablet back, mirroring the per-tablet cloud-cast fallback below.
    auto* engine = dynamic_cast<CloudStorageEngine*>(&ExecEnv::GetInstance()->storage_engine());
    if (engine == nullptr) {
        for (const auto& scan_range : scan_ranges) {
            fallback_reasons[scan_range.scan_range.palo_scan_range.tablet_id] =
                    "storage engine is not cloud";
        }
        return fallback_reasons;
    }

    // (a) The BE is tearing down: do not add new sync work. Fall every scanned
    // tablet back rather than race teardown.
    if (engine->stopped()) {
        for (const auto& scan_range : scan_ranges) {
            fallback_reasons[scan_range.scan_range.palo_scan_range.tablet_id] =
                    "be is stopping, sync skipped";
        }
        return fallback_reasons;
    }

    // Bound how many decision-sync waits may block the query-admission light pool at
    // once. The fan-out wait below runs on that pool ("must be light, not locked"), and
    // single-flight coalesces only IDENTICAL keys, so under a meta-service brownout a
    // wave of DISTINCT stale keys (and, on a first-call race, the paired scan and cache-
    // source operators each waiting) could otherwise park every admission worker for the
    // full timeout and starve unrelated fragments. A soft counting cap keeps spare
    // workers free: a query over the cap skips incremental this round and recomputes in
    // full (always correct).
    //
    // The ceiling is the SMALLER of the tunable (query_cache_max_concurrent_decision_sync,
    // default 32) and half the ACTUAL light-pool width. The width must be figured in, not
    // just the config: brpc_light_work_pool_threads is operator-configurable, and on a
    // pool shrunk to e.g. 16 a fixed 32 would never bind -- every worker could still park.
    // The width is recomputed with the same formula internal_service.cpp sizes the pool by
    // (the configured thread count, or max(128, 4*cores) when left at -1), so any pool
    // keeps a worker reserve. The config only tightens below that floor; a non-positive
    // config does NOT disable the guard (which would reopen the exact starvation this
    // prevents) but falls back to the width-derived half. The fetch_add-then-back-out on a
    // miss allows a brief boundary overshoot (harmless for a pool guard) while reading the
    // mutable configs each call. Placed before the registry and fan-out so a rejected
    // query does no work at all, and the Defer releases the slot on every admitted exit
    // path (including the timeout and unwind paths below).
    const int light_pool_width = config::brpc_light_work_pool_threads != -1
                                         ? config::brpc_light_work_pool_threads
                                         : std::max(128, CpuInfo::num_cores() * 4);
    // Integer half: a degenerate 1-thread light pool floors to 0, so the guard then
    // admits nobody and every stale query falls back rather than parking the sole worker
    // for the decision timeout (the worker-reserve guarantee would otherwise be violated
    // exactly where it matters most).
    const int width_cap = light_pool_width / 2;
    int wait_cap = config::query_cache_max_concurrent_decision_sync;
    if (wait_cap <= 0 || wait_cap > width_cap) {
        wait_cap = width_cap;
    }
    const bool wait_admitted =
            _presync_active_waiters.fetch_add(1, std::memory_order_acq_rel) < wait_cap;
    if (!wait_admitted) {
        _presync_active_waiters.fetch_sub(1, std::memory_order_acq_rel);
        for (const auto& scan_range : scan_ranges) {
            fallback_reasons[scan_range.scan_range.palo_scan_range.tablet_id] =
                    "cloud decision sync at capacity";
        }
        return fallback_reasons;
    }
    Defer release_wait_slot(
            [&] { _presync_active_waiters.fetch_sub(1, std::memory_order_acq_rel); });

    // Coalesce identical concurrent fan-outs through the cache's single-flight
    // registry. Runtimes are private per fragment context, so without this, N
    // concurrent identical queries hitting the same stale key would each submit
    // the full per-tablet fan-out: the duplicates waste the bounded queue's
    // capacity (rejecting tablets that would have fit as one fan-out) and,
    // during a meta-service brownout, park pool workers on the same per-tablet
    // _sync_meta_lock, denying admission to unrelated keys. The first arrival
    // (the flight owner) submits the fan-out; later identical arrivals join as
    // waiters on the SAME completion latch, each under its own deadline. The
    // flight's shared pieces are exactly what a private fan-out uses; a null
    // cache (test-only) skips the registry and keeps the fan-out private.
    bool flight_owner = true;
    std::shared_ptr<QueryCache::PresyncFlight> flight;
    std::string flight_key;
    auto make_flight = [&] {
        // Attribute the flight's retained memory to the STABLE query-cache MemTracker
        // rather than to the transient per-query tracker of whichever admission thread
        // happened to create the flight (the flight outlives that query: coalesced
        // waiters and still-running fan-out tasks keep its shared pieces alive past the
        // query's teardown).
        //
        // The whole flight's estimated retained bytes are charged as ONE lump via the
        // manual consume()/release() side-channel on the query-cache MemTrackerLimiter
        // -- the SAME idiom the LRU layer uses for its tracking_bytes
        // (QueryCache::insert above). consume()/release() are plain atomic counter ops:
        // they NEVER allocate and NEVER throw, so the matching release runs safely in
        // ~PresyncFlight (a noexcept destructor) on whatever pool/waiter thread drops
        // the last ref. An earlier version charged the two hot buffers through a
        // DorisVector whose deleter had to SWITCH the thread's MemTrackerLimiter, but
        // that switch pushes onto a std::vector (attach_limiter_tracker) and can throw
        // bad_alloc under memory pressure, and a throw from a shared_ptr deleter runs
        // inside a noexcept destructor and std::terminates the BE. The single manual
        // lump keeps stable attribution without that terminate risk, lets every buffer
        // be a plain make_shared, and (unlike the earlier two-buffer-only charge) covers
        // ALL of the flight's significant allocations -- the object, the
        // per_range_reason / tablet_ids / slot_counted O(tablets) buffers, the control
        // blocks, and the registry key -- so the query-cache tracker does not
        // underreport retained RSS while flights and tombstones accumulate during a
        // meta-service brownout. (BE has no global new/malloc hook --
        // be/src/runtime/memory/jemalloc_hook.cpp aliases malloc/free straight to
        // jemalloc with no tracker interaction, and there is no global ::operator new
        // override -- so absent this manual charge a plain new/std::vector/std::string
        // is invisible to every MemTrackerLimiter and shows up only in process RSS.)
        // The lifetimes coincide: publish_slot (captured by every task) holds the
        // flight shared_ptr, so the flight object outlives every buffer, and
        // ~PresyncFlight releases exactly when its last ref -- and thus all the buffers
        // -- is gone.
        //
        // This is a tracking charge, not a process reservation. The O(tablets) pieces
        // are small metadata (a pointer / an int64 / an atomic per tablet), doubly
        // bounded by the concurrency cap (query_cache_max_concurrent_decision_sync) and
        // the fan-out pool's fixed queue, and are dwarfed by the delta scan data this
        // decision gates -- a query near the process limit is tipped over by that scan,
        // not by these vectors. If an allocation here does fail under pressure it
        // surfaces as an ordinary per-query bad_alloc (the admission caller fails that
        // one query; a fan-out task's catch below turns its own into a fallback), never
        // data corruption or a BE crash, so a try_reserve for metadata this small and
        // this well-bounded is deliberately not taken.
        auto qc_limiter = ExecEnv::GetInstance()->query_cache_mem_tracker();
        const int64_t slot_n = static_cast<int64_t>(scan_ranges.size());

        auto f = std::make_shared<QueryCache::PresyncFlight>();
        // Index-aligned so the fan-out tasks write disjoint slots without a lock; an
        // empty (nullptr) slot means synced (or skipped as non-append-only), a
        // non-empty slot is the fallback reason (a static literal). Each piece sits
        // behind its own shared_ptr because the fan-out is waited on with a fast-fail
        // deadline: on a timeout a waiter returns while still-running tasks keep writing
        // their slots, so the storage must outlive every frame -- the shared_ptr each
        // task captured keeps it alive until the last one finishes. Only read on the
        // completed path.
        f->per_range_reason = std::make_shared<std::vector<const char*>>(scan_ranges.size());
        f->fanout_done = std::make_shared<CountDownLatch>(static_cast<int>(scan_ranges.size()));
        // Per-slot single-owner claim flags; see the claim comments at the submit loop
        // below. (Value-initialized to false.)
        f->slot_counted = std::make_shared<std::vector<std::atomic<bool>>>(scan_ranges.size());
        // Set true when the LAST interested waiter abandons the flight (its deadline
        // passed): a not-yet-started task then skips its sync RPC instead of spending
        // the full retry budget, so a meta-service brownout drains the bounded queue
        // fast rather than running abandoned work to completion and starving the pool.
        f->abandoned = std::make_shared<std::atomic<bool>>(false);
        // Snapshot the owner's tablet ids in slot order. Every waiter (this owner and
        // any that coalesce onto the flight) builds its fallback map from THESE ids, not
        // from its own scan-range order: build_cache_key sorts tablet ids into the key,
        // so two runtimes whose scan ranges hold the same tablets in a different order
        // share this flight, and keying the shared index-aligned slots off a joiner's
        // own order would pin the owner's per-slot reason to the wrong tablet.
        auto ids = std::make_shared<std::vector<int64_t>>();
        ids->reserve(scan_ranges.size());
        for (const auto& scan_range : scan_ranges) {
            ids->push_back(scan_range.scan_range.palo_scan_range.tablet_id);
        }
        f->tablet_ids = std::move(ids);
        // The flight's own registry key (a plain std::string); on the null-cache test
        // path flight_key is empty, so this is a harmless no-op there.
        f->key = flight_key;
        f->slots = scan_ranges.size();
        f->waiters = 1;
        // Estimate the flight's retained bytes and charge them as one lump. Covered:
        // the object; the three O(tablets) buffers (per_range_reason 8B, tablet_ids 8B,
        // slot_counted ~1B per tablet); BOTH copies of the registry key (PresyncFlight::
        // key and the registry map node's own key are separate std::string buffers); the
        // N queued fan-out task closures (submit_func heap-wraps each lambda, which
        // captures the shared pieces -- a handful of shared_ptrs plus small scalars -- so
        // a per-task slack covers each); and a fixed slack for the ~6 shared_ptr control
        // blocks and the latch/atomic bodies. Arm the release (mem_tracker +
        // tracked_bytes) BEFORE the consume so any later unwind still balances;
        // ~PresyncFlight releases the same tracked_bytes off the same tracker.
        constexpr int64_t kPerTaskClosureBytes = 128; // ~a handful of shared_ptrs + scalars
        const int64_t flight_tracked_bytes =
                static_cast<int64_t>(sizeof(QueryCache::PresyncFlight)) +
                slot_n * static_cast<int64_t>(sizeof(const char*) + sizeof(int64_t) +
                                              sizeof(std::atomic<bool>) + kPerTaskClosureBytes) +
                2 * static_cast<int64_t>(f->key.size()) + 6 * 64;
        f->tracked_bytes = flight_tracked_bytes;
        f->mem_tracker = qc_limiter;
        qc_limiter->consume(flight_tracked_bytes);
        return f;
    };
    if (cache != nullptr) {
        // flight_key embeds the digest and the SORTED tablet set + version, and
        // is injective: build_cache_key appends the version as decimal digits, so
        // this '@' is the LAST one in the string (no digit is '@') and splitting
        // at the final '@' recovers exactly (cache_key, version) even though the
        // binary cache_key may itself contain '@' bytes. Equal flight_key
        // therefore means the identical tablet set at the identical version --
        // correct to coalesce, never a cross-key alias.
        flight_key = cache_key;
        flight_key += '@';
        flight_key += std::to_string(current_version);
        // Double-checked registration. Check under the lock FIRST (the common
        // coalescing case joins without allocating anything); only on a miss do we
        // build the O(tablets) candidate -- OUTSIDE the lock, because holding the
        // BE-global registry lock across that allocation would serialize every
        // cloud incremental query, and a stampeding follower must not build a
        // candidate it immediately discards -- then re-lock and re-check, since a
        // concurrent owner may have inserted in the gap (we then join theirs and
        // free the candidate). A found flight always covers the identical tablet
        // set and slot count (the key invariant above), so assert rather than
        // branch; joining a flight whose original waiters all already timed out is
        // intentional -- it rides the draining fan-out instead of submitting a
        // DUPLICATE one, which is what keeps a sustained brownout from recreating
        // wave after wave of identical syncs. The entry is reaped only once the
        // latch drains -- by the last waiter (leave_flight) or, if none is left,
        // by the final task (publish_slot) -- so it never lingers.
        {
            std::lock_guard<std::mutex> reg_lock(cache->_presync_flights_lock);
            auto it = cache->_presync_flights.find(flight_key);
            if (it != cache->_presync_flights.end()) {
                DCHECK_EQ(it->second->slots, scan_ranges.size());
                flight = it->second;
                ++flight->waiters;
                flight_owner = false;
            }
        }
        if (flight_owner) {
            auto candidate = make_flight();
            std::lock_guard<std::mutex> reg_lock(cache->_presync_flights_lock);
            // try_emplace does the re-check and the insert in ONE hash of this long
            // key (digest + sorted tablet set + version): it inserts `candidate`
            // only when no concurrent owner beat us to the key in the unlocked gap,
            // otherwise it leaves the map untouched and hands back the existing
            // flight to join (and `candidate` is dropped). This replaces a find()
            // plus a second operator[] that would hash the key twice under the
            // BE-global registry lock.
            auto [it, inserted] = cache->_presync_flights.try_emplace(flight_key, candidate);
            if (inserted) {
                flight = candidate;
                DorisMetrics::instance()->query_cache_presync_inflight->increment(1);
            } else {
                DCHECK_EQ(it->second->slots, scan_ranges.size());
                flight = it->second;
                ++flight->waiters;
                flight_owner = false;
            }
        }
    } else {
        flight = make_flight();
    }
    auto per_range_reason = flight->per_range_reason;
    auto fanout_done = flight->fanout_done;
    auto slot_counted = flight->slot_counted;
    auto abandoned = flight->abandoned;
    // The owner's tablet ids in slot order; every waiter (owner and coalesced
    // joiners) keys the merged reasons off THESE, never off its own scan-range
    // order (build_cache_key sorts, so a joiner's order may differ). Shared for
    // read only, so a plain copy of the shared_ptr is enough.
    auto flight_tablet_ids = flight->tablet_ids;
    // Leave the flight on EVERY exit path -- normal return AND an exception
    // unwinding out of the submit loop or the merge below -- via RAII, so a
    // decrement can never be skipped and strand the registry entry. Two rules:
    //   (1) Reap the entry only once the fan-out has actually DRAINED (latch at
    //       0), NOT merely when the last waiter's deadline expired. Erasing while
    //       tasks are still mid-RPC would let the next identical query find no
    //       entry, become a fresh owner, and submit a DUPLICATE fan-out; under a
    //       sustained meta-service brownout that recreates wave after wave of
    //       duplicate syncs -- the exact bounded-pool pressure single-flight
    //       exists to prevent. So while tasks remain, KEEP the entry: post-timeout
    //       arrivals then join this draining flight (see the join above) instead
    //       of piling on. If a waiter is present when the latch drains it reaps
    //       here; if every waiter already left (a timed-out tombstone), the FINAL
    //       task reaps in publish_slot when it drives the latch to 0 and sees zero
    //       waiters -- so a drained tombstone is never stranded, even with no
    //       future identical query. Every slot is published exactly once (run,
    //       skip, inline-reject, or submit-throw fallback), so the latch always
    //       reaches 0 in bounded time (except at pool shutdown, which discards
    //       queued-but-unstarted tasks; that strand is a process-teardown residual
    //       reclaimed when the cache is destroyed, not runtime growth), and a
    //       drained flight makes wait_for return immediately for a joiner.
    //   (2) A last leaver that leaves while tasks still run -- its deadline
    //       expired, OR an exception (bad_alloc) unwound the owner mid-submit --
    //       sets `abandoned` UNDER the registry lock (so a joiner cannot observe
    //       the entry pre-abandon), telling not-yet-started tasks to skip so the
    //       pool drains fast instead of running work no one will read. NOT
    //       per-waiter: a timed-out waiter must not cancel work a still-waiting
    //       sibling needs. A private (null-cache) flight has exactly one waiter,
    //       so this degenerates to plain abandon-if-tasks-remain.
    bool timed_out = false;
    Defer leave_flight([&] {
        if (cache != nullptr) {
            std::lock_guard<std::mutex> reg_lock(cache->_presync_flights_lock);
            bool last = (--flight->waiters == 0);
            bool drained = (fanout_done->count() == 0);
            if (last && drained) {
                // Guard `it->second == flight`: a prior tombstone for this key may
                // already have been reaped and a fresh flight inserted; erase only
                // our own.
                auto it = cache->_presync_flights.find(flight_key);
                if (it != cache->_presync_flights.end() && it->second == flight) {
                    cache->_presync_flights.erase(it);
                    DorisMetrics::instance()->query_cache_presync_inflight->increment(-1);
                }
            } else if (last) {
                // Last waiter leaving with the fan-out still running (timeout or an
                // owner exception): keep the tombstone and abandon so queued tasks
                // skip; the final task reaps it in publish_slot once it drains.
                abandoned->store(true, std::memory_order_release);
            }
        } else if (fanout_done->count() != 0) {
            // Private (null-cache) flight, tasks still running: abandon so they
            // skip. There is no registry entry to reap.
            abandoned->store(true, std::memory_order_release);
        }
    });
    // Called ONLY by whoever already won the exclusive slot claim (the slot_counted
    // exchange below), so it is the sole writer of the slot: publish its reason (a
    // null/empty reason means synced OK / skipped, leaving the default-empty slot)
    // and release the latch exactly once. The claim winner counting down is what
    // lets the caller move the slot out the instant the latch settles.
    //
    // `reason` is a `const char*` (a static literal), NOT a std::string, and the
    // slot is a `const char*` too, so storing it is a plain pointer write that
    // CANNOT allocate or throw. This matters because this also runs on the
    // bad_alloc unwind (publish_unsubmitted below): a throwing store inside that
    // Defer would std::terminate. A null/empty reason means synced OK / skipped and
    // leaves the default-null slot. The latch count_down and the reap always run,
    // so a slot never strands the flight; the tablet-id map materializes the
    // reason strings once, after the latch settles.
    auto publish_slot = [fanout_done, per_range_reason, cache, flight](size_t idx,
                                                                       const char* reason) {
        if (reason != nullptr && reason[0] != '\0') {
            (*per_range_reason)[idx] = reason;
        }
        fanout_done->count_down();
        // Reap-on-drain by the FINAL publisher when no waiter is left. If this
        // count_down drained the latch and every interested waiter has already
        // left (a timed-out tombstone, or an owner that unwound on bad_alloc), no
        // leave_flight will run to reap it -- so reap here. The count()==0
        // pre-check takes only the latch's own lock, NOT the BE-global registry
        // lock (the latch saturates at 0, so once true it stays true, and every
        // slot publishes exactly once); only the settling publisher takes the
        // registry lock, where it re-reads waiters and erases only its own entry.
        // A joiner that raced in (++waiters under the same lock) makes waiters != 0,
        // so this skips and that joiner reaps on its own leave. `cache` is null on
        // the test-only private path (no registry).
        if (cache != nullptr && fanout_done->count() == 0) {
            std::lock_guard<std::mutex> reg_lock(cache->_presync_flights_lock);
            if (flight->waiters == 0) {
                auto it = cache->_presync_flights.find(flight->key);
                if (it != cache->_presync_flights.end() && it->second == flight) {
                    cache->_presync_flights.erase(it);
                    DorisMetrics::instance()->query_cache_presync_inflight->increment(-1);
                }
            }
        }
    };

    // Run the per-tablet syncs on the engine-owned, bounded query-cache delta
    // pre-sync pool -- a DEDICATED pool, not the shared SyncLoadForTablets warmup
    // pool, so a meta-service brownout cannot couple the two paths. Properties
    // this relies on: (1) fixed width + a bounded queue (cloud/config.h
    // query_cache_delta_sync_*), so a brownout can neither spawn unbounded
    // concurrent syncs nor grow the backlog without bound (a submit past the queue
    // cap fails fast and that tablet falls back); (2) CloudStorageEngine::stop()
    // drains this pool (joins running tasks, discards queued ones) and the
    // destructor calls stop() before _meta_mgr/_tablet_mgr are destroyed, so a
    // task that outlived its timed-out query still runs against a live engine and
    // none can survive it. The raw detached bthreads this replaced were joined by
    // nothing, so one sleeping in retry_rpc could wake after the engine was freed.
    //
    // Fast-fail is preserved: this runs in operator init on the bounded query
    // admission pool (light_work_pool, "must be light, not locked"). submit_func
    // never creates a worker -- the pool's fixed worker set (min == max) is
    // pre-started AND verified present in the engine ctor (a CHECK_EQ on num_threads,
    // because ThreadPool::init swallows creation failures), so submit is pure enqueue:
    // it returns immediately at capacity/shutdown and never blocks on thread creation,
    // keeping thread-creation latency off this critical path. The whole fan-out plus
    // wait is then bounded by ONE absolute deadline (query_cache_decision_sync_
    // timeout_ms from the start below), which the pure-enqueue submit loop cannot
    // exhaust before the wait even begins. A healthy sync is milliseconds, far under
    // the budget, so the steady-state path is unchanged; this only trips under real
    // meta-service degradation. Every task records its own sync error into its slot so
    // no failure aborts the others.
    //
    // The tasks do no explicit MemTracker/ResourceContext attach: this is a detached
    // engine-layer sync with no live query context to charge to (the query may have
    // already timed out and returned, so attaching to it would be both unavailable
    // here -- this is a static engine path -- and WRONG: sync_rowsets' allocations
    // land in the tablet cache's own structures (rowset metas, delete bitmap) that
    // outlive this decision, so charging them to the query-cache or a per-query
    // tracker would mis-attribute tablet-cache memory to the wrong owner). Their
    // allocations fall to the worker thread's default accounting, matching the
    // same-shaped sync on the same kind of pool by CloudStorageEngine's other
    // sync_rowsets caller, CloudBackendService::sync_load_for_tablets (the FE warmup
    // path, be/src/cloud/cloud_backend_service.cpp), which likewise submits the sync
    // to its pool with no attach.
    auto sync_fanout_start = std::chrono::steady_clock::now();
    if (!flight_owner) {
        // Joined an in-flight fan-out for the same (cache_key, version): the
        // owner's tasks fill the shared slots; just wait on the shared latch
        // below under this waiter's own deadline.
        TEST_SYNC_POINT("QueryCacheRuntime::_presync_cloud_delta_tablets.follower_joined");
    } else {
        auto& pool = engine->query_cache_delta_sync_pool();
        // Safe to capture the engine by raw pointer: stop() drains this pool
        // before the engine (and _meta_mgr/_tablet_mgr) is destroyed, so a
        // running task always sees a live engine.
        CloudStorageEngine* engine_ptr = engine;
        // Guarantee the latch always settles even if the submit loop unwinds
        // partway (submit_func can throw bad_alloc heap-allocating a task closure
        // under memory pressure). `submitted` counts the slots the loop has fully
        // handled (a task was enqueued, or the inline-reject path published it);
        // on an unwind this Defer publishes exactly the slots NOT yet reached, so
        // the CountDownLatch still reaches 0. It must publish only [submitted, N):
        // slots already submitted are owned by their (possibly still-queued) tasks
        // and stealing their claim would falsely report a scheduled sync as
        // unscheduled -- so on the normal path (submitted == N) this publishes
        // nothing. A settling latch is what lets the leave Defer's reap-on-drain
        // rule eventually remove the registry entry instead of stranding it.
        size_t submitted = 0;
        Defer publish_unsubmitted([&] {
            for (size_t j = submitted; j < scan_ranges.size(); ++j) {
                if (!(*slot_counted)[j].exchange(true, std::memory_order_acq_rel)) {
                    publish_slot(j, "cloud rowset sync not scheduled");
                }
            }
        });
        for (size_t i = 0; i < scan_ranges.size(); ++i) {
            int64_t tablet_id = scan_ranges[i].scan_range.palo_scan_range.tablet_id;
            Status submit_st = pool.submit_func([tablet_id, current_version, abandoned, i,
                                                 engine_ptr, publish_slot, slot_counted]() {
                // Claim this slot atomically BEFORE any external work. If the inline
                // submit-failure path already claimed it -- the narrow enqueue-then-fail
                // case where do_submit queued this task but then returned an error at zero
                // live workers, and the caller finished the slot inline -- bail before the
                // sync RPC: the caller owns the slot and the count-down, and this task's
                // result would be discarded. Claiming before the RPC (an exchange, not a
                // plain load that a concurrent inline claim could race between the read
                // and the RPC) is what keeps an enqueue-then-fail task that a later worker
                // picks up from firing a meta-service call for an already-settled query.
                if ((*slot_counted)[i].exchange(true, std::memory_order_acq_rel)) {
                    return;
                }
                // This task now exclusively owns the slot and MUST publish it exactly once
                // on every exit path so the caller's bounded wait can settle. Empty reason
                // means synced OK (or skipped as non-append-only); a non-empty reason
                // falls this tablet back.
                // A static literal (const char*), never an owning std::string, so
                // publishing it in `done` cannot allocate on the bad_alloc unwind
                // path (see publish_slot's contract above).
                const char* reason = nullptr;
                Defer done([&] { publish_slot(i, reason); });
                // Guard the whole per-tablet body: get_tablet fans out through
                // CloudTabletMgr's single-flight loader, which THROWS
                // std::system_error when a duplicate concurrent load's
                // CountdownEvent wait returns non-zero (be/src/cloud/
                // cloud_tablet_mgr.cpp), and sync_rowsets can surface a bad_alloc
                // (or a callee throw) under memory pressure. FunctionRunnable::run
                // invokes this closure with no catch, so an escaped exception on
                // this engine-pool worker would std::terminate the whole BE --
                // defeating the per-tablet fallback contract (any sync failure
                // must cost only the incremental merge, never the process). The
                // catch below converts it to a fallback reason.
                try {
                    // (a) Shutdown began, or the interested waiters all abandoned the
                    // wait (their deadlines passed, or an owner unwound on bad_alloc):
                    // bail before a fresh sync RPC so the pool drains quickly instead of
                    // spending the full RPC retry budget on work the timed-out waiters
                    // will not read. A coalesced joiner MAY later reuse this slot (it
                    // rides the draining tombstone), so the reason must be accurate:
                    // keep "be is stopping" for a genuine teardown, but give the
                    // abandonment case a distinct reason (covering BOTH the timeout and
                    // the owner-unwind causes) so a joiner on a HEALTHY BE never sees a
                    // false shutdown diagnosis in its profile.
                    if (engine_ptr->stopped()) {
                        reason = "be is stopping, sync skipped";
                        return;
                    }
                    if (abandoned->load(std::memory_order_acquire)) {
                        reason = "cloud rowset sync abandoned (all waiters left)";
                        return;
                    }
                    auto tablet_res = ExecEnv::get_tablet(tablet_id);
                    if (!tablet_res) {
                        // Publish the failure instead of leaving the slot empty: an
                        // empty reason tells _capture_tablet_delta the view is synced,
                        // so its own get_tablet would RETRY this very cache-miss
                        // meta-service load synchronously on the admission thread,
                        // outside the fast-fail deadline -- during a brownout (the one
                        // situation where this load fails slowly) that repeats the
                        // slow RPC on the pool this budget exists to protect. Falling
                        // back is also never wrong here: this worker-side load is
                        // strictly conservative (a transient failure only costs the
                        // incremental merge, never correctness).
                        reason = "cloud tablet load failed";
                        return;
                    }
                    BaseTabletSPtr tablet = std::move(tablet_res.value());
                    const bool append_only = tablet->keys_type() == KeysType::DUP_KEYS ||
                                             (tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                                              tablet->enable_unique_key_merge_on_write());
                    if (!append_only) {
                        // Skip the RPC entirely: _capture_tablet_delta rejects this
                        // tablet at its keys-type check before it would consume a sync.
                        return;
                    }
                    auto cloud_tablet = std::dynamic_pointer_cast<CloudTablet>(std::move(tablet));
                    if (cloud_tablet == nullptr) {
                        // is_cloud_mode() flipped on a live local deployment
                        // (cloud_unique_id is a mutable config): degrade to a full
                        // recompute rather than dereference the failed cast downstream.
                        reason = "tablet is not a cloud tablet";
                        return;
                    }
                    SyncOptions options;
                    options.query_version = current_version;
                    options.merge_schema = true;
                    // The history-rewrite check reads the delete bitmap this sync merges,
                    // so request it explicitly rather than inherit the struct default (which
                    // a refactor could flip with no compile error): a merge-on-write delta
                    // cannot be classified on a bitmap the sync did not bring up to the
                    // queried version. This is the IDENTICAL sync shape the normal cloud MOW
                    // scan issues (olap_scan_operator.cpp sets query_version + merge_schema
                    // and leaves sync_delete_bitmap at its true default, cloud_tablet.h), and
                    // the classification then reads the very same tablet_meta()->delete_
                    // bitmap() with no lazy per-rowset re-fetch, so it is no more exposed than
                    // the scan whose result it caches.
                    //
                    // It is NOT fully immune, but the gap is pre-existing and shared with
                    // that scan, not introduced here. sync_rowsets' fast path (_max_version
                    // >= query_version, cloud_tablet.cpp) returns without consulting
                    // sync_delete_bitmap, so if the tablet is already resident with rowsets
                    // synced to the queried version while its bitmap was left behind, this
                    // sync no-ops and the check reads the short bitmap. A full-compaction
                    // SUBMISSION creates exactly that state: it cache-miss-loads the tablet
                    // with sync_delete_bitmap=false (task_worker_pool.cpp and cloud_
                    // compaction_action.cpp both pass false for FULL_COMPACTION), advancing
                    // _max_version ahead of the bitmap for the span of an admin-triggered
                    // full compaction that lands on a query-serving BE (with
                    // enable_compaction_rw_separation compaction and queries never share a
                    // BE's tablet cache, so it cannot happen). In that window a plain MOW
                    // scan already returns un-deduplicated rows; the only thing this feature
                    // adds is that a mis-classified delta is written back, so the wrong
                    // result persists in the entry instead of being a one-shot -- bounded by
                    // the same window, since committing the compaction swaps the rowsets and
                    // rebases the entry. The clean repair is in the storage layer (have the
                    // fast path require bitmap coverage too, or keep a sync_delete_bitmap=
                    // false load out of the query-visible cache), which fixes the scan and
                    // this path together; it is left as a follow-up rather than widened into
                    // this change. A delta rowset a compaction folded away in the meantime is
                    // a separate, benign case -- the capture below fails to find it and falls
                    // the tablet back to a full recompute.
                    options.sync_delete_bitmap = true;
                    // Re-check abandonment/shutdown right before the sync: a task that
                    // passed the earlier checks may have sat here while its waiters all
                    // timed out. sync_rowsets serializes same-tablet syncs on
                    // _sync_meta_lock and early-returns once one succeeds, but on a
                    // persistent brownout each DISTINCT-key task that reaches it re-runs
                    // the full retry budget (distinct keys do not share this flight's
                    // `abandoned` flag); skipping here when nobody will read the result
                    // keeps abandoned work from occupying the bounded pool and prolonging
                    // teardown. This narrows the window to the common case (abandonment
                    // during the wait, before the sync starts); it cannot be fully closed
                    // at this layer, because a task already inside sync_rowsets blocked on
                    // _sync_meta_lock does not re-check -- threading cancellation into
                    // sync_rowsets is a storage-layer change left as a follow-up.
                    if (engine_ptr->stopped()) {
                        reason = "be is stopping, sync skipped";
                        return;
                    }
                    if (abandoned->load(std::memory_order_acquire)) {
                        reason = "cloud rowset sync abandoned (all waiters left)";
                        return;
                    }
                    Status st = cloud_tablet->sync_rowsets(options);
                    if (!st.ok()) {
                        // Unlike the other fallback reasons (expected data shapes), a
                        // failed sync is an infrastructure error: log the status so the
                        // profile reason correlates to a cause. Throttled with
                        // LOG_EVERY_N because a meta-service brownout fails this for
                        // every stale tablet of every query at once; the underlying RPC
                        // errors are already logged per retry inside retry_rpc, so a
                        // sampled line here is enough to correlate without a storm, and
                        // the exact reason still reaches the user via the query profile.
                        LOG_EVERY_N(WARNING, 100)
                                << "query cache incremental merge falls back, cloud rowset sync "
                                   "failed"
                                << ", tablet_id=" << tablet_id << ", status=" << st.to_string();
                        reason = "cloud rowset sync failed";
                    }
                } catch (const doris::Exception& e) {
                    // A Doris-specific throw (e.g. surfaced by a nested THROW_IF_ERROR):
                    // catch it BEFORE the generic std::exception so the Doris error code
                    // survives into the log, per the standard catch order
                    // doris::Exception -> std::exception -> ... (be/src/common/AGENTS.md).
                    // Same fallback as below: the tablet recomputes in full rather than
                    // the escaped exception terminating the BE.
                    reason = "cloud tablet sync raised";
                    LOG_EVERY_N(WARNING, 100)
                            << "query cache incremental merge falls back, cloud tablet sync raised"
                            << ", tablet_id=" << tablet_id << ", error=" << e.code() << ": "
                            << e.to_string();
                } catch (const std::exception& e) {
                    // Any other escaped exception (single-flight std::system_error, a
                    // bad_alloc, a callee throw) falls this tablet back instead of
                    // terminating the BE. `done` then publishes this reason, so the
                    // empty-slot "synced OK" path (which would make
                    // _capture_tablet_delta reissue the load synchronously) is never
                    // taken on a throw. Throttled like the sync-failed log because a
                    // brownout raises it for many tablets of many queries at once.
                    reason = "cloud tablet sync raised";
                    LOG_EVERY_N(WARNING, 100)
                            << "query cache incremental merge falls back, cloud tablet sync raised"
                            << ", tablet_id=" << tablet_id << ", what=" << e.what();
                } catch (...) {
                    reason = "cloud tablet sync raised";
                    LOG_EVERY_N(WARNING, 100) << "query cache incremental merge falls back, cloud "
                                                 "tablet sync raised (non-std exception)"
                                              << ", tablet_id=" << tablet_id;
                }
            });
            if (!submit_st.ok()) {
                // Pool shutting down or queue at capacity: do not block. Claim the
                // slot for the inline fallback. do_submit rejects shutdown and
                // capacity before enqueue, so the closure will not run; on the
                // narrow path where it did enqueue before failing (thread creation
                // at zero live workers), the exchange makes exactly one of this
                // path and the queued task own the slot -- if the task claimed it
                // first it will publish, so do nothing here.
                if (!(*slot_counted)[i].exchange(true, std::memory_order_acq_rel)) {
                    publish_slot(i, "cloud rowset sync not scheduled");
                }
            }
            // Slot i is now fully handled (a task was enqueued above, or the
            // inline-reject path published it); advance the safety-net floor so
            // the unwind Defer never re-touches it. Reached only when submit_func
            // did NOT throw -- a throw skips this and leaves slot i to the Defer.
            submitted = i + 1;
        }
    }

    // ONE absolute deadline across the whole operation, anchored at the fan-out
    // start above rather than restarted here. With the pre-started pool the submit
    // loop is pure enqueue and returns in microseconds, so remaining is essentially
    // the full budget; pinning the wait to the same start keeps the TOTAL budget at
    // query_cache_decision_sync_timeout_ms even if a degraded pool ever made
    // submission slow, instead of granting the full timeout again after submission
    // already consumed part of it.
    int64_t decision_sync_timeout_ms = config::query_cache_decision_sync_timeout_ms;
    // Test seam: lets a test pin a deterministic deadline per coalescing role. When
    // two runtimes share one flight, a test that wants to observe the intermediate
    // "first waiter left, second still waiting" state needs the leave order fixed by
    // construction (give the owner a shorter timeout than the joining follower)
    // rather than by wall-clock start staggering, which a descheduled CI runner could
    // invert. `flight_owner` is the role discriminator (true for the runtime that
    // created the flight, false for one that joined it). A no-op in production (no
    // callback registered); this function is static, so the role, not `this`, is what
    // a test can key on.
    TEST_SYNC_POINT_CALLBACK("QueryCacheRuntime::_presync_cloud_delta_tablets.deadline_ms",
                             flight_owner, &decision_sync_timeout_ms);
    auto sync_deadline = sync_fanout_start + std::chrono::milliseconds(decision_sync_timeout_ms);
    auto sync_now = std::chrono::steady_clock::now();
    // Keep full clock precision here -- do NOT truncate to whole milliseconds.
    // Truncating would shave the sub-millisecond submit time off the budget and let
    // the total blocked time dip just under query_cache_decision_sync_timeout_ms.
    // wait_for never returns before its duration elapses, so with the raw remaining
    // the total time from sync_fanout_start stays >= the budget.
    auto remaining = sync_deadline > sync_now ? (sync_deadline - sync_now)
                                              : std::chrono::steady_clock::duration::zero();
    // The leave-the-flight bookkeeping (decrement waiters, reap on drain, mark
    // abandoned) runs in the `leave_flight` Defer established above, so it fires
    // on every exit including an unwind. That Defer keys its reap/abandon decision
    // off the latch count (`drained`), NOT off `timed_out` -- a last leaver can
    // exit with `timed_out==false` (an owner unwinding on bad_alloc mid-submit)
    // yet with tasks still running, and only the latch count captures that. Record
    // the timeout result here; `timed_out` is read only by the fallback below.
    timed_out = !fanout_done->wait_for(remaining);
    DorisMetrics::instance()->query_cache_decision_sync_time_ms->increment(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                  sync_fanout_start)
                    .count());

    if (timed_out) {
        // The fan-out outran its fast-fail budget. Do NOT read per_range_reason
        // (still-running tasks are writing it); fall every scanned tablet back so
        // the whole instance recomputes in full. A distinct reason from a hard
        // sync failure so the profile tells a slow meta service apart from a
        // failing one. Throttled like the sync-failure log above: a brownout
        // trips this for every stale tablet of every query at once, and this
        // timeout is the primary operator-visible symptom of that brownout, so a
        // sampled line correlates it without a storm (the per-query reason still
        // reaches the user via the profile).
        LOG_EVERY_N(WARNING, 100)
                << "query cache incremental merge falls back, cloud rowset sync did not finish"
                << " within query_cache_decision_sync_timeout_ms="
                << config::query_cache_decision_sync_timeout_ms << "ms";
        for (const auto& scan_range : scan_ranges) {
            fallback_reasons[scan_range.scan_range.palo_scan_range.tablet_id] =
                    "cloud rowset sync timed out";
        }
        return fallback_reasons;
    }
    // Merge the index-aligned slots into a tablet-id map for the capture loop.
    // Safe to read now: the fan-out completed, so no task is still writing. COPY
    // rather than move: the slots are shared with every other runtime coalesced
    // onto this flight, and each builds its own map from them. Key each slot off
    // the OWNER's tablet id (flight_tablet_ids), NOT this waiter's scan_ranges:
    // build_cache_key sorts tablet ids, so a joiner whose scan ranges differ
    // from the owner's only in order still shares this flight, and reading a
    // slot through the joiner's own position would misassign the owner's
    // per-slot reason to the wrong tablet (a failed tablet's reason lands on a
    // sibling, and the truly failed tablet, left with no reason, is treated as
    // synced -- so _capture_tablet_delta reissues a synchronous get_tablet on
    // the admission thread outside the fast-fail budget, exactly the brownout
    // stall the presync exists to avoid). Scan ranges carry distinct tablet ids
    // in practice (each tablet is scanned once per query), but were one to
    // appear twice the fan-out would only issue an idempotent redundant sync
    // (serialized inside sync_rowsets by _sync_meta_lock) and this map, keyed by
    // tablet id, would still resolve to one consistent reason for it -- so a
    // duplicate is harmless, not something to guard against with an unreachable
    // de-dup branch.
    for (size_t i = 0; i < per_range_reason->size(); ++i) {
        // A null slot is a synced-OK / skipped tablet; a non-null slot is a static
        // reason literal (publish_slot only stores non-empty ones). Materialize the
        // literal into the tablet-id map here, once, now that the fan-out has drained.
        if ((*per_range_reason)[i] != nullptr) {
            fallback_reasons[(*flight_tablet_ids)[i]] = (*per_range_reason)[i];
        }
    }
    return fallback_reasons;
}

bool QueryCacheRuntime::_capture_tablet_delta(
        int64_t tablet_id, int64_t cached_version,
        const std::unordered_map<int64_t, std::string>& presync_reasons,
        QueryCacheInstanceDecision* decision) {
    if (config::is_cloud_mode()) {
        // A CloudTablet keeps its rowset list (and for merge-on-write tables
        // its delete bitmap) as a lazily synced copy of the meta service, and
        // this decision runs before the scan node's own sync round, so the
        // local view had to be brought up to the queried version first.
        // _try_prepare_incremental already did that for every append-only
        // tablet in one parallel fork-join (_presync_cloud_delta_tablets),
        // which is the single sync per tablet; this loop only consumes its
        // outcome. A recorded reason means the sync could not vouch for the
        // view (a failed cast on a misconfigured deployment, an infrastructure
        // sync failure, the fast-fail budget expiring on a slow meta service,
        // or the pool refusing the sync outright), so fall back to a full
        // recompute. Consumed BEFORE get_tablet below for two reasons: a refused
        // submit loads nothing, so this recorded reason is more precise than the
        // generic cache-only miss get_tablet would report; and a sync that
        // FAILED may still leave a stale copy cached from an earlier query, which
        // the cache-only get_tablet would return -- classifying on that unsynced
        // view would be wrong, so the reason must veto it first. No reason means
        // the view
        // is synced up to query_version, and the history-rewrite check below
        // reads the tablet's delete bitmap as the sync brought it to that
        // version (sync_delete_bitmap=true) -- exactly as complete as the
        // merge-on-write read path itself sees it, and no more: the
        // classification inherits that read path's completeness, including any
        // of its known limitations (e.g. a bitmap left short by a prior
        // sync_delete_bitmap=false materialization of the tablet), rather than
        // adding a stronger guarantee of its own. The scan node's later sync
        // hits the same no-op shortcut, so the RPC count per query does not
        // grow.
        auto reason_it = presync_reasons.find(tablet_id);
        if (reason_it != presync_reasons.end()) {
            decision->incremental_fallback_reason = reason_it->second;
            return false;
        }
    }
    // Cache-only lookup (force_use_only_cached=true): the presync fan-out just
    // synced this tablet, so in the common case it is still cached here and this
    // is a hit with no RPC. If capacity pressure evicted it in the narrow window
    // between that sync and this consume, a plain get_tablet would take the
    // synchronous cache-miss meta-service load on THIS light admission thread --
    // the very blocking window the presync fast-fail budget caps, reopened after
    // the fact. Forcing cache-only turns that miss into an immediate error that
    // falls the tablet back to a full recompute instead; the fallback is never
    // wrong (a missed incremental costs only recompute, never correctness), and
    // the scan node's own later get_tablet still performs any genuine reload off
    // this thread. The "tablet not found" reason is deliberately shared with a
    // genuinely absent tablet id: both are simply "not resident locally", and
    // this path cannot (nor needs to) tell an eviction from a never-loaded id.
    // In non-cloud mode the flag is ignored (StorageEngine::get_tablet reads the
    // resident tablet map, which never evicts), so this is a no-op there.
    auto tablet_res = ExecEnv::get_tablet(tablet_id, /*sync_stats=*/nullptr,
                                          /*force_use_only_cached=*/true);
    if (!tablet_res) {
        decision->incremental_fallback_reason = "tablet not found";
        return false;
    }
    BaseTabletSPtr tablet = std::move(tablet_res.value());
    // Defensive re-check of what FE promised with allow_incremental:
    // "cached snapshot + delta rowsets == new snapshot" holds unconditionally
    // for append-only DUP_KEYS data, and for merge-on-write UNIQUE data as
    // long as the delta did not rewrite any row that predates the cached
    // version (verified below once the delta rowsets are known). It never
    // holds for merge-on-read UNIQUE data (duplicates are resolved by merging
    // across rowsets at read time, so a delta-only scan cannot stand alone)
    // nor for AGG tables (rows merge in the storage layer likewise).
    const bool merge_on_write = tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                                tablet->enable_unique_key_merge_on_write();
    if (tablet->keys_type() != KeysType::DUP_KEYS && !merge_on_write) {
        decision->incremental_fallback_reason = "keys type not append-only";
        return false;
    }

    // quiet: on a version-graph miss (the delta merged away by compaction, an
    // expected per-query situation) this option makes the capture API neither
    // log nor error; it returns whatever PREFIX of the path it walked before
    // the miss (empty when the window start itself is gone), so emptiness AND
    // window coverage are both checked below as the failure signals.
    auto source_res = tablet->capture_read_source({cached_version + 1, decision->current_version},
                                                  {.quiet = true});
    if (!source_res) {
        // Non-pathfinding failures (e.g. a rowset object missing for a found
        // path) still surface as errors even under quiet.
        decision->incremental_fallback_reason = "delta versions not capturable";
        return false;
    }
    auto read_source = std::make_unique<TabletReadSource>(std::move(source_res.value()));
    if (read_source->rs_splits.empty()) {
        // A non-empty version window always lies on a consistent path with at
        // least one rowset; an empty capture therefore means the delta
        // versions are gone, never "no new data". Treating it as INCREMENTAL
        // would silently drop the delta rows from the result.
        decision->incremental_fallback_reason = "delta versions not capturable";
        return false;
    }
    // A quiet capture broken mid-window (e.g. a replica whose local view ends
    // short of current_version) yields a partial prefix, and treating it as
    // INCREMENTAL would silently drop the tail rows and poison the entry on
    // write-back. The path is contiguous by construction, so checking both
    // endpoints pins the whole (cached_version, current_version] window.
    if (read_source->rs_splits.front().rs_reader->rowset()->start_version() != cached_version + 1 ||
        read_source->rs_splits.back().rs_reader->rowset()->end_version() !=
                decision->current_version) {
        decision->incremental_fallback_reason = "delta versions not capturable";
        return false;
    }
    read_source->fill_delete_predicates();
    if (!read_source->delete_predicates.empty()) {
        // A delete in the delta logically removes rows that are already folded
        // into the cached partial result; that cannot be undone by merging, so
        // fall back to a full recompute.
        decision->incremental_fallback_reason = "delta contains delete predicates";
        return false;
    }
    // Pin the cached side for as long as the classification below reads the
    // delete bitmap. The markers it looks for sit on the rowsets the cached
    // snapshot was built from, and the capture above pins only the delta
    // side, so nothing else keeps that evidence alive: once a compaction
    // spanning both sides retires such a rowset, an overwritten row has no
    // counterpart in the output and its marker is therefore not relocated
    // (BaseTablet::calc_compaction_output_rowset_delete_bitmap skips a source
    // row whose id does not convert), leaving the marker only on the retired
    // input, which the stale sweep then hands to the unused-rowset GC.
    // Classification would see no rewrite and merge the cached rows with
    // their replacements: a wrong entry, stored at the current version.
    // Holding the rowsets stops that: both storage engines drop a retired
    // rowset's bitmap only once nothing else references it, so raising the
    // use count with this pin blocks the drop in either deployment mode.
    // Local storage does it in start_delete_unused_rowset(): it collects a
    // retired rowset only once nothing else references it, and only what it
    // collects gets remove_rowset_delete_bitmap(), which drops every version
    // of that rowset's bitmap. Cloud does it in
    // CloudTablet::remove_unused_rowsets(), which skips any rowset whose
    // use_count() still exceeds one before the same remove_rowset_delete_bitmap().
    // The other remover, the key ranges the stale sweep queues onto the
    // pre-window rowsets (the engine's unused-delete-bitmap queue locally,
    // CloudTablet::_unused_delete_bitmap in cloud), is held off by the delta
    // capture instead: that queue is released only once the swept path's own
    // rowsets are unreferenced.
    std::vector<RowsetSharedPtr> cached_side_pin;
    if (merge_on_write) {
        {
            std::shared_lock rdlock(tablet->get_header_lock());
            auto pin_res = tablet->capture_consistent_rowsets_unlocked({0, cached_version},
                                                                       {.quiet = true});
            if (pin_res) {
                cached_side_pin = std::move(pin_res.value().rowsets);
            }
        }
        // quiet yields the prefix it walked, so cover the whole cached range
        // before trusting the classification: an unpinned tail is evidence we
        // cannot vouch for.
        if (cached_side_pin.empty() || cached_side_pin.front()->start_version() != 0 ||
            cached_side_pin.back()->end_version() != cached_version) {
            decision->incremental_fallback_reason = "cached versions not pinnable";
            return false;
        }
        // The pin is only worth anything while the classification runs, so
        // let a test observe it exactly there rather than infer it.
        TEST_SYNC_POINT_CALLBACK("QueryCacheRuntime::_capture_tablet_delta.cached_side_pinned",
                                 &cached_side_pin);
    }
    if (merge_on_write &&
        _delta_rewrites_history(*tablet, *read_source, cached_version, decision->current_version)) {
        // A load in the delta window marked rows of the cached snapshot as
        // deleted through the delete bitmap (an upsert, a partial update or a
        // delete sign hit a pre-existing key). Those rows are already folded
        // into the cached partial result and cannot be subtracted, so only a
        // full recompute is safe. The full run re-bases the entry at the new
        // version, so an occasional backfill costs exactly one full scan and
        // the next pure-append hour is incremental again.
        decision->incremental_fallback_reason = "delta rewrites history rows";
        return false;
    }
    decision->_delta_read_sources[tablet_id] = std::move(read_source);
    return true;
}

bool QueryCacheRuntime::_delta_rewrites_history(BaseTablet& tablet,
                                                const TabletReadSource& delta_source,
                                                int64_t cached_version, int64_t current_version) {
    RowsetIdUnorderedSet delta_rowsets;
    for (const auto& split : delta_source.rs_splits) {
        delta_rowsets.insert(split.rs_reader->rowset()->rowset_id());
    }
    // The tablet delete bitmap is shared and it does change at versions that
    // are already visible, so what keeps this scan safe is not immutability
    // but the fallback it feeds. Loads add their markers before the version
    // becomes visible. Compaction relocates existing markers onto its output
    // rowset keeping their original version (BaseTablet::calc_compaction_
    // output_rowset_delete_bitmap); markers that matter here sit on a rowset
    // at or below the cached version, and an output that swallowed such a
    // rowset starts no later than it does, so the relocated marker lands
    // outside the delta and is read as a rewrite: conservative, never a miss.
    // (An output whose inputs all sit inside the window does belong to the
    // delta, and markers on it are delta-internal, which is what we want.) The one
    // rewrite that moves markers out of the window, the aggregation that
    // re-stamps a stale range onto its end version (BaseTablet::agg_delete_
    // bitmap_for_stale_rowsets, run by delete_expired_stale_rowset), only
    // fires once the merged versions are swept, and the capture this scan
    // pairs with anchors on the window endpoints, so a window whose path is
    // gone is rejected before we get here. Capture and scan are not atomic,
    // so a sweep landing exactly in between (with the aggregated keys also
    // already recycled) can still hide a rewrite: a known narrow race the
    // fallback does not cover, the same one any merge-on-write snapshot read
    // faces when a concurrent sweep re-stamps and recycles a marker (the
    // exact-hit cache path is not itself exposed, since it serves cached
    // blocks without re-reading the bitmap).
    // Pending entries use DeleteBitmap::TEMP_VERSION_COMMON (= 0) and stay
    // below the window as well.
    //
    // The map is ordered by (rowset, segment, version), and most entries of
    // a group lie OUTSIDE the delta window (dedup history below it, pending
    // entries at 0, concurrent newer loads above it), so on a long-lived
    // merge-on-write tablet the map is much larger than the window. Hop from
    // group to group with lower_bound/upper_bound instead of visiting every
    // entry: the shared-lock hold, which excludes bitmap writers such as
    // load publish, is then bounded by the in-window entries plus one
    // logarithmic hop per group instead of the full map size.
    // Compare versions in the uint64 key domain (both window bounds are
    // non-negative), so a hypothetical out-of-int64-range stored version can
    // only take the above-window hop, whose target is strictly past the
    // whole group: forward progress holds for arbitrary map contents, like
    // the plain walk this replaces.
    const auto window_begin = static_cast<uint64_t>(cached_version) + 1;
    const auto window_end = static_cast<uint64_t>(current_version);
    const DeleteBitmap& delete_bitmap = tablet.tablet_meta()->delete_bitmap();
    std::shared_lock rdlock(delete_bitmap.lock);
    const auto& bitmap_map = delete_bitmap.delete_bitmap;
    auto it = bitmap_map.begin();
    while (it != bitmap_map.end()) {
        const auto& [rowset_id, segment_id, version_key] = it->first;
        if (version_key < window_begin) {
            it = bitmap_map.lower_bound({rowset_id, segment_id, window_begin});
            continue;
        }
        if (version_key > window_end) {
            it = bitmap_map.upper_bound(
                    {rowset_id, segment_id, std::numeric_limits<uint64_t>::max()});
            continue;
        }
        if (!it->second.isEmpty() && !delta_rowsets.contains(rowset_id)) {
            return true;
        }
        ++it;
    }
    return false;
}

} // namespace doris
