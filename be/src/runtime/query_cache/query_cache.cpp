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
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "cpp/sync_point.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "util/countdown_latch.h"
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
        presync_reasons = _presync_cloud_delta_tablets(scan_ranges, decision->current_version);
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

std::unordered_map<int64_t, std::string> QueryCacheRuntime::_presync_cloud_delta_tablets(
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

    // Index-aligned so the fan-out tasks write disjoint slots without a lock; an
    // empty slot means synced (or skipped as non-append-only), a non-empty slot
    // is the fallback reason. Held (with the latch and per-slot count guard)
    // behind a shared_ptr because the fan-out is waited on with a fast-fail
    // deadline: on a timeout this frame returns while still-running tasks keep
    // writing their slots, so the storage must outlive the frame -- the shared_ptr
    // each task captured keeps it alive until the last one finishes. Only read on
    // the completed path.
    auto per_range_reason = std::make_shared<std::vector<std::string>>(scan_ranges.size());
    auto fanout_done = std::make_shared<CountDownLatch>(static_cast<int>(scan_ranges.size()));
    // Single-owner claim per slot. Two parties can target the same slot: the task,
    // and -- on the narrow ThreadPool path where do_submit enqueues the task and
    // THEN returns an error (thread creation failed with zero live workers) -- the
    // inline submit-failure path, which then runs concurrently with the very task it
    // failed to schedule. Each party claims the slot with an atomic exchange BEFORE
    // touching it: the winner runs the sync (or the inline fallback), writes
    // per_range_reason[idx] via publish_slot, and counts the latch down; the loser
    // returns having touched neither. Claiming before the RPC (an exchange, not a
    // check-then-act load) is what stops the enqueue-then-fail task from firing a
    // sync for an already-settled query, and it keeps the caller, once the latch
    // settles, the sole reader of a slot no task is still writing. (Value-initialized
    // to false.)
    auto slot_counted = std::make_shared<std::vector<std::atomic<bool>>>(scan_ranges.size());
    // Set true when the caller abandons the wait (its deadline passed): a not-yet-
    // started task then skips its sync RPC instead of spending the full retry
    // budget, so a meta-service brownout drains the bounded queue fast rather than
    // running abandoned work to completion and starving the pool.
    auto abandoned = std::make_shared<std::atomic<bool>>(false);
    // Called ONLY by whoever already won the exclusive slot claim (the slot_counted
    // exchange below), so it is the sole writer of the slot: publish its reason (an
    // empty reason means synced OK / skipped, leaving the default-empty slot) and
    // release the latch exactly once. The claim winner counting down is what lets the
    // caller move the slot out the instant the latch settles.
    auto publish_slot = [fanout_done, per_range_reason](size_t idx, std::string reason) {
        if (!reason.empty()) {
            (*per_range_reason)[idx] = std::move(reason);
        }
        fanout_done->count_down();
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
    // The tasks do no explicit MemTracker/ResourceContext attach: this is a
    // detached engine-layer sync with no live query context to charge to (the
    // query may have already timed out and returned, so attaching to it would be
    // both unavailable here -- this is a static engine path -- and wrong). Their
    // allocations fall to the worker thread's default accounting, matching the
    // same-shaped sync on the same kind of pool by CloudStorageEngine's other
    // sync_rowsets caller, CloudBackendService::sync_load_for_tablets (the FE
    // warmup path), which likewise does not attach.
    auto sync_fanout_start = std::chrono::steady_clock::now();
    auto& pool = engine->query_cache_delta_sync_pool();
    // Safe to capture the engine by raw pointer: stop() drains this pool before
    // the engine (and _meta_mgr/_tablet_mgr) is destroyed, so a running task
    // always sees a live engine.
    CloudStorageEngine* engine_ptr = engine;
    for (size_t i = 0; i < scan_ranges.size(); ++i) {
        int64_t tablet_id = scan_ranges[i].scan_range.palo_scan_range.tablet_id;
        Status submit_st = pool.submit_func([tablet_id, current_version, abandoned, i, engine_ptr,
                                             publish_slot, slot_counted]() {
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
            std::string reason;
            Defer done([&] { publish_slot(i, std::move(reason)); });
            // (a) Shutdown began, or the caller already abandoned the wait (its
            // deadline passed): bail before a fresh sync RPC so the pool drains
            // quickly instead of spending the full RPC retry budget on work whose
            // result no one will read. (An abandoned task's reason is never read --
            // the caller took the timeout path -- but recording it is harmless.)
            if (engine_ptr->stopped() || abandoned->load(std::memory_order_acquire)) {
                reason = "be is stopping, sync skipped";
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
            // so pin the dependency explicitly rather than inherit the struct
            // default (which a refactor could flip with no compile error): a
            // merge-on-write delta cannot be classified on a bitmap the sync did
            // not bring up to the queried version.
            options.sync_delete_bitmap = true;
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
                        << "query cache incremental merge falls back, cloud rowset sync failed"
                        << ", tablet_id=" << tablet_id << ", status=" << st.to_string();
                reason = "cloud rowset sync failed";
            }
        });
        if (!submit_st.ok()) {
            // Pool shutting down or queue at capacity: do not block. Claim the slot
            // for the inline fallback. do_submit rejects shutdown and capacity before
            // enqueue, so the closure will not run; on the narrow path where it did
            // enqueue before failing (thread creation at zero live workers), the
            // exchange makes exactly one of this path and the queued task own the slot
            // -- if the task claimed it first it will publish, so do nothing here.
            if (!(*slot_counted)[i].exchange(true, std::memory_order_acq_rel)) {
                publish_slot(i, "cloud rowset sync not scheduled");
            }
        }
    }

    // ONE absolute deadline across the whole operation, anchored at the fan-out
    // start above rather than restarted here. With the pre-started pool the submit
    // loop is pure enqueue and returns in microseconds, so remaining is essentially
    // the full budget; pinning the wait to the same start keeps the TOTAL budget at
    // query_cache_decision_sync_timeout_ms even if a degraded pool ever made
    // submission slow, instead of granting the full timeout again after submission
    // already consumed part of it.
    auto sync_deadline = sync_fanout_start +
                         std::chrono::milliseconds(config::query_cache_decision_sync_timeout_ms);
    auto sync_now = std::chrono::steady_clock::now();
    // Keep full clock precision here -- do NOT truncate to whole milliseconds.
    // Truncating would shave the sub-millisecond submit time off the budget and let
    // the total blocked time dip just under query_cache_decision_sync_timeout_ms.
    // wait_for never returns before its duration elapses, so with the raw remaining
    // the total time from sync_fanout_start stays >= the budget.
    auto remaining = sync_deadline > sync_now ? (sync_deadline - sync_now)
                                              : std::chrono::steady_clock::duration::zero();
    bool timed_out = !fanout_done->wait_for(remaining);
    if (timed_out) {
        // Tell not-yet-started tasks to skip their sync RPC so the pool drains
        // fast instead of running abandoned work to completion.
        abandoned->store(true, std::memory_order_release);
    }
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
    // Safe to read now: the fan-out completed, so no task is still writing.
    // Scan ranges carry distinct tablet ids in practice (each tablet is scanned
    // once per query), but were one to appear twice the fan-out would only
    // issue an idempotent redundant sync (serialized inside sync_rowsets by
    // _sync_meta_lock) and this map, keyed by tablet id, would still resolve to
    // one consistent reason for it -- so a duplicate is harmless, not something
    // to guard against with an unreachable de-dup branch.
    for (size_t i = 0; i < per_range_reason->size(); ++i) {
        if (!(*per_range_reason)[i].empty()) {
            fallback_reasons[scan_ranges[i].scan_range.palo_scan_range.tablet_id] =
                    std::move((*per_range_reason)[i]);
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
        // recompute. Consumed BEFORE get_tablet below, because that reason can
        // also mean the tablet was never loaded here (a refused submit loads
        // nothing): get_tablet would then take the synchronous cache-miss
        // meta-service load on this admission thread -- the very blocking
        // window the fast-fail budget exists to cap -- to fetch a tablet whose
        // decision is already known to be a fallback. No reason means the view
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
    auto tablet_res = ExecEnv::get_tablet(tablet_id);
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
