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
#include <future>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_meta_mgr.h"
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
        presync_reasons =
                _presync_cloud_delta_tablets(scan_ranges, decision->current_version);
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
    // Index-aligned so the fork-joined tasks write disjoint slots without a
    // lock; an empty slot means synced (or skipped as non-append-only), a
    // non-empty slot is the fallback reason. Held behind a shared_ptr because
    // the fan-out below is waited on with a fast-fail deadline: on a timeout
    // this frame returns while the still-running tasks keep writing their
    // slots, so the storage must outlive the frame -- the shared_ptr each task
    // captured keeps it alive until the last one finishes. Only read on the
    // completed (non-timeout) path, where every task is done.
    auto per_range_reason = std::make_shared<std::vector<std::string>>(scan_ranges.size());
    std::vector<std::function<Status()>> tasks;
    tasks.reserve(scan_ranges.size());
    for (size_t i = 0; i < scan_ranges.size(); ++i) {
        int64_t tablet_id = scan_ranges[i].scan_range.palo_scan_range.tablet_id;
        tasks.emplace_back([tablet_id, current_version, per_range_reason, i]() -> Status {
            auto tablet_res = ExecEnv::get_tablet(tablet_id);
            if (!tablet_res) {
                // _capture_tablet_delta reports "tablet not found" from its own
                // get_tablet; nothing to sync. Should that later get_tablet
                // succeed instead (a transient failure here), it takes the
                // cache-miss load, which itself syncs rowsets AND the delete
                // bitmap up to the visible version -- that load-bearing
                // invariant (plus the endpoint check downstream) is what keeps
                // a tablet that skipped this pre-sync safe to capture.
                return Status::OK();
            }
            BaseTabletSPtr tablet = std::move(tablet_res.value());
            const bool append_only =
                    tablet->keys_type() == KeysType::DUP_KEYS ||
                    (tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                     tablet->enable_unique_key_merge_on_write());
            if (!append_only) {
                // Skip the RPC entirely: _capture_tablet_delta rejects this
                // tablet at its keys-type check before it would consume a sync.
                return Status::OK();
            }
            auto cloud_tablet = std::dynamic_pointer_cast<CloudTablet>(std::move(tablet));
            if (cloud_tablet == nullptr) {
                // is_cloud_mode() flipped on a live local deployment
                // (cloud_unique_id is a mutable config): degrade to a full
                // recompute rather than dereference the failed cast downstream.
                (*per_range_reason)[i] = "tablet is not a cloud tablet";
                return Status::OK();
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
                (*per_range_reason)[i] = "cloud rowset sync failed";
            }
            return Status::OK();
        });
    }
    // Fan the syncs out asynchronously and wait on the result with a fast-fail
    // budget instead of blocking unconditionally. This decision runs in
    // operator init on a bounded query-admission pool (the BE light_work_pool,
    // contractually "must be light, not locked"), so a meta-service brownout
    // that stalls these RPCs must not hold that thread for the full RPC retry
    // budget (tens of seconds): sustained, it would exhaust the pool and reject
    // query admission cluster-wide. If the fan-out does not finish within
    // query_cache_decision_sync_timeout_ms the decision abandons the wait and
    // falls the whole instance back to a full recompute; the still-running
    // tasks are correctness-harmless and bounded: on a slow-but-healthy sync
    // each merely advances the tablet view early (work the scan node's own
    // async sync would otherwise do), and on a failing sync each is at worst one
    // extra bounded sync attempt that changes no result; either way they stay
    // cheap while pending (small per-task captures; the brpc I/O yields the
    // pthread rather than pinning it) and self-limiting once the meta service
    // recovers, and keep their slot storage alive through the shared_ptr they
    // captured. A healthy sync is
    // milliseconds, far under the budget, so this only trips under real
    // meta-service degradation and leaves the steady-state incremental path
    // unchanged.
    //
    // Parallelism reuses init_scanner_sync_rowsets_parallelism on purpose: this
    // is the same per-tablet rowset-sync fan-out the scan node runs, and one
    // shared knob keeps the two fan-outs' budgets aligned rather than letting a
    // cache-only config silently drift apart from it. Every task swallows its
    // own error into its slot and returns OK, which is load-bearing:
    // bthread_fork_join stops DISPATCHING the tasks queued after the first one
    // that returns non-OK (see cloud_meta_mgr.cpp), so a task that propagated
    // its sync error would leave the tablets queued behind it un-synced;
    // recording the failure into the slot keeps every tablet's sync attempted.
    // The blocking time (bounded by the budget) is folded into
    // query_cache_decision_sync_time_ms.
    auto sync_fanout_start = std::chrono::steady_clock::now();
    std::future<Status> fanout_done;
    // Clamp the shared parallelism knob to >= 1: bthread_fork_join waits for a
    // free slot before dispatching its first task (count 0 >= concurrency 0),
    // so a misconfigured 0/negative would park the driver bthread forever with
    // nothing to notify it. On the scan node that surfaces as a hung query; on
    // this fast-fail path the caller's wait_for still expires and falls back, so
    // the deadlock would instead leak the driver bthread and its task closures
    // silently, once per incremental candidate. A local floor avoids that.
    Status launch_st = cloud::bthread_fork_join(
            std::move(tasks), std::max(1, config::init_scanner_sync_rowsets_parallelism),
            &fanout_done);
    bool timed_out = false;
    if (!launch_st.ok()) {
        // Could not even spawn the fan-out driver bthread: fall back rather than
        // block, the same as a timeout (its future would never be fulfilled).
        timed_out = true;
    } else if (fanout_done.wait_for(std::chrono::milliseconds(
                       config::query_cache_decision_sync_timeout_ms)) !=
               std::future_status::ready) {
        timed_out = true;
    } else {
        // Completed within budget. Every task returns OK by construction (each
        // swallows its own error into its slot), so the join status is always
        // OK; assert it rather than discard, so a future refactor that lets a
        // task propagate an error trips here instead of silently leaving the
        // tablets queued behind it un-synced.
        Status join_st = fanout_done.get();
        DCHECK(join_st.ok()) << join_st;
    }
    DorisMetrics::instance()->query_cache_decision_sync_time_ms->increment(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - sync_fanout_start)
                    .count());

    std::unordered_map<int64_t, std::string> fallback_reasons;
    if (timed_out) {
        // The fan-out outran its fast-fail budget. Do NOT read per_range_reason
        // (the detached tasks are still writing it); fall every scanned tablet
        // back so the whole instance recomputes in full. A distinct reason from
        // a hard sync failure so the profile tells a slow meta service apart
        // from a failing one. Throttled like the sync-failure log above: a
        // brownout trips this for every stale tablet of every query at once, and
        // this timeout is the primary operator-visible symptom of that brownout,
        // so a sampled line correlates it without a storm (the per-query reason
        // still reaches the user via the profile).
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
        // sync failure, or the fast-fail budget expiring on a slow meta
        // service), so fall back to a full recompute. No reason means the view
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
