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

#include <butil/macros.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker.h"
#include "util/lru_cache.h"
#include "util/slice.h"
#include "util/time.h"

namespace doris {

class BaseTablet;
struct TabletReadSource;

using CacheResult = std::vector<BlockUPtr>;
// A handle for mid-result from query lru cache.
// The handle will automatically release the cache entry when it is destroyed.
// So the caller need to make sure the handle is valid in lifecycle.
class QueryCacheHandle {
public:
    QueryCacheHandle() = default;
    QueryCacheHandle(LRUCachePolicy* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~QueryCacheHandle() {
        if (_handle != nullptr) {
            CHECK(_cache != nullptr);
            {
                SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                        ExecEnv::GetInstance()->query_cache_mem_tracker());
                _cache->release(_handle);
            }
        }
    }

    QueryCacheHandle(QueryCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    QueryCacheHandle& operator=(QueryCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    bool valid() const { return _handle != nullptr; }

    std::vector<int>* get_cache_slot_orders();

    CacheResult* get_cache_result();

    int64_t get_cache_version();

    // How many incremental merges have been accumulated on this entry since the
    // last full recompute. See QueryCacheRuntime for the compaction policy.
    int64_t get_cache_delta_count();

    int64_t get_cache_total_bytes();

    int64_t get_cache_total_rows();

private:
    LRUCachePolicy* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(QueryCacheHandle);
};

class QueryCache : public LRUCachePolicy {
public:
    using LRUCachePolicy::insert;

    struct CacheValue : public LRUCacheValueBase {
        int64_t version;
        CacheResult result;
        std::vector<int> slot_orders;
        // Number of incremental merges accumulated on this entry since the last
        // full recompute. 0 means the entry was produced by a full scan.
        int64_t delta_count;
        // Size of this entry, used to decide upfront whether an incremental
        // merge could ever be written back under the entry_max_bytes/rows
        // limits (a merged entry can only be larger than the cached one).
        int64_t total_bytes;
        int64_t total_rows;

        CacheValue(int64_t v, CacheResult&& r, const std::vector<int>& so, int64_t dc = 0,
                   int64_t bytes = 0)
                : LRUCacheValueBase(),
                  version(v),
                  result(std::move(r)),
                  slot_orders(so),
                  delta_count(dc),
                  total_bytes(bytes) {
            total_rows = 0;
            for (const auto& block : result) {
                total_rows += block->rows();
            }
        }
    };

    // Create global instance of this class
    static QueryCache* create_global_cache(size_t capacity, uint32_t num_shards = 16) {
        auto* res = new QueryCache(capacity, num_shards);
        return res;
    }

    static Status build_cache_key(const std::vector<TScanRangeParams>& scan_ranges,
                                  const TQueryCacheParam& cache_param, std::string* cache_key,
                                  int64_t* version) {
        if (scan_ranges.empty()) {
            return Status::InternalError("scan_ranges is empty, plan error");
        }

        std::string digest;
        try {
            digest = cache_param.digest;
        } catch (const std::exception&) {
            return Status::InternalError("digest is invalid, plan error");
        }
        if (digest.empty()) {
            return Status::InternalError("digest is empty, plan error");
        }

        if (cache_param.tablet_to_range.empty()) {
            return Status::InternalError("tablet_to_range is empty, plan error");
        }

        std::vector<int64_t> tablet_ids;
        tablet_ids.reserve(scan_ranges.size());
        for (const auto& scan_range : scan_ranges) {
            auto tablet_id = scan_range.scan_range.palo_scan_range.tablet_id;
            tablet_ids.push_back(tablet_id);
        }
        std::sort(tablet_ids.begin(), tablet_ids.end());

        int64_t first_version = -1;
        std::string first_tablet_range;
        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            auto tablet_id = tablet_ids[i];

            auto find_tablet = cache_param.tablet_to_range.find(tablet_id);
            if (find_tablet == cache_param.tablet_to_range.end()) {
                return Status::InternalError("Not find tablet in partition_to_tablets, plan error");
            }

            auto scan_range_iter =
                    std::find_if(scan_ranges.begin(), scan_ranges.end(),
                                 [&tablet_id](const TScanRangeParams& range) {
                                     return range.scan_range.palo_scan_range.tablet_id == tablet_id;
                                 });
            int64_t current_version = -1;
            std::from_chars(scan_range_iter->scan_range.palo_scan_range.version.data(),
                            scan_range_iter->scan_range.palo_scan_range.version.data() +
                                    scan_range_iter->scan_range.palo_scan_range.version.size(),
                            current_version);

            if (i == 0) {
                first_version = current_version;
                first_tablet_range = find_tablet->second;
            } else {
                if (current_version != first_version) {
                    return Status::InternalError(
                            "All tablets in one instance must have the same version, plan error");
                }
                if (find_tablet->second != first_tablet_range) {
                    return Status::InternalError(
                            "All tablets in one instance must have the same tablet_to_range, plan "
                            "error");
                }
            }
        }

        *version = first_version;

        *cache_key = digest;
        for (auto tablet_id : tablet_ids) {
            *cache_key += std::string(reinterpret_cast<char*>(&tablet_id), sizeof(tablet_id));
        }
        *cache_key += first_tablet_range;

        return Status::OK();
    }

    // Return global instance.
    // Client should call create_global_cache before.
    static QueryCache* instance() { return ExecEnv::GetInstance()->get_query_cache(); }

    QueryCache() = delete;

    QueryCache(size_t capacity, uint32_t num_shards)
            : LRUCachePolicy(CachePolicy::CacheType::QUERY_CACHE, capacity, LRUCacheType::SIZE,
                             3600 * 24, /*num_shards*/ num_shards,
                             /*element_count_capacity*/ 0, /*enable_prune*/ true,
                             /*is_lru_k*/ true) {}

    // Ensure Block memory freed during eviction is tracked under query cache, not Orphan.
    int64_t adjust_capacity_weighted(double adjust_weighted) override {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->query_cache_mem_tracker());
        return LRUCachePolicy::adjust_capacity_weighted(adjust_weighted);
    }

    int64_t reset_initial_capacity(double adjust_weighted) override {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->query_cache_mem_tracker());
        return LRUCachePolicy::reset_initial_capacity(adjust_weighted);
    }

    void prune_stale() override {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->query_cache_mem_tracker());
        LRUCachePolicy::prune_stale();
    }

    bool lookup(const CacheKey& key, int64_t version, QueryCacheHandle* handle);

    // Look up the entry by key regardless of its version. The caller decides
    // whether the entry is an exact hit (cached version == expected version) or
    // a stale entry usable for incremental merge. Returns false if the key is
    // not in the cache at all.
    bool lookup_any_version(const CacheKey& key, QueryCacheHandle* handle);

    void insert(const CacheKey& key, int64_t version, CacheResult& result,
                const std::vector<int>& solt_orders, int64_t cache_size, int64_t delta_count = 0);
};

// The per-fragment-instance decision of how the query cache participates in the
// execution, made exactly once (see QueryCacheRuntime) and consumed by both the
// olap scan operator and the cache source operator, so the two operators can
// never disagree (e.g. scan skips scanning because the entry looked fresh while
// cache source misses because the entry got evicted in between -- which would
// silently produce an empty result and poison the cache with it).
struct QueryCacheInstanceDecision {
    enum class Mode {
        // Run the full scan and (if the key is valid) write the result back.
        MISS,
        // The cached entry matches the current version: emit cached blocks,
        // skip scanning entirely, do not write back.
        HIT,
        // A stale entry is reusable: scan only the delta rowsets in
        // (cached_version, current_version], emit the cached blocks and the
        // delta partial result side by side (the upstream merge aggregation
        // combines them), then write the merged entry back.
        INCREMENTAL,
    };

    ~QueryCacheInstanceDecision();

    // Take the pre-captured delta read source of one tablet. Returns nullptr if
    // absent (already taken or never captured). Only meaningful in INCREMENTAL
    // mode; each tablet's read source can be consumed exactly once.
    std::unique_ptr<TabletReadSource> take_delta_read_source(int64_t tablet_id);

    Mode mode = Mode::MISS;
    // False when build_cache_key failed (e.g. tablets in this instance carry
    // different versions because FE could not align instances to partitions).
    // In that case the query degrades to an uncached scan: no lookup, no write
    // back, but the query itself still succeeds.
    bool key_valid = false;
    // False when the merged entry could never satisfy entry_max_bytes/rows
    // because the reused cached entry alone already exceeds them: the query
    // still scans only the delta (INCREMENTAL), but skips cloning blocks for a
    // write back that would be discarded anyway.
    bool write_back_feasible = true;
    // Why a stale entry was not reused incrementally (empty when it was, or
    // when incremental merge is not enabled for this query). For the query
    // profile only.
    std::string incremental_fallback_reason;
    std::string cache_key;
    // The version this query is reading (from the scan ranges).
    int64_t current_version = 0;
    // Only set in INCREMENTAL mode: the version of the reused stale entry.
    int64_t cached_version = 0;
    // Only set in HIT/INCREMENTAL mode: delta merges accumulated on the entry.
    int64_t cached_delta_count = 0;
    // Pins the cache entry in HIT/INCREMENTAL mode so it cannot be evicted (and
    // its blocks cannot be freed) while this query is using it. Note the pin
    // lives until the fragment is torn down; when the merged entry replaces
    // this one under the same key, both stay in memory for that window and the
    // LRU usage accounting only sees the new one (the mem tracker still sees
    // both) -- bounded by (in-flight incremental queries) x entry size.
    QueryCacheHandle handle;

private:
    friend class QueryCacheRuntime;
    std::mutex _take_lock;
    // INCREMENTAL mode: read sources of (cached_version, current_version]
    // captured at decision time, keyed by tablet id. Captured eagerly so that a
    // capture failure (e.g. the delta versions were merged away by compaction)
    // downgrades the decision to MISS *before* any operator acts on it; if the
    // scan discovered the failure only at prepare time, the cache source might
    // already have decided to emit the stale blocks.
    std::unordered_map<int64_t, std::unique_ptr<TabletReadSource>> _delta_read_sources;
};

// Fragment-level query cache context shared by the olap scan operator and the
// cache source operator of the same fragment. Both operators obtain the cache
// decision of their instance through get_or_make_decision(); the first caller
// makes the decision and the other one observes the same object, whatever the
// operator local-state init order is.
class QueryCacheRuntime {
public:
    // `cache` is injectable for tests; production callers pass nullptr and the
    // global instance is used.
    explicit QueryCacheRuntime(const TQueryCacheParam& param, QueryCache* cache = nullptr)
            : _param(param), _cache(cache != nullptr ? cache : QueryCache::instance()) {}

    QueryCache* cache() const { return _cache; }

    // Row-binlog scans read a different data stream and must not serve or fill
    // the query cache. Called while building the operator tree (single
    // threaded, before any local state init), so no locking is needed.
    void disable_for_binlog_scan() { _binlog_scan = true; }

    // Idempotent: the first call for a given instance (identified by the cache
    // key derived from its scan ranges) makes the decision, later calls return
    // the same decision object. Never returns nullptr.
    std::shared_ptr<QueryCacheInstanceDecision> get_or_make_decision(
            const std::vector<TScanRangeParams>& scan_ranges);

#ifdef BE_TEST
    // Tests inject a hand-crafted decision (e.g. INCREMENTAL) for an instance,
    // since a real storage engine is unavailable to capture delta read sources.
    void inject_decision_for_test(const std::string& cache_key,
                                  std::shared_ptr<QueryCacheInstanceDecision> decision) {
        std::lock_guard<std::mutex> lock(_lock);
        _decisions[cache_key] = std::move(decision);
    }
#endif

private:
    void _make_decision(const std::vector<TScanRangeParams>& scan_ranges,
                        QueryCacheInstanceDecision* decision);

    // Try to turn a stale entry into an INCREMENTAL decision. Returns true on
    // success; on any failure the caller keeps the decision as MISS (full
    // recompute), which is always safe.
    bool _try_prepare_incremental(const std::vector<TScanRangeParams>& scan_ranges,
                                  QueryCacheInstanceDecision* decision);

    // Cloud only: bring every append-only scanned tablet's local view up to the
    // queried version in parallel (a fork-join of sync_rowsets, matching the
    // scan node's own tablet-sync fan-out) before the serial per-tablet capture
    // below runs, instead of issuing those RPCs one tablet at a time on the
    // shared prepare thread. This is the single sync per tablet: _capture_
    // tablet_delta consumes the result here and does not sync again. Returns
    // the per-tablet fallback reasons the sync produced (keyed by tablet id,
    // only failures present): a cast failure ("tablet is not a cloud tablet")
    // or an infrastructure sync failure ("cloud rowset sync failed"). Tablets
    // that are not append-only are skipped (no wasted RPC); _capture_tablet_
    // delta rejects them at its own keys-type check.
    static std::unordered_map<int64_t, std::string> _presync_cloud_delta_tablets(
            const std::vector<TScanRangeParams>& scan_ranges, int64_t current_version);

    // Validate one tablet for incremental merge and capture its delta read
    // source of (cached_version, current_version]. On any failure records the
    // fallback reason in the decision and returns false. In cloud mode the view
    // sync already ran in _presync_cloud_delta_tablets; its per-tablet failure
    // reasons arrive through presync_reasons.
    bool _capture_tablet_delta(int64_t tablet_id, int64_t cached_version,
                               const std::unordered_map<int64_t, std::string>& presync_reasons,
                               QueryCacheInstanceDecision* decision);

    // Merge-on-write only: true if any delete-bitmap entry stamped with a
    // version inside (cached_version, current_version] targets a rowset
    // OUTSIDE the captured delta set, i.e. the delta window rewrote rows that
    // are already folded into the cached partial result (an upsert, a partial
    // update or a delete sign hit a key that predates the cached version).
    // Entries targeting the delta rowsets themselves are harmless: the delta
    // scan reads those rowsets with the delete bitmap applied.
    static bool _delta_rewrites_history(BaseTablet& tablet, const TabletReadSource& delta_source,
                                        int64_t cached_version, int64_t current_version);

    TQueryCacheParam _param;
    QueryCache* _cache = nullptr;
    bool _binlog_scan = false;

    std::mutex _lock;
    std::map<std::string, std::shared_ptr<QueryCacheInstanceDecision>> _decisions;
    // Shared by every instance whose cache key cannot be built (see
    // get_or_make_decision): one immutable MISS decision, one log line.
    std::shared_ptr<QueryCacheInstanceDecision> _invalid_decision;
};

} // namespace doris
