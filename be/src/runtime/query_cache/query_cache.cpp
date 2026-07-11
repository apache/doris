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

#include "cloud/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
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

    // The lock also serializes decision making of different instances of this
    // fragment. That is acceptable in the common case: a decision only touches
    // tablet metadata (no data IO), so it finishes in microseconds to
    // milliseconds. On very wide fragments (hundreds of tablets per instance)
    // combined with tablet header lock contention (e.g. compaction meta
    // commits) this serialization can stretch the fragment init tail; if that
    // ever shows up in profiles, move the capture out of the lock and let the
    // losing racer drop its duplicate decision.
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _decisions.find(cache_key);
    if (it != _decisions.end()) {
        return it->second;
    }

    auto decision = std::make_shared<QueryCacheInstanceDecision>();
    decision->key_valid = true;
    decision->cache_key = cache_key;
    decision->current_version = version;
    _make_decision(scan_ranges, decision.get());
    _decisions.emplace(std::move(cache_key), decision);
    return decision;
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
        DorisMetrics::instance()->query_cache_stale_hit_total->increment(1);
        return;
    }

    // Stale entry not reusable: drop the pin and fall back to a full scan.
    decision->_delta_read_sources.clear();
    decision->handle = QueryCacheHandle();
    decision->mode = QueryCacheInstanceDecision::Mode::MISS;
    if (!decision->incremental_fallback_reason.empty()) {
        // Only count real fallbacks: an empty reason means incremental merge
        // was not enabled for this query in the first place.
        DorisMetrics::instance()->query_cache_incremental_fallback_total->increment(1);
    }
}

bool QueryCacheRuntime::_try_prepare_incremental(const std::vector<TScanRangeParams>& scan_ranges,
                                                 QueryCacheInstanceDecision* decision) {
    if (!(_param.__isset.allow_incremental && _param.allow_incremental)) {
        // Not a fallback: incremental merge is simply not enabled for this
        // query, so leave incremental_fallback_reason empty.
        return false;
    }
    // Cloud tablets capture rowsets through a different (partly asynchronous)
    // path; incremental merge only supports local storage for now.
    if (config::is_cloud_mode()) {
        decision->incremental_fallback_reason = "cloud mode";
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

    for (const auto& scan_range : scan_ranges) {
        int64_t tablet_id = scan_range.scan_range.palo_scan_range.tablet_id;
        if (!_capture_tablet_delta(tablet_id, cached_version, decision)) {
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

bool QueryCacheRuntime::_capture_tablet_delta(int64_t tablet_id, int64_t cached_version,
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

    // quiet: on a version-graph miss (the delta merged away by compaction, an
    // expected per-query situation) this option makes the capture API neither
    // log nor error; it returns an EMPTY read source instead, so emptiness is
    // checked below as the failure signal.
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
    read_source->fill_delete_predicates();
    if (!read_source->delete_predicates.empty()) {
        // A delete in the delta logically removes rows that are already folded
        // into the cached partial result; that cannot be undone by merging, so
        // fall back to a full recompute.
        decision->incremental_fallback_reason = "delta contains delete predicates";
        return false;
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
    // The tablet delete bitmap is shared with concurrent loads, but entries
    // stamped with a version <= current_version are immutable once that
    // version is visible (merge-on-write publishes the bitmap update before
    // the version becomes visible), so this scan is race free for the window
    // we care about. Pending entries use DeleteBitmap::TEMP_VERSION_COMMON
    // (= 0) and stay below the window as well.
    const DeleteBitmap& delete_bitmap = tablet.tablet_meta()->delete_bitmap();
    std::shared_lock rdlock(delete_bitmap.lock);
    for (const auto& [bitmap_key, bitmap] : delete_bitmap.delete_bitmap) {
        const auto version = static_cast<int64_t>(std::get<2>(bitmap_key));
        if (version <= cached_version || version > current_version || bitmap.isEmpty()) {
            continue;
        }
        if (!delta_rowsets.contains(std::get<0>(bitmap_key))) {
            return true;
        }
    }
    return false;
}

} // namespace doris
