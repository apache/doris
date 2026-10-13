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

#include "io/cache/tablet_cache_policy_registry.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <chrono>
#include <limits>

#include "common/config.h"
#include "common/logging.h"
#include "io/cache/cache_block_meta_store.h"
#include "runtime/exec_env.h"
#include "storage/tablet/tablet_meta.h"
#include "util/thread.h"
#include "util/time.h"

namespace doris::io {

bvar::Adder<uint64_t> g_tablet_cache_policy_fetch_num("file_cache_tablet_policy_fetch_num");
bvar::Adder<uint64_t> g_tablet_cache_policy_fetch_failed_num(
        "file_cache_tablet_policy_fetch_failed_num");
bvar::PassiveStatus<size_t> g_tablet_cache_policy_num(
        "file_cache_tablet_policy_num",
        [](void*) { return TabletCachePolicyRegistry::instance()->size(); }, nullptr);

namespace {

// How often the record is scanned for policies nobody needs any more.
constexpr int64_t kPruneIntervalS = 60;
// How long a tablet whose policy could not be fetched is left alone.
constexpr int64_t kFetchRetryIntervalS = 60;
// How many reported tablets are looked up per hold of the exclusive lock.
constexpr size_t kReportBatchSize = 1024;
// How often the background refresh runs.
constexpr std::chrono::seconds kRefreshInterval {1};

Status fetch_from_tablet_meta(int64_t tablet_id, TabletCachePolicy* policy) {
    TabletMetaSharedPtr tablet_meta;
    RETURN_IF_ERROR(ExecEnv::get_tablet_meta(tablet_id, &tablet_meta, false));
    DORIS_CHECK(tablet_meta != nullptr);
    policy->ttl_seconds = tablet_meta->ttl_seconds();
    policy->ctime = tablet_meta->creation_time();
    return Status::OK();
}

} // namespace

int64_t TabletCachePolicy::deadline() const {
    if (ttl_seconds <= 0 || ctime <= 0) {
        return 0;
    }
    // Same rule as TabletMeta::file_cache_ttl_expiration_time(): a huge ttl must not wrap into
    // a deadline in the past.
    if (ctime > std::numeric_limits<int64_t>::max() - ttl_seconds) {
        return std::numeric_limits<int64_t>::max();
    }
    return ctime + ttl_seconds;
}

TabletCachePolicyRegistry* TabletCachePolicyRegistry::instance() {
    // Never destroyed: a cache path may still be running when the process exits.
    static auto* registry = new TabletCachePolicyRegistry();
    return registry;
}

std::optional<int64_t> TabletCachePolicyRegistry::deadline(int64_t tablet_id) const {
    std::shared_lock lock(_mutex);
    auto it = _entries.find(tablet_id);
    if (it == _entries.end()) {
        return std::nullopt;
    }
    return it->second.policy.deadline();
}

int64_t TabletCachePolicyRegistry::protection_deadline(int64_t tablet_id, int64_t legacy_deadline) {
    // tablet_id 0 is data outside any Doris tablet (external tables, v2 cache files that never
    // recorded one): it only has the deadline it was written with.
    if (tablet_id <= 0) {
        return legacy_deadline;
    }
    const std::optional<int64_t> known = deadline(tablet_id);
    if (!known.has_value()) {
        note_tablet(tablet_id);
        return legacy_deadline;
    }
    return *known;
}

std::optional<TabletCachePolicy> TabletCachePolicyRegistry::set_locked(
        int64_t tablet_id, const TabletCachePolicy& policy, int64_t refreshed_at_s) {
    auto [it, inserted] =
            _entries.try_emplace(tablet_id, Entry {policy, refreshed_at_s, refreshed_at_s});
    std::optional<TabletCachePolicy> before;
    if (!inserted) {
        before = it->second.policy;
        if (const int64_t old_deadline = before->deadline(); old_deadline > 0) {
            _deadlines.erase(_deadlines.find(old_deadline));
        }
        it->second.policy = policy;
        it->second.refreshed_at_s = refreshed_at_s;
    }
    if (const int64_t new_deadline = policy.deadline(); new_deadline > 0) {
        _deadlines.insert(new_deadline);
    }
    return before;
}

void TabletCachePolicyRegistry::bump_versions(const std::optional<TabletCachePolicy>& before,
                                              const std::optional<TabletCachePolicy>& after) {
    // An unknown tablet is judged by the deadline its blocks carry, so learning or forgetting a
    // tablet can move its blocks either way.
    const int64_t old_deadline = before.has_value() ? before->deadline() : -1;
    const int64_t new_deadline = after.has_value() ? after->deadline() : -1;
    if (!before.has_value() || !after.has_value() || new_deadline > old_deadline) {
        _protection_gained_version.fetch_add(1, std::memory_order_acq_rel);
    }
    if (!before.has_value() || !after.has_value() || new_deadline < old_deadline) {
        _protection_lost_version.fetch_add(1, std::memory_order_acq_rel);
    }
}

void TabletCachePolicyRegistry::persist(int64_t tablet_id,
                                        const std::optional<TabletCachePolicy>& policy) {
    std::lock_guard lock(_stores_mutex);
    for (auto* store : _stores) {
        if (policy.has_value()) {
            store->put_tablet_policy(tablet_id, *policy);
        } else {
            store->delete_tablet_policy(tablet_id);
        }
    }
}

void TabletCachePolicyRegistry::upsert(int64_t tablet_id, const TabletCachePolicy& policy) {
    DORIS_CHECK(tablet_id > 0);
    const int64_t now = UnixSeconds();
    {
        // Unchanged and recently confirmed is the common case: every meta refresh reports the
        // policy. Only take the exclusive lock to record a change, or to push the refresh time
        // forward once it is half way to being due.
        std::shared_lock lock(_mutex);
        auto it = _entries.find(tablet_id);
        if (it != _entries.end() && it->second.policy == policy &&
            it->second.refreshed_at_s >
                    now - config::file_cache_tablet_policy_refresh_interval_s / 2) {
            return;
        }
    }
    std::optional<TabletCachePolicy> before;
    {
        std::unique_lock lock(_mutex);
        before = set_locked(tablet_id, policy, now);
    }
    if (before == policy) {
        return;
    }
    bump_versions(before, policy);
    persist(tablet_id, policy);
}

void TabletCachePolicyRegistry::erase(int64_t tablet_id) {
    std::optional<TabletCachePolicy> before;
    {
        std::unique_lock lock(_mutex);
        auto it = _entries.find(tablet_id);
        if (it == _entries.end()) {
            return;
        }
        before = it->second.policy;
        if (const int64_t old_deadline = before->deadline(); old_deadline > 0) {
            _deadlines.erase(_deadlines.find(old_deadline));
        }
        _entries.erase(it);
    }
    bump_versions(before, std::nullopt);
    persist(tablet_id, std::nullopt);
}

void TabletCachePolicyRegistry::restore(int64_t tablet_id, const TabletCachePolicy& policy) {
    std::optional<TabletCachePolicy> before;
    {
        std::unique_lock lock(_mutex);
        if (_entries.contains(tablet_id)) {
            // Already known from the live meta or from another cache path's copy.
            return;
        }
        // Never refreshed by this process, so it is due as soon as a cache path reports it;
        // counted as reported now so that it is not forgotten before the cache has loaded.
        before = set_locked(tablet_id, policy, 0);
        _entries[tablet_id].reported_at_s = UnixSeconds();
    }
    bump_versions(before, policy);
}

void TabletCachePolicyRegistry::note_tablet(int64_t tablet_id) {
    _noted_tablets.enqueue(tablet_id);
}

bool TabletCachePolicyRegistry::has_deadline_in(int64_t from, int64_t to) const {
    std::shared_lock lock(_mutex);
    auto it = _deadlines.upper_bound(from);
    return it != _deadlines.end() && *it <= to;
}

void TabletCachePolicyRegistry::report_cached_tablets(const std::vector<int64_t>& tablet_ids) {
    const int64_t now = UnixSeconds();
    const int64_t stale_before = now - config::file_cache_tablet_policy_refresh_interval_s;
    std::vector<int64_t> due;
    // In batches: a path may hold blocks of many tablets, and the readers waiting on this lock
    // hold their cache lock.
    for (size_t begin = 0; begin < tablet_ids.size(); begin += kReportBatchSize) {
        const size_t end = std::min(tablet_ids.size(), begin + kReportBatchSize);
        std::unique_lock lock(_mutex);
        for (size_t i = begin; i < end; ++i) {
            const int64_t tablet_id = tablet_ids[i];
            auto it = _entries.find(tablet_id);
            if (it == _entries.end()) {
                due.push_back(tablet_id);
                continue;
            }
            it->second.reported_at_s = now;
            if (it->second.refreshed_at_s <= stale_before) {
                due.push_back(tablet_id);
            }
        }
    }
    std::lock_guard due_lock(_due_mutex);
    _due.insert(due.begin(), due.end());
}

std::vector<int64_t> TabletCachePolicyRegistry::take_due(int64_t now, size_t budget) {
    std::vector<int64_t> unknown;
    int64_t tablet_id = 0;
    while (_noted_tablets.try_dequeue(tablet_id)) {
        if (!deadline(tablet_id).has_value()) {
            unknown.push_back(tablet_id);
        }
    }
    std::lock_guard due_lock(_due_mutex);
    _due.insert(unknown.begin(), unknown.end());
    std::vector<int64_t> batch;
    for (auto it = _due.begin(); it != _due.end() && batch.size() < budget; it = _due.erase(it)) {
        auto retry = _retry_at.find(*it);
        if (retry == _retry_at.end() || retry->second <= now) {
            batch.push_back(*it);
        }
    }
    return batch;
}

void TabletCachePolicyRegistry::prune(int64_t now) {
    if (now < _next_prune_s) {
        return;
    }
    _next_prune_s = now + kPruneIntervalS;
    std::erase_if(_retry_at, [now](const auto& retry) { return retry.second <= now; });
    // A tablet no cache path holds and nothing refreshes (unloaded, or dropped) has no blocks
    // to judge; it is learned again if its blocks come back.
    const int64_t forget_before = now - 2 * config::file_cache_tablet_policy_refresh_interval_s;
    std::vector<int64_t> unused;
    {
        std::shared_lock lock(_mutex);
        for (const auto& [id, entry] : _entries) {
            if (std::max(entry.refreshed_at_s, entry.reported_at_s) < forget_before) {
                unused.push_back(id);
            }
        }
    }
    for (int64_t id : unused) {
        erase(id);
    }
}

void TabletCachePolicyRegistry::refresh(size_t budget) {
    std::lock_guard refresh_lock(_refresh_mutex);
    const int64_t now = UnixSeconds();
    const std::vector<int64_t> batch = take_due(now, budget);
    prune(now);
    const Fetcher& fetch = _fetcher ? _fetcher : Fetcher(fetch_from_tablet_meta);
    for (size_t i = 0; i < batch.size(); ++i) {
        const int64_t tablet_id = batch[i];
        TabletCachePolicy policy;
        Status st = fetch(tablet_id, &policy);
        g_tablet_cache_policy_fetch_num << 1;
        if (st.ok()) {
            _retry_at.erase(tablet_id);
            upsert(tablet_id, policy);
            continue;
        }
        if (st.is<ErrorCode::NOT_FOUND>()) {
            _retry_at.erase(tablet_id);
            erase(tablet_id);
            continue;
        }
        // Most likely the meta service is unavailable: the rest waits for the next round rather
        // than for one failed call after another. Meanwhile a known tablet keeps being judged by
        // its last known policy, an unknown one by the deadline its blocks were written with.
        g_tablet_cache_policy_fetch_failed_num << 1;
        LOG_EVERY_N(WARNING, 100) << "failed to fetch file cache ttl policy of tablet " << tablet_id
                                  << ": " << st;
        _retry_at[tablet_id] = UnixSeconds() + kFetchRetryIntervalS;
        std::lock_guard due_lock(_due_mutex);
        _due.insert(batch.begin() + i + 1, batch.end());
        break;
    }
}

void TabletCachePolicyRegistry::register_cache() {
    std::lock_guard lock(_thread_mutex);
    if (_open_caches++ > 0) {
        return;
    }
    {
        std::lock_guard stop_lock(_stop_mutex);
        _stop_refresh = false;
    }
    _refresh_thread = std::thread(&TabletCachePolicyRegistry::refresh_loop, this);
}

void TabletCachePolicyRegistry::unregister_cache() {
    std::lock_guard lock(_thread_mutex);
    DCHECK_GT(_open_caches, 0);
    if (--_open_caches > 0) {
        return;
    }
    {
        std::lock_guard stop_lock(_stop_mutex);
        _stop_refresh = true;
    }
    _stop_cv.notify_all();
    _refresh_thread.join();
}

void TabletCachePolicyRegistry::refresh_loop() {
    Thread::set_self_name("file_cache_tablet_policy_refresh");
    while (true) {
        {
            std::unique_lock stop_lock(_stop_mutex);
            if (_stop_cv.wait_for(stop_lock, kRefreshInterval, [this] { return _stop_refresh; })) {
                return;
            }
        }
        refresh(static_cast<size_t>(config::file_cache_tablet_policy_refresh_batch));
    }
}

void TabletCachePolicyRegistry::add_store(CacheBlockMetaStore* store) {
    std::lock_guard lock(_stores_mutex);
    _stores.push_back(store);
}

void TabletCachePolicyRegistry::remove_store(CacheBlockMetaStore* store) {
    std::lock_guard lock(_stores_mutex);
    std::erase(_stores, store);
}

size_t TabletCachePolicyRegistry::size() const {
    std::shared_lock lock(_mutex);
    return _entries.size();
}

void TabletCachePolicyRegistry::set_fetcher(Fetcher fetcher) {
    std::lock_guard refresh_lock(_refresh_mutex);
    _fetcher = std::move(fetcher);
}

void TabletCachePolicyRegistry::clear() {
    std::lock_guard refresh_lock(_refresh_mutex);
    std::unique_lock lock(_mutex);
    _entries.clear();
    _deadlines.clear();
    {
        std::lock_guard due_lock(_due_mutex);
        _due.clear();
    }
    _retry_at.clear();
    _next_prune_s = 0;
    int64_t tablet_id = 0;
    while (_noted_tablets.try_dequeue(tablet_id)) {
    }
    _protection_gained_version.fetch_add(1, std::memory_order_acq_rel);
    _protection_lost_version.fetch_add(1, std::memory_order_acq_rel);
}

} // namespace doris::io
