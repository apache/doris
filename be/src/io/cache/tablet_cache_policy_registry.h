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

#include <concurrentqueue.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"

namespace doris::io {

class CacheBlockMetaStore;

// The file cache TTL policy of a tablet: its blocks are protected from LRU eviction until
// ctime + ttl_seconds. Only ttl_seconds changes over the life of a tablet (ALTER), ctime never.
struct TabletCachePolicy {
    int64_t ttl_seconds = 0;
    int64_t ctime = 0;

    // The absolute deadline, 0 when the tablet has no TTL.
    int64_t deadline() const;
    bool operator==(const TabletCachePolicy&) const = default;
};

// BE-wide record of the TTL policy of the tablets whose blocks are cached on this BE. It is
// shared by all cache paths because the blocks of a tablet are spread over every path by hash.
//
// Whether a block is protected is never stored on the block. It is computed from this record
// whenever a block is written or considered for eviction, so a policy change takes effect at
// once and a stale copy cannot outlive a restart. A tablet the record does not know yet falls
// back to the deadline its blocks were written with, which is how every block was judged
// before this record existed.
class TabletCachePolicyRegistry {
public:
    using Fetcher = std::function<Status(int64_t tablet_id, TabletCachePolicy* policy)>;

    static TabletCachePolicyRegistry* instance();

    // The tablet's deadline (0 when it has no TTL), or nullopt when the tablet is unknown.
    std::optional<int64_t> deadline(int64_t tablet_id) const;
    // The deadline a block of `tablet_id` written with `legacy_deadline` is protected until: its
    // tablet's current one, or else `legacy_deadline`, in which case `*legacy` is set. An unknown
    // tablet is noted for the next refresh.
    int64_t protected_until(int64_t tablet_id, int64_t legacy_deadline, bool* legacy);
    // Whether a block of `tablet_id` written with `legacy_deadline` is protected at `now`.
    bool is_protected(int64_t tablet_id, int64_t legacy_deadline, int64_t now);

    // Records the tablet's current policy as read from its meta. Compares under the shared lock
    // first, so reporting an unchanged policy on every meta refresh costs no exclusive lock.
    void upsert(int64_t tablet_id, const TabletCachePolicy& policy);
    // Forgets a tablet that no longer exists.
    void erase(int64_t tablet_id);
    // Loads a persisted policy at startup: known at once, but due for a refresh.
    void restore(int64_t tablet_id, const TabletCachePolicy& policy);

    // Notes a tablet whose blocks are cached here, so that its policy gets fetched if it is not
    // known yet. Called under the cache lock, so it only enqueues.
    void note_tablet(int64_t tablet_id);
    // Called periodically by each cache path with the tablets it holds blocks of. Their policy is
    // fetched when it is unknown, or when nothing confirmed it (a meta refresh of the loaded
    // tablet) for file_cache_tablet_policy_refresh_interval_s. Only cached tablets are fetched.
    // Never waits for a fetch.
    void report_cached_tablets(const std::vector<int64_t>& tablet_ids);
    // Fetches due policies, at most `budget` of them, and forgets the policies of tablets that
    // no cache path reported and nothing refreshed for twice the refresh interval. A fetch that
    // fails ends the round and puts that tablet off for a while, so that an unavailable meta
    // service is not asked again and again. Run every second by the background refresh.
    void refresh(size_t budget);

    // The background refresh runs while at least one cache is open, so that it never outlives
    // what it fetches for. The fetches are remote calls; they never hold up a cache path.
    void register_cache();
    void unregister_cache();

    // Bumped whenever some tablet gains protection (TTL added or deadline moved later), and
    // whenever some tablet loses it (TTL removed or deadline moved earlier). A cache path
    // sweeps the queue that can be affected when the matching version changes.
    uint64_t protection_gained_version() const {
        return _protection_gained_version.load(std::memory_order_acquire);
    }
    uint64_t protection_lost_version() const {
        return _protection_lost_version.load(std::memory_order_acquire);
    }
    // Whether the deadline of some known tablet falls in (from, to].
    bool has_deadline_in(int64_t from, int64_t to) const;

    // Every cache path keeps a copy of the record, so that a restart knows the policies before
    // the first refresh. The copies are written asynchronously and only on change.
    void add_store(CacheBlockMetaStore* store);
    void remove_store(CacheBlockMetaStore* store);

    size_t size() const;
    // Test only: replaces how a tablet's policy is fetched.
    void set_fetcher(Fetcher fetcher);
    // Test only: forgets every tablet.
    void clear();

private:
    struct Entry {
        TabletCachePolicy policy;
        // When the policy was last confirmed from the tablet's meta.
        int64_t refreshed_at_s = 0;
        // When a cache path last reported holding blocks of the tablet.
        int64_t reported_at_s = 0;
    };

    // Applies the change under the exclusive lock, returns the previous policy if any.
    std::optional<TabletCachePolicy> set_locked(int64_t tablet_id, const TabletCachePolicy& policy,
                                                int64_t refreshed_at_s);
    std::optional<TabletCachePolicy> erase_locked(int64_t tablet_id);
    void bump_versions(const std::optional<TabletCachePolicy>& before,
                       const std::optional<TabletCachePolicy>& after);
    void persist(int64_t tablet_id, const std::optional<TabletCachePolicy>& policy);
    // Takes at most `budget` tablets to fetch: the due ones, and the noted ones still unknown.
    // A tablet whose fetch failed recently is dropped instead; it is noted or reported again
    // later. Caller holds _refresh_mutex.
    std::vector<int64_t> take_due(int64_t now, size_t budget);
    // Forgets the policies nobody needs any more. Caller holds _refresh_mutex.
    void prune(int64_t now);
    void refresh_loop();

    mutable std::shared_mutex _mutex;
    std::unordered_map<int64_t, Entry> _entries;
    // Deadlines (> 0) of the known tablets, to tell when one has passed.
    std::multiset<int64_t> _deadlines;

    std::atomic<uint64_t> _protection_gained_version {0};
    std::atomic<uint64_t> _protection_lost_version {0};

    moodycamel::ConcurrentQueue<int64_t> _noted_tablets;
    // Tablets waiting to be fetched. Its own lock, never held across a fetch.
    std::mutex _due_mutex;
    std::unordered_set<int64_t> _due;
    // Held for a whole refresh, fetches included; only the refresh and the test hooks take it.
    std::mutex _refresh_mutex;
    // Guarded by _refresh_mutex: when a tablet whose fetch failed may be fetched again, and when
    // the record is next scanned for policies nobody needs any more.
    std::unordered_map<int64_t, int64_t> _retry_at;
    int64_t _next_prune_s = 0;
    Fetcher _fetcher;

    std::mutex _thread_mutex;
    size_t _open_caches = 0;
    std::thread _refresh_thread;
    std::mutex _stop_mutex;
    std::condition_variable _stop_cv;
    bool _stop_refresh = false;

    std::mutex _stores_mutex;
    std::vector<CacheBlockMetaStore*> _stores;
};

} // namespace doris::io
