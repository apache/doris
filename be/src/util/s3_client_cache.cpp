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

#include "util/s3_client_cache.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <future>
#include <iterator>
#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

#include "core/custom_allocator.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/cache_policy.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/s3_util.h"
#include "util/time.h"

namespace doris {

struct S3ClientCache::Impl {
    struct Metrics {
        // Atomic counters do not allocate per-thread bvar agents while a cache
        // or manager mutex is held (especially important during memory GC).
        struct Counter {
            std::atomic<int64_t> value {0};
            bvar::PassiveStatus<int64_t> metric {
                    [](void* p) {
                        return static_cast<Counter*>(p)->value.load(std::memory_order_relaxed);
                    },
                    this};
            void add(int64_t delta) { value.fetch_add(delta, std::memory_order_relaxed); }
            int64_t get_value() const { return value.load(std::memory_order_relaxed); }
        };
        Counter hits;
        Counter weak_hits;
        Counter misses;
        Counter waits;
        Counter created;
        Counter destroyed;
        Counter live;
        Counter failures;
        Counter evictions;
    };

    struct ClientLifetime {
        ClientLifetime(std::shared_ptr<MemTrackerLimiter> mem_tracker,
                       std::shared_ptr<Metrics> client_metrics)
                : tracker(std::move(mem_tracker)), metrics(std::move(client_metrics)) {
            metrics->created.add(1);
            metrics->live.add(1);
        }
        ~ClientLifetime() {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
            metrics->destroyed.add(1);
            metrics->live.add(-1);
        }
        std::shared_ptr<MemTrackerLimiter> tracker;
        std::shared_ptr<Metrics> metrics;
    };

    struct Retained {
        // Map-key addresses survive rehash. Only retained nodes dereference
        // this pointer; detached release batches never inspect it.
        const S3ClientConf* key;
        Client client;
        int64_t last_acquire;
    };
    using RetainedList = std::list<Retained, CustomStdAllocator<Retained>>;

    struct Entry {
        std::weak_ptr<ObjStorageClient> live;
        std::optional<RetainedList::iterator> retained;
    };
    struct Pending {
        std::promise<Result<Client>> promise;
        std::shared_future<Result<Client>> future = promise.get_future().share();
    };
    template <typename Value>
    using Map =
            std::unordered_map<S3ClientConf, Value, S3ClientConfHash, std::equal_to<S3ClientConf>,
                               CustomStdAllocator<std::pair<const S3ClientConf, Value>>>;

    struct ReleaseBatch {
        explicit ReleaseBatch(std::shared_ptr<MemTrackerLimiter> mem_tracker)
                : tracker(std::move(mem_tracker)) {}
        ~ReleaseBatch() {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
            clients.clear();
        }
        std::shared_ptr<MemTrackerLimiter> tracker;
        RetainedList clients;
    };

    Impl(size_t cache_capacity, uint32_t idle_ttl, Clock clock_source)
            : capacity(cache_capacity),
              ttl(idle_ttl),
              clock(clock_source ? std::move(clock_source)
                                 : Clock([] { return MonotonicSeconds(); })),
              tracker(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE,
                                                       "S3ClientCache")),
              metrics(std::make_shared<Metrics>()),
              entries_metric(std::make_unique<bvar::PassiveStatus<size_t>>(
                      [](void* p) { return static_cast<Impl*>(p)->snapshot().entries; }, this)),
              indexed_metric(std::make_unique<bvar::PassiveStatus<size_t>>(
                      [](void* p) { return static_cast<Impl*>(p)->snapshot().indexed; }, this)),
              inflight_metric(std::make_unique<bvar::PassiveStatus<size_t>>(
                      [](void* p) { return static_cast<Impl*>(p)->snapshot().inflight; }, this)) {
        DORIS_CHECK(ttl > 0);
    }

    ~Impl() {
        // Destroy the samplers, not just their exposure, before releasing the
        // maps they inspect. Hiding a bvar alone does not stop its series sampler.
        entries_metric.reset();
        indexed_metric.reset();
        inflight_metric.reset();
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
        // Free bucket arrays under the same tracker used for insertion, too.
        Map<Entry>().swap(entries);
        Map<std::shared_ptr<Pending>>().swap(pending);
    }

    Client track(Client client) {
        // Do not introduce an aliasing control block: list_objects() uses
        // shared_from_this(), and its iterator must keep the cache's weak entry
        // alive too. A client-owned guard observes actual client destruction
        // without owning the client or referring back to Impl.
        client->set_lifetime_guard(std::make_shared<ClientLifetime>(tracker, metrics));
        return client;
    }

    // mutex held. Expiring retention does not invalidate a live weak entry.
    void evict(RetainedList::iterator it, ReleaseBatch& release) {
        auto entry = entries.find(*it->key);
        DCHECK(entry != entries.end());
        entry->second.retained.reset();
        release.clients.splice(release.clients.end(), retained, it);
        metrics->evictions.add(1);
    }

    void sweep(int64_t now, bool all, ReleaseBatch& release, bool clean_index = true) {
        while (!retained.empty()) {
            auto oldest = std::prev(retained.end());
            // Entries are ordered by factory acquisition time, not IO activity.
            // A long scan can outlive TTL and continues through its own owner.
            if (!all && retained.size() <= capacity && now - oldest->last_acquire < ttl) {
                break;
            }
            evict(oldest, release);
        }
        if (!clean_index) {
            return; // Acquisitions must not scan all live configurations.
        }
        for (auto it = entries.begin(); it != entries.end();) {
            // Once the last caller and retention release their owners, the key
            // and weak control block must also be reclaimed.
            if (it->second.live.expired()) {
                DCHECK(!it->second.retained.has_value());
                it = entries.erase(it);
            } else {
                ++it;
            }
        }
        // Amortize weak-index scans over growth. This also reclaims dead keys
        // when periodic memory GC is disabled, without scanning all active
        // configurations on every acquisition.
        next_index_sweep = entries.size() * 2 + 64;
    }

    void retain(typename Map<Entry>::iterator entry, const Client& client, int64_t now,
                ReleaseBatch& release) {
        if (capacity == 0) {
            return; // Explicitly disable warm retention, while preserving weak reuse.
        }
        if (entry->second.retained.has_value()) {
            auto it = *entry->second.retained;
            it->last_acquire = now;
            retained.splice(retained.begin(), retained, it);
        } else {
            retained.push_front({&entry->first, client, now});
            entry->second.retained = retained.begin();
        }
        while (retained.size() > capacity) {
            evict(std::prev(retained.end()), release);
        }
    }

    Result<Client> acquire(const S3ClientConf& conf, const Creator& creator) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
        ReleaseBatch release(tracker); // Destruction is outside the mutex on every exit.
        // A successful weak lock can become the last owner while retain()
        // allocates a list node. Keep it outside the lock's scope so allocation
        // failure cannot run its SDK destructor under the cache mutex.
        Client cached;
        std::shared_ptr<Pending> task;
        bool leader = false;
        {
            std::lock_guard lock(mutex);
            if (closed) {
                return ResultError(Status::InternalError("S3 client cache is closed"));
            }
            const auto now = clock();
            // Scan the full weak index only after sufficient growth; otherwise
            // only inspect the bounded retention LRU and the requested key.
            sweep(now, false, release, entries.size() >= next_index_sweep);
            auto it = entries.find(conf);
            if (it != entries.end()) {
                cached = it->second.live.lock();
                if (cached) {
                    metrics->hits.add(1);
                    if (!it->second.retained.has_value()) {
                        metrics->weak_hits.add(1);
                    }
                    retain(it, cached, now, release);
                    return cached;
                }
                // The final external owner can disappear after sweep().
                entries.erase(it);
            }
            metrics->misses.add(1);
            auto creating = pending.find(conf);
            if (creating != pending.end()) {
                metrics->waits.add(1);
                task = creating->second;
            } else {
                task = std::make_shared<Pending>();
                pending.emplace(conf, task);
                leader = true;
            }
        }
        if (!leader) {
            return task->future.get(); // No cache/holder/manager lock is held while waiting.
        }

        try {
            auto result = creator(conf);
            if (result.has_value()) {
                DORIS_CHECK(result.value() != nullptr);
                result = track(std::move(result).value());
            } else {
                metrics->failures.add(1);
            }
            {
                std::lock_guard lock(mutex);
                // close() waits for this publication. The caller still receives
                // its result, but no new retention is introduced during close.
                if (result.has_value() && !closed) {
                    auto [entry, inserted] = entries.try_emplace(conf);
                    DORIS_CHECK(inserted);
                    try {
                        retain(entry, result.value(), clock(), release);
                    } catch (...) {
                        // Allocation of the retention node can fail. Do not
                        // expose a half-published client while unwinding to the
                        // common failure notification below.
                        entries.erase(entry);
                        throw;
                    }
                    entry->second.live = result.value();
                }
                task->promise.set_value(result);
                pending.erase(conf);
                creators_done.notify_all();
            }
            return result;
        } catch (...) {
            // A throwing creator/allocation must wake all followers with the
            // same exception and remove the task so a later attempt can retry.
            metrics->failures.add(1);
            {
                std::lock_guard lock(mutex);
                task->promise.set_exception(std::current_exception());
                pending.erase(conf);
                creators_done.notify_all();
            }
            throw;
        }
    }

    std::function<void()> prune(bool all, std::optional<size_t> new_capacity = {}) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
        auto release = std::make_shared<ReleaseBatch>(tracker);
        // Allocate the type-erased action before detaching anything. Allocation
        // failure during GC must not destroy SDK clients under the manager lock.
        std::function<void()> action = [release]() mutable { release.reset(); };
        {
            std::lock_guard lock(mutex);
            if (new_capacity.has_value()) {
                capacity = *new_capacity;
            }
            sweep(clock(), all, *release);
        }
        // No Impl/policy capture: unregistering/destroying the policy is safe
        // before CacheManager executes this detached release action.
        return action;
    }

    void close() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
        ReleaseBatch release(tracker);
        {
            std::unique_lock lock(mutex);
            closed = true;
            creators_done.wait(lock, [&] { return pending.empty(); });
            release.clients.splice(release.clients.end(), retained);
            entries.clear();
        }
    }

    Stats snapshot() const {
        std::lock_guard lock(mutex);
        return {.capacity = capacity,
                .entries = retained.size(),
                .indexed = entries.size(),
                .inflight = pending.size(),
                .closed = closed,
                .hits = metrics->hits.get_value(),
                .weak_hits = metrics->weak_hits.get_value(),
                .misses = metrics->misses.get_value(),
                .waits = metrics->waits.get_value(),
                .created = metrics->created.get_value(),
                .destroyed = metrics->destroyed.get_value(),
                .live = metrics->live.get_value(),
                .failures = metrics->failures.get_value(),
                .evictions = metrics->evictions.get_value()};
    }

    mutable std::mutex mutex;
    std::condition_variable creators_done;
    size_t capacity;
    size_t next_index_sweep = 64;
    const uint32_t ttl;
    const Clock clock;
    const std::shared_ptr<MemTrackerLimiter> tracker;
    const std::shared_ptr<Metrics> metrics;
    Map<Entry> entries;
    Map<std::shared_ptr<Pending>> pending;
    RetainedList retained;
    bool closed = false;
    std::unique_ptr<bvar::PassiveStatus<size_t>> entries_metric;
    std::unique_ptr<bvar::PassiveStatus<size_t>> indexed_metric;
    std::unique_ptr<bvar::PassiveStatus<size_t>> inflight_metric;
};

class S3ClientCachePolicy final : public CachePolicy {
public:
    explicit S3ClientCachePolicy(S3ClientCache& cache, uint32_t ttl)
            : CachePolicy(CacheType::S3_CLIENT_CACHE, cache.stats().capacity, ttl, true),
              _cache(cache) {
        CacheManager::instance()->register_cache(this);
    }

    void prune_stale() override {
        COUNTER_UPDATE(_prune_stale_number_counter, 1);
        prune(false);
    }
    void prune_all(bool /*force*/) override {
        COUNTER_UPDATE(_prune_all_number_counter, 1);
        prune(true);
    }
    int64_t adjust_capacity_weighted(double weight) override {
        return resize(static_cast<size_t>(static_cast<double>(_initial_capacity) * weight));
    }
    int64_t reset_initial_capacity(double weight) override {
        _initial_capacity = static_cast<size_t>(static_cast<double>(_initial_capacity) * weight);
        return resize(_initial_capacity);
    }
    size_t get_capacity() override { return _cache.stats().capacity; }
    std::function<void()> take_deferred_release() override { return std::exchange(_release, {}); }

private:
    void prune(bool all) {
        SCOPED_TIMER(_cost_timer);
        auto before = _cache.stats().entries;
        _release = _cache._impl->prune(all);
        update_profile(before);
    }
    int64_t resize(size_t capacity) {
        COUNTER_UPDATE(_adjust_capacity_weighted_number_counter, 1);
        auto before = _cache.stats().entries;
        _release = _cache._impl->prune(false, capacity);
        update_profile(before);
        return 0; // Released SDK/TLS bytes cannot be inferred from entry count.
    }
    void update_profile(size_t before) {
        auto after = _cache.stats().entries;
        _freed_memory_counter->set(0);
        _freed_entrys_counter->set(static_cast<int64_t>(before - std::min(before, after)));
    }

    S3ClientCache& _cache;
    std::function<void()> _release;
};

S3ClientCache::S3ClientCache(size_t capacity, uint32_t ttl, Clock clock)
        : _impl(std::make_unique<Impl>(capacity, ttl, std::move(clock))) {}

S3ClientCache::~S3ClientCache() {
    close();
}

Result<S3ClientCache::Client> S3ClientCache::get_or_create(const S3ClientConf& conf,
                                                           const Creator& creator) {
    return _impl->acquire(conf, creator);
}

void S3ClientCache::prune_stale() {
    _impl->prune(false)();
}

void S3ClientCache::prune_all() {
    _impl->prune(true)();
}

void S3ClientCache::set_capacity(size_t capacity) {
    _impl->prune(false, capacity)();
}

S3ClientCache::Stats S3ClientCache::stats() const {
    return _impl->snapshot();
}

void S3ClientCache::close() {
    _impl->close();
}

std::unique_ptr<CachePolicy> S3ClientCache::create_cache_policy() {
    return std::make_unique<S3ClientCachePolicy>(*this, _impl->ttl);
}

void S3ClientCache::expose_metrics() {
    auto& m = *_impl->metrics;
    for (const auto& [name, value] : {std::pair {"hit", &m.hits},
                                      {"weak_hit", &m.weak_hits},
                                      {"miss", &m.misses},
                                      {"wait", &m.waits},
                                      {"created", &m.created},
                                      {"destroyed", &m.destroyed},
                                      {"live", &m.live},
                                      {"failure", &m.failures},
                                      {"eviction", &m.evictions}}) {
        DORIS_CHECK(value->metric.expose_as("s3_client_cache", name) == 0);
    }
    DORIS_CHECK(_impl->entries_metric->expose_as("s3_client_cache", "entries") == 0);
    DORIS_CHECK(_impl->indexed_metric->expose_as("s3_client_cache", "indexed") == 0);
    DORIS_CHECK(_impl->inflight_metric->expose_as("s3_client_cache", "inflight") == 0);
}

} // namespace doris
