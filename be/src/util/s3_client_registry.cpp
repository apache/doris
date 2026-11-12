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

#include "util/s3_client_registry.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <future>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "core/custom_allocator.h"
#include "cpp/sync_point.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/s3_util.h"

namespace doris {

class S3ClientRegistry::ClientCache final : public LRUCachePolicy {
public:
    ClientCache(size_t capacity, uint32_t idle_timeout_seconds)
            : LRUCachePolicy(CacheType::S3_CLIENT_CACHE, capacity, LRUCacheType::NUMBER,
                             idle_timeout_seconds, /*num_shards=*/1,
                             /*element_count_capacity=*/0, /*enable_prune=*/true,
                             /*is_lru_k=*/false),
              _cached_metric(
                      [](void* p) { return static_cast<ClientCache*>(p)->get_element_count(); },
                      this) {
        DORIS_CHECK(idle_timeout_seconds > 0);
    }

    void expose_metrics() { DORIS_CHECK(_cached_metric.expose_as("s3_client", "cached") == 0); }

    ~ClientCache() override {
        // Unregister while the derived object is still intact: a concurrent
        // memory-GC sweep must finish before its cache and sampler disappear.
        CacheManager::instance()->unregister_cache(type());
        close();
    }

    void retain(const Client& client) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker());
        {
            std::lock_guard lock(_lifecycle_mutex);
            // A completed acquisition can reach here after close() stopped the
            // registry. Return its real owner without refilling the cache.
            if (_closed) {
                return;
            }
            ++_retainers;
        }
        Defer finished([this] {
            std::lock_guard lock(_lifecycle_mutex);
            --_retainers;
            _retainers_done.notify_all();
        });
        // LRU callbacks can destroy an SDK client and re-enter acquisition.
        // Neither the registry nor the lifecycle mutex covers these operations.
        // The full-configuration weak index already selected this client. Its
        // address is unique while any real/cache owner exists; unlike a config
        // hash, it cannot merge distinct identities on a collision.
        const auto* identity = client.get();
        CacheKey key(reinterpret_cast<const char*>(&identity), sizeof(identity));
        auto* handle = lookup(key);
        const auto now = now_millis();
        if (handle) {
            static_cast<Value*>(value(handle))
                    ->last_access_ms.store(now, std::memory_order_relaxed);
        } else {
            handle = insert(key, new Value(client, now), 1, sizeof(Value));
        }
        release(handle);
    }

    void prune_stale() override {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker());
        std::lock_guard lock(_lock);
        COUNTER_SET(_freed_memory_counter, int64_t {0});
        COUNTER_SET(_freed_entrys_counter, int64_t {0});
        COUNTER_SET(_cost_timer, int64_t {0});
        const auto now = now_millis();
        const auto timeout_ms = int64_t(_stale_sweep_time_s) * 1000;
        SCOPED_TIMER(_cost_timer);
        auto info = _cache->prune_if(
                [&](const LRUHandle* handle) {
                    return now - static_cast<const Value*>(handle->value)
                                           ->last_access_ms.load(std::memory_order_relaxed) >=
                           timeout_ms;
                },
                false);
        COUNTER_SET(_freed_entrys_counter, info.pruned_count);
        // SDK-owned allocations are not measured by this metadata accounting.
        COUNTER_SET(_freed_memory_counter,
                    static_cast<int64_t>(info.pruned_count *
                                         (sizeof(LRUHandle) - 1 + sizeof(void*) + sizeof(Value))));
        COUNTER_UPDATE(_prune_stale_number_counter, 1);
    }

    bool exceed_prune_limit() override {
        // Even one idle SDK client must expire; the generic 1024-object GC
        // threshold would otherwise retain a small cache indefinitely.
        return true;
    }

    int64_t adjust_capacity_weighted(double weight) override {
        return LRUCachePolicy::adjust_capacity_weighted(std::min(weight, 1.0));
    }

    int64_t reset_initial_capacity(double weight) override {
        // Raising mem_limit scales byte-cache budgets, but must not increase
        // the configured SDK object-count ceiling (e.g. 64 must not become 128).
        return adjust_capacity_weighted(weight);
    }

    void close() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker());
        {
            std::unique_lock lock(_lifecycle_mutex);
            _closed = true;
            _retainers_done.wait(lock, [this] { return _retainers == 0; });
        }
        prune_all(true);
    }

private:
    struct Value final : LRUCacheValueBase {
        Value(Client owner, int64_t now) : client(std::move(owner)), last_access_ms(now) {}
        Client client;
        std::atomic<int64_t> last_access_ms;
    };

    static int64_t now_millis() {
        int64_t now = MonotonicMillis();
        TEST_SYNC_POINT_CALLBACK("S3ClientRegistry::cache_clock", &now);
        return now;
    }

    bool _closed = false;
    size_t _retainers = 0;
    std::mutex _lifecycle_mutex;
    std::condition_variable _retainers_done;
    bvar::PassiveStatus<size_t> _cached_metric;
};

struct S3ClientRegistry::Impl : std::enable_shared_from_this<Impl> {
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

    struct ClientLifetime {
        ClientLifetime(std::shared_ptr<Impl> registry, S3ClientConf key, const Client& client)
                : state(std::move(registry)), conf(std::move(key)), identity(client) {
            state->created.add(1);
            state->live.add(1);
        }
        ~ClientLifetime() {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(state->tracker);
            // SDK members have already been destroyed. Remove just this client's
            // registration: a new acquisition may have replaced the expired weak
            // pointer while the old SDK destructor was still running.
            std::lock_guard lock(state->mutex);
            auto it = state->entries.find(conf);
            if (it != state->entries.end() && !it->second.owner_before(identity) &&
                !identity.owner_before(it->second)) {
                state->entries.erase(it);
            }
            state->destroyed.add(1);
            state->live.add(-1);
        }
        // State has only weak references back to clients, so this is not a cycle.
        // Keep tracking and index cleanup valid even if a user outlives the registry.
        std::shared_ptr<Impl> state;
        S3ClientConf conf;
        std::weak_ptr<ObjStorageClient> identity;
    };

    struct Pending {
        std::promise<Result<Client>> promise;
        std::shared_future<Result<Client>> future = promise.get_future().share();
    };
    template <typename Value>
    using Map =
            std::unordered_map<S3ClientConf, Value, S3ClientConfHash, std::equal_to<S3ClientConf>,
                               CustomStdAllocator<std::pair<const S3ClientConf, Value>>>;

    Impl()
            : tracker(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                       "S3ClientRegistry")),
              indexed_metric(std::make_unique<bvar::PassiveStatus<size_t>>(
                      [](void* p) { return static_cast<Impl*>(p)->snapshot().indexed; }, this)),
              inflight_metric(std::make_unique<bvar::PassiveStatus<size_t>>(
                      [](void* p) { return static_cast<Impl*>(p)->snapshot().inflight; }, this)) {}

    ~Impl() {
        // Stop bvar samplers before destroying the maps they inspect.
        indexed_metric.reset();
        inflight_metric.reset();
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
        Map<std::weak_ptr<ObjStorageClient>>().swap(entries);
        Map<std::shared_ptr<Pending>>().swap(pending);
    }

    Result<Client> acquire(const S3ClientConf& conf, const Creator& creator) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
        // A weak lock may become the last owner while other users finish IO.
        // Release both clients and completed futures outside the registry mutex.
        Client client;
        std::shared_ptr<Pending> task;
        bool leader = false;
        {
            std::lock_guard lock(mutex);
            if (closed) {
                return ResultError(Status::InternalError("S3 client registry is closed"));
            }
            auto it = entries.find(conf);
            if (it != entries.end()) {
                client = it->second.lock();
                if (client) {
                    hits.add(1);
                    return client;
                }
                // The old SDK destructor can still be running. Its lifetime guard
                // will not erase a newer client published for this configuration.
                entries.erase(it);
            }
            misses.add(1);
            auto creating = pending.find(conf);
            if (creating != pending.end()) {
                waits.add(1);
                task = creating->second;
            } else {
                task = std::make_shared<Pending>();
                pending.emplace(conf, task);
                leader = true;
            }
        }
        if (!leader) {
            return task->future.get();
        }
        return create_and_publish(conf, creator, task);
    }

    Result<Client> create_and_publish(const S3ClientConf& conf, const Creator& creator,
                                      const std::shared_ptr<Pending>& task) {
        try {
            auto result = creator(conf);
            if (result.has_value()) {
                DORIS_CHECK(result.value() != nullptr);
                result.value()->set_lifetime_guard(
                        std::make_shared<ClientLifetime>(shared_from_this(), conf, result.value()));
            } else {
                failures.add(1);
            }
            {
                std::lock_guard lock(mutex);
                // close() waits for publication; callers still receive their
                // client, but a closing registry no longer indexes new clients.
                if (result.has_value() && !closed) {
                    TEST_SYNC_POINT_CALLBACK("S3ClientRegistry::before_publication", nullptr);
                    auto [_, inserted] = entries.emplace(conf, result.value());
                    DORIS_CHECK(inserted);
                }
                task->promise.set_value(result);
                pending.erase(conf);
                creators_done.notify_all();
            }
            return result;
        } catch (...) {
            // Construction or publication allocation can fail. All followers
            // must observe that exception and a later acquisition must retry.
            failures.add(1);
            {
                std::lock_guard lock(mutex);
                task->promise.set_exception(std::current_exception());
                pending.erase(conf);
                creators_done.notify_all();
            }
            throw;
        }
    }

    void close() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
        std::unique_lock lock(mutex);
        closed = true;
        creators_done.wait(lock, [&] { return pending.empty(); });
        entries.clear();
    }

    Stats snapshot() const {
        std::lock_guard lock(mutex);
        return {.indexed = entries.size(),
                .inflight = pending.size(),
                .closed = closed,
                .hits = hits.get_value(),
                .misses = misses.get_value(),
                .waits = waits.get_value(),
                .created = created.get_value(),
                .destroyed = destroyed.get_value(),
                .live = live.get_value(),
                .failures = failures.get_value(),
                .cached = 0};
    }

    mutable std::mutex mutex;
    std::condition_variable creators_done;
    const std::shared_ptr<MemTrackerLimiter> tracker;
    Counter hits;
    Counter misses;
    Counter waits;
    Counter created;
    Counter destroyed;
    Counter live;
    Counter failures;
    Map<std::weak_ptr<ObjStorageClient>> entries;
    Map<std::shared_ptr<Pending>> pending;
    bool closed = false;
    // Only the process factory publishes the global bvar names. Isolated
    // registries used by tests must not replace its sampler registrations.
    bool metrics_exposed = false;
    std::unique_ptr<bvar::PassiveStatus<size_t>> indexed_metric;
    std::unique_ptr<bvar::PassiveStatus<size_t>> inflight_metric;
};

S3ClientRegistry::S3ClientRegistry() : _impl(std::make_shared<Impl>()) {}

S3ClientRegistry::~S3ClientRegistry() {
    close();
}

Result<S3ClientRegistry::Client> S3ClientRegistry::get_or_create(const S3ClientConf& conf,
                                                                 const Creator& creator) {
    auto result = _impl->acquire(conf, creator);
    // Invalid configuration/provider failures never enter the retention cache.
    if (result.has_value() && _cache) {
        // LRU eviction can destroy SDK clients and re-enter the weak index.
        // Do this after acquire() has released the registry mutex.
        _cache->retain(result.value());
    }
    return result;
}

S3ClientRegistry::Stats S3ClientRegistry::stats() const {
    auto snapshot = _impl->snapshot();
    snapshot.cached = _cache ? _cache->get_element_count() : 0;
    return snapshot;
}

void S3ClientRegistry::init_cache(size_t capacity, uint32_t idle_timeout_seconds) {
    DORIS_CHECK(_cache == nullptr);
    // Explicitly disabling retention leaves active-client sharing unchanged.
    if (capacity > 0) {
        _cache = std::make_unique<ClientCache>(capacity, idle_timeout_seconds);
        if (_impl->metrics_exposed) {
            _cache->expose_metrics();
        }
    }
}

void S3ClientRegistry::release_cache() {
    _cache.reset();
}

void S3ClientRegistry::close() {
    _impl->close();
    if (_cache) {
        _cache->close();
    }
}

void S3ClientRegistry::expose_metrics() {
    for (const auto& [name, counter] : {std::pair {"hit", &_impl->hits},
                                        {"miss", &_impl->misses},
                                        {"wait", &_impl->waits},
                                        {"created", &_impl->created},
                                        {"destroyed", &_impl->destroyed},
                                        {"live", &_impl->live},
                                        {"failure", &_impl->failures}}) {
        DORIS_CHECK(counter->metric.expose_as("s3_client", name) == 0);
    }
    DORIS_CHECK(_impl->indexed_metric->expose_as("s3_client", "indexed") == 0);
    DORIS_CHECK(_impl->inflight_metric->expose_as("s3_client", "inflight") == 0);
    _impl->metrics_exposed = true;
    if (_cache) {
        _cache->expose_metrics();
    }
}

struct S3ClientScope::Impl {
    using Client = std::shared_ptr<ObjStorageClient>;
    using Map = std::unordered_map<S3ClientConf, Client, S3ClientConfHash, std::equal_to<>,
                                   CustomStdAllocator<std::pair<const S3ClientConf, Client>>>;

    Impl() : tracker(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr()) {}
    ~Impl() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
        Map().swap(clients);
    }

    const std::shared_ptr<MemTrackerLimiter> tracker;
    std::mutex mutex;
    Map clients;
};

S3ClientScope::S3ClientScope() : _impl(std::make_unique<Impl>()) {}
S3ClientScope::~S3ClientScope() = default;

Result<std::shared_ptr<ObjStorageClient>> S3ClientScope::get_or_create(const S3ClientConf& conf) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_impl->tracker);
    {
        std::lock_guard lock(_impl->mutex);
        auto it = _impl->clients.find(conf);
        if (it != _impl->clients.end()) {
            return it->second;
        }
    }
    // SDK construction and losing-owner destruction run outside the scope lock.
    // The factory's single-flight registry coordinates overlapping acquisitions.
    auto result = S3ClientFactory::instance().create(conf);
    // Invalid configuration or provider construction failure remains retryable.
    if (!result.has_value()) {
        return ResultError(std::move(result).error());
    }
    auto client = std::move(result).value();
    std::shared_ptr<ObjStorageClient> winner;
    {
        std::lock_guard lock(_impl->mutex);
        auto [it, _] = _impl->clients.emplace(conf, client);
        winner = it->second;
    }
    return winner;
}

} // namespace doris
