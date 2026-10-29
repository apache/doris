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

#include <atomic>
#include <condition_variable>
#include <exception>
#include <future>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "core/custom_allocator.h"
#include "cpp/sync_point.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/s3_util.h"

namespace doris {

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
                .failures = failures.get_value()};
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
    std::unique_ptr<bvar::PassiveStatus<size_t>> indexed_metric;
    std::unique_ptr<bvar::PassiveStatus<size_t>> inflight_metric;
};

S3ClientRegistry::S3ClientRegistry() : _impl(std::make_shared<Impl>()) {}

S3ClientRegistry::~S3ClientRegistry() {
    close();
}

Result<S3ClientRegistry::Client> S3ClientRegistry::get_or_create(const S3ClientConf& conf,
                                                                 const Creator& creator) {
    return _impl->acquire(conf, creator);
}

S3ClientRegistry::Stats S3ClientRegistry::stats() const {
    return _impl->snapshot();
}

void S3ClientRegistry::close() {
    _impl->close();
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
}

} // namespace doris
