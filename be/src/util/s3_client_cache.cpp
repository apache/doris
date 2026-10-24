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

#include <future>
#include <list>
#include <unordered_map>

#include "core/custom_allocator.h"
#include "cpp/sync_point.h"
#include "runtime/thread_context.h"

namespace doris {
namespace {
bvar::Adder<int64_t> s3_client_cache_entries("s3_client_cache_entries");
bvar::Adder<int64_t> s3_client_cache_hits("s3_client_cache_hits");
bvar::Adder<int64_t> s3_client_cache_misses("s3_client_cache_misses");
bvar::Adder<int64_t> s3_client_cache_waits("s3_client_cache_waits");
bvar::Adder<int64_t> s3_client_cache_creations("s3_client_cache_creations");
bvar::Adder<int64_t> s3_client_cache_failures("s3_client_cache_failures");
bvar::Adder<int64_t> s3_client_cache_evictions("s3_client_cache_evictions");
} // namespace

struct S3ClientCache::Impl {
    struct Entry {
        S3ClientConf conf;
        Client client;
        int64_t last_access;
    };
    using Entries = std::list<Entry, CustomStdAllocator<Entry>>;
    template <typename T>
    using Map = std::unordered_map<S3ClientConf, T, S3ClientConfHash, std::equal_to<S3ClientConf>,
                                   CustomStdAllocator<std::pair<const S3ClientConf, T>>>;

    Impl(size_t capacity, int64_t ttl, Clock clock)
            : capacity(capacity), ttl(ttl), clock(std::move(clock)) {}

    void retire(Entries::iterator entry, Entries& retired) {
        index.erase(entry->conf);
        retired.splice(retired.end(), lru, entry);
        s3_client_cache_entries << -1;
        s3_client_cache_evictions << 1;
    }

    void prune_stale(Entries& retired) {
        const auto now = clock();
        // The least recently used entry is also the oldest: a hit updates both order and time.
        while (!lru.empty() && now - lru.back().last_access >= ttl) {
            retire(std::prev(lru.end()), retired);
        }
    }

    // Cache nodes cross query and GC threads. Charge their allocation and release to the same
    // cache tracker, rather than charging insertion to one query and eviction to another.
    const std::shared_ptr<MemTrackerLimiter> mem_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE, "S3ClientCache");
    const size_t capacity;
    const int64_t ttl;
    const Clock clock;
    mutable std::mutex mutex;
    Entries lru;
    Map<Entries::iterator> index;
    Map<std::shared_future<Result<Client>>> pending;
};

S3ClientCache::S3ClientCache(size_t capacity, int64_t idle_ttl_seconds, Clock clock)
        : _impl(std::make_unique<Impl>(capacity, idle_ttl_seconds, std::move(clock))) {}

S3ClientCache::~S3ClientCache() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_impl->mem_tracker);
    clear();
    // The hash-table bucket arrays are retained by clear(); release them under the cache tracker.
    _impl.reset();
}

Result<S3ClientCache::Client> S3ClientCache::get_or_create(const S3ClientConf& conf,
                                                           const Creator& creator) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_impl->mem_tracker);
    // Retired SDK clients can close connections or join threads. Destroy them outside mutex.
    Impl::Entries retired;
    std::shared_future<Result<Client>> future;
    std::shared_ptr<std::promise<Result<Client>>> promise;
    {
        std::lock_guard lock(_impl->mutex);
        _impl->prune_stale(retired);
        auto hit = _impl->index.find(conf);
        if (hit != _impl->index.end()) {
            hit->second->last_access = _impl->clock();
            _impl->lru.splice(_impl->lru.begin(), _impl->lru, hit->second);
            s3_client_cache_hits << 1;
            return hit->second->client;
        }
        auto pending = _impl->pending.find(conf);
        if (pending != _impl->pending.end()) {
            future = pending->second;
            s3_client_cache_waits << 1;
        } else {
            promise = std::make_shared<std::promise<Result<Client>>>();
            future = promise->get_future().share();
            _impl->pending.emplace(conf, future);
            s3_client_cache_misses << 1;
        }
    }
    retired.clear();
    // Only the creator owns the promise. Waiting does not hold the cache lock, so a slow
    // endpoint cannot prevent unrelated configurations from creating or reusing clients.
    if (!promise) {
        TEST_SYNC_POINT("s3_client_cache::wait");
        return future.get();
    }

    try {
        s3_client_cache_creations << 1;
        auto result = creator();
        {
            std::lock_guard lock(_impl->mutex);
            if (result.has_value() && _impl->capacity > 0) {
                _impl->lru.push_front({conf, result.value(), _impl->clock()});
                try {
                    _impl->index.emplace(conf, _impl->lru.begin());
                } catch (...) {
                    _impl->lru.pop_front();
                    throw;
                }
                s3_client_cache_entries << 1;
                while (_impl->lru.size() > _impl->capacity) {
                    _impl->retire(std::prev(_impl->lru.end()), retired);
                }
            }
            // Publish before removing the flight, including failures. A new caller can retry
            // after failure; existing waiters receive the result of their own creation attempt.
            promise->set_value(result);
            _impl->pending.erase(conf);
        }
        if (!result.has_value()) {
            s3_client_cache_failures << 1;
        }
        return result;
    } catch (...) {
        // SDK construction can throw. Without completion, waiters would block indefinitely.
        std::lock_guard lock(_impl->mutex);
        promise->set_exception(std::current_exception());
        _impl->pending.erase(conf);
        s3_client_cache_failures << 1;
        throw;
    }
}

size_t S3ClientCache::prune_stale() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_impl->mem_tracker);
    Impl::Entries retired;
    {
        std::lock_guard lock(_impl->mutex);
        _impl->prune_stale(retired);
    }
    return retired.size();
}

size_t S3ClientCache::clear() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_impl->mem_tracker);
    Impl::Entries retired;
    {
        std::lock_guard lock(_impl->mutex);
        while (!_impl->lru.empty()) {
            _impl->retire(std::prev(_impl->lru.end()), retired);
        }
    }
    return retired.size();
}

size_t S3ClientCache::size() const {
    std::lock_guard lock(_impl->mutex);
    return _impl->lru.size();
}

} // namespace doris
