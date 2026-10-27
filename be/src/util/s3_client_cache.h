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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "common/status.h"

namespace doris {

class CachePolicy;
class ObjStorageClient;
struct S3ClientConf;

// Retention and ownership are independent: evicting a strong cache reference never
// closes a client still owned by a holder/request. A weak index keeps that client
// discoverable until its last strong owner releases it.
class S3ClientCache {
public:
    using Client = std::shared_ptr<ObjStorageClient>;
    using Creator = std::function<Result<Client>(const S3ClientConf&)>;
    using Clock = std::function<int64_t()>;

    struct Stats {
        size_t capacity;
        size_t entries;
        size_t indexed;
        size_t inflight;
        bool closed;
        int64_t hits;
        int64_t weak_hits;
        int64_t misses;
        int64_t waits;
        int64_t created;
        int64_t destroyed;
        int64_t live;
        int64_t failures;
        int64_t evictions;
    };

    // Clock returns monotonic seconds; injectable so TTL tests need no sleeps.
    S3ClientCache(size_t capacity, uint32_t idle_ttl_seconds, Clock clock = {});
    ~S3ClientCache();

    // creator returns a freshly constructed client whose lifetime guard has not
    // been attached yet. All returned owners, including shared_from_this(),
    // share that client's original control block.
    Result<Client> get_or_create(const S3ClientConf& conf, const Creator& creator);
    void prune_stale();
    void prune_all();
    void set_capacity(size_t capacity);
    Stats stats() const;

    // Called after request producers have stopped. Reject new acquisitions, wait
    // for creators, and release cache ownership; external owners remain valid.
    void close();

    // The policy must be destroyed before CacheManager and before this cache.
    std::unique_ptr<CachePolicy> create_cache_policy();
    void expose_metrics();

private:
    friend class S3ClientCachePolicy;
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace doris
