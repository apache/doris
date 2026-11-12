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

class ObjStorageClient;
struct S3ClientConf;

// Index real users weakly and optionally retain a bounded LRU of clients across
// queries. Eviction releases only the cache's owner, never an active user's.
class S3ClientRegistry {
public:
    using Client = std::shared_ptr<ObjStorageClient>;
    using Creator = std::function<Result<Client>(const S3ClientConf&)>;

    struct Stats {
        size_t indexed;
        size_t inflight;
        bool closed;
        int64_t hits;
        int64_t misses;
        int64_t waits;
        int64_t created;
        int64_t destroyed;
        int64_t live;
        int64_t failures;
        size_t cached;
    };

    S3ClientRegistry();
    ~S3ClientRegistry();

    S3ClientRegistry(const S3ClientRegistry&) = delete;
    S3ClientRegistry& operator=(const S3ClientRegistry&) = delete;

    // creator returns a new client. Preserve its original control block so
    // shared_from_this(), iterators and asynchronous users share its lifetime.
    Result<Client> get_or_create(const S3ClientConf& conf, const Creator& creator);
    Stats stats() const;

    // Initialize before request producers start, after CacheManager exists.
    // A zero capacity keeps only the weak index (e.g. offline tools).
    void init_cache(size_t capacity, uint32_t idle_timeout_seconds);
    // Release after request producers stop, before CacheManager is destroyed.
    void release_cache();

    // Stop acquisitions and wait for creators after request producers stop.
    // Existing users continue to own valid clients until they release them.
    void close();
    void expose_metrics();

private:
    class ClientCache;
    struct Impl;
    std::shared_ptr<Impl> _impl;
    std::unique_ptr<ClientCache> _cache;
};

// Strong owners bounded by an existing scan or expression-block lifetime. For
// example, 64 sequential files with the same credentials need one SDK client,
// even if global retention is disabled or memory GC evicts its cache entry.
class S3ClientScope {
public:
    S3ClientScope();
    ~S3ClientScope();
    S3ClientScope(const S3ClientScope&) = delete;
    S3ClientScope& operator=(const S3ClientScope&) = delete;

    Result<std::shared_ptr<ObjStorageClient>> get_or_create(const S3ClientConf& conf);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace doris
