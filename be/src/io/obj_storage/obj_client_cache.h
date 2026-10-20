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
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "io/obj_storage/obj_storage_config.h"

namespace doris::io {

// Client cache owned by one client provider. Keys are full client configurations, so
// two configurations that only collide on hash() still get different clients.
//
// SDK clients are built outside the lock. When two threads build the same identity
// concurrently, the first publish wins and the loser's candidate stays owned by its
// caller, so the losing SDK client is destroyed after the lock is released.
//
// A bounded publish keeps at most `capacity` entries. Short-lived credentials make this
// necessary: each new SAS token is a new identity, e.g. an Iceberg REST catalog that vends a
// fresh SAS per query would otherwise add one Azure client (with its own connection pool) per
// query and never release the old ones. Entries whose credential has expired go first, then
// the least recently used. Eviction only drops the cache's reference: a reader that holds the
// client keeps using it, and the last reference is released outside the lock.
class ObjClientCache {
public:
    // Returns the cached client and marks it as recently used, or nullptr.
    std::shared_ptr<ObjStorageClient> find(const ObjClientConfig& config);

    // Unbounded publish, for clients whose credentials do not expire on their own (e.g. S3
    // static keys). Returns the client cached for config: candidate if it was published, or
    // the one another thread published first.
    std::shared_ptr<ObjStorageClient> publish(const ObjClientConfig& config,
                                              const std::shared_ptr<ObjStorageClient>& candidate);

    // Bounded publish. expires_at_ms is when the candidate's credential expires, if known;
    // capacity must be positive.
    std::shared_ptr<ObjStorageClient> publish(const ObjClientConfig& config,
                                              const std::shared_ptr<ObjStorageClient>& candidate,
                                              std::optional<int64_t> expires_at_ms, int64_t now_ms,
                                              size_t capacity);

    // Applies a new capacity at once: evicts expired entries, then the least recently used
    // ones, until at most `capacity` remain. capacity must be positive.
    void shrink(int64_t now_ms, size_t capacity);

    size_t size();

private:
    struct Entry {
        ObjClientConfig config;
        std::shared_ptr<ObjStorageClient> client;
        std::optional<int64_t> expires_at_ms;
    };
    using EntryList = std::list<Entry>;

    // Inserts or finds config under _lock; returns the cached client.
    std::shared_ptr<ObjStorageClient> _publish_locked(
            const ObjClientConfig& config, const std::shared_ptr<ObjStorageClient>& candidate,
            std::optional<int64_t> expires_at_ms);

    // Moves the clients to drop into `evicted`, which the caller destroys after releasing
    // _lock: expired entries first, then the least recently used beyond `capacity`.
    // keep_front exempts the entry a publish just placed at the front, which the caller has
    // validated, from the expiry sweep.
    void _evict_locked(int64_t now_ms, size_t capacity, bool keep_front,
                       std::vector<std::shared_ptr<ObjStorageClient>>* evicted);

    std::mutex _lock;
    // Most recently used first.
    EntryList _entries;
    std::unordered_map<ObjClientConfig, EntryList::iterator, ObjClientConfigHash> _index;
};

} // namespace doris::io
