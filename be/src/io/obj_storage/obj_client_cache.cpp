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

#include "io/obj_storage/obj_client_cache.h"

#include <iterator>
#include <utility>
#include <vector>

#include "common/check.h"

namespace doris::io {

std::shared_ptr<ObjStorageClient> ObjClientCache::find(const ObjClientConfig& config) {
    std::lock_guard lock(_lock);
    auto it = _index.find(config);
    if (it == _index.end()) {
        return nullptr;
    }
    _entries.splice(_entries.begin(), _entries, it->second);
    return it->second->client;
}

std::shared_ptr<ObjStorageClient> ObjClientCache::publish(
        const ObjClientConfig& config, const std::shared_ptr<ObjStorageClient>& candidate) {
    std::lock_guard lock(_lock);
    return _publish_locked(config, candidate, std::nullopt);
}

std::shared_ptr<ObjStorageClient> ObjClientCache::publish(
        const ObjClientConfig& config, const std::shared_ptr<ObjStorageClient>& candidate,
        std::optional<int64_t> expires_at_ms, int64_t now_ms, size_t capacity) {
    DORIS_CHECK(capacity > 0);
    // Declared before the lock so that evicted clients, possibly holding the last reference
    // to an SDK client, are destroyed after it is released.
    std::vector<std::shared_ptr<ObjStorageClient>> evicted;
    std::lock_guard lock(_lock);
    auto client = _publish_locked(config, candidate, expires_at_ms);
    // Expired entries can never be returned again (every lookup validates the credential
    // first), so they go before any live entry. The entry just published is at the front and
    // was validated by the caller.
    for (auto it = std::next(_entries.begin()); it != _entries.end();) {
        if (it->expires_at_ms.has_value() && *it->expires_at_ms <= now_ms) {
            evicted.push_back(std::move(it->client));
            _index.erase(it->config);
            it = _entries.erase(it);
        } else {
            ++it;
        }
    }
    while (_entries.size() > capacity) {
        auto& oldest = _entries.back();
        evicted.push_back(std::move(oldest.client));
        _index.erase(oldest.config);
        _entries.pop_back();
    }
    return client;
}

size_t ObjClientCache::size() {
    std::lock_guard lock(_lock);
    return _entries.size();
}

std::shared_ptr<ObjStorageClient> ObjClientCache::_publish_locked(
        const ObjClientConfig& config, const std::shared_ptr<ObjStorageClient>& candidate,
        std::optional<int64_t> expires_at_ms) {
    if (auto it = _index.find(config); it != _index.end()) {
        _entries.splice(_entries.begin(), _entries, it->second);
        return it->second->client;
    }
    _entries.push_front(Entry {
            .config = config,
            .client = candidate,
            .expires_at_ms = expires_at_ms,
    });
    _index.emplace(config, _entries.begin());
    return candidate;
}

} // namespace doris::io
