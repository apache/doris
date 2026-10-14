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

#include <memory>
#include <mutex>
#include <unordered_map>

#include "io/obj_storage/obj_storage_config.h"

namespace doris::io {

// Client cache owned by one client provider. Keys are full client configurations, so
// two configurations that only collide on hash() still get different clients.
//
// SDK clients are built outside the lock. When two threads build the same identity
// concurrently, the first publish wins and the loser's candidate stays owned by its
// caller, so the losing SDK client is destroyed after the lock is released.
class ObjStorageClientCache {
public:
    std::shared_ptr<ObjStorageClient> find(const ObjStorageClientConfig& config) {
        std::lock_guard lock(_lock);
        auto it = _clients.find(config);
        return it == _clients.end() ? nullptr : it->second;
    }

    // Returns the client cached for config: candidate if it was published, or the one
    // another thread published first.
    std::shared_ptr<ObjStorageClient> publish(const ObjStorageClientConfig& config,
                                              const std::shared_ptr<ObjStorageClient>& candidate) {
        std::lock_guard lock(_lock);
        auto [it, _] = _clients.try_emplace(config, candidate);
        return it->second;
    }

private:
    std::mutex _lock;
    std::unordered_map<ObjStorageClientConfig, std::shared_ptr<ObjStorageClient>,
                       ObjStorageClientConfigHash>
            _clients;
};

} // namespace doris::io
