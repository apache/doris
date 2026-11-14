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
#include <shared_mutex>

#include "common/status.h"
#include "cpp/obj-storage/obj_storage_config.h"

namespace doris {
class ObjStorageRateLimitPolicy;
}

namespace doris::io {

// Shares the active backend client across a file system and its open files.
class ObjStorageClientHolder {
public:
    explicit ObjStorageClientHolder(
            ObjStorageClientConfig conf,
            std::shared_ptr<const ObjStorageRateLimitPolicy> policy = nullptr);
    ~ObjStorageClientHolder();

    Status init();

    // Readers keep using the current client while a credential update is being built.
    Status reset(const ObjStorageClientConfig& conf);

    std::shared_ptr<ObjStorageClient> get() const {
        std::shared_lock lock(_mtx);
        return _client;
    }

    ObjStorageClientConfig config() const {
        std::shared_lock lock(_mtx);
        return _conf;
    }

private:
    Result<std::shared_ptr<ObjStorageClient>> create_client(
            const ObjStorageClientConfig& conf) const;

    std::shared_ptr<const ObjStorageRateLimitPolicy> _rate_limit_policy;
    std::mutex _update_mutex;
    mutable std::shared_mutex _mtx;
    std::shared_ptr<ObjStorageClient> _client;
    ObjStorageClientConfig _conf;
};

} // namespace doris::io
