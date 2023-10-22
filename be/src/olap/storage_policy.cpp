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

#include "olap/storage_policy.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <unordered_map>

#include "util/lock.h"

namespace doris {

struct StoragePolicyMgr {
    std::mutex mtx;
    std::unordered_map<int64_t, StoragePolicyPtr> map;
};

static StoragePolicyMgr s_storage_policy_mgr;

Status get_remote_file_system(int64_t storage_policy_id,
                              std::shared_ptr<io::RemoteFileSystem>* fs) {
    auto storage_policy = get_storage_policy(storage_policy_id);
    if (storage_policy == nullptr) {
        return Status::InternalError("could not find storage_policy, storage_policy_id={}",
                                     storage_policy_id);
    }
    auto resource = get_storage_resource(storage_policy->resource_id);
    *fs = std::static_pointer_cast<io::RemoteFileSystem>(resource.fs);
    if (*fs == nullptr) {
        return Status::InternalError("could not find resource, resouce_id={}",
                                     storage_policy->resource_id);
    }
    DCHECK((*fs)->type() != io::FileSystemType::LOCAL);
    return Status::OK();
}

StoragePolicyPtr get_storage_policy(int64_t id) {
    std::lock_guard lock(s_storage_policy_mgr.mtx);
    if (auto it = s_storage_policy_mgr.map.find(id); it != s_storage_policy_mgr.map.end()) {
        return it->second;
    }
    return nullptr;
}

void put_storage_policy(int64_t id, StoragePolicyPtr policy) {
    std::lock_guard lock(s_storage_policy_mgr.mtx);
    s_storage_policy_mgr.map[id] = std::move(policy);
}

void delete_storage_policy(int64_t id) {
    std::lock_guard lock(s_storage_policy_mgr.mtx);
    s_storage_policy_mgr.map.erase(id);
}

std::vector<std::pair<int64_t, int64_t>> get_storage_policy_ids() {
    std::vector<std::pair<int64_t, int64_t>> res;
    res.reserve(s_storage_policy_mgr.map.size());
    std::lock_guard lock(s_storage_policy_mgr.mtx);
    for (auto& [id, policy] : s_storage_policy_mgr.map) {
        res.emplace_back(id, policy->version);
    }
    return res;
}

struct StorageResouceMgr {
    doris::Mutex mtx;
    std::unordered_map<int64_t, StorageResource> map;
};

static StorageResouceMgr s_storage_resource_mgr;

io::FileSystemSPtr get_filesystem(const std::string& resource_id) {
    int64_t id = std::atol(resource_id.c_str());
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    if (auto it = s_storage_resource_mgr.map.find(id); it != s_storage_resource_mgr.map.end()) {
        return it->second.fs;
    }
    return nullptr;
}

StorageResource get_storage_resource(int64_t resource_id) {
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    if (auto it = s_storage_resource_mgr.map.find(resource_id);
        it != s_storage_resource_mgr.map.end()) {
        return it->second;
    }
    return StorageResource {nullptr, -1};
}

void put_storage_resource(int64_t resource_id, StorageResource resource) {
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    s_storage_resource_mgr.map[resource_id] = std::move(resource);
}

void delete_storage_resource(int64_t resource_id) {
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    s_storage_resource_mgr.map.erase(resource_id);
}

std::vector<std::pair<int64_t, int64_t>> get_storage_resource_ids() {
    std::vector<std::pair<int64_t, int64_t>> res;
    res.reserve(s_storage_resource_mgr.map.size());
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    for (auto& [id, resource] : s_storage_resource_mgr.map) {
        res.emplace_back(id, resource.version);
    }
    return res;
}

} // namespace doris
