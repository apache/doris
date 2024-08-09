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

#include "gen_cpp/cloud.pb.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_meta.h"
#include "util/hash_util.hpp"

namespace doris {

struct StoragePolicyMgr {
    std::mutex mtx;
    std::unordered_map<int64_t, StoragePolicyPtr> map;
};

static StoragePolicyMgr s_storage_policy_mgr;

Result<StorageResource> get_resource_by_storage_policy_id(int64_t storage_policy_id) {
    auto storage_policy = get_storage_policy(storage_policy_id);
    if (storage_policy == nullptr) {
        return ResultError(Status::NotFound<false>(
                "could not find storage_policy, storage_policy_id={}", storage_policy_id));
    }

    if (auto resource = get_storage_resource(storage_policy->resource_id); resource) {
        return resource->first;
    } else {
        return ResultError(Status::NotFound<false>("could not find resource, resource_id={}",
                                                   storage_policy->resource_id));
    }
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

struct StorageResourceMgr {
    std::mutex mtx;
    // resource_id -> storage_resource, resource_version
    std::unordered_map<std::string, std::pair<StorageResource, int64_t>> map;
};

static StorageResourceMgr s_storage_resource_mgr;

io::RemoteFileSystemSPtr get_filesystem(const std::string& resource_id) {
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    if (auto it = s_storage_resource_mgr.map.find(resource_id);
        it != s_storage_resource_mgr.map.end()) {
        return it->second.first.fs;
    }
    return nullptr;
}

std::optional<std::pair<StorageResource, int64_t>> get_storage_resource(int64_t resource_id) {
    return get_storage_resource(std::to_string(resource_id));
}

std::optional<std::pair<StorageResource, int64_t>> get_storage_resource(
        const std::string& resource_id) {
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    if (auto it = s_storage_resource_mgr.map.find(resource_id);
        it != s_storage_resource_mgr.map.end()) {
        return it->second;
    }
    return std::nullopt;
}

void put_storage_resource(std::string resource_id, StorageResource resource, int64_t version) {
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    s_storage_resource_mgr.map[resource_id] = std::make_pair(std::move(resource), version);
}

void put_storage_resource(int64_t resource_id, StorageResource resource, int64_t version) {
    auto id_str = std::to_string(resource_id);
    put_storage_resource(id_str, std::move(resource), version);
}

void delete_storage_resource(int64_t resource_id) {
    auto id_str = std::to_string(resource_id);
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    s_storage_resource_mgr.map.erase(id_str);
}

std::vector<std::pair<std::string, int64_t>> get_storage_resource_ids() {
    std::vector<std::pair<std::string, int64_t>> res;
    res.reserve(s_storage_resource_mgr.map.size());
    std::lock_guard lock(s_storage_resource_mgr.mtx);
    for (auto& [id, resource] : s_storage_resource_mgr.map) {
        res.emplace_back(id, resource.second);
    }
    return res;
}

namespace {

[[noreturn]] void exit_at_unknown_path_version(std::string_view resource_id, int64_t path_version) {
    LOG(FATAL) << "unknown path version, please upgrade BE or drop this storage vault. resource_id="
               << resource_id << " path_version=" << path_version;
}

} // namespace

StorageResource::StorageResource(io::RemoteFileSystemSPtr fs_,
                                 const cloud::StorageVaultPB_PathFormat& path_format)
        : fs(std::move(fs_)), path_version(path_format.path_version()) {
    switch (path_version) {
    case 0:
        break;
    case 1:
        shard_fn = [shard_num = path_format.shard_num()](int64_t tablet_id) {
            return HashUtil::murmur_hash64A(static_cast<void*>(&tablet_id), sizeof(tablet_id),
                                            HashUtil::MURMUR_SEED) %
                   shard_num;
        };
        break;
    default:
        exit_at_unknown_path_version(fs->id(), path_version);
    }
}

std::string StorageResource::remote_segment_path(int64_t tablet_id, std::string_view rowset_id,
                                                 int64_t seg_id) const {
    switch (path_version) {
    case 0:
        return fmt::format("{}/{}/{}_{}.dat", DATA_PREFIX, tablet_id, rowset_id, seg_id);
    case 1:
        return fmt::format("{}/{}/{}/{}/{}.dat", DATA_PREFIX, shard_fn(tablet_id), tablet_id,
                           rowset_id, seg_id);
    default:
        exit_at_unknown_path_version(fs->id(), path_version);
    }
}

std::string StorageResource::remote_segment_path(const RowsetMeta& rowset, int64_t seg_id) const {
    switch (path_version) {
    case 0:
        return fmt::format("{}/{}/{}_{}.dat", DATA_PREFIX, rowset.tablet_id(),
                           rowset.rowset_id().to_string(), seg_id);
    case 1:
        return fmt::format("{}/{}/{}/{}/{}.dat", DATA_PREFIX, shard_fn(rowset.tablet_id()),
                           rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    default:
        exit_at_unknown_path_version(fs->id(), path_version);
    }
}

std::string StorageResource::remote_idx_v1_path(const RowsetMeta& rowset, int64_t seg_id,
                                                int64_t index_id,
                                                std::string_view index_path_suffix) const {
    std::string suffix =
            index_path_suffix.empty() ? "" : std::string {"@"} + index_path_suffix.data();
    switch (path_version) {
    case 0:
        return fmt::format("{}/{}/{}_{}_{}{}.idx", DATA_PREFIX, rowset.tablet_id(),
                           rowset.rowset_id().to_string(), seg_id, index_id, suffix);
    case 1:
        return fmt::format("{}/{}/{}/{}/{}_{}{}.idx", DATA_PREFIX, shard_fn(rowset.tablet_id()),
                           rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id, index_id,
                           suffix);
    default:
        exit_at_unknown_path_version(fs->id(), path_version);
    }
}

std::string StorageResource::remote_idx_v2_path(const RowsetMeta& rowset, int64_t seg_id) const {
    switch (path_version) {
    case 0:
        return fmt::format("{}/{}/{}_{}.idx", DATA_PREFIX, rowset.tablet_id(),
                           rowset.rowset_id().to_string(), seg_id);
    case 1:
        return fmt::format("{}/{}/{}/{}/{}.idx", DATA_PREFIX, shard_fn(rowset.tablet_id()),
                           rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    default:
        exit_at_unknown_path_version(fs->id(), path_version);
    }
}

std::string StorageResource::remote_tablet_path(int64_t tablet_id) const {
    switch (path_version) {
    case 0:
        return fmt::format("{}/{}", DATA_PREFIX, tablet_id);
    case 1:
        return fmt::format("{}/{}/{}", DATA_PREFIX, shard_fn(tablet_id), tablet_id);
    default:
        exit_at_unknown_path_version(fs->id(), path_version);
    }
}

std::string StorageResource::cooldown_tablet_meta_path(int64_t tablet_id, int64_t replica_id,
                                                       int64_t cooldown_term) const {
    return remote_tablet_path(tablet_id) + '/' +
           cooldown_tablet_meta_filename(replica_id, cooldown_term);
}

} // namespace doris
