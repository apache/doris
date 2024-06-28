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

#include <fmt/format.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/remote_file_system.h"

namespace doris {
class RowsetMeta;

namespace cloud {
class StorageVaultPB_PathFormat;
}

struct StoragePolicy {
    std::string name;
    int64_t version;
    int64_t cooldown_datetime;
    int64_t cooldown_ttl;
    int64_t resource_id;

    std::string to_string() const {
        return fmt::format(
                "(name={}, version={}, cooldown_date_time={}, cooldown_ttl={}, resource_id={})",
                name, version, cooldown_datetime, cooldown_ttl, resource_id);
    }
};

using StoragePolicyPtr = std::shared_ptr<StoragePolicy>;

// return nullptr if not found
StoragePolicyPtr get_storage_policy(int64_t id);

// always success
void put_storage_policy(int64_t id, StoragePolicyPtr policy);

void delete_storage_policy(int64_t id);

// return [id, version] of all storage policies
std::vector<std::pair<int64_t, int64_t>> get_storage_policy_ids();

struct StorageResource {
    io::RemoteFileSystemSPtr fs;
    int64_t path_version = 0;
    std::function<int64_t(int64_t)> shard_fn;

    StorageResource() = default;
    StorageResource(io::RemoteFileSystemSPtr fs_) : fs(std::move(fs_)) {}
    StorageResource(io::RemoteFileSystemSPtr, const cloud::StorageVaultPB_PathFormat&);

    std::string remote_segment_path(int64_t tablet_id, std::string_view rowset_id,
                                    int64_t seg_id) const;
    std::string remote_segment_path(const RowsetMeta& rowset, int64_t seg_id) const;
    std::string remote_tablet_path(int64_t tablet_id) const;
    std::string cooldown_tablet_meta_path(int64_t tablet_id, int64_t replica_id,
                                          int64_t cooldown_term) const;
};

// return nullptr if not found
io::RemoteFileSystemSPtr get_filesystem(const std::string& resource_id);

// Get `StorageResource` and its version
std::optional<std::pair<StorageResource, int64_t>> get_storage_resource(int64_t resource_id);
std::optional<std::pair<StorageResource, int64_t>> get_storage_resource(
        const std::string& resource_id);

Result<StorageResource> get_resource_by_storage_policy_id(int64_t storage_policy_id);

// always success
void put_storage_resource(std::string resource_id, StorageResource resource, int64_t version);

// always success
void put_storage_resource(int64_t resource_id, StorageResource resource, int64_t version);

void delete_storage_resource(int64_t resource_id);

// return [id, version] of all resources
std::vector<std::pair<std::string, int64_t>> get_storage_resource_ids();

} // namespace doris
