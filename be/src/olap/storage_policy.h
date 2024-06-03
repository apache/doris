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
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/remote_file_system.h"

namespace doris {

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

Status get_remote_file_system(int64_t storage_policy_id, std::shared_ptr<io::RemoteFileSystem>* fs);

// return nullptr if not found
StoragePolicyPtr get_storage_policy(int64_t id);

// always success
void put_storage_policy(int64_t id, StoragePolicyPtr policy);

void delete_storage_policy(int64_t id);

// return [id, version] of all storage policies
std::vector<std::pair<int64_t, int64_t>> get_storage_policy_ids();

struct StorageResource {
    io::RemoteFileSystemSPtr fs;
    int64_t version = -1;
};

// return nullptr if not found
io::RemoteFileSystemSPtr get_filesystem(const std::string& resource_id);

// return [nullptr, -1] if not found
StorageResource get_storage_resource(int64_t resource_id);

// always success
void put_storage_resource(std::string resource_id, StorageResource resource);

// always success
void put_storage_resource(int64_t resource_id, StorageResource resource);

void delete_storage_resource(int64_t resource_id);

// return [id, version] of all resources
std::vector<std::pair<std::string, int64_t>> get_storage_resource_ids();

} // namespace doris
