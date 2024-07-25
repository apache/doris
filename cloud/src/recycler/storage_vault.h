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
#include <optional>
#include <string>

#include "cpp/remote_path_helper.h"

namespace doris {
class RowsetMetaCloudPB;

namespace cloud {
class StorageVaultAccessor;
class StorageVaultRecycler;
class StorageVaultPB;
class ObjectStoreInfoPB;
class InvertedIndexInfoCache;
class SimpleThreadPool;

class StorageVault {
public:
    // Return null if create `StorageVault` failed
    static std::optional<StorageVault> create(const StorageVaultPB& storage_vault_pb);
    static std::optional<StorageVault> create(const ObjectStoreInfoPB& obj_store_info,
                                              const RemotePathContext& ctx);

    explicit StorageVault(std::shared_ptr<StorageVaultAccessor> accessor,
                          RemotePathHelper remote_path_helper)
            : accessor_(std::move(accessor)), remote_path_helper_(std::move(remote_path_helper)) {}
    ~StorageVault();

    StorageVault(const StorageVault&) = default;
    StorageVault& operator=(const StorageVault&) = default;
    StorageVault(StorageVault&&) noexcept = default;
    StorageVault& operator=(StorageVault&&) noexcept = default;

    std::string tablet_path(int64_t tablet_id) const {
        return remote_path_helper_.remote_tablet_path(tablet_id);
    }

    std::string rowset_path_prefix(int64_t tablet_id, std::string_view rowset_id) const {
        return remote_path_helper_.remote_rowset_path_prefix(tablet_id, rowset_id);
    }

    std::string segment_path(int64_t tablet_id, std::string_view rowset_id, int seg_id) const {
        return remote_path_helper_.remote_segment_path(tablet_id, rowset_id, seg_id);
    }

    std::string inverted_index_path_v1(int64_t tablet_id, std::string_view rowset_id,
                                       int64_t segment_id, int64_t inverted_index_id,
                                       std::string_view index_path_suffix) const;

    std::string inverted_index_path_v2(int64_t tablet_id, std::string_view rowset_id,
                                       int64_t segment_id) const;

    const std::shared_ptr<StorageVaultAccessor>& accessor() const { return accessor_; }

    // Create a recycler to submit delete task on this storage vault
    std::unique_ptr<StorageVaultRecycler> create_recycler(
            std::shared_ptr<SimpleThreadPool> worker_pool,
            InvertedIndexInfoCache& inverted_index_info_cache) const;

private:
    std::shared_ptr<StorageVaultAccessor> accessor_;
    RemotePathHelper remote_path_helper_;
};

class StorageVaultRecycler {
public:
    virtual ~StorageVaultRecycler() = default;

    // Submit a delete task for a rowset.
    // Return 0 if success, otherwise error.
    virtual int submit_delete_rowset(const RowsetMetaCloudPB& rowset) = 0;

    // Submit a delete task for a rowset.
    // Return 0 if success, otherwise error.
    virtual int submit_delete_rowset(int64_t tablet_id, std::string_view rowset_id) = 0;

    // Submit a delete task for a tablet.
    // Return 0 if success, otherwise error.
    virtual int submit_delete_tablet(int64_t tablet_id) = 0;

    // Wait until all submitted tasks have been finished.
    // Return 0 if all tasks have been finished successfully, otherwise error.
    virtual int wait() = 0;
};

} // namespace cloud
} // namespace doris
