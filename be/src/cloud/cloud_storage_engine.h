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

//#include "cloud/cloud_cumulative_compaction.h"
//#include "cloud/cloud_base_compaction.h"
//#include "cloud/cloud_full_compaction.h"
#include "cloud/cloud_cumulative_compaction_policy.h"
#include "cloud/cloud_tablet.h"
#include "cloud_txn_delete_bitmap_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "util/threadpool.h"

namespace doris {
namespace cloud {
class CloudMetaMgr;
}
namespace io {
class FileCacheBlockDownloader;
}

class CloudTabletMgr;
class CloudCumulativeCompaction;
class CloudBaseCompaction;
class CloudFullCompaction;
class TabletHotspot;
class CloudWarmUpManager;

class CloudStorageEngine final : public BaseStorageEngine {
public:
    CloudStorageEngine(const UniqueId& backend_uid);

    ~CloudStorageEngine() override;

    Status open() override;
    void stop() override;
    bool stopped() override;

    Result<BaseTabletSPtr> get_tablet(int64_t tablet_id) override;

    Status start_bg_threads() override;

    Status set_cluster_id(int32_t cluster_id) override {
        _effective_cluster_id = cluster_id;
        return Status::OK();
    }

    cloud::CloudMetaMgr& meta_mgr() const { return *_meta_mgr; }

    CloudTabletMgr& tablet_mgr() const { return *_tablet_mgr; }

    CloudTxnDeleteBitmapCache& txn_delete_bitmap_cache() const { return *_txn_delete_bitmap_cache; }
    ThreadPool& calc_tablet_delete_bitmap_task_thread_pool() const {
        return *_calc_tablet_delete_bitmap_task_thread_pool;
    }
    void _check_file_cache_ttl_block_valid();

    std::optional<StorageResource> get_storage_resource(const std::string& vault_id) {
        LOG(INFO) << "Getting storage resource for vault_id: " << vault_id;
        if (vault_id.empty()) {
            if (latest_fs() == nullptr) {
                LOG(INFO) << "there is not latest fs";
                return std::nullopt;
            }
            return StorageResource {latest_fs()};
        }

        bool synced = false;
        do {
            if (auto storage_resource = doris::get_storage_resource(vault_id); storage_resource) {
                return storage_resource->first;
            }
            if (synced) {
                break;
            }
            sync_storage_vault();
            synced = true;
        } while (true);

        return std::nullopt;
    }

    io::RemoteFileSystemSPtr latest_fs() const {
        std::lock_guard lock(_latest_fs_mtx);
        return _latest_fs;
    }

    void set_latest_fs(const io::RemoteFileSystemSPtr& fs) {
        std::lock_guard lock(_latest_fs_mtx);
        _latest_fs = fs;
    }

    void get_cumu_compaction(int64_t tablet_id,
                             std::vector<std::shared_ptr<CloudCumulativeCompaction>>& res);

    Status submit_compaction_task(const CloudTabletSPtr& tablet, CompactionType compaction_type);

    Status get_compaction_status_json(std::string* result);

    bool has_base_compaction(int64_t tablet_id) const {
        std::lock_guard lock(_compaction_mtx);
        return _submitted_base_compactions.contains(tablet_id);
    }

    bool has_cumu_compaction(int64_t tablet_id) const {
        std::lock_guard lock(_compaction_mtx);
        return _submitted_cumu_compactions.contains(tablet_id);
    }

    bool has_full_compaction(int64_t tablet_id) const {
        std::lock_guard lock(_compaction_mtx);
        return _submitted_full_compactions.contains(tablet_id);
    }

    std::shared_ptr<CloudCumulativeCompactionPolicy> cumu_compaction_policy(
            std::string_view compaction_policy);

    void sync_storage_vault();

    io::FileCacheBlockDownloader& file_cache_block_downloader() const {
        return *_file_cache_block_downloader;
    }

    CloudWarmUpManager& cloud_warm_up_manager() const { return *_cloud_warm_up_manager; }

    TabletHotspot& tablet_hotspot() const { return *_tablet_hotspot; }

    ThreadPool& sync_load_for_tablets_thread_pool() const {
        return *_sync_load_for_tablets_thread_pool;
    }

private:
    void _refresh_storage_vault_info_thread_callback();
    void _vacuum_stale_rowsets_thread_callback();
    void _sync_tablets_thread_callback();
    void _compaction_tasks_producer_callback();
    std::vector<CloudTabletSPtr> _generate_cloud_compaction_tasks(CompactionType compaction_type,
                                                                  bool check_score);
    Status _adjust_compaction_thread_num();
    Status _submit_base_compaction_task(const CloudTabletSPtr& tablet);
    Status _submit_cumulative_compaction_task(const CloudTabletSPtr& tablet);
    Status _submit_full_compaction_task(const CloudTabletSPtr& tablet);
    void _lease_compaction_thread_callback();

    std::atomic_bool _stopped {false};

    std::unique_ptr<cloud::CloudMetaMgr> _meta_mgr;
    std::unique_ptr<CloudTabletMgr> _tablet_mgr;
    std::unique_ptr<CloudTxnDeleteBitmapCache> _txn_delete_bitmap_cache;
    std::unique_ptr<ThreadPool> _calc_tablet_delete_bitmap_task_thread_pool;

    // Components for cache warmup
    std::unique_ptr<io::FileCacheBlockDownloader> _file_cache_block_downloader;
    // Depended by `FileCacheBlockDownloader`
    std::unique_ptr<CloudWarmUpManager> _cloud_warm_up_manager;
    std::unique_ptr<TabletHotspot> _tablet_hotspot;
    std::unique_ptr<ThreadPool> _sync_load_for_tablets_thread_pool;

    // FileSystem with latest shared storage info, new data will be written to this fs.
    mutable std::mutex _latest_fs_mtx;
    io::RemoteFileSystemSPtr _latest_fs;

    std::vector<scoped_refptr<Thread>> _bg_threads;

    // ATTN: Compactions in maps depend on `CloudTabletMgr` and `CloudMetaMgr`
    mutable std::mutex _compaction_mtx;
    // tablet_id -> submitted base compaction, guarded by `_compaction_mtx`
    std::unordered_map<int64_t, std::shared_ptr<CloudBaseCompaction>> _submitted_base_compactions;
    // tablet_id -> submitted full compaction, guarded by `_compaction_mtx`
    std::unordered_map<int64_t, std::shared_ptr<CloudFullCompaction>> _submitted_full_compactions;
    // Store tablets which are preparing cumu compaction, guarded by `_compaction_mtx`
    std::unordered_set<int64_t> _tablet_preparing_cumu_compaction;
    // tablet_id -> submitted cumu compactions, guarded by `_compaction_mtx`
    std::unordered_map<int64_t, std::vector<std::shared_ptr<CloudCumulativeCompaction>>>
            _submitted_cumu_compactions;

    std::unique_ptr<ThreadPool> _base_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _cumu_compaction_thread_pool;

    using CumuPolices =
            std::unordered_map<std::string_view, std::shared_ptr<CloudCumulativeCompactionPolicy>>;
    CumuPolices _cumulative_compaction_policies;
};

} // namespace doris
