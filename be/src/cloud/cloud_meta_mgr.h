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

#include <gen_cpp/olap_file.pb.h>

#include <future>
#include <memory>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "olap/rowset/rowset_meta.h"
#include "util/s3_util.h"

namespace doris {
#include "common/compile_check_begin.h"

class DeleteBitmap;
class StreamLoadContext;
class CloudTablet;
class TabletMeta;
class TabletSchema;
class TabletMetaPB;
class RowsetMeta;

namespace cloud {

class FinishTabletJobResponse;
class StartTabletJobResponse;
class TabletJobInfoPB;
class TabletStatsPB;
class TabletIndexPB;

using StorageVaultInfos = std::vector<
        std::tuple<std::string, std::variant<S3Conf, HdfsVaultInfo>, StorageVaultPB_PathFormat>>;

// run tasks in bthread with concurrency and wait until all tasks done
// it stops running tasks if there are any tasks return !ok, leaving some tasks untouched
// return OK if all tasks successfully done, otherwise return the result of the failed task
Status bthread_fork_join(const std::vector<std::function<Status()>>& tasks, int concurrency);

// An async wrap of `bthread_fork_join` declared previously using promise-future
// return OK if fut successfully created, otherwise return error
Status bthread_fork_join(std::vector<std::function<Status()>>&& tasks, int concurrency,
                         std::future<Status>* fut);

class CloudMetaMgr {
public:
    CloudMetaMgr() = default;
    ~CloudMetaMgr() = default;
    CloudMetaMgr(const CloudMetaMgr&) = delete;
    CloudMetaMgr& operator=(const CloudMetaMgr&) = delete;

    Status get_tablet_meta(int64_t tablet_id, std::shared_ptr<TabletMeta>* tablet_meta);

    Status get_schema_dict(int64_t index_id, std::shared_ptr<SchemaCloudDictionary>* schema_dict);

    Status sync_tablet_rowsets(CloudTablet* tablet, const SyncOptions& options = {},
                               SyncRowsetStats* sync_stats = nullptr);
    Status sync_tablet_rowsets_unlocked(
            CloudTablet* tablet, std::unique_lock<bthread::Mutex>& lock /* _sync_meta_lock */,
            const SyncOptions& options = {}, SyncRowsetStats* sync_stats = nullptr);

    Status prepare_rowset(const RowsetMeta& rs_meta, const std::string& job_id,
                          std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr);

    Status commit_rowset(RowsetMeta& rs_meta, const std::string& job_id,
                         std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr);

    Status update_tmp_rowset(const RowsetMeta& rs_meta);

    Status commit_txn(const StreamLoadContext& ctx, bool is_2pc);

    Status abort_txn(const StreamLoadContext& ctx);

    Status precommit_txn(const StreamLoadContext& ctx);

    /**
     * Prepares a restore job for a tablet to meta-service
     */
    Status prepare_restore_job(const TabletMetaPB& tablet_meta);

    /**
     * Commits a restore job for a tablet to meta-service
     */
    Status commit_restore_job(const int64_t tablet_id);

    /**
     * Remove a restore job for a tablet from meta-service
     */
    Status finish_restore_job(const int64_t tablet_id);

    /**
     * Gets storage vault (storage backends) from meta-service
     * 
     * @param vault_info output param, all storage backends
     * @param is_vault_mode output param, true for pure vault mode, false for legacy mode
     * @return status
     */
    Status get_storage_vault_info(StorageVaultInfos* vault_infos, bool* is_vault_mode);

    Status prepare_tablet_job(const TabletJobInfoPB& job, StartTabletJobResponse* res);

    Status commit_tablet_job(const TabletJobInfoPB& job, FinishTabletJobResponse* res);

    Status abort_tablet_job(const TabletJobInfoPB& job);

    Status lease_tablet_job(const TabletJobInfoPB& job);

    Status update_delete_bitmap(const CloudTablet& tablet, int64_t lock_id, int64_t initiator,
                                DeleteBitmap* delete_bitmap, int64_t txn_id = -1,
                                bool is_explicit_txn = false, int64_t next_visible_version = -1);

    Status cloud_update_delete_bitmap_without_lock(
            const CloudTablet& tablet, DeleteBitmap* delete_bitmap,
            std::map<std::string, int64_t>& rowset_to_versions,
            int64_t pre_rowset_agg_start_version = 0, int64_t pre_rowset_agg_end_version = 0);

    Status get_delete_bitmap_update_lock(const CloudTablet& tablet, int64_t lock_id,
                                         int64_t initiator);

    void remove_delete_bitmap_update_lock(int64_t table_id, int64_t lock_id, int64_t initiator,
                                          int64_t tablet_id);

private:
    bool sync_tablet_delete_bitmap_by_cache(CloudTablet* tablet, int64_t old_max_version,
                                            std::ranges::range auto&& rs_metas,
                                            DeleteBitmap* delete_bitmap);

    Status sync_tablet_delete_bitmap(CloudTablet* tablet, int64_t old_max_version,
                                     std::ranges::range auto&& rs_metas, const TabletStatsPB& stats,
                                     const TabletIndexPB& idx, DeleteBitmap* delete_bitmap,
                                     bool full_sync = false, SyncRowsetStats* sync_stats = nullptr);
    void check_table_size_correctness(const RowsetMeta& rs_meta);
    int64_t get_segment_file_size(const RowsetMeta& rs_meta);
    int64_t get_inverted_index_file_szie(const RowsetMeta& rs_meta);
};

} // namespace cloud
#include "common/compile_check_end.h"
} // namespace doris
