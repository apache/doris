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
#include <string>
#include <tuple>
#include <variant>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset_meta.h"
#include "util/s3_util.h"

namespace doris {

class DeleteBitmap;
class StreamLoadContext;
class CloudTablet;
class TabletMeta;
class TabletSchema;
class RowsetMeta;

namespace cloud {

class FinishTabletJobResponse;
class StartTabletJobResponse;
class TabletJobInfoPB;
class TabletStatsPB;
class TabletIndexPB;

using StorageVaultInfos = std::vector<
        std::tuple<std::string, std::variant<S3Conf, HdfsVaultInfo>, StorageVaultPB_PathFormat>>;

Status bthread_fork_join(const std::vector<std::function<Status()>>& tasks, int concurrency);

class CloudMetaMgr {
public:
    CloudMetaMgr() = default;
    ~CloudMetaMgr() = default;
    CloudMetaMgr(const CloudMetaMgr&) = delete;
    CloudMetaMgr& operator=(const CloudMetaMgr&) = delete;

    Status get_tablet_meta(int64_t tablet_id, std::shared_ptr<TabletMeta>* tablet_meta);

    Status sync_tablet_rowsets(CloudTablet* tablet, bool warmup_delta_data = false);

    Status prepare_rowset(const RowsetMeta& rs_meta,
                          std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr);

    Status commit_rowset(const RowsetMeta& rs_meta,
                         std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr);

    Status update_tmp_rowset(const RowsetMeta& rs_meta);

    Status commit_txn(const StreamLoadContext& ctx, bool is_2pc);

    Status abort_txn(const StreamLoadContext& ctx);

    Status precommit_txn(const StreamLoadContext& ctx);

    Status get_storage_vault_info(StorageVaultInfos* vault_infos);

    Status prepare_tablet_job(const TabletJobInfoPB& job, StartTabletJobResponse* res);

    Status commit_tablet_job(const TabletJobInfoPB& job, FinishTabletJobResponse* res);

    Status abort_tablet_job(const TabletJobInfoPB& job);

    Status lease_tablet_job(const TabletJobInfoPB& job);

    Status update_tablet_schema(int64_t tablet_id, const TabletSchema& tablet_schema);

    Status update_delete_bitmap(const CloudTablet& tablet, int64_t lock_id, int64_t initiator,
                                DeleteBitmap* delete_bitmap);

    Status get_delete_bitmap_update_lock(const CloudTablet& tablet, int64_t lock_id,
                                         int64_t initiator);

private:
    bool sync_tablet_delete_bitmap_by_cache(CloudTablet* tablet, int64_t old_max_version,
                                            std::ranges::range auto&& rs_metas,
                                            DeleteBitmap* delete_bitmap);

    Status sync_tablet_delete_bitmap(CloudTablet* tablet, int64_t old_max_version,
                                     std::ranges::range auto&& rs_metas, const TabletStatsPB& stats,
                                     const TabletIndexPB& idx, DeleteBitmap* delete_bitmap);
};

} // namespace cloud
} // namespace doris
