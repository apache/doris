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

#include "cloud/meta_mgr.h"
#include "olap/rowset/rowset_meta.h"

namespace doris::cloud {
class TabletStatsPB;
class TabletIndexPB;

class CloudMetaMgr final : public MetaMgr {
public:
    CloudMetaMgr() = default;
    ~CloudMetaMgr() override = default;
    CloudMetaMgr(const CloudMetaMgr&) = delete;
    CloudMetaMgr& operator=(const CloudMetaMgr&) = delete;

    Status get_tablet_meta(int64_t tablet_id, std::shared_ptr<TabletMeta>* tablet_meta) override;

    Status sync_tablet_rowsets(Tablet* tablet, bool warmup_delta_data = false) override;

    Status prepare_rowset(const RowsetMeta* rs_meta, bool is_tmp,
                          std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr) override;

    Status commit_rowset(const RowsetMeta* rs_meta, bool is_tmp,
                         std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr) override;

    Status update_tmp_rowset(const RowsetMeta& rs_meta) override;

    Status commit_txn(StreamLoadContext* ctx, bool is_2pc) override;

    Status abort_txn(StreamLoadContext* ctx) override;

    Status precommit_txn(StreamLoadContext* ctx) override;

    Status get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) override;

    Status prepare_tablet_job(const TabletJobInfoPB& job, StartTabletJobResponse* res) override;

    Status commit_tablet_job(const TabletJobInfoPB& job, FinishTabletJobResponse* res) override;

    Status abort_tablet_job(const TabletJobInfoPB& job) override;

    Status lease_tablet_job(const TabletJobInfoPB& job) override;

    Status update_tablet_schema(int64_t tablet_id, const TabletSchema* tablet_schema) override;

    Status update_delete_bitmap(const Tablet* tablet, int64_t lock_id, int64_t initiator,
                                DeleteBitmap* delete_bitmap) override;

    Status get_delete_bitmap_update_lock(const Tablet* tablet, int64_t lock_id,
                                         int64_t initiator) override;

private:
    Status sync_tablet_delete_bitmap(
            Tablet* tablet, int64_t old_max_version,
            const google::protobuf::RepeatedPtrField<RowsetMetaPB>& rs_metas,
            const TabletStatsPB& stas, const TabletIndexPB& idx, DeleteBitmap* delete_bitmap);
};

} // namespace doris::cloud
