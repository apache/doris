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
#include <vector>

#include "common/status.h"
#include "util/s3_util.h"

namespace doris {
class StreamLoadContext;
class Tablet;
class TabletMeta;
class RowsetMeta;
class TabletSchema;
class DeleteBitmap;

namespace cloud {

class TabletJobInfoPB;
class StartTabletJobResponse;
class FinishTabletJobResponse;

class MetaMgr {
public:
    virtual ~MetaMgr() = default;

    virtual Status open() { return Status::OK(); }

    virtual Status get_tablet_meta(int64_t tablet_id, std::shared_ptr<TabletMeta>* tablet_meta) = 0;

    // If `warmup_delta_data` is true, download the new version rowset data in background
    virtual Status sync_tablet_rowsets(Tablet* tablet, bool warmup_delta_data = false) = 0;

    virtual Status prepare_rowset(const RowsetMeta* rs_meta, bool is_tmp,
                                  std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr) = 0;

    virtual Status commit_rowset(const RowsetMeta* rs_meta, bool is_tmp,
                                 std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr) = 0;

    virtual Status update_tmp_rowset(const RowsetMeta& rs_meta) = 0;

    virtual Status commit_txn(StreamLoadContext* ctx, bool is_2pc) = 0;

    virtual Status abort_txn(StreamLoadContext* ctx) = 0;

    virtual Status precommit_txn(StreamLoadContext* ctx) = 0;

    virtual Status get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) = 0;

    virtual Status prepare_tablet_job(const TabletJobInfoPB& job, StartTabletJobResponse* res) = 0;

    virtual Status commit_tablet_job(const TabletJobInfoPB& job, FinishTabletJobResponse* res) = 0;

    virtual Status abort_tablet_job(const TabletJobInfoPB& job) = 0;

    virtual Status lease_tablet_job(const TabletJobInfoPB& job) = 0;

    virtual Status update_delete_bitmap(const Tablet* tablet, int64_t lock_id, int64_t initiator,
                                        DeleteBitmap* delete_bitmap) = 0;

    virtual Status get_delete_bitmap_update_lock(const Tablet* tablet, int64_t lock_id,
                                                 int64_t initiator) = 0;

    virtual Status update_tablet_schema(int64_t tablet_id, const TabletSchema* tablet_schema) = 0;
};

} // namespace cloud
} // namespace doris
