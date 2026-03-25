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

#include <gen_cpp/AgentService_types.h>

#include <memory>
#include <mutex>
#include <optional>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/task/engine_task.h"

namespace doris {

class MemTrackerLimiter;

class CloudTabletCalcDeleteBitmapAsyncPublishTask {
public:
    CloudTabletCalcDeleteBitmapAsyncPublishTask(CloudStorageEngine& engine, int64_t tablet_id,
                                                int64_t transaction_id, int64_t version,
                                                int64_t db_id, int64_t table_id, int64_t index_id,
                                                int64_t partition_id);
    ~CloudTabletCalcDeleteBitmapAsyncPublishTask() = default;

    void set_tablet_state(int64_t tablet_state);

    Status handle() const;

private:
    Status _handle_rowset(std::shared_ptr<CloudTablet> tablet, int64_t version) const;

    Status _handle_async_publish(std::shared_ptr<CloudTablet> tablet, int64_t version) const;

    Status _apply_rowset_to_tablet(std::shared_ptr<CloudTablet> tablet, int64_t version,
                                   RowsetSharedPtr& rowset,
                                   const std::shared_ptr<DeleteBitmap>& delete_bitmap,
                                   int64_t visible_ts_ms,
                                   std::unique_lock<std::shared_mutex>& meta_lock) const;

    CloudStorageEngine& _engine;

    int64_t _tablet_id;
    int64_t _transaction_id;
    int64_t _version;
    std::optional<int64_t> _ms_tablet_state;
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;

    int64_t _db_id;
    int64_t _table_id;
    int64_t _index_id;
    int64_t _partition_id;
};

class CloudCalcDeleteBitmapAsyncPublishTask : public EngineTask {
public:
    CloudCalcDeleteBitmapAsyncPublishTask(
            CloudStorageEngine& engine,
            const TCalcDeleteBitmapAsyncPublishRequest& request,
            std::vector<TTabletId>* error_tablet_ids,
            std::vector<TTabletId>* succ_tablet_ids = nullptr);
    Status execute() override;

    void add_error_tablet_id(int64_t tablet_id, const Status& err);
    void add_succ_tablet_id(int64_t tablet_id);

private:
    CloudStorageEngine& _engine;
    const TCalcDeleteBitmapAsyncPublishRequest& _request;
    std::mutex _mutex;
    std::vector<TTabletId>* _error_tablet_ids;
    std::vector<TTabletId>* _succ_tablet_ids;

    Status _res;
};

} // namespace doris
