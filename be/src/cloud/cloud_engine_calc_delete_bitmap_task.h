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

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "gen_cpp/AgentService_types.h"
#include "olap/tablet_fwd.h"
#include "olap/task/engine_task.h"

namespace doris {

class CloudEngineCalcDeleteBitmapTask;
class MemTrackerLimiter;

class CloudTabletCalcDeleteBitmapTask {
public:
    CloudTabletCalcDeleteBitmapTask(CloudStorageEngine& engine,
                                    CloudEngineCalcDeleteBitmapTask* engine_task, int64_t tablet_id,
                                    int64_t transaction_id, int64_t version);
    ~CloudTabletCalcDeleteBitmapTask() = default;

    void set_compaction_stats(int64_t ms_base_compaction_cnt, int64_t ms_cumulative_compaction_cnt,
                              int64_t ms_cumulative_point);

    Status handle() const;

private:
    CloudStorageEngine& _engine;
    CloudEngineCalcDeleteBitmapTask* _engine_calc_delete_bitmap_task;

    int64_t _tablet_id;
    int64_t _transaction_id;
    int64_t _version;

    int64_t _ms_base_compaction_cnt {-1};
    int64_t _ms_cumulative_compaction_cnt {-1};
    int64_t _ms_cumulative_point {-1};
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
};

class CloudEngineCalcDeleteBitmapTask : public EngineTask {
public:
    CloudEngineCalcDeleteBitmapTask(CloudStorageEngine& engine,
                                    const TCalcDeleteBitmapRequest& cal_delete_bitmap_req,
                                    std::vector<TTabletId>* error_tablet_ids,
                                    std::vector<TTabletId>* succ_tablet_ids = nullptr);
    Status execute() override;

    void add_error_tablet_id(int64_t tablet_id, const Status& err);
    void add_succ_tablet_id(int64_t tablet_id);

private:
    CloudStorageEngine& _engine;
    const TCalcDeleteBitmapRequest& _cal_delete_bitmap_req;
    std::mutex _mutex;
    std::vector<TTabletId>* _error_tablet_ids;
    std::vector<TTabletId>* _succ_tablet_ids;

    Status _res;
};

} // namespace doris
