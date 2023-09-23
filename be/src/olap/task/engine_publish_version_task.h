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

#ifndef DORIS_BE_SRC_OLAP_TASK_ENGINE_PUBLISH_VERSION_TASK_H
#define DORIS_BE_SRC_OLAP_TASK_ENGINE_PUBLISH_VERSION_TASK_H

#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet.h"
#include "olap/task/engine_task.h"
#include "util/time.h"

namespace doris {

class EnginePublishVersionTask;
class TPublishVersionRequest;

struct TabletPublishStatistics {
    int64_t submit_time_us = 0;
    int64_t schedule_time_us = 0;
    int64_t lock_wait_time_us = 0;
    int64_t save_meta_time_us = 0;
    int64_t calc_delete_bitmap_time_us = 0;
    int64_t partial_update_write_segment_us = 0;
    int64_t add_inc_rowset_us = 0;

    std::string to_string() {
        return fmt::format(
                "[Publish Statistics: schedule time(us): {}, lock wait time(us): {}, save meta "
                "time(us): {}, calc delete bitmap time(us): {}, partial update write segment "
                "time(us): {}, add inc rowset time(us): {}]",
                schedule_time_us, lock_wait_time_us, save_meta_time_us, calc_delete_bitmap_time_us,
                partial_update_write_segment_us, add_inc_rowset_us);
    }

    void record_in_bvar();
};

class TabletPublishTxnTask {
public:
    TabletPublishTxnTask(EnginePublishVersionTask* engine_task, TabletSharedPtr tablet,
                         RowsetSharedPtr rowset, int64_t partition_id, int64_t transaction_id,
                         Version version, const TabletInfo& tablet_info);
    ~TabletPublishTxnTask() = default;

    void handle();

private:
    EnginePublishVersionTask* _engine_publish_version_task;

    TabletSharedPtr _tablet;
    RowsetSharedPtr _rowset;
    int64_t _partition_id;
    int64_t _transaction_id;
    Version _version;
    TabletInfo _tablet_info;
    TabletPublishStatistics _stats;
};

class EnginePublishVersionTask : public EngineTask {
public:
    EnginePublishVersionTask(
            const TPublishVersionRequest& publish_version_req,
            std::set<TTabletId>* error_tablet_ids, std::map<TTabletId, TVersion>* succ_tablets,
            std::vector<std::tuple<int64_t, int64_t, int64_t>>* discontinous_version_tablets,
            std::map<TTabletId, int64_t>* tablet_id_to_num_delta_rows);
    ~EnginePublishVersionTask() override = default;

    Status finish() override;

    void add_error_tablet_id(int64_t tablet_id);

    int64_t finish_task();

private:
    const TPublishVersionRequest& _publish_version_req;
    std::mutex _tablet_ids_mutex;
    std::set<TTabletId>* _error_tablet_ids;
    std::map<TTabletId, TVersion>* _succ_tablets;
    std::vector<std::tuple<int64_t, int64_t, int64_t>>* _discontinuous_version_tablets;
    std::map<TTabletId, int64_t>* _tablet_id_to_num_delta_rows;
};

class AsyncTabletPublishTask {
public:
    AsyncTabletPublishTask(TabletSharedPtr tablet, int64_t partition_id, int64_t transaction_id,
                           int64_t version)
            : _tablet(tablet),
              _partition_id(partition_id),
              _transaction_id(transaction_id),
              _version(version) {
        _stats.submit_time_us = MonotonicMicros();
    }
    ~AsyncTabletPublishTask() = default;

    void handle();

private:
    TabletSharedPtr _tablet;
    int64_t _partition_id;
    int64_t _transaction_id;
    int64_t _version;
    TabletPublishStatistics _stats;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_TASK_ENGINE_PUBLISH_VERSION_TASK_H
