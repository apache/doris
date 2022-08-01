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

#include "gen_cpp/AgentService_types.h"
#include "olap/olap_define.h"
#include "olap/task/engine_task.h"

namespace doris {

class EnginePublishVersionTask;
class TabletPublishTxnTask {
public:
    TabletPublishTxnTask(EnginePublishVersionTask* engine_task, TabletSharedPtr tablet,
                         RowsetSharedPtr rowset, int64_t partition_id, int64_t transaction_id,
                         Version version, const TabletInfo& tablet_info,
                         std::atomic<int64_t>* total_task_num);
    ~TabletPublishTxnTask() {}

    void handle();

private:
    EnginePublishVersionTask* _engine_publish_version_task;

    TabletSharedPtr _tablet;
    RowsetSharedPtr _rowset;
    int64_t _partition_id;
    int64_t _transaction_id;
    Version _version;
    TabletInfo _tablet_info;

    std::atomic<int64_t>* _total_task_num;
};

class EnginePublishVersionTask : public EngineTask {
public:
    EnginePublishVersionTask(TPublishVersionRequest& publish_version_req,
                             vector<TTabletId>* error_tablet_ids,
                             std::vector<TTabletId>* succ_tablet_ids = nullptr);
    ~EnginePublishVersionTask() {}

    virtual Status finish() override;

    void add_error_tablet_id(int64_t tablet_id);
    void add_succ_tablet_id(int64_t tablet_id);

    void notify();
    void wait();

private:
    const TPublishVersionRequest& _publish_version_req;
    std::mutex _tablet_ids_mutex;
    vector<TTabletId>* _error_tablet_ids;
    vector<TTabletId>* _succ_tablet_ids;

    std::mutex _tablet_finish_sleep_mutex;
    std::condition_variable _tablet_finish_sleep_cond;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_TASK_ENGINE_PUBLISH_VERSION_TASK_H
