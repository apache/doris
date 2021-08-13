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

#ifndef DORIS_BE_SRC_OLAP_TASK_ENGINE_CLONE_TASK_H
#define DORIS_BE_SRC_OLAP_TASK_ENGINE_CLONE_TASK_H

#include "agent/utils.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/olap_define.h"
#include "olap/task/engine_task.h"

namespace doris {

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineCloneTask : public EngineTask {
public:
    virtual OLAPStatus execute();

public:
    EngineCloneTask(const TCloneReq& _clone_req, const TMasterInfo& _master_info,
                    int64_t _signature, vector<string>* error_msgs,
                    vector<TTabletInfo>* tablet_infos, AgentStatus* _res_status);
    ~EngineCloneTask() {}

private:
    OLAPStatus _do_clone();

    virtual OLAPStatus _finish_clone(Tablet* tablet, const std::string& clone_dir,
                                     int64_t committed_version, bool is_incremental_clone);

    OLAPStatus _finish_incremental_clone(Tablet* tablet, const TabletMeta& cloned_tablet_meta,
                                       int64_t committed_version);

    OLAPStatus _finish_full_clone(Tablet* tablet, TabletMeta* cloned_tablet_meta);

    AgentStatus _make_and_download_snapshots(DataDir& data_dir, const string& local_data_path, TBackend* src_host,
                            string* src_file_path, vector<string>* error_msgs,
                            const vector<Version>* missing_versions, bool* allow_incremental_clone);

    void _set_tablet_info(AgentStatus status, bool is_new_tablet);

    // Download tablet files from
    Status _download_files(DataDir* data_dir, const std::string& remote_url_prefix,
                           const std::string& local_path);

    Status _make_snapshot(const std::string& ip, int port, TTableId tablet_id,
                          TSchemaHash schema_hash, int timeout_s,
                          const std::vector<Version>* missed_versions, std::string* snapshot_path,
                          bool* allow_incremental_clone, int32_t* snapshot_version);

    Status _release_snapshot(const std::string& ip, int port, const std::string& snapshot_path);

private:
    const TCloneReq& _clone_req;
    vector<string>* _error_msgs;
    vector<TTabletInfo>* _tablet_infos;
    AgentStatus* _res_status;
    int64_t _signature;
    const TMasterInfo& _master_info;
    int64_t _copy_size;
    int64_t _copy_time_ms;
}; // EngineTask

} // namespace doris
#endif //DORIS_BE_SRC_OLAP_TASK_ENGINE_CLONE_TASK_H
