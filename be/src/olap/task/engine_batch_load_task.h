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

#ifndef DORIS_BE_SRC_OLAP_TASK_ENGINE_BATCH_LOAD_TASK_H
#define DORIS_BE_SRC_OLAP_TASK_ENGINE_BATCH_LOAD_TASK_H

#include <utility>
#include <vector>

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/task/engine_task.h"

using namespace std;

namespace doris {

const uint32_t PUSH_MAX_RETRY = 1;
const uint32_t MAX_RETRY = 3;
const uint32_t DEFAULT_DOWNLOAD_TIMEOUT = 3600;
class StorageEngine;

class EngineBatchLoadTask : public EngineTask {
public:
    EngineBatchLoadTask(TPushReq& push_req, std::vector<TTabletInfo>* tablet_infos,
                        int64_t signature, Status* res_status);
    virtual ~EngineBatchLoadTask();

    virtual Status execute();

private:
    virtual Status _init();

    // The process of push data to olap engine
    //
    // Output parameters:
    // * tablet_infos: The info of pushed tablet after push data
    virtual Status _process();

    // Delete data of specified tablet according to delete conditions,
    // once delete_data command submit success, deleted data is not visible,
    // but not actually deleted util delay_delete_time run out.
    //
    // @param [in] request specify tablet and delete conditions
    // @param [out] tablet_info_vec return tablet last status, which
    //              include version info, row count, data size, etc
    // @return OLAP_SUCCESS if submit delete_data success
    virtual Status _delete_data(const TPushReq& request, vector<TTabletInfo>* tablet_info_vec);

    Status _get_tmp_file_dir(const std::string& root_path, std::string* local_path);
    Status _push(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec);
    void _get_file_name_from_path(const std::string& file_path, std::string* file_name);

    bool _is_init = false;
    TPushReq& _push_req;
    std::vector<TTabletInfo>* _tablet_infos;
    int64_t _signature;
    Status _download_status;
    Status* _res_status;
    std::string _remote_file_path;
    std::string _local_file_path;
    std::shared_ptr<MemTracker> _mem_tracker;
}; // class EngineBatchLoadTask
} // namespace doris
#endif // DORIS_BE_SRC_OLAP_TASK_ENGINE_BATCH_LOAD_TASK_H
