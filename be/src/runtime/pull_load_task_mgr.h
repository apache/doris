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
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "util/hash_util.hpp"

namespace doris {

class PullLoadTaskCtx;
class TPullLoadSubTaskInfo;
class TFetchPullLoadTaskInfoResult;
class TFetchAllPullLoadTaskInfosResult;

// Pull load task manager, used for 
class PullLoadTaskMgr {
public:
    PullLoadTaskMgr(const std::string& dir);
    ~PullLoadTaskMgr();

    // Initialize pull load task manager, recovery task context from file.
    Status init();

    // Register one pull load task in this manager.
    Status register_task(const TUniqueId& id, int num_senders);

    // Deregister one pull load task, no need to 
    Status deregister_task(const TUniqueId& id);

    // Called by network service when one sub-task has been finished
    Status report_sub_task_info(const TPullLoadSubTaskInfo& sub_task_info);

    // Fetch task's information with task id
    Status fetch_task_info(const TUniqueId& tid, 
                           TFetchPullLoadTaskInfoResult* result);

    // Fetch all task informations
    Status fetch_all_task_infos(TFetchAllPullLoadTaskInfosResult* result);

private:
    // Load all tasks from files
    Status load_task_ctxes();

    // Generate file path through task id
    std::string task_file_path(const TUniqueId& id) const;
    bool is_valid_task_file(const std::string& file_name) const;

    // Save task contex to task information file.
    Status save_task_ctx(PullLoadTaskCtx* task_ctx);

    // Load task contex from file
    Status load_task_ctx(const std::string& file_path);

    std::string _path;
    mutable std::mutex _lock;
    bool _dir_exist;
    typedef std::unordered_map<TUniqueId, std::shared_ptr<PullLoadTaskCtx>> TaskCtxMap;
    TaskCtxMap _task_ctx_map;
};

}
