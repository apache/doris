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

#ifndef DORIS_BE_RUNTIME_FRAGMENT_MGR_H
#define DORIS_BE_RUNTIME_FRAGMENT_MGR_H

#include <mutex>
#include <memory>
#include <unordered_map>
#include <functional>
#include <thread>
#include <vector>

#include "common/status.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/priority_thread_pool.hpp"
#include "util/hash_util.hpp"
#include "http/rest_monitor_iface.h"

namespace doris {

class ExecEnv;
class FragmentExecState;
class TExecPlanFragmentParams;
class TUniqueId;
class PlanFragmentExecutor;

std::string to_load_error_http_path(const std::string& file_name);

// This class used to manage all the fragment execute in this instance
class FragmentMgr : public RestMonitorIface {
public:
    typedef std::function<void (PlanFragmentExecutor*)> FinishCallback;

    FragmentMgr(ExecEnv* exec_env);
    virtual ~FragmentMgr();

    // execute one plan fragment
    Status exec_plan_fragment(const TExecPlanFragmentParams& params);

    // TODO(zc): report this is over
    Status exec_plan_fragment(const TExecPlanFragmentParams& params, FinishCallback cb);

    Status cancel(const TUniqueId& fragment_id) {
        return cancel(fragment_id, PPlanFragmentCancelReason::INTERNAL_ERROR);
    }

    Status cancel(const TUniqueId& fragment_id, const PPlanFragmentCancelReason& reason);

    void cancel_worker();

    virtual void debug(std::stringstream& ss);

    Status trigger_profile_report(const PTriggerProfileReportRequest* request);

    // input: TScanOpenParams fragment_instance_id
    // output: selected_columns
    // execute external query, all query info are packed in TScanOpenParams
    Status exec_external_plan_fragment(const TScanOpenParams& params, const TUniqueId& fragment_instance_id, std::vector<TScanColumnDesc>* selected_columns);

private:
    void exec_actual(std::shared_ptr<FragmentExecState> exec_state,
                     FinishCallback cb);

    // This is input params
    ExecEnv* _exec_env;

    std::mutex _lock;

    // Make sure that remove this before no data reference FragmentExecState
    std::unordered_map<TUniqueId, std::shared_ptr<FragmentExecState>> _fragment_map;

    // Cancel thread
    bool _stop;
    std::thread _cancel_thread;
    // every job is a pool
    PriorityThreadPool _thread_pool;
};

}

#endif

