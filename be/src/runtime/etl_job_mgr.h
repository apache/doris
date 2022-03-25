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

#ifndef DORIS_BE_RUNTIME_ETL_JOB_MGR_H
#define DORIS_BE_RUNTIME_ETL_JOB_MGR_H

#include <pthread.h>

#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "http/rest_monitor_iface.h"
#include "util/hash_util.hpp"
#include "util/lru_cache.hpp"

namespace doris {

// used to report to master
struct EtlJobResult {
    EtlJobResult() : process_normal_rows(0), process_abnormal_rows(0) {}
    std::string debug_path;
    std::map<std::string, int64_t> file_map;
    int64_t process_normal_rows;
    int64_t process_abnormal_rows;
};

// used to report to master
struct EtlJobCtx {
    Status finish_status;
    EtlJobResult result;
};

class TMiniLoadEtlStatusResult;
class TMiniLoadEtlTaskRequest;
class ExecEnv;
class PlanFragmentExecutor;
class TDeleteEtlFilesRequest;

// manager of all the Etl job
// used this because master may loop be to check if a load job is finished.
class EtlJobMgr : public RestMonitorIface {
public:
    EtlJobMgr(ExecEnv* exec_env);

    virtual ~EtlJobMgr();

    // make trash directory for collect
    Status init();

    // Make a job to running state
    // If this job is successful, return OK
    // If this job is failed, move this job from _failed_jobs to _running_jobs
    // Otherwise, put it to _running_jobs
    Status start_job(const TMiniLoadEtlTaskRequest& req);

    // Make a running job to failed job
    Status cancel_job(const TUniqueId& id);

    Status finish_job(const TUniqueId& id, const Status& finish_status, const EtlJobResult& result);

    Status get_job_state(const TUniqueId& id, TMiniLoadEtlStatusResult* result);

    Status erase_job(const TDeleteEtlFilesRequest& req);

    void finalize_job(PlanFragmentExecutor* executor);

    virtual void debug(std::stringstream& ss);

private:
    std::string to_http_path(const std::string& file_path);
    std::string to_load_error_http_path(const std::string& file_path);

    void report_to_master(PlanFragmentExecutor* executor);

    ExecEnv* _exec_env;
    std::mutex _lock;
    std::unordered_set<TUniqueId> _running_jobs;
    LruCache<TUniqueId, EtlJobCtx> _success_jobs;
    LruCache<TUniqueId, EtlJobCtx> _failed_jobs;
};

} // namespace doris

#endif
