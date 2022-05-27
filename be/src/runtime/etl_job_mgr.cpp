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

#include "runtime/etl_job_mgr.h"

#include <filesystem>
#include <functional>

#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/Status_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/file_utils.h"
#include "util/uid_util.h"

namespace doris {

#define VLOG_ETL VLOG_CRITICAL

std::string EtlJobMgr::to_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_download_load?"
        << "token=" << _exec_env->token() << "&file=" << file_name;
    return url.str();
}

std::string EtlJobMgr::to_load_error_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_load_error_log?"
        << "file=" << file_name;
    return url.str();
}

const std::string DPP_NORMAL_ALL = "dpp.norm.ALL";
const std::string DPP_ABNORMAL_ALL = "dpp.abnorm.ALL";
const std::string ERROR_FILE_PREFIX = "error_log";

EtlJobMgr::EtlJobMgr(ExecEnv* exec_env)
        : _exec_env(exec_env), _success_jobs(5000), _failed_jobs(5000) {}

EtlJobMgr::~EtlJobMgr() {}

Status EtlJobMgr::init() {
    return Status::OK();
}

Status EtlJobMgr::start_job(const TMiniLoadEtlTaskRequest& req) {
    const TUniqueId& id = req.params.params.fragment_instance_id;
    std::lock_guard<std::mutex> l(_lock);
    auto it = _running_jobs.find(id);
    if (it != _running_jobs.end()) {
        // Already have this job, return what???
        LOG(INFO) << "Duplicated etl job(" << id << ")";
        return Status::OK();
    }

    // If already success, we return Status::OK()
    // and wait master ask me success information
    if (_success_jobs.exists(id)) {
        // Already success
        LOG(INFO) << "Already successful etl job(" << id << ")";
        return Status::OK();
    }

    RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(
            req.params, std::bind<void>(&EtlJobMgr::finalize_job, this, std::placeholders::_1)));

    // redo this job if failed before
    if (_failed_jobs.exists(id)) {
        _failed_jobs.erase(id);
    }

    VLOG_ETL << "Job id(" << id << ") insert to EtlJobMgr.";
    _running_jobs.insert(id);

    return Status::OK();
}

void EtlJobMgr::report_to_master(PlanFragmentExecutor* executor) {
    TUpdateMiniEtlTaskStatusRequest request;
    RuntimeState* state = executor->runtime_state();
    request.protocolVersion = FrontendServiceVersion::V1;
    request.etlTaskId = state->fragment_instance_id();
    Status status = get_job_state(state->fragment_instance_id(), &request.etlTaskStatus);
    if (!status.ok()) {
        return;
    }
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    FrontendServiceConnection client(_exec_env->frontend_client_cache(), master_address,
                                     config::thrift_rpc_timeout_ms, &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "Connect master failed, with address(" << master_address.hostname << ":"
           << master_address.port << ")";
        LOG(WARNING) << ss.str();
        return;
    }
    TFeResult res;
    try {
        try {
            client->updateMiniEtlTaskStatus(res, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Retrying report etl jobs status to master(" << master_address.hostname
                         << ":" << master_address.port << ") because: " << e.what();
            status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!status.ok()) {
                LOG(WARNING) << "Client repoen failed. with address(" << master_address.hostname
                             << ":" << master_address.port << ")";
                return;
            }
            client->updateMiniEtlTaskStatus(res, request);
        }
    } catch (apache::thrift::TException& e) {
        // failed when retry.
        // reopen to disable this connection
        client.reopen(config::thrift_rpc_timeout_ms);
        std::stringstream ss;
        ss << "Report etl task to master(" << master_address.hostname << ":" << master_address.port
           << ") failed because: " << e.what();
        LOG(WARNING) << ss.str();
    }
    // TODO(lingbin): check status of 'res' here.
    // because there are some checks in updateMiniEtlTaskStatus, for example max_filter_ratio.
    LOG(INFO) << "Successfully report elt job status to master.id=" << print_id(request.etlTaskId);
}

void EtlJobMgr::finalize_job(PlanFragmentExecutor* executor) {
    EtlJobResult result;

    RuntimeState* state = executor->runtime_state();
    if (executor->status().ok()) {
        // Get files
        for (auto& it : state->output_files()) {
            int64_t file_size = std::filesystem::file_size(it);
            result.file_map[to_http_path(it)] = file_size;
        }
        // set statistics
        result.process_normal_rows = state->num_rows_load_success();
        result.process_abnormal_rows = state->num_rows_load_filtered();
    } else {
        // get debug path
        result.process_normal_rows = state->num_rows_load_success();
        result.process_abnormal_rows = state->num_rows_load_filtered();
    }

    result.debug_path = state->get_error_log_file_path();

    finish_job(state->fragment_instance_id(), executor->status(), result);

    // Try to report this finished task to master
    report_to_master(executor);
}

Status EtlJobMgr::cancel_job(const TUniqueId& id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _running_jobs.find(id);
    if (it == _running_jobs.end()) {
        // Nothing to do
        LOG(INFO) << "No such job id, just print to info " << id;
        return Status::OK();
    }
    _running_jobs.erase(it);
    VLOG_ETL << "id(" << id << ") have been removed from EtlJobMgr.";
    EtlJobCtx job_ctx;
    job_ctx.finish_status = Status::Cancelled("Cancelled");
    _failed_jobs.put(id, job_ctx);
    return Status::OK();
}

Status EtlJobMgr::finish_job(const TUniqueId& id, const Status& finish_status,
                             const EtlJobResult& result) {
    std::lock_guard<std::mutex> l(_lock);

    auto it = _running_jobs.find(id);
    if (it == _running_jobs.end()) {
        std::stringstream ss;
        ss << "Unknown job id(" << id << ").";
        return Status::InternalError(ss.str());
    }
    _running_jobs.erase(it);

    EtlJobCtx ctx;
    ctx.finish_status = finish_status;
    ctx.result = result;
    if (finish_status.ok()) {
        _success_jobs.put(id, ctx);
    } else {
        _failed_jobs.put(id, ctx);
    }

    VLOG_ETL << "Move job(" << id << ") from running to "
             << (finish_status.ok() ? "success jobs" : "failed jobs");

    return Status::OK();
}

Status EtlJobMgr::get_job_state(const TUniqueId& id, TMiniLoadEtlStatusResult* result) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _running_jobs.find(id);
    if (it != _running_jobs.end()) {
        result->status.__set_status_code(TStatusCode::OK);
        result->__set_etl_state(TEtlState::RUNNING);
        return Status::OK();
    }
    // Successful
    if (_success_jobs.exists(id)) {
        EtlJobCtx ctx;
        _success_jobs.get(id, &ctx);
        result->status.__set_status_code(TStatusCode::OK);
        result->__set_etl_state(TEtlState::FINISHED);
        result->__set_file_map(ctx.result.file_map);

        // set counter
        std::map<std::string, std::string> counter;
        counter[DPP_NORMAL_ALL] = std::to_string(ctx.result.process_normal_rows);
        counter[DPP_ABNORMAL_ALL] = std::to_string(ctx.result.process_abnormal_rows);
        result->__set_counters(counter);

        if (!ctx.result.debug_path.empty()) {
            result->__set_tracking_url(to_load_error_http_path(ctx.result.debug_path));
        }
        return Status::OK();
    }
    // failed information
    if (_failed_jobs.exists(id)) {
        EtlJobCtx ctx;
        _failed_jobs.get(id, &ctx);
        result->status.__set_status_code(TStatusCode::OK);
        result->__set_etl_state(TEtlState::CANCELLED);

        if (!ctx.result.debug_path.empty()) {
            result->__set_tracking_url(to_http_path(ctx.result.debug_path));
        }
        return Status::OK();
    }
    // NO this jobs
    result->status.__set_status_code(TStatusCode::OK);
    result->__set_etl_state(TEtlState::CANCELLED);
    return Status::OK();
}

Status EtlJobMgr::erase_job(const TDeleteEtlFilesRequest& req) {
    std::lock_guard<std::mutex> l(_lock);
    const TUniqueId& id = req.mini_load_id;
    auto it = _running_jobs.find(id);
    if (it != _running_jobs.end()) {
        std::stringstream ss;
        ss << "Job(" << id << ") is running, can not be deleted.";
        return Status::InternalError(ss.str());
    }
    _success_jobs.erase(id);
    _failed_jobs.erase(id);

    return Status::OK();
}

void EtlJobMgr::debug(std::stringstream& ss) {
    // Make things easy
    std::lock_guard<std::mutex> l(_lock);

    // Debug summary
    ss << "we have " << _running_jobs.size() << " jobs Running\n";
    ss << "we have " << _failed_jobs.size() << " jobs Failed\n";
    ss << "we have " << _success_jobs.size() << " jobs Successful\n";
    // Debug running jobs
    for (auto& it : _running_jobs) {
        ss << "running jobs: " << it << "\n";
    }
    // Debug success jobs
    for (auto& it : _success_jobs) {
        ss << "successful jobs: " << it.first << "\n";
    }
    // Debug failed jobs
    for (auto& it : _failed_jobs) {
        ss << "failed jobs: " << it.first << "\n";
    }
}

} // namespace doris
