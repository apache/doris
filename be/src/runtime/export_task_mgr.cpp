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

#include "runtime/export_task_mgr.h"

#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

namespace doris {

#define VLOG_EXPORT VLOG_CRITICAL

static size_t LRU_MAX_CASH_TASK_NUM = 1000;

ExportTaskMgr::ExportTaskMgr(ExecEnv* exec_env)
        : _exec_env(exec_env),
          _success_tasks(LRU_MAX_CASH_TASK_NUM),
          _failed_tasks(LRU_MAX_CASH_TASK_NUM) {}

ExportTaskMgr::~ExportTaskMgr() {}

Status ExportTaskMgr::init() {
    return Status::OK();
}

Status ExportTaskMgr::start_task(const TExportTaskRequest& request) {
    const TUniqueId& id = request.params.params.fragment_instance_id;
    std::lock_guard<std::mutex> l(_lock);
    auto it = _running_tasks.find(id);
    if (it != _running_tasks.end()) {
        // Already have this task, return what???
        LOG(INFO) << "Duplicated export task(" << id << ")";
        return Status::OK();
    }

    // If already success, we return Status::OK()
    // and wait master ask me success information
    if (_success_tasks.exists(id)) {
        // Already success
        LOG(INFO) << "Already successful export task(" << id << ")";
        return Status::OK();
    }

    RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(
            request.params,
            std::bind<void>(&ExportTaskMgr::finalize_task, this, std::placeholders::_1)));

    // redo this task if failed before
    if (_failed_tasks.exists(id)) {
        _failed_tasks.erase(id);
    }

    VLOG_EXPORT << "accept one export Task. id=" << id;
    _running_tasks.insert(id);

    return Status::OK();
}

Status ExportTaskMgr::cancel_task(const TUniqueId& id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _running_tasks.find(id);
    if (it == _running_tasks.end()) {
        // Nothing to do
        LOG(INFO) << "No such export task id, just print to info " << id;
        return Status::OK();
    }
    _running_tasks.erase(it);
    VLOG_EXPORT << "task id(" << id << ") have been removed from ExportTaskMgr.";
    ExportTaskCtx ctx;
    ctx.status = Status::Cancelled("Cancelled");
    _failed_tasks.put(id, ctx);
    return Status::OK();
}

Status ExportTaskMgr::erase_task(const TUniqueId& id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _running_tasks.find(id);
    if (it != _running_tasks.end()) {
        std::stringstream ss;
        ss << "Task(" << id << ") is running, can not be deleted.";
        return Status::InternalError(ss.str());
    }
    _success_tasks.erase(id);
    _failed_tasks.erase(id);

    return Status::OK();
}

void ExportTaskMgr::finalize_task(PlanFragmentExecutor* executor) {
    ExportTaskResult result;

    RuntimeState* state = executor->runtime_state();

    if (executor->status().ok()) {
        result.files = state->export_output_files();
    }

    finish_task(state->fragment_instance_id(), executor->status(), result);

    // Try to report this finished task to master
    report_to_master(executor);
}

Status ExportTaskMgr::finish_task(const TUniqueId& id, const Status& status,
                                  const ExportTaskResult& result) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _running_tasks.find(id);
    if (it == _running_tasks.end()) {
        std::stringstream ss;
        ss << "Unknown task id(" << id << ").";
        return Status::InternalError(ss.str());
    }
    _running_tasks.erase(it);

    ExportTaskCtx ctx;
    ctx.status = status;
    ctx.result = result;
    if (status.ok()) {
        _success_tasks.put(id, ctx);
    } else {
        _failed_tasks.put(id, ctx);
    }

    VLOG_EXPORT << "Move task(" << id << ") from running to "
                << (status.ok() ? "success tasks" : "failed tasks");

    return Status::OK();
}

Status ExportTaskMgr::get_task_state(const TUniqueId& id, TExportStatusResult* result) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _running_tasks.find(id);
    if (it != _running_tasks.end()) {
        result->status.__set_status_code(TStatusCode::OK);
        result->__set_state(TExportState::RUNNING);
        return Status::OK();
    }

    // Successful
    if (_success_tasks.exists(id)) {
        ExportTaskCtx ctx;
        _success_tasks.get(id, &ctx);
        result->status.__set_status_code(TStatusCode::OK);
        result->__set_state(TExportState::FINISHED);
        result->__set_files(ctx.result.files);
        return Status::OK();
    }

    // failed information
    if (_failed_tasks.exists(id)) {
        ExportTaskCtx ctx;
        _success_tasks.get(id, &ctx);
        result->status.__set_status_code(TStatusCode::OK);
        result->__set_state(TExportState::CANCELLED);
        return Status::OK();
    }

    // NO this task
    result->status.__set_status_code(TStatusCode::OK);
    result->__set_state(TExportState::CANCELLED);
    return Status::OK();
}

void ExportTaskMgr::report_to_master(PlanFragmentExecutor* executor) {
    TUpdateExportTaskStatusRequest request;
    RuntimeState* state = executor->runtime_state();
    request.protocolVersion = FrontendServiceVersion::V1;
    request.taskId = state->fragment_instance_id();
    Status status = get_task_state(state->fragment_instance_id(), &request.taskStatus);
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

    VLOG_ROW << "export updateExportTaskStatus. request  is "
             << apache::thrift::ThriftDebugString(request).c_str();

    TFeResult res;
    try {
        try {
            client->updateExportTaskStatus(res, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Retrying report export tasks status to master("
                         << master_address.hostname << ":" << master_address.port
                         << ") because: " << e.what();
            status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!status.ok()) {
                LOG(WARNING) << "Client repoen failed. with address(" << master_address.hostname
                             << ":" << master_address.port << ")";
                return;
            }
            client->updateExportTaskStatus(res, request);
        }
    } catch (apache::thrift::TException& e) {
        // failed when retry.
        // reopen to disable this connection
        client.reopen(config::thrift_rpc_timeout_ms);
        std::stringstream ss;
        ss << "Fail to report export task to master(" << master_address.hostname << ":"
           << master_address.port << "). reason: " << e.what();
        LOG(WARNING) << ss.str();
    }

    LOG(INFO) << "Successfully report elt task status to master."
              << " id=" << print_id(request.taskId);
}

} // end namespace doris
