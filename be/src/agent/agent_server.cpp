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

#include "agent/agent_server.h"

#include <filesystem>
#include <string>

#include "agent/task_worker_pool.h"
#include "agent/topic_subscriber.h"
#include "agent/user_resource_listener.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "olap/snapshot_manager.h"
#include "runtime/etl_job_mgr.h"

using std::string;
using std::vector;

namespace doris {

AgentServer::AgentServer(ExecEnv* exec_env, const TMasterInfo& master_info)
        : _exec_env(exec_env), _master_info(master_info), _topic_subscriber(new TopicSubscriber()) {
    for (auto& path : exec_env->store_paths()) {
        try {
            string dpp_download_path_str = path.path + DPP_PREFIX;
            std::filesystem::path dpp_download_path(dpp_download_path_str);
            if (std::filesystem::exists(dpp_download_path)) {
                std::filesystem::remove_all(dpp_download_path);
            }
        } catch (...) {
            LOG(WARNING) << "boost exception when remove dpp download path. path=" << path.path;
        }
    }

    // It is the same code to create workers of each type, so we use a macro
    // to make code to be more readable.

#ifndef BE_TEST
#define CREATE_AND_START_POOL(type, pool_name)                                                 \
    pool_name.reset(                                                                           \
            new TaskWorkerPool(TaskWorkerPool::TaskWorkerType::type, _exec_env, master_info,   \
                               TaskWorkerPool::ThreadModel::MULTI_THREADS));                   \
    pool_name->start();

#define CREATE_AND_START_THREAD(type, pool_name)                                               \
    pool_name.reset(                                                                           \
            new TaskWorkerPool(TaskWorkerPool::TaskWorkerType::type, _exec_env, master_info,   \
                               TaskWorkerPool::ThreadModel::SINGLE_THREAD));                   \
    pool_name->start();
#else
#define CREATE_AND_START_POOL(type, pool_name)
#define CREATE_AND_START_THREAD(type, pool_name)
#endif // BE_TEST

    CREATE_AND_START_POOL(CREATE_TABLE, _create_tablet_workers);
    CREATE_AND_START_POOL(DROP_TABLE, _drop_tablet_workers);
    // Both PUSH and REALTIME_PUSH type use _push_workers
    CREATE_AND_START_POOL(PUSH, _push_workers);
    CREATE_AND_START_POOL(PUBLISH_VERSION, _publish_version_workers);
    CREATE_AND_START_POOL(CLEAR_TRANSACTION_TASK, _clear_transaction_task_workers);
    CREATE_AND_START_POOL(DELETE, _delete_workers);
    CREATE_AND_START_POOL(ALTER_TABLE, _alter_tablet_workers);
    CREATE_AND_START_POOL(CLONE, _clone_workers);
    CREATE_AND_START_POOL(STORAGE_MEDIUM_MIGRATE, _storage_medium_migrate_workers);
    CREATE_AND_START_POOL(CHECK_CONSISTENCY, _check_consistency_workers);
    CREATE_AND_START_POOL(UPLOAD, _upload_workers);
    CREATE_AND_START_POOL(DOWNLOAD, _download_workers);
    CREATE_AND_START_POOL(MAKE_SNAPSHOT, _make_snapshot_workers);
    CREATE_AND_START_POOL(RELEASE_SNAPSHOT, _release_snapshot_workers);
    CREATE_AND_START_POOL(MOVE, _move_dir_workers);
    CREATE_AND_START_POOL(UPDATE_TABLET_META_INFO, _update_tablet_meta_info_workers);

    CREATE_AND_START_THREAD(REPORT_TASK, _report_task_workers);
    CREATE_AND_START_THREAD(REPORT_DISK_STATE, _report_disk_state_workers);
    CREATE_AND_START_THREAD(REPORT_OLAP_TABLE, _report_tablet_workers);
    CREATE_AND_START_POOL(SUBMIT_TABLE_COMPACTION, _submit_table_compaction_workers);
#undef CREATE_AND_START_POOL
#undef CREATE_AND_START_THREAD

#ifndef BE_TEST
    // Add subscriber here and register listeners
    TopicListener* user_resource_listener = new UserResourceListener(exec_env, master_info);
    LOG(INFO) << "Register user resource listener";
    _topic_subscriber->register_listener(doris::TTopicType::type::RESOURCE, user_resource_listener);
#endif
}

AgentServer::~AgentServer() {}

// TODO(lingbin): each task in the batch may have it own status or FE must check and
// resend request when something is wrong(BE may need some logic to guarantee idempotence.
void AgentServer::submit_tasks(TAgentResult& agent_result,
                               const std::vector<TAgentTaskRequest>& tasks) {
    Status ret_st;

    // TODO check master_info here if it is the same with that of heartbeat rpc
    if (_master_info.network_address.hostname == "" || _master_info.network_address.port == 0) {
        Status ret_st = Status::Cancelled("Have not get FE Master heartbeat yet");
        ret_st.to_thrift(&agent_result.status);
        return;
    }

    for (auto task : tasks) {
        VLOG_RPC << "submit one task: " << apache::thrift::ThriftDebugString(task).c_str();
        TTaskType::type task_type = task.task_type;
        int64_t signature = task.signature;

#define HANDLE_TYPE(t_task_type, work_pool, req_member)                         \
    case t_task_type:                                                           \
        if (task.__isset.req_member) {                                          \
            work_pool->submit_task(task);                                       \
        } else {                                                                \
            ret_st = Status::InvalidArgument(strings::Substitute(               \
                    "task(signature=$0) has wrong request member", signature)); \
        }                                                                       \
        break;

        // TODO(lingbin): It still too long, divided these task types into several categories
        switch (task_type) {
            HANDLE_TYPE(TTaskType::CREATE, _create_tablet_workers, create_tablet_req);
            HANDLE_TYPE(TTaskType::DROP, _drop_tablet_workers, drop_tablet_req);
            HANDLE_TYPE(TTaskType::PUBLISH_VERSION, _publish_version_workers, publish_version_req);
            HANDLE_TYPE(TTaskType::CLEAR_TRANSACTION_TASK, _clear_transaction_task_workers,
                        clear_transaction_task_req);
            HANDLE_TYPE(TTaskType::CLONE, _clone_workers, clone_req);
            HANDLE_TYPE(TTaskType::STORAGE_MEDIUM_MIGRATE, _storage_medium_migrate_workers,
                        storage_medium_migrate_req);
            HANDLE_TYPE(TTaskType::CHECK_CONSISTENCY, _check_consistency_workers,
                        check_consistency_req);
            HANDLE_TYPE(TTaskType::UPLOAD, _upload_workers, upload_req);
            HANDLE_TYPE(TTaskType::DOWNLOAD, _download_workers, download_req);
            HANDLE_TYPE(TTaskType::MAKE_SNAPSHOT, _make_snapshot_workers, snapshot_req);
            HANDLE_TYPE(TTaskType::RELEASE_SNAPSHOT, _release_snapshot_workers,
                        release_snapshot_req);
            HANDLE_TYPE(TTaskType::MOVE, _move_dir_workers, move_dir_req);
            HANDLE_TYPE(TTaskType::UPDATE_TABLET_META_INFO, _update_tablet_meta_info_workers,
                        update_tablet_meta_info_req);
            HANDLE_TYPE(TTaskType::COMPACTION, _submit_table_compaction_workers, compaction_req);

        case TTaskType::REALTIME_PUSH:
        case TTaskType::PUSH:
            if (!task.__isset.push_req) {
                ret_st = Status::InvalidArgument(strings::Substitute(
                        "task(signature=$0) has wrong request member", signature));
                break;
            }
            if (task.push_req.push_type == TPushType::LOAD ||
                task.push_req.push_type == TPushType::LOAD_DELETE ||
                task.push_req.push_type == TPushType::LOAD_V2) {
                _push_workers->submit_task(task);
            } else if (task.push_req.push_type == TPushType::DELETE) {
                _delete_workers->submit_task(task);
            } else {
                ret_st = Status::InvalidArgument(strings::Substitute(
                        "task(signature=$0, type=$1, push_type=$2) has wrong push_type", signature,
                        task_type, task.push_req.push_type));
            }
            break;
        case TTaskType::ALTER:
            if (task.__isset.alter_tablet_req || task.__isset.alter_tablet_req_v2) {
                _alter_tablet_workers->submit_task(task);
            } else {
                ret_st = Status::InvalidArgument(strings::Substitute(
                        "task(signature=$0) has wrong request member", signature));
            }
            break;
        default:
            ret_st = Status::InvalidArgument(strings::Substitute(
                    "task(signature=$0, type=$1) has wrong task type", signature, task_type));
            break;
        }
#undef HANDLE_TYPE

        if (!ret_st.ok()) {
            LOG(WARNING) << "fail to submit task. reason: " << ret_st.get_error_msg()
                         << ", task: " << task;
            // For now, all tasks in the batch share one status, so if any task
            // was failed to submit, we can only return error to FE(even when some
            // tasks have already been successfully submitted).
            // However, Fe does not check the return status of submit_tasks() currently,
            // and it is not sure that FE will retry when something is wrong, so here we
            // only print an warning log and go on(i.e. do not break current loop),
            // to ensure every task can be submitted once. It is OK for now, because the
            // ret_st can be error only when it encounters an wrong task_type and
            // req-member in TAgentTaskRequest, which is basically impossible.
            // TODO(lingbin): check the logic in FE again later.
        }
    }

    ret_st.to_thrift(&agent_result.status);
}

void AgentServer::make_snapshot(TAgentResult& t_agent_result,
                                const TSnapshotRequest& snapshot_request) {
    Status ret_st;
    string snapshot_path;
    bool allow_incremental_clone = false;
    OLAPStatus err_code =
            SnapshotManager::instance()->make_snapshot(snapshot_request, &snapshot_path, &allow_incremental_clone);
    if (err_code != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to make_snapshot. tablet_id=" << snapshot_request.tablet_id
                     << ", schema_hash=" << snapshot_request.schema_hash
                     << ", error_code=" << err_code;
        ret_st = Status::RuntimeError(
                strings::Substitute("fail to make_snapshot. err_code=$0", err_code));
    } else {
        LOG(INFO) << "success to make_snapshot. tablet_id=" << snapshot_request.tablet_id
                  << ", schema_hash=" << snapshot_request.schema_hash
                  << ", snapshot_path: " << snapshot_path;
        t_agent_result.__set_snapshot_path(snapshot_path);
        t_agent_result.__set_allow_incremental_clone(allow_incremental_clone);
    }

    ret_st.to_thrift(&t_agent_result.status);
    t_agent_result.__set_snapshot_version(snapshot_request.preferred_snapshot_version);
}

void AgentServer::release_snapshot(TAgentResult& t_agent_result, const std::string& snapshot_path) {
    Status ret_st;
    OLAPStatus err_code = SnapshotManager::instance()->release_snapshot(snapshot_path);
    if (err_code != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to release_snapshot. snapshot_path: " << snapshot_path
                     << ", err_code: " << err_code;
        ret_st = Status::RuntimeError(
                strings::Substitute("fail to release_snapshot. err_code=$0", err_code));
    } else {
        LOG(INFO) << "success to release_snapshot. snapshot_path=" << snapshot_path
                  << ", err_code=" << err_code;
    }
    ret_st.to_thrift(&t_agent_result.status);
}

void AgentServer::publish_cluster_state(TAgentResult& t_agent_result,
                                        const TAgentPublishRequest& request) {
    Status status = Status::NotSupported("deprecated method(publish_cluster_state) was invoked");
    status.to_thrift(&t_agent_result.status);
}

void AgentServer::submit_etl_task(TAgentResult& t_agent_result,
                                  const TMiniLoadEtlTaskRequest& request) {
    Status status = _exec_env->etl_job_mgr()->start_job(request);
    auto fragment_instance_id = request.params.params.fragment_instance_id;
    if (status.ok()) {
        VLOG_RPC << "success to submit etl task. id=" << fragment_instance_id;
    } else {
        VLOG_RPC << "fail to submit etl task. id=" << fragment_instance_id
                 << ", err_msg=" << status.get_error_msg();
    }
    status.to_thrift(&t_agent_result.status);
}

void AgentServer::get_etl_status(TMiniLoadEtlStatusResult& t_agent_result,
                                 const TMiniLoadEtlStatusRequest& request) {
    Status status = _exec_env->etl_job_mgr()->get_job_state(request.mini_load_id, &t_agent_result);
    if (!status.ok()) {
        LOG(WARNING) << "fail to get job state. [id=" << request.mini_load_id << "]";
    }

    VLOG_RPC << "success to get job state. [id=" << request.mini_load_id
             << ", status=" << t_agent_result.status.status_code
             << ", etl_state=" << t_agent_result.etl_state << ", files=";
    for (auto& item : t_agent_result.file_map) {
        VLOG_RPC << item.first << ":" << item.second << ";";
    }
    VLOG_RPC << "]";
}

void AgentServer::delete_etl_files(TAgentResult& t_agent_result,
                                   const TDeleteEtlFilesRequest& request) {
    Status status = _exec_env->etl_job_mgr()->erase_job(request);
    if (!status.ok()) {
        LOG(WARNING) << "fail to delete etl files. because " << status.get_error_msg()
                     << " with request " << request;
    }

    VLOG_RPC << "success to delete etl files. request=" << request;
    status.to_thrift(&t_agent_result.status);
}

} // namespace doris
