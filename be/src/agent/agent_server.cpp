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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <filesystem>
#include <ostream>
#include <string>

#include "agent/task_worker_pool.h"
#include "agent/topic_subscriber.h"
#include "agent/utils.h"
#include "agent/workload_group_listener.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/snapshot_manager.h"
#include "runtime/exec_env.h"

using std::string;
using std::vector;

namespace doris {

AgentServer::AgentServer(ExecEnv* exec_env, const TMasterInfo& master_info)
        : _master_info(master_info), _topic_subscriber(new TopicSubscriber()) {
    for (auto& path : exec_env->store_paths()) {
        try {
            string dpp_download_path_str = path.path + "/" + DPP_PREFIX;
            std::filesystem::path dpp_download_path(dpp_download_path_str);
            if (std::filesystem::exists(dpp_download_path)) {
                std::filesystem::remove_all(dpp_download_path);
            }
        } catch (...) {
            LOG(WARNING) << "boost exception when remove dpp download path. path=" << path.path;
        }
    }

    MasterServerClient::create(master_info);

    // It is the same code to create workers of each type, so we use a macro
    // to make code to be more readable.

#ifndef BE_TEST
#define CREATE_AND_START_POOL(type, pool_name)                                                    \
    pool_name.reset(new TaskWorkerPool(TaskWorkerPool::TaskWorkerType::type, exec_env,            \
                                       master_info, TaskWorkerPool::ThreadModel::MULTI_THREADS)); \
    pool_name->start();

#define CREATE_AND_START_THREAD(type, pool_name)                                                  \
    pool_name.reset(new TaskWorkerPool(TaskWorkerPool::TaskWorkerType::type, exec_env,            \
                                       master_info, TaskWorkerPool::ThreadModel::SINGLE_THREAD)); \
    pool_name->start();
#else
#define CREATE_AND_START_POOL(type, pool_name)
#define CREATE_AND_START_THREAD(type, pool_name)
#endif // BE_TEST

#ifndef BE_TEST
    _create_tablet_workers.reset(
            new CreateTableTaskPool(exec_env, TaskWorkerPool::ThreadModel::MULTI_THREADS));
    _create_tablet_workers->start();
    _drop_tablet_workers.reset(
            new DropTableTaskPool(exec_env, TaskWorkerPool::ThreadModel::MULTI_THREADS));
    _drop_tablet_workers->start();

    // Both PUSH and REALTIME_PUSH type use _push_load_workers
    _push_load_workers.reset(new PushTaskPool(exec_env, TaskWorkerPool::ThreadModel::MULTI_THREADS,
                                              PushTaskPool::PushWokerType::LOAD_V2));
    _push_load_workers->start();
    _publish_version_workers.reset(
            new PublishVersionTaskPool(exec_env, TaskWorkerPool::ThreadModel::MULTI_THREADS));
    _publish_version_workers->start();
    _clear_transaction_task_workers.reset(
            new ClearTransactionTaskPool(exec_env, TaskWorkerPool::ThreadModel::MULTI_THREADS));
    _clear_transaction_task_workers->start();
    _push_delete_workers.reset(new PushTaskPool(exec_env,
                                                TaskWorkerPool::ThreadModel::MULTI_THREADS,
                                                PushTaskPool::PushWokerType::DELETE));
    _push_delete_workers->start();
    _alter_tablet_workers.reset(
            new AlterTableTaskPool(exec_env, TaskWorkerPool::ThreadModel::MULTI_THREADS));
    _alter_tablet_workers->start();
    _clone_workers.reset(new CloneTaskPool(exec_env, TaskWorkerPool::ThreadModel::MULTI_THREADS));
    _clone_workers->start();
    _storage_medium_migrate_workers.reset(
            new StorageMediumMigrateTaskPool(exec_env, TaskWorkerPool::ThreadModel::MULTI_THREADS));
    _storage_medium_migrate_workers->start();
#endif
    CREATE_AND_START_POOL(ALTER_INVERTED_INDEX, _alter_inverted_index_workers);
    CREATE_AND_START_POOL(CHECK_CONSISTENCY, _check_consistency_workers);
    CREATE_AND_START_POOL(UPLOAD, _upload_workers);
    CREATE_AND_START_POOL(DOWNLOAD, _download_workers);
    CREATE_AND_START_POOL(MAKE_SNAPSHOT, _make_snapshot_workers);
    CREATE_AND_START_POOL(RELEASE_SNAPSHOT, _release_snapshot_workers);
    CREATE_AND_START_POOL(MOVE, _move_dir_workers);
    CREATE_AND_START_POOL(UPDATE_TABLET_META_INFO, _update_tablet_meta_info_workers);
    CREATE_AND_START_THREAD(PUSH_COOLDOWN_CONF, _push_cooldown_conf_workers);

    CREATE_AND_START_THREAD(REPORT_TASK, _report_task_workers);
    CREATE_AND_START_THREAD(REPORT_DISK_STATE, _report_disk_state_workers);
    CREATE_AND_START_THREAD(REPORT_OLAP_TABLE, _report_tablet_workers);
    CREATE_AND_START_POOL(SUBMIT_TABLE_COMPACTION, _submit_table_compaction_workers);
    CREATE_AND_START_POOL(PUSH_STORAGE_POLICY, _push_storage_policy_workers);
    CREATE_AND_START_THREAD(GC_BINLOG, _gc_binlog_workers);
#undef CREATE_AND_START_POOL
#undef CREATE_AND_START_THREAD

#if !defined(BE_TEST) && !defined(__APPLE__)
    // Add subscriber here and register listeners
    std::unique_ptr<TopicListener> wg_listener = std::make_unique<WorkloadGroupListener>(exec_env);
    LOG(INFO) << "Register workload group listener";
    _topic_subscriber->register_listener(doris::TTopicInfoType::type::WORKLOAD_GROUP,
                                         std::move(wg_listener));
#endif
}

AgentServer::~AgentServer() {}

// TODO(lingbin): each task in the batch may have it own status or FE must check and
// resend request when something is wrong(BE may need some logic to guarantee idempotence.
void AgentServer::submit_tasks(TAgentResult& agent_result,
                               const std::vector<TAgentTaskRequest>& tasks) {
    Status ret_st;

    // TODO check master_info here if it is the same with that of heartbeat rpc
    if (_master_info.network_address.hostname.empty() || _master_info.network_address.port == 0) {
        Status ret_st = Status::Cancelled("Have not get FE Master heartbeat yet");
        ret_st.to_thrift(&agent_result.status);
        return;
    }

    for (auto task : tasks) {
        VLOG_RPC << "submit one task: " << apache::thrift::ThriftDebugString(task).c_str();
        TTaskType::type task_type = task.task_type;
        int64_t signature = task.signature;

#define HANDLE_TYPE(t_task_type, work_pool, req_member)                                          \
    case t_task_type:                                                                            \
        if (task.__isset.req_member) {                                                           \
            work_pool->submit_task(task);                                                        \
        } else {                                                                                 \
            ret_st = Status::InvalidArgument("task(signature={}) has wrong request member = {}", \
                                             signature, #req_member);                            \
        }                                                                                        \
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
            HANDLE_TYPE(TTaskType::PUSH_STORAGE_POLICY, _push_storage_policy_workers,
                        push_storage_policy_req);

        case TTaskType::REALTIME_PUSH:
        case TTaskType::PUSH:
            if (!task.__isset.push_req) {
                ret_st = Status::InvalidArgument(
                        "task(signature={}) has wrong request member = push_req", signature);
                break;
            }
            if (task.push_req.push_type == TPushType::LOAD_V2) {
                _push_load_workers->submit_task(task);
            } else if (task.push_req.push_type == TPushType::DELETE) {
                _push_delete_workers->submit_task(task);
            } else {
                ret_st = Status::InvalidArgument(
                        "task(signature={}, type={}, push_type={}) has wrong push_type", signature,
                        task_type, task.push_req.push_type);
            }
            break;
        case TTaskType::ALTER:
            if (task.__isset.alter_tablet_req || task.__isset.alter_tablet_req_v2) {
                _alter_tablet_workers->submit_task(task);
            } else {
                ret_st = Status::InvalidArgument(
                        "task(signature={}) has wrong request member = alter_tablet_req",
                        signature);
            }
            break;
        case TTaskType::ALTER_INVERTED_INDEX:
            if (task.__isset.alter_inverted_index_req) {
                _alter_inverted_index_workers->submit_task(task);
            } else {
                ret_st = Status::InvalidArgument(strings::Substitute(
                        "task(signature=$0) has wrong request member = alter_inverted_index_req",
                        signature));
            }
            break;
        case TTaskType::PUSH_COOLDOWN_CONF:
            if (task.__isset.push_cooldown_conf) {
                _push_cooldown_conf_workers->submit_task(task);
            } else {
                ret_st = Status::InvalidArgument(
                        "task(signature={}) has wrong request member = push_cooldown_conf",
                        signature);
            }
            break;
        case TTaskType::GC_BINLOG:
            if (task.__isset.gc_binlog_req) {
                _gc_binlog_workers->submit_task(task);
            } else {
                ret_st = Status::InvalidArgument(
                        "task(signature={}) has wrong request member = gc_binlog_req", signature);
            }
            break;
        default:
            ret_st = Status::InvalidArgument("task(signature={}, type={}) has wrong task type",
                                             signature, task_type);
            break;
        }
#undef HANDLE_TYPE

        if (!ret_st.ok()) {
            LOG_WARNING("failed to submit task").tag("task", task).error(ret_st);
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
    string snapshot_path;
    bool allow_incremental_clone = false;
    Status status = SnapshotManager::instance()->make_snapshot(snapshot_request, &snapshot_path,
                                                               &allow_incremental_clone);
    if (!status) {
        LOG_WARNING("failed to make snapshot")
                .tag("tablet_id", snapshot_request.tablet_id)
                .tag("schema_hash", snapshot_request.schema_hash)
                .error(status);
    } else {
        LOG_INFO("successfully make snapshot")
                .tag("tablet_id", snapshot_request.tablet_id)
                .tag("schema_hash", snapshot_request.schema_hash)
                .tag("snapshot_path", snapshot_path);
        t_agent_result.__set_snapshot_path(snapshot_path);
        t_agent_result.__set_allow_incremental_clone(allow_incremental_clone);
    }

    status.to_thrift(&t_agent_result.status);
    t_agent_result.__set_snapshot_version(snapshot_request.preferred_snapshot_version);
}

void AgentServer::release_snapshot(TAgentResult& t_agent_result, const std::string& snapshot_path) {
    Status status = SnapshotManager::instance()->release_snapshot(snapshot_path);
    if (!status) {
        LOG_WARNING("failed to release snapshot").tag("snapshot_path", snapshot_path).error(status);
    } else {
        LOG_INFO("successfully release snapshot").tag("snapshot_path", snapshot_path);
    }
    status.to_thrift(&t_agent_result.status);
}

void AgentServer::publish_cluster_state(TAgentResult& t_agent_result,
                                        const TAgentPublishRequest& request) {
    Status status = Status::NotSupported("deprecated method(publish_cluster_state) was invoked");
    status.to_thrift(&t_agent_result.status);
}

} // namespace doris
