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
#include <memory>
#include <ostream>
#include <string>

#include "agent/task_worker_pool.h"
#include "agent/topic_subscriber.h"
#include "agent/utils.h"
#include "agent/workload_group_listener.h"
#include "agent/workload_sched_policy_listener.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

using std::string;
using std::vector;

namespace doris {

AgentServer::AgentServer(ExecEnv* exec_env, const TMasterInfo& master_info)
        : _master_info(master_info), _topic_subscriber(new TopicSubscriber()) {
    MasterServerClient::create(master_info);

#if !defined(BE_TEST) && !defined(__APPLE__)
    // Add subscriber here and register listeners
    std::unique_ptr<TopicListener> wg_listener = std::make_unique<WorkloadGroupListener>(exec_env);
    LOG(INFO) << "Register workload group listener";
    _topic_subscriber->register_listener(doris::TTopicInfoType::type::WORKLOAD_GROUP,
                                         std::move(wg_listener));

    std::unique_ptr<TopicListener> policy_listener =
            std::make_unique<WorkloadschedPolicyListener>(exec_env);
    LOG(INFO) << "Register workload scheduler policy listener";
    _topic_subscriber->register_listener(doris::TTopicInfoType::type::WORKLOAD_SCHED_POLICY,
                                         std::move(policy_listener));

#endif
}

AgentServer::~AgentServer() = default;

class PushTaskWorkerPool final : public TaskWorkerPoolIf {
public:
    PushTaskWorkerPool(StorageEngine& engine)
            : _push_delete_workers(
                      TaskWorkerPool("DELETE", config::delete_worker_count,
                                     [&engine](auto&& task) { push_callback(engine, task); })),
              _push_load_workers(PriorTaskWorkerPool(
                      "PUSH", config::push_worker_count_normal_priority,
                      config::push_worker_count_high_priority,
                      [&engine](auto&& task) { push_callback(engine, task); })) {}

    ~PushTaskWorkerPool() override { stop(); }

    void stop() {
        _push_delete_workers.stop();
        _push_load_workers.stop();
    }

    Status submit_task(const TAgentTaskRequest& task) override {
        if (task.push_req.push_type == TPushType::LOAD_V2) {
            return _push_load_workers.submit_task(task);
        } else if (task.push_req.push_type == TPushType::DELETE) {
            return _push_delete_workers.submit_task(task);
        } else {
            return Status::InvalidArgument(
                    "task(signature={}, type={}, push_type={}) has wrong push_type", task.signature,
                    task.task_type, task.push_req.push_type);
        }
    }

private:
    TaskWorkerPool _push_delete_workers;
    PriorTaskWorkerPool _push_load_workers;
};

void AgentServer::start_workers(StorageEngine& engine, ExecEnv* exec_env) {
    for (const auto& path : exec_env->store_paths()) {
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

    // clang-format off
    _workers[TTaskType::ALTER_INVERTED_INDEX] = std::make_unique<TaskWorkerPool>(
        "ALTER_INVERTED_INDEX", config::alter_index_worker_count, [&engine](auto&& task) { return alter_inverted_index_callback(engine, task); });

    _workers[TTaskType::CHECK_CONSISTENCY] = std::make_unique<TaskWorkerPool>(
        "CHECK_CONSISTENCY", config::check_consistency_worker_count, [&engine](auto&& task) { return check_consistency_callback(engine, task); });

    _workers[TTaskType::UPLOAD] = std::make_unique<TaskWorkerPool>(
            "UPLOAD", config::upload_worker_count, [&engine, exec_env](auto&& task) { return upload_callback(engine, exec_env, task); });

    _workers[TTaskType::DOWNLOAD] = std::make_unique<TaskWorkerPool>(
            "DOWNLOAD", config::download_worker_count, [&engine, exec_env](auto&& task) { return download_callback(engine, exec_env, task); });

    _workers[TTaskType::MAKE_SNAPSHOT] = std::make_unique<TaskWorkerPool>(
            "MAKE_SNAPSHOT", config::make_snapshot_worker_count, [&engine](auto&& task) { return make_snapshot_callback(engine, task); });

    _workers[TTaskType::RELEASE_SNAPSHOT] = std::make_unique<TaskWorkerPool>(
            "RELEASE_SNAPSHOT", config::release_snapshot_worker_count, [&engine](auto&& task) { return release_snapshot_callback(engine, task); });

    _workers[TTaskType::MOVE] = std::make_unique<TaskWorkerPool>(
            "MOVE", 1, [&engine, exec_env](auto&& task) { return move_dir_callback(engine, exec_env, task); });

    _workers[TTaskType::COMPACTION] = std::make_unique<TaskWorkerPool>(
            "SUBMIT_TABLE_COMPACTION", 1, [&engine](auto&& task) { return submit_table_compaction_callback(engine, task); });

    _workers[TTaskType::PUSH_STORAGE_POLICY] = std::make_unique<TaskWorkerPool>(
            "PUSH_STORAGE_POLICY", 1, [&engine](auto&& task) { return push_storage_policy_callback(engine, task); });

    _workers[TTaskType::PUSH_COOLDOWN_CONF] = std::make_unique<TaskWorkerPool>(
            "PUSH_COOLDOWN_CONF", 1, [&engine](auto&& task) { return push_cooldown_conf_callback(engine, task); });

    _workers[TTaskType::CREATE] = std::make_unique<TaskWorkerPool>(
            "CREATE_TABLE", config::create_tablet_worker_count, [&engine](auto&& task) { return create_tablet_callback(engine, task); });

    _workers[TTaskType::DROP] = std::make_unique<TaskWorkerPool>(
            "DROP_TABLE", config::drop_tablet_worker_count, [&engine](auto&& task) { return drop_tablet_callback(engine, task); });

    _workers[TTaskType::PUBLISH_VERSION] = std::make_unique<PublishVersionWorkerPool>(engine);

    _workers[TTaskType::CLEAR_TRANSACTION_TASK] = std::make_unique<TaskWorkerPool>(
            "CLEAR_TRANSACTION_TASK", config::clear_transaction_task_worker_count, [&engine](auto&& task) { return clear_transaction_task_callback(engine, task); });

    _workers[TTaskType::PUSH] = std::make_unique<PushTaskWorkerPool>(engine);

    _workers[TTaskType::UPDATE_TABLET_META_INFO] = std::make_unique<TaskWorkerPool>(
            "UPDATE_TABLET_META_INFO", 1, [&engine](auto&& task) { return update_tablet_meta_callback(engine, task); });

    _workers[TTaskType::ALTER] = std::make_unique<TaskWorkerPool>(
            "ALTER_TABLE", config::alter_tablet_worker_count, [&engine](auto&& task) { return alter_tablet_callback(engine, task); });

    _workers[TTaskType::CLONE] = std::make_unique<TaskWorkerPool>(
            "CLONE", config::clone_worker_count, [&engine, &master_info = _master_info](auto&& task) { return clone_callback(engine, master_info, task); });

    _workers[TTaskType::STORAGE_MEDIUM_MIGRATE] = std::make_unique<TaskWorkerPool>(
            "STORAGE_MEDIUM_MIGRATE", config::storage_medium_migrate_count, [&engine](auto&& task) { return storage_medium_migrate_callback(engine, task); });

    _workers[TTaskType::GC_BINLOG] = std::make_unique<TaskWorkerPool>(
            "GC_BINLOG", 1, [&engine](auto&& task) { return gc_binlog_callback(engine, task); });

    _workers[TTaskType::CLEAN_TRASH] = std::make_unique<TaskWorkerPool>(
            "CLEAN_TRASH", 1, [&engine](auto&& task) {return clean_trash_callback(engine, task); });

    _workers[TTaskType::CLEAN_UDF_CACHE] = std::make_unique<TaskWorkerPool>(
            "CLEAN_UDF_CACHE", 1, [](auto&& task) {return clean_udf_cache_callback(task); });

    _workers[TTaskType::UPDATE_VISIBLE_VERSION] = std::make_unique<TaskWorkerPool>(
            "UPDATE_VISIBLE_VERSION", 1, [&engine](auto&& task) { return visible_version_callback(engine, task); });

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_TASK", _master_info, config::report_task_interval_seconds, [&master_info = _master_info] { report_task_callback(master_info); }));

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_DISK_STATE", _master_info, config::report_disk_state_interval_seconds, [&engine, &master_info = _master_info] { report_disk_callback(engine, master_info); }));

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_OLAP_TABLE", _master_info, config::report_tablet_interval_seconds,[&engine, &master_info = _master_info] { report_tablet_callback(engine, master_info); }));
    // clang-format on
}

void AgentServer::cloud_start_workers(CloudStorageEngine& engine, ExecEnv* exec_env) {
    _workers[TTaskType::PUSH] = std::make_unique<TaskWorkerPool>(
            "PUSH", config::delete_worker_count,
            [&engine](auto&& task) { cloud_push_callback(engine, task); });
    // TODO(plat1ko): SUBMIT_TABLE_COMPACTION worker

    _workers[TTaskType::ALTER] = std::make_unique<TaskWorkerPool>(
            "ALTER_TABLE", config::alter_tablet_worker_count,
            [&engine](auto&& task) { return alter_cloud_tablet_callback(engine, task); });

    _workers[TTaskType::CALCULATE_DELETE_BITMAP] = std::make_unique<TaskWorkerPool>(
            "CALC_DBM_TASK", config::calc_delete_bitmap_worker_count,
            [&engine](auto&& task) { return calc_delete_bitmap_callback(engine, task); });

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_TASK", _master_info, config::report_task_interval_seconds,
            [&master_info = _master_info] { report_task_callback(master_info); }));

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_DISK_STATE", _master_info, config::report_disk_state_interval_seconds,
            [&engine, &master_info = _master_info] { report_disk_callback(engine, master_info); }));
}

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

    for (auto&& task : tasks) {
        VLOG_RPC << "submit one task: " << apache::thrift::ThriftDebugString(task).c_str();
        auto task_type = task.task_type;
        if (task_type == TTaskType::REALTIME_PUSH) {
            task_type = TTaskType::PUSH;
        }
        int64_t signature = task.signature;
        if (auto it = _workers.find(task_type); it != _workers.end()) {
            auto& worker = it->second;
            ret_st = worker->submit_task(task);
        } else {
            ret_st = Status::InvalidArgument("task(signature={}, type={}) has wrong task type",
                                             signature, task.task_type);
        }

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

void AgentServer::publish_cluster_state(TAgentResult& t_agent_result,
                                        const TAgentPublishRequest& request) {
    Status status = Status::NotSupported("deprecated method(publish_cluster_state) was invoked");
    status.to_thrift(&t_agent_result.status);
}

void AgentServer::stop_report_workers() {
    for (auto& work : _report_workers) {
        work->stop();
    }
}

} // namespace doris
