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

#include "agent/task_worker_pool.h"

#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <unistd.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <atomic>
#include <chrono> // IWYU pragma: keep
#include <ctime>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "agent/utils.h"
#include "cloud/cloud_delete_task.h"
#include "cloud/cloud_engine_calc_delete_bitmap_task.h"
#include "cloud/cloud_schema_change_job.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_system.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/task/engine_batch_load_task.h"
#include "olap/task/engine_checksum_task.h"
#include "olap/task/engine_clone_task.h"
#include "olap/task/engine_index_change_task.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/task/engine_storage_migration_task.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/snapshot_loader.h"
#include "service/backend_options.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"
#include "util/jni-util.h"
#include "util/mem_info.h"
#include "util/random.h"
#include "util/s3_util.h"
#include "util/scoped_cleanup.h"
#include "util/stopwatch.hpp"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/trace.h"

namespace doris {
using namespace ErrorCode;

namespace {

std::mutex s_task_signatures_mtx;
std::unordered_map<TTaskType::type, std::unordered_set<int64_t>> s_task_signatures;

std::atomic_ulong s_report_version(time(nullptr) * 100000);

void increase_report_version() {
    s_report_version.fetch_add(1, std::memory_order_relaxed);
}

// FIXME(plat1ko): Paired register and remove task info
bool register_task_info(const TTaskType::type task_type, int64_t signature) {
    if (task_type == TTaskType::type::PUSH_STORAGE_POLICY ||
        task_type == TTaskType::type::PUSH_COOLDOWN_CONF ||
        task_type == TTaskType::type::COMPACTION) {
        // no need to report task of these types
        return true;
    }

    if (signature == -1) { // No need to report task with unintialized signature
        return true;
    }

    std::lock_guard lock(s_task_signatures_mtx);
    auto& set = s_task_signatures[task_type];
    return set.insert(signature).second;
}

void remove_task_info(const TTaskType::type task_type, int64_t signature) {
    size_t queue_size;
    {
        std::lock_guard lock(s_task_signatures_mtx);
        auto& set = s_task_signatures[task_type];
        set.erase(signature);
        queue_size = set.size();
    }

    VLOG_NOTICE << "remove task info. type=" << task_type << ", signature=" << signature
                << ", queue_size=" << queue_size;
}

void finish_task(const TFinishTaskRequest& finish_task_request) {
    // Return result to FE
    TMasterResult result;
    uint32_t try_time = 0;
    constexpr int TASK_FINISH_MAX_RETRY = 3;
    while (try_time < TASK_FINISH_MAX_RETRY) {
        DorisMetrics::instance()->finish_task_requests_total->increment(1);
        Status client_status =
                MasterServerClient::instance()->finish_task(finish_task_request, &result);

        if (client_status.ok()) {
            break;
        } else {
            DorisMetrics::instance()->finish_task_requests_failed->increment(1);
            LOG_WARNING("failed to finish task")
                    .tag("type", finish_task_request.task_type)
                    .tag("signature", finish_task_request.signature)
                    .error(result.status);
            try_time += 1;
        }
        sleep(1);
    }
}

Status get_tablet_info(StorageEngine& engine, const TTabletId tablet_id,
                       const TSchemaHash schema_hash, TTabletInfo* tablet_info) {
    tablet_info->__set_tablet_id(tablet_id);
    tablet_info->__set_schema_hash(schema_hash);
    return engine.tablet_manager()->report_tablet_info(tablet_info);
}

void random_sleep(int second) {
    Random rnd(UnixMillis());
    sleep(rnd.Uniform(second) + 1);
}

void alter_tablet(StorageEngine& engine, const TAgentTaskRequest& agent_task_req, int64_t signature,
                  const TTaskType::type task_type, TFinishTaskRequest* finish_task_request) {
    Status status;

    std::string_view process_name = "alter tablet";
    // Check last schema change status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    TTabletId new_tablet_id = 0;
    TSchemaHash new_schema_hash = 0;
    if (status.ok()) {
        new_tablet_id = agent_task_req.alter_tablet_req_v2.new_tablet_id;
        new_schema_hash = agent_task_req.alter_tablet_req_v2.new_schema_hash;
        auto mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::SCHEMA_CHANGE,
                fmt::format("EngineAlterTabletTask#baseTabletId={}:newTabletId={}",
                            std::to_string(agent_task_req.alter_tablet_req_v2.base_tablet_id),
                            std::to_string(agent_task_req.alter_tablet_req_v2.new_tablet_id),
                            engine.memory_limitation_bytes_per_thread_for_schema_change()));
        SCOPED_ATTACH_TASK(mem_tracker);
        DorisMetrics::instance()->create_rollup_requests_total->increment(1);
        Status res = Status::OK();
        try {
            LOG_INFO("start {}", process_name)
                    .tag("signature", agent_task_req.signature)
                    .tag("base_tablet_id", agent_task_req.alter_tablet_req_v2.base_tablet_id)
                    .tag("new_tablet_id", new_tablet_id)
                    .tag("mem_limit",
                         engine.memory_limitation_bytes_per_thread_for_schema_change());
            SchemaChangeJob job(engine, agent_task_req.alter_tablet_req_v2,
                                std::to_string(agent_task_req.alter_tablet_req_v2.__isset.job_id
                                                       ? agent_task_req.alter_tablet_req_v2.job_id
                                                       : 0));
            status = job.process_alter_tablet(agent_task_req.alter_tablet_req_v2);
        } catch (const Exception& e) {
            status = e.to_status();
        }
        if (!status.ok()) {
            DorisMetrics::instance()->create_rollup_requests_failed->increment(1);
        }
    }

    if (status.ok()) {
        increase_report_version();
    }

    // Return result to fe
    finish_task_request->__set_backend(BackendOptions::get_local_backend());
    finish_task_request->__set_report_version(s_report_version);
    finish_task_request->__set_task_type(task_type);
    finish_task_request->__set_signature(signature);

    std::vector<TTabletInfo> finish_tablet_infos;
    if (status.ok()) {
        TTabletInfo tablet_info;
        status = get_tablet_info(engine, new_tablet_id, new_schema_hash, &tablet_info);
        if (status.ok()) {
            finish_tablet_infos.push_back(tablet_info);
        }
    }

    if (!status.ok() && !status.is<NOT_IMPLEMENTED_ERROR>()) {
        LOG_WARNING("failed to {}", process_name)
                .tag("signature", agent_task_req.signature)
                .tag("base_tablet_id", agent_task_req.alter_tablet_req_v2.base_tablet_id)
                .tag("new_tablet_id", new_tablet_id)
                .error(status);
    } else {
        finish_task_request->__set_finish_tablet_infos(finish_tablet_infos);
        LOG_INFO("successfully {}", process_name)
                .tag("signature", agent_task_req.signature)
                .tag("base_tablet_id", agent_task_req.alter_tablet_req_v2.base_tablet_id)
                .tag("new_tablet_id", new_tablet_id);
    }
    finish_task_request->__set_task_status(status.to_thrift());
}

void alter_cloud_tablet(CloudStorageEngine& engine, const TAgentTaskRequest& agent_task_req,
                        int64_t signature, const TTaskType::type task_type,
                        TFinishTaskRequest* finish_task_request) {
    Status status;

    std::string_view process_name = "alter tablet";
    // Check last schema change status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    TTabletId new_tablet_id = 0;
    if (status.ok()) {
        new_tablet_id = agent_task_req.alter_tablet_req_v2.new_tablet_id;
        auto mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::SCHEMA_CHANGE,
                fmt::format("EngineAlterTabletTask#baseTabletId={}:newTabletId={}",
                            std::to_string(agent_task_req.alter_tablet_req_v2.base_tablet_id),
                            std::to_string(agent_task_req.alter_tablet_req_v2.new_tablet_id),
                            engine.memory_limitation_bytes_per_thread_for_schema_change()));
        SCOPED_ATTACH_TASK(mem_tracker);
        DorisMetrics::instance()->create_rollup_requests_total->increment(1);
        Status res = Status::OK();
        try {
            LOG_INFO("start {}", process_name)
                    .tag("signature", agent_task_req.signature)
                    .tag("base_tablet_id", agent_task_req.alter_tablet_req_v2.base_tablet_id)
                    .tag("new_tablet_id", new_tablet_id)
                    .tag("mem_limit",
                         engine.memory_limitation_bytes_per_thread_for_schema_change());
            DCHECK(agent_task_req.alter_tablet_req_v2.__isset.job_id);
            CloudSchemaChangeJob job(engine,
                                     std::to_string(agent_task_req.alter_tablet_req_v2.job_id),
                                     agent_task_req.alter_tablet_req_v2.expiration);
            status = job.process_alter_tablet(agent_task_req.alter_tablet_req_v2);
        } catch (const Exception& e) {
            status = e.to_status();
        }
        if (!status.ok()) {
            DorisMetrics::instance()->create_rollup_requests_failed->increment(1);
        }
    }

    if (status.ok()) {
        increase_report_version();
    }

    // Return result to fe
    finish_task_request->__set_backend(BackendOptions::get_local_backend());
    finish_task_request->__set_report_version(s_report_version);
    finish_task_request->__set_task_type(task_type);
    finish_task_request->__set_signature(signature);

    if (!status.ok() && !status.is<NOT_IMPLEMENTED_ERROR>()) {
        LOG_WARNING("failed to {}", process_name)
                .tag("signature", agent_task_req.signature)
                .tag("base_tablet_id", agent_task_req.alter_tablet_req_v2.base_tablet_id)
                .tag("new_tablet_id", new_tablet_id)
                .error(status);
    } else {
        LOG_INFO("successfully {}", process_name)
                .tag("signature", agent_task_req.signature)
                .tag("base_tablet_id", agent_task_req.alter_tablet_req_v2.base_tablet_id)
                .tag("new_tablet_id", new_tablet_id);
    }
    finish_task_request->__set_task_status(status.to_thrift());
}

Status check_migrate_request(StorageEngine& engine, const TStorageMediumMigrateReq& req,
                             TabletSharedPtr& tablet, DataDir** dest_store) {
    int64_t tablet_id = req.tablet_id;
    tablet = engine.tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::InternalError("could not find tablet {}", tablet_id);
    }

    if (req.__isset.data_dir) {
        // request specify the data dir
        *dest_store = engine.get_store(req.data_dir);
        if (*dest_store == nullptr) {
            return Status::InternalError("could not find data dir {}", req.data_dir);
        }
    } else {
        // this is a storage medium
        // get data dir by storage medium

        // judge case when no need to migrate
        uint32_t count = engine.available_storage_medium_type_count();
        if (count <= 1) {
            return Status::InternalError("available storage medium type count is less than 1");
        }
        // check current tablet storage medium
        TStorageMedium::type storage_medium = req.storage_medium;
        TStorageMedium::type src_storage_medium = tablet->data_dir()->storage_medium();
        if (src_storage_medium == storage_medium) {
            return Status::InternalError("tablet is already on specified storage medium {}",
                                         storage_medium);
        }
        // get a random store of specified storage medium
        auto stores = engine.get_stores_for_create_tablet(tablet->partition_id(), storage_medium);
        if (stores.empty()) {
            return Status::InternalError("failed to get root path for create tablet");
        }

        *dest_store = stores[0];
    }
    if (tablet->data_dir()->path() == (*dest_store)->path()) {
        LOG_WARNING("tablet is already on specified path").tag("path", tablet->data_dir()->path());
        return Status::Error<FILE_ALREADY_EXIST, false>("tablet is already on specified path: {}",
                                                        tablet->data_dir()->path());
    }

    // check local disk capacity
    int64_t tablet_size = tablet->tablet_local_size();
    if ((*dest_store)->reach_capacity_limit(tablet_size)) {
        return Status::Error<EXCEEDED_LIMIT>("reach the capacity limit of path {}, tablet_size={}",
                                             (*dest_store)->path(), tablet_size);
    }
    return Status::OK();
}

// Return `true` if report success
bool handle_report(const TReportRequest& request, const TMasterInfo& master_info,
                   std::string_view name) {
    TMasterResult result;
    Status status = MasterServerClient::instance()->report(request, &result);
    if (!status.ok()) [[unlikely]] {
        LOG_WARNING("failed to report {}", name)
                .tag("host", master_info.network_address.hostname)
                .tag("port", master_info.network_address.port)
                .error(status);
        return false;
    }

    else if (result.status.status_code != TStatusCode::OK) [[unlikely]] {
        LOG_WARNING("failed to report {}", name)
                .tag("host", master_info.network_address.hostname)
                .tag("port", master_info.network_address.port)
                .error(result.status);
        return false;
    }

    return true;
}

Status _submit_task(const TAgentTaskRequest& task,
                    std::function<Status(const TAgentTaskRequest&)> submit_op) {
    const TTaskType::type task_type = task.task_type;
    int64_t signature = task.signature;

    std::string type_str;
    EnumToString(TTaskType, task_type, type_str);
    VLOG_CRITICAL << "submitting task. type=" << type_str << ", signature=" << signature;

    if (!register_task_info(task_type, signature)) {
        LOG_WARNING("failed to register task").tag("type", type_str).tag("signature", signature);
        // Duplicated task request, just return OK
        return Status::OK();
    }

    // TODO(plat1ko): check task request member

    // Set the receiving time of task so that we can determine whether it is timed out later
    (const_cast<TAgentTaskRequest&>(task)).__set_recv_time(time(nullptr));
    auto st = submit_op(task);
    if (!st.ok()) [[unlikely]] {
        LOG_INFO("failed to submit task").tag("type", type_str).tag("signature", signature);
        return st;
    }

    LOG_INFO("successfully submit task").tag("type", type_str).tag("signature", signature);
    return Status::OK();
}

bvar::LatencyRecorder g_publish_version_latency("doris_pk", "publish_version");

bvar::Adder<uint64_t> ALTER_INVERTED_INDEX_count("task", "ALTER_INVERTED_INDEX");
bvar::Adder<uint64_t> CHECK_CONSISTENCY_count("task", "CHECK_CONSISTENCY");
bvar::Adder<uint64_t> UPLOAD_count("task", "UPLOAD");
bvar::Adder<uint64_t> DOWNLOAD_count("task", "DOWNLOAD");
bvar::Adder<uint64_t> MAKE_SNAPSHOT_count("task", "MAKE_SNAPSHOT");
bvar::Adder<uint64_t> RELEASE_SNAPSHOT_count("task", "RELEASE_SNAPSHOT");
bvar::Adder<uint64_t> MOVE_count("task", "MOVE");
bvar::Adder<uint64_t> COMPACTION_count("task", "COMPACTION");
bvar::Adder<uint64_t> PUSH_STORAGE_POLICY_count("task", "PUSH_STORAGE_POLICY");
bvar::Adder<uint64_t> PUSH_COOLDOWN_CONF_count("task", "PUSH_COOLDOWN_CONF");
bvar::Adder<uint64_t> CREATE_count("task", "CREATE_TABLE");
bvar::Adder<uint64_t> DROP_count("task", "DROP_TABLE");
bvar::Adder<uint64_t> PUBLISH_VERSION_count("task", "PUBLISH_VERSION");
bvar::Adder<uint64_t> CLEAR_TRANSACTION_TASK_count("task", "CLEAR_TRANSACTION_TASK");
bvar::Adder<uint64_t> DELETE_count("task", "DELETE");
bvar::Adder<uint64_t> PUSH_count("task", "PUSH");
bvar::Adder<uint64_t> UPDATE_TABLET_META_INFO_count("task", "UPDATE_TABLET_META_INFO");
bvar::Adder<uint64_t> ALTER_count("task", "ALTER_TABLE");
bvar::Adder<uint64_t> CLONE_count("task", "CLONE");
bvar::Adder<uint64_t> STORAGE_MEDIUM_MIGRATE_count("task", "STORAGE_MEDIUM_MIGRATE");
bvar::Adder<uint64_t> GC_BINLOG_count("task", "GC_BINLOG");
bvar::Adder<uint64_t> UPDATE_VISIBLE_VERSION_count("task", "UPDATE_VISIBLE_VERSION");

void add_task_count(const TAgentTaskRequest& task, int n) {
    // clang-format off
    switch (task.task_type) {
    #define ADD_TASK_COUNT(type) \
    case TTaskType::type:        \
        type##_count << n;       \
        return;
    ADD_TASK_COUNT(ALTER_INVERTED_INDEX)
    ADD_TASK_COUNT(CHECK_CONSISTENCY)
    ADD_TASK_COUNT(UPLOAD)
    ADD_TASK_COUNT(DOWNLOAD)
    ADD_TASK_COUNT(MAKE_SNAPSHOT)
    ADD_TASK_COUNT(RELEASE_SNAPSHOT)
    ADD_TASK_COUNT(MOVE)
    ADD_TASK_COUNT(COMPACTION)
    ADD_TASK_COUNT(PUSH_STORAGE_POLICY)
    ADD_TASK_COUNT(PUSH_COOLDOWN_CONF)
    ADD_TASK_COUNT(CREATE)
    ADD_TASK_COUNT(DROP)
    ADD_TASK_COUNT(PUBLISH_VERSION)
    ADD_TASK_COUNT(CLEAR_TRANSACTION_TASK)
    ADD_TASK_COUNT(UPDATE_TABLET_META_INFO)
    ADD_TASK_COUNT(CLONE)
    ADD_TASK_COUNT(STORAGE_MEDIUM_MIGRATE)
    ADD_TASK_COUNT(GC_BINLOG)
    ADD_TASK_COUNT(UPDATE_VISIBLE_VERSION)
    #undef ADD_TASK_COUNT
    case TTaskType::REALTIME_PUSH:
    case TTaskType::PUSH:
        if (task.push_req.push_type == TPushType::LOAD_V2) {
            PUSH_count << n;
        } else if (task.push_req.push_type == TPushType::DELETE) {
            DELETE_count << n;
        }
        return;
    case TTaskType::ALTER:
    {
        ALTER_count << n;
        // cloud auto stop need sc jobs, a tablet's sc can also be considered a fragment
        doris::g_fragment_executing_count << 1;
        int64 now = duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
        g_fragment_last_active_time.set_value(now);
        return;
    }
    default:
        return;
    }
    // clang-format on
}

bvar::Adder<uint64_t> report_task_total("report", "task_total");
bvar::Adder<uint64_t> report_task_failed("report", "task_failed");
bvar::Adder<uint64_t> report_disk_total("report", "disk_total");
bvar::Adder<uint64_t> report_disk_failed("report", "disk_failed");
bvar::Adder<uint64_t> report_tablet_total("report", "tablet_total");
bvar::Adder<uint64_t> report_tablet_failed("report", "tablet_failed");

} // namespace

TaskWorkerPool::TaskWorkerPool(std::string_view name, int worker_count,
                               std::function<void(const TAgentTaskRequest& task)> callback)
        : _callback(std::move(callback)) {
    auto st = ThreadPoolBuilder(fmt::format("TaskWP_{}", name))
                      .set_min_threads(worker_count)
                      .set_max_threads(worker_count)
                      .build(&_thread_pool);
    CHECK(st.ok()) << name << ": " << st;
}

TaskWorkerPool::~TaskWorkerPool() {
    stop();
}

void TaskWorkerPool::stop() {
    if (_stopped.exchange(true)) {
        return;
    }

    if (_thread_pool) {
        _thread_pool->shutdown();
    }
}

Status TaskWorkerPool::submit_task(const TAgentTaskRequest& task) {
    return _submit_task(task, [this](auto&& task) {
        add_task_count(task, 1);
        return _thread_pool->submit_func([this, task]() {
            _callback(task);
            add_task_count(task, -1);
        });
    });
}

PriorTaskWorkerPool::PriorTaskWorkerPool(
        const std::string& name, int normal_worker_count, int high_prior_worker_count,
        std::function<void(const TAgentTaskRequest& task)> callback)
        : _callback(std::move(callback)) {
    for (int i = 0; i < normal_worker_count; ++i) {
        auto st = Thread::create(
                "Normal", name, [this] { normal_loop(); }, &_workers.emplace_back());
        CHECK(st.ok()) << name << ": " << st;
    }

    for (int i = 0; i < high_prior_worker_count; ++i) {
        auto st = Thread::create(
                "HighPrior", name, [this] { high_prior_loop(); }, &_workers.emplace_back());
        CHECK(st.ok()) << name << ": " << st;
    }
}

PriorTaskWorkerPool::~PriorTaskWorkerPool() {
    stop();
}

void PriorTaskWorkerPool::stop() {
    {
        std::lock_guard lock(_mtx);
        if (_stopped) {
            return;
        }

        _stopped = true;
    }
    _normal_condv.notify_all();
    _high_prior_condv.notify_all();

    for (auto&& w : _workers) {
        if (w) {
            w->join();
        }
    }
}

Status PriorTaskWorkerPool::submit_task(const TAgentTaskRequest& task) {
    return _submit_task(task, [this](auto&& task) {
        auto req = std::make_unique<TAgentTaskRequest>(task);
        add_task_count(*req, 1);
        if (req->__isset.priority && req->priority == TPriority::HIGH) {
            std::lock_guard lock(_mtx);
            _high_prior_queue.push_back(std::move(req));
            _high_prior_condv.notify_one();
            _normal_condv.notify_one();
        } else {
            std::lock_guard lock(_mtx);
            _normal_queue.push_back(std::move(req));
            _normal_condv.notify_one();
        }
        return Status::OK();
    });
}

void PriorTaskWorkerPool::normal_loop() {
    while (true) {
        std::unique_ptr<TAgentTaskRequest> req;

        {
            std::unique_lock lock(_mtx);
            _normal_condv.wait(lock, [&] {
                return !_normal_queue.empty() || !_high_prior_queue.empty() || _stopped;
            });

            if (_stopped) {
                return;
            }

            if (!_high_prior_queue.empty()) {
                req = std::move(_high_prior_queue.front());
                _high_prior_queue.pop_front();
            } else if (!_normal_queue.empty()) {
                req = std::move(_normal_queue.front());
                _normal_queue.pop_front();
            } else {
                continue;
            }
        }

        _callback(*req);
        add_task_count(*req, -1);
    }
}

void PriorTaskWorkerPool::high_prior_loop() {
    while (true) {
        std::unique_ptr<TAgentTaskRequest> req;

        {
            std::unique_lock lock(_mtx);
            _high_prior_condv.wait(lock, [&] { return !_high_prior_queue.empty() || _stopped; });

            if (_stopped) {
                return;
            }

            if (_high_prior_queue.empty()) {
                continue;
            }

            req = std::move(_high_prior_queue.front());
            _high_prior_queue.pop_front();
        }

        _callback(*req);
        add_task_count(*req, -1);
    }
}

ReportWorker::ReportWorker(std::string name, const TMasterInfo& master_info, int report_interval_s,
                           std::function<void()> callback)
        : _name(std::move(name)) {
    auto report_loop = [this, &master_info, report_interval_s, callback = std::move(callback)] {
        auto& engine = ExecEnv::GetInstance()->storage_engine();
        engine.register_report_listener(this);
        while (true) {
            {
                std::unique_lock lock(_mtx);
                _condv.wait_for(lock, std::chrono::seconds(report_interval_s),
                                [&] { return _stopped || _signal; });

                if (_stopped) {
                    break;
                }

                if (_signal) {
                    // Consume received signal
                    _signal = false;
                }
            }

            if (master_info.network_address.port == 0) {
                // port == 0 means not received heartbeat yet
                LOG(INFO) << "waiting to receive first heartbeat from frontend before doing report";
                continue;
            }

            callback();
        }
        engine.deregister_report_listener(this);
    };

    auto st = Thread::create("ReportWorker", _name, report_loop, &_thread);
    CHECK(st.ok()) << _name << ": " << st;
}

ReportWorker::~ReportWorker() {
    stop();
}

void ReportWorker::notify() {
    {
        std::lock_guard lock(_mtx);
        _signal = true;
    }
    _condv.notify_all();
}

void ReportWorker::stop() {
    {
        std::lock_guard lock(_mtx);
        if (_stopped) {
            return;
        }

        _stopped = true;
    }
    _condv.notify_all();
    if (_thread) {
        _thread->join();
    }
}

void alter_inverted_index_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& alter_inverted_index_rq = req.alter_inverted_index_req;
    LOG(INFO) << "get alter inverted index task. signature=" << req.signature
              << ", tablet_id=" << alter_inverted_index_rq.tablet_id
              << ", job_id=" << alter_inverted_index_rq.job_id;

    Status status = Status::OK();
    auto tablet_ptr = engine.tablet_manager()->get_tablet(alter_inverted_index_rq.tablet_id);
    if (tablet_ptr != nullptr) {
        EngineIndexChangeTask engine_task(engine, alter_inverted_index_rq);
        SCOPED_ATTACH_TASK(engine_task.mem_tracker());
        status = engine_task.execute();
    } else {
        status = Status::NotFound("could not find tablet {}", alter_inverted_index_rq.tablet_id);
    }

    // Return result to fe
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    std::vector<TTabletInfo> finish_tablet_infos;
    if (!status.ok()) {
        LOG(WARNING) << "failed to alter inverted index task, signature=" << req.signature
                     << ", tablet_id=" << alter_inverted_index_rq.tablet_id
                     << ", job_id=" << alter_inverted_index_rq.job_id << ", error=" << status;
    } else {
        LOG(INFO) << "successfully alter inverted index task, signature=" << req.signature
                  << ", tablet_id=" << alter_inverted_index_rq.tablet_id
                  << ", job_id=" << alter_inverted_index_rq.job_id;
        TTabletInfo tablet_info;
        status = get_tablet_info(engine, alter_inverted_index_rq.tablet_id,
                                 alter_inverted_index_rq.schema_hash, &tablet_info);
        if (status.ok()) {
            finish_tablet_infos.push_back(tablet_info);
        }
        finish_task_request.__set_finish_tablet_infos(finish_tablet_infos);
    }
    finish_task_request.__set_task_status(status.to_thrift());
    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void update_tablet_meta_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    LOG(INFO) << "get update tablet meta task. signature=" << req.signature;

    Status status;
    const auto& update_tablet_meta_req = req.update_tablet_meta_info_req;
    for (const auto& tablet_meta_info : update_tablet_meta_req.tabletMetaInfos) {
        auto tablet = engine.tablet_manager()->get_tablet(tablet_meta_info.tablet_id);
        if (tablet == nullptr) {
            status = Status::NotFound("tablet not found");
            LOG(WARNING) << "could not find tablet when update tablet meta. tablet_id="
                         << tablet_meta_info.tablet_id;
            continue;
        }
        bool need_to_save = false;
        if (tablet_meta_info.__isset.partition_id) {
            // for fix partition_id = 0
            LOG(WARNING) << "change be tablet id: " << tablet->tablet_meta()->tablet_id()
                         << "partition id from : " << tablet->tablet_meta()->partition_id()
                         << " to : " << tablet_meta_info.partition_id;
            auto succ = engine.tablet_manager()->update_tablet_partition_id(
                    tablet_meta_info.partition_id, tablet->tablet_meta()->tablet_id());
            if (!succ) {
                std::string err_msg = fmt::format(
                        "change be tablet id : {} partition_id : {} failed",
                        tablet->tablet_meta()->tablet_id(), tablet_meta_info.partition_id);
                LOG(WARNING) << err_msg;
                status = Status::InvalidArgument(err_msg);
                continue;
            }
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.storage_policy_id) {
            tablet->tablet_meta()->set_storage_policy_id(tablet_meta_info.storage_policy_id);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.is_in_memory) {
            tablet->tablet_meta()->mutable_tablet_schema()->set_is_in_memory(
                    tablet_meta_info.is_in_memory);
            std::shared_lock rlock(tablet->get_header_lock());
            for (auto& rowset_meta : tablet->tablet_meta()->all_mutable_rs_metas()) {
                rowset_meta->tablet_schema()->set_is_in_memory(tablet_meta_info.is_in_memory);
            }
            tablet->tablet_schema_unlocked()->set_is_in_memory(tablet_meta_info.is_in_memory);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.compaction_policy) {
            if (tablet_meta_info.compaction_policy != CUMULATIVE_SIZE_BASED_POLICY &&
                tablet_meta_info.compaction_policy != CUMULATIVE_TIME_SERIES_POLICY) {
                status = Status::InvalidArgument(
                        "invalid compaction policy, only support for size_based or "
                        "time_series");
                continue;
            }
            tablet->tablet_meta()->set_compaction_policy(tablet_meta_info.compaction_policy);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.time_series_compaction_goal_size_mbytes) {
            if (tablet->tablet_meta()->compaction_policy() != CUMULATIVE_TIME_SERIES_POLICY) {
                status = Status::InvalidArgument(
                        "only time series compaction policy support time series config");
                continue;
            }
            tablet->tablet_meta()->set_time_series_compaction_goal_size_mbytes(
                    tablet_meta_info.time_series_compaction_goal_size_mbytes);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.time_series_compaction_file_count_threshold) {
            if (tablet->tablet_meta()->compaction_policy() != CUMULATIVE_TIME_SERIES_POLICY) {
                status = Status::InvalidArgument(
                        "only time series compaction policy support time series config");
                continue;
            }
            tablet->tablet_meta()->set_time_series_compaction_file_count_threshold(
                    tablet_meta_info.time_series_compaction_file_count_threshold);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.time_series_compaction_time_threshold_seconds) {
            if (tablet->tablet_meta()->compaction_policy() != CUMULATIVE_TIME_SERIES_POLICY) {
                status = Status::InvalidArgument(
                        "only time series compaction policy support time series config");
                continue;
            }
            tablet->tablet_meta()->set_time_series_compaction_time_threshold_seconds(
                    tablet_meta_info.time_series_compaction_time_threshold_seconds);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.time_series_compaction_empty_rowsets_threshold) {
            if (tablet->tablet_meta()->compaction_policy() != CUMULATIVE_TIME_SERIES_POLICY) {
                status = Status::InvalidArgument(
                        "only time series compaction policy support time series config");
                continue;
            }
            tablet->tablet_meta()->set_time_series_compaction_empty_rowsets_threshold(
                    tablet_meta_info.time_series_compaction_empty_rowsets_threshold);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.time_series_compaction_level_threshold) {
            if (tablet->tablet_meta()->compaction_policy() != CUMULATIVE_TIME_SERIES_POLICY) {
                status = Status::InvalidArgument(
                        "only time series compaction policy support time series config");
                continue;
            }
            tablet->tablet_meta()->set_time_series_compaction_level_threshold(
                    tablet_meta_info.time_series_compaction_level_threshold);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.replica_id) {
            tablet->tablet_meta()->set_replica_id(tablet_meta_info.replica_id);
        }
        if (tablet_meta_info.__isset.binlog_config) {
            // check binlog_config require fields: enable, ttl_seconds, max_bytes, max_history_nums
            const auto& t_binlog_config = tablet_meta_info.binlog_config;
            if (!t_binlog_config.__isset.enable || !t_binlog_config.__isset.ttl_seconds ||
                !t_binlog_config.__isset.max_bytes || !t_binlog_config.__isset.max_history_nums) {
                status = Status::InvalidArgument("invalid binlog config, some fields not set");
                LOG(WARNING) << fmt::format(
                        "invalid binlog config, some fields not set, tablet_id={}, "
                        "t_binlog_config={}",
                        tablet_meta_info.tablet_id,
                        apache::thrift::ThriftDebugString(t_binlog_config));
                continue;
            }

            BinlogConfig new_binlog_config;
            new_binlog_config = tablet_meta_info.binlog_config;
            LOG(INFO) << fmt::format(
                    "update tablet meta binlog config. tablet_id={}, old_binlog_config={}, "
                    "new_binlog_config={}",
                    tablet_meta_info.tablet_id, tablet->tablet_meta()->binlog_config().to_string(),
                    new_binlog_config.to_string());
            tablet->set_binlog_config(new_binlog_config);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.enable_single_replica_compaction) {
            std::shared_lock rlock(tablet->get_header_lock());
            tablet->tablet_meta()->mutable_tablet_schema()->set_enable_single_replica_compaction(
                    tablet_meta_info.enable_single_replica_compaction);
            for (auto& rowset_meta : tablet->tablet_meta()->all_mutable_rs_metas()) {
                rowset_meta->tablet_schema()->set_enable_single_replica_compaction(
                        tablet_meta_info.enable_single_replica_compaction);
            }
            tablet->tablet_schema_unlocked()->set_enable_single_replica_compaction(
                    tablet_meta_info.enable_single_replica_compaction);
            need_to_save = true;
        }
        if (tablet_meta_info.__isset.disable_auto_compaction) {
            std::shared_lock rlock(tablet->get_header_lock());
            tablet->tablet_meta()->mutable_tablet_schema()->set_disable_auto_compaction(
                    tablet_meta_info.disable_auto_compaction);
            for (auto& rowset_meta : tablet->tablet_meta()->all_mutable_rs_metas()) {
                rowset_meta->tablet_schema()->set_disable_auto_compaction(
                        tablet_meta_info.disable_auto_compaction);
            }
            tablet->tablet_schema_unlocked()->set_disable_auto_compaction(
                    tablet_meta_info.disable_auto_compaction);
            need_to_save = true;
        }

        if (tablet_meta_info.__isset.skip_write_index_on_load) {
            std::shared_lock rlock(tablet->get_header_lock());
            tablet->tablet_meta()->mutable_tablet_schema()->set_skip_write_index_on_load(
                    tablet_meta_info.skip_write_index_on_load);
            for (auto& rowset_meta : tablet->tablet_meta()->all_mutable_rs_metas()) {
                rowset_meta->tablet_schema()->set_skip_write_index_on_load(
                        tablet_meta_info.skip_write_index_on_load);
            }
            tablet->tablet_schema_unlocked()->set_skip_write_index_on_load(
                    tablet_meta_info.skip_write_index_on_load);
            need_to_save = true;
        }
        if (need_to_save) {
            std::shared_lock rlock(tablet->get_header_lock());
            tablet->save_meta();
        }
    }

    LOG(INFO) << "finish update tablet meta task. signature=" << req.signature;
    if (req.signature != -1) {
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(status.to_thrift());
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
        finish_task_request.__set_task_type(req.task_type);
        finish_task_request.__set_signature(req.signature);
        finish_task(finish_task_request);
        remove_task_info(req.task_type, req.signature);
    }
}

void check_consistency_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    uint32_t checksum = 0;
    const auto& check_consistency_req = req.check_consistency_req;
    EngineChecksumTask engine_task(engine, check_consistency_req.tablet_id,
                                   check_consistency_req.schema_hash, check_consistency_req.version,
                                   &checksum);
    SCOPED_ATTACH_TASK(engine_task.mem_tracker());
    Status status = engine_task.execute();
    if (!status.ok()) {
        LOG_WARNING("failed to check consistency")
                .tag("signature", req.signature)
                .tag("tablet_id", check_consistency_req.tablet_id)
                .error(status);
    } else {
        LOG_INFO("successfully check consistency")
                .tag("signature", req.signature)
                .tag("tablet_id", check_consistency_req.tablet_id)
                .tag("checksum", checksum);
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());
    finish_task_request.__set_tablet_checksum(static_cast<int64_t>(checksum));
    finish_task_request.__set_request_version(check_consistency_req.version);

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void report_task_callback(const TMasterInfo& master_info) {
    TReportRequest request;
    if (config::report_random_wait) {
        random_sleep(5);
    }
    request.__isset.tasks = true;
    {
        std::lock_guard lock(s_task_signatures_mtx);
        auto& tasks = request.tasks;
        for (auto&& [task_type, signatures] : s_task_signatures) {
            auto& set = tasks[task_type];
            for (auto&& signature : signatures) {
                set.insert(signature);
            }
        }
    }
    request.__set_backend(BackendOptions::get_local_backend());
    bool succ = handle_report(request, master_info, "task");
    report_task_total << 1;
    if (!succ) [[unlikely]] {
        report_task_failed << 1;
    }
}

void report_disk_callback(StorageEngine& engine, const TMasterInfo& master_info) {
    TReportRequest request;
    request.__set_backend(BackendOptions::get_local_backend());
    request.__isset.disks = true;

    std::vector<DataDirInfo> data_dir_infos;
    static_cast<void>(engine.get_all_data_dir_info(&data_dir_infos, true /* update */));

    for (auto& root_path_info : data_dir_infos) {
        TDisk disk;
        disk.__set_root_path(root_path_info.path);
        disk.__set_path_hash(root_path_info.path_hash);
        disk.__set_storage_medium(root_path_info.storage_medium);
        disk.__set_disk_total_capacity(root_path_info.disk_capacity);
        disk.__set_data_used_capacity(root_path_info.local_used_capacity);
        disk.__set_remote_used_capacity(root_path_info.remote_used_capacity);
        disk.__set_disk_available_capacity(root_path_info.available);
        disk.__set_trash_used_capacity(root_path_info.trash_used_capacity);
        disk.__set_used(root_path_info.is_used);
        request.disks[root_path_info.path] = disk;
    }
    request.__set_num_cores(CpuInfo::num_cores());
    request.__set_pipeline_executor_size(config::pipeline_executor_size > 0
                                                 ? config::pipeline_executor_size
                                                 : CpuInfo::num_cores());
    bool succ = handle_report(request, master_info, "disk");
    report_disk_total << 1;
    if (!succ) [[unlikely]] {
        report_disk_failed << 1;
    }
}

void report_disk_callback(CloudStorageEngine& engine, const TMasterInfo& master_info) {
    // Random sleep 1~5 seconds before doing report.
    // In order to avoid the problem that the FE receives many report requests at the same time
    // and can not be processed.
    if (config::report_random_wait) {
        random_sleep(5);
    }
    (void)engine; // To be used in the future

    TReportRequest request;
    request.__set_backend(BackendOptions::get_local_backend());
    request.__isset.disks = true;

    // TODO(deardeng): report disk info in cloud mode. And make it more clear
    //                 that report CPU by using a separte report procedure
    //                 or abstracting disk report as "host info report"
    request.__set_num_cores(CpuInfo::num_cores());
    request.__set_pipeline_executor_size(config::pipeline_executor_size > 0
                                                 ? config::pipeline_executor_size
                                                 : CpuInfo::num_cores());
    bool succ = handle_report(request, master_info, "disk");
    report_disk_total << 1;
    report_disk_failed << !succ;
}

void report_tablet_callback(StorageEngine& engine, const TMasterInfo& master_info) {
    if (config::report_random_wait) {
        random_sleep(5);
    }

    TReportRequest request;
    request.__set_backend(BackendOptions::get_local_backend());
    request.__isset.tablets = true;

    increase_report_version();
    uint64_t report_version;
    for (int i = 0; i < 5; i++) {
        request.tablets.clear();
        report_version = s_report_version;
        engine.tablet_manager()->build_all_report_tablets_info(&request.tablets);
        if (report_version == s_report_version) {
            break;
        }
    }

    if (report_version < s_report_version) {
        // TODO llj This can only reduce the possibility for report error, but can't avoid it.
        // If FE create a tablet in FE meta and send CREATE task to this BE, the tablet may not be included in this
        // report, and the report version has a small probability that it has not been updated in time. When FE
        // receives this report, it is possible to delete the new tablet.
        LOG(WARNING) << "report version " << report_version << " change to " << s_report_version;
        DorisMetrics::instance()->report_all_tablets_requests_skip->increment(1);
        return;
    }

    std::map<int64_t, int64_t> partitions_version;
    engine.tablet_manager()->get_partitions_visible_version(&partitions_version);
    request.__set_partitions_version(std::move(partitions_version));

    int64_t max_compaction_score =
            std::max(DorisMetrics::instance()->tablet_cumulative_max_compaction_score->value(),
                     DorisMetrics::instance()->tablet_base_max_compaction_score->value());
    request.__set_tablet_max_compaction_score(max_compaction_score);
    request.__set_report_version(report_version);

    // report storage policy and resource
    auto& storage_policy_list = request.storage_policy;
    for (auto [id, version] : get_storage_policy_ids()) {
        auto& storage_policy = storage_policy_list.emplace_back();
        storage_policy.__set_id(id);
        storage_policy.__set_version(version);
    }
    request.__isset.storage_policy = true;
    auto& resource_list = request.resource;
    for (auto [id_str, version] : get_storage_resource_ids()) {
        auto& resource = resource_list.emplace_back();
        int64_t id = -1;
        if (auto [_, ec] = std::from_chars(id_str.data(), id_str.data() + id_str.size(), id);
            ec != std::errc {}) [[unlikely]] {
            LOG(ERROR) << "invalid resource id format: " << id_str;
        } else {
            resource.__set_id(id);
            resource.__set_version(version);
        }
    }
    request.__isset.resource = true;

    bool succ = handle_report(request, master_info, "tablet");
    report_tablet_total << 1;
    if (!succ) [[unlikely]] {
        report_tablet_failed << 1;
    }
}

void upload_callback(StorageEngine& engine, ExecEnv* env, const TAgentTaskRequest& req) {
    const auto& upload_request = req.upload_req;

    LOG(INFO) << "get upload task. signature=" << req.signature
              << ", job_id=" << upload_request.job_id;

    std::map<int64_t, std::vector<std::string>> tablet_files;
    std::unique_ptr<SnapshotLoader> loader = std::make_unique<SnapshotLoader>(
            engine, env, upload_request.job_id, req.signature, upload_request.broker_addr,
            upload_request.broker_prop);
    Status status =
            loader->init(upload_request.__isset.storage_backend ? upload_request.storage_backend
                                                                : TStorageBackendType::type::BROKER,
                         upload_request.__isset.location ? upload_request.location : "");
    if (status.ok()) {
        status = loader->upload(upload_request.src_dest_map, &tablet_files);
    }

    if (!status.ok()) {
        LOG_WARNING("failed to upload")
                .tag("signature", req.signature)
                .tag("job_id", upload_request.job_id)
                .error(status);
    } else {
        LOG_INFO("successfully upload")
                .tag("signature", req.signature)
                .tag("job_id", upload_request.job_id);
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());
    finish_task_request.__set_tablet_files(tablet_files);

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void download_callback(StorageEngine& engine, ExecEnv* env, const TAgentTaskRequest& req) {
    const auto& download_request = req.download_req;
    LOG(INFO) << "get download task. signature=" << req.signature
              << ", job_id=" << download_request.job_id
              << ", task detail: " << apache::thrift::ThriftDebugString(download_request);

    // TODO: download
    std::vector<int64_t> downloaded_tablet_ids;

    auto status = Status::OK();
    if (download_request.__isset.remote_tablet_snapshots) {
        std::unique_ptr<SnapshotLoader> loader = std::make_unique<SnapshotLoader>(
                engine, env, download_request.job_id, req.signature);
        status = loader->remote_http_download(download_request.remote_tablet_snapshots,
                                              &downloaded_tablet_ids);
    } else {
        std::unique_ptr<SnapshotLoader> loader = std::make_unique<SnapshotLoader>(
                engine, env, download_request.job_id, req.signature, download_request.broker_addr,
                download_request.broker_prop);
        status = loader->init(download_request.__isset.storage_backend
                                      ? download_request.storage_backend
                                      : TStorageBackendType::type::BROKER,
                              download_request.__isset.location ? download_request.location : "");
        if (status.ok()) {
            status = loader->download(download_request.src_dest_map, &downloaded_tablet_ids);
        }
    }

    if (!status.ok()) {
        LOG_WARNING("failed to download")
                .tag("signature", req.signature)
                .tag("job_id", download_request.job_id)
                .error(status);
    } else {
        LOG_INFO("successfully download")
                .tag("signature", req.signature)
                .tag("job_id", download_request.job_id);
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());
    finish_task_request.__set_downloaded_tablet_ids(downloaded_tablet_ids);

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void make_snapshot_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& snapshot_request = req.snapshot_req;

    LOG(INFO) << "get snapshot task. signature=" << req.signature;

    string snapshot_path;
    bool allow_incremental_clone = false; // not used
    std::vector<string> snapshot_files;
    Status status = engine.snapshot_mgr()->make_snapshot(snapshot_request, &snapshot_path,
                                                         &allow_incremental_clone);
    if (status.ok() && snapshot_request.__isset.list_files) {
        // list and save all snapshot files
        // snapshot_path like: data/snapshot/20180417205230.1.86400
        // we need to add subdir: tablet_id/schema_hash/
        std::vector<io::FileInfo> files;
        bool exists = true;
        io::Path path = fmt::format("{}/{}/{}/", snapshot_path, snapshot_request.tablet_id,
                                    snapshot_request.schema_hash);
        status = io::global_local_filesystem()->list(path, true, &files, &exists);
        if (status.ok()) {
            for (auto& file : files) {
                snapshot_files.push_back(file.file_name);
            }
        }
    }
    if (!status.ok()) {
        LOG_WARNING("failed to make snapshot")
                .tag("signature", req.signature)
                .tag("tablet_id", snapshot_request.tablet_id)
                .tag("version", snapshot_request.version)
                .error(status);
    } else {
        LOG_INFO("successfully make snapshot")
                .tag("signature", req.signature)
                .tag("tablet_id", snapshot_request.tablet_id)
                .tag("version", snapshot_request.version)
                .tag("snapshot_path", snapshot_path);
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_snapshot_path(snapshot_path);
    finish_task_request.__set_snapshot_files(snapshot_files);
    finish_task_request.__set_task_status(status.to_thrift());

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void release_snapshot_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& release_snapshot_request = req.release_snapshot_req;

    LOG(INFO) << "get release snapshot task. signature=" << req.signature;

    const string& snapshot_path = release_snapshot_request.snapshot_path;
    Status status = engine.snapshot_mgr()->release_snapshot(snapshot_path);
    if (!status.ok()) {
        LOG_WARNING("failed to release snapshot")
                .tag("signature", req.signature)
                .tag("snapshot_path", snapshot_path)
                .error(status);
    } else {
        LOG_INFO("successfully release snapshot")
                .tag("signature", req.signature)
                .tag("snapshot_path", snapshot_path);
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void move_dir_callback(StorageEngine& engine, ExecEnv* env, const TAgentTaskRequest& req) {
    const auto& move_dir_req = req.move_dir_req;

    LOG(INFO) << "get move dir task. signature=" << req.signature
              << ", job_id=" << move_dir_req.job_id;
    Status status;
    auto tablet = engine.tablet_manager()->get_tablet(move_dir_req.tablet_id);
    if (tablet == nullptr) {
        status = Status::InvalidArgument("Could not find tablet");
    } else {
        SnapshotLoader loader(engine, env, move_dir_req.job_id, move_dir_req.tablet_id);
        status = loader.move(move_dir_req.src, tablet, true);
    }

    if (!status.ok()) {
        LOG_WARNING("failed to move dir")
                .tag("signature", req.signature)
                .tag("job_id", move_dir_req.job_id)
                .tag("tablet_id", move_dir_req.tablet_id)
                .tag("src", move_dir_req.src)
                .error(status);
    } else {
        LOG_INFO("successfully move dir")
                .tag("signature", req.signature)
                .tag("job_id", move_dir_req.job_id)
                .tag("tablet_id", move_dir_req.tablet_id)
                .tag("src", move_dir_req.src);
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void submit_table_compaction_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& compaction_req = req.compaction_req;

    LOG(INFO) << "get compaction task. signature=" << req.signature
              << ", compaction_type=" << compaction_req.type;

    CompactionType compaction_type;
    if (compaction_req.type == "base") {
        compaction_type = CompactionType::BASE_COMPACTION;
    } else {
        compaction_type = CompactionType::CUMULATIVE_COMPACTION;
    }

    auto tablet_ptr = engine.tablet_manager()->get_tablet(compaction_req.tablet_id);
    if (tablet_ptr != nullptr) {
        auto* data_dir = tablet_ptr->data_dir();
        if (!tablet_ptr->can_do_compaction(data_dir->path_hash(), compaction_type)) {
            LOG(WARNING) << "could not do compaction. tablet_id=" << tablet_ptr->tablet_id()
                         << ", compaction_type=" << compaction_type;
            return;
        }

        Status status = engine.submit_compaction_task(tablet_ptr, compaction_type, false);
        if (!status.ok()) {
            LOG(WARNING) << "failed to submit table compaction task. error=" << status;
        }
    }
}

namespace {

void update_s3_resource(const TStorageResource& param, io::RemoteFileSystemSPtr existed_fs) {
    Status st;
    io::RemoteFileSystemSPtr fs;

    if (!existed_fs) {
        // No such FS instance on BE
        auto res = io::S3FileSystem::create(S3Conf::get_s3_conf(param.s3_storage_param),
                                            std::to_string(param.id));
        if (!res.has_value()) {
            st = std::move(res).error();
        } else {
            fs = std::move(res).value();
        }
    } else {
        DCHECK_EQ(existed_fs->type(), io::FileSystemType::S3) << param.id << ' ' << param.name;
        auto client = static_cast<io::S3FileSystem*>(existed_fs.get())->client_holder();
        auto new_s3_conf = S3Conf::get_s3_conf(param.s3_storage_param);
        S3ClientConf conf {
                .endpoint {},
                .region {},
                .ak = std::move(new_s3_conf.client_conf.ak),
                .sk = std::move(new_s3_conf.client_conf.sk),
                .token = std::move(new_s3_conf.client_conf.token),
                .bucket {},
                .provider = new_s3_conf.client_conf.provider,
        };
        st = client->reset(conf);
        fs = std::move(existed_fs);
    }

    if (!st.ok()) {
        LOG(WARNING) << "update s3 resource failed: " << st;
    } else {
        LOG_INFO("successfully update hdfs resource")
                .tag("resource_id", param.id)
                .tag("resource_name", param.name);
        put_storage_resource(param.id, {std::move(fs)}, param.version);
    }
}

void update_hdfs_resource(const TStorageResource& param, io::RemoteFileSystemSPtr existed_fs) {
    Status st;
    io::RemoteFileSystemSPtr fs;
    std::string root_path =
            param.hdfs_storage_param.__isset.root_path ? param.hdfs_storage_param.root_path : "";

    if (!existed_fs) {
        // No such FS instance on BE
        auto res = io::HdfsFileSystem::create(
                param.hdfs_storage_param, param.hdfs_storage_param.fs_name,
                std::to_string(param.id), nullptr, std::move(root_path));
        if (!res.has_value()) {
            st = std::move(res).error();
        } else {
            fs = std::move(res).value();
        }

    } else {
        DCHECK_EQ(existed_fs->type(), io::FileSystemType::HDFS) << param.id << ' ' << param.name;
        // TODO(plat1ko): update hdfs conf
        fs = std::move(existed_fs);
    }

    if (!st.ok()) {
        LOG(WARNING) << "update hdfs resource failed: " << st;
    } else {
        LOG_INFO("successfully update hdfs resource")
                .tag("resource_id", param.id)
                .tag("resource_name", param.name)
                .tag("root_path", fs->root_path().string());
        put_storage_resource(param.id, {std::move(fs)}, param.version);
    }
}

} // namespace

void push_storage_policy_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& push_storage_policy_req = req.push_storage_policy_req;
    // refresh resource
    for (auto&& param : push_storage_policy_req.resource) {
        io::RemoteFileSystemSPtr fs;
        if (auto existed_resource = get_storage_resource(param.id); existed_resource) {
            if (existed_resource->second >= param.version) {
                // Stale request, ignore
                continue;
            }

            fs = std::move(existed_resource->first.fs);
        }

        if (param.__isset.s3_storage_param) {
            update_s3_resource(param, std::move(fs));
        } else if (param.__isset.hdfs_storage_param) {
            update_hdfs_resource(param, std::move(fs));
        } else {
            LOG(WARNING) << "unknown resource=" << param;
        }
    }
    // drop storage policy
    for (auto policy_id : push_storage_policy_req.dropped_storage_policy) {
        delete_storage_policy(policy_id);
    }
    // refresh storage policy
    for (auto&& storage_policy : push_storage_policy_req.storage_policy) {
        auto existed_storage_policy = get_storage_policy(storage_policy.id);
        if (existed_storage_policy == nullptr ||
            existed_storage_policy->version < storage_policy.version) {
            auto storage_policy1 = std::make_shared<StoragePolicy>();
            storage_policy1->name = storage_policy.name;
            storage_policy1->version = storage_policy.version;
            storage_policy1->cooldown_datetime = storage_policy.cooldown_datetime;
            storage_policy1->cooldown_ttl = storage_policy.cooldown_ttl;
            storage_policy1->resource_id = storage_policy.resource_id;
            LOG_INFO("successfully update storage policy")
                    .tag("storage_policy_id", storage_policy.id)
                    .tag("storage_policy", storage_policy1->to_string());
            put_storage_policy(storage_policy.id, std::move(storage_policy1));
        }
    }
}

void push_cooldown_conf_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& push_cooldown_conf_req = req.push_cooldown_conf;
    for (const auto& cooldown_conf : push_cooldown_conf_req.cooldown_confs) {
        int64_t tablet_id = cooldown_conf.tablet_id;
        TabletSharedPtr tablet = engine.tablet_manager()->get_tablet(tablet_id);
        if (tablet == nullptr) {
            LOG(WARNING) << "failed to get tablet. tablet_id=" << tablet_id;
            continue;
        }
        if (tablet->update_cooldown_conf(cooldown_conf.cooldown_term,
                                         cooldown_conf.cooldown_replica_id) &&
            cooldown_conf.cooldown_replica_id == tablet->replica_id() &&
            tablet->tablet_meta()->cooldown_meta_id().initialized()) {
            Tablet::async_write_cooldown_meta(tablet);
        }
    }
}

void create_tablet_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& create_tablet_req = req.create_tablet_req;
    RuntimeProfile runtime_profile("CreateTablet");
    RuntimeProfile* profile = &runtime_profile;
    MonotonicStopWatch watch;
    watch.start();
    SCOPED_CLEANUP({
        auto elapsed_time = static_cast<int64_t>(watch.elapsed_time());
        if (elapsed_time / 1e9 > config::agent_task_trace_threshold_sec) {
            COUNTER_UPDATE(profile->total_time_counter(), elapsed_time);
            std::stringstream ss;
            profile->pretty_print(&ss);
            LOG(WARNING) << "create tablet cost(s) " << elapsed_time / 1e9 << std::endl << ss.str();
        }
    });
    DorisMetrics::instance()->create_tablet_requests_total->increment(1);
    VLOG_NOTICE << "start to create tablet " << create_tablet_req.tablet_id;

    std::vector<TTabletInfo> finish_tablet_infos;
    VLOG_NOTICE << "create tablet: " << create_tablet_req;
    Status status = engine.create_tablet(create_tablet_req, profile);
    if (!status.ok()) {
        DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
        LOG_WARNING("failed to create tablet, reason={}", status.to_string())
                .tag("signature", req.signature)
                .tag("tablet_id", create_tablet_req.tablet_id)
                .error(status);
    } else {
        increase_report_version();
        // get path hash of the created tablet
        TabletSharedPtr tablet;
        {
            SCOPED_TIMER(ADD_TIMER(profile, "GetTablet"));
            tablet = engine.tablet_manager()->get_tablet(create_tablet_req.tablet_id);
        }
        DCHECK(tablet != nullptr);
        TTabletInfo tablet_info;
        tablet_info.tablet_id = tablet->table_id();
        tablet_info.schema_hash = tablet->schema_hash();
        tablet_info.version = create_tablet_req.version;
        // Useless but it is a required field in TTabletInfo
        tablet_info.version_hash = 0;
        tablet_info.row_count = 0;
        tablet_info.data_size = 0;
        tablet_info.__set_path_hash(tablet->data_dir()->path_hash());
        tablet_info.__set_replica_id(tablet->replica_id());
        finish_tablet_infos.push_back(tablet_info);
        LOG_INFO("successfully create tablet")
                .tag("signature", req.signature)
                .tag("tablet_id", create_tablet_req.tablet_id);
    }
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_finish_tablet_infos(finish_tablet_infos);
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_report_version(s_report_version);
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());
    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void drop_tablet_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& drop_tablet_req = req.drop_tablet_req;
    Status status;
    auto dropped_tablet = engine.tablet_manager()->get_tablet(drop_tablet_req.tablet_id, false);
    if (dropped_tablet != nullptr) {
        status = engine.tablet_manager()->drop_tablet(drop_tablet_req.tablet_id,
                                                      drop_tablet_req.replica_id,
                                                      drop_tablet_req.is_drop_table_or_partition);
    } else {
        status = Status::NotFound("could not find tablet {}", drop_tablet_req.tablet_id);
    }
    if (status.ok()) {
        // if tablet is dropped by fe, then the related txn should also be removed
        engine.txn_manager()->force_rollback_tablet_related_txns(
                dropped_tablet->data_dir()->get_meta(), drop_tablet_req.tablet_id,
                dropped_tablet->tablet_uid());
        LOG_INFO("successfully drop tablet")
                .tag("signature", req.signature)
                .tag("tablet_id", drop_tablet_req.tablet_id);
    } else {
        LOG_WARNING("failed to drop tablet")
                .tag("signature", req.signature)
                .tag("tablet_id", drop_tablet_req.tablet_id)
                .error(status);
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void push_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& push_req = req.push_req;

    LOG(INFO) << "get push task. signature=" << req.signature
              << " push_type=" << push_req.push_type;
    std::vector<TTabletInfo> tablet_infos;

    EngineBatchLoadTask engine_task(engine, const_cast<TPushReq&>(push_req), &tablet_infos);
    SCOPED_ATTACH_TASK(engine_task.mem_tracker());
    auto status = engine_task.execute();

    // Return result to fe
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    if (push_req.push_type == TPushType::DELETE) {
        finish_task_request.__set_request_version(push_req.version);
    }

    if (status.ok()) {
        LOG_INFO("successfully execute push task")
                .tag("signature", req.signature)
                .tag("tablet_id", push_req.tablet_id)
                .tag("push_type", push_req.push_type);
        increase_report_version();
        finish_task_request.__set_finish_tablet_infos(tablet_infos);
    } else {
        LOG_WARNING("failed to execute push task")
                .tag("signature", req.signature)
                .tag("tablet_id", push_req.tablet_id)
                .tag("push_type", push_req.push_type)
                .error(status);
    }
    finish_task_request.__set_task_status(status.to_thrift());
    finish_task_request.__set_report_version(s_report_version);

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void cloud_push_callback(CloudStorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& push_req = req.push_req;

    LOG(INFO) << "get push task. signature=" << req.signature
              << " push_type=" << push_req.push_type;

    // Return result to fe
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);

    // Only support DELETE in cloud mode now
    if (push_req.push_type != TPushType::DELETE) {
        finish_task_request.__set_task_status(
                Status::NotSupported("push_type {} not is supported",
                                     std::to_string(push_req.push_type))
                        .to_thrift());
        return;
    }

    finish_task_request.__set_request_version(push_req.version);

    DorisMetrics::instance()->delete_requests_total->increment(1);
    auto st = CloudDeleteTask::execute(engine, req.push_req);
    if (st.ok()) {
        LOG_INFO("successfully execute push task")
                .tag("signature", req.signature)
                .tag("tablet_id", push_req.tablet_id)
                .tag("push_type", push_req.push_type);
        increase_report_version();
        auto& tablet_info = finish_task_request.finish_tablet_infos.emplace_back();
        // Just need tablet_id
        tablet_info.tablet_id = push_req.tablet_id;
        finish_task_request.__isset.finish_tablet_infos = true;
    } else {
        DorisMetrics::instance()->delete_requests_failed->increment(1);
        LOG_WARNING("failed to execute push task")
                .tag("signature", req.signature)
                .tag("tablet_id", push_req.tablet_id)
                .tag("push_type", push_req.push_type)
                .error(st);
    }

    finish_task_request.__set_task_status(st.to_thrift());
    finish_task_request.__set_report_version(s_report_version);

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

PublishVersionWorkerPool::PublishVersionWorkerPool(StorageEngine& engine)
        : TaskWorkerPool("PUBLISH_VERSION", config::publish_version_worker_count,
                         [this](const TAgentTaskRequest& task) { publish_version_callback(task); }),
          _engine(engine) {}

PublishVersionWorkerPool::~PublishVersionWorkerPool() = default;

void PublishVersionWorkerPool::publish_version_callback(const TAgentTaskRequest& req) {
    const auto& publish_version_req = req.publish_version_req;
    DorisMetrics::instance()->publish_task_request_total->increment(1);
    VLOG_NOTICE << "get publish version task. signature=" << req.signature;

    std::set<TTabletId> error_tablet_ids;
    std::map<TTabletId, TVersion> succ_tablets;
    // partition_id, tablet_id, publish_version
    std::vector<std::tuple<int64_t, int64_t, int64_t>> discontinuous_version_tablets;
    std::map<TTableId, std::map<TTabletId, int64_t>> table_id_to_tablet_id_to_num_delta_rows;
    uint32_t retry_time = 0;
    Status status;
    constexpr uint32_t PUBLISH_VERSION_MAX_RETRY = 3;
    while (retry_time < PUBLISH_VERSION_MAX_RETRY) {
        succ_tablets.clear();
        error_tablet_ids.clear();
        table_id_to_tablet_id_to_num_delta_rows.clear();
        EnginePublishVersionTask engine_task(_engine, publish_version_req, &error_tablet_ids,
                                             &succ_tablets, &discontinuous_version_tablets,
                                             &table_id_to_tablet_id_to_num_delta_rows);
        SCOPED_ATTACH_TASK(engine_task.mem_tracker());
        status = engine_task.execute();
        if (status.ok()) {
            break;
        }

        if (status.is<PUBLISH_VERSION_NOT_CONTINUOUS>()) {
            // there are too many missing versions, it has been be added to async
            // publish task, so no need to retry here.
            if (discontinuous_version_tablets.empty()) {
                break;
            }
            LOG_EVERY_SECOND(INFO) << "wait for previous publish version task to be done, "
                                   << "transaction_id: " << publish_version_req.transaction_id;

            int64_t time_elapsed = time(nullptr) - req.recv_time;
            if (time_elapsed > config::publish_version_task_timeout_s) {
                LOG(INFO) << "task elapsed " << time_elapsed
                          << " seconds since it is inserted to queue, it is timeout";
                break;
            }

            // Version not continuous, put to queue and wait pre version publish task execute
            PUBLISH_VERSION_count << 1;
            auto st = _thread_pool->submit_func([this, req] {
                this->publish_version_callback(req);
                PUBLISH_VERSION_count << -1;
            });
            if (!st.ok()) [[unlikely]] {
                PUBLISH_VERSION_count << -1;
                status = std::move(st);
            } else {
                return;
            }
        }

        LOG_WARNING("failed to publish version")
                .tag("transaction_id", publish_version_req.transaction_id)
                .tag("error_tablets_num", error_tablet_ids.size())
                .tag("retry_time", retry_time)
                .error(status);
        ++retry_time;
    }

    for (auto& item : discontinuous_version_tablets) {
        _engine.add_async_publish_task(std::get<0>(item), std::get<1>(item), std::get<2>(item),
                                       publish_version_req.transaction_id, false);
    }
    TFinishTaskRequest finish_task_request;
    if (!status.ok()) [[unlikely]] {
        DorisMetrics::instance()->publish_task_failed_total->increment(1);
        // if publish failed, return failed, FE will ignore this error and
        // check error tablet ids and FE will also republish this task
        LOG_WARNING("failed to publish version")
                .tag("signature", req.signature)
                .tag("transaction_id", publish_version_req.transaction_id)
                .tag("error_tablets_num", error_tablet_ids.size())
                .error(status);
    } else {
        if (!config::disable_auto_compaction &&
            !GlobalMemoryArbitrator::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE)) {
            for (auto [tablet_id, _] : succ_tablets) {
                TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(tablet_id);
                if (tablet != nullptr) {
                    if (!tablet->tablet_meta()->tablet_schema()->disable_auto_compaction()) {
                        tablet->published_count.fetch_add(1);
                        int64_t published_count = tablet->published_count.load();
                        if (tablet->exceed_version_limit(config::max_tablet_version_num * 2 / 3) &&
                            published_count % 20 == 0) {
                            auto st = _engine.submit_compaction_task(
                                    tablet, CompactionType::CUMULATIVE_COMPACTION, true, false);
                            if (!st.ok()) [[unlikely]] {
                                LOG(WARNING) << "trigger compaction failed, tablet_id=" << tablet_id
                                             << ", published=" << published_count << " : " << st;
                            } else {
                                LOG(INFO) << "trigger compaction succ, tablet_id:" << tablet_id
                                          << ", published:" << published_count;
                            }
                        }
                    }
                } else {
                    LOG(WARNING) << "trigger compaction failed, tablet_id:" << tablet_id;
                }
            }
        }
        uint32_t cost_second = time(nullptr) - req.recv_time;
        g_publish_version_latency << cost_second;
        LOG_INFO("successfully publish version")
                .tag("signature", req.signature)
                .tag("transaction_id", publish_version_req.transaction_id)
                .tag("tablets_num", succ_tablets.size())
                .tag("cost(s)", cost_second);
    }

    status.to_thrift(&finish_task_request.task_status);
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_report_version(s_report_version);
    finish_task_request.__set_succ_tablets(succ_tablets);
    finish_task_request.__set_error_tablet_ids(
            std::vector<TTabletId>(error_tablet_ids.begin(), error_tablet_ids.end()));
    finish_task_request.__set_table_id_to_tablet_id_to_delta_num_rows(
            table_id_to_tablet_id_to_num_delta_rows);
    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void clear_transaction_task_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& clear_transaction_task_req = req.clear_transaction_task_req;
    LOG(INFO) << "get clear transaction task. signature=" << req.signature
              << ", transaction_id=" << clear_transaction_task_req.transaction_id
              << ", partition_id_size=" << clear_transaction_task_req.partition_id.size();

    Status status;

    if (clear_transaction_task_req.transaction_id > 0) {
        // transaction_id should be greater than zero.
        // If it is not greater than zero, no need to execute
        // the following clear_transaction_task() function.
        if (!clear_transaction_task_req.partition_id.empty()) {
            engine.clear_transaction_task(clear_transaction_task_req.transaction_id,
                                          clear_transaction_task_req.partition_id);
        } else {
            engine.clear_transaction_task(clear_transaction_task_req.transaction_id);
        }
        LOG(INFO) << "finish to clear transaction task. signature=" << req.signature
                  << ", transaction_id=" << clear_transaction_task_req.transaction_id;
    } else {
        LOG(WARNING) << "invalid transaction id " << clear_transaction_task_req.transaction_id
                     << ". signature= " << req.signature;
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_task_status(status.to_thrift());
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void alter_tablet_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    int64_t signature = req.signature;
    LOG(INFO) << "get alter table task, signature: " << signature;
    bool is_task_timeout = false;
    if (req.__isset.recv_time) {
        int64_t time_elapsed = time(nullptr) - req.recv_time;
        if (time_elapsed > config::report_task_interval_seconds * 20) {
            LOG(INFO) << "task elapsed " << time_elapsed
                      << " seconds since it is inserted to queue, it is timeout";
            is_task_timeout = true;
        }
    }
    if (!is_task_timeout) {
        TFinishTaskRequest finish_task_request;
        TTaskType::type task_type = req.task_type;
        alter_tablet(engine, req, signature, task_type, &finish_task_request);
        finish_task(finish_task_request);
    }
    doris::g_fragment_executing_count << -1;
    int64 now = duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    g_fragment_last_active_time.set_value(now);
    remove_task_info(req.task_type, req.signature);
}

void alter_cloud_tablet_callback(CloudStorageEngine& engine, const TAgentTaskRequest& req) {
    int64_t signature = req.signature;
    LOG(INFO) << "get alter table task, signature: " << signature;
    bool is_task_timeout = false;
    if (req.__isset.recv_time) {
        int64_t time_elapsed = time(nullptr) - req.recv_time;
        if (time_elapsed > config::report_task_interval_seconds * 20) {
            LOG(INFO) << "task elapsed " << time_elapsed
                      << " seconds since it is inserted to queue, it is timeout";
            is_task_timeout = true;
        }
    }
    if (!is_task_timeout) {
        TFinishTaskRequest finish_task_request;
        TTaskType::type task_type = req.task_type;
        alter_cloud_tablet(engine, req, signature, task_type, &finish_task_request);
        finish_task(finish_task_request);
    }
    doris::g_fragment_executing_count << -1;
    int64 now = duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    g_fragment_last_active_time.set_value(now);
    remove_task_info(req.task_type, req.signature);
}

void gc_binlog_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    std::unordered_map<int64_t, int64_t> gc_tablet_infos;
    if (!req.__isset.gc_binlog_req) {
        LOG(WARNING) << "gc binlog task is not valid";
        return;
    }
    if (!req.gc_binlog_req.__isset.tablet_gc_binlog_infos) {
        LOG(WARNING) << "gc binlog task tablet_gc_binlog_infos is not valid";
        return;
    }

    const auto& tablet_gc_binlog_infos = req.gc_binlog_req.tablet_gc_binlog_infos;
    for (auto&& tablet_info : tablet_gc_binlog_infos) {
        // gc_tablet_infos.emplace(tablet_info.tablet_id, tablet_info.schema_hash);
        gc_tablet_infos.emplace(tablet_info.tablet_id, tablet_info.version);
    }

    engine.gc_binlogs(gc_tablet_infos);
}

void visible_version_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const TVisibleVersionReq& visible_version_req = req.visible_version_req;
    engine.tablet_manager()->update_partitions_visible_version(
            visible_version_req.partition_version);
}

void clone_callback(StorageEngine& engine, const TMasterInfo& master_info,
                    const TAgentTaskRequest& req) {
    const auto& clone_req = req.clone_req;

    DorisMetrics::instance()->clone_requests_total->increment(1);
    LOG(INFO) << "get clone task. signature=" << req.signature;

    std::vector<TTabletInfo> tablet_infos;
    EngineCloneTask engine_task(engine, clone_req, master_info, req.signature, &tablet_infos);
    SCOPED_ATTACH_TASK(engine_task.mem_tracker());
    auto status = engine_task.execute();
    // Return result to fe
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());

    if (!status.ok()) {
        DorisMetrics::instance()->clone_requests_failed->increment(1);
        LOG_WARNING("failed to clone tablet")
                .tag("signature", req.signature)
                .tag("tablet_id", clone_req.tablet_id)
                .error(status);
    } else {
        LOG_INFO("successfully clone tablet")
                .tag("signature", req.signature)
                .tag("tablet_id", clone_req.tablet_id);
        if (engine_task.is_new_tablet()) {
            increase_report_version();
            finish_task_request.__set_report_version(s_report_version);
        }
        finish_task_request.__set_finish_tablet_infos(tablet_infos);
    }

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void storage_medium_migrate_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    const auto& storage_medium_migrate_req = req.storage_medium_migrate_req;

    // check request and get info
    TabletSharedPtr tablet;
    DataDir* dest_store = nullptr;

    auto status = check_migrate_request(engine, storage_medium_migrate_req, tablet, &dest_store);
    if (status.ok()) {
        EngineStorageMigrationTask engine_task(engine, tablet, dest_store);
        SCOPED_ATTACH_TASK(engine_task.mem_tracker());
        status = engine_task.execute();
    }
    // fe should ignore this err
    if (status.is<FILE_ALREADY_EXIST>()) {
        status = Status::OK();
    }
    if (!status.ok()) {
        LOG_WARNING("failed to migrate storage medium")
                .tag("signature", req.signature)
                .tag("tablet_id", storage_medium_migrate_req.tablet_id)
                .error(status);
    } else {
        LOG_INFO("successfully migrate storage medium")
                .tag("signature", req.signature)
                .tag("tablet_id", storage_medium_migrate_req.tablet_id);
    }

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_task_status(status.to_thrift());

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void calc_delete_bitmap_callback(CloudStorageEngine& engine, const TAgentTaskRequest& req) {
    std::vector<TTabletId> error_tablet_ids;
    std::vector<TTabletId> succ_tablet_ids;
    Status status;
    error_tablet_ids.clear();
    const auto& calc_delete_bitmap_req = req.calc_delete_bitmap_req;
    CloudEngineCalcDeleteBitmapTask engine_task(engine, calc_delete_bitmap_req, &error_tablet_ids,
                                                &succ_tablet_ids);
    SCOPED_ATTACH_TASK(engine_task.mem_tracker());
    status = engine_task.execute();

    TFinishTaskRequest finish_task_request;
    if (!status) {
        DorisMetrics::instance()->publish_task_failed_total->increment(1);
        LOG_WARNING("failed to calculate delete bitmap")
                .tag("signature", req.signature)
                .tag("transaction_id", calc_delete_bitmap_req.transaction_id)
                .tag("error_tablets_num", error_tablet_ids.size())
                .error(status);
    }

    status.to_thrift(&finish_task_request.task_status);
    finish_task_request.__set_backend(BackendOptions::get_local_backend());
    finish_task_request.__set_task_type(req.task_type);
    finish_task_request.__set_signature(req.signature);
    finish_task_request.__set_report_version(s_report_version);
    finish_task_request.__set_error_tablet_ids(error_tablet_ids);
    finish_task_request.__set_resp_partitions(calc_delete_bitmap_req.partitions);

    finish_task(finish_task_request);
    remove_task_info(req.task_type, req.signature);
}

void clean_trash_callback(StorageEngine& engine, const TAgentTaskRequest& req) {
    LOG(INFO) << "clean trash start";
    DBUG_EXECUTE_IF("clean_trash_callback_sleep", { sleep(100); })
    static_cast<void>(engine.start_trash_sweep(nullptr, true));
    static_cast<void>(engine.notify_listener("REPORT_DISK_STATE"));
    LOG(INFO) << "clean trash finish";
}

void clean_udf_cache_callback(const TAgentTaskRequest& req) {
    if (doris::config::enable_java_support) {
        LOG(INFO) << "clean udf cache start: " << req.clean_udf_cache_req.function_signature;
        static_cast<void>(
                JniUtil::clean_udf_class_load_cache(req.clean_udf_cache_req.function_signature));
        LOG(INFO) << "clean udf cache  finish: " << req.clean_udf_cache_req.function_signature;
    }
}

} // namespace doris
