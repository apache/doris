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

#include <pthread.h>
#include <sys/stat.h>

#include <boost/lexical_cast.hpp>
#include <chrono>
#include <csignal>
#include <ctime>
#include <sstream>
#include <string>

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy_mgr.h"
#include "olap/tablet.h"
#include "olap/task/engine_alter_tablet_task.h"
#include "olap/task/engine_batch_load_task.h"
#include "olap/task/engine_checksum_task.h"
#include "olap/task/engine_clone_task.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/task/engine_storage_migration_task.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "runtime/snapshot_loader.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/file_utils.h"
#include "util/random.h"
#include "util/scoped_cleanup.h"
#include "util/stopwatch.hpp"
#include "util/threadpool.h"
#include "util/trace.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(agent_task_queue_size, MetricUnit::NOUNIT);

const uint32_t TASK_FINISH_MAX_RETRY = 3;
const uint32_t PUBLISH_VERSION_MAX_RETRY = 3;
const int64_t PUBLISH_TIMEOUT_SEC = 10;

std::atomic_ulong TaskWorkerPool::_s_report_version(time(nullptr) * 10000);
std::mutex TaskWorkerPool::_s_task_signatures_lock;
std::map<TTaskType::type, std::set<int64_t>> TaskWorkerPool::_s_task_signatures;
FrontendServiceClientCache TaskWorkerPool::_master_service_client_cache;

TaskWorkerPool::TaskWorkerPool(const TaskWorkerType task_worker_type, ExecEnv* env,
                               const TMasterInfo& master_info, ThreadModel thread_model)
        : _master_info(master_info),
          _agent_utils(new AgentUtils()),
          _master_client(new MasterServerClient(_master_info, &_master_service_client_cache)),
          _env(env),
          _stop_background_threads_latch(1),
          _is_work(false),
          _thread_model(thread_model),
          _is_doing_work(false),
          _task_worker_type(task_worker_type) {
    _backend.__set_host(BackendOptions::get_localhost());
    _backend.__set_be_port(config::be_port);
    _backend.__set_http_port(config::webserver_port);

    string task_worker_type_name = TYPE_STRING(task_worker_type);
    _name = strings::Substitute("TaskWorkerPool.$0", task_worker_type_name);

    _metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            task_worker_type_name, {{"type", task_worker_type_name}});
    REGISTER_ENTITY_HOOK_METRIC(_metric_entity, this, agent_task_queue_size, [this]() -> uint64_t {
        if (_thread_model == ThreadModel::SINGLE_THREAD) {
            return _is_doing_work.load();
        } else {
            std::lock_guard<std::mutex> lock(_worker_thread_lock);
            return _tasks.size();
        }
    });
}

TaskWorkerPool::~TaskWorkerPool() {
    _stop_background_threads_latch.count_down();
    stop();

    DEREGISTER_ENTITY_HOOK_METRIC(_metric_entity, agent_task_queue_size);
    DorisMetrics::instance()->metric_registry()->deregister_entity(_metric_entity);
}

void TaskWorkerPool::start() {
    // Init task pool and task workers
    _is_work = true;
    if (_thread_model == ThreadModel::SINGLE_THREAD) {
        _worker_count = 1;
    }
    std::function<void()> cb;
    switch (_task_worker_type) {
    case TaskWorkerType::CREATE_TABLE:
        _worker_count = config::create_tablet_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_create_tablet_worker_thread_callback, this);
        break;
    case TaskWorkerType::DROP_TABLE:
        _worker_count = config::drop_tablet_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_drop_tablet_worker_thread_callback, this);
        break;
    case TaskWorkerType::PUSH:
    case TaskWorkerType::REALTIME_PUSH:
        _worker_count =
                config::push_worker_count_normal_priority + config::push_worker_count_high_priority;
        cb = std::bind<void>(&TaskWorkerPool::_push_worker_thread_callback, this);
        break;
    case TaskWorkerType::PUBLISH_VERSION:
        _worker_count = config::publish_version_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_publish_version_worker_thread_callback, this);
        break;
    case TaskWorkerType::CLEAR_TRANSACTION_TASK:
        _worker_count = config::clear_transaction_task_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_clear_transaction_task_worker_thread_callback, this);
        break;
    case TaskWorkerType::DELETE:
        _worker_count = config::delete_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_push_worker_thread_callback, this);
        break;
    case TaskWorkerType::ALTER_TABLE:
        _worker_count = config::alter_tablet_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_alter_tablet_worker_thread_callback, this);
        break;
    case TaskWorkerType::CLONE:
        _worker_count = config::clone_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_clone_worker_thread_callback, this);
        break;
    case TaskWorkerType::STORAGE_MEDIUM_MIGRATE:
        _worker_count = config::storage_medium_migrate_count;
        cb = std::bind<void>(&TaskWorkerPool::_storage_medium_migrate_worker_thread_callback, this);
        break;
    case TaskWorkerType::CHECK_CONSISTENCY:
        _worker_count = config::check_consistency_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_check_consistency_worker_thread_callback, this);
        break;
    case TaskWorkerType::REPORT_TASK:
        cb = std::bind<void>(&TaskWorkerPool::_report_task_worker_thread_callback, this);
        break;
    case TaskWorkerType::REPORT_DISK_STATE:
        cb = std::bind<void>(&TaskWorkerPool::_report_disk_state_worker_thread_callback, this);
        break;
    case TaskWorkerType::REPORT_OLAP_TABLE:
        cb = std::bind<void>(&TaskWorkerPool::_report_tablet_worker_thread_callback, this);
        break;
    case TaskWorkerType::UPLOAD:
        _worker_count = config::upload_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_upload_worker_thread_callback, this);
        break;
    case TaskWorkerType::DOWNLOAD:
        _worker_count = config::download_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_download_worker_thread_callback, this);
        break;
    case TaskWorkerType::MAKE_SNAPSHOT:
        _worker_count = config::make_snapshot_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_make_snapshot_thread_callback, this);
        break;
    case TaskWorkerType::RELEASE_SNAPSHOT:
        _worker_count = config::release_snapshot_worker_count;
        cb = std::bind<void>(&TaskWorkerPool::_release_snapshot_thread_callback, this);
        break;
    case TaskWorkerType::MOVE:
        _worker_count = 1;
        cb = std::bind<void>(&TaskWorkerPool::_move_dir_thread_callback, this);
        break;
    case TaskWorkerType::UPDATE_TABLET_META_INFO:
        _worker_count = 1;
        cb = std::bind<void>(&TaskWorkerPool::_update_tablet_meta_worker_thread_callback, this);
        break;
    case TaskWorkerType::SUBMIT_TABLE_COMPACTION:
        _worker_count = 1;
        cb = std::bind<void>(&TaskWorkerPool::_submit_table_compaction_worker_thread_callback,
                             this);
        break;
    case TaskWorkerType::REFRESH_STORAGE_POLICY:
        cb = std::bind<void>(
                &TaskWorkerPool::_storage_refresh_storage_policy_worker_thread_callback, this);
        break;
    case TaskWorkerType::UPDATE_STORAGE_POLICY:
        _worker_count = 1;
        cb = std::bind<void>(&TaskWorkerPool::_storage_update_storage_policy_worker_thread_callback,
                             this);
        break;
    default:
        // pass
        break;
    }
    CHECK(_thread_model == ThreadModel::MULTI_THREADS || _worker_count == 1);

#ifndef BE_TEST
    ThreadPoolBuilder(_name)
            .set_min_threads(_worker_count)
            .set_max_threads(_worker_count)
            .build(&_thread_pool);

    for (int i = 0; i < _worker_count; i++) {
        auto st = _thread_pool->submit_func(cb);
        CHECK(st.ok()) << st.to_string();
    }
#endif
}

void TaskWorkerPool::stop() {
    {
        std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
        _is_work = false;
        _worker_thread_condition_variable.notify_all();
    }
    _thread_pool->shutdown();
}

void TaskWorkerPool::submit_task(const TAgentTaskRequest& task) {
    const TTaskType::type task_type = task.task_type;
    int64_t signature = task.signature;

    std::string type_str;
    EnumToString(TTaskType, task_type, type_str);
    VLOG_CRITICAL << "submitting task. type=" << type_str << ", signature=" << signature;

    if (_register_task_info(task_type, signature)) {
        // Set the receiving time of task so that we can determine whether it is timed out later
        (const_cast<TAgentTaskRequest&>(task)).__set_recv_time(time(nullptr));
        size_t task_count_in_queue = 0;
        {
            std::lock_guard<std::mutex> worker_thread_lock(_worker_thread_lock);
            _tasks.push_back(task);
            task_count_in_queue = _tasks.size();
            _worker_thread_condition_variable.notify_one();
        }
        LOG_INFO("successfully submit task")
                .tag("type", type_str)
                .tag("signature", signature)
                .tag("queue_size", task_count_in_queue);
    } else {
        LOG_WARNING("failed to register task").tag("type", type_str).tag("signature", signature);
    }
}

void TaskWorkerPool::notify_thread() {
    _worker_thread_condition_variable.notify_one();
    VLOG_CRITICAL << "notify task worker pool: " << _name;
}

bool TaskWorkerPool::_register_task_info(const TTaskType::type task_type, int64_t signature) {
    lock_guard<std::mutex> task_signatures_lock(_s_task_signatures_lock);
    set<int64_t>& signature_set = _s_task_signatures[task_type];
    return signature_set.insert(signature).second;
}

void TaskWorkerPool::_remove_task_info(const TTaskType::type task_type, int64_t signature) {
    size_t queue_size;
    {
        lock_guard<std::mutex> task_signatures_lock(_s_task_signatures_lock);
        set<int64_t>& signature_set = _s_task_signatures[task_type];
        signature_set.erase(signature);
        queue_size = signature_set.size();
    }

    std::string type_str;
    EnumToString(TTaskType, task_type, type_str);
    VLOG_NOTICE << "remove task info. type=" << type_str << ", signature=" << signature
                << ", queue_size=" << queue_size;
    TRACE("remove task info");
}

void TaskWorkerPool::_finish_task(const TFinishTaskRequest& finish_task_request) {
    // Return result to FE
    TMasterResult result;
    uint32_t try_time = 0;

    while (try_time < TASK_FINISH_MAX_RETRY) {
        DorisMetrics::instance()->finish_task_requests_total->increment(1);
        Status client_status = _master_client->finish_task(finish_task_request, &result);

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
        sleep(config::sleep_one_second);
    }
    TRACE("finish task");
}

uint32_t TaskWorkerPool::_get_next_task_index(int32_t thread_count,
                                              std::deque<TAgentTaskRequest>& tasks,
                                              TPriority::type priority) {
    int32_t index = -1;
    deque<TAgentTaskRequest>::size_type task_count = tasks.size();
    for (uint32_t i = 0; i < task_count; ++i) {
        TAgentTaskRequest task = tasks[i];
        if (priority == TPriority::HIGH) {
            if (task.__isset.priority && task.priority == TPriority::HIGH) {
                index = i;
                break;
            }
        }
    }

    if (index == -1) {
        if (priority == TPriority::HIGH) {
            return index;
        }

        index = 0;
    }

    return index;
}

void TaskWorkerPool::_create_tablet_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TCreateTabletReq create_tablet_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            create_tablet_req = agent_task_req.create_tablet_req;
            _tasks.pop_front();
        }

        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (watch.elapsed_time() / 1e9 > config::agent_task_trace_threshold_sec) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        DorisMetrics::instance()->create_tablet_requests_total->increment(1);
        TRACE("start to create tablet $0", create_tablet_req.tablet_id);

        std::vector<TTabletInfo> finish_tablet_infos;
        VLOG_NOTICE << "create tablet: " << create_tablet_req;
        Status status = _env->storage_engine()->create_tablet(create_tablet_req);
        if (!status.ok()) {
            DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
            LOG_WARNING("failed to create tablet")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", create_tablet_req.tablet_id)
                    .error(status);
        } else {
            ++_s_report_version;
            // get path hash of the created tablet
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    create_tablet_req.tablet_id);
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
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", create_tablet_req.tablet_id);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_finish_tablet_infos(finish_tablet_infos);
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_report_version(_s_report_version);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_drop_tablet_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TDropTabletReq drop_tablet_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            drop_tablet_req = agent_task_req.drop_tablet_req;
            _tasks.pop_front();
        }

        Status status;
        TabletSharedPtr dropped_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                drop_tablet_req.tablet_id, false);
        if (dropped_tablet != nullptr) {
            status = StorageEngine::instance()->tablet_manager()->drop_tablet(
                    drop_tablet_req.tablet_id, drop_tablet_req.replica_id,
                    drop_tablet_req.is_drop_table_or_partition);
        } else {
            status = Status::NotFound("could not find tablet {}", drop_tablet_req.tablet_id);
        }
        if (status.ok()) {
            // if tablet is dropped by fe, then the related txn should also be removed
            StorageEngine::instance()->txn_manager()->force_rollback_tablet_related_txns(
                    dropped_tablet->data_dir()->get_meta(), drop_tablet_req.tablet_id,
                    drop_tablet_req.schema_hash, dropped_tablet->tablet_uid());
            LOG_INFO("successfully drop tablet")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", drop_tablet_req.tablet_id);
        } else {
            LOG_WARNING("failed to drop tablet")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", drop_tablet_req.tablet_id)
                    .error(status);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_alter_tablet_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            _tasks.pop_front();
        }
        int64_t signature = agent_task_req.signature;
        LOG(INFO) << "get alter table task, signature: " << agent_task_req.signature;
        bool is_task_timeout = false;
        if (agent_task_req.__isset.recv_time) {
            int64_t time_elapsed = time(nullptr) - agent_task_req.recv_time;
            if (time_elapsed > config::report_task_interval_seconds * 20) {
                LOG(INFO) << "task elapsed " << time_elapsed
                          << " seconds since it is inserted to queue, it is timeout";
                is_task_timeout = true;
            }
        }
        if (!is_task_timeout) {
            TFinishTaskRequest finish_task_request;
            TTaskType::type task_type = agent_task_req.task_type;
            switch (task_type) {
            case TTaskType::ALTER:
                _alter_tablet(agent_task_req, signature, task_type, &finish_task_request);
                break;
            default:
                // pass
                break;
            }
            _finish_task(finish_task_request);
        }
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_alter_tablet(const TAgentTaskRequest& agent_task_req, int64_t signature,
                                   const TTaskType::type task_type,
                                   TFinishTaskRequest* finish_task_request) {
    Status status;

    string process_name;
    switch (task_type) {
    case TTaskType::ALTER:
        process_name = "alter tablet";
        break;
    default:
        std::string task_name;
        EnumToString(TTaskType, task_type, task_name);
        LOG(WARNING) << "schema change type invalid. type: " << task_name
                     << ", signature: " << signature;
        status = Status::NotSupported("Schema change type invalid");
        break;
    }

    // Check last schema change status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    TTabletId new_tablet_id;
    TSchemaHash new_schema_hash = 0;
    if (status.ok()) {
        new_tablet_id = agent_task_req.alter_tablet_req_v2.new_tablet_id;
        new_schema_hash = agent_task_req.alter_tablet_req_v2.new_schema_hash;
        EngineAlterTabletTask engine_task(agent_task_req.alter_tablet_req_v2);
        status = _env->storage_engine()->execute_task(&engine_task);
    }

    if (status.ok()) {
        ++_s_report_version;
    }

    // Return result to fe
    finish_task_request->__set_backend(_backend);
    finish_task_request->__set_report_version(_s_report_version);
    finish_task_request->__set_task_type(task_type);
    finish_task_request->__set_signature(signature);

    std::vector<TTabletInfo> finish_tablet_infos;
    if (status.ok()) {
        TTabletInfo tablet_info;
        status = _get_tablet_info(new_tablet_id, new_schema_hash, signature, &tablet_info);
        if (status.ok()) {
            finish_tablet_infos.push_back(tablet_info);
        }
    }

    if (!status.ok() && status.code() != TStatusCode::NOT_IMPLEMENTED_ERROR) {
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

void TaskWorkerPool::_push_worker_thread_callback() {
    // gen high priority worker thread
    TPriority::type priority = TPriority::NORMAL;
    int32_t push_worker_count_high_priority = config::push_worker_count_high_priority;
    static uint32_t s_worker_count = 0;
    {
        std::lock_guard<std::mutex> worker_thread_lock(_worker_thread_lock);
        if (s_worker_count < push_worker_count_high_priority) {
            ++s_worker_count;
            priority = TPriority::HIGH;
        }
    }

    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TPushReq push_req;
        int32_t index = 0;
        do {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            index = _get_next_task_index(config::push_worker_count_normal_priority +
                                                 config::push_worker_count_high_priority,
                                         _tasks, priority);

            if (index < 0) {
                // there is no high priority task. notify other thread to handle normal task
                _worker_thread_condition_variable.notify_one();
                break;
            }

            agent_task_req = _tasks[index];
            push_req = agent_task_req.push_req;
            _tasks.erase(_tasks.begin() + index);
        } while (false);

        if (index < 0) {
            // there is no high priority task in queue
            sleep(1);
            continue;
        }

        LOG(INFO) << "get push task. signature=" << agent_task_req.signature
                  << ", priority=" << priority << " push_type=" << push_req.push_type;
        std::vector<TTabletInfo> tablet_infos;

        EngineBatchLoadTask engine_task(push_req, &tablet_infos);
        auto status = _env->storage_engine()->execute_task(&engine_task);

        // Return result to fe
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        if (push_req.push_type == TPushType::DELETE) {
            finish_task_request.__set_request_version(push_req.version);
        }

        if (status.ok()) {
            LOG_INFO("successfully execute push task")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", push_req.tablet_id)
                    .tag("push_type", push_req.push_type);
            ++_s_report_version;
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        } else {
            LOG_WARNING("failed to execute push task")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", push_req.tablet_id)
                    .tag("push_type", push_req.push_type)
                    .error(status);
        }
        finish_task_request.__set_task_status(status.to_thrift());
        finish_task_request.__set_report_version(_s_report_version);

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_publish_version_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TPublishVersionRequest publish_version_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            publish_version_req = agent_task_req.publish_version_req;
            _tasks.pop_front();
        }

        DorisMetrics::instance()->publish_task_request_total->increment(1);
        VLOG_NOTICE << "get publish version task. signature=" << agent_task_req.signature;

        std::vector<TTabletId> error_tablet_ids;
        std::vector<TTabletId> succ_tablet_ids;
        uint32_t retry_time = 0;
        Status status;
        bool is_task_timeout = false;
        while (retry_time < PUBLISH_VERSION_MAX_RETRY) {
            error_tablet_ids.clear();
            EnginePublishVersionTask engine_task(publish_version_req, &error_tablet_ids,
                                                 &succ_tablet_ids);
            status = _env->storage_engine()->execute_task(&engine_task);
            if (status.ok()) {
                break;
            } else if (status.precise_code() == OLAP_ERR_PUBLISH_VERSION_NOT_CONTINUOUS) {
                int64_t time_elapsed = time(nullptr) - agent_task_req.recv_time;
                if (time_elapsed > PUBLISH_TIMEOUT_SEC) {
                    LOG(INFO) << "task elapsed " << time_elapsed
                              << " seconds since it is inserted to queue, it is timeout";
                    is_task_timeout = true;
                } else {
                    // version not continuous, put to queue and wait pre version publish
                    // task execute
                    std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
                    _tasks.push_back(agent_task_req);
                    _worker_thread_condition_variable.notify_one();
                }
                break;
            } else {
                LOG_WARNING("failed to publish version")
                        .tag("transaction_id", publish_version_req.transaction_id)
                        .tag("error_tablets_num", error_tablet_ids.size())
                        .tag("retry_time", retry_time)
                        .error(status);
                ++retry_time;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        if (status.precise_code() == OLAP_ERR_PUBLISH_VERSION_NOT_CONTINUOUS && !is_task_timeout) {
            continue;
        }

        TFinishTaskRequest finish_task_request;
        if (!status) {
            DorisMetrics::instance()->publish_task_failed_total->increment(1);
            // if publish failed, return failed, FE will ignore this error and
            // check error tablet ids and FE will also republish this task
            LOG_WARNING("failed to publish version")
                    .tag("signature", agent_task_req.signature)
                    .tag("transaction_id", publish_version_req.transaction_id)
                    .tag("error_tablets_num", error_tablet_ids.size())
                    .error(status);
            finish_task_request.__set_error_tablet_ids(error_tablet_ids);
        } else {
            for (int i = 0; i < succ_tablet_ids.size(); i++) {
                TabletSharedPtr tablet =
                        StorageEngine::instance()->tablet_manager()->get_tablet(succ_tablet_ids[i]);
                if (tablet != nullptr) {
                    tablet->publised_count++;
                    if (tablet->publised_count % 10 == 0) {
                        StorageEngine::instance()->submit_compaction_task(
                                tablet, CompactionType::CUMULATIVE_COMPACTION);
                        LOG(INFO) << "trigger compaction succ, tabletid:" << succ_tablet_ids[i]
                                  << ", publised:" << tablet->publised_count;
                    }
                } else {
                    LOG(WARNING) << "trigger compaction failed, tabletid:" << succ_tablet_ids[i];
                }
            }
            LOG_INFO("successfully publish version")
                    .tag("signature", agent_task_req.signature)
                    .tag("transaction_id", publish_version_req.transaction_id)
                    .tag("tablets_num", succ_tablet_ids.size());
        }

        status.to_thrift(&finish_task_request.task_status);
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_report_version(_s_report_version);

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_clear_transaction_task_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TClearTransactionTaskRequest clear_transaction_task_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            clear_transaction_task_req = agent_task_req.clear_transaction_task_req;
            _tasks.pop_front();
        }
        LOG(INFO) << "get clear transaction task. signature=" << agent_task_req.signature
                  << ", transaction_id=" << clear_transaction_task_req.transaction_id
                  << ", partition_id_size=" << clear_transaction_task_req.partition_id.size();

        Status status;

        if (clear_transaction_task_req.transaction_id > 0) {
            // transaction_id should be greater than zero.
            // If it is not greater than zero, no need to execute
            // the following clear_transaction_task() function.
            if (!clear_transaction_task_req.partition_id.empty()) {
                _env->storage_engine()->clear_transaction_task(
                        clear_transaction_task_req.transaction_id,
                        clear_transaction_task_req.partition_id);
            } else {
                _env->storage_engine()->clear_transaction_task(
                        clear_transaction_task_req.transaction_id);
            }
            LOG(INFO) << "finish to clear transaction task. signature=" << agent_task_req.signature
                      << ", transaction_id=" << clear_transaction_task_req.transaction_id;
        } else {
            LOG(WARNING) << "invalid transaction id " << clear_transaction_task_req.transaction_id
                         << ". signature= " << agent_task_req.signature;
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(status.to_thrift());
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_update_tablet_meta_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TUpdateTabletMetaInfoReq update_tablet_meta_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            update_tablet_meta_req = agent_task_req.update_tablet_meta_info_req;
            _tasks.pop_front();
        }
        LOG(INFO) << "get update tablet meta task. signature=" << agent_task_req.signature;

        Status status;

        for (auto& tablet_meta_info : update_tablet_meta_req.tabletMetaInfos) {
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    tablet_meta_info.tablet_id);
            if (tablet == nullptr) {
                LOG(WARNING) << "could not find tablet when update partition id. tablet_id="
                             << tablet_meta_info.tablet_id
                             << ", schema_hash=" << tablet_meta_info.schema_hash;
                continue;
            }
            std::lock_guard<std::shared_mutex> wrlock(tablet->get_header_lock());
            // update tablet meta
            if (!tablet_meta_info.__isset.meta_type) {
                tablet->set_partition_id(tablet_meta_info.partition_id);
            } else {
                switch (tablet_meta_info.meta_type) {
                case TTabletMetaType::PARTITIONID:
                    tablet->set_partition_id(tablet_meta_info.partition_id);
                    break;
                case TTabletMetaType::INMEMORY:
                    if (tablet_meta_info.storage_policy.empty()) {
                        tablet->tablet_meta()->mutable_tablet_schema()->set_is_in_memory(
                                tablet_meta_info.is_in_memory);
                        // The field is_in_memory should not be in the tablet_schema.
                        // it should be in the tablet_meta.
                        for (auto rowset_meta : tablet->tablet_meta()->all_mutable_rs_metas()) {
                            rowset_meta->tablet_schema()->set_is_in_memory(
                                    tablet_meta_info.is_in_memory);
                        }
                        tablet->get_max_version_schema(wrlock)->set_is_in_memory(
                                tablet_meta_info.is_in_memory);
                    } else {
                        LOG(INFO) << "set tablet cooldown resource "
                                  << tablet_meta_info.storage_policy;
                        tablet->tablet_meta()->set_storage_policy(tablet_meta_info.storage_policy);
                    }
                    break;
                }
            }
            tablet->save_meta();
        }

        LOG(INFO) << "finish update tablet meta task. signature=" << agent_task_req.signature;

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(status.to_thrift());
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_clone_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TCloneReq clone_req;

        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            clone_req = agent_task_req.clone_req;
            _tasks.pop_front();
        }

        DorisMetrics::instance()->clone_requests_total->increment(1);
        LOG(INFO) << "get clone task. signature=" << agent_task_req.signature;

        std::vector<TTabletInfo> tablet_infos;
        EngineCloneTask engine_task(clone_req, _master_info, agent_task_req.signature,
                                    &tablet_infos);
        auto status = _env->storage_engine()->execute_task(&engine_task);
        // Return result to fe
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());

        if (!status.ok()) {
            DorisMetrics::instance()->clone_requests_failed->increment(1);
            LOG_WARNING("failed to clone tablet")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", clone_req.tablet_id)
                    .error(status);
        } else {
            LOG_INFO("successfully clone tablet")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", clone_req.tablet_id);
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        }

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_storage_medium_migrate_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TStorageMediumMigrateReq storage_medium_migrate_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            storage_medium_migrate_req = agent_task_req.storage_medium_migrate_req;
            _tasks.pop_front();
        }

        // check request and get info
        TabletSharedPtr tablet;
        DataDir* dest_store = nullptr;

        auto status = _check_migrate_request(storage_medium_migrate_req, tablet, &dest_store);
        if (status.ok()) {
            EngineStorageMigrationTask engine_task(tablet, dest_store);
            status = _env->storage_engine()->execute_task(&engine_task);
        }
        if (!status.ok()) {
            LOG_WARNING("failed to migrate storage medium")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", storage_medium_migrate_req.tablet_id)
                    .error(status);
        } else {
            LOG_INFO("successfully migrate storage medium")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", storage_medium_migrate_req.tablet_id);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

Status TaskWorkerPool::_check_migrate_request(const TStorageMediumMigrateReq& req,
                                              TabletSharedPtr& tablet, DataDir** dest_store) {
    int64_t tablet_id = req.tablet_id;
    tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::InternalError("could not find tablet {}", tablet_id);
    }

    if (req.__isset.data_dir) {
        // request specify the data dir
        *dest_store = StorageEngine::instance()->get_store(req.data_dir);
        if (*dest_store == nullptr) {
            return Status::InternalError("could not find data dir {}", req.data_dir);
        }
    } else {
        // this is a storage medium
        // get data dir by storage medium

        // judge case when no need to migrate
        uint32_t count = StorageEngine::instance()->available_storage_medium_type_count();
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
        auto stores = StorageEngine::instance()->get_stores_for_create_tablet(storage_medium);
        if (stores.empty()) {
            return Status::InternalError("failed to get root path for create tablet");
        }

        *dest_store = stores[0];
    }
    if (tablet->data_dir()->path() == (*dest_store)->path()) {
        return Status::InternalError("tablet is already on specified path {}",
                                     tablet->data_dir()->path());
    }

    // check local disk capacity
    int64_t tablet_size = tablet->tablet_local_size();
    if ((*dest_store)->reach_capacity_limit(tablet_size)) {
        return Status::InternalError("reach the capacity limit of path {}, tablet_size={}",
                                     (*dest_store)->path(), tablet_size);
    }

    return Status::OK();
}

void TaskWorkerPool::_check_consistency_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TCheckConsistencyReq check_consistency_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            check_consistency_req = agent_task_req.check_consistency_req;
            _tasks.pop_front();
        }

        uint32_t checksum = 0;
        EngineChecksumTask engine_task(check_consistency_req.tablet_id,
                                       check_consistency_req.schema_hash,
                                       check_consistency_req.version, &checksum);
        Status status = _env->storage_engine()->execute_task(&engine_task);
        if (!status.ok()) {
            LOG_WARNING("failed to check consistency")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", check_consistency_req.tablet_id)
                    .error(status);
        } else {
            LOG_INFO("successfully check consistency")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", check_consistency_req.tablet_id)
                    .tag("checksum", checksum);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());
        finish_task_request.__set_tablet_checksum(static_cast<int64_t>(checksum));
        finish_task_request.__set_request_version(check_consistency_req.version);

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_report_task_worker_thread_callback() {
    StorageEngine::instance()->register_report_listener(this);
    TReportRequest request;
    request.__set_backend(_backend);

    while (_is_work) {
        _is_doing_work = false;
        {
            // wait at most report_task_interval_seconds, or being notified
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait_for(
                    worker_thread_lock, std::chrono::seconds(config::report_task_interval_seconds));
        }
        if (!_is_work) {
            break;
        }

        if (_master_info.network_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            LOG(INFO)
                    << "waiting to receive first heartbeat from frontend before doing task report";
            continue;
        }

        _is_doing_work = true;
        // See _random_sleep() comment in _report_disk_state_worker_thread_callback
        _random_sleep(5);
        {
            lock_guard<std::mutex> task_signatures_lock(_s_task_signatures_lock);
            request.__set_tasks(_s_task_signatures);
        }
        _handle_report(request, ReportType::TASK);
    }
    StorageEngine::instance()->deregister_report_listener(this);
}

/// disk state report thread will report disk state at a configurable fix interval.
void TaskWorkerPool::_report_disk_state_worker_thread_callback() {
    StorageEngine::instance()->register_report_listener(this);

    TReportRequest request;
    request.__set_backend(_backend);

    while (_is_work) {
        _is_doing_work = false;
        {
            // wait at most report_disk_state_interval_seconds, or being notified
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait_for(
                    worker_thread_lock,
                    std::chrono::seconds(config::report_disk_state_interval_seconds));
        }
        if (!_is_work) {
            break;
        }

        if (_master_info.network_address.port == 0) {
            // port == 0 means not received heartbeat yet
            LOG(INFO)
                    << "waiting to receive first heartbeat from frontend before doing disk report";
            continue;
        }

        _is_doing_work = true;
        // Random sleep 1~5 seconds before doing report.
        // In order to avoid the problem that the FE receives many report requests at the same time
        // and can not be processed.
        _random_sleep(5);

        std::vector<DataDirInfo> data_dir_infos;
        _env->storage_engine()->get_all_data_dir_info(&data_dir_infos, true /* update */);

        map<string, TDisk> disks;
        for (auto& root_path_info : data_dir_infos) {
            TDisk disk;
            disk.__set_root_path(root_path_info.path);
            disk.__set_path_hash(root_path_info.path_hash);
            disk.__set_storage_medium(root_path_info.storage_medium);
            disk.__set_disk_total_capacity(root_path_info.disk_capacity);
            disk.__set_data_used_capacity(root_path_info.local_used_capacity);
            disk.__set_remote_used_capacity(root_path_info.remote_used_capacity);
            disk.__set_disk_available_capacity(root_path_info.available);
            disk.__set_used(root_path_info.is_used);
            disks[root_path_info.path] = disk;
        }
        request.__set_disks(disks);
        _handle_report(request, ReportType::DISK);
    }
    StorageEngine::instance()->deregister_report_listener(this);
}

void TaskWorkerPool::_report_tablet_worker_thread_callback() {
    StorageEngine::instance()->register_report_listener(this);

    TReportRequest request;
    request.__set_backend(_backend);
    request.__isset.tablets = true;
    while (_is_work) {
        _is_doing_work = false;

        {
            // wait at most report_tablet_interval_seconds, or being notified
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait_for(
                    worker_thread_lock,
                    std::chrono::seconds(config::report_tablet_interval_seconds));
        }
        if (!_is_work) {
            break;
        }

        if (_master_info.network_address.port == 0) {
            // port == 0 means not received heartbeat yet
            LOG(INFO) << "waiting to receive first heartbeat from frontend before doing tablet "
                         "report";
            continue;
        }

        _is_doing_work = true;
        // See _random_sleep() comment in _report_disk_state_worker_thread_callback
        _random_sleep(5);
        request.tablets.clear();
        uint64_t report_version = _s_report_version;
        StorageEngine::instance()->tablet_manager()->build_all_report_tablets_info(
                &request.tablets);
        if (report_version < _s_report_version) {
            // TODO llj This can only reduce the possibility for report error, but can't avoid it.
            // If FE create a tablet in FE meta and send CREATE task to this BE, the tablet may not be included in this
            // report, and the report version has a small probability that it has not been updated in time. When FE
            // receives this report, it is possible to delete the new tablet.
            LOG(WARNING) << "report version " << report_version << " change to "
                         << _s_report_version;
            DorisMetrics::instance()->report_all_tablets_requests_skip->increment(1);
            continue;
        }
        int64_t max_compaction_score =
                std::max(DorisMetrics::instance()->tablet_cumulative_max_compaction_score->value(),
                         DorisMetrics::instance()->tablet_base_max_compaction_score->value());
        request.__set_tablet_max_compaction_score(max_compaction_score);
        request.__set_report_version(report_version);
        _handle_report(request, ReportType::TABLET);
    }
    StorageEngine::instance()->deregister_report_listener(this);
}

void TaskWorkerPool::_upload_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TUploadReq upload_request;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            upload_request = agent_task_req.upload_req;
            _tasks.pop_front();
        }

        LOG(INFO) << "get upload task. signature=" << agent_task_req.signature
                  << ", job_id=" << upload_request.job_id;

        std::map<int64_t, std::vector<std::string>> tablet_files;
        std::unique_ptr<SnapshotLoader> loader = std::make_unique<SnapshotLoader>(
                _env, upload_request.job_id, agent_task_req.signature, upload_request.broker_addr,
                upload_request.broker_prop,
                upload_request.__isset.storage_backend ? upload_request.storage_backend
                                                       : TStorageBackendType::type::BROKER);
        Status status = loader->upload(upload_request.src_dest_map, &tablet_files);

        if (!status.ok()) {
            LOG_WARNING("failed to upload")
                    .tag("signature", agent_task_req.signature)
                    .tag("job_id", upload_request.job_id)
                    .error(status);
        } else {
            LOG_INFO("successfully upload")
                    .tag("signature", agent_task_req.signature)
                    .tag("job_id", upload_request.job_id);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());
        finish_task_request.__set_tablet_files(tablet_files);

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_download_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TDownloadReq download_request;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            download_request = agent_task_req.download_req;
            _tasks.pop_front();
        }
        LOG(INFO) << "get download task. signature=" << agent_task_req.signature
                  << ", job_id=" << download_request.job_id;

        // TODO: download
        std::vector<int64_t> downloaded_tablet_ids;

        std::unique_ptr<SnapshotLoader> loader = std::make_unique<SnapshotLoader>(
                _env, download_request.job_id, agent_task_req.signature,
                download_request.broker_addr, download_request.broker_prop,
                download_request.__isset.storage_backend ? download_request.storage_backend
                                                         : TStorageBackendType::type::BROKER);
        Status status = loader->download(download_request.src_dest_map, &downloaded_tablet_ids);

        if (!status.ok()) {
            LOG_WARNING("failed to download")
                    .tag("signature", agent_task_req.signature)
                    .tag("job_id", download_request.job_id)
                    .error(status);
        } else {
            LOG_INFO("successfully download")
                    .tag("signature", agent_task_req.signature)
                    .tag("job_id", download_request.job_id);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());
        finish_task_request.__set_downloaded_tablet_ids(downloaded_tablet_ids);

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_make_snapshot_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TSnapshotRequest snapshot_request;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            snapshot_request = agent_task_req.snapshot_req;
            _tasks.pop_front();
        }
        LOG(INFO) << "get snapshot task. signature=" << agent_task_req.signature;

        string snapshot_path;
        bool allow_incremental_clone = false; // not used
        std::vector<string> snapshot_files;
        Status status = SnapshotManager::instance()->make_snapshot(snapshot_request, &snapshot_path,
                                                                   &allow_incremental_clone);
        if (status.ok() && snapshot_request.__isset.list_files) {
            // list and save all snapshot files
            // snapshot_path like: data/snapshot/20180417205230.1.86400
            // we need to add subdir: tablet_id/schema_hash/
            std::stringstream ss;
            ss << snapshot_path << "/" << snapshot_request.tablet_id << "/"
               << snapshot_request.schema_hash << "/";
            status = FileUtils::list_files(Env::Default(), ss.str(), &snapshot_files);
        }
        if (!status.ok()) {
            LOG_WARNING("failed to make snapshot")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", snapshot_request.tablet_id)
                    .tag("version", snapshot_request.version)
                    .error(status);
        } else {
            LOG_INFO("successfully make snapshot")
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", snapshot_request.tablet_id)
                    .tag("version", snapshot_request.version)
                    .tag("snapshot_path", snapshot_path);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_snapshot_path(snapshot_path);
        finish_task_request.__set_snapshot_files(snapshot_files);
        finish_task_request.__set_task_status(status.to_thrift());

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_release_snapshot_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TReleaseSnapshotRequest release_snapshot_request;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            release_snapshot_request = agent_task_req.release_snapshot_req;
            _tasks.pop_front();
        }
        LOG(INFO) << "get release snapshot task. signature=" << agent_task_req.signature;

        string& snapshot_path = release_snapshot_request.snapshot_path;
        Status status = SnapshotManager::instance()->release_snapshot(snapshot_path);
        if (!status.ok()) {
            LOG_WARNING("failed to release snapshot")
                    .tag("signature", agent_task_req.signature)
                    .tag("snapshot_path", snapshot_path)
                    .error(status);
        } else {
            LOG_INFO("successfully release snapshot")
                    .tag("signature", agent_task_req.signature)
                    .tag("snapshot_path", snapshot_path);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

Status TaskWorkerPool::_get_tablet_info(const TTabletId tablet_id, const TSchemaHash schema_hash,
                                        int64_t signature, TTabletInfo* tablet_info) {
    tablet_info->__set_tablet_id(tablet_id);
    tablet_info->__set_schema_hash(schema_hash);
    return StorageEngine::instance()->tablet_manager()->report_tablet_info(tablet_info);
}

void TaskWorkerPool::_move_dir_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TMoveDirReq move_dir_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            move_dir_req = agent_task_req.move_dir_req;
            _tasks.pop_front();
        }
        LOG(INFO) << "get move dir task. signature=" << agent_task_req.signature
                  << ", job_id=" << move_dir_req.job_id;
        Status status = _move_dir(move_dir_req.tablet_id, move_dir_req.src, move_dir_req.job_id,
                                  true /* TODO */);

        if (!status.ok()) {
            LOG_WARNING("failed to move dir")
                    .tag("signature", agent_task_req.signature)
                    .tag("job_id", move_dir_req.job_id)
                    .tag("tablet_id", move_dir_req.tablet_id)
                    .tag("src", move_dir_req.src)
                    .error(status);
        } else {
            LOG_INFO("successfully move dir")
                    .tag("signature", agent_task_req.signature)
                    .tag("job_id", move_dir_req.job_id)
                    .tag("tablet_id", move_dir_req.tablet_id)
                    .tag("src", move_dir_req.src);
        }

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

Status TaskWorkerPool::_move_dir(const TTabletId tablet_id, const std::string& src, int64_t job_id,
                                 bool overwrite) {
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::InvalidArgument("Could not find tablet");
    }
    SnapshotLoader loader(_env, job_id, tablet_id);
    return loader.move(src, tablet, overwrite);
}

void TaskWorkerPool::_handle_report(TReportRequest& request, ReportType type) {
    TMasterResult result;
    Status status = _master_client->report(request, &result);
    bool is_report_success = false;
    if (!status.ok()) {
        LOG_WARNING("failed to report {}", TYPE_STRING(type))
                .tag("host", _master_info.network_address.hostname)
                .tag("port", _master_info.network_address.port)
                .error(status);
    } else if (result.status.status_code != TStatusCode::OK) {
        LOG_WARNING("failed to report {}", TYPE_STRING(type))
                .tag("host", _master_info.network_address.hostname)
                .tag("port", _master_info.network_address.port)
                .error(result.status);
    } else {
        is_report_success = true;
        LOG_INFO("successfully report {}", TYPE_STRING(type))
                .tag("host", _master_info.network_address.hostname)
                .tag("port", _master_info.network_address.port);
    }
    switch (type) {
    case TASK:
        DorisMetrics::instance()->report_task_requests_total->increment(1);
        if (!is_report_success) {
            DorisMetrics::instance()->report_task_requests_failed->increment(1);
        }
        break;
    case DISK:
        DorisMetrics::instance()->report_disk_requests_total->increment(1);
        if (!is_report_success) {
            DorisMetrics::instance()->report_disk_requests_failed->increment(1);
        }
        break;
    case TABLET:
        DorisMetrics::instance()->report_tablet_requests_total->increment(1);
        if (!is_report_success) {
            DorisMetrics::instance()->report_tablet_requests_failed->increment(1);
        }
        break;
    default:
        break;
    }
}

void TaskWorkerPool::_random_sleep(int second) {
    Random rnd(UnixMillis());
    sleep(rnd.Uniform(second) + 1);
}

void TaskWorkerPool::_submit_table_compaction_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TCompactionReq compaction_req;

        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            compaction_req = agent_task_req.compaction_req;
            _tasks.pop_front();
        }

        LOG(INFO) << "get compaction task. signature=" << agent_task_req.signature
                  << ", compaction_type=" << compaction_req.type;

        CompactionType compaction_type;
        if (compaction_req.type == "base") {
            compaction_type = CompactionType::BASE_COMPACTION;
        } else {
            compaction_type = CompactionType::CUMULATIVE_COMPACTION;
        }

        TabletSharedPtr tablet_ptr =
                StorageEngine::instance()->tablet_manager()->get_tablet(compaction_req.tablet_id);
        if (tablet_ptr != nullptr) {
            auto data_dir = tablet_ptr->data_dir();
            if (!tablet_ptr->can_do_compaction(data_dir->path_hash(), compaction_type)) {
                LOG(WARNING) << "could not do compaction. tablet_id=" << tablet_ptr->tablet_id()
                             << ", compaction_type=" << compaction_type;
                _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
                continue;
            }

            Status status =
                    StorageEngine::instance()->submit_compaction_task(tablet_ptr, compaction_type);
            if (!status.ok()) {
                LOG(WARNING) << "failed to submit table compaction task. error=" << status;
            }
            _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
        }
    }
}

void TaskWorkerPool::_storage_refresh_storage_policy_worker_thread_callback() {
    while (_is_work) {
        _is_doing_work = false;
        {
            // wait at most report_task_interval_seconds, or being notified
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait_for(
                    worker_thread_lock,
                    std::chrono::seconds(
                            config::storage_refresh_storage_policy_task_interval_seconds));
        }
        if (!_is_work) {
            break;
        }

        if (_master_info.network_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            LOG(INFO)
                    << "waiting to receive first heartbeat from frontend before doing task report";
            continue;
        }

        _is_doing_work = true;

        TGetStoragePolicyResult result;
        Status status = _master_client->refresh_storage_policy(&result);
        if (status.ok() && result.status.status_code == TStatusCode::OK) {
            // update storage policy mgr.
            StoragePolicyMgr* spm = ExecEnv::GetInstance()->storage_policy_mgr();
            for (const auto& iter : result.result_entrys) {
                shared_ptr<StoragePolicy> policy_ptr = make_shared<StoragePolicy>();
                policy_ptr->storage_policy_name = iter.policy_name;
                policy_ptr->cooldown_datetime = iter.cooldown_datetime;
                policy_ptr->cooldown_ttl = iter.cooldown_ttl;
                policy_ptr->s3_endpoint = iter.s3_storage_param.s3_endpoint;
                policy_ptr->s3_region = iter.s3_storage_param.s3_region;
                policy_ptr->s3_ak = iter.s3_storage_param.s3_ak;
                policy_ptr->s3_sk = iter.s3_storage_param.s3_sk;
                policy_ptr->root_path = iter.s3_storage_param.root_path;
                policy_ptr->bucket = iter.s3_storage_param.bucket;
                policy_ptr->s3_conn_timeout_ms = iter.s3_storage_param.s3_conn_timeout_ms;
                policy_ptr->s3_max_conn = iter.s3_storage_param.s3_max_conn;
                policy_ptr->s3_request_timeout_ms = iter.s3_storage_param.s3_request_timeout_ms;
                policy_ptr->md5_sum = iter.md5_checksum;

                LOG_EVERY_N(INFO, 12) << "refresh storage policy task. policy=" << *policy_ptr;
                spm->periodic_put(iter.policy_name, policy_ptr);
            }
        }
    }
}

void TaskWorkerPool::_storage_update_storage_policy_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        TGetStoragePolicy get_storage_policy_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            get_storage_policy_req = agent_task_req.update_policy;
            _tasks.pop_front();
        }

        StoragePolicyMgr* spm = ExecEnv::GetInstance()->storage_policy_mgr();
        shared_ptr<StoragePolicy> policy_ptr = make_shared<StoragePolicy>();
        policy_ptr->storage_policy_name = get_storage_policy_req.policy_name;
        policy_ptr->cooldown_datetime = get_storage_policy_req.cooldown_datetime;
        policy_ptr->cooldown_ttl = get_storage_policy_req.cooldown_ttl;
        policy_ptr->s3_endpoint = get_storage_policy_req.s3_storage_param.s3_endpoint;
        policy_ptr->s3_region = get_storage_policy_req.s3_storage_param.s3_region;
        policy_ptr->s3_ak = get_storage_policy_req.s3_storage_param.s3_ak;
        policy_ptr->s3_sk = get_storage_policy_req.s3_storage_param.s3_sk;
        policy_ptr->root_path = get_storage_policy_req.s3_storage_param.root_path;
        policy_ptr->bucket = get_storage_policy_req.s3_storage_param.bucket;
        policy_ptr->s3_conn_timeout_ms = get_storage_policy_req.s3_storage_param.s3_conn_timeout_ms;
        policy_ptr->s3_max_conn = get_storage_policy_req.s3_storage_param.s3_max_conn;
        policy_ptr->s3_request_timeout_ms =
                get_storage_policy_req.s3_storage_param.s3_request_timeout_ms;
        policy_ptr->md5_sum = get_storage_policy_req.md5_checksum;

        LOG(INFO) << "get storage update policy task. policy=" << *policy_ptr;

        spm->update(get_storage_policy_req.policy_name, policy_ptr);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

} // namespace doris
