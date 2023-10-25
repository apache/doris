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
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "agent/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/ref_counted.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
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
#include "olap/task/engine_alter_tablet_task.h"
#include "olap/task/engine_batch_load_task.h"
#include "olap/task/engine_checksum_task.h"
#include "olap/task/engine_clone_task.h"
#include "olap/task/engine_index_change_task.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/task/engine_storage_migration_task.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "runtime/snapshot_loader.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
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

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(agent_task_queue_size, MetricUnit::NOUNIT);

const uint32_t TASK_FINISH_MAX_RETRY = 3;
const uint32_t PUBLISH_VERSION_MAX_RETRY = 3;

std::atomic_ulong TaskWorkerPool::_s_report_version(time(nullptr) * 10000);
std::mutex TaskWorkerPool::_s_task_signatures_lock;
std::map<TTaskType::type, std::set<int64_t>> TaskWorkerPool::_s_task_signatures;

static bvar::LatencyRecorder g_publish_version_latency("doris_pk", "publish_version");

TaskWorkerPool::TaskWorkerPool(const TaskWorkerType task_worker_type, ExecEnv* env,
                               const TMasterInfo& master_info, ThreadModel thread_model)
        : _master_info(master_info),
          _agent_utils(new AgentUtils()),
          _env(env),
          _stop_background_threads_latch(1),
          _is_work(false),
          _thread_model(thread_model),
          _is_doing_work(false),
          _task_worker_type(task_worker_type) {
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
    switch (_task_worker_type) {
    case TaskWorkerType::CREATE_TABLE:
        break;
    case TaskWorkerType::DROP_TABLE:
        break;
    case TaskWorkerType::PUSH:
    case TaskWorkerType::REALTIME_PUSH:
        break;
    case TaskWorkerType::PUBLISH_VERSION:
        break;
    case TaskWorkerType::CLEAR_TRANSACTION_TASK:
        break;
    case TaskWorkerType::DELETE:
        break;
    case TaskWorkerType::ALTER_TABLE:
        break;
    case TaskWorkerType::ALTER_INVERTED_INDEX:
        _worker_count = config::alter_index_worker_count;
        _cb = std::bind<void>(&TaskWorkerPool::_alter_inverted_index_worker_thread_callback, this);
        break;
    case TaskWorkerType::CLONE:
        break;
    case TaskWorkerType::STORAGE_MEDIUM_MIGRATE:
        break;
    case TaskWorkerType::CHECK_CONSISTENCY:
        _worker_count = config::check_consistency_worker_count;
        _cb = std::bind<void>(&TaskWorkerPool::_check_consistency_worker_thread_callback, this);
        break;
    case TaskWorkerType::REPORT_TASK:
        _cb = std::bind<void>(&TaskWorkerPool::_report_task_worker_thread_callback, this);
        break;
    case TaskWorkerType::REPORT_DISK_STATE:
        _cb = std::bind<void>(&TaskWorkerPool::_report_disk_state_worker_thread_callback, this);
        break;
    case TaskWorkerType::REPORT_OLAP_TABLE:
        _cb = std::bind<void>(&TaskWorkerPool::_report_tablet_worker_thread_callback, this);
        break;
    case TaskWorkerType::UPLOAD:
        _worker_count = config::upload_worker_count;
        _cb = std::bind<void>(&TaskWorkerPool::_upload_worker_thread_callback, this);
        break;
    case TaskWorkerType::DOWNLOAD:
        _worker_count = config::download_worker_count;
        _cb = std::bind<void>(&TaskWorkerPool::_download_worker_thread_callback, this);
        break;
    case TaskWorkerType::MAKE_SNAPSHOT:
        _worker_count = config::make_snapshot_worker_count;
        _cb = std::bind<void>(&TaskWorkerPool::_make_snapshot_thread_callback, this);
        break;
    case TaskWorkerType::RELEASE_SNAPSHOT:
        _worker_count = config::release_snapshot_worker_count;
        _cb = std::bind<void>(&TaskWorkerPool::_release_snapshot_thread_callback, this);
        break;
    case TaskWorkerType::MOVE:
        _worker_count = 1;
        _cb = std::bind<void>(&TaskWorkerPool::_move_dir_thread_callback, this);
        break;
    case TaskWorkerType::UPDATE_TABLET_META_INFO:
        _worker_count = 1;
        _cb = std::bind<void>(&TaskWorkerPool::_update_tablet_meta_worker_thread_callback, this);
        break;
    case TaskWorkerType::SUBMIT_TABLE_COMPACTION:
        _worker_count = 1;
        _cb = std::bind<void>(&TaskWorkerPool::_submit_table_compaction_worker_thread_callback,
                              this);
        break;
    case TaskWorkerType::PUSH_STORAGE_POLICY:
        _worker_count = 1;
        _cb = std::bind<void>(&TaskWorkerPool::_push_storage_policy_worker_thread_callback, this);
        break;
    case TaskWorkerType::PUSH_COOLDOWN_CONF:
        _worker_count = 1;
        _cb = std::bind<void>(&TaskWorkerPool::_push_cooldown_conf_worker_thread_callback, this);
        break;
    case TaskWorkerType::GC_BINLOG:
        _worker_count = 1;
        _cb = std::bind<void>(&TaskWorkerPool::_gc_binlog_worker_thread_callback, this);
        break;
    default:
        // pass
        break;
    }
    CHECK(_thread_model == ThreadModel::MULTI_THREADS || _worker_count == 1);

#ifndef BE_TEST
    static_cast<void>(ThreadPoolBuilder(_name)
                              .set_min_threads(_worker_count)
                              .set_max_threads(_worker_count)
                              .build(&_thread_pool));

    for (int i = 0; i < _worker_count; i++) {
        auto st = _thread_pool->submit_func(_cb);
        CHECK(st.ok()) << st;
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
    if (task_type == TTaskType::type::PUSH_STORAGE_POLICY ||
        task_type == TTaskType::type::PUSH_COOLDOWN_CONF) {
        // no need to report task of these types
        return true;
    }
    if (signature == -1) { // No need to report task with unintialized signature
        return true;
    }
    std::lock_guard<std::mutex> task_signatures_lock(_s_task_signatures_lock);
    std::set<int64_t>& signature_set = _s_task_signatures[task_type];
    return signature_set.insert(signature).second;
}

void TaskWorkerPool::_remove_task_info(const TTaskType::type task_type, int64_t signature) {
    size_t queue_size;
    {
        std::lock_guard<std::mutex> task_signatures_lock(_s_task_signatures_lock);
        std::set<int64_t>& signature_set = _s_task_signatures[task_type];
        signature_set.erase(signature);
        queue_size = signature_set.size();
    }

    VLOG_NOTICE << "remove task info. type=" << task_type << ", signature=" << signature
                << ", queue_size=" << queue_size;
}

void TaskWorkerPool::_finish_task(const TFinishTaskRequest& finish_task_request) {
    // Return result to FE
    TMasterResult result;
    uint32_t try_time = 0;

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
        sleep(config::sleep_one_second);
    }
}

void TaskWorkerPool::_alter_inverted_index_worker_thread_callback() {
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

        auto& alter_inverted_index_rq = agent_task_req.alter_inverted_index_req;
        LOG(INFO) << "get alter inverted index task. signature=" << agent_task_req.signature
                  << ", tablet_id=" << alter_inverted_index_rq.tablet_id
                  << ", job_id=" << alter_inverted_index_rq.job_id;

        Status status = Status::OK();
        TabletSharedPtr tablet_ptr = StorageEngine::instance()->tablet_manager()->get_tablet(
                alter_inverted_index_rq.tablet_id);
        if (tablet_ptr != nullptr) {
            EngineIndexChangeTask engine_task(alter_inverted_index_rq);
            status = StorageEngine::instance()->execute_task(&engine_task);
        } else {
            status =
                    Status::NotFound("could not find tablet {}", alter_inverted_index_rq.tablet_id);
        }

        // Return result to fe
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        std::vector<TTabletInfo> finish_tablet_infos;
        if (!status.ok()) {
            LOG(WARNING) << "failed to alter inverted index task, signature="
                         << agent_task_req.signature
                         << ", tablet_id=" << alter_inverted_index_rq.tablet_id
                         << ", job_id=" << alter_inverted_index_rq.job_id << ", error=" << status;
        } else {
            LOG(INFO) << "successfully alter inverted index task, signature="
                      << agent_task_req.signature
                      << ", tablet_id=" << alter_inverted_index_rq.tablet_id
                      << ", job_id=" << alter_inverted_index_rq.job_id;
            TTabletInfo tablet_info;
            status = _get_tablet_info(alter_inverted_index_rq.tablet_id,
                                      alter_inverted_index_rq.schema_hash, agent_task_req.signature,
                                      &tablet_info);
            if (status.ok()) {
                finish_tablet_infos.push_back(tablet_info);
            }
            finish_task_request.__set_finish_tablet_infos(finish_tablet_infos);
        }
        finish_task_request.__set_task_status(status.to_thrift());
        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

void TaskWorkerPool::_update_tablet_meta_worker_thread_callback() {
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
        LOG(INFO) << "get update tablet meta task. signature=" << agent_task_req.signature;

        Status status;
        auto& update_tablet_meta_req = agent_task_req.update_tablet_meta_info_req;
        for (auto& tablet_meta_info : update_tablet_meta_req.tabletMetaInfos) {
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    tablet_meta_info.tablet_id);
            if (tablet == nullptr) {
                status = Status::NotFound("tablet not found");
                LOG(WARNING) << "could not find tablet when update tablet meta. tablet_id="
                             << tablet_meta_info.tablet_id;
                continue;
            }
            bool need_to_save = false;
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
                if (tablet_meta_info.compaction_policy != "size_based" &&
                    tablet_meta_info.compaction_policy != "time_series") {
                    status = Status::InvalidArgument(
                            "invalid compaction policy, only support for size_based or "
                            "time_series");
                    continue;
                }
                tablet->tablet_meta()->set_compaction_policy(tablet_meta_info.compaction_policy);
                need_to_save = true;
            }
            if (tablet_meta_info.__isset.time_series_compaction_goal_size_mbytes) {
                if (tablet->tablet_meta()->compaction_policy() != "time_series") {
                    status = Status::InvalidArgument(
                            "only time series compaction policy support time series config");
                    continue;
                }
                tablet->tablet_meta()->set_time_series_compaction_goal_size_mbytes(
                        tablet_meta_info.time_series_compaction_goal_size_mbytes);
                need_to_save = true;
            }
            if (tablet_meta_info.__isset.time_series_compaction_file_count_threshold) {
                if (tablet->tablet_meta()->compaction_policy() != "time_series") {
                    status = Status::InvalidArgument(
                            "only time series compaction policy support time series config");
                    continue;
                }
                tablet->tablet_meta()->set_time_series_compaction_file_count_threshold(
                        tablet_meta_info.time_series_compaction_file_count_threshold);
                need_to_save = true;
            }
            if (tablet_meta_info.__isset.time_series_compaction_time_threshold_seconds) {
                if (tablet->tablet_meta()->compaction_policy() != "time_series") {
                    status = Status::InvalidArgument(
                            "only time series compaction policy support time series config");
                    continue;
                }
                tablet->tablet_meta()->set_time_series_compaction_time_threshold_seconds(
                        tablet_meta_info.time_series_compaction_time_threshold_seconds);
                need_to_save = true;
            }
            if (tablet_meta_info.__isset.replica_id) {
                tablet->tablet_meta()->set_replica_id(tablet_meta_info.replica_id);
            }
            if (tablet_meta_info.__isset.binlog_config) {
                // check binlog_config require fields: enable, ttl_seconds, max_bytes, max_history_nums
                auto& t_binlog_config = tablet_meta_info.binlog_config;
                if (!t_binlog_config.__isset.enable || !t_binlog_config.__isset.ttl_seconds ||
                    !t_binlog_config.__isset.max_bytes ||
                    !t_binlog_config.__isset.max_history_nums) {
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
                        tablet_meta_info.tablet_id,
                        tablet->tablet_meta()->binlog_config().to_string(),
                        new_binlog_config.to_string());
                tablet->set_binlog_config(new_binlog_config);
                need_to_save = true;
            }
            if (tablet_meta_info.__isset.enable_single_replica_compaction) {
                std::shared_lock rlock(tablet->get_header_lock());
                tablet->tablet_meta()
                        ->mutable_tablet_schema()
                        ->set_enable_single_replica_compaction(
                                tablet_meta_info.enable_single_replica_compaction);
                for (auto& rowset_meta : tablet->tablet_meta()->all_mutable_rs_metas()) {
                    rowset_meta->tablet_schema()->set_enable_single_replica_compaction(
                            tablet_meta_info.enable_single_replica_compaction);
                }
                tablet->tablet_schema_unlocked()->set_enable_single_replica_compaction(
                        tablet_meta_info.enable_single_replica_compaction);
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

        LOG(INFO) << "finish update tablet meta task. signature=" << agent_task_req.signature;
        if (agent_task_req.signature != -1) {
            TFinishTaskRequest finish_task_request;
            finish_task_request.__set_task_status(status.to_thrift());
            finish_task_request.__set_backend(BackendOptions::get_local_backend());
            finish_task_request.__set_task_type(agent_task_req.task_type);
            finish_task_request.__set_signature(agent_task_req.signature);
            _finish_task(finish_task_request);
            _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
        }
    }
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
        Status status = StorageEngine::instance()->execute_task(&engine_task);
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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
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
            std::lock_guard<std::mutex> task_signatures_lock(_s_task_signatures_lock);
            request.__set_tasks(_s_task_signatures);
            request.__set_backend(BackendOptions::get_local_backend());
        }
        _handle_report(request, ReportType::TASK);
    }
    StorageEngine::instance()->deregister_report_listener(this);
}

/// disk state report thread will report disk state at a configurable fix interval.
void TaskWorkerPool::_report_disk_state_worker_thread_callback() {
    StorageEngine::instance()->register_report_listener(this);

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

        TReportRequest request;
        request.__set_backend(BackendOptions::get_local_backend());
        request.__isset.disks = true;

        std::vector<DataDirInfo> data_dir_infos;
        static_cast<void>(StorageEngine::instance()->get_all_data_dir_info(&data_dir_infos,
                                                                           true /* update */));

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
        _handle_report(request, ReportType::DISK);
    }
    StorageEngine::instance()->deregister_report_listener(this);
}

void TaskWorkerPool::_report_tablet_worker_thread_callback() {
    StorageEngine::instance()->register_report_listener(this);

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

        TReportRequest request;
        request.__set_backend(BackendOptions::get_local_backend());
        request.__isset.tablets = true;

        uint64_t report_version = _s_report_version;
        static_cast<void>(
                StorageEngine::instance()->tablet_manager()->build_all_report_tablets_info(
                        &request.tablets));
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

        // report storage policy and resource
        auto& storage_policy_list = request.storage_policy;
        for (auto [id, version] : get_storage_policy_ids()) {
            auto& storage_policy = storage_policy_list.emplace_back();
            storage_policy.__set_id(id);
            storage_policy.__set_version(version);
        }
        request.__isset.storage_policy = true;
        auto& resource_list = request.resource;
        for (auto [id, version] : get_storage_resource_ids()) {
            auto& resource = resource_list.emplace_back();
            resource.__set_id(id);
            resource.__set_version(version);
        }
        request.__isset.resource = true;

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
                upload_request.broker_prop);
        Status status = loader->init(
                upload_request.__isset.storage_backend ? upload_request.storage_backend
                                                       : TStorageBackendType::type::BROKER,
                upload_request.__isset.location ? upload_request.location : "");
        if (status.ok()) {
            status = loader->upload(upload_request.src_dest_map, &tablet_files);
        }

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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
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
                  << ", job_id=" << download_request.job_id
                  << ", task detail: " << apache::thrift::ThriftDebugString(download_request);

        // TODO: download
        std::vector<int64_t> downloaded_tablet_ids;

        auto status = Status::OK();
        if (download_request.__isset.remote_tablet_snapshots) {
            std::unique_ptr<SnapshotLoader> loader = std::make_unique<SnapshotLoader>(
                    _env, download_request.job_id, agent_task_req.signature);
            status = loader->remote_http_download(download_request.remote_tablet_snapshots,
                                                  &downloaded_tablet_ids);
        } else {
            std::unique_ptr<SnapshotLoader> loader = std::make_unique<SnapshotLoader>(
                    _env, download_request.job_id, agent_task_req.signature,
                    download_request.broker_addr, download_request.broker_prop);
            status = loader->init(
                    download_request.__isset.storage_backend ? download_request.storage_backend
                                                             : TStorageBackendType::type::BROKER,
                    download_request.__isset.location ? download_request.location : "");
            if (status.ok()) {
                status = loader->download(download_request.src_dest_map, &downloaded_tablet_ids);
            }
        }

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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
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

void TaskWorkerPool::_handle_report(const TReportRequest& request, ReportType type) {
    TMasterResult result;
    Status status = MasterServerClient::instance()->report(request, &result);
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

            Status status = StorageEngine::instance()->submit_compaction_task(
                    tablet_ptr, compaction_type, false);
            if (!status.ok()) {
                LOG(WARNING) << "failed to submit table compaction task. error=" << status;
            }
            _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
        }
    }
}

void TaskWorkerPool::_push_storage_policy_worker_thread_callback() {
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
        TPushStoragePolicyReq& push_storage_policy_req = agent_task_req.push_storage_policy_req;
        // refresh resource
        for (auto& resource : push_storage_policy_req.resource) {
            auto existed_resource = get_storage_resource(resource.id);
            if (existed_resource.version >= resource.version) {
                continue;
            }
            if (resource.__isset.s3_storage_param) {
                Status st;
                S3Conf s3_conf;
                s3_conf.ak = std::move(resource.s3_storage_param.ak);
                s3_conf.sk = std::move(resource.s3_storage_param.sk);
                s3_conf.endpoint = std::move(resource.s3_storage_param.endpoint);
                s3_conf.region = std::move(resource.s3_storage_param.region);
                s3_conf.prefix = std::move(resource.s3_storage_param.root_path);
                s3_conf.bucket = std::move(resource.s3_storage_param.bucket);
                s3_conf.connect_timeout_ms = resource.s3_storage_param.conn_timeout_ms;
                s3_conf.max_connections = resource.s3_storage_param.max_conn;
                s3_conf.request_timeout_ms = resource.s3_storage_param.request_timeout_ms;
                // When using cold heat separation in minio, user might use ip address directly,
                // which needs enable use_virtual_addressing to true
                s3_conf.use_virtual_addressing = !resource.s3_storage_param.use_path_style;
                std::shared_ptr<io::S3FileSystem> fs;
                if (existed_resource.fs == nullptr) {
                    st = io::S3FileSystem::create(s3_conf, std::to_string(resource.id), &fs);
                } else {
                    fs = std::static_pointer_cast<io::S3FileSystem>(existed_resource.fs);
                    fs->set_conf(s3_conf);
                }
                if (!st.ok()) {
                    LOG(WARNING) << "update s3 resource failed: " << st;
                } else {
                    LOG_INFO("successfully update s3 resource")
                            .tag("resource_id", resource.id)
                            .tag("resource_name", resource.name)
                            .tag("s3_conf", s3_conf.to_string());
                    put_storage_resource(resource.id, {std::move(fs), resource.version});
                }
            } else if (resource.__isset.hdfs_storage_param) {
                Status st;
                std::shared_ptr<io::HdfsFileSystem> fs;
                if (existed_resource.fs == nullptr) {
                    st = io::HdfsFileSystem::create(resource.hdfs_storage_param, "", nullptr, &fs);
                } else {
                    fs = std::static_pointer_cast<io::HdfsFileSystem>(existed_resource.fs);
                }
                if (!st.ok()) {
                    LOG(WARNING) << "update hdfs resource failed: " << st;
                } else {
                    LOG_INFO("successfully update hdfs resource")
                            .tag("resource_id", resource.id)
                            .tag("resource_name", resource.name);
                    put_storage_resource(resource.id, {std::move(fs), resource.version});
                }
            } else {
                LOG(WARNING) << "unknown resource=" << resource;
            }
        }
        // drop storage policy
        for (auto policy_id : push_storage_policy_req.dropped_storage_policy) {
            delete_storage_policy(policy_id);
        }
        // refresh storage policy
        for (auto& storage_policy : push_storage_policy_req.storage_policy) {
            auto existed_storage_policy = get_storage_policy(storage_policy.id);
            if (existed_storage_policy == nullptr ||
                existed_storage_policy->version < storage_policy.version) {
                auto storage_policy1 = std::make_shared<StoragePolicy>();
                storage_policy1->name = std::move(storage_policy.name);
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
}

void TaskWorkerPool::_push_cooldown_conf_worker_thread_callback() {
    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            while (_is_work && _tasks.empty()) {
                _worker_thread_condition_variable.wait(worker_thread_lock);
            }
            if (!_is_work) {
                return;
            }

            agent_task_req = _tasks.front();
            _tasks.pop_front();
        }

        TPushCooldownConfReq& push_cooldown_conf_req = agent_task_req.push_cooldown_conf;
        for (auto& cooldown_conf : push_cooldown_conf_req.cooldown_confs) {
            int64_t tablet_id = cooldown_conf.tablet_id;
            TabletSharedPtr tablet =
                    StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
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
}

CreateTableTaskPool::CreateTableTaskPool(ExecEnv* env, ThreadModel thread_model)
        : TaskWorkerPool(TaskWorkerType::CREATE_TABLE, env, *env->master_info(), thread_model) {
    _worker_count = config::create_tablet_worker_count;
    _cb = [this]() { _create_tablet_worker_thread_callback(); };
}

void CreateTableTaskPool::_create_tablet_worker_thread_callback() {
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
        const TCreateTabletReq& create_tablet_req = agent_task_req.create_tablet_req;
        RuntimeProfile runtime_profile("CreateTablet");
        RuntimeProfile* profile = &runtime_profile;
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            int64_t elapsed_time = static_cast<int64_t>(watch.elapsed_time());
            if (elapsed_time / 1e9 > config::agent_task_trace_threshold_sec) {
                COUNTER_UPDATE(profile->total_time_counter(), elapsed_time);
                std::stringstream ss;
                profile->pretty_print(&ss);
                LOG(WARNING) << "create tablet cost(s) " << elapsed_time / 1e9 << std::endl
                             << ss.str();
            }
        });
        DorisMetrics::instance()->create_tablet_requests_total->increment(1);
        VLOG_NOTICE << "start to create tablet " << create_tablet_req.tablet_id;

        std::vector<TTabletInfo> finish_tablet_infos;
        VLOG_NOTICE << "create tablet: " << create_tablet_req;
        Status status = StorageEngine::instance()->create_tablet(create_tablet_req, profile);
        if (!status.ok()) {
            DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
            LOG_WARNING("failed to create tablet, reason={}", status.to_string())
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", create_tablet_req.tablet_id)
                    .error(status);
        } else {
            ++_s_report_version;
            // get path hash of the created tablet
            TabletSharedPtr tablet;
            {
                SCOPED_TIMER(ADD_TIMER(profile, "GetTablet"));
                tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                        create_tablet_req.tablet_id);
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
                    .tag("signature", agent_task_req.signature)
                    .tag("tablet_id", create_tablet_req.tablet_id);
        }
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_finish_tablet_infos(finish_tablet_infos);
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
        finish_task_request.__set_report_version(_s_report_version);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());
        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

DropTableTaskPool::DropTableTaskPool(ExecEnv* env, ThreadModel thread_model)
        : TaskWorkerPool(TaskWorkerType::DROP_TABLE, env, *env->master_info(), thread_model) {
    _worker_count = config::drop_tablet_worker_count;
    _cb = [this]() { _drop_tablet_worker_thread_callback(); };
}

void DropTableTaskPool::_drop_tablet_worker_thread_callback() {
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
        const TDropTabletReq& drop_tablet_req = agent_task_req.drop_tablet_req;
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
                    dropped_tablet->tablet_uid());
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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

PushTaskPool::PushTaskPool(ExecEnv* env, ThreadModel thread_model, PushWokerType type)
        : TaskWorkerPool(
                  type == PushWokerType::LOAD_V2 ? TaskWorkerType::PUSH : TaskWorkerType::DELETE,
                  env, *env->master_info(), thread_model),
          _push_worker_type(type) {
    if (_push_worker_type == PushWokerType::LOAD_V2) {
        _worker_count =
                config::push_worker_count_normal_priority + config::push_worker_count_high_priority;

    } else {
        _worker_count = config::delete_worker_count;
    }
    _cb = [this]() { _push_worker_thread_callback(); };
}

void PushTaskPool::_push_worker_thread_callback() {
    // gen high priority worker thread
    TPriority::type priority = TPriority::NORMAL;
    int32_t push_worker_count_high_priority = config::push_worker_count_high_priority;
    if (_push_worker_type == PushWokerType::LOAD_V2) {
        static uint32_t s_worker_count = 0;
        std::lock_guard<std::mutex> worker_thread_lock(_worker_thread_lock);
        if (s_worker_count < push_worker_count_high_priority) {
            ++s_worker_count;
            priority = TPriority::HIGH;
        }
    }

    while (_is_work) {
        TAgentTaskRequest agent_task_req;
        {
            std::unique_lock<std::mutex> worker_thread_lock(_worker_thread_lock);
            _worker_thread_condition_variable.wait(
                    worker_thread_lock, [this]() { return !_is_work || !_tasks.empty(); });
            if (!_is_work) {
                return;
            }

            if (priority == TPriority::HIGH) {
                const auto it = std::find_if(
                        _tasks.cbegin(), _tasks.cend(), [](const TAgentTaskRequest& req) {
                            return req.__isset.priority && req.priority == TPriority::HIGH;
                        });

                if (it == _tasks.cend()) {
                    // there is no high priority task. notify other thread to handle normal task
                    _worker_thread_condition_variable.notify_all();
                    sleep(1);
                    continue;
                }
                agent_task_req = *it;
                _tasks.erase(it);
            } else {
                agent_task_req = _tasks.front();
                _tasks.pop_front();
            }
        }
        TPushReq& push_req = agent_task_req.push_req;

        LOG(INFO) << "get push task. signature=" << agent_task_req.signature
                  << ", priority=" << priority << " push_type=" << push_req.push_type;
        std::vector<TTabletInfo> tablet_infos;

        EngineBatchLoadTask engine_task(push_req, &tablet_infos);
        auto status = StorageEngine::instance()->execute_task(&engine_task);

        // Return result to fe
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
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

PublishVersionTaskPool::PublishVersionTaskPool(ExecEnv* env, ThreadModel thread_model)
        : TaskWorkerPool(TaskWorkerType::PUBLISH_VERSION, env, *env->master_info(), thread_model) {
    _worker_count = config::publish_version_worker_count;
    _cb = [this]() { _publish_version_worker_thread_callback(); };
}

void PublishVersionTaskPool::_publish_version_worker_thread_callback() {
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

        const TPublishVersionRequest& publish_version_req = agent_task_req.publish_version_req;
        DorisMetrics::instance()->publish_task_request_total->increment(1);
        VLOG_NOTICE << "get publish version task. signature=" << agent_task_req.signature;

        std::set<TTabletId> error_tablet_ids;
        std::map<TTabletId, TVersion> succ_tablets;
        // partition_id, tablet_id, publish_version
        std::vector<std::tuple<int64_t, int64_t, int64_t>> discontinuous_version_tablets;
        std::map<TTableId, int64_t> table_id_to_num_delta_rows;
        uint32_t retry_time = 0;
        Status status;
        bool is_task_timeout = false;
        while (retry_time < PUBLISH_VERSION_MAX_RETRY) {
            succ_tablets.clear();
            error_tablet_ids.clear();
            table_id_to_num_delta_rows.clear();
            EnginePublishVersionTask engine_task(publish_version_req, &error_tablet_ids,
                                                 &succ_tablets, &discontinuous_version_tablets,
                                                 &table_id_to_num_delta_rows);
            status = StorageEngine::instance()->execute_task(&engine_task);
            if (status.ok()) {
                break;
            } else if (status.is<PUBLISH_VERSION_NOT_CONTINUOUS>()) {
                int64_t time_elapsed = time(nullptr) - agent_task_req.recv_time;
                if (time_elapsed > config::publish_version_task_timeout_s) {
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
                LOG_EVERY_SECOND(INFO) << "wait for previous publish version task to be done, "
                                       << "transaction_id: " << publish_version_req.transaction_id;
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
        if (status.is<PUBLISH_VERSION_NOT_CONTINUOUS>() && !is_task_timeout) {
            continue;
        }

        for (auto& item : discontinuous_version_tablets) {
            StorageEngine::instance()->add_async_publish_task(
                    std::get<0>(item), std::get<1>(item), std::get<2>(item),
                    publish_version_req.transaction_id, false);
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
        } else {
            if (!config::disable_auto_compaction &&
                !MemInfo::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE)) {
                for (auto [tablet_id, _] : succ_tablets) {
                    TabletSharedPtr tablet =
                            StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
                    if (tablet != nullptr) {
                        int64_t published_count =
                                tablet->published_count.fetch_add(1, std::memory_order_relaxed);
                        if (published_count % 10 == 0) {
                            auto st = StorageEngine::instance()->submit_compaction_task(
                                    tablet, CompactionType::CUMULATIVE_COMPACTION, true);
                            if (!st.ok()) [[unlikely]] {
                                LOG(WARNING) << "trigger compaction failed, tablet_id=" << tablet_id
                                             << ", published=" << published_count << " : " << st;
                            } else {
                                LOG(INFO) << "trigger compaction succ, tablet_id:" << tablet_id
                                          << ", published:" << published_count;
                            }
                        }
                    } else {
                        LOG(WARNING) << "trigger compaction failed, tablet_id:" << tablet_id;
                    }
                }
            }
            uint32_t cost_second = time(nullptr) - agent_task_req.recv_time;
            g_publish_version_latency << cost_second;
            LOG_INFO("successfully publish version")
                    .tag("signature", agent_task_req.signature)
                    .tag("transaction_id", publish_version_req.transaction_id)
                    .tag("tablets_num", succ_tablets.size())
                    .tag("cost(s)", cost_second);
        }

        status.to_thrift(&finish_task_request.task_status);
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_report_version(_s_report_version);
        finish_task_request.__set_succ_tablets(succ_tablets);
        finish_task_request.__set_error_tablet_ids(
                std::vector<TTabletId>(error_tablet_ids.begin(), error_tablet_ids.end()));
        finish_task_request.__set_table_id_to_delta_num_rows(table_id_to_num_delta_rows);
        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

ClearTransactionTaskPool::ClearTransactionTaskPool(ExecEnv* env, ThreadModel thread_model)
        : TaskWorkerPool(TaskWorkerType::CLEAR_TRANSACTION_TASK, env, *env->master_info(),
                         thread_model) {
    _worker_count = config::clear_transaction_task_worker_count;
    _cb = [this]() { _clear_transaction_task_worker_thread_callback(); };
}

void ClearTransactionTaskPool::_clear_transaction_task_worker_thread_callback() {
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
        const TClearTransactionTaskRequest& clear_transaction_task_req =
                agent_task_req.clear_transaction_task_req;
        LOG(INFO) << "get clear transaction task. signature=" << agent_task_req.signature
                  << ", transaction_id=" << clear_transaction_task_req.transaction_id
                  << ", partition_id_size=" << clear_transaction_task_req.partition_id.size();

        Status status;

        if (clear_transaction_task_req.transaction_id > 0) {
            // transaction_id should be greater than zero.
            // If it is not greater than zero, no need to execute
            // the following clear_transaction_task() function.
            if (!clear_transaction_task_req.partition_id.empty()) {
                StorageEngine::instance()->clear_transaction_task(
                        clear_transaction_task_req.transaction_id,
                        clear_transaction_task_req.partition_id);
            } else {
                StorageEngine::instance()->clear_transaction_task(
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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

AlterTableTaskPool::AlterTableTaskPool(ExecEnv* env, ThreadModel thread_model)
        : TaskWorkerPool(TaskWorkerType::ALTER_TABLE, env, *env->master_info(), thread_model) {
    _worker_count = config::alter_tablet_worker_count;
    _cb = [this]() { _alter_tablet_worker_thread_callback(); };
}

void AlterTableTaskPool::_alter_tablet_worker_thread_callback() {
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
        LOG(INFO) << "get alter table task, signature: " << signature;
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

void AlterTableTaskPool::_alter_tablet(const TAgentTaskRequest& agent_task_req, int64_t signature,
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
    TTabletId new_tablet_id = 0;
    TSchemaHash new_schema_hash = 0;
    if (status.ok()) {
        new_tablet_id = agent_task_req.alter_tablet_req_v2.new_tablet_id;
        new_schema_hash = agent_task_req.alter_tablet_req_v2.new_schema_hash;
        EngineAlterTabletTask engine_task(agent_task_req.alter_tablet_req_v2);
        status = StorageEngine::instance()->execute_task(&engine_task);
    }

    if (status.ok()) {
        ++_s_report_version;
    }

    // Return result to fe
    finish_task_request->__set_backend(BackendOptions::get_local_backend());
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

void TaskWorkerPool::_gc_binlog_worker_thread_callback() {
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

        std::unordered_map<int64_t, int64_t> gc_tablet_infos;
        if (!agent_task_req.__isset.gc_binlog_req) {
            LOG(WARNING) << "gc binlog task is not valid";
            return;
        }
        if (!agent_task_req.gc_binlog_req.__isset.tablet_gc_binlog_infos) {
            LOG(WARNING) << "gc binlog task tablet_gc_binlog_infos is not valid";
            return;
        }

        auto& tablet_gc_binlog_infos = agent_task_req.gc_binlog_req.tablet_gc_binlog_infos;
        for (auto& tablet_info : tablet_gc_binlog_infos) {
            // gc_tablet_infos.emplace(tablet_info.tablet_id, tablet_info.schema_hash);
            gc_tablet_infos.emplace(tablet_info.tablet_id, tablet_info.version);
        }

        StorageEngine::instance()->gc_binlogs(gc_tablet_infos);
    }
}

CloneTaskPool::CloneTaskPool(ExecEnv* env, ThreadModel thread_model)
        : TaskWorkerPool(TaskWorkerType::CLONE, env, *env->master_info(), thread_model) {
    _worker_count = config::clone_worker_count;
    _cb = [this]() { _clone_worker_thread_callback(); };
}

void CloneTaskPool::_clone_worker_thread_callback() {
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
        const TCloneReq& clone_req = agent_task_req.clone_req;

        DorisMetrics::instance()->clone_requests_total->increment(1);
        LOG(INFO) << "get clone task. signature=" << agent_task_req.signature;

        std::vector<TTabletInfo> tablet_infos;
        EngineCloneTask engine_task(clone_req, _master_info, agent_task_req.signature,
                                    &tablet_infos);
        auto status = StorageEngine::instance()->execute_task(&engine_task);
        // Return result to fe
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
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
            ++_s_report_version;
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        }
        finish_task_request.__set_report_version(_s_report_version);
        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

StorageMediumMigrateTaskPool::StorageMediumMigrateTaskPool(ExecEnv* env, ThreadModel thread_model)
        : TaskWorkerPool(TaskWorkerType::STORAGE_MEDIUM_MIGRATE, env, *env->master_info(),
                         thread_model) {
    _worker_count = config::storage_medium_migrate_count;
    _cb = [this]() { _storage_medium_migrate_worker_thread_callback(); };
}

void StorageMediumMigrateTaskPool::_storage_medium_migrate_worker_thread_callback() {
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
        const TStorageMediumMigrateReq& storage_medium_migrate_req =
                agent_task_req.storage_medium_migrate_req;

        // check request and get info
        TabletSharedPtr tablet;
        DataDir* dest_store = nullptr;

        auto status = _check_migrate_request(storage_medium_migrate_req, tablet, &dest_store);
        if (status.ok()) {
            EngineStorageMigrationTask engine_task(tablet, dest_store);
            status = StorageEngine::instance()->execute_task(&engine_task);
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
        finish_task_request.__set_backend(BackendOptions::get_local_backend());
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(status.to_thrift());

        _finish_task(finish_task_request);
        _remove_task_info(agent_task_req.task_type, agent_task_req.signature);
    }
}

Status StorageMediumMigrateTaskPool::_check_migrate_request(const TStorageMediumMigrateReq& req,
                                                            TabletSharedPtr& tablet,
                                                            DataDir** dest_store) {
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

} // namespace doris
