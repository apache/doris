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
#include <atomic>
#include <chrono>
#include <csignal>
#include <ctime>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/stat.h>

#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>

#include "agent/status.h"
#include "agent/utils.h"
#include "env/env.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_types.h"
#include "http/http_client.h"
#include "olap/olap_common.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/data_dir.h"
#include "olap/snapshot_manager.h"
#include "olap/task/engine_checksum_task.h"
#include "olap/task/engine_clear_alter_task.h"
#include "olap/task/engine_clone_task.h"
#include "olap/task/engine_schema_change_task.h"
#include "olap/task/engine_alter_tablet_task.h"
#include "olap/task/engine_batch_load_task.h"
#include "olap/task/engine_storage_migration_task.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/utils.h"
#include "common/resource_tls.h"
#include "common/status.h"
#include "util/file_utils.h"
#include "agent/cgroups_mgr.h"
#include "service/backend_options.h"
#include "runtime/exec_env.h"
#include "runtime/snapshot_loader.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"

using std::deque;
using std::list;
using std::lock_guard;
using std::map;
using std::set;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;

namespace doris {

const uint32_t TASK_FINISH_MAX_RETRY = 3;
const uint32_t PUBLISH_VERSION_MAX_RETRY = 3;
const uint32_t REPORT_TASK_WORKER_COUNT = 1;
const uint32_t REPORT_DISK_STATE_WORKER_COUNT = 1;
const uint32_t REPORT_OLAP_TABLE_WORKER_COUNT = 1;

std::atomic_ulong TaskWorkerPool::_s_report_version(time(NULL) * 10000);
Mutex TaskWorkerPool::_s_task_signatures_lock;
Mutex TaskWorkerPool::_s_running_task_user_count_lock;
map<TTaskType::type, set<int64_t>> TaskWorkerPool::_s_task_signatures;
map<TTaskType::type, map<string, uint32_t>> TaskWorkerPool::_s_running_task_user_count;
map<TTaskType::type, map<string, uint32_t>> TaskWorkerPool::_s_total_task_user_count;
map<TTaskType::type, uint32_t> TaskWorkerPool::_s_total_task_count;
FrontendServiceClientCache TaskWorkerPool::_master_service_client_cache;

TaskWorkerPool::TaskWorkerPool(
        const TaskWorkerType task_worker_type,
        ExecEnv* env,
        const TMasterInfo& master_info) :
        _master_info(master_info),
        _worker_thread_condition_lock(_worker_thread_lock),
        _task_worker_type(task_worker_type) {
    _agent_utils = new AgentUtils();
    _master_client = new MasterServerClient(_master_info, &_master_service_client_cache);
    _env = env;
    _backend.__set_host(BackendOptions::get_localhost());
    _backend.__set_be_port(config::be_port);
    _backend.__set_http_port(config::webserver_port);
}

TaskWorkerPool::~TaskWorkerPool() {
    if (_agent_utils != NULL) {
        delete _agent_utils;
        _agent_utils = NULL;
    }
    if (_master_client != NULL) {
        delete _master_client;
        _master_client = NULL;
    }
}

void TaskWorkerPool::start() {
    // Init task pool and task workers
    switch (_task_worker_type) {
    case TaskWorkerType::CREATE_TABLE:
        _worker_count = config::create_tablet_worker_count;
        _callback_function = _create_tablet_worker_thread_callback;
        break;
    case TaskWorkerType::DROP_TABLE:
        _worker_count = config::drop_tablet_worker_count;
        _callback_function = _drop_tablet_worker_thread_callback;
        break;
    case TaskWorkerType::PUSH:
    case TaskWorkerType::REALTIME_PUSH:
        _worker_count =  config::push_worker_count_normal_priority
                + config::push_worker_count_high_priority;
        _callback_function = _push_worker_thread_callback;
        break;
    case TaskWorkerType::PUBLISH_VERSION:
        _worker_count = config::publish_version_worker_count;
        _callback_function = _publish_version_worker_thread_callback;
        break;
    case TaskWorkerType::CLEAR_ALTER_TASK:
        _worker_count = config::clear_alter_task_worker_count;
        _callback_function = _clear_alter_task_worker_thread_callback;
        break;
    case TaskWorkerType::CLEAR_TRANSACTION_TASK:
        _worker_count = config::clear_transaction_task_worker_count;
        _callback_function = _clear_transaction_task_worker_thread_callback;
        break;
    case TaskWorkerType::DELETE:
        _worker_count = config::delete_worker_count;
        _callback_function = _push_worker_thread_callback;
        break;
    case TaskWorkerType::ALTER_TABLE:
        _worker_count = config::alter_tablet_worker_count;
        _callback_function = _alter_tablet_worker_thread_callback;
        break;
    case TaskWorkerType::CLONE:
        _worker_count = config::clone_worker_count;
        _callback_function = _clone_worker_thread_callback;
        break;
    case TaskWorkerType::STORAGE_MEDIUM_MIGRATE:
        _worker_count = config::storage_medium_migrate_count;
        _callback_function = _storage_medium_migrate_worker_thread_callback;
        break;
    case TaskWorkerType::CHECK_CONSISTENCY:
        _worker_count = config::check_consistency_worker_count;
        _callback_function = _check_consistency_worker_thread_callback;
        break;
    case TaskWorkerType::REPORT_TASK:
        _worker_count = REPORT_TASK_WORKER_COUNT;
        _callback_function = _report_task_worker_thread_callback;
        break;
    case TaskWorkerType::REPORT_DISK_STATE:
        _worker_count = REPORT_DISK_STATE_WORKER_COUNT;
        _callback_function = _report_disk_state_worker_thread_callback;
        break;
    case TaskWorkerType::REPORT_OLAP_TABLE:
        _worker_count = REPORT_OLAP_TABLE_WORKER_COUNT;
        _callback_function = _report_tablet_worker_thread_callback;
        break;
    case TaskWorkerType::UPLOAD:
        _worker_count = config::upload_worker_count;
        _callback_function = _upload_worker_thread_callback;
        break;
    case TaskWorkerType::DOWNLOAD:
        _worker_count = config::download_worker_count;
        _callback_function = _download_worker_thread_callback;
        break;
    case TaskWorkerType::MAKE_SNAPSHOT:
        _worker_count = config::make_snapshot_worker_count;
        _callback_function = _make_snapshot_thread_callback;
        break;
    case TaskWorkerType::RELEASE_SNAPSHOT:
        _worker_count = config::release_snapshot_worker_count;
        _callback_function = _release_snapshot_thread_callback;
        break;
    case TaskWorkerType::MOVE:
        _worker_count = 1;
        _callback_function = _move_dir_thread_callback;
        break;
    case TaskWorkerType::RECOVER_TABLET:
        _worker_count = 1;
        _callback_function = _recover_tablet_thread_callback;
        break;
    case TaskWorkerType::UPDATE_TABLET_META_INFO:
        _worker_count = 1;
        _callback_function = _update_tablet_meta_worker_thread_callback;
        break;
    default:
        // pass
        break;
    }

#ifndef BE_TEST
    for (uint32_t i = 0; i < _worker_count; i++) {
        _spawn_callback_worker_thread(_callback_function);
    }
#endif
}

void TaskWorkerPool::submit_task(const TAgentTaskRequest& task) {
    // Submit task to dequeue
    TTaskType::type task_type = task.task_type;
    int64_t signature = task.signature;
    string user("");
    if (task.__isset.resource_info) {
        user = task.resource_info.user;
    }
    bool ret = _record_task_info(task_type, signature, user);
    if (ret == true) {
        lock_guard<Mutex> worker_thread_lock(_worker_thread_lock);
        // set the task receive time
        (const_cast<TAgentTaskRequest&>(task)).__set_recv_time(time(nullptr));
        _tasks.push_back(task);
        _worker_thread_condition_lock.notify();
    }
}

bool TaskWorkerPool::_record_task_info(
        const TTaskType::type task_type,
        int64_t signature,
        const string& user) {
    bool ret = true;
    lock_guard<Mutex> task_signatures_lock(_s_task_signatures_lock);

    set<int64_t>& signature_set = _s_task_signatures[task_type];
    std::string task_name;
    EnumToString(TTaskType, task_type, task_name);
    if (signature_set.count(signature) > 0) {
        LOG(INFO) << "type: " << task_name
                  << ", signature: " << signature << ", already exist"
                  << ". queue size: " << signature_set.size();
        ret = false;
    } else {
        signature_set.insert(signature);
        LOG(INFO) << "type: " << task_name
                  << ", signature: " << signature << ", has been inserted"
                  << ", queue size: " << signature_set.size();
        if (task_type == TTaskType::PUSH) {
            _s_total_task_user_count[task_type][user] += 1;
            _s_total_task_count[task_type] += 1;
        }
    }

    return ret;
}

void TaskWorkerPool::_remove_task_info(
        const TTaskType::type task_type,
        int64_t signature,
        const string& user) {
    lock_guard<Mutex> task_signatures_lock(_s_task_signatures_lock);
    set<int64_t>& signature_set = _s_task_signatures[task_type];
    signature_set.erase(signature);

    if (task_type == TTaskType::PUSH) {
        _s_total_task_user_count[task_type][user] -= 1;
        _s_total_task_count[task_type] -= 1;

        {
            lock_guard<Mutex> running_task_user_count_lock(_s_running_task_user_count_lock);
            _s_running_task_user_count[task_type][user] -= 1;
        }
    }

    std::string task_name;
    EnumToString(TTaskType, task_type, task_name);
    LOG(INFO) << "type: " << task_name
              << ", signature: " << signature << ", has been erased"
              << ", queue size: " << signature_set.size();
}

void TaskWorkerPool::_spawn_callback_worker_thread(CALLBACK_FUNCTION callback_func) {
    // Create worker thread
    pthread_t thread;
    sigset_t mask;
    sigset_t omask;
    int err = 0;

    sigemptyset(&mask);
    sigaddset(&mask, SIGCHLD);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGPIPE);
    pthread_sigmask(SIG_SETMASK, &mask, &omask);

    while (true) {
        err = pthread_create(&thread, NULL, callback_func, this);
        if (err != 0) {
            OLAP_LOG_WARNING("failed to spawn a thread. error: %d", err);
#ifndef BE_TEST
            sleep(config::sleep_one_second);
#endif
        } else {
            pthread_detach(thread);
            break;
        }
    }
}

void TaskWorkerPool::_finish_task(const TFinishTaskRequest& finish_task_request) {
    // Return result to fe
    TMasterResult result;
    int32_t try_time = 0;

    while (try_time < TASK_FINISH_MAX_RETRY) {
        DorisMetrics::finish_task_requests_total.increment(1);
        AgentStatus client_status = _master_client->finish_task(finish_task_request, &result);

        if (client_status == DORIS_SUCCESS) {
            LOG(INFO) << "finish task success. result:" <<  result.status.status_code;
            break;
        } else {
            DorisMetrics::finish_task_requests_failed.increment(1);
            OLAP_LOG_WARNING("finish task failed.result: %d", result.status.status_code);
            try_time += 1;
        }
#ifndef BE_TEST
        sleep(config::sleep_one_second);
#endif
    }
}

uint32_t TaskWorkerPool::_get_next_task_index(
        int32_t thread_count,
        std::deque<TAgentTaskRequest>& tasks,
        TPriority::type priority) {
    deque<TAgentTaskRequest>::size_type task_count = tasks.size();
    string user;
    int32_t index = -1;
    set<string> improper_users;

    for (uint32_t i = 0; i < task_count; ++i) {
        TAgentTaskRequest task = tasks[i];
        if (task.__isset.resource_info) {
            user = task.resource_info.user;
        }

        if (priority == TPriority::HIGH) {
            if (task.__isset.priority && task.priority == TPriority::HIGH) {
                index = i;
                break;
            } else {
                continue;
            }
        }

        if (improper_users.count(user) != 0) {
            continue;
        }

        float user_total_rate = 0;
        float user_running_rate = 0;
        {
            lock_guard<Mutex> task_signatures_lock(_s_task_signatures_lock);
            user_total_rate = _s_total_task_user_count[task.task_type][user] * 1.0 /
                              _s_total_task_count[task.task_type];
            user_running_rate = (_s_running_task_user_count[task.task_type][user] + 1) * 1.0 /
                                thread_count;
        }

        LOG(INFO) << "get next task. signature:" << task.signature
                  << ", user:" << user
                  << ", total_task_user_count:" << _s_total_task_user_count[task.task_type][user]
                  << ", total_task_count:" << _s_total_task_count[task.task_type]
                  << ", running_task_user_count:" << _s_running_task_user_count[task.task_type][user] + 1
                  << ", thread_count:" << thread_count << ", user_total_rate" << user_total_rate
                  << ", user_running_rate:" << user_running_rate;
        if (_s_running_task_user_count[task.task_type][user] == 0
                || user_running_rate <= user_total_rate) {
            index = i;
            break;
        } else {
            improper_users.insert(user);
        }
    }

    if (index == -1) {
        if (priority == TPriority::HIGH) {
            return index;
        }

        index = 0;
        if (tasks[0].__isset.resource_info) {
            user = tasks[0].resource_info.user;
        } else {
            user = "";
        }
    }

    {
        lock_guard<Mutex> running_task_user_count_lock(_s_running_task_user_count_lock);
        _s_running_task_user_count[tasks[index].task_type][user] += 1;
    }
    return index;
}

void* TaskWorkerPool::_create_tablet_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TCreateTabletReq create_tablet_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            create_tablet_req = agent_task_req.create_tablet_req;
            worker_pool_this->_tasks.pop_front();
        }

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        std::vector<TTabletInfo> finish_tablet_infos;
        OLAPStatus create_status = worker_pool_this->_env->storage_engine()->create_tablet(create_tablet_req);
        if (create_status != OLAPStatus::OLAP_SUCCESS) {
            OLAP_LOG_WARNING("create table failed. status: %d, signature: %ld",
                             create_status, agent_task_req.signature);
            // TODO liutao09 distinguish the OLAPStatus
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            ++_s_report_version;
            // get path hash of the created tablet
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                create_tablet_req.tablet_id, create_tablet_req.tablet_schema.schema_hash);
            DCHECK(tablet != nullptr);
            TTabletInfo tablet_info;
            tablet_info.tablet_id = tablet->table_id(); 
            tablet_info.schema_hash = tablet->schema_hash();
            tablet_info.version = create_tablet_req.version;
            tablet_info.version_hash = create_tablet_req.version_hash;
            tablet_info.row_count = 0;
            tablet_info.data_size = 0;
            tablet_info.__set_path_hash(tablet->data_dir()->path_hash());
            finish_tablet_infos.push_back(tablet_info);
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_finish_tablet_infos(finish_tablet_infos);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_report_version(_s_report_version);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_drop_tablet_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TDropTabletReq drop_tablet_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            drop_tablet_req = agent_task_req.drop_tablet_req;
            worker_pool_this->_tasks.pop_front();
        }

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;
        TabletSharedPtr dropped_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            drop_tablet_req.tablet_id, drop_tablet_req.schema_hash);
        if (dropped_tablet != nullptr) {
            OLAPStatus drop_status = StorageEngine::instance()->tablet_manager()->drop_tablet(
                drop_tablet_req.tablet_id, drop_tablet_req.schema_hash);
            if (drop_status != OLAP_SUCCESS ) {
                LOG(WARNING) << "drop table failed! signature: " << agent_task_req.signature;
                error_msgs.push_back("drop table failed!");
                status_code = TStatusCode::RUNTIME_ERROR;
            }
            // if tablet is dropped by fe, then the related txn should also be removed
            StorageEngine::instance()->txn_manager()->force_rollback_tablet_related_txns(dropped_tablet->data_dir()->get_meta(), 
                drop_tablet_req.tablet_id, drop_tablet_req.schema_hash, dropped_tablet->tablet_uid());
        }
        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_alter_tablet_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            worker_pool_this->_tasks.pop_front();
        }
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        int64_t signatrue = agent_task_req.signature;
        LOG(INFO) << "get alter table task, signature: " <<  agent_task_req.signature;
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
            case TTaskType::SCHEMA_CHANGE:
            case TTaskType::ROLLUP:
            case TTaskType::ALTER:
                worker_pool_this->_alter_tablet(worker_pool_this,
                                            agent_task_req,
                                            signatrue,
                                            task_type,
                                            &finish_task_request);
                break;
            default:
                // pass
                break;
            }
            worker_pool_this->_finish_task(finish_task_request);
        }
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void TaskWorkerPool::_alter_tablet(
        TaskWorkerPool* worker_pool_this,
        const TAgentTaskRequest& agent_task_req,
        int64_t signature,
        const TTaskType::type task_type,
        TFinishTaskRequest* finish_task_request) {
    AgentStatus status = DORIS_SUCCESS;
    TStatus task_status;
    vector<string> error_msgs;

    string process_name;
    switch (task_type) {
    case TTaskType::ROLLUP:
        process_name = "roll up";
        break;
    case TTaskType::SCHEMA_CHANGE:
        process_name = "schema change";
        break;
    case TTaskType::ALTER:
        process_name = "alter";
        break;
    default:
        std::string task_name;
        EnumToString(TTaskType, task_type, task_name);
        LOG(WARNING) << "schema change type invalid. type: " << task_name
                     << ", signature: " << signature;
        status = DORIS_TASK_REQUEST_ERROR;
        break;
    }

    // Check last schema change status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    TTabletId new_tablet_id;
    TSchemaHash new_schema_hash = 0;
    if (status == DORIS_SUCCESS) {
        OLAPStatus sc_status = OLAP_SUCCESS;
        if (task_type == TTaskType::ALTER) {
            new_tablet_id = agent_task_req.alter_tablet_req_v2.new_tablet_id;
            new_schema_hash = agent_task_req.alter_tablet_req_v2.new_schema_hash;
            EngineAlterTabletTask engine_task(agent_task_req.alter_tablet_req_v2, signature, task_type, &error_msgs, process_name);
            sc_status = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
        } else {
            new_tablet_id = agent_task_req.alter_tablet_req.new_tablet_req.tablet_id;
            new_schema_hash = agent_task_req.alter_tablet_req.new_tablet_req.tablet_schema.schema_hash;
            EngineSchemaChangeTask engine_task(agent_task_req.alter_tablet_req, signature, task_type, &error_msgs, process_name);
            sc_status = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
        }
        if (sc_status != OLAP_SUCCESS) {
            status = DORIS_ERROR;
        } else {
            status = DORIS_SUCCESS;
        }
    }

    if (status == DORIS_SUCCESS) {
        ++_s_report_version;
        LOG(INFO) << process_name << " finished. signature: " << signature;
    }

    // Return result to fe
    finish_task_request->__set_backend(_backend);
    finish_task_request->__set_report_version(_s_report_version);
    finish_task_request->__set_task_type(task_type);
    finish_task_request->__set_signature(signature);

    vector<TTabletInfo> finish_tablet_infos;
    if (status == DORIS_SUCCESS) {
        TTabletInfo tablet_info;
        status = _get_tablet_info(
                new_tablet_id,
                new_schema_hash,
                signature,
                &tablet_info);

        if (status != DORIS_SUCCESS) {
            LOG(WARNING) << process_name<< " success, but get new tablet info failed."
                         << "tablet_id: " << new_tablet_id
                         << ", schema_hash: " << new_schema_hash
                         << ", signature: " << signature;
        } else {
            finish_tablet_infos.push_back(tablet_info);
        }
    }

    if (status == DORIS_SUCCESS) {
        finish_task_request->__set_finish_tablet_infos(finish_tablet_infos);
        LOG(INFO) << process_name << " success. signature: " << signature;
        error_msgs.push_back(process_name + " success");
        task_status.__set_status_code(TStatusCode::OK);
    } else if (status == DORIS_TASK_REQUEST_ERROR) {
        LOG(WARNING) << "alter table request task type invalid. "
                     << "signature:" << signature;
        error_msgs.push_back("alter table request new tablet id or schema count invalid.");
        task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
    } else {
        LOG(WARNING) << process_name << " failed. signature: " << signature;
        error_msgs.push_back(process_name + " failed");
        error_msgs.push_back("status: " + _agent_utils->print_agent_status(status));
        task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
    }

    task_status.__set_error_msgs(error_msgs);
    finish_task_request->__set_task_status(task_status);
}

void* TaskWorkerPool::_push_worker_thread_callback(void* arg_this) {
    // Try to register to cgroups_mgr
    CgroupsMgr::apply_system_cgroup();
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    // gen high priority worker thread
    TPriority::type priority = TPriority::NORMAL;
    int32_t push_worker_count_high_priority = config::push_worker_count_high_priority;
    static uint32_t s_worker_count = 0;
    {
        lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
        if (s_worker_count < push_worker_count_high_priority) {
            ++s_worker_count;
            priority = TPriority::HIGH;
        }
    }

#ifndef BE_TEST
    while (true) {
#endif
        AgentStatus status = DORIS_SUCCESS;
        TAgentTaskRequest agent_task_req;
        TPushReq push_req;
        string user;
        int32_t index = 0;
        do {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            index = worker_pool_this->_get_next_task_index(
                    config::push_worker_count_normal_priority
                            + config::push_worker_count_high_priority,
                    worker_pool_this->_tasks, priority);

            if (index < 0) {
                // there is no high priority task. notify other thread to handle normal task
                worker_pool_this->_worker_thread_condition_lock.notify();
                break;
            }

            agent_task_req = worker_pool_this->_tasks[index];
            if (agent_task_req.__isset.resource_info) {
                user = agent_task_req.resource_info.user;
            }
            push_req = agent_task_req.push_req;
            worker_pool_this->_tasks.erase(worker_pool_this->_tasks.begin() + index);
        } while (0);

#ifndef BE_TEST
        if (index < 0) {
            // there is no high priority task in queue
            sleep(1);
            continue;
        }
#endif

        LOG(INFO) << "get push task. signature: " << agent_task_req.signature
                  << " user: " << user << " priority: " << priority;
        vector<TTabletInfo> tablet_infos;
        
        EngineBatchLoadTask engine_task(push_req, &tablet_infos, agent_task_req.signature, &status);
        worker_pool_this->_env->storage_engine()->execute_task(&engine_task);

#ifndef BE_TEST
        if (status == DORIS_PUSH_HAD_LOADED) {
            // remove the task and not return to fe
            worker_pool_this->_remove_task_info(
                agent_task_req.task_type, agent_task_req.signature, user);
            continue;
        }
#endif
        // Return result to fe
        vector<string> error_msgs;
        TStatus task_status;

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        if (push_req.push_type == TPushType::DELETE) {
            finish_task_request.__set_request_version(push_req.version);
            finish_task_request.__set_request_version_hash(push_req.version_hash);
        }

        if (status == DORIS_SUCCESS) {
            VLOG(3) << "push ok.signature: " << agent_task_req.signature;
            error_msgs.push_back("push success");

            ++_s_report_version;

            task_status.__set_status_code(TStatusCode::OK);
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        } else if (status == DORIS_TASK_REQUEST_ERROR) {
            OLAP_LOG_WARNING("push request push_type invalid. type: %d, signature: %ld",
                             push_req.push_type, agent_task_req.signature);
            error_msgs.push_back("push request push_type invalid.");
            task_status.__set_status_code(TStatusCode::ANALYSIS_ERROR);
        } else {
            OLAP_LOG_WARNING("push failed, error_code: %d, signature: %ld",
                             status, agent_task_req.signature);
            error_msgs.push_back("push failed");
            task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        }
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_report_version(_s_report_version);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(
                agent_task_req.task_type, agent_task_req.signature, user);
#ifndef BE_TEST
    }
#endif

    return (void*)0;
}

void* TaskWorkerPool::_publish_version_worker_thread_callback(void* arg_this) {

    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;
#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TPublishVersionRequest publish_version_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            publish_version_req = agent_task_req.publish_version_req;
            worker_pool_this->_tasks.pop_front();
        }

        DorisMetrics::publish_task_request_total.increment(1);
        LOG(INFO)<< "get publish version task, signature:" << agent_task_req.signature;

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        vector<TTabletId> error_tablet_ids;
        uint32_t retry_time = 0;
        OLAPStatus res = OLAP_SUCCESS;
        while (retry_time < PUBLISH_VERSION_MAX_RETRY) {
            error_tablet_ids.clear();
            EnginePublishVersionTask engine_task(publish_version_req, &error_tablet_ids);
            res = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
            if (res == OLAP_SUCCESS) {
                break;
            } else {
                OLAP_LOG_WARNING("publish version error, retry. "
                                 "[transaction_id=%ld, error_tablet_size=%d]",
                                 publish_version_req.transaction_id, error_tablet_ids.size());
                retry_time += 1;
                sleep(1);
            }
        }

        TFinishTaskRequest finish_task_request;
        if (res != OLAP_SUCCESS) {
            // if publish failed, return failed, fe will ignore this error and 
            // check error tablet ids and fe will also republish this task
            status_code = TStatusCode::RUNTIME_ERROR;
            LOG(WARNING) << "publish version failed. signature:" << agent_task_req.signature;
            error_msgs.push_back("publish version failed");
            finish_task_request.__set_error_tablet_ids(error_tablet_ids);
            DorisMetrics::publish_task_failed_total.increment(1);
        } else {
            _s_report_version++;
            LOG(INFO) << "publish_version success. signature:" << agent_task_req.signature;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_report_version(_s_report_version);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_clear_alter_task_worker_thread_callback(void* arg_this) {

    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;
#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TClearAlterTaskRequest clear_alter_task_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            clear_alter_task_req = agent_task_req.clear_alter_task_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "get clear alter task task, signature:" << agent_task_req.signature;

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;
        EngineClearAlterTask engine_task(clear_alter_task_req);
        OLAPStatus clear_status = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
        if (clear_status != OLAPStatus::OLAP_SUCCESS) {
            OLAP_LOG_WARNING("clear alter task failed. [signature: %ld status=%d]",
                             agent_task_req.signature, clear_status);
            error_msgs.push_back("clear alter task failed");
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            LOG(INFO) << "clear alter task success. signature:" << agent_task_req.signature;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_clear_transaction_task_worker_thread_callback(void* arg_this) {

    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;
#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TClearTransactionTaskRequest clear_transaction_task_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            clear_transaction_task_req = agent_task_req.clear_transaction_task_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "get clear transaction task task, signature:" << agent_task_req.signature
                  << ", transaction_id: " << clear_transaction_task_req.transaction_id
                  << ", partition id size: " << clear_transaction_task_req.partition_id.size();

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        if (clear_transaction_task_req.transaction_id > 0) {
            // transaction_id should be greater than zero.
            // If it is not greater than zero, no need to execute
            // the following clear_transaction_task() function.
            if (!clear_transaction_task_req.partition_id.empty()) {
                worker_pool_this->_env->storage_engine()->clear_transaction_task(
                        clear_transaction_task_req.transaction_id, clear_transaction_task_req.partition_id);
            } else {
                worker_pool_this->_env->storage_engine()->clear_transaction_task(
                        clear_transaction_task_req.transaction_id);
            }
            LOG(INFO) << "finish to clear transaction task. signature:" << agent_task_req.signature
                      << ", transaction_id: " << clear_transaction_task_req.transaction_id;
        } else {
            LOG(WARNING) << "invalid transaction id: " << clear_transaction_task_req.transaction_id
                         << ", signature: " << agent_task_req.signature;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_update_tablet_meta_worker_thread_callback(void* arg_this) {

    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;
    while (true) {
        TAgentTaskRequest agent_task_req;
        TUpdateTabletMetaInfoReq update_tablet_meta_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            update_tablet_meta_req = agent_task_req.update_tablet_meta_info_req;
            worker_pool_this->_tasks.pop_front();
        }
        LOG(INFO) << "get update tablet meta task, signature:" << agent_task_req.signature;

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        for (auto tablet_meta_info : update_tablet_meta_req.tabletMetaInfos) {
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                tablet_meta_info.tablet_id, tablet_meta_info.schema_hash);
            if (tablet == nullptr) {
                LOG(WARNING) << "could not find tablet when update partition id"
                             << " tablet_id=" << tablet_meta_info.tablet_id
                             << " schema_hash=" << tablet_meta_info.schema_hash;
                continue;
            }
            WriteLock wrlock(tablet->get_header_lock_ptr());
            tablet->set_partition_id(tablet_meta_info.partition_id);
            tablet->save_meta();
        }

        LOG(INFO) << "finish update tablet meta task. signature:" << agent_task_req.signature;

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
    }
    return (void*)0;
}

void* TaskWorkerPool::_clone_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        AgentStatus status = DORIS_SUCCESS;
        TAgentTaskRequest agent_task_req;
        TCloneReq clone_req;

        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            clone_req = agent_task_req.clone_req;
            worker_pool_this->_tasks.pop_front();
        }

        DorisMetrics::clone_requests_total.increment(1);
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        LOG(INFO) << "get clone task. signature:" << agent_task_req.signature;

        vector<string> error_msgs;
        vector<TTabletInfo> tablet_infos;
        EngineCloneTask engine_task(clone_req, worker_pool_this->_master_info,
                                    agent_task_req.signature,
                                    &error_msgs, &tablet_infos,
                                    &status);
        worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
        // Return result to fe
        TStatus task_status;
        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);

        TStatusCode::type status_code = TStatusCode::OK;
        if (status != DORIS_SUCCESS && status != DORIS_CREATE_TABLE_EXIST) {
            DorisMetrics::clone_requests_failed.increment(1);
            status_code = TStatusCode::RUNTIME_ERROR;
            OLAP_LOG_WARNING("clone failed. signature: %ld",
                             agent_task_req.signature);
            error_msgs.push_back("clone failed.");
        } else {
            LOG(INFO) << "clone success, set tablet infos."
                      << "signature:" << agent_task_req.signature;
            finish_task_request.__set_finish_tablet_infos(tablet_infos);
        }
        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif

    return (void*)0;
}

void* TaskWorkerPool::_storage_medium_migrate_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        TAgentTaskRequest agent_task_req;
        TStorageMediumMigrateReq storage_medium_migrate_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            storage_medium_migrate_req = agent_task_req.storage_medium_migrate_req;
            worker_pool_this->_tasks.pop_front();
        }

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;
        EngineStorageMigrationTask engine_task(storage_medium_migrate_req);
        OLAPStatus res = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("storage media migrate failed. status: %d, signature: %ld",
                             res, agent_task_req.signature);
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            LOG(INFO) << "storage media migrate success. status:" << res << ","
                      << ", signature:" << agent_task_req.signature;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_check_consistency_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        TAgentTaskRequest agent_task_req;
        TCheckConsistencyReq check_consistency_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            check_consistency_req = agent_task_req.check_consistency_req;
            worker_pool_this->_tasks.pop_front();
        }

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        uint32_t checksum = 0;
        EngineChecksumTask engine_task(check_consistency_req.tablet_id,
                check_consistency_req.schema_hash,
                check_consistency_req.version,
                check_consistency_req.version_hash,
                &checksum);
        OLAPStatus res = worker_pool_this->_env->storage_engine()->execute_task(&engine_task);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("check consistency failed. status: %d, signature: %ld",
                             res, agent_task_req.signature);
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            LOG(INFO) << "check consistency success. status:" << res
                      << ", signature:" << agent_task_req.signature
                      << ", checksum:" << checksum;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_tablet_checksum(static_cast<int64_t>(checksum));
        finish_task_request.__set_request_version(check_consistency_req.version);
        finish_task_request.__set_request_version_hash(check_consistency_req.version_hash);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_report_task_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    TReportRequest request;
    request.__set_force_recovery(config::force_recovery);
    request.__set_backend(worker_pool_this->_backend);

#ifndef BE_TEST
    while (true) {
#endif
        {
            lock_guard<Mutex> task_signatures_lock(_s_task_signatures_lock);
            request.__set_tasks(_s_task_signatures);
        }

        DorisMetrics::report_task_requests_total.increment(1);
        TMasterResult result;
        AgentStatus status = worker_pool_this->_master_client->report(request, &result);

        if (status != DORIS_SUCCESS) {
            DorisMetrics::report_task_requests_failed.increment(1);
            LOG(WARNING) << "finish report task failed. status:" << status
                << ", master host:" << worker_pool_this->_master_info.network_address.hostname
                << "port:" << worker_pool_this->_master_info.network_address.port;
        }

#ifndef BE_TEST
        sleep(config::report_task_interval_seconds);
    }
#endif

    return (void*)0;
}

void* TaskWorkerPool::_report_disk_state_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    TReportRequest request;
    request.__set_force_recovery(config::force_recovery);
    request.__set_backend(worker_pool_this->_backend);

#ifndef BE_TEST
    while (true) {
        if (worker_pool_this->_master_info.network_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            LOG(INFO) << "waiting to receive first heartbeat from frontend";
            sleep(config::sleep_one_second);
            continue;
        }
#endif
        vector<DataDirInfo> data_dir_infos;
        worker_pool_this->_env->storage_engine()->get_all_data_dir_info(&data_dir_infos, true /* update */);

        map<string, TDisk> disks;
        for (auto& root_path_info : data_dir_infos) {
            TDisk disk;
            disk.__set_root_path(root_path_info.path);
            disk.__set_path_hash(root_path_info.path_hash);
            disk.__set_storage_medium(root_path_info.storage_medium);
            disk.__set_disk_total_capacity(static_cast<double>(root_path_info.capacity));
            disk.__set_data_used_capacity(static_cast<double>(root_path_info.data_used_capacity));
            disk.__set_disk_available_capacity(static_cast<double>(root_path_info.available));
            disk.__set_used(root_path_info.is_used);
            disks[root_path_info.path] = disk;

            DorisMetrics::disks_total_capacity.set_metric(root_path_info.path, root_path_info.capacity);
            DorisMetrics::disks_avail_capacity.set_metric(root_path_info.path, root_path_info.available);
            DorisMetrics::disks_data_used_capacity.set_metric(root_path_info.path, root_path_info.data_used_capacity);
            DorisMetrics::disks_state.set_metric(root_path_info.path, root_path_info.is_used ? 1L : 0L);
        }
        request.__set_disks(disks);

        DorisMetrics::report_disk_requests_total.increment(1);
        TMasterResult result;
        AgentStatus status = worker_pool_this->_master_client->report(request, &result);

        if (status != DORIS_SUCCESS) {
            DorisMetrics::report_disk_requests_failed.increment(1);
            LOG(WARNING) << "finish report disk state failed. status:" << status
                << ", master host:" << worker_pool_this->_master_info.network_address.hostname
                << ", port:" << worker_pool_this->_master_info.network_address.port;
        }

#ifndef BE_TEST
        // wait for notifying until timeout
        StorageEngine::instance()->wait_for_report_notify(
                config::report_disk_state_interval_seconds, false);
    }
#endif

    return (void*)0;
}

void* TaskWorkerPool::_report_tablet_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    TReportRequest request;
    request.__set_force_recovery(config::force_recovery);
    request.__set_backend(worker_pool_this->_backend);
    request.__isset.tablets = true;
    AgentStatus status = DORIS_SUCCESS;

#ifndef BE_TEST
    while (true) {
        if (worker_pool_this->_master_info.network_address.port == 0) {
            // port == 0 means not received heartbeat yet
            // sleep a short time and try again
            LOG(INFO) << "waiting to receive first heartbeat from frontend";
            sleep(config::sleep_one_second);
            continue;
        }
#endif
        request.tablets.clear();

        request.__set_report_version(_s_report_version);
        OLAPStatus report_all_tablets_info_status =
                StorageEngine::instance()->tablet_manager()->report_all_tablets_info(&request.tablets);
        if (report_all_tablets_info_status != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("report get all tablets info failed. status: %d",
                             report_all_tablets_info_status);
#ifndef BE_TEST
            // wait for notifying until timeout
            StorageEngine::instance()->wait_for_report_notify(
                    config::report_tablet_interval_seconds, true);
            continue;
#else
            return (void*)0;
#endif
        }

        TMasterResult result;
        status = worker_pool_this->_master_client->report(request, &result);

        if (status != DORIS_SUCCESS) {
            DorisMetrics::report_all_tablets_requests_failed.increment(1);
            LOG(WARNING) << "finish report olap table state failed. status:" << status
                << ", master host:" << worker_pool_this->_master_info.network_address.hostname
                << ", port:" << worker_pool_this->_master_info.network_address.port;
        }

#ifndef BE_TEST
        // wait for notifying until timeout
        StorageEngine::instance()->wait_for_report_notify(
                config::report_tablet_interval_seconds, true);
    }
#endif

    return (void*)0;
}

void* TaskWorkerPool::_upload_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*) arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TUploadReq upload_request;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            upload_request = agent_task_req.upload_req;
            worker_pool_this->_tasks.pop_front();
        }

        LOG(INFO) << "get upload task, signature:" << agent_task_req.signature
                  << ", job id:" << upload_request.job_id;

        std::map<int64_t, std::vector<std::string>> tablet_files;
        SnapshotLoader loader(worker_pool_this->_env, upload_request.job_id, agent_task_req.signature);
        Status status = loader.upload(
                upload_request.src_dest_map,
                upload_request.broker_addr,
                upload_request.broker_prop,
                &tablet_files);

        TStatusCode::type status_code = TStatusCode::OK; 
        std::vector<string> error_msgs;
        if (!status.ok()) {
            status_code = TStatusCode::RUNTIME_ERROR;
            OLAP_LOG_WARNING("upload failed. job id: %ld, msg: %s",
                upload_request.job_id,
                status.get_error_msg().c_str());
            error_msgs.push_back(status.get_error_msg());
        }

        TStatus task_status;
        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_tablet_files(tablet_files);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");

        LOG(INFO) << "finished upload task, signature: " << agent_task_req.signature
                  << ", job id:" << upload_request.job_id;
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_download_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TDownloadReq download_request;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            download_request = agent_task_req.download_req;
            worker_pool_this->_tasks.pop_front();
        }
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        LOG(INFO) << "get download task, signature: " << agent_task_req.signature
                  << ", job id:" << download_request.job_id;

        TStatusCode::type status_code = TStatusCode::OK;
        std::vector<string> error_msgs;
        TStatus task_status;

        // TODO: download
        std::vector<int64_t> downloaded_tablet_ids;
        SnapshotLoader loader(worker_pool_this->_env, download_request.job_id, agent_task_req.signature);
        Status status = loader.download(
                download_request.src_dest_map,
                download_request.broker_addr,
                download_request.broker_prop,
                &downloaded_tablet_ids);

        if (!status.ok()) {
            status_code = TStatusCode::RUNTIME_ERROR;
            OLAP_LOG_WARNING("download failed. job id: %ld, msg: %s",
                download_request.job_id,
                status.get_error_msg().c_str());
            error_msgs.push_back(status.get_error_msg());
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);
        finish_task_request.__set_downloaded_tablet_ids(downloaded_tablet_ids);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");

        LOG(INFO) << "finished download task, signature: " << agent_task_req.signature
                  << ", job id:" << download_request.job_id;
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_make_snapshot_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TSnapshotRequest snapshot_request;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                 worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            snapshot_request = agent_task_req.snapshot_req;
            worker_pool_this->_tasks.pop_front();
        }
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        LOG(INFO) << "get snapshot task, signature:" <<  agent_task_req.signature;

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        string snapshot_path;
        std::vector<string> snapshot_files;
        OLAPStatus make_snapshot_status = SnapshotManager::instance()->make_snapshot(
                snapshot_request, &snapshot_path);
        if (make_snapshot_status != OLAP_SUCCESS) {
            status_code = TStatusCode::RUNTIME_ERROR;
            OLAP_LOG_WARNING("make_snapshot failed. tablet_id: %ld, schema_hash: %ld, version: %d,"
                             "version_hash: %ld, status: %d",
                             snapshot_request.tablet_id, snapshot_request.schema_hash,
                             snapshot_request.version, snapshot_request.version_hash,
                             make_snapshot_status);
            error_msgs.push_back("make_snapshot failed. status: " +
                                 boost::lexical_cast<string>(make_snapshot_status));
        } else {
            LOG(INFO) << "make_snapshot success. tablet_id:" << snapshot_request.tablet_id
                      << ", schema_hash:" << snapshot_request.schema_hash
                      << ", version:" << snapshot_request.version
                      << ", version_hash:" << snapshot_request.version_hash
                      << ", snapshot_path:" << snapshot_path;
            if (snapshot_request.__isset.list_files) {
                // list and save all snapshot files
                // snapshot_path like: data/snapshot/20180417205230.1.86400
                // we need to add subdir: tablet_id/schema_hash/
                std::stringstream ss;
                ss << snapshot_path << "/" << snapshot_request.tablet_id
                    << "/" << snapshot_request.schema_hash << "/";
                Status st = FileUtils::list_files(Env::Default(), ss.str(), &snapshot_files); 
                if (!st.ok()) {
                    status_code = TStatusCode::RUNTIME_ERROR;
                    OLAP_LOG_WARNING("make_snapshot failed. tablet_id: %ld, schema_hash: %ld, version: %d,"
                        "version_hash: %ld, list file failed: %s",
                        snapshot_request.tablet_id, snapshot_request.schema_hash,
                        snapshot_request.version, snapshot_request.version_hash,
                        st.get_error_msg().c_str());
                    error_msgs.push_back("make_snapshot failed. list file failed: " +
                                 st.get_error_msg());
                }
            }
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_snapshot_path(snapshot_path);
        finish_task_request.__set_snapshot_files(snapshot_files);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void* TaskWorkerPool::_release_snapshot_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TReleaseSnapshotRequest release_snapshot_request;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            release_snapshot_request = agent_task_req.release_snapshot_req;
            worker_pool_this->_tasks.pop_front();
        }
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        LOG(INFO) << "get release snapshot task, signature:" << agent_task_req.signature;

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        string& snapshot_path = release_snapshot_request.snapshot_path;
        OLAPStatus release_snapshot_status =
                SnapshotManager::instance()->release_snapshot(snapshot_path);
        if (release_snapshot_status != OLAP_SUCCESS) {
            status_code = TStatusCode::RUNTIME_ERROR;
            LOG(WARNING) << "release_snapshot failed. snapshot_path: " << snapshot_path
                         << ". status: " << release_snapshot_status;
            error_msgs.push_back("release_snapshot failed. status: " +
                                 boost::lexical_cast<string>(release_snapshot_status));
        } else {
            LOG(INFO) << "release_snapshot success. snapshot_path: " << snapshot_path
                      << ". status: " << release_snapshot_status;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

AgentStatus TaskWorkerPool::_get_tablet_info(
        const TTabletId tablet_id,
        const TSchemaHash schema_hash,
        int64_t signature,
        TTabletInfo* tablet_info) {
    AgentStatus status = DORIS_SUCCESS;

    tablet_info->__set_tablet_id(tablet_id);
    tablet_info->__set_schema_hash(schema_hash);
    OLAPStatus olap_status = StorageEngine::instance()->tablet_manager()->report_tablet_info(tablet_info);
    if (olap_status != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("get tablet info failed. status: %d, signature: %ld",
                         olap_status, signature);
        status = DORIS_ERROR;
    }
    return status;
}

void* TaskWorkerPool::_move_dir_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TMoveDirReq move_dir_req;
        {
            MutexLock worker_thread_lock(&(worker_pool_this->_worker_thread_lock));
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            move_dir_req =  agent_task_req.move_dir_req;
            worker_pool_this->_tasks.pop_front();
        }
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        LOG(INFO) << "get move dir task, signature:" << agent_task_req.signature
                  << ", job id:" << move_dir_req.job_id;

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        // TODO: move dir
        AgentStatus status = worker_pool_this->_move_dir(
                    move_dir_req.tablet_id,
                    move_dir_req.schema_hash,
                    move_dir_req.src,
                    move_dir_req.job_id,
                    true /* TODO */,
                    &error_msgs);

        if (status != DORIS_SUCCESS) {
            status_code = TStatusCode::RUNTIME_ERROR;
            OLAP_LOG_WARNING("failed to move dir: %s, tablet id: %ld, signature: %ld, job id: %ld",
                    move_dir_req.src.c_str(), move_dir_req.tablet_id, agent_task_req.signature,
                    move_dir_req.job_id);
        } else {
            LOG(INFO) << "finished to move dir:" << move_dir_req.src
                      << ", tablet_id:" << move_dir_req.tablet_id
                      << ", signature:" << agent_task_req.signature
                      << ", job id:" << move_dir_req.job_id;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");

#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

AgentStatus TaskWorkerPool::_move_dir(
     const TTabletId tablet_id,
     const TSchemaHash schema_hash,
     const std::string& src,
     int64_t job_id,
     bool overwrite,
     std::vector<std::string>* error_msgs) {

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                tablet_id, schema_hash);
    if (tablet == nullptr) {
        LOG(INFO) << "failed to get tablet. tablet_id:" << tablet_id
                  << ", schema hash:" << schema_hash;
        error_msgs->push_back("failed to get tablet");
        return DORIS_TASK_REQUEST_ERROR;
    }

    std::string dest_tablet_dir = tablet->tablet_path();
    SnapshotLoader loader(_env, job_id, tablet_id);
    Status status = loader.move(src, tablet, overwrite);

    if (!status.ok()) {
        LOG(WARNING) << "move failed. job id: " << job_id << ", msg: " << status.get_error_msg();
        error_msgs->push_back(status.get_error_msg());
        return DORIS_INTERNAL_ERROR;
    }

    return DORIS_SUCCESS;
}

void* TaskWorkerPool::_recover_tablet_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

    while (true) {
        TAgentTaskRequest agent_task_req;
        TRecoverTabletReq recover_tablet_req;
        {
            MutexLock worker_thread_lock(&(worker_pool_this->_worker_thread_lock));
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            recover_tablet_req = agent_task_req.recover_tablet_req;
            worker_pool_this->_tasks.pop_front();
        }
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        LOG(INFO) << "begin to recover tablet."
              << ", tablet_id:" << recover_tablet_req.tablet_id << "." << recover_tablet_req.schema_hash
              << ", version:" << recover_tablet_req.version << "-" << recover_tablet_req.version_hash;
        OLAPStatus status = worker_pool_this->_env->storage_engine()->recover_tablet_until_specfic_version(recover_tablet_req);
        if (status != OLAP_SUCCESS) {
            status_code = TStatusCode::RUNTIME_ERROR;
            LOG(WARNING) << "failed to recover tablet."
                << "signature:" << agent_task_req.signature
                << ", table:" << recover_tablet_req.tablet_id << "." << recover_tablet_req.schema_hash
                << ", version:" << recover_tablet_req.version << "-" << recover_tablet_req.version_hash;
        } else {
            LOG(WARNING) << "succeed to recover tablet."
                << "signature:" << agent_task_req.signature
                << ", table:" << recover_tablet_req.tablet_id << "." << recover_tablet_req.schema_hash
                << ", version:" << recover_tablet_req.version << "-" << recover_tablet_req.version_hash;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
        finish_task_request.__set_backend(worker_pool_this->_backend);
        finish_task_request.__set_task_type(agent_task_req.task_type);
        finish_task_request.__set_signature(agent_task_req.signature);
        finish_task_request.__set_task_status(task_status);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");

    }
    return (void*)0;
}

}  // namespace doris
