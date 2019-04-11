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

#include "boost/filesystem.hpp"
#include "boost/lexical_cast.hpp"
#include "agent/pusher.h"
#include "agent/status.h"
#include "agent/utils.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_types.h"
#include "http/http_client.h"
#include "olap/olap_common.h"
#include "olap/olap_engine.h"
#include "olap/olap_table.h"
#include "olap/store.h"
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

const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;
const uint32_t TASK_FINISH_MAX_RETRY = 3;
const uint32_t PUSH_MAX_RETRY = 1;
const uint32_t PUBLISH_VERSION_MAX_RETRY = 3;
const uint32_t REPORT_TASK_WORKER_COUNT = 1;
const uint32_t REPORT_DISK_STATE_WORKER_COUNT = 1;
const uint32_t REPORT_OLAP_TABLE_WORKER_COUNT = 1;
const uint32_t LIST_REMOTE_FILE_TIMEOUT = 15;
const std::string HTTP_REQUEST_PREFIX = "/api/_tablet/_download?";
const std::string HTTP_REQUEST_TOKEN_PARAM = "token=";
const std::string HTTP_REQUEST_FILE_PARAM = "&file=";

const uint32_t GET_LENGTH_TIMEOUT = 10;
const uint32_t CURL_OPT_CONNECTTIMEOUT = 120;

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
        _worker_count = config::create_table_worker_count;
        _callback_function = _create_table_worker_thread_callback;
        break;
    case TaskWorkerType::DROP_TABLE:
        _worker_count = config::drop_table_worker_count;
        _callback_function = _drop_table_worker_thread_callback;
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
        _worker_count = config::alter_table_worker_count;
        _callback_function = _alter_table_worker_thread_callback;
        break;
    case TaskWorkerType::CLONE:
        _worker_count = config::clone_worker_count;
        _callback_function = _clone_worker_thread_callback;
        break;
    case TaskWorkerType::STORAGE_MEDIUM_MIGRATE:
        _worker_count = config::storage_medium_migrate_count;
        _callback_function = _storage_medium_migrate_worker_thread_callback;
        break;
    case TaskWorkerType::CANCEL_DELETE_DATA:
        _worker_count = config::cancel_delete_data_worker_count;
        _callback_function = _cancel_delete_data_worker_thread_callback;
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
        _callback_function = _report_olap_table_worker_thread_callback;
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
        {
            lock_guard<Mutex> worker_thread_lock(_worker_thread_lock);
            _tasks.push_back(task);
            _worker_thread_condition_lock.notify();
        }
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
                  << ". queue size: " << signature_set.size();
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

void* TaskWorkerPool::_create_table_worker_thread_callback(void* arg_this) {
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

        OLAPStatus create_status =
            worker_pool_this->_env->olap_engine()->create_table(create_tablet_req);
        if (create_status != OLAPStatus::OLAP_SUCCESS) {
            OLAP_LOG_WARNING("create table failed. status: %d, signature: %ld",
                             create_status, agent_task_req.signature);
            // TODO liutao09 distinguish the OLAPStatus
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            ++_s_report_version;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

        TFinishTaskRequest finish_task_request;
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

void* TaskWorkerPool::_drop_table_worker_thread_callback(void* arg_this) {
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

        AgentStatus status = worker_pool_this->_drop_table(drop_tablet_req);
        if (status != DORIS_SUCCESS) {
            OLAP_LOG_WARNING(
                "drop table failed! signature: %ld", agent_task_req.signature);
            error_msgs.push_back("drop table failed!");
            status_code = TStatusCode::RUNTIME_ERROR;
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

void* TaskWorkerPool::_alter_table_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TAlterTabletReq alter_tablet_request;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            alter_tablet_request = agent_task_req.alter_tablet_req;
            worker_pool_this->_tasks.pop_front();
        }
        // Try to register to cgroups_mgr
        CgroupsMgr::apply_system_cgroup();
        int64_t signatrue = agent_task_req.signature;
        LOG(INFO) << "get alter table task, signature: " <<  agent_task_req.signature;

        TFinishTaskRequest finish_task_request;
        TTaskType::type task_type = agent_task_req.task_type;
        switch (task_type) {
        case TTaskType::SCHEMA_CHANGE:
        case TTaskType::ROLLUP:
            worker_pool_this->_alter_table(alter_tablet_request,
                                           signatrue,
                                           task_type,
                                           &finish_task_request);
            break;
        default:
            // pass
            break;
        }

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif
    return (void*)0;
}

void TaskWorkerPool::_alter_table(
        const TAlterTabletReq& alter_tablet_request,
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
    default:
        std::string task_name;
        EnumToString(TTaskType, task_type, task_name);
        LOG(WARNING) << "schema change type invalid. type: " << task_name
                     << ", signature: " << signature;
        status = DORIS_TASK_REQUEST_ERROR;
        break;
    }

    TTabletId base_tablet_id = alter_tablet_request.base_tablet_id;
    TSchemaHash base_schema_hash = alter_tablet_request.base_schema_hash;

    // Check last schema change status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    if (status == DORIS_SUCCESS) {
        // Check lastest schema change status
        AlterTableStatus alter_table_status = _show_alter_table_status(
                base_tablet_id,
                base_schema_hash);
        LOG(INFO) << "get alter table status:" << alter_table_status
                  << ", signature:" << signature;

        // Delete failed alter table tablet file
        if (alter_table_status == ALTER_TABLE_FAILED) {
            TDropTabletReq drop_tablet_req;
            drop_tablet_req.__set_tablet_id(alter_tablet_request.new_tablet_req.tablet_id);
            drop_tablet_req.__set_schema_hash(alter_tablet_request.new_tablet_req.tablet_schema.schema_hash);
            status = _drop_table(drop_tablet_req);

            if (status != DORIS_SUCCESS) {
                OLAP_LOG_WARNING("delete failed rollup file failed, status: %d, "
                                 "signature: %ld.",
                                 status, signature);
                error_msgs.push_back("delete failed rollup file failed, "
                                     "signature: " + to_string(signature));
            }
        }

        if (status == DORIS_SUCCESS) {
            if (alter_table_status == ALTER_TABLE_FINISHED
                    || alter_table_status == ALTER_TABLE_FAILED
                    || alter_table_status == ALTER_TABLE_WAITING) {
                // Create rollup table
                OLAPStatus ret = OLAPStatus::OLAP_SUCCESS;
                switch (task_type) {
                case TTaskType::ROLLUP:
                    ret = _env->olap_engine()->create_rollup_table(alter_tablet_request);
                    break;
                case TTaskType::SCHEMA_CHANGE:
                    ret = _env->olap_engine()->schema_change(alter_tablet_request);
                    break;
                default:
                    // pass
                    break;
                }
                if (ret != OLAPStatus::OLAP_SUCCESS) {
                    status = DORIS_ERROR;
                    LOG(WARNING) << process_name << " failed. signature: " << signature << " status: " << status;
                }
            }
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
                alter_tablet_request.new_tablet_req.tablet_id,
                alter_tablet_request.new_tablet_req.tablet_schema.schema_hash,
                signature,
                &tablet_info);

        if (status != DORIS_SUCCESS) {
            OLAP_LOG_WARNING("%s success, but get new tablet info failed."
                             "tablet_id: %ld, schema_hash: %ld, signature: %ld.",
                             process_name.c_str(),
                             alter_tablet_request.new_tablet_req.tablet_id,
                             alter_tablet_request.new_tablet_req.tablet_schema.schema_hash,
                             signature);
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
        OLAP_LOG_WARNING("alter table request task type invalid. "
                         "signature: %ld", signature);
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
        if (push_req.push_type == TPushType::LOAD || push_req.push_type == TPushType::LOAD_DELETE) {
#ifndef BE_TEST
            Pusher pusher(worker_pool_this->_env->olap_engine(), push_req);
            status = pusher.init();
#else
            status = worker_pool_this->_pusher->init();
#endif

            if (status == DORIS_SUCCESS) {
                uint32_t retry_time = 0;
                while (retry_time < PUSH_MAX_RETRY) {
#ifndef BE_TEST
                    status = pusher.process(&tablet_infos);
#else
                    status = worker_pool_this->_pusher->process(&tablet_infos);
#endif
                    if (status == DORIS_PUSH_HAD_LOADED) {
                        OLAP_LOG_WARNING("transaction exists when realtime push, "
                                         "but unfinished, do not report to fe, signature: %ld",
                                         agent_task_req.signature);
                        break;  // not retry any more
                    }
                    // Internal error, need retry
                    if (status == DORIS_ERROR) {
                        OLAP_LOG_WARNING("push internal error, need retry.signature: %ld",
                                         agent_task_req.signature);
                        retry_time += 1;
                    } else {
                        break;
                    }
                }
            }
        } else if (push_req.push_type == TPushType::DELETE) {
            OLAPStatus delete_data_status =
                     worker_pool_this->_env->olap_engine()->delete_data(push_req, &tablet_infos);
            if (delete_data_status != OLAPStatus::OLAP_SUCCESS) {
                OLAP_LOG_WARNING("delete data failed. status: %d, signature: %ld",
                                 delete_data_status, agent_task_req.signature);
                status = DORIS_ERROR;
            }
        } else {
            status = DORIS_TASK_REQUEST_ERROR;
        }

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
        LOG(INFO)<< "get publish version task, signature:" << agent_task_req.signature;

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        vector<TTabletId> error_tablet_ids;
        uint32_t retry_time = 0;
        OLAPStatus res = OLAP_SUCCESS;
        while (retry_time < PUBLISH_VERSION_MAX_RETRY) {
            error_tablet_ids.clear();
            res = worker_pool_this->_env->olap_engine()->publish_version(
                publish_version_req, &error_tablet_ids);
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
            status_code = TStatusCode::RUNTIME_ERROR;
            OLAP_LOG_WARNING("publish version failed. signature: %ld", agent_task_req.signature);
            error_msgs.push_back("publish version failed");
            finish_task_request.__set_error_tablet_ids(error_tablet_ids);
        } else {
            LOG(INFO) << "publish_version success. signature:" << agent_task_req.signature;
        }

        task_status.__set_status_code(status_code);
        task_status.__set_error_msgs(error_msgs);

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

        OLAPStatus clear_status = worker_pool_this->_env->olap_engine()->
            clear_alter_task(clear_alter_task_req.tablet_id, clear_alter_task_req.schema_hash);
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
                  << ", transaction_id:" << clear_transaction_task_req.transaction_id;

        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        worker_pool_this->_env->olap_engine()->clear_transaction_task(
            clear_transaction_task_req.transaction_id, clear_transaction_task_req.partition_id);
        LOG(INFO) << "finish to clear transaction task. signature:" << agent_task_req.signature
                  << ", transaction_id:" << clear_transaction_task_req.transaction_id;

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
        string src_file_path;
        TBackend src_host;
        // Check local tablet exist or not
        OLAPTablePtr tablet =
                worker_pool_this->_env->olap_engine()->get_table(
                clone_req.tablet_id, clone_req.schema_hash);

        int64_t copy_size = 0;
        int64_t copy_time_ms = 0;
        if (tablet.get() != NULL) {
            LOG(INFO) << "clone tablet exist yet, begin to incremental clone. "
                      << "signature:" << agent_task_req.signature
                      << ", tablet_id:" << clone_req.tablet_id
                      << ", schema_hash:" << clone_req.schema_hash
                      << ", committed_version:" << clone_req.committed_version;

            // try to incremental clone
            vector<Version> missing_versions;
            string local_data_path = worker_pool_this->_env->olap_engine()->
                get_info_before_incremental_clone(tablet, clone_req.committed_version, &missing_versions);

            bool allow_incremental_clone = false;
            status = worker_pool_this->_clone_copy(clone_req,
                                                   agent_task_req.signature,
                                                   local_data_path,
                                                   &src_host,
                                                   &src_file_path,
                                                   &error_msgs,
                                                   &missing_versions,
                                                   &allow_incremental_clone,
                                                   &copy_size,
                                                   &copy_time_ms);
            if (status == DORIS_SUCCESS) {
                OLAPStatus olap_status = worker_pool_this->_env->olap_engine()->
                    finish_clone(tablet, local_data_path, clone_req.committed_version, allow_incremental_clone);
                if (olap_status != OLAP_SUCCESS) {
                    LOG(WARNING) << "failed to finish incremental clone. [table=" << tablet->full_name()
                                 << " res=" << olap_status << "]";
                    error_msgs.push_back("incremental clone error.");
                    status = DORIS_ERROR;
                }
            } else {
                // begin to full clone if incremental failed
                LOG(INFO) << "begin to full clone. [table=" << tablet->full_name();
                status = worker_pool_this->_clone_copy(clone_req,
                                                       agent_task_req.signature,
                                                       local_data_path,
                                                       &src_host,
                                                       &src_file_path,
                                                       &error_msgs,
                                                       NULL, NULL,
                                                       &copy_size,
                                                       &copy_time_ms);
                if (status == DORIS_SUCCESS) {
                    LOG(INFO) << "download successfully when full clone. [table=" << tablet->full_name()
                              << " src_host=" << src_host.host << " src_file_path=" << src_file_path
                              << " local_data_path=" << local_data_path << "]";

                    OLAPStatus olap_status = worker_pool_this->_env->olap_engine()->
                        finish_clone(tablet, local_data_path, clone_req.committed_version, false);

                    if (olap_status != OLAP_SUCCESS) {
                        LOG(WARNING) << "fail to finish full clone. [table=" << tablet->full_name()
                                     << " res=" << olap_status << "]";
                        error_msgs.push_back("full clone error.");
                        status = DORIS_ERROR;
                    }
                }
            }
        } else { // create a new tablet
            // Get local disk from olap
            string local_shard_root_path;
            OlapStore* store = nullptr;
            OLAPStatus olap_status = OLAP_ERR_OTHER_ERROR;
            if (clone_req.__isset.task_version && clone_req.task_version == 2) {
                // use path specified in clone request
                olap_status = worker_pool_this->_env->olap_engine()->obtain_shard_path_by_hash(
                        clone_req.dest_path_hash, &local_shard_root_path, &store);
            }

            // if failed to get path by hash, or path hash is not specified, get arbitrary one
            if (olap_status != OLAP_SUCCESS || clone_req.task_version == 1) {
                olap_status = worker_pool_this->_env->olap_engine()->obtain_shard_path(
                        clone_req.storage_medium, &local_shard_root_path, &store);
            }

            if (olap_status != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("clone get local root path failed. signature: %ld",
                                 agent_task_req.signature);
                error_msgs.push_back("clone get local root path failed.");
                status = DORIS_ERROR;
            }

            if (status == DORIS_SUCCESS) {
                stringstream tablet_dir_stream;
                tablet_dir_stream << local_shard_root_path
                                  << "/" << clone_req.tablet_id
                                  << "/" << clone_req.schema_hash;
                status = worker_pool_this->_clone_copy(clone_req,
                                                       agent_task_req.signature,
                                                       tablet_dir_stream.str(),
                                                       &src_host,
                                                       &src_file_path,
                                                       &error_msgs,
                                                       NULL, NULL,
                                                       &copy_size,
                                                       &copy_time_ms);
            }

            if (status == DORIS_SUCCESS) {
                LOG(INFO) << "clone copy done. src_host: " << src_host.host
                          << " src_file_path: " << src_file_path;
                // Load header
                OLAPStatus load_header_status =
                    worker_pool_this->_env->olap_engine()->load_header(
                        store,
                        local_shard_root_path,
                        clone_req.tablet_id,
                        clone_req.schema_hash);
                if (load_header_status != OLAP_SUCCESS) {
                    LOG(WARNING) << "load header failed. local_shard_root_path: '" << local_shard_root_path
                                 << "' schema_hash: " << clone_req.schema_hash << ". status: " << load_header_status
                                 << ". signature: " << agent_task_req.signature;
                    error_msgs.push_back("load header failed.");
                    status = DORIS_ERROR;
                }
            }

#ifndef BE_TEST
            // Clean useless dir, if failed, ignore it.
            if (status != DORIS_SUCCESS && status != DORIS_CREATE_TABLE_EXIST) {
                stringstream local_data_path_stream;
                local_data_path_stream << local_shard_root_path
                                       << "/" << clone_req.tablet_id;
                string local_data_path = local_data_path_stream.str();
                LOG(INFO) << "clone failed. want to delete local dir: " << local_data_path
                          << ". signature: " << agent_task_req.signature;
                try {
                    boost::filesystem::path local_path(local_data_path);
                    if (boost::filesystem::exists(local_path)) {
                        boost::filesystem::remove_all(local_path);
                    }
                } catch (boost::filesystem::filesystem_error e) {
                    // Ignore the error, OLAP will delete it
                    OLAP_LOG_WARNING("clone delete useless dir failed. "
                                     "error: %s, local dir: %s, signature: %ld",
                                     e.what(), local_data_path.c_str(),
                                     agent_task_req.signature);
                }
            }
#endif
        }

        // Get clone tablet info
        vector<TTabletInfo> tablet_infos;
        if (status == DORIS_SUCCESS || status == DORIS_CREATE_TABLE_EXIST) {
            TTabletInfo tablet_info;
            AgentStatus get_tablet_info_status = worker_pool_this->_get_tablet_info(
                    clone_req.tablet_id,
                    clone_req.schema_hash,
                    agent_task_req.signature,
                    &tablet_info);
            if (get_tablet_info_status != DORIS_SUCCESS) {
                OLAP_LOG_WARNING("clone success, but get tablet info failed."
                                 "tablet id: %ld, schema hash: %ld, signature: %ld",
                                 clone_req.tablet_id, clone_req.schema_hash,
                                 agent_task_req.signature);
                error_msgs.push_back("clone success, but get tablet info failed.");
                status = DORIS_ERROR;
            } else if (
                (clone_req.__isset.committed_version
                        && clone_req.__isset.committed_version_hash)
                        && (tablet_info.version < clone_req.committed_version ||
                            (tablet_info.version == clone_req.committed_version
                            && tablet_info.version_hash != clone_req.committed_version_hash))) {

                // we need to check if this cloned table's version is what we expect.
                // if not, maybe this is a stale remaining table which is waiting for drop.
                // we drop it.
                LOG(INFO) << "begin to drop the stale table. tablet_id:" << clone_req.tablet_id
                          << ", schema_hash:" << clone_req.schema_hash
                          << ", signature:" << agent_task_req.signature
                          << ", version:" << tablet_info.version
                          << ", version_hash:" << tablet_info.version_hash
                          << ", expected_version: " << clone_req.committed_version
                          << ", version_hash:" << clone_req.committed_version_hash;
                TDropTabletReq drop_req;
                drop_req.tablet_id = clone_req.tablet_id;
                drop_req.schema_hash = clone_req.schema_hash;
                AgentStatus drop_status = worker_pool_this->_drop_table(drop_req);
                if (drop_status != DORIS_SUCCESS) {
                    // just log
                    OLAP_LOG_WARNING(
                        "drop stale cloned table failed! tabelt id: %ld", clone_req.tablet_id);
                }

                status = DORIS_ERROR;
            } else {
                LOG(INFO) << "clone get tablet info success. tablet_id:" << clone_req.tablet_id
                          << ", schema_hash:" << clone_req.schema_hash
                          << ", signature:" << agent_task_req.signature
                          << ", version:" << tablet_info.version
                          << ", version_hash:" << tablet_info.version_hash;
                tablet_infos.push_back(tablet_info);
            }
        }

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

        finish_task_request.__set_copy_size(copy_size);
        finish_task_request.__set_copy_time_ms(copy_time_ms);

        worker_pool_this->_finish_task(finish_task_request);
        worker_pool_this->_remove_task_info(agent_task_req.task_type, agent_task_req.signature, "");
#ifndef BE_TEST
    }
#endif

    return (void*)0;
}

AgentStatus TaskWorkerPool::_clone_copy(
        const TCloneReq& clone_req,
        int64_t signature,
        const string& local_data_path,
        TBackend* src_host,
        string* src_file_path,
        vector<string>* error_msgs,
        const vector<Version>* missing_versions,
        bool* allow_incremental_clone,
        int64_t* copy_size,
        int64_t* copy_time_ms) {
    AgentStatus status = DORIS_SUCCESS;

    std::string token = _master_info.token;
    for (auto& src_backend : clone_req.src_backends) {
        stringstream http_host_stream;
        http_host_stream << "http://" << src_backend.host << ":" << src_backend.http_port;
        string http_host = http_host_stream.str();
        // Make snapshot in remote olap engine
        *src_host = src_backend;
#ifndef BE_TEST
        AgentServerClient agent_client(*src_host);
#endif
        TAgentResult make_snapshot_result;
        status = DORIS_SUCCESS;

        LOG(INFO) << "pre make snapshot. backend_ip: " << src_host->host;
        TSnapshotRequest snapshot_request;
        snapshot_request.__set_tablet_id(clone_req.tablet_id);
        snapshot_request.__set_schema_hash(clone_req.schema_hash);
        if (missing_versions != NULL) {
            // TODO: missing version composed of singleton delta.
            // if not, this place should be rewrote.
            vector<int64_t> snapshot_versions;
            for (Version version : *missing_versions) {
               snapshot_versions.push_back(version.first); 
            } 
            snapshot_request.__set_missing_version(snapshot_versions);
        }
#ifndef BE_TEST
        agent_client.make_snapshot(
                snapshot_request,
                &make_snapshot_result);
#else
        _agent_client->make_snapshot(
                snapshot_request,
                &make_snapshot_result);
#endif

        if (make_snapshot_result.__isset.allow_incremental_clone) {
            // During upgrading, some BE nodes still be installed an old previous old.
            // which incremental clone is not ready in those nodes.
            // should add a symbol to indicate it.
            *allow_incremental_clone = make_snapshot_result.allow_incremental_clone;
        }
        if (make_snapshot_result.status.status_code == TStatusCode::OK) {
            if (make_snapshot_result.__isset.snapshot_path) {
                *src_file_path = make_snapshot_result.snapshot_path;
                if (src_file_path->at(src_file_path->length() - 1) != '/') {
                    src_file_path->append("/");
                }
                LOG(INFO) << "make snapshot success. backend_ip: " << src_host->host << ". src_file_path: "
                          << *src_file_path << ". signature: " << signature;
            } else {
                OLAP_LOG_WARNING("clone make snapshot success, "
                                 "but get src file path failed. signature: %ld",
                                 signature);
                status = DORIS_ERROR;
                continue;
            }
        } else {
            LOG(WARNING) << "make snapshot failed. tablet_id: " << clone_req.tablet_id
                         << ". schema_hash: " << clone_req.schema_hash
                         << ". backend_ip: " << src_host->host
                         << ". backend_port: " << src_host->be_port << ". signature: " << signature;
            error_msgs->push_back("make snapshot failed. backend_ip: " + src_host->host);
            status = DORIS_ERROR;
            continue;
        }

        // Get remote and local full path
        stringstream src_file_full_path_stream;
        stringstream local_file_full_path_stream;

        if (status == DORIS_SUCCESS) {
            src_file_full_path_stream << *src_file_path
                                      << "/" << clone_req.tablet_id
                                      << "/" << clone_req.schema_hash << "/";
            local_file_full_path_stream << local_data_path  << "/";
        }
        string src_file_full_path = src_file_full_path_stream.str();
        string local_file_full_path = local_file_full_path_stream.str();

        // Check local path exist, if exist, remove it, then create the dir
        if (status == DORIS_SUCCESS) {
            boost::filesystem::path local_file_full_dir(local_file_full_path);
            if (boost::filesystem::exists(local_file_full_dir)) {
                boost::filesystem::remove_all(local_file_full_dir);
            }
            boost::filesystem::create_directories(local_file_full_dir);
        }

        // Get remove dir file list
        HttpClient client;
        std::string remote_file_path = http_host + HTTP_REQUEST_PREFIX
            + HTTP_REQUEST_TOKEN_PARAM + token
            + HTTP_REQUEST_FILE_PARAM + src_file_full_path;

        string file_list_str;
        auto list_files_cb = [&remote_file_path, &file_list_str] (HttpClient* client) {
            RETURN_IF_ERROR(client->init(remote_file_path));
            client->set_timeout_ms(LIST_REMOTE_FILE_TIMEOUT * 1000);
            RETURN_IF_ERROR(client->execute(&file_list_str));
            return Status::OK;
        };

        Status download_status = HttpClient::execute_with_retry(
            DOWNLOAD_FILE_MAX_RETRY, 1, list_files_cb);
        if (!download_status.ok()) {
            OLAP_LOG_WARNING("clone get remote file list failed over max time. backend_ip: %s, "
                             "src_file_path: %s, signature: %ld",
                             src_host->host.c_str(),
                             remote_file_path.c_str(),
                             signature);
            status = DORIS_ERROR;
        }

        vector<string> file_name_list;
        if (status == DORIS_SUCCESS) {
            size_t start_position = 0;
            size_t end_position = file_list_str.find("\n");

            // Split file name from file_list_str
            while (end_position != string::npos) {
                string file_name = file_list_str.substr(
                        start_position, end_position - start_position);
                // If the header file is not exist, the table could't loaded by olap engine.
                // Avoid of data is not complete, we copy the header file at last.
                // The header file's name is end of .hdr.
                if (file_name.size() > 4 && file_name.substr(file_name.size() - 4, 4) == ".hdr") {
                    file_name_list.push_back(file_name);
                } else {
                    file_name_list.insert(file_name_list.begin(), file_name);
                }

                start_position = end_position + 1;
                end_position = file_list_str.find("\n", start_position);
            }
            if (start_position != file_list_str.size()) {
                string file_name = file_list_str.substr(
                        start_position, file_list_str.size() - start_position);
                if (file_name.size() > 4 && file_name.substr(file_name.size() - 4, 4) == ".hdr") {
                    file_name_list.push_back(file_name);
                } else {
                    file_name_list.insert(file_name_list.begin(), file_name);
                }
            }
        }

        // Get copy from remote
        uint64_t total_file_size = 0;
        MonotonicStopWatch watch;
        watch.start();
        for (auto& file_name : file_name_list) {
            remote_file_path = http_host + HTTP_REQUEST_PREFIX
                + HTTP_REQUEST_TOKEN_PARAM + token
                + HTTP_REQUEST_FILE_PARAM + src_file_full_path + file_name;

            // get file length
            uint64_t file_size = 0;
            auto get_file_size_cb = [&remote_file_path, &file_size] (HttpClient* client) {
                RETURN_IF_ERROR(client->init(remote_file_path));
                client->set_timeout_ms(GET_LENGTH_TIMEOUT * 1000);
                RETURN_IF_ERROR(client->head());
                file_size = client->get_content_length();
                return Status::OK;
            };
            download_status = HttpClient::execute_with_retry(
                DOWNLOAD_FILE_MAX_RETRY, 1, get_file_size_cb);
            if (!download_status.ok()) {
                LOG(WARNING) << "clone copy get file length failed over max time. remote_path="
                    << remote_file_path
                    << ", signature=" << signature;
                status = DORIS_ERROR;
                break;
            }

            total_file_size += file_size;
            uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
            if (estimate_timeout < config::download_low_speed_time) {
                estimate_timeout = config::download_low_speed_time;
            }

            std::string local_file_path = local_file_full_path + file_name;

            auto download_cb = [&remote_file_path,
                                estimate_timeout,
                                &local_file_path,
                                file_size] (HttpClient* client) {
                RETURN_IF_ERROR(client->init(remote_file_path));
                client->set_timeout_ms(estimate_timeout * 1000);
                RETURN_IF_ERROR(client->download(local_file_path));

                // Check file length
                uint64_t local_file_size = boost::filesystem::file_size(local_file_path);
                if (local_file_size != file_size) {
                    LOG(WARNING) << "download file length error"
                        << ", remote_path=" << remote_file_path
                        << ", file_size=" << file_size
                        << ", local_file_size=" << local_file_size;
                    return Status("downloaded file size is not equal");
                }
                chmod(local_file_path.c_str(), S_IRUSR | S_IWUSR);
                return Status::OK;
            };
            download_status = HttpClient::execute_with_retry(
                DOWNLOAD_FILE_MAX_RETRY, 1, download_cb);
            if (!download_status.ok()) {
                LOG(WARNING) << "download file failed over max retry."
                    << ", remote_path=" << remote_file_path
                    << ", signature=" << signature
                    << ", errormsg=" << download_status.get_error_msg();
                status = DORIS_ERROR;
                break;
            }
        } // Clone files from remote backend

        uint64_t total_time_ms = watch.elapsed_time() / 1000 / 1000;
        total_time_ms = total_time_ms > 0 ? total_time_ms : 0;
        double copy_rate = 0.0;
        if (total_time_ms > 0) {
            copy_rate = total_file_size / ((double) total_time_ms) / 1000;
        }
        *copy_size = (int64_t) total_file_size;
        *copy_time_ms = (int64_t) total_time_ms;
        LOG(INFO) << "succeed to copy tablet " << signature
                  << ", total file size: " << total_file_size << " B"
                  << ", cost: " << total_time_ms << " ms"
                  << ", rate: " << copy_rate << " B/s";

        // Release snapshot, if failed, ignore it. OLAP engine will drop useless snapshot
        TAgentResult release_snapshot_result;
#ifndef BE_TEST
        agent_client.release_snapshot(
                make_snapshot_result.snapshot_path,
                &release_snapshot_result);
#else
        _agent_client->release_snapshot(
                make_snapshot_result.snapshot_path,
                &release_snapshot_result);
#endif
        if (release_snapshot_result.status.status_code != TStatusCode::OK) {
            LOG(WARNING) << "release snapshot failed. src_file_path: " << *src_file_path
                         << ". signature: " << signature;
        }

        if (status == DORIS_SUCCESS) {
            break;
        }
    } // clone copy from one backend
    return status;
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

        OLAPStatus res = OLAPStatus::OLAP_SUCCESS;
        res = worker_pool_this->_env->olap_engine()->storage_medium_migrate(
            storage_medium_migrate_req.tablet_id,
            storage_medium_migrate_req.schema_hash,
            storage_medium_migrate_req.storage_medium);
        if (res != OLAPStatus::OLAP_SUCCESS) {
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

void* TaskWorkerPool::_cancel_delete_data_worker_thread_callback(void* arg_this) {
    TaskWorkerPool* worker_pool_this = (TaskWorkerPool*)arg_this;

#ifndef BE_TEST
    while (true) {
#endif
        TAgentTaskRequest agent_task_req;
        TCancelDeleteDataReq cancel_delete_data_req;
        {
            lock_guard<Mutex> worker_thread_lock(worker_pool_this->_worker_thread_lock);
            while (worker_pool_this->_tasks.empty()) {
                worker_pool_this->_worker_thread_condition_lock.wait();
            }

            agent_task_req = worker_pool_this->_tasks.front();
            cancel_delete_data_req = agent_task_req.cancel_delete_data_req;
            worker_pool_this->_tasks.pop_front();
        }

        LOG(INFO) << "get cancel delete data task. signature:" << agent_task_req.signature;
        TStatusCode::type status_code = TStatusCode::OK;
        vector<string> error_msgs;
        TStatus task_status;

        OLAPStatus cancel_delete_data_status = OLAPStatus::OLAP_SUCCESS;
        cancel_delete_data_status =
                worker_pool_this->_env->olap_engine()->cancel_delete(cancel_delete_data_req);
        if (cancel_delete_data_status != OLAPStatus::OLAP_SUCCESS) {
            OLAP_LOG_WARNING("cancel delete data failed. statusta: %d, signature: %ld",
                             cancel_delete_data_status, agent_task_req.signature);
            status_code = TStatusCode::RUNTIME_ERROR;
        } else {
            LOG(INFO) << "cancel delete data success. status:" << cancel_delete_data_status
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
        worker_pool_this->_remove_task_info(
                agent_task_req.task_type, agent_task_req.signature, "");
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

        OLAPStatus res = OLAPStatus::OLAP_SUCCESS;
        uint32_t checksum = 0;
        res = worker_pool_this->_env->olap_engine()->compute_checksum(
                check_consistency_req.tablet_id,
                check_consistency_req.schema_hash,
                check_consistency_req.version,
                check_consistency_req.version_hash,
                &checksum);
        if (res != OLAPStatus::OLAP_SUCCESS) {
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
        vector<RootPathInfo> root_paths_info;
        worker_pool_this->_env->olap_engine()->get_all_root_path_info(&root_paths_info);

        map<string, TDisk> disks;
        for (auto& root_path_info : root_paths_info) {
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
        OLAPEngine::get_instance()->wait_for_report_notify(
                config::report_disk_state_interval_seconds, false);
    }
#endif

    return (void*)0;
}

void* TaskWorkerPool::_report_olap_table_worker_thread_callback(void* arg_this) {
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
                worker_pool_this->_env->olap_engine()->report_all_tablets_info(&request.tablets);
        if (report_all_tablets_info_status != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("report get all tablets info failed. status: %d",
                             report_all_tablets_info_status);
#ifndef BE_TEST
            // wait for notifying until timeout
            OLAPEngine::get_instance()->wait_for_report_notify(
                    config::report_olap_table_interval_seconds, true);
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
        OLAPEngine::get_instance()->wait_for_report_notify(
                config::report_olap_table_interval_seconds, true);
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
        OLAPStatus make_snapshot_status = worker_pool_this->_env->olap_engine()->make_snapshot(
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
                // snapshot_path like: data/snapshot/20180417205230.1
                // we need to add subdir: tablet_id/schema_hash/
                std::stringstream ss;
                ss << snapshot_path << "/" << snapshot_request.tablet_id
                    << "/" << snapshot_request.schema_hash << "/";
                Status st = FileUtils::scan_dir(ss.str(), &snapshot_files); 
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
                worker_pool_this->_env->olap_engine()->release_snapshot(snapshot_path);
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

AlterTableStatus TaskWorkerPool::_show_alter_table_status(
        TTabletId tablet_id,
        TSchemaHash schema_hash) {
    AlterTableStatus alter_table_status =
            _env->olap_engine()->show_alter_table_status(tablet_id, schema_hash);
    return alter_table_status;
}

AgentStatus TaskWorkerPool::_drop_table(const TDropTabletReq& req) {
    AgentStatus status = DORIS_SUCCESS;
    OLAPStatus drop_status = _env->olap_engine()->drop_table(req.tablet_id, req.schema_hash);
    if (drop_status != OLAP_SUCCESS && drop_status != OLAP_ERR_TABLE_NOT_FOUND) {
        status = DORIS_ERROR;
    }
    return status;
}

AgentStatus TaskWorkerPool::_get_tablet_info(
        const TTabletId tablet_id,
        const TSchemaHash schema_hash,
        int64_t signature,
        TTabletInfo* tablet_info) {
    AgentStatus status = DORIS_SUCCESS;

    tablet_info->__set_tablet_id(tablet_id);
    tablet_info->__set_schema_hash(schema_hash);
    OLAPStatus olap_status = _env->olap_engine()->report_tablet_info(tablet_info);
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

    OLAPTablePtr tablet = _env->olap_engine()->get_table(
                tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        LOG(INFO) << "failed to get tablet. tablet_id:" << tablet_id
                  << ", schema hash:" << schema_hash;
        error_msgs->push_back("failed to get tablet");
        return DORIS_TASK_REQUEST_ERROR;
    }

    std::string dest_tablet_dir = tablet->construct_dir_path();
    std::string store_path = tablet->store()->path();

    SnapshotLoader loader(_env, job_id, tablet_id);
    Status status = loader.move(src, dest_tablet_dir, store_path, overwrite);

    if (!status.ok()) {
        OLAP_LOG_WARNING("move failed. job id: %ld, msg: %s",
            job_id, status.get_error_msg().c_str());
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
        OLAPStatus status = worker_pool_this->_env->olap_engine()->recover_tablet_until_specfic_version(recover_tablet_req);
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
