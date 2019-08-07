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
#include <string>
#include "boost/filesystem.hpp"
#include "boost/lexical_cast.hpp"
#include "thrift/concurrency/ThreadManager.h"
#include "thrift/concurrency/PosixThreadFactory.h"
#include "thrift/server/TThreadPoolServer.h"
#include "thrift/server/TThreadedServer.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TTransportUtils.h"
#include "util/thrift_server.h"
#include "agent/status.h"
#include "agent/task_worker_pool.h"
#include "agent/user_resource_listener.h"
#include "common/status.h"
#include "common/logging.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/Status_types.h"
#include "olap/utils.h"
#include "olap/snapshot_manager.h"
#include "runtime/exec_env.h"
#include "runtime/etl_job_mgr.h"
#include "util/debug_util.h"

using apache::thrift::transport::TProcessor;
using std::deque;
using std::list;
using std::map;
using std::nothrow;
using std::set;
using std::string;
using std::to_string;
using std::vector;

namespace doris {

AgentServer::AgentServer(ExecEnv* exec_env,
                         const TMasterInfo& master_info) :
        _exec_env(exec_env),
        _master_info(master_info),
        _topic_subscriber(new TopicSubscriber()) {
    
    for (auto& path : exec_env->store_paths()) {
        try {
            string dpp_download_path_str = path.path + DPP_PREFIX;
            boost::filesystem::path dpp_download_path(dpp_download_path_str);
            if (boost::filesystem::exists(dpp_download_path)) {
                boost::filesystem::remove_all(dpp_download_path);
            }
        } catch (...) {
            LOG(WARNING) << "boost exception when remove dpp download path. path="
                         << path.path;
        }
    }

    // init task worker pool
    _create_tablet_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::CREATE_TABLE,
            _exec_env,
            master_info);
    _drop_tablet_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::DROP_TABLE,
            _exec_env,
            master_info);
    _push_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::PUSH,
            _exec_env,
            master_info);
    _publish_version_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::PUBLISH_VERSION,
            _exec_env,
            master_info);
    _clear_alter_task_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::CLEAR_ALTER_TASK,
            _exec_env,
            master_info);
    _clear_transaction_task_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::CLEAR_TRANSACTION_TASK,
            exec_env,
            master_info);
    _delete_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::DELETE,
            _exec_env,
            master_info);
    _alter_tablet_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::ALTER_TABLE,
            _exec_env,
            master_info);
    _clone_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::CLONE,
            _exec_env,
            master_info);
    _storage_medium_migrate_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::STORAGE_MEDIUM_MIGRATE,
            _exec_env,
            master_info);
    _check_consistency_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::CHECK_CONSISTENCY,
            _exec_env,
            master_info);
    _report_task_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::REPORT_TASK,
            _exec_env,
            master_info);
    _report_disk_state_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::REPORT_DISK_STATE,
            _exec_env,
            master_info);
    _report_tablet_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::REPORT_OLAP_TABLE,
            _exec_env,
            master_info);
    _upload_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::UPLOAD,
            _exec_env,
            master_info);
    _download_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::DOWNLOAD,
            _exec_env,
            master_info);
    _make_snapshot_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::MAKE_SNAPSHOT,
            _exec_env,
            master_info);
    _release_snapshot_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::RELEASE_SNAPSHOT,
            _exec_env,
            master_info);
    _move_dir_workers= new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::MOVE,
            _exec_env,
            master_info);
    _recover_tablet_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::RECOVER_TABLET,
            _exec_env,
            master_info);
    _update_tablet_meta_info_workers = new TaskWorkerPool(
            TaskWorkerPool::TaskWorkerType::UPDATE_TABLET_META_INFO,
            _exec_env,
            master_info);
#ifndef BE_TEST
    _create_tablet_workers->start();
    _drop_tablet_workers->start();
    _push_workers->start();
    _publish_version_workers->start();
    _clear_alter_task_workers->start();
    _clear_transaction_task_workers->start();
    _delete_workers->start();
    _alter_tablet_workers->start();
    _clone_workers->start();
    _storage_medium_migrate_workers->start();
    _check_consistency_workers->start();
    _report_task_workers->start();
    _report_disk_state_workers->start();
    _report_tablet_workers->start();
    _upload_workers->start();
    _download_workers->start();
    _make_snapshot_workers->start();
    _release_snapshot_workers->start();
    _move_dir_workers->start();
    _recover_tablet_workers->start();
    _update_tablet_meta_info_workers->start();
    // Add subscriber here and register listeners
    TopicListener* user_resource_listener = new UserResourceListener(exec_env, master_info);
    LOG(INFO) << "Register user resource listener";
    _topic_subscriber->register_listener(doris::TTopicType::type::RESOURCE, user_resource_listener);
#endif
}

AgentServer::~AgentServer() {
    if (_create_tablet_workers != NULL) {
        delete _create_tablet_workers;
    }
    if (_drop_tablet_workers != NULL) {
        delete _drop_tablet_workers;
    }
    if (_push_workers != NULL) {
        delete _push_workers;
    }
    if (_publish_version_workers != NULL) {
        delete _publish_version_workers;
    }
    if (_clear_alter_task_workers != NULL) {
        delete _clear_alter_task_workers;
    }
    if (_clear_transaction_task_workers != NULL) {
        delete _clear_transaction_task_workers;
    }
    if (_delete_workers != NULL) {
        delete _delete_workers;
    }
    if (_alter_tablet_workers != NULL) {
        delete _alter_tablet_workers;
    }
    if (_clone_workers != NULL) {
        delete _clone_workers;
    }
    if (_storage_medium_migrate_workers != NULL) {
        delete _storage_medium_migrate_workers;
    }
    if (_check_consistency_workers != NULL) {
        delete _check_consistency_workers;
    }
    if (_report_task_workers != NULL) {
        delete _report_task_workers;
    }
    if (_report_disk_state_workers != NULL) {
        delete _report_disk_state_workers;
    }
    if (_report_tablet_workers != NULL) {
        delete _report_tablet_workers;
    }
    if (_upload_workers != NULL) {
        delete _upload_workers;
    }
    if (_download_workers != NULL) {
        delete _download_workers;
    }
    if (_make_snapshot_workers != NULL) {
        delete _make_snapshot_workers;
    }
    if (_move_dir_workers!= NULL) {
        delete _move_dir_workers;
    }
    if (_recover_tablet_workers != NULL) {
        delete _recover_tablet_workers;
    }

    if (_update_tablet_meta_info_workers != NULL) {
        delete _update_tablet_meta_info_workers;
    }
    if (_release_snapshot_workers != NULL) {
        delete _release_snapshot_workers;
    }
    if (_topic_subscriber !=NULL) {
        delete _topic_subscriber;
    }
}

void AgentServer::submit_tasks(
        TAgentResult& return_value,
        const vector<TAgentTaskRequest>& tasks) {

    // Set result to dm
    vector<string> error_msgs;
    TStatusCode::type status_code = TStatusCode::OK;

    // TODO check require master same to heartbeat master
    if (_master_info.network_address.hostname == ""
            || _master_info.network_address.port == 0) {
        error_msgs.push_back("Not get master heartbeat yet.");
        return_value.status.__set_error_msgs(error_msgs);
        return_value.status.__set_status_code(TStatusCode::CANCELLED);
        return;
    }

    for (auto task : tasks) {
        TTaskType::type task_type = task.task_type;
        int64_t signature = task.signature;

        switch (task_type) {
        case TTaskType::CREATE:
            if (task.__isset.create_tablet_req) {
               _create_tablet_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::DROP:
            if (task.__isset.drop_tablet_req) {
                _drop_tablet_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::REALTIME_PUSH:
        case TTaskType::PUSH:
            if (task.__isset.push_req) {
                if (task.push_req.push_type == TPushType::LOAD
                        || task.push_req.push_type == TPushType::LOAD_DELETE) {
                    _push_workers->submit_task(task);
                } else if (task.push_req.push_type == TPushType::DELETE) {
                    _delete_workers->submit_task(task);
                } else {
                    status_code = TStatusCode::ANALYSIS_ERROR;
                }
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::PUBLISH_VERSION:
            if (task.__isset.publish_version_req) {
                _publish_version_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::CLEAR_ALTER_TASK:
            if (task.__isset.clear_alter_task_req) {
                _clear_alter_task_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::CLEAR_TRANSACTION_TASK:
            if (task.__isset.clear_transaction_task_req) {
                _clear_transaction_task_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::ROLLUP:
        case TTaskType::SCHEMA_CHANGE:
        case TTaskType::ALTER_TASK:
            if (task.__isset.alter_tablet_req) {
                _alter_tablet_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::CLONE:
            if (task.__isset.clone_req) {
                _clone_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::STORAGE_MEDIUM_MIGRATE:
            if (task.__isset.storage_medium_migrate_req) {
                _storage_medium_migrate_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::CHECK_CONSISTENCY:
            if (task.__isset.check_consistency_req) {
                _check_consistency_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::UPLOAD:
            if (task.__isset.upload_req) {
                _upload_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::DOWNLOAD:
            if (task.__isset.download_req) {
                _download_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::MAKE_SNAPSHOT:
            if (task.__isset.snapshot_req) {
                _make_snapshot_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::RELEASE_SNAPSHOT:
            if (task.__isset.release_snapshot_req) {
                _release_snapshot_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::MOVE:
            if (task.__isset.move_dir_req) {
                _move_dir_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::RECOVER_TABLET:
            if (task.__isset.recover_tablet_req) {
                _recover_tablet_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        case TTaskType::UPDATE_TABLET_META_INFO:
            if (task.__isset.update_tablet_meta_info_req) {
                _update_tablet_meta_info_workers->submit_task(task);
            } else {
                status_code = TStatusCode::ANALYSIS_ERROR;
            }
            break;
        default:
            status_code = TStatusCode::ANALYSIS_ERROR;
            break;
        }

        if (status_code == TStatusCode::ANALYSIS_ERROR) {
            OLAP_LOG_WARNING("task anaysis_error, signature: %ld", signature);
            error_msgs.push_back("the task signature is:" + to_string(signature) + " has wrong request.");
        }
    }

    return_value.status.__set_error_msgs(error_msgs);
    return_value.status.__set_status_code(status_code);
}

void AgentServer::make_snapshot(TAgentResult& return_value,
        const TSnapshotRequest& snapshot_request) {
    TStatus status;
    vector<string> error_msgs;
    TStatusCode::type status_code = TStatusCode::OK;
    return_value.__set_snapshot_version(PREFERRED_SNAPSHOT_VERSION);
    string snapshot_path;
    OLAPStatus make_snapshot_status =
            SnapshotManager::instance()->make_snapshot(snapshot_request, &snapshot_path);
    if (make_snapshot_status != OLAP_SUCCESS) {
        status_code = TStatusCode::RUNTIME_ERROR;
        OLAP_LOG_WARNING("make_snapshot failed. tablet_id: %ld, schema_hash: %ld, status: %d",
                         snapshot_request.tablet_id, snapshot_request.schema_hash,
                         make_snapshot_status);
        error_msgs.push_back("make_snapshot failed. status: " +
                             boost::lexical_cast<string>(make_snapshot_status));
    } else {
        LOG(INFO) << "make_snapshot success. tablet_id: " << snapshot_request.tablet_id
                  << " schema_hash: " << snapshot_request.schema_hash << " snapshot_path: " << snapshot_path;
        return_value.__set_snapshot_path(snapshot_path);
    }

    status.__set_error_msgs(error_msgs);
    status.__set_status_code(status_code);
    return_value.__set_status(status);
    if (snapshot_request.__isset.allow_incremental_clone) {
        return_value.__set_allow_incremental_clone(snapshot_request.allow_incremental_clone);
    }
}

void AgentServer::release_snapshot(TAgentResult& return_value, const std::string& snapshot_path) {
    vector<string> error_msgs;
    TStatusCode::type status_code = TStatusCode::OK;

    OLAPStatus release_snapshot_status =
            SnapshotManager::instance()->release_snapshot(snapshot_path);
    if (release_snapshot_status != OLAP_SUCCESS) {
        status_code = TStatusCode::RUNTIME_ERROR;
        LOG(WARNING) << "release_snapshot failed. snapshot_path: " << snapshot_path << ", status: " << release_snapshot_status;
        error_msgs.push_back("release_snapshot failed. status: " +
                             boost::lexical_cast<string>(release_snapshot_status));
    } else {
        LOG(INFO) << "release_snapshot success. snapshot_path: " << snapshot_path << ", status: " << release_snapshot_status;
    }

    return_value.status.__set_error_msgs(error_msgs);
    return_value.status.__set_status_code(status_code);
}

void AgentServer::publish_cluster_state(TAgentResult& _return, const TAgentPublishRequest& request) {
    vector<string> error_msgs;
    _topic_subscriber->handle_updates(request);
    _return.status.__set_status_code(TStatusCode::OK);
}

void AgentServer::submit_etl_task(TAgentResult& return_value,
                                 const TMiniLoadEtlTaskRequest& request) {
    Status status = _exec_env->etl_job_mgr()->start_job(request);
    if (status.ok()) {
        VLOG_RPC << "start etl task successfull id="
            << request.params.params.fragment_instance_id;
    } else {
        VLOG_RPC << "start etl task failed id="
            << request.params.params.fragment_instance_id
            << " and err_msg=" << status.get_error_msg();
    }
    status.to_thrift(&return_value.status);
}

void AgentServer::get_etl_status(TMiniLoadEtlStatusResult& return_value,
                                 const TMiniLoadEtlStatusRequest& request) {
    Status status = _exec_env->etl_job_mgr()->get_job_state(request.mini_load_id, &return_value);
    if (!status.ok()) {
        LOG(WARNING) << "get job state failed. [id=" << request.mini_load_id << "]";
    } else {
        VLOG_RPC << "get job state successful. [id=" << request.mini_load_id << ",status="
            << return_value.status.status_code << ",etl_state=" << return_value.etl_state
            << ",files=";
        for (auto& item : return_value.file_map) {
            VLOG_RPC << item.first << ":" << item.second << ";";
        }
        VLOG_RPC << "]";
    }
}

void AgentServer::delete_etl_files(TAgentResult& result,
                                   const TDeleteEtlFilesRequest& request) {
    Status status = _exec_env->etl_job_mgr()->erase_job(request);
    if (!status.ok()) {
        LOG(WARNING) << "delete etl files failed. because " << status.get_error_msg()
            << " with request " << request;
    } else {
        VLOG_RPC << "delete etl files successful with param " << request;
    }
    status.to_thrift(&result.status);
}

}  // namesapce doris
