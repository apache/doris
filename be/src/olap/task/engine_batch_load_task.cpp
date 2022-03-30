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

#include "olap/task/engine_batch_load_task.h"

#include <pthread.h>

#include <cstdio>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "agent/cgroups_mgr.h"
#include "boost/lexical_cast.hpp"
#include "gen_cpp/AgentService_types.h"
#include "http/http_client.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/push_handler.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"

using apache::thrift::ThriftDebugString;
using std::list;
using std::string;
using std::vector;

namespace doris {

EngineBatchLoadTask::EngineBatchLoadTask(TPushReq& push_req, std::vector<TTabletInfo>* tablet_infos,
                                         int64_t signature, Status* res_status)
        : _push_req(push_req),
          _tablet_infos(tablet_infos),
          _signature(signature),
          _res_status(res_status) {
    _download_status = Status::OK();
    _mem_tracker = MemTracker::create_tracker(
            -1, fmt::format("{}: {}", _push_req.push_type, std::to_string(_push_req.tablet_id)),
            StorageEngine::instance()->batch_load_mem_tracker(), MemTrackerLevel::TASK);
}

EngineBatchLoadTask::~EngineBatchLoadTask() {}

OLAPStatus EngineBatchLoadTask::execute() {
    Status status = Status::OK();
    if (_push_req.push_type == TPushType::LOAD || _push_req.push_type == TPushType::LOAD_DELETE ||
        _push_req.push_type == TPushType::LOAD_V2) {
        status = _init();
        if (status.ok()) {
            uint32_t retry_time = 0;
            while (retry_time < PUSH_MAX_RETRY) {
                status = _process();
                // Internal error, need retry
                if (!status.ok()) {
                    OLAP_LOG_WARNING("push internal error, need retry.signature: %ld", _signature);
                    retry_time += 1;
                } else {
                    break;
                }
            }
        }
    } else if (_push_req.push_type == TPushType::DELETE) {
        OLAPStatus delete_data_status = _delete_data(_push_req, _tablet_infos);
        if (delete_data_status != OLAPStatus::OLAP_SUCCESS) {
            OLAP_LOG_WARNING("delete data failed. status: %d, signature: %ld", delete_data_status,
                             _signature);
            status = Status::InternalError("Delete data failed");
        }
    } else {
        status = Status::InvalidArgument("Not support task type");
    }
    *_res_status = status;
    return OLAP_SUCCESS;
}

Status EngineBatchLoadTask::_init() {
    Status status = Status::OK();

    if (_is_init) {
        VLOG_NOTICE << "has been inited";
        return status;
    }

    // Check replica exist
    TabletSharedPtr tablet;
    tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_push_req.tablet_id,
                                                                     _push_req.schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "get tables failed. "
                     << "tablet_id: " << _push_req.tablet_id
                     << ", schema_hash: " << _push_req.schema_hash;
        return Status::InvalidArgument(
                fmt::format("Could not find tablet {}", _push_req.tablet_id));
    }

    // check disk capacity
    if (_push_req.push_type == TPushType::LOAD || _push_req.push_type == TPushType::LOAD_V2) {
        if (tablet->data_dir()->reach_capacity_limit(_push_req.__isset.http_file_size)) {
            return Status::IOError("Disk does not have enough capacity");
        }
    }

    // Empty remote_path
    if (!_push_req.__isset.http_file_path || !_push_req.__isset.http_file_size) {
        _is_init = true;
        return status;
    }

    // Check remote path
    _remote_file_path = _push_req.http_file_path;
    LOG(INFO) << "start get file. remote_file_path: " << _remote_file_path;
    // Set download param
    string tmp_file_dir;
    string root_path = tablet->data_dir()->path();
    status = _get_tmp_file_dir(root_path, &tmp_file_dir);

    if (!status.ok()) {
        LOG(WARNING) << "get local path failed. tmp file dir: " << tmp_file_dir;
        return status;
    }
    string tmp_file_name;
    _get_file_name_from_path(_push_req.http_file_path, &tmp_file_name);
    _local_file_path = tmp_file_dir + "/" + tmp_file_name;
    _is_init = true;
    return status;
}

// Get replica root path
Status EngineBatchLoadTask::_get_tmp_file_dir(const string& root_path, string* download_path) {
    Status status = Status::OK();
    *download_path = root_path + DPP_PREFIX;

    // Check path exist
    std::filesystem::path full_path(*download_path);

    if (!std::filesystem::exists(full_path)) {
        LOG(INFO) << "download dir not exist: " << *download_path;
        std::error_code ec;
        std::filesystem::create_directories(*download_path, ec);

        if (ec) {
            status = Status::IOError("Create download dir failed " + *download_path);
            LOG(WARNING) << "create download dir failed.path: " << *download_path
                         << ", error code: " << ec;
        }
    }

    return status;
}

void EngineBatchLoadTask::_get_file_name_from_path(const string& file_path, string* file_name) {
    size_t found = file_path.find_last_of("/\\");
    pthread_t tid = pthread_self();
    *file_name = file_path.substr(found + 1) + "_" + boost::lexical_cast<string>(tid);
}

Status EngineBatchLoadTask::_process() {
    Status status = Status::OK();
    if (!_is_init) {
        LOG(WARNING) << "has not init yet. tablet_id: " << _push_req.tablet_id;
        return Status::InternalError("Tablet has not init yet");
    }
    // Remote file not empty, need to download
    if (_push_req.__isset.http_file_path) {
        // Get file length and timeout
        uint64_t file_size = 0;
        uint64_t estimate_time_out = DEFAULT_DOWNLOAD_TIMEOUT;
        if (_push_req.__isset.http_file_size) {
            file_size = _push_req.http_file_size;
            estimate_time_out = file_size / config::download_low_speed_limit_kbps / 1024;
        }
        if (estimate_time_out < config::download_low_speed_time) {
            estimate_time_out = config::download_low_speed_time;
        }
        bool is_timeout = false;
        auto download_cb = [this, estimate_time_out, file_size, &is_timeout](HttpClient* client) {
            // Check timeout and set timeout
            time_t now = time(nullptr);
            if (_push_req.timeout > 0 && _push_req.timeout < now) {
                // return status to break this callback
                VLOG_NOTICE << "check time out. time_out:" << _push_req.timeout << ", now:" << now;
                is_timeout = true;
                return Status::OK();
            }

            RETURN_IF_ERROR(client->init(_remote_file_path));
            // sent timeout
            uint64_t timeout = _push_req.timeout > 0 ? _push_req.timeout - now : 0;
            if (timeout > 0 && timeout < estimate_time_out) {
                client->set_timeout_ms(timeout * 1000);
            } else {
                client->set_timeout_ms(estimate_time_out * 1000);
            }

            // download remote file
            RETURN_IF_ERROR(client->download(_local_file_path));

            // check file size
            if (_push_req.__isset.http_file_size) {
                // Check file size
                uint64_t local_file_size = std::filesystem::file_size(_local_file_path);
                if (file_size != local_file_size) {
                    LOG(WARNING) << "download_file size error. file_size=" << file_size
                                 << ", local_file_size=" << local_file_size;
                    return Status::InternalError("downloaded file's size isn't right");
                }
            }
            // NOTE: change http_file_path is not good design
            _push_req.http_file_path = _local_file_path;
            return Status::OK();
        };

        MonotonicStopWatch stopwatch;
        stopwatch.start();
        auto st = HttpClient::execute_with_retry(MAX_RETRY, 1, download_cb);
        auto cost = stopwatch.elapsed_time();
        if (cost <= 0) {
            cost = 1;
        }
        if (st.ok() && !is_timeout) {
            double rate = -1.0;
            if (_push_req.__isset.http_file_size) {
                rate = (double)_push_req.http_file_size / (cost / 1000 / 1000 / 1000) / 1024;
            }
            LOG(INFO) << "down load file success. local_file=" << _local_file_path
                      << ", remote_file=" << _remote_file_path << ", tablet_id"
                      << _push_req.tablet_id << ", cost=" << cost / 1000 << "us, file_size"
                      << _push_req.http_file_size << ", download rage:" << rate << "KB/s";
        } else {
            LOG(WARNING) << "down load file failed. remote_file=" << _remote_file_path
                         << ", tablet=" << _push_req.tablet_id << ", cost=" << cost / 1000
                         << "us, errmsg=" << st.get_error_msg() << ", is_timeout=" << is_timeout;
            status = Status::InternalError("Download file failed");
        }
    }

    if (status.ok()) {
        // Load delta file
        time_t push_begin = time(nullptr);
        OLAPStatus push_status = _push(_push_req, _tablet_infos);
        time_t push_finish = time(nullptr);
        LOG(INFO) << "Push finish, cost time: " << (push_finish - push_begin);
        if (push_status == OLAPStatus::OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
            status = Status::OK();
        } else if (push_status != OLAPStatus::OLAP_SUCCESS) {
            status = Status::InternalError("Unknown");
        }
    }

    // Delete download file
    if (std::filesystem::exists(_local_file_path)) {
        if (remove(_local_file_path.c_str()) == -1) {
            LOG(WARNING) << "can not remove file=" << _local_file_path;
        }
    }

    return status;
}

OLAPStatus EngineBatchLoadTask::_push(const TPushReq& request,
                                      std::vector<TTabletInfo>* tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to process push. "
              << " transaction_id=" << request.transaction_id << " tablet_id=" << request.tablet_id
              << ", version=" << request.version;

    if (tablet_info_vec == nullptr) {
        LOG(WARNING) << "invalid output parameter which is nullptr pointer.";
        DorisMetrics::instance()->push_requests_fail_total->increment(1);
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.tablet_id, request.schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "false to find tablet. tablet=" << request.tablet_id
                     << ", schema_hash=" << request.schema_hash;
        DorisMetrics::instance()->push_requests_fail_total->increment(1);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    PushType type = PUSH_NORMAL;
    if (request.push_type == TPushType::LOAD_DELETE) {
        type = PUSH_FOR_LOAD_DELETE;
    } else if (request.push_type == TPushType::LOAD_V2) {
        type = PUSH_NORMAL_V2;
    }

    int64_t duration_ns = 0;
    PushHandler push_handler;
    if (request.__isset.transaction_id) {
        {
            SCOPED_RAW_TIMER(&duration_ns);
            res = push_handler.process_streaming_ingestion(tablet, request, type, tablet_info_vec);
        }
    } else {
        {
            SCOPED_RAW_TIMER(&duration_ns);
            res = OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED;
        }
    }

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to push delta, "
                     << "transaction_id=" << request.transaction_id
                     << " tablet=" << tablet->full_name()
                     << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::instance()->push_requests_fail_total->increment(1);
    } else {
        LOG(INFO) << "success to push delta, "
                  << "transaction_id=" << request.transaction_id
                  << " tablet=" << tablet->full_name()
                  << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::instance()->push_requests_success_total->increment(1);
        DorisMetrics::instance()->push_request_duration_us->increment(duration_ns / 1000);
        DorisMetrics::instance()->push_request_write_bytes->increment(push_handler.write_bytes());
        DorisMetrics::instance()->push_request_write_rows->increment(push_handler.write_rows());
    }
    return res;
}

OLAPStatus EngineBatchLoadTask::_delete_data(const TPushReq& request,
                                             std::vector<TTabletInfo>* tablet_info_vec) {
    VLOG_DEBUG << "begin to process delete data. request=" << ThriftDebugString(request);
    DorisMetrics::instance()->delete_requests_total->increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    if (tablet_info_vec == nullptr) {
        LOG(WARNING) << "invalid tablet info parameter which is nullptr pointer.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    // 1. Get all tablets with same tablet_id
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.tablet_id, request.schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "can't find tablet. tablet=" << request.tablet_id
                     << ", schema_hash=" << request.schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Process delete data by push interface
    PushHandler push_handler;
    if (request.__isset.transaction_id) {
        res = push_handler.process_streaming_ingestion(tablet, request, PUSH_FOR_DELETE,
                                                       tablet_info_vec);
    } else {
        res = OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED;
    }

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING(
                "fail to push empty version for delete data. "
                "[res=%d tablet='%s']",
                res, tablet->full_name().c_str());
        DorisMetrics::instance()->delete_requests_failed->increment(1);
        return res;
    }

    LOG(INFO) << "finish to process delete data. res=" << res;
    return res;
}

} // namespace doris
