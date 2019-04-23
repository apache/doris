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
#include "boost/filesystem.hpp"
#include "boost/lexical_cast.hpp"
#include "agent/cgroups_mgr.h"
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
    
EngineBatchLoadTask::EngineBatchLoadTask(TPushReq& push_req, 
    std::vector<TTabletInfo>* tablet_infos,
    int64_t signature, 
    AgentStatus* res_status) :
        _push_req(push_req),
        _tablet_infos(tablet_infos),
        _signature(signature),
        _res_status(res_status) {
    _download_status = DORIS_SUCCESS;
}

EngineBatchLoadTask::~EngineBatchLoadTask() {
}

OLAPStatus EngineBatchLoadTask::execute() {
    AgentStatus status = DORIS_SUCCESS;
    if (_push_req.push_type == TPushType::LOAD || _push_req.push_type == TPushType::LOAD_DELETE) {
        status = _init();
        if (status == DORIS_SUCCESS) {
            uint32_t retry_time = 0;
            while (retry_time < PUSH_MAX_RETRY) {
                status = _process();

                if (status == DORIS_PUSH_HAD_LOADED) {
                    OLAP_LOG_WARNING("transaction exists when realtime push, "
                                        "but unfinished, do not report to fe, signature: %ld",
                                        _signature);
                    break;  // not retry any more
                }
                // Internal error, need retry
                if (status == DORIS_ERROR) {
                    OLAP_LOG_WARNING("push internal error, need retry.signature: %ld",
                                        _signature);
                    retry_time += 1;
                } else {
                    break;
                }
            }
        }
    } else if (_push_req.push_type == TPushType::DELETE) {
        OLAPStatus delete_data_status = _delete_data(_push_req, _tablet_infos);
        if (delete_data_status != OLAPStatus::OLAP_SUCCESS) {
            OLAP_LOG_WARNING("delete data failed. status: %d, signature: %ld",
                                delete_data_status, _signature);
            status = DORIS_ERROR;
        }
    } else {
        status = DORIS_TASK_REQUEST_ERROR;
    }
    *_res_status = status;
    return OLAP_SUCCESS;
}

AgentStatus EngineBatchLoadTask::_init() {
    AgentStatus status = DORIS_SUCCESS;

    if (_is_init) {
        VLOG(3) << "has been inited";
        return status;
    }

    // Check replica exist
    TabletSharedPtr tablet;
    tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            _push_req.tablet_id,
            _push_req.schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "get tables failed. "
                     << "tablet_id: " << _push_req.tablet_id
                     << ", schema_hash: " << _push_req.schema_hash;
        status = DORIS_PUSH_INVALID_TABLE;
    }

    // Empty remote_path
    if (status == DORIS_SUCCESS && !_push_req.__isset.http_file_path) {
        _is_init = true;
        return status;
    }

    // Check remote path
    string remote_full_path;
    string tmp_file_dir;

    if (status == DORIS_SUCCESS) {
        remote_full_path = _push_req.http_file_path;

        // Get local download path
        LOG(INFO) << "start get file. remote_full_path: " << remote_full_path;
        string root_path = tablet->data_dir()->path();

        status = _get_tmp_file_dir(root_path, &tmp_file_dir);
        if (DORIS_SUCCESS != status) {
            LOG(WARNING) << "get local path failed. tmp file dir: " << tmp_file_dir;
        }
    }

    // Set download param
    if (status == DORIS_SUCCESS) {
        string tmp_file_name;
        _get_file_name_from_path(_push_req.http_file_path, &tmp_file_name);

        _downloader_param.username = "";
        _downloader_param.password = "";
        _downloader_param.remote_file_path = remote_full_path;
        _downloader_param.local_file_path = tmp_file_dir + "/" + tmp_file_name;

        _is_init = true;
    }

    return status;
}

// Get replica root path
AgentStatus EngineBatchLoadTask::_get_tmp_file_dir(const string& root_path, string* download_path) {
    AgentStatus status = DORIS_SUCCESS;
    *download_path = root_path + DPP_PREFIX;

    // Check path exist
    boost::filesystem::path full_path(*download_path);

    if (!boost::filesystem::exists(full_path)) {
        LOG(INFO) << "download dir not exist: " << *download_path;
        boost::system::error_code error_code;
        boost::filesystem::create_directories(*download_path, error_code);

        if (0 != error_code) {
            status = DORIS_ERROR;
            LOG(WARNING) << "create download dir failed.path: "
                         << *download_path << ", error code: " << error_code;
        }
    }

    return status;
}

AgentStatus EngineBatchLoadTask::_download_file() {
    LOG(INFO) << "begin download file. tablet_id=" << _push_req.tablet_id;
    time_t start = time(NULL);
    AgentStatus status = DORIS_SUCCESS;

    status = _file_downloader->download_file();

    time_t cost = time(NULL) - start;
    if (cost <= 0) {
        cost = 1;
    }
    // KB/s
    double rate = -1.0;
    if (_push_req.__isset.http_file_size) {
        rate = (double) _push_req.http_file_size / cost / 1024;
    }
    if (status == DORIS_SUCCESS) {
        LOG(INFO) << "down load file success. local_file=" << _downloader_param.local_file_path
                  << ", remote_file=" << _downloader_param.remote_file_path
                  << ", tablet_id" << _push_req.tablet_id
                  << ", cost=" << cost << ", file_size" << _push_req.http_file_size
                  << ", download rage:" << rate << "KB/s";
    } else {
        LOG(WARNING) << "down load file failed. remote_file=" << _downloader_param.remote_file_path
                     << ", tablet=" << _push_req.tablet_id
                     << ", cost=" << cost << " file size: " << _push_req.http_file_size << " B";
    }

    // todo check data length and mv name tmp
    return status;
}

void EngineBatchLoadTask::_get_file_name_from_path(const string& file_path, string* file_name) {
    size_t found = file_path.find_last_of("/\\");
    pthread_t tid = pthread_self();
    *file_name = file_path.substr(found + 1) + "_" + boost::lexical_cast<string>(tid);
}

AgentStatus EngineBatchLoadTask::_process() {
    AgentStatus status = DORIS_SUCCESS;

    if (!_is_init) {
        LOG(WARNING) << "has not init yet. tablet_id: " 
                     << _push_req.tablet_id;
        return DORIS_ERROR;
    }

    // Remote file not empty, need to download
    if (_push_req.__isset.http_file_path) {
        // Get file length
        uint64_t file_size = 0;
        uint64_t estimate_time_out = DEFAULT_DOWNLOAD_TIMEOUT;
        if (_push_req.__isset.http_file_size) {
            file_size = _push_req.http_file_size;
            estimate_time_out = 
                    file_size / config::download_low_speed_limit_kbps / 1024;
        }
        if (estimate_time_out < config::download_low_speed_time) {
            estimate_time_out = config::download_low_speed_time;
        }

        // Download file from hdfs
        for (uint32_t i = 0; i < MAX_RETRY; ++i) {
            // Check timeout and set timeout
            time_t now = time(NULL);

            if (_push_req.timeout > 0) {
                VLOG(3) << "check time out. time_out:" << _push_req.timeout
                        << ", now:" << now;
                if (_push_req.timeout < now) {
                    LOG(WARNING) << "push time out";
                    status = DORIS_PUSH_TIME_OUT;
                    break;
                }
            }

            _downloader_param.curl_opt_timeout = estimate_time_out;
            uint64_t timeout = _push_req.timeout > 0 ? _push_req.timeout - now : 0;
            if (timeout > 0 && timeout < estimate_time_out) {
                _downloader_param.curl_opt_timeout = timeout;
            }

            VLOG(3) << "estimate_time_out: " << estimate_time_out
                    << ", download_timeout: " << estimate_time_out
                    << ", curl_opt_timeout: " << _downloader_param.curl_opt_timeout
                    << ", download file, retry time:" << i;
#ifndef BE_TEST
            _file_downloader = new FileDownloader(_downloader_param);
            _download_status = _download_file();
            if (_file_downloader != NULL) {
                delete _file_downloader;
                _file_downloader = NULL;
            }
#endif

            status = _download_status;
            if (_push_req.__isset.http_file_size && status == DORIS_SUCCESS) {
                // Check file size
                boost::filesystem::path local_file_path(_downloader_param.local_file_path);
                uint64_t local_file_size = boost::filesystem::file_size(local_file_path);
                VLOG(3) << "file_size: " << file_size
                        << ", local_file_size: " << local_file_size;

                if (file_size != local_file_size) {
                    OLAP_LOG_WARNING(
                            "download_file size error. file_size: %d, local_file_size: %d",
                            file_size, local_file_size);
                    status = DORIS_FILE_DOWNLOAD_FAILED;
                }
            }
            
            if (status == DORIS_SUCCESS) {
                _push_req.http_file_path = _downloader_param.local_file_path;
                break;
            }
#ifndef BE_TEST
            sleep(config::sleep_one_second);
#endif
        }
    }

    if (status == DORIS_SUCCESS) {
        // Load delta file
        time_t push_begin = time(NULL);
        OLAPStatus push_status = _push(_push_req, _tablet_infos);
        time_t push_finish = time(NULL);
        LOG(INFO) << "Push finish, cost time: " << (push_finish - push_begin);
        if (push_status == OLAPStatus::OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
            status = DORIS_PUSH_HAD_LOADED;
        } else if (push_status != OLAPStatus::OLAP_SUCCESS) {
            status = DORIS_ERROR;
        }
    }

    // Delete download file
    boost::filesystem::path download_file_path(_downloader_param.local_file_path);
    if (boost::filesystem::exists(download_file_path)) {
        if (remove(_downloader_param.local_file_path.c_str()) == -1) {
            OLAP_LOG_WARNING("can not remove file: %s",
                             _downloader_param.local_file_path.c_str());
        }
    }

    return status;
}

OLAPStatus EngineBatchLoadTask::_push(const TPushReq& request,
                        vector<TTabletInfo>* tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to process push. " 
              << " transaction_id=" << request.transaction_id
              << " tablet_id=" << request.tablet_id
              << ", version=" << request.version;

    if (tablet_info_vec == NULL) {
        LOG(WARNING) << "invalid output parameter which is null pointer.";
        DorisMetrics::push_requests_fail_total.increment(1);
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.tablet_id, request.schema_hash);
    if (NULL == tablet.get()) {
        OLAP_LOG_WARNING("false to find tablet. [tablet=%ld schema_hash=%d]",
                         request.tablet_id, request.schema_hash);
        DorisMetrics::push_requests_fail_total.increment(1);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    PushType type = PUSH_NORMAL;
    if (request.push_type == TPushType::LOAD_DELETE) {
        type = PUSH_FOR_LOAD_DELETE;
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
        DorisMetrics::push_requests_fail_total.increment(1);
    } else {
        LOG(INFO) << "success to push delta, " 
            << "transaction_id=" << request.transaction_id
            << " tablet=" << tablet->full_name()
            << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::push_requests_success_total.increment(1);
        DorisMetrics::push_request_duration_us.increment(duration_ns / 1000);
        DorisMetrics::push_request_write_bytes.increment(push_handler.write_bytes());
        DorisMetrics::push_request_write_rows.increment(push_handler.write_rows());
    }
    return res;
}

OLAPStatus EngineBatchLoadTask::_delete_data(
        const TPushReq& request,
        vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to process delete data. request=" << ThriftDebugString(request);
    DorisMetrics::delete_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    if (tablet_info_vec == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    // 1. Get all tablets with same tablet_id
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id, request.schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find tablet. [tablet=%ld schema_hash=%d]",
                         request.tablet_id, request.schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Process delete data by push interface
    PushHandler push_handler;
    if (request.__isset.transaction_id) {
        res = push_handler.process_streaming_ingestion(tablet, request, PUSH_FOR_DELETE, tablet_info_vec);
    } else {
        res = OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED;
    }

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to push empty version for delete data. "
                         "[res=%d tablet='%s']",
                         res, tablet->full_name().c_str());
        DorisMetrics::delete_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "finish to process delete data. res=" << res;
    return res;
}

} // namespace doris
