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

#include "agent/pusher.h"
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
#include "agent/file_downloader.h"
#include "gen_cpp/AgentService_types.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/olap_table.h"

using std::list;
using std::string;
using std::vector;

namespace doris {

    
Pusher::Pusher(OLAPEngine* engine, const TPushReq& push_req) :
        _push_req(push_req), _engine(engine) {
    _download_status = DORIS_SUCCESS;
}

Pusher::~Pusher() {
}

AgentStatus Pusher::init() {
    AgentStatus status = DORIS_SUCCESS;

    if (_is_init) {
        OLAP_LOG_DEBUG("has been inited");
        return status;
    }

    // Check replica exist
    OLAPTablePtr olap_table;
    olap_table = _engine->get_table(
            _push_req.tablet_id,
            _push_req.schema_hash);
    if (olap_table.get() == NULL) {
        OLAP_LOG_WARNING("get tables failed. tablet_id: %ld, schema_hash: %ld",
                         _push_req.tablet_id, _push_req.schema_hash);
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
        string root_path = olap_table->storage_root_path_name();

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
AgentStatus Pusher::_get_tmp_file_dir(const string& root_path, string* download_path) {
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

AgentStatus Pusher::_download_file() {
    OLAP_LOG_INFO("begin download file. tablet=%d", _push_req.tablet_id);
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
        OLAP_LOG_INFO("down load file success. local_file=%s, remote_file=%s, "
                      "tablet=%d, cost=%ld, file size: %ld B, download rate: %f KB/s",
                _downloader_param.local_file_path.c_str(),
                _downloader_param.remote_file_path.c_str(),
                _push_req.tablet_id, cost, _push_req.http_file_size, rate);
    } else {
        LOG(WARNING) << "down load file failed. remote_file=" << _downloader_param.remote_file_path
                     << " tablet=" << _push_req.tablet_id
                     << " cost=" << cost << " file size: " << _push_req.http_file_size << " B";
    }

    // todo check data length and mv name tmp
    return status;
}

void Pusher::_get_file_name_from_path(const string& file_path, string* file_name) {
    size_t found = file_path.find_last_of("/\\");
    pthread_t tid = pthread_self();
    *file_name = file_path.substr(found + 1) + "_" + boost::lexical_cast<string>(tid);
}

AgentStatus Pusher::process(vector<TTabletInfo>* tablet_infos) {
    AgentStatus status = DORIS_SUCCESS;

    if (!_is_init) {
        OLAP_LOG_WARNING("has not init yet. tablet_id: %d", _push_req.tablet_id);
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
                OLAP_LOG_DEBUG(
                    "check time out. time_out:%ld, now:%d",
                    _push_req.timeout, now);

                if (_push_req.timeout < now) {
                    OLAP_LOG_WARNING("push time out");
                    status = DORIS_PUSH_TIME_OUT;
                    break;
                }
            }

            _downloader_param.curl_opt_timeout = estimate_time_out;
            uint64_t timeout = _push_req.timeout > 0 ? _push_req.timeout - now : 0;
            if (timeout > 0 && timeout < estimate_time_out) {
                _downloader_param.curl_opt_timeout = timeout;
            }

            OLAP_LOG_DEBUG(
                    "estimate_time_out: %d, download_timeout: %d, curl_opt_timeout: %d",
                    estimate_time_out,
                    _push_req.timeout,
                    _downloader_param.curl_opt_timeout);
            OLAP_LOG_DEBUG("download file, retry time:%d", i);

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
                OLAP_LOG_DEBUG(
                    "file_size: %d, local_file_size: %d",
                    file_size, local_file_size);

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
        OLAPStatus push_status = _engine->push(_push_req, tablet_infos);
        time_t push_finish = time(NULL);
        OLAP_LOG_INFO("Push finish, cost time: %ld", push_finish - push_begin);
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
} // namespace doris
