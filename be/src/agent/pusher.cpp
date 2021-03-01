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

#include "agent/cgroups_mgr.h"
#include "boost/filesystem.hpp"
#include "boost/lexical_cast.hpp"
#include "gen_cpp/AgentService_types.h"
#include "http/http_client.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/olap_table.h"
#include "util/stopwatch.hpp"

using std::list;
using std::string;
using std::vector;

namespace doris {

Pusher::Pusher(OLAPEngine* engine, const TPushReq& push_req)
        : _push_req(push_req), _engine(engine) {}

Pusher::~Pusher() {}

AgentStatus Pusher::init() {
    AgentStatus status = DORIS_SUCCESS;

    // Check replica exist
    OLAPTablePtr olap_table;
    olap_table = _engine->get_table(_push_req.tablet_id, _push_req.schema_hash);
    if (olap_table.get() == NULL) {
        OLAP_LOG_WARNING("get tables failed. tablet_id: %ld, schema_hash: %ld", _push_req.tablet_id,
                         _push_req.schema_hash);
        return DORIS_PUSH_INVALID_TABLE;
    }

    // Empty remote_path
    if (!_push_req.__isset.http_file_path) {
        return status;
    }

    // Check remote path
    _remote_file_path = _push_req.http_file_path;

    // Get local download path
    LOG(INFO) << "start get file. remote_file_path: " << _remote_file_path;

    // Set download param
    string tmp_file_dir;
    status = _get_tmp_file_dir(olap_table->storage_root_path_name(), &tmp_file_dir);
    if (status != DORIS_SUCCESS) {
        LOG(WARNING) << "get local path failed. tmp file dir: " << tmp_file_dir;
        return status;
    }
    string tmp_file_name;
    _get_file_name_from_path(_push_req.http_file_path, &tmp_file_name);
    _local_file_path = tmp_file_dir + "/" + tmp_file_name;

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
            LOG(WARNING) << "create download dir failed.path: " << *download_path
                         << ", error code: " << error_code;
        }
    }

    return status;
}

void Pusher::_get_file_name_from_path(const string& file_path, string* file_name) {
    size_t found = file_path.find_last_of("/\\");
    pthread_t tid = pthread_self();
    *file_name = file_path.substr(found + 1) + "_" + boost::lexical_cast<string>(tid);
}

AgentStatus Pusher::process(vector<TTabletInfo>* tablet_infos) {
    AgentStatus status = DORIS_SUCCESS;
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
            time_t now = time(NULL);
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
                uint64_t local_file_size = boost::filesystem::file_size(_local_file_path);
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
            status = DORIS_ERROR;
        }
    }

    if (status == DORIS_SUCCESS) {
        // Load delta file
        time_t push_begin = time(NULL);
        OLAPStatus push_status = _engine->push(_push_req, tablet_infos);
        time_t push_finish = time(NULL);
        LOG(INFO) << "Push finish, cost time: " << (push_finish - push_begin);
        if (push_status == OLAPStatus::OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
            status = DORIS_PUSH_HAD_LOADED;
        } else if (push_status != OLAPStatus::OLAP_SUCCESS) {
            status = DORIS_ERROR;
        }
    }

    // Delete download file
    if (boost::filesystem::exists(_local_file_path)) {
        if (remove(_local_file_path.c_str()) == -1) {
            LOG(WARNING) << "can not remove file=" << _local_file_path;
        }
    }

    return status;
}

} // namespace doris
