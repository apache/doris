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

#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <pthread.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <list>
#include <string>
#include <system_error>

#include "boost/lexical_cast.hpp"
#include "common/config.h"
#include "common/logging.h"
#include "http/http_client.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/push_handler.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"

using apache::thrift::ThriftDebugString;
using std::string;
using std::vector;

namespace doris {
namespace {
constexpr uint32_t PUSH_MAX_RETRY = 1;
constexpr uint32_t MAX_RETRY = 3;
constexpr uint32_t DEFAULT_DOWNLOAD_TIMEOUT = 3600;
} // namespace

using namespace ErrorCode;

EngineBatchLoadTask::EngineBatchLoadTask(StorageEngine& engine, TPushReq& push_req,
                                         std::vector<TTabletInfo>* tablet_infos)
        : _engine(engine), _push_req(push_req), _tablet_infos(tablet_infos) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::LOAD,
            fmt::format("EngineBatchLoadTask#pushType={}:tabletId={}", _push_req.push_type,
                        std::to_string(_push_req.tablet_id)));
}

EngineBatchLoadTask::~EngineBatchLoadTask() = default;

Status EngineBatchLoadTask::execute() {
    Status status;
    if (_push_req.push_type == TPushType::LOAD_V2) {
        RETURN_IF_ERROR(_init());
        uint32_t retry_time = 0;
        while (retry_time < PUSH_MAX_RETRY) {
            status = _process();
            // Internal error, need retry
            if (status.ok()) {
                break;
            }
            retry_time += 1;
        }
    } else if (_push_req.push_type == TPushType::DELETE) {
        status = _delete_data(_push_req, _tablet_infos);
    } else {
        return Status::InvalidArgument("Not support task type");
    }
    return status;
}

Status EngineBatchLoadTask::_init() {
    Status status = Status::OK();

    if (_is_init) {
        VLOG_NOTICE << "has been inited";
        return status;
    }

    // Check replica exist
    TabletSharedPtr tablet;
    tablet = _engine.tablet_manager()->get_tablet(_push_req.tablet_id);
    if (tablet == nullptr) {
        return Status::InvalidArgument("Could not find tablet {}", _push_req.tablet_id);
    }

    // check disk capacity
    if (_push_req.push_type == TPushType::LOAD_V2) {
        if (tablet->data_dir()->reach_capacity_limit(_push_req.http_file_size)) {
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
    *download_path = root_path + "/" + DPP_PREFIX;

    // Check path exist
    std::filesystem::path full_path(*download_path);

    if (!std::filesystem::exists(full_path)) {
        LOG(INFO) << "download dir not exist: " << *download_path;
        std::error_code ec;
        std::filesystem::create_directories(*download_path, ec);

        if (ec) {
            return Status::IOError("Create download dir failed {}", *download_path);
        }
    }
    return Status::OK();
}

void EngineBatchLoadTask::_get_file_name_from_path(const string& file_path, string* file_name) {
    size_t found = file_path.find_last_of("/\\");
    pthread_t tid = pthread_self();
    *file_name = file_path.substr(found + 1) + "_" + boost::lexical_cast<string>(tid);
}

Status EngineBatchLoadTask::_process() {
    Status status = Status::OK();
    if (!_is_init) {
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
                    return Status::InternalError(
                            "download_file size error. file_size={}, local_file_size={}", file_size,
                            local_file_size);
                }
            }
            // NOTE: change http_file_path is not good design
            _push_req.http_file_path = _local_file_path;
            return Status::OK();
        };

        MonotonicStopWatch stopwatch;
        stopwatch.start();
        status = HttpClient::execute_with_retry(MAX_RETRY, 1, download_cb);
        auto cost = stopwatch.elapsed_time();
        if (cost <= 0) {
            cost = 1;
        }
        if (status.ok() && !is_timeout) {
            double rate = -1.0;
            if (_push_req.__isset.http_file_size) {
                rate = (double)_push_req.http_file_size / (cost / 1000 / 1000 / 1000) / 1024;
            }
            LOG(INFO) << "succeed to download file. local_file=" << _local_file_path
                      << ", remote_file=" << _remote_file_path << ", tablet_id"
                      << _push_req.tablet_id << ", cost=" << cost / 1000 << "us, file_size"
                      << _push_req.http_file_size << ", download rage:" << rate << "KB/s";
        } else {
            LOG(WARNING) << "download file failed. remote_file=" << _remote_file_path
                         << ", tablet=" << _push_req.tablet_id << ", cost=" << cost / 1000
                         << "us, is_timeout=" << is_timeout;
        }
    }

    if (status.ok()) {
        // Load delta file
        time_t push_begin = time(nullptr);
        status = _push(_push_req, _tablet_infos);
        time_t push_finish = time(nullptr);
        LOG(INFO) << "Push finish, cost time: " << (push_finish - push_begin);
        if (status.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
            status = Status::OK();
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

Status EngineBatchLoadTask::_push(const TPushReq& request,
                                  std::vector<TTabletInfo>* tablet_info_vec) {
    Status res = Status::OK();
    LOG(INFO) << "begin to process push. "
              << " transaction_id=" << request.transaction_id << " tablet_id=" << request.tablet_id
              << ", version=" << request.version;

    if (tablet_info_vec == nullptr) {
        DorisMetrics::instance()->push_requests_fail_total->increment(1);
        return Status::InvalidArgument("invalid tablet_info_vec which is nullptr");
    }

    TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(request.tablet_id);
    if (tablet == nullptr) {
        DorisMetrics::instance()->push_requests_fail_total->increment(1);
        return Status::InternalError("could not find tablet {}", request.tablet_id);
    }

    PushType type = PushType::PUSH_NORMAL_V2;
    int64_t duration_ns = 0;
    PushHandler push_handler(_engine);
    if (!request.__isset.transaction_id) {
        return Status::InvalidArgument("transaction_id is not set");
    }
    {
        SCOPED_RAW_TIMER(&duration_ns);
        res = push_handler.process_streaming_ingestion(tablet, request, type, tablet_info_vec);
    }

    if (!res.ok()) {
        LOG(WARNING) << "failed to push delta, transaction_id=" << request.transaction_id
                     << ", tablet=" << tablet->tablet_id()
                     << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::instance()->push_requests_fail_total->increment(1);
    } else {
        LOG(INFO) << "succeed to push delta, transaction_id=" << request.transaction_id
                  << ", tablet=" << tablet->tablet_id()
                  << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::instance()->push_requests_success_total->increment(1);
        DorisMetrics::instance()->push_request_duration_us->increment(duration_ns / 1000);
        DorisMetrics::instance()->push_request_write_bytes->increment(push_handler.write_bytes());
        DorisMetrics::instance()->push_request_write_rows->increment(push_handler.write_rows());
    }
    return res;
}

Status EngineBatchLoadTask::_delete_data(const TPushReq& request,
                                         std::vector<TTabletInfo>* tablet_info_vec) {
    VLOG_DEBUG << "begin to process delete data. request=" << ThriftDebugString(request);
    DorisMetrics::instance()->delete_requests_total->increment(1);

    Status res = Status::OK();

    if (tablet_info_vec == nullptr) {
        return Status::InvalidArgument("invalid tablet_info_vec which is nullptr");
    }

    // 1. Get all tablets with same tablet_id
    TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(request.tablet_id);
    if (tablet == nullptr) {
        return Status::InternalError("could not find tablet {}", request.tablet_id);
    }

    // 2. Process delete data by push interface
    PushHandler push_handler(_engine);
    if (!request.__isset.transaction_id) {
        return Status::InvalidArgument("transaction_id is not set");
    }
    res = push_handler.process_streaming_ingestion(tablet, request, PushType::PUSH_FOR_DELETE,
                                                   tablet_info_vec);
    if (!res.ok()) {
        DorisMetrics::instance()->delete_requests_failed->increment(1);
    }
    return res;
}

} // namespace doris
