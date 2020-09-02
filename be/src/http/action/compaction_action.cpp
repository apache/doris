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

#include "http/action/compaction_action.h"

#include <sstream>
#include <string>
#include <sys/syscall.h>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "util/json_util.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

bool CompactionAction::_is_compaction_running = false;
std::mutex CompactionAction::_compaction_running_mutex;

Status CompactionAction::_check_param(HttpRequest* req, uint64_t* tablet_id, uint32_t* schema_hash) {
    
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    std::string req_schema_hash = req->param(TABLET_SCHEMA_HASH_KEY);
    if (req_tablet_id == "" && req_schema_hash == "") {
        // TODO(cmy): View the overall compaction status
        return Status::NotSupported("The overall compaction status is not supported yet");
    }

    try {
        *tablet_id = std::stoull(req_tablet_id);
        *schema_hash = std::stoul(req_schema_hash);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id
                     << ", schema_hash:" << req_schema_hash;
        return Status::InternalError(strings::Substitute("convert failed, $0", e.what()));
    }

    return Status::OK();
}

// for viewing the compaction status
Status CompactionAction::_handle_show_compaction(HttpRequest* req, std::string* json_result) {
    
    uint64_t tablet_id = 0;
    uint32_t schema_hash = 0;
    
    Status status = _check_param(req, &tablet_id, &schema_hash);
    RETURN_IF_ERROR(status);

    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found");
    }

    tablet->get_compaction_status(json_result);
    return Status::OK();
}

Status CompactionAction::_handle_run_compaction(HttpRequest *req, std::string* json_result) {

    // 1. param check
    uint64_t tablet_id = 0;
    uint32_t schema_hash = 0;
    
    // check req_tablet_id and req_schema_hash is not empty
    Status check_status = _check_param(req, &tablet_id, &schema_hash);
    RETURN_IF_ERROR(check_status);

    std::string compaction_type = req->param(PARAM_COMPACTION_TYPE);
    // check compaction_type is not empty and equals base or cumulative
    if (compaction_type == "" && !(compaction_type == PARAM_COMPACTION_BASE || compaction_type == PARAM_COMPACTION_CUMULATIVE)) {
        return Status::NotSupported("The compaction type is not supported");
    }

    // 2. fetch the tablet by tablet_id and schema_hash
    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);

    if (tablet == nullptr) {
        LOG(WARNING) << "invalid argument.tablet_id:" << tablet_id
                     << ", schema_hash:" << schema_hash;
        return Status::InternalError(
                strings::Substitute("fail to get $0, $1", tablet_id, schema_hash));
    }

    // 3. execute compaction task
    std::packaged_task<OLAPStatus()> task([this, tablet, compaction_type]() { 
            return _execute_compaction_callback(tablet, compaction_type);
    });
    std::future<OLAPStatus> future_obj = task.get_future();

    {
        // 3.1 check is there compaction running
        std::lock_guard<std::mutex> lock(_compaction_running_mutex);
        if (_is_compaction_running) {
            return Status::TooManyTasks("Manual compaction task is running");
        } else {
            // 3.2 execute the compaction task and set compaction task running 
            _is_compaction_running = true;
            std::thread(std::move(task)).detach();
        }
    }

    // 4. wait for result for 2 seconds by async
    std::future_status status = future_obj.wait_for(std::chrono::seconds(2));
    if (status == std::future_status::ready) {
        // fetch execute result
        OLAPStatus olap_status = future_obj.get();
        if (olap_status != OLAP_SUCCESS) {
            return Status::InternalError(
                    strings::Substitute("fail to execute compaction, error = $0", olap_status));
        }
    } else {
        LOG(INFO) << "Manual compaction task is timeout for waiting " << (status == std::future_status::timeout);
    }
   
    LOG(INFO) << "Manual compaction task is successfully triggered";
    *json_result = "{\"status\": \"Success\", \"msg\": \"compaction task is successfully triggered.\"}";

    return Status::OK();
}

Status CompactionAction::_handle_run_status_compaction(HttpRequest *req, std::string* json_result) {

    uint64_t tablet_id = 0;
    uint32_t schema_hash = 0;
    
    // check req_tablet_id and req_schema_hash is not empty
    Status check_status = _check_param(req, &tablet_id, &schema_hash);
    RETURN_IF_ERROR(check_status);

    // fetch the tablet by tablet_id and schema_hash
    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);

    if (tablet == nullptr) {
        LOG(WARNING) << "invalid argument.tablet_id:" << tablet_id
                     << ", schema_hash:" << schema_hash;
        return Status::InternalError(
                strings::Substitute("fail to get $0, $1", tablet_id, schema_hash));
    }

    std::string json_template = R"({
        "status" : "Success",
        "run_status" : $0,
        "msg" : "$1",
        "tablet_id" : $2,
        "schema_hash" : $3,
        "compact_type" : "$4"
})";

    std::string msg = "this tablet_id is not running";
    std::string compaction_type = "";
    bool run_status = 0;

    {
        // use try lock to check this tablet is running cumulative compaction
        MutexLock lock_cumulativie(tablet->get_cumulative_lock(), TRY_LOCK);
        if (!lock_cumulativie.own_lock()) {
            msg = "this tablet_id is running";
            compaction_type = "cumulative";
            run_status = 1;
            *json_result = strings::Substitute(json_template, run_status, msg, tablet_id, schema_hash,
                                            compaction_type);
            return Status::OK();
        }
    }

    {
        // use try lock to check this tablet is running base compaction
        MutexLock lock_base(tablet->get_base_lock(), TRY_LOCK);
        if (!lock_base.own_lock()) {
            msg = "this tablet_id is running";
            compaction_type = "base";
            run_status = 1;
            *json_result = strings::Substitute(json_template, run_status, msg, tablet_id, schema_hash,
                                            compaction_type);
            return Status::OK();
        }
    }
    // not running any compaction
    *json_result = strings::Substitute(json_template, run_status, msg, tablet_id, schema_hash,
                                           compaction_type);
    return Status::OK();
}

OLAPStatus CompactionAction::_execute_compaction_callback(TabletSharedPtr tablet,
                                                    const std::string& compaction_type) {
    OLAPStatus status = OLAP_SUCCESS;
    if (compaction_type == PARAM_COMPACTION_BASE) {
        std::string tracker_label = "base compaction " + std::to_string(syscall(__NR_gettid));
        BaseCompaction base_compaction(tablet, tracker_label, _compaction_mem_tracker);
        OLAPStatus res = base_compaction.compact();
        if (res != OLAP_SUCCESS) {
            if (res != OLAP_ERR_BE_NO_SUITABLE_VERSION) {
                DorisMetrics::instance()->base_compaction_request_failed->increment(1);
                LOG(WARNING) << "failed to init base compaction. res=" << res
                            << ", table=" << tablet->full_name();
            }
        }
        status = res;
    } else if (compaction_type == PARAM_COMPACTION_CUMULATIVE) {
        std::string tracker_label = "cumulative compaction " + std::to_string(syscall(__NR_gettid));
        CumulativeCompaction cumulative_compaction(tablet, tracker_label, _compaction_mem_tracker);

        OLAPStatus res = cumulative_compaction.compact();
        if (res != OLAP_SUCCESS) {
            if (res != OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS) {
                DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
                LOG(WARNING) << "failed to do cumulative compaction. res=" << res
                            << ", table=" << tablet->full_name();
            }
        }
        status = res;
    }
    
    LOG(INFO) << "Manual compaction task finish, status = " << status;
    std::lock_guard<std::mutex> lock(_compaction_running_mutex);
    _is_compaction_running = false;

    return status;
}

void CompactionAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());

    if (_type == CompactionActionType::SHOW_INFO) {
        std::string json_result;
        Status st = _handle_show_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, to_json(st));
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else if (_type == CompactionActionType::RUN_COMPACTION) {
        std::string json_result;
        Status st = _handle_run_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, to_json(st));
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else {
        std::string json_result;
        Status st = _handle_run_status_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, to_json(st));
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    }

}
} // end namespace doris
