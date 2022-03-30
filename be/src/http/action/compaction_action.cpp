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

#include <sys/syscall.h>

#include <sstream>
#include <string>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "util/json_util.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

bool CompactionAction::_is_compaction_running = false;
std::mutex CompactionAction::_compaction_running_mutex;

Status CompactionAction::_check_param(HttpRequest* req, uint64_t* tablet_id,
                                      uint32_t* schema_hash) {
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    std::string req_schema_hash = req->param(TABLET_SCHEMA_HASH_KEY);
    if (req_tablet_id == "" && req_schema_hash == "") {
        return Status::OK();
    }

    try {
        *tablet_id = std::stoull(req_tablet_id);
        *schema_hash = std::stoul(req_schema_hash);
    } catch (const std::exception& e) {
        return Status::InternalError(
            strings::Substitute("convert tablet_id and schema_hash failed, $0", e.what()));
    }

    return Status::OK();
}

// for viewing the compaction status
Status CompactionAction::_handle_show_compaction(HttpRequest* req, std::string* json_result) {
    uint64_t tablet_id = 0;
    uint32_t schema_hash = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, &schema_hash),
                                   "check param failed");

    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
    if (tablet == nullptr) {
        return Status::NotFound(
            strings::Substitute("Tablet not found. tablet_id=$0, schema_hash=$1",
                                tablet_id, schema_hash));
    }

    tablet->get_compaction_status(json_result);
    return Status::OK();
}

Status CompactionAction::_handle_run_compaction(HttpRequest* req, std::string* json_result) {
    // 1. param check
    // check req_tablet_id and req_schema_hash is not empty
    uint64_t tablet_id = 0;
    uint32_t schema_hash = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, &schema_hash),
                                   "check param failed");

    // check compaction_type equals 'base' or 'cumulative'
    std::string compaction_type = req->param(PARAM_COMPACTION_TYPE);
    if (compaction_type != PARAM_COMPACTION_BASE &&
        compaction_type != PARAM_COMPACTION_CUMULATIVE) {
        return Status::NotSupported(
            strings::Substitute("The compaction type '$0' is not supported", compaction_type));
    }

    // 2. fetch the tablet by tablet_id and schema_hash
    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
    if (tablet == nullptr) {
        return Status::NotFound(
            strings::Substitute("Tablet not found. tablet_id=$0, schema_hash=$1",
                                tablet_id, schema_hash));
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
        LOG(INFO) << "Manual compaction task is timeout for waiting "
                  << (status == std::future_status::timeout);
    }

    LOG(INFO) << "Manual compaction task is successfully triggered";
    *json_result =
            "{\"status\": \"Success\", \"msg\": \"compaction task is successfully triggered.\"}";

    return Status::OK();
}

Status CompactionAction::_handle_run_status_compaction(HttpRequest* req, std::string* json_result) {
    uint64_t tablet_id = 0;
    uint32_t schema_hash = 0;

    // check req_tablet_id and req_schema_hash is not empty
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, &schema_hash),
                                   "check param failed");


    if (tablet_id == 0) {
        // overall compaction status
        RETURN_IF_ERROR(StorageEngine::instance()->get_compaction_status_json(json_result));
        return Status::OK();
    } else {
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
    
        std::string msg = "compaction task for this tablet is not running";
        std::string compaction_type = "";
        bool run_status = 0;
    
        {
            // use try lock to check this tablet is running cumulative compaction
            std::unique_lock<std::mutex> lock_cumulative(tablet->get_cumulative_compaction_lock(), std::try_to_lock);
            if (!lock_cumulative.owns_lock()) {
                msg = "compaction task for this tablet is running";
                compaction_type = "cumulative";
                run_status = 1;
                *json_result = strings::Substitute(json_template, run_status, msg, tablet_id,
                                                   schema_hash, compaction_type);
                return Status::OK();
            }
        }
    
        {
            // use try lock to check this tablet is running base compaction
            std::unique_lock<std::mutex> lock_base(tablet->get_base_compaction_lock(), std::try_to_lock);
            if (!lock_base.owns_lock()) {
                msg = "compaction task for this tablet is running";
                compaction_type = "base";
                run_status = 1;
                *json_result = strings::Substitute(json_template, run_status, msg, tablet_id,
                                                   schema_hash, compaction_type);
                return Status::OK();
            }
        }
        // not running any compaction
        *json_result = strings::Substitute(json_template, run_status, msg, tablet_id, schema_hash,
                                           compaction_type);
        return Status::OK();
    }
}

OLAPStatus CompactionAction::_execute_compaction_callback(TabletSharedPtr tablet,
                                                          const std::string& compaction_type) {
    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            _create_cumulative_compaction_policy();
    if (tablet->get_cumulative_compaction_policy() == nullptr ||
        tablet->get_cumulative_compaction_policy()->name() != cumulative_compaction_policy->name()) {
        tablet->set_cumulative_compaction_policy(cumulative_compaction_policy);
    }

    OLAPStatus status = OLAP_SUCCESS;
    if (compaction_type == PARAM_COMPACTION_BASE) {
        BaseCompaction base_compaction(tablet);
        OLAPStatus res = base_compaction.compact();
        if (res != OLAP_SUCCESS && res != OLAP_ERR_BE_NO_SUITABLE_VERSION) {
            DorisMetrics::instance()->base_compaction_request_failed->increment(1);
            LOG(WARNING) << "failed to init base compaction. res=" << res
                         << ", table=" << tablet->full_name();
        }
        status = res;
    } else if (compaction_type == PARAM_COMPACTION_CUMULATIVE) {
        CumulativeCompaction cumulative_compaction(tablet);
        OLAPStatus res = cumulative_compaction.compact();
        if (res != OLAP_SUCCESS && res != OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION) {
            DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
            LOG(WARNING) << "failed to do cumulative compaction. res=" << res
                         << ", table=" << tablet->full_name();
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

std::shared_ptr<CumulativeCompactionPolicy> CompactionAction::_create_cumulative_compaction_policy() {
    std::string current_policy = "";
    {
        std::lock_guard<std::mutex> lock(*config::get_mutable_string_config_lock());
        current_policy = config::cumulative_compaction_policy;
    }
    boost::to_upper(current_policy);

    if (current_policy == CUMULATIVE_SIZE_BASED_POLICY) {
        // check size_based cumulative compaction config
        StorageEngine::instance()->check_cumulative_compaction_config();
    }

    return CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(current_policy);
}
} // end namespace doris
