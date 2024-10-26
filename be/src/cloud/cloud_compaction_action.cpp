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

#include "cloud/cloud_compaction_action.h"

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_compaction_action.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_full_compaction.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/full_compaction.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"

namespace doris {
using namespace ErrorCode;

namespace {}

const static std::string HEADER_JSON = "application/json";

CloudCompactionAction::CloudCompactionAction(CompactionActionType ctype, ExecEnv* exec_env,
                                             CloudStorageEngine& engine, TPrivilegeHier::type hier,
                                             TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype), _engine(engine), _compaction_type(ctype) {}

/// check param and fetch tablet_id & table_id from req
static Status _check_param(HttpRequest* req, uint64_t* tablet_id, uint64_t* table_id) {
    // req tablet id and table id, we have to set only one of them.
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    std::string req_table_id = req->param(TABLE_ID_KEY);
    if (req_tablet_id == "") {
        if (req_table_id == "") {
            // both tablet id and table id are empty, return error.
            return Status::InternalError(
                    "tablet id and table id can not be empty at the same time!");
        } else {
            try {
                *table_id = std::stoull(req_table_id);
            } catch (const std::exception& e) {
                return Status::InternalError("convert table_id failed, {}", e.what());
            }
            return Status::OK();
        }
    } else {
        if (req_table_id == "") {
            try {
                *tablet_id = std::stoull(req_tablet_id);
            } catch (const std::exception& e) {
                return Status::InternalError("convert tablet_id failed, {}", e.what());
            }
            return Status::OK();
        } else {
            // both tablet id and table id are not empty, return err.
            return Status::InternalError("tablet id and table id can not be set at the same time!");
        }
    }
}

/// retrieve specific id from req
static Status _check_param(HttpRequest* req, uint64_t* id_param, const std::string param_name) {
    const auto& req_id_param = req->param(param_name);
    if (!req_id_param.empty()) {
        try {
            *id_param = std::stoull(req_id_param);
        } catch (const std::exception& e) {
            return Status::InternalError("convert {} failed, {}", param_name, e.what());
        }
    }

    return Status::OK();
}

// for viewing the compaction status
Status CloudCompactionAction::_handle_show_compaction(HttpRequest* req, std::string* json_result) {
    uint64_t tablet_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, TABLET_ID_KEY),
                                   "check param failed");
    if (tablet_id == 0) {
        return Status::InternalError("check param failed: missing tablet_id");
    }

    LOG(INFO) << "begin to handle show compaction, tablet id: " << tablet_id;

    //TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(tablet_id);
    CloudTabletSPtr tablet = DORIS_TRY(_engine.tablet_mgr().get_tablet(tablet_id));
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    }

    tablet->get_compaction_status(json_result);
    LOG(INFO) << "finished to handle show compaction, tablet id: " << tablet_id;
    return Status::OK();
}

Status CloudCompactionAction::_handle_run_compaction(HttpRequest* req, std::string* json_result) {
    // 1. param check
    // check req_tablet_id or req_table_id is not empty and can not be set together.
    uint64_t tablet_id = 0;
    uint64_t table_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, &table_id), "check param failed");
    LOG(INFO) << "begin to handle run compaction, tablet id: " << tablet_id
              << " table id: " << table_id;

    // check compaction_type equals 'base' or 'cumulative'
    auto& compaction_type = req->param(PARAM_COMPACTION_TYPE);
    if (compaction_type != PARAM_COMPACTION_BASE &&
        compaction_type != PARAM_COMPACTION_CUMULATIVE &&
        compaction_type != PARAM_COMPACTION_FULL) {
        return Status::NotSupported("The compaction type '{}' is not supported", compaction_type);
    }

    CloudTabletSPtr tablet = DORIS_TRY(_engine.tablet_mgr().get_tablet(tablet_id));
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    }

    LOG(INFO) << "manual submit compaction task, tablet id: " << tablet_id
              << " table id: " << table_id;
    // 3. submit compaction task
    RETURN_IF_ERROR(_engine.submit_compaction_task(
            tablet, compaction_type == PARAM_COMPACTION_BASE ? CompactionType::BASE_COMPACTION
                    : compaction_type == PARAM_COMPACTION_CUMULATIVE
                            ? CompactionType::CUMULATIVE_COMPACTION
                            : CompactionType::FULL_COMPACTION));

    LOG(INFO) << "Manual compaction task is successfully triggered, tablet id: " << tablet_id
              << " table id: " << table_id;
    *json_result =
            "{\"status\": \"Success\", \"msg\": \"compaction task is successfully triggered. Table "
            "id: " +
            std::to_string(table_id) + ". Tablet id: " + std::to_string(tablet_id) + "\"}";
    return Status::OK();
}

Status CloudCompactionAction::_handle_run_status_compaction(HttpRequest* req,
                                                            std::string* json_result) {
    uint64_t tablet_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, TABLET_ID_KEY),
                                   "check param failed");
    LOG(INFO) << "begin to handle run status compaction, tablet id: " << tablet_id;

    if (tablet_id == 0) {
        // overall compaction status
        RETURN_IF_ERROR(_engine.get_compaction_status_json(json_result));
    } else {
        std::string json_template = R"({
            "status" : "Success",
            "run_status" : $0,
            "msg" : "$1",
            "tablet_id" : $2,
            "compact_type" : "$3"
        })";

        std::string msg = "compaction task for this tablet is not running";
        std::string compaction_type;
        bool run_status = false;

        if (_engine.has_cumu_compaction(tablet_id)) {
            msg = "compaction task for this tablet is running";
            compaction_type = "cumulative";
            run_status = true;
            *json_result =
                    strings::Substitute(json_template, run_status, msg, tablet_id, compaction_type);
            return Status::OK();
        }

        if (_engine.has_base_compaction(tablet_id)) {
            msg = "compaction task for this tablet is running";
            compaction_type = "base";
            run_status = true;
            *json_result =
                    strings::Substitute(json_template, run_status, msg, tablet_id, compaction_type);
            return Status::OK();
        }

        if (_engine.has_full_compaction(tablet_id)) {
            msg = "compaction task for this tablet is running";
            compaction_type = "full";
            run_status = true;
            *json_result =
                    strings::Substitute(json_template, run_status, msg, tablet_id, compaction_type);
            return Status::OK();
        }
        // not running any compaction
        *json_result =
                strings::Substitute(json_template, run_status, msg, tablet_id, compaction_type);
    }
    LOG(INFO) << "finished to handle run status compaction, tablet id: " << tablet_id;
    return Status::OK();
}

void CloudCompactionAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());

    if (_compaction_type == CompactionActionType::SHOW_INFO) {
        std::string json_result;
        Status st = _handle_show_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else if (_compaction_type == CompactionActionType::RUN_COMPACTION) {
        std::string json_result;
        Status st = _handle_run_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else {
        std::string json_result;
        Status st = _handle_run_status_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    }
}

} // end namespace doris
