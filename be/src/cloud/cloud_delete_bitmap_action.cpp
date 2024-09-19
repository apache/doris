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

#include "cloud_delete_bitmap_action.h"

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <chrono> // IWYU pragma: keep
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"

namespace doris {
using namespace ErrorCode;

namespace {

constexpr std::string_view HEADER_JSON = "application/json";

} // namespace

CloudDeleteBitmapAction::CloudDeleteBitmapAction(DeleteBitmapActionType ctype, ExecEnv* exec_env,
                                                 CloudStorageEngine& engine,
                                                 TPrivilegeHier::type hier,
                                                 TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype),
          _engine(engine),
          _delete_bitmap_action_type(ctype) {}

static Status _check_param(HttpRequest* req, uint64_t* tablet_id) {
    const auto& req_tablet_id = req->param(TABLET_ID_KEY);
    if (req_tablet_id.empty()) {
        return Status::InternalError("tablet id is empty!");
    }
    try {
        *tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        return Status::InternalError("convert tablet_id failed, {}", e.what());
    }
    return Status::OK();
}

Status CloudDeleteBitmapAction::_handle_show_delete_bitmap_count(HttpRequest* req,
                                                                 std::string* json_result) {
    uint64_t tablet_id = 0;
    // check & retrieve tablet_id from req if it contains
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id), "check param failed");
    if (tablet_id == 0) {
        return Status::InternalError("check param failed: missing tablet_id");
    }

    CloudTabletSPtr tablet = DORIS_TRY(_engine.tablet_mgr().get_tablet(tablet_id));
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    }

    auto count = tablet->tablet_meta()->delete_bitmap().get_delete_bitmap_count();
    auto cardinality = tablet->tablet_meta()->delete_bitmap().cardinality();
    auto size = tablet->tablet_meta()->delete_bitmap().get_size();

    rapidjson::Document root;
    root.SetObject();
    root.AddMember("delete_bitmap_count", count, root.GetAllocator());
    root.AddMember("cardinality", cardinality, root.GetAllocator());
    root.AddMember("size", size, root.GetAllocator());

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());

    return Status::OK();
}

void CloudDeleteBitmapAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.data());
    if (_delete_bitmap_action_type == DeleteBitmapActionType::COUNT_INFO) {
        std::string json_result;
        Status st = _handle_show_delete_bitmap_count(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    }
}

} // namespace doris