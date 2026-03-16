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

#include "service/http/action/compaction_profile_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <exception>
#include <string>

#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "storage/compaction/compaction_profile_mgr.h"

namespace doris {

CompactionProfileAction::CompactionProfileAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                                  TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype) {}

void CompactionProfileAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");

    int64_t tablet_id = 0;
    int64_t top_n = 0;

    const auto& tablet_id_str = req->param("tablet_id");
    if (!tablet_id_str.empty()) {
        try {
            tablet_id = std::stoll(tablet_id_str);
        } catch (const std::exception& e) {
            HttpChannel::send_reply(
                    req, HttpStatus::BAD_REQUEST,
                    R"({"status": "Failed", "msg": "invalid tablet_id parameter"})");
            return;
        }
    }

    const auto& top_n_str = req->param("top_n");
    if (!top_n_str.empty()) {
        try {
            top_n = std::stoll(top_n_str);
        } catch (const std::exception& e) {
            HttpChannel::send_reply(
                    req, HttpStatus::BAD_REQUEST,
                    R"({"status": "Failed", "msg": "invalid top_n parameter"})");
            return;
        }
    }

    auto records = CompactionProfileManager::instance()->get_records(tablet_id, top_n);

    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();

    root.AddMember("status", "Success", allocator);

    rapidjson::Value profiles(rapidjson::kArrayType);
    for (const auto& record : records) {
        rapidjson::Value obj;
        record.to_json(obj, allocator);
        profiles.PushBack(obj, allocator);
    }
    root.AddMember("compaction_profiles", profiles, allocator);

    rapidjson::StringBuffer str_buf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(str_buf);
    root.Accept(writer);

    HttpChannel::send_reply(req, HttpStatus::OK, str_buf.GetString());
}

} // namespace doris
