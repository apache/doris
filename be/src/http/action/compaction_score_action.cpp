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
#include "http/action/compaction_score_action.h"

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_manager.h"

namespace doris {

constexpr std::string_view TABLET_ID = "tablet_id";
constexpr std::string_view COMPACTION_SCORE = "compaction_score";

CompactionScoreAction::CompactionScoreAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                             TPrivilegeType::type type,
                                             StorageEngine& storage_engine)
        : HttpHandlerWithAuth(exec_env, hier, type), _storage_engine(storage_engine) {}

static rapidjson::Value jsonfy_tablet_compaction_score(
        const TabletSharedPtr& tablet, rapidjson::MemoryPoolAllocator<>& allocator) {
    rapidjson::Value node;
    node.SetObject();

    rapidjson::Value tablet_id_key;
    tablet_id_key.SetString(TABLET_ID.data(), TABLET_ID.length(), allocator);
    rapidjson::Value tablet_id_val;
    auto tablet_id_str = std::to_string(tablet->tablet_id());
    tablet_id_val.SetString(tablet_id_str.c_str(), tablet_id_str.length(), allocator);

    rapidjson::Value score_key;
    score_key.SetString(COMPACTION_SCORE.data(), COMPACTION_SCORE.size());
    rapidjson::Value score_val;
    auto score = tablet->get_real_compaction_score();
    auto score_str = std::to_string(score);
    score_val.SetString(score_str.c_str(), score_str.length(), allocator);
    node.AddMember(score_key, score_val, allocator);

    node.AddMember(tablet_id_key, tablet_id_val, allocator);
    return node;
}

void CompactionScoreAction::handle(HttpRequest* req) {
    std::string result;
    if (auto st = _handle(req, &result); !st) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_json());
    }
    HttpChannel::send_reply(req, HttpStatus::OK, result);
}

Status CompactionScoreAction::_handle(HttpRequest* req, std::string* result) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HttpHeaders::JsonType.data());
    auto tablet_id_param = req->param(TABLET_ID.data());
    rapidjson::Document root;
    if (tablet_id_param.empty()) {
        // fetch comapction scores from all tablets
        // [{tablet_id: xxx, base_compaction_score: xxx, cumu_compaction_score: xxx}, ...]
        auto tablets = _storage_engine.tablet_manager()->get_all_tablet();
        root.SetArray();
        auto& allocator = root.GetAllocator();
        for (const auto& tablet : tablets) {
            root.PushBack(jsonfy_tablet_compaction_score(tablet, allocator), allocator);
        }
    } else {
        // {tablet_id: xxx, base_compaction_score: xxx, cumu_compaction_score: xxx}
        int64_t tablet_id;
        try {
            tablet_id = std::stoll(tablet_id_param);
        } catch (const std::exception& e) {
            LOG(WARNING) << "convert failed:" << e.what();
            return Status::InvalidArgument("invalid argument: tablet_id={}", tablet_id_param);
        }
        auto base_tablet = DORIS_TRY(_storage_engine.get_tablet(tablet_id));
        auto tablet = std::static_pointer_cast<Tablet>(base_tablet);
        root.SetObject();
        auto val = jsonfy_tablet_compaction_score(tablet, root.GetAllocator());
        root.Swap(val);
    }
    rapidjson::StringBuffer str_buf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(str_buf);
    root.Accept(writer);
    *result = str_buf.GetString();
    return Status::OK();
}

} // namespace doris
