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

#include "service/http/action/config_action.h"

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstdint>
#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/substitute.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "service/http/action/action_constants.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "service/http/utils.h"

namespace doris {

const static std::string PERSIST_PARAM = "persist";
const std::string CONF_ITEM = "conf_item";

namespace {

// Who the caller claims to be, for the config update audit line. The claim is only verified
// when the auth gate is on (config::enable_all_http_auth); with the gate off the request never
// went through authentication, so this is the presented user name and nothing more. It is still
// worth recording: together with the remote address it is everything the request tells us about
// its origin.
//
// Deliberately the two-out-parameter parse_basic_auth: the AuthInfo overload also parses the
// deprecated auth_code header with std::stoll, which throws on a non-numeric value. On the
// gate-off path this would be the first call, so a junk header would turn into an exception
// escaping the handler.
std::string claimed_identity(HttpRequest* req) {
    std::string user;
    std::string passwd;
    if (parse_basic_auth(*req, &user, &passwd) && !user.empty()) {
        return user;
    }
    // A request authenticated by cluster token carries no user name.
    if (!req->header(HttpHeaders::AUTH_TOKEN).empty() || !req->header("token").empty()) {
        return "<token>";
    }
    return "-";
}

} // namespace

void ConfigAction::handle(HttpRequest* req) {
    if (_config_type == ConfigActionType::UPDATE_CONFIG) {
        handle_update_config(req);
    } else if (_config_type == ConfigActionType::SHOW_CONFIG) {
        handle_show_config(req);
    }
}

void ConfigAction::handle_show_config(HttpRequest* req) {
    std::vector<std::vector<std::string>> config_info = config::get_config_info();

    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);

    const std::string& conf_item = req->param(CONF_ITEM);

    writer.StartArray();
    for (const auto& _config : config_info) {
        if (!conf_item.empty()) {
            if (_config[0] == conf_item) {
                writer.StartArray();
                for (const std::string& config_filed : _config) {
                    writer.String(config_filed.c_str());
                }
                writer.EndArray();
                break;
            }
        } else {
            writer.StartArray();
            for (const std::string& config_filed : _config) {
                writer.String(config_filed.c_str());
            }
            writer.EndArray();
        }
    }

    writer.EndArray();
    HttpChannel::send_reply(req, str_buf.GetString());
}

void ConfigAction::handle_update_config(HttpRequest* req) {
    LOG(INFO) << req->debug_string();

    Status s;
    std::string msg;
    rapidjson::Document root;
    root.SetObject();
    rapidjson::Document results;
    results.SetArray();
    if (req->params()->size() < 1) {
        s = Status::InvalidArgument("");
        msg = "Now only support to set a single config once, via 'config_name=new_value', and with "
              "an optional parameter 'persist'.";
    } else {
        bool need_persist = false;
        auto persist_param = req->params()->find(PERSIST_PARAM);
        if (persist_param != req->params()->end() && persist_param->second == "true") {
            need_persist = true;
        }
        const std::string identity = claimed_identity(req);
        const char* remote = req->remote_host();
        for (const auto& [key, value] : *req->params()) {
            if (key == PERSIST_PARAM) {
                continue;
            }
            // Read the old value before the update, so the audit line can report the change and
            // not just its result. Both ends go through the mask: a secret is as much a secret
            // on the way out as on the way in.
            const std::string old_value =
                    config::mask_config_value(key, config::get_config_value(key));
            const std::string new_value = config::mask_config_value(key, value);
            s = config::set_config(key, value, need_persist);
            // One audit line per config, whether it took effect or not, answering who changed
            // what from what to what, and whether it survives a restart. A rejected update stays
            // at WARNING, which is the level it was reported at before.
            const std::string audit = absl::Substitute(
                    "update_config: remote=$0, user=$1, config=$2, old=$3, new=$4, persist=$5, "
                    "result=$6",
                    remote == nullptr ? "-" : remote, identity, key, old_value, new_value,
                    need_persist, s.ok() ? std::string("OK") : s.to_string());
            if (s.ok()) {
                LOG(INFO) << audit;
            } else {
                LOG(WARNING) << audit;
                msg = absl::Substitute("set $0=$1 failed, reason: $2.", key, new_value,
                                       s.to_string());
            }
            std::string status(s.ok() ? "OK" : "BAD");
            rapidjson::Value result;
            result.SetObject();
            result.AddMember("config_name",
                             rapidjson::Value(key.c_str(), cast_set<uint32_t>(key.size()),
                                              results.GetAllocator()),
                             results.GetAllocator());
            result.AddMember("status",
                             rapidjson::Value(status.c_str(), cast_set<uint32_t>(status.size()),
                                              results.GetAllocator()),
                             results.GetAllocator());
            result.AddMember("msg",
                             rapidjson::Value(msg.c_str(), cast_set<uint32_t>(msg.size()),
                                              results.GetAllocator()),
                             results.GetAllocator());
            results.PushBack(result, results.GetAllocator());
        }
    }

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    results.Accept(writer);

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

} // namespace doris
