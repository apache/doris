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

#include "http/action/config_action.h"

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";
const static std::string PERSIST_PARAM = "persist";
const std::string CONF_ITEM = "conf_item";

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
        if (req->params()->find(PERSIST_PARAM)->second.compare("true") == 0) {
            need_persist = true;
        }
        for (const auto& [key, value] : *req->params()) {
            if (key == PERSIST_PARAM) {
                continue;
            }
            s = config::set_config(key, value, need_persist);
            if (s.ok()) {
                LOG(INFO) << "set_config " << key << "=" << value
                          << " success. persist: " << need_persist;
            } else {
                LOG(WARNING) << "set_config " << key << "=" << value << " failed";
                msg = strings::Substitute("set $0=$1 failed, reason: $2.", key, value,
                                          s.to_string());
            }
            std::string status(s.ok() ? "OK" : "BAD");
            rapidjson::Value result;
            result.SetObject();
            result.AddMember("config_name",
                             rapidjson::Value(key.c_str(), key.size(), results.GetAllocator()),
                             results.GetAllocator());
            result.AddMember(
                    "status",
                    rapidjson::Value(status.c_str(), status.size(), results.GetAllocator()),
                    results.GetAllocator());
            result.AddMember("msg",
                             rapidjson::Value(msg.c_str(), msg.size(), results.GetAllocator()),
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
