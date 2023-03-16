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
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/configbase.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

const static std::string PERSIST_PARAM = "persist";

void ConfigAction::handle(HttpRequest* req) {
    if (_type == ConfigActionType::UPDATE_CONFIG) {
        handle_update_config(req);
    } else if (_type == ConfigActionType::SHOW_CONFIG) {
        handle_show_config(req);
    }
}

void ConfigAction::handle_show_config(HttpRequest* req) {
    std::vector<std::vector<std::string>> config_info = config::get_config_info();

    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);

    writer.StartArray();
    for (const auto& _config : config_info) {
        writer.StartArray();
        for (const std::string& config_filed : _config) {
            writer.String(config_filed.c_str());
        }
        writer.EndArray();
    }

    writer.EndArray();
    HttpChannel::send_reply(req, str_buf.GetString());
}

void ConfigAction::handle_update_config(HttpRequest* req) {
    LOG(INFO) << req->debug_string();

    Status s;
    std::string msg;
    // We only support set one config at a time, and along with a optional param "persist".
    // So the number of query params should at most be 2.
    if (req->params()->size() > 2 || req->params()->size() < 1) {
        s = Status::InvalidArgument("");
        msg = "Now only support to set a single config once, via 'config_name=new_value', and with "
              "an optional parameter 'persist'.";
    } else {
        if (req->params()->size() == 1) {
            const std::string& config = req->params()->begin()->first;
            const std::string& new_value = req->params()->begin()->second;
            s = config::set_config(config, new_value, false);
            if (s.ok()) {
                LOG(INFO) << "set_config " << config << "=" << new_value << " success";
            } else {
                LOG(WARNING) << "set_config " << config << "=" << new_value << " failed";
                msg = strings::Substitute("set $0=$1 failed, reason: $2", config, new_value,
                                          s.to_string());
            }
        } else if (req->params()->size() == 2) {
            if (req->params()->find(PERSIST_PARAM) == req->params()->end()) {
                s = Status::InvalidArgument("");
                msg = "Now only support to set a single config once, via 'config_name=new_value', "
                      "and with an optional parameter 'persist'.";
            } else {
                bool need_persist = false;
                if (req->params()->find(PERSIST_PARAM)->second.compare("true") == 0) {
                    need_persist = true;
                }
                for (auto const& iter : *(req->params())) {
                    if (iter.first.compare(PERSIST_PARAM) == 0) {
                        continue;
                    }
                    s = config::set_config(iter.first, iter.second, need_persist);
                    if (s.ok()) {
                        LOG(INFO) << "set_config " << iter.first << "=" << iter.second
                                  << " success. persist: " << need_persist;
                    } else {
                        LOG(WARNING)
                                << "set_config " << iter.first << "=" << iter.second << " failed";
                        msg = strings::Substitute("set $0=$1 failed, reason: $2", iter.first,
                                                  iter.second, s.to_string());
                    }
                }
            }
        }
    }

    std::string status(s.ok() ? "OK" : "BAD");
    rapidjson::Document root;
    root.SetObject();
    root.AddMember("status", rapidjson::Value(status.c_str(), status.size()), root.GetAllocator());
    root.AddMember("msg", rapidjson::Value(msg.c_str(), msg.size()), root.GetAllocator());
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

} // namespace doris
