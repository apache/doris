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
#include "config_action.h"

#include <brpc/http_method.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>
#include <vector>

#include "gutil/strings/substitute.h"
namespace doris {
ConfigHandler::ConfigHandler() : BaseHttpHandler("config") {}

const static std::string& SHOW_TYPE = "show_config";
const static std::string& UPDATE_TYEP = "update_config";
const static std::string& PERSIST_PARAM = "persist";

void ConfigHandler::handle_sync(brpc::Controller* cntl) {
    vector<std::string> path_array;
    get_path_array(cntl, path_array);
    if (path_array.size() != 2) {
        on_bad_req(cntl, "inproper path variable");
        return;
    }
    const std::string& type = path_array[1];
    if (type == UPDATE_TYEP) {
        handle_update_config(cntl);
    } else if (type == SHOW_TYPE) {
        handle_show_config(cntl);
    }
}

bool ConfigHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET || method == brpc::HTTP_METHOD_POST;
}

void ConfigHandler::handle_update_config(brpc::Controller* cntl) {
    Status s;
    std::string msg;
    // We only support set one config at a time, and along with a optional param "persist".
    // So the number of query params should at most be 2.
    const size_t req_params = get_uri(cntl).QueryCount();
    if (req_params > 2 || req_params < 1) {
        s = Status::InvalidArgument("");
        msg = "Now only support to set a single config once, via 'config_name=new_value', and with "
              "an optional parameter 'persist'.";
    } else {
        if (req_params == 1) {
            const std::string& config = get_uri(cntl).QueryBegin()->first;
            const std::string& new_value = get_uri(cntl).QueryBegin()->second;
            s = config::set_config(config, new_value, false);
            if (s.ok()) {
                LOG(INFO) << "set_config " << config << "=" << new_value << " success";
            } else {
                LOG(WARNING) << "set_config " << config << "=" << new_value << " failed";
                msg = strings::Substitute("set $0=$1 failed, reason: $2", config, new_value,
                                          s.to_string());
            }
        } else if (req_params == 2) {
            const std::string* persist_param = get_param(cntl, PERSIST_PARAM);
            if (persist_param == nullptr) {
                return;
            }
            if (*persist_param == get_uri(cntl).QueryEnd()->second) {
                s = Status::InvalidArgument("");
                msg = "Now only support to set a single config once, via 'config_name=new_value', "
                      "and with an optional parameter 'persist'.";
            } else {
                bool need_persist = false;
                if (*persist_param == "true") {
                    need_persist = true;
                }
                auto uri = get_uri(cntl);
                for (auto iter = uri.QueryBegin(); iter != uri.QueryEnd(); ++iter) {
                    if (iter->first.compare(PERSIST_PARAM) == 0) {
                        continue;
                    }
                    s = config::set_config(iter->first, iter->second, need_persist);
                    if (s.ok()) {
                        LOG(INFO) << "set_config " << iter->first << "=" << iter->second
                                  << " success. persist: " << need_persist;
                    } else {
                        LOG(WARNING)
                                << "set_config " << iter->first << "=" << iter->second << " failed";
                        msg = strings::Substitute("set $0=$1 failed, reason: $2", iter->first,
                                                  iter->second, s.to_string());
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

    on_succ_json(cntl, strbuf.GetString());
}

void ConfigHandler::handle_show_config(brpc::Controller* cntl) {
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
    on_succ_json(cntl, str_buf.GetString());
}
} // namespace doris
