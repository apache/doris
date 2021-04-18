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

#include "http/monitor_action.h"

#include <sstream>

#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "http/rest_monitor_iface.h"

namespace doris {

const std::string MODULE_KEY = "module";

MonitorAction::MonitorAction() {}

void MonitorAction::register_module(const std::string& name, RestMonitorIface* module) {
    _module_by_name.insert(std::make_pair(name, module));
}

void MonitorAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();
    const std::string& module = req->param(MODULE_KEY);
    if (module.empty()) {
        std::string err_msg = "No module params\n";
#if 0
        HttpResponse response(HttpStatus::OK, &err_msg);
        channel->send_response(response);
#endif
        HttpChannel::send_reply(req, HttpStatus::OK, err_msg);
        return;
    }
    if (_module_by_name.find(module) == _module_by_name.end()) {
        std::string err_msg = "Unknown module(";
        err_msg += module + ")\n";
#if 0
        HttpResponse response(HttpStatus::OK, &err_msg);
        channel->send_response(response);
#endif
        HttpChannel::send_reply(req, HttpStatus::OK, err_msg);
        return;
    }
    std::stringstream ss;
    _module_by_name[module]->debug(ss);
    std::string str = ss.str();
#if 0
    HttpResponse response(HttpStatus::OK, &str);
    channel->send_response(response);
#endif
    HttpChannel::send_reply(req, HttpStatus::OK, str);
}

} // namespace doris
