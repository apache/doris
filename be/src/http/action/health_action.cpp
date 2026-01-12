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

#include "http/action/health_action.h"

#include <sstream>
#include <string>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

void HealthAction::handle(HttpRequest* req) {
    std::string status;
    std::string msg;
    HttpStatus st;
    // always return HttpStatus::OK
    // because in k8s, we don't want the pod to be removed
    // from service during shutdown
    if (!doris::k_is_server_ready) {
        status = "Server is not available";
        msg = "Server is not ready";
        st = HttpStatus::OK;
    } else if (doris::k_doris_exit) {
        status = "Server is not available";
        msg = "Server is shutting down";
        st = HttpStatus::OK;
    } else {
        status = "OK";
        msg = "OK";
        st = HttpStatus::OK;
    }

    std::stringstream ss;
    ss << "{";
    ss << "\"status\": \"" << status << "\",";
    ss << "\"msg\": \"" << msg << "\"";
    ss << "}";
    std::string result = ss.str();

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, st, result);
}

} // end namespace doris
