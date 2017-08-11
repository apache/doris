// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"

namespace palo {

const static std::string HEADER_JSON = "application/json";

HealthAction::HealthAction(ExecEnv* exec_env) :
        _exec_env(exec_env) {
}

void HealthAction::handle(HttpRequest *req, HttpChannel *channel) {
    std::stringstream ss;
    ss << "{";
    ss << "\"status\": \"OK\",";
    ss << "\"msg\": \"To Be Added\"";
    ss << "}";
    std::string result = ss.str();

    HttpResponse response(HttpStatus::OK, HEADER_JSON, &result);
    channel->send_response(response);
}

} // end namespace palo

