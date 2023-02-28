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

#include "health_action.h"

namespace doris {
HealthHandler::HealthHandler() : BaseHttpHandler("health") {}

void HealthHandler::handle_sync(brpc::Controller* cntl) {
    std::stringstream ss;
    ss << "{";
    ss << "\"status\": \"OK\",";
    ss << "\"msg\": \"To Be Added\"";
    ss << "}";
    std::string result = ss.str();

    on_succ_json(cntl, result);
}

bool HealthHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET;
}

} // namespace doris