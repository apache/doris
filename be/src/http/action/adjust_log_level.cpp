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

#include <http/action/adjust_log_level.h>

#include "common/logging.h"
#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_request.h"

namespace doris {

// **Note**: If the module_name does not exist in the vlog modules, vlog
// would create corresponding module for it.
int handle_request(HttpRequest* req) {
    auto parse_param = [&req](std::string param) {
        const auto& value = req->param(param);
        if (value.empty()) {
            auto error_msg = fmt::format("parameter {} not specified in url.", param);
            throw std::runtime_error(error_msg);
        }
        return value;
    };
    const auto& module = parse_param("module");
    const auto& level = parse_param("level");
    int new_level = std::stoi(level);
    return google::SetVLOGLevel(module.c_str(), new_level);
}

void AdjustLogLevelAction::handle(HttpRequest* req) {
    try {
        auto old_level = handle_request(req);
        auto msg = fmt::format("adjust log level success, origin level is {}", old_level);
        HttpChannel::send_reply(req, msg);
    } catch (const std::exception& e) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, e.what());
        LOG(WARNING) << "adjust log level failed, error: " << e.what();
        return;
    }
}
} // namespace doris