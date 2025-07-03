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

#include <tuple>

#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "util/string_util.h"

namespace doris {

// **Note**: If the module_name does not exist in the vlog modules, vlog
// would create corresponding module for it.
Result<std::tuple<std::string, int, int>> handle_request(HttpRequest* req) {
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
    auto result = safe_stoi(level, "level");
    if (result.has_value()) {
        return std::make_tuple(module, google::SetVLOGLevel(module.c_str(), result.value()),
                               result.value());
    } else {
        return unexpected(std::move(result).error());
    }
}

void AdjustLogLevelAction::handle(HttpRequest* req) {
    try {
        auto handle_result = handle_request(req);
        if (handle_result.has_value()) {
            auto msg = fmt::format(
                    "adjust vlog of {} from {} to {} succeed", std::get<0>(handle_result.value()),
                    std::get<1>(handle_result.value()), std::get<2>(handle_result.value()));
            LOG(INFO) << msg;
            HttpChannel::send_reply(req, msg);
        } else {
            LOG(WARNING) << "adjust log level failed, error: " << handle_result.error();
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    handle_result.error().to_string_no_stack());
            return;
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "adjust log level failed, error: " << e.what();
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, e.what());
        return;
    }
}
} // namespace doris
