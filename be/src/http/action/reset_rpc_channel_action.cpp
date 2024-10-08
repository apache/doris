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

#include "http/action/reset_rpc_channel_action.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <string>
#include <vector>

#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"
#include "util/brpc_client_cache.h"
#include "util/string_util.h"

namespace doris {
ResetRPCChannelAction::ResetRPCChannelAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                             TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type) {}
void ResetRPCChannelAction::handle(HttpRequest* req) {
    std::string endpoints = req->param("endpoints");
    if (iequal(endpoints, "all")) {
        int size = _exec_env->brpc_internal_client_cache()->size();
        if (size > 0) {
            std::vector<std::string> endpoints;
            _exec_env->brpc_internal_client_cache()->get_all(&endpoints);
            _exec_env->brpc_internal_client_cache()->clear();
            HttpChannel::send_reply(req, HttpStatus::OK,
                                    fmt::format("reseted: {0}", join(endpoints, ",")));
            return;
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, "no cached channel.");
            return;
        }
    } else {
        std::vector<std::string> reseted;
        for (const std::string& endpoint : split(endpoints, ",")) {
            if (!_exec_env->brpc_internal_client_cache()->exist(endpoint)) {
                std::string err = fmt::format("{0}: not found.", endpoint);
                LOG(WARNING) << err;
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, err);
                return;
            }

            if (_exec_env->brpc_internal_client_cache()->erase(endpoint)) {
                reseted.push_back(endpoint);
            } else {
                std::string err = fmt::format("{0}: reset failed.", endpoint);
                LOG(WARNING) << err;
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, err);
                return;
            }
        }
        HttpChannel::send_reply(req, HttpStatus::OK,
                                fmt::format("reseted: {0}", join(reseted, ",")));
        return;
    }
}

} // namespace doris
