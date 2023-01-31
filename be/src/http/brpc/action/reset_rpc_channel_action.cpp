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
#include "reset_rpc_channel_action.h"

#include <gen_cpp/internal_service.pb.h>

#include "olap/storage_engine.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"

namespace doris {

ResetRpcChannelHandler::ResetRpcChannelHandler(ExecEnv* exec_env)
        : BaseHttpHandler("reset_rpc_channel", exec_env) {}

void ResetRpcChannelHandler::handle_sync(brpc::Controller* cntl) {
    std::string endpoints = *get_param(cntl, "endpoints");
    if (iequal(endpoints, "all")) {
        int size = get_exec_env()->brpc_internal_client_cache()->size();
        if (size > 0) {
            std::vector<std::string> endpoints;
            get_exec_env()->brpc_internal_client_cache()->get_all(&endpoints);
            get_exec_env()->brpc_internal_client_cache()->clear();
            on_succ(cntl, fmt::format("reseted: {0}", join(endpoints, ",")));
            return;
        } else {
            on_succ(cntl, "no cached channel.");
            return;
        }
    } else {
        std::vector<std::string> reseted;
        for (const std::string& endpoint : split(endpoints, ",")) {
            if (!get_exec_env()->brpc_internal_client_cache()->exist(endpoint)) {
                std::string err = fmt::format("{0}: not found.", endpoint);
                LOG(WARNING) << err;
                on_error(cntl, err);
                return;
            }

            if (get_exec_env()->brpc_internal_client_cache()->erase(endpoint)) {
                reseted.push_back(endpoint);
            } else {
                std::string err = fmt::format("{0}: reset failed.", endpoint);
                LOG(WARNING) << err;
                on_error(cntl, err);
                return;
            }
        }
        on_error(cntl, fmt::format("reseted: {0}", join(reseted, ",")));
    }
}
} // namespace doris