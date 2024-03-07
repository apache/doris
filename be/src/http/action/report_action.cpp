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

#include "http/action/report_action.h"

#include "common/status.h"
#include "http/http_channel.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

ReportAction::ReportAction(ExecEnv* exec_env, TPrivilegeHier::type hier, TPrivilegeType::type type,
                           const std::string& report_name)
        : HttpHandlerWithAuth(exec_env, hier, type), _report_name(report_name) {}

void ReportAction::handle(HttpRequest* req) {
    if (ExecEnv::GetInstance()->storage_engine().notify_listener(_report_name)) {
        HttpChannel::send_reply(req, HttpStatus::OK, Status::OK().to_json());
    } else {
        HttpChannel::send_reply(
                req, HttpStatus::INTERNAL_SERVER_ERROR,
                Status::InternalError("unknown reporter with name: " + _report_name).to_json());
    }
}

} // namespace doris
