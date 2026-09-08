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

#include "service/http/action/coverage_action.h"

#ifdef LLVM_PROFILE

#include "common/status.h"
#include "service/http/action/action_constants.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"

extern "C" {
void __llvm_profile_reset_counters();
int __llvm_profile_write_file();
}

namespace doris {
namespace {

void send_status(HttpRequest* req, HttpStatus http_status, const Status& status) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, http_status, status.to_json());
}

} // namespace

void CoverageResetAction::handle(HttpRequest* req) {
    __llvm_profile_reset_counters();
    send_status(req, HttpStatus::OK, Status::OK());
}

void CoverageDumpAction::handle(HttpRequest* req) {
    int result = __llvm_profile_write_file();
    if (result != 0) {
        auto status = Status::InternalError("Failed to write LLVM profile, error code: {}", result);
        LOG(ERROR) << status;
        send_status(req, HttpStatus::INTERNAL_SERVER_ERROR, status);
        return;
    }
    send_status(req, HttpStatus::OK, Status::OK());
}

} // namespace doris

#endif
