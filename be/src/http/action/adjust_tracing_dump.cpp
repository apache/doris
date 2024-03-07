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

#include "adjust_tracing_dump.h"

#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"

namespace doris {
void AdjustTracingDump::handle(HttpRequest* req) {
    auto* ctx = ExecEnv::GetInstance()->pipeline_tracer_context();
    auto* params = req->params();
    if (auto status = ctx->change_record_params(*params); status.ok()) {
        HttpChannel::send_reply(req, "change record type succeed!\n"); // ok
    } else {                                                           // not ok
        LOG(WARNING) << "adjust pipeline tracing dump method failed:" << status.msg() << '\n';
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, status.msg().data());
    }
}
} // namespace doris
