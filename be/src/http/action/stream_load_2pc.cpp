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

#include "http/action/stream_load_2pc.h"

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "http/utils.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "util/json_util.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

StreamLoad2PCAction::StreamLoad2PCAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

void StreamLoad2PCAction::handle(HttpRequest* req) {
    Status status = Status::OK();
    std::string status_result;

    StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
    ctx->ref();
    req->set_handler_ctx(ctx);
    ctx->db = req->param(HTTP_DB_KEY);
    std::string req_txn_id = req->header(HTTP_TXN_ID_KEY);
    try {
        ctx->txn_id = std::stoull(req_txn_id);
    } catch (const std::exception& e) {
        status = Status::InternalError("convert txn_id [{}] failed", req_txn_id);
        status_result = status.to_json();
        HttpChannel::send_reply(req, HttpStatus::OK, status_result);
        return;
    }
    ctx->txn_operation = req->header(HTTP_TXN_OPERATION_KEY);
    if (ctx->txn_operation.compare("commit") != 0 && ctx->txn_operation.compare("abort") != 0) {
        status = Status::InternalError("transaction operation should be \'commit\' or \'abort\'");
        status_result = status.to_json();
        HttpChannel::send_reply(req, HttpStatus::OK, status_result);
        return;
    }

    if (!parse_basic_auth(*req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed.";
        status = Status::InternalError("no valid Basic authorization");
    }

    status = _exec_env->stream_load_executor()->operate_txn_2pc(ctx);

    if (!status.ok()) {
        status_result = status.to_json();
    } else {
        status_result = get_success_info(req_txn_id, ctx->txn_operation);
    }
    HttpChannel::send_reply(req, HttpStatus::OK, status_result);
}

std::string StreamLoad2PCAction::get_success_info(const std::string txn_id,
                                                  const std::string txn_operation) {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);

    writer.StartObject();
    // status
    writer.Key("status");
    writer.String("Success");
    // msg
    std::string msg = "transaction [" + txn_id + "] " + txn_operation + " successfully.";
    writer.Key("msg");
    writer.String(msg.c_str());
    writer.EndObject();
    return s.GetString();
}

} // namespace doris
