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

#include "stream_load_2pc.h"

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "http/http_common.h"
#include "http/http_headers.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "util/url_coding.h"

namespace doris {
StreamLoad2PCAction::StreamLoad2PCAction(ExecEnv* exec_env)
        : BaseHttpHandler("stream_load_2pc", exec_env) {}

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

void StreamLoad2PCAction::handle_sync(brpc::Controller* cntl) {
    Status status = Status::OK();
    std::string status_result;

    StreamLoadContext* ctx = new StreamLoadContext(get_exec_env());
    //ctx doesn't need a manually unref
    ctx->ref();
    ctx->db = *get_param(cntl, HTTP_DB_KEY);
    std::string req_txn_id = *get_header(cntl, HTTP_TXN_ID_KEY);
    try {
        ctx->txn_id = std::stoull(req_txn_id);
    } catch (const std::exception& e) {
        status = Status::InternalError("convert txn_id [{}] failed", req_txn_id);
        status_result = status.to_json();
        on_succ_json(cntl, status_result);
        return;
    }
    ctx->txn_operation = *get_header(cntl, HTTP_TXN_OPERATION_KEY);
    if (ctx->txn_operation.compare("commit") != 0 && ctx->txn_operation.compare("abort") != 0) {
        status = Status::InternalError("transaction operation should be \'commit\' or \'abort\'");
        status_result = status.to_json();
        on_succ_json(cntl, status_result);
        return;
    }

    if (!_parse_basic_auth(cntl, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed.";
        status = Status::InternalError("no valid Basic authorization");
    }

    status = get_exec_env()->stream_load_executor()->operate_txn_2pc(ctx);

    if (!status.ok()) {
        status_result = status.to_json();
    } else {
        status_result = get_success_info(req_txn_id, ctx->txn_operation);
    }
    on_succ_json(cntl, status_result);
}

bool StreamLoad2PCAction::_parse_basic_auth(brpc::Controller* cntl, AuthInfo* auth) {
    std::string full_user;
    if (!_parse_basic_auth(cntl, &full_user, &auth->passwd)) {
        return false;
    }
    auto pos = full_user.find('@');
    if (pos != std::string::npos) {
        auth->user.assign(full_user.data(), pos);
        auth->cluster.assign(full_user.data() + pos + 1);
    } else {
        auth->user = full_user;
    }
    // set user ip
    auth->user_ip.assign(get_remote_host(cntl).c_str());
    return true;
}

bool StreamLoad2PCAction::_parse_basic_auth(brpc::Controller* cntl, std::string* user,
                                            std::string* passwd) {
    const char k_basic[] = "Basic ";
    auto& auth = *get_header(cntl, HttpHeaders::AUTHORIZATION);
    if (auth.compare(0, sizeof(k_basic) - 1, k_basic, sizeof(k_basic) - 1) != 0) {
        return false;
    }
    std::string encoded_str = auth.substr(sizeof(k_basic) - 1);
    std::string decoded_auth;
    if (!base64_decode(encoded_str, &decoded_auth)) {
        return false;
    }
    auto pos = decoded_auth.find(':');
    if (pos == std::string::npos) {
        return false;
    }
    user->assign(decoded_auth.c_str(), pos);
    passwd->assign(decoded_auth.c_str() + pos + 1);

    return true;
}
} // namespace doris