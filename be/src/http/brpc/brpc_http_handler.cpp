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

#include "brpc_http_handler.h"

#include <brpc/http_status_code.h>

#include <exception>
#include <string>

namespace doris {
BaseHttpHandler::BaseHttpHandler(const std::string& name)
        : handler_name(name), _is_async(false), _exec_env(nullptr) {}

BaseHttpHandler::BaseHttpHandler(const std::string& name, ExecEnv* exec_env)
        : handler_name(name), _is_async(false), _exec_env(exec_env) {}

BaseHttpHandler::BaseHttpHandler(const std::string& name, bool is_async, ExecEnv* exec_env)
        : handler_name(name), _is_async(is_async), _exec_env(exec_env) {}

const std::string& BaseHttpHandler::get_name() const {
    return handler_name;
}

bool BaseHttpHandler::is_async() const {
    return _is_async;
}

ExecEnv* BaseHttpHandler::get_exec_env() {
    return _exec_env;
}

void BaseHttpHandler::set_exec_env(ExecEnv* env) {
    _exec_env = env;
}

void BaseHttpHandler::handle(RpcController* controller, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    if (_is_async) {
        handle_async(cntl, done);
        done_guard.release();
    } else {
        handle_sync(cntl);
    }
}

void BaseHttpHandler::handle_sync(brpc::Controller* cntl) {}

void BaseHttpHandler::handle_async(brpc::Controller* cntl, Closure* done) {}

const std::string* BaseHttpHandler::get_param(brpc::Controller* cntl,
                                              const std::string& key) const {
    return get_uri(cntl).GetQuery(key);
}

const std::string* BaseHttpHandler::get_header(brpc::Controller* cntl,
                                               const std::string& key) const {
    return cntl->http_request().GetHeader(key);
}

brpc::URI BaseHttpHandler::get_uri(brpc::Controller* cntl) const {
    return cntl->http_request().uri();
}

butil::EndPointStr BaseHttpHandler::get_localhost(brpc::Controller* cntl) {
    return butil::endpoint2str(cntl->local_side());
}

butil::EndPointStr BaseHttpHandler::get_remote_host(brpc::Controller* cntl) {
    return butil::endpoint2str(cntl->remote_side());
}

void BaseHttpHandler::on_succ(brpc::Controller* cntl, const std::string& msg) {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    cntl->response_attachment().append(msg);
}

void BaseHttpHandler::on_succ_json(brpc::Controller* cntl, const std::string& json_text) {
    cntl->http_response().set_content_type("application/json");
    on_succ(cntl, json_text);
}

void BaseHttpHandler::on_error(brpc::Controller* cntl, const std::string& err_msg) {
    on_error(cntl, err_msg, brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
}

void BaseHttpHandler::on_error(brpc::Controller* cntl, const std::string& err_msg, int status) {
    cntl->http_response().set_status_code(status);
    cntl->response_attachment().append(err_msg);
}

void BaseHttpHandler::on_error_json(brpc::Controller* cntl, const std::string& json_text) {
    on_error_json(cntl, json_text, brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
}

void BaseHttpHandler::on_error_json(brpc::Controller* cntl, const std::string& json_text,
                                    int status) {
    cntl->http_response().set_content_type("application/json");
    on_error(cntl, json_text, status);
}

void BaseHttpHandler::on_bad_req(brpc::Controller* cntl, const std::string& err_msg) {
    on_error(cntl, err_msg, brpc::HTTP_STATUS_BAD_REQUEST);
}

void BaseHttpHandler::on_fobidden(brpc::Controller* cntl, const std::string& err_msg) {
    on_error(cntl, err_msg, brpc::HTTP_STATUS_FORBIDDEN);
}

void BaseHttpHandler::on_not_found(brpc::Controller* cntl, const std::string& err_msg) {
    on_error(cntl, err_msg, brpc::HTTP_STATUS_NOT_FOUND);
}
} // namespace doris
