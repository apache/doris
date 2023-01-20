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

#include <exception>

namespace doris {
BaseHttpHandler::BaseHttpHandler(const std::string& name, bool is_async, ExecEnv* exec_env)
        : _name(name), _is_async(is_async), _exec_env(exec_env) {}

const std::string& BaseHttpHandler::get_name() const {
    return _name;
}

bool BaseHttpHandler::is_async() const {
    return _is_async;
}

ExecEnv* BaseHttpHandler::get_exec_env() {
    return _exec_env;
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

const std::string* BaseHttpHandler::get_param(brpc::Controller* cntl,
                                              const std::string& key) const {
    return cntl->http_request().uri().GetQuery(key);
}

void BaseHttpHandler::on_succ(brpc::Controller* cntl, const std::string& msg) {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    cntl->response_attachment().append(msg);
}

void BaseHttpHandler::on_succ_json(brpc::Controller* cntl, const std::string& json_text) {
    cntl->http_response().set_content_type("application/json");
    cntl->response_attachment().append(json_text);
}

void BaseHttpHandler::on_error(brpc::Controller* cntl, const std::string& err_msg) {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
    cntl->response_attachment().append(err_msg);
}

} // namespace doris
