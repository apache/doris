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

#include <brpc/http_method.h>
#include <brpc/http_status_code.h>
#include <brpc/policy/gzip_compress.h>
#include <glog/logging.h>

#include <exception>
#include <sstream>
#include <string>
#include <vector>

#include "http/http_headers.h"

namespace doris {

const char PATH_DELIMETER = '/';

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
    brpc::HttpMethod method = cntl->http_request().method();
    if (!support_method(method)) {
        const std::string& err_msg = "unsupported request method";
        LOG(WARNING) << err_msg;
        on_bad_req(cntl, err_msg);
        return;
    }
    _decompress_req(cntl);
    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
    if (_is_async) {
        handle_async(cntl, done);
        done_guard.release();
    } else {
        handle_sync(cntl);
    }
}

void BaseHttpHandler::handle_sync(brpc::Controller* cntl) {}

void BaseHttpHandler::handle_async(brpc::Controller* cntl, Closure* done) {}

bool BaseHttpHandler::support_method(brpc::HttpMethod method) const {
    return true;
}

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

butil::EndPointStr BaseHttpHandler::get_localhost(brpc::Controller* cntl) const {
    return butil::endpoint2str(cntl->local_side());
}

butil::EndPointStr BaseHttpHandler::get_remote_host(brpc::Controller* cntl) const {
    return butil::endpoint2str(cntl->remote_side());
}

void BaseHttpHandler::get_path_array(brpc::Controller* cntl,
                                     std::vector<std::string>& path_array) const {
    std::stringstream ss;
    ss << get_uri(cntl).path();
    std::string token;
    while (std::getline(ss, token, PATH_DELIMETER)) {
        if (!token.empty()) {
            path_array.push_back(token);
        }
    }
}

void BaseHttpHandler::_decompress_req(brpc::Controller* cntl) {
    const std::string* encoding = get_header(cntl, HttpHeaders::CONTENT_ENCODING);
    if (encoding != nullptr && *encoding == "gzip") {
        butil::IOBuf uncompressed;
        if (!brpc::policy::GzipDecompress(cntl->request_attachment(), &uncompressed)) {
            LOG(WARNING) << "Fail to un-gzip request body";
            return;
        }
        cntl->request_attachment().swap(uncompressed);
    }
}

void BaseHttpHandler::on_succ(brpc::Controller* cntl, const std::string& msg) const {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    cntl->response_attachment().append(msg);
}

void BaseHttpHandler::on_succ_json(brpc::Controller* cntl, const std::string& json_text) const {
    cntl->http_response().set_content_type(HttpHeaders::JsonType);
    on_succ(cntl, json_text);
}

void BaseHttpHandler::on_error(brpc::Controller* cntl, const std::string& err_msg) const {
    on_error(cntl, err_msg, brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
}

void BaseHttpHandler::on_error(brpc::Controller* cntl, const std::string& err_msg,
                               int status) const {
    cntl->http_response().set_status_code(status);
    cntl->response_attachment().append(err_msg);
}

void BaseHttpHandler::on_error_json(brpc::Controller* cntl, const std::string& json_text) const {
    on_error_json(cntl, json_text, brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
}

void BaseHttpHandler::on_error_json(brpc::Controller* cntl, const std::string& json_text,
                                    int status) const {
    cntl->http_response().set_content_type(HttpHeaders::JsonType);
    on_error(cntl, json_text, status);
}

void BaseHttpHandler::on_bad_req(brpc::Controller* cntl, const std::string& err_msg) const {
    on_error(cntl, err_msg, brpc::HTTP_STATUS_BAD_REQUEST);
}

void BaseHttpHandler::on_fobidden(brpc::Controller* cntl, const std::string& err_msg) const {
    on_error(cntl, err_msg, brpc::HTTP_STATUS_FORBIDDEN);
}

void BaseHttpHandler::on_not_found(brpc::Controller* cntl, const std::string& err_msg) const {
    on_error(cntl, err_msg, brpc::HTTP_STATUS_NOT_FOUND);
}
} // namespace doris
