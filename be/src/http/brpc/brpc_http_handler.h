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

#pragma once

#include <brpc/http_method.h>
#include <brpc/restful.h>
#include <brpc/server.h>
#include <brpc/uri.h>

#include <string>
#include <vector>

#include "runtime/exec_env.h"

namespace brpc {
class Controller;
}

namespace doris {

/// Handler for brpc based http request
class BaseHttpHandler {
    using RpcController = ::google::protobuf::RpcController;
    using Closure = ::google::protobuf::Closure;

public:
    BaseHttpHandler() = default;

    virtual ~BaseHttpHandler() = default;

    ///! Keep the handler name consistent with the http endpoint name
    virtual const std::string& get_name() const;

    virtual bool is_async() const;

    ExecEnv* get_exec_env();

    void set_exec_env(ExecEnv* env);

    void handle(RpcController* controller, Closure* done);

protected:
    const std::string handler_name;

    BaseHttpHandler(const std::string& name);
    BaseHttpHandler(const std::string& name, ExecEnv* exec_env);
    BaseHttpHandler(const std::string& name, bool is_async, ExecEnv* exec_env);

    ///! biz logic of sync handler
    virtual void handle_sync(brpc::Controller* cntl);

    ///! biz logic of async handler
    virtual void handle_async(brpc::Controller* cntl, Closure* done);

    ///! check if the handler supports the corresponding method
    virtual bool support_method(brpc::HttpMethod method) const;

    void on_succ(brpc::Controller* cntl, const std::string& msg) const;

    void on_succ_json(brpc::Controller* cntl, const std::string& json_text) const;

    void on_error(brpc::Controller* cntl, const std::string& err_msg) const;

    void on_error_json(brpc::Controller* cntl, const std::string& json_text) const;

    void on_error(brpc::Controller* cntl, const std::string& err_msg, int status) const;

    void on_error_json(brpc::Controller* cntl, const std::string& json_text, int status) const;

    void on_bad_req(brpc::Controller* cntl, const std::string& err_msg) const;

    void on_fobidden(brpc::Controller* cntl, const std::string& err_msg) const;

    void on_not_found(brpc::Controller* cntl, const std::string& err_msg) const;

    const std::string* get_param(brpc::Controller* cntl, const std::string& key) const;

    const std::string* get_header(brpc::Controller* cntl, const std::string& key) const;

    brpc::URI get_uri(brpc::Controller* cntl) const;

    butil::EndPointStr get_localhost(brpc::Controller* cntl) const;

    butil::EndPointStr get_remote_host(brpc::Controller* cntl) const;

    void get_path_array(brpc::Controller* cntl, std::vector<std::string>& path_array) const;

private:
    bool _is_async;
    ExecEnv* _exec_env;

    void _decompress_req(brpc::Controller* cntl);
};
} // namespace doris