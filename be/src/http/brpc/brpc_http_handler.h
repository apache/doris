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

#include <brpc/restful.h>
#include <brpc/server.h>
#include <brpc/uri.h>

#include <string>

#include "runtime/exec_env.h"

namespace brpc {
class Controller;
}

namespace doris {
class BaseHttpHandler {
    using RpcController = ::google::protobuf::RpcController;
    using Closure = ::google::protobuf::Closure;

public:
    BaseHttpHandler() = default;

    virtual ~BaseHttpHandler() = default;

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

    virtual void handle_sync(brpc::Controller* cntl);

    virtual void handle_async(brpc::Controller* cntl, Closure* done);

    void on_succ(brpc::Controller* cntl, const std::string& msg);

    void on_succ_json(brpc::Controller* cntl, const std::string& json_text);

    void on_error(brpc::Controller* cntl, const std::string& err_msg);

    void on_error_json(brpc::Controller* cntl, const std::string& json_text);

    void on_error(brpc::Controller* cntl, const std::string& err_msg, int status);

    void on_error_json(brpc::Controller* cntl, const std::string& json_text, int status);

    void on_bad_req(brpc::Controller* cntl, const std::string& err_msg);

    void on_fobidden(brpc::Controller* cntl, const std::string& err_msg);

    void on_not_found(brpc::Controller* cntl, const std::string& err_msg);

    const std::string* get_param(brpc::Controller* cntl, const std::string& key) const;

    const std::string* get_header(brpc::Controller* cntl, const std::string& key) const;

    brpc::URI get_uri(brpc::Controller* cntl) const;

    butil::EndPointStr get_localhost(brpc::Controller* cntl);

    butil::EndPointStr get_remote_host(brpc::Controller* cntl);

private:
    bool _is_async;
    ExecEnv* _exec_env;
};
} // namespace doris