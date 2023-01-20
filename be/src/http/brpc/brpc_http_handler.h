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
    explicit BaseHttpHandler(const std::string& name, bool is_async, ExecEnv* exec_env);

    const std::string& get_name() const;

    bool is_async() const;

    ExecEnv* get_exec_env();

    void handle(RpcController* controller, Closure* done);

protected:
    virtual void handle_sync(brpc::Controller* cntl);

    virtual void handle_async(brpc::Controller* cntl, Closure* done);

    const std::string* get_param(brpc::Controller* cntl, const std::string& key) const;

    virtual void on_succ(brpc::Controller* cntl, const std::string& msg);

    virtual void on_succ_json(brpc::Controller* cntl, const std::string& json_text);

    virtual void on_error(brpc::Controller* cntl, const std::string& err_msg);

private:
    std::string _name;
    bool _is_async;
    ExecEnv* _exec_env;
};
} // namespace doris