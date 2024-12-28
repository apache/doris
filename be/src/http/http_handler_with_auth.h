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

#include <gen_cpp/FrontendService.h>

#include "http_handler.h"
#include "runtime/exec_env.h"

namespace doris {

class ExecEnv;
class HttpRequest;
class RestMonitorIface;
class TCheckAuthRequest;
class TPrivilegeCtrl;
class TPrivilegeHier;
class TPrivilegeType;

// Handler for on http request with auth
class HttpHandlerWithAuth : public HttpHandler {
public:
    HttpHandlerWithAuth(ExecEnv* exec_env, TPrivilegeHier::type hier, TPrivilegeType::type type);

    HttpHandlerWithAuth(ExecEnv* exec_env) : _exec_env(exec_env) {}
    ~HttpHandlerWithAuth() override = default;

    // return 0 if auth pass, otherwise -1.
    int on_header(HttpRequest* req) override;

    // return true if fill privilege success, otherwise false.
    virtual bool on_privilege(const HttpRequest& req, TCheckAuthRequest& auth_request) {
        TPrivilegeCtrl priv_ctrl;
        priv_ctrl.priv_hier = _hier;
        auth_request.__set_priv_ctrl(priv_ctrl);
        auth_request.__set_priv_type(_type);
        return true;
    }

protected:
    ExecEnv* _exec_env;
    TPrivilegeHier::type _hier = TPrivilegeHier::GLOBAL;
    TPrivilegeType::type _type = TPrivilegeType::ADMIN;
};

} // namespace doris
