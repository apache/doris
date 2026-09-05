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

#include "service/http/http_handler.h"
#include "service/http/http_handler_with_auth.h"

namespace doris {
class ExecEnv;
class HttpRequest;

// State of the deployed Java plugins as JSON: which ones loaded, which ones failed and why,
// and the plugin API version each declares. Answers "why is this plugin not loading" from
// outside the process, which nothing else could do - the whole thing was only in the log.
//
// Deliberately does not start a JVM: on a BE that has not touched Java yet it reports the
// deployment as seen from disk and says so. Polling a status endpoint must not be the thing
// that creates a JVM on a BE that would otherwise never have one.
class JniPluginStatusAction : public HttpHandlerWithAuth {
public:
    JniPluginStatusAction(ExecEnv* exec_env, TPrivilegeHier::type hier, TPrivilegeType::type type)
            : HttpHandlerWithAuth(exec_env, hier, type) {}

    ~JniPluginStatusAction() override = default;

    void handle(HttpRequest* req) override;
};

} // end namespace doris
