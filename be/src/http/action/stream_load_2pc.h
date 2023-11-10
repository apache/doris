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

#include <string>

#include "http/http_handler.h"

namespace doris {

class ExecEnv;
class HttpRequest;

class StreamLoad2PCAction : public HttpHandler {
public:
    StreamLoad2PCAction(ExecEnv* exec_env);

    ~StreamLoad2PCAction() override = default;

    void handle(HttpRequest* req) override;
    std::string get_success_info(const std::string msg, const std::string txn_operation);

private:
    ExecEnv* _exec_env;
};

} // namespace doris
