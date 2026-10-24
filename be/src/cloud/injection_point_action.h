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

#include "service/http/http_handler_with_auth.h"

namespace doris {

class ExecEnv;

// Only registered under the ENABLE_INJECTION_POINT build flag, but it still goes through the
// same auth gate as every other handler: inheriting HttpHandler directly is what bypasses the
// gate entirely, and there is no reason for a debug-only endpoint to be the exception.
class InjectionPointAction : public HttpHandlerWithAuth {
public:
    // The single-argument base constructor defaults to GLOBAL/ADMIN.
    InjectionPointAction(ExecEnv* exec_env);

    ~InjectionPointAction() override = default;

    void handle(HttpRequest* req) override;
};

} // namespace doris
