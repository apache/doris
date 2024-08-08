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

#include "common/status.h"
#include "http/http_handler_with_auth.h"
#include "http/http_request.h"
#include "olap/storage_engine.h"
namespace doris {

class CompactionScoreAction : public HttpHandlerWithAuth {
public:
    CompactionScoreAction(ExecEnv* exec_env, TPrivilegeHier::type hier, TPrivilegeType::type type,
                          StorageEngine& storage_engine);

    void handle(HttpRequest* req) override;

private:
    Status _handle(HttpRequest* req, std::string* result);

    StorageEngine& _storage_engine;
};

} // namespace doris
