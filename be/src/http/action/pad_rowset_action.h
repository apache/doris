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

#include "common/status.h"
#include "http/http_handler_with_auth.h"

namespace doris {
class HttpRequest;
struct Version;

class ExecEnv;
class Tablet;
class StorageEngine;

class PadRowsetAction : public HttpHandlerWithAuth {
public:
    PadRowsetAction(ExecEnv* exec_env, StorageEngine& engine, TPrivilegeHier::type hier,
                    TPrivilegeType::type type)
            : HttpHandlerWithAuth(exec_env, hier, type), _engine(engine) {}

    ~PadRowsetAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _handle(HttpRequest* req);

    static Status _pad_rowset(Tablet* tablet, const Version& version);

    StorageEngine& _engine;
};
} // end namespace doris
