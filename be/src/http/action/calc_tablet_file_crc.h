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

#include <stdint.h>

#include <string>

#include "common/status.h"
#include "http/http_handler_with_auth.h"

namespace doris {
class HttpRequest;
class StorageEngine;
class ExecEnv;

/// This action is used for viewing the compaction status.
/// See compaction-action.md for details.
class CalcTabletFileCrcAction : public HttpHandlerWithAuth {
public:
    CalcTabletFileCrcAction(ExecEnv* exec_env, StorageEngine& engine, TPrivilegeHier::type hier,
                            TPrivilegeType::type ptype);

    ~CalcTabletFileCrcAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _handle_calc_crc(HttpRequest* req, uint32_t* crc_value);

private:
    StorageEngine& _engine;
};

} // end namespace doris
