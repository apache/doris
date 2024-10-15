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
class BaseStorageEngine;
class ExecEnv;

const std::string PARAM_START_VERSION = "start_version";
const std::string PARAM_END_VERSION = "end_version";

// This action is used to calculate the crc value of the files in the tablet.
class CalcFileCrcAction : public HttpHandlerWithAuth {
public:
    CalcFileCrcAction(ExecEnv* exec_env, BaseStorageEngine& engine, TPrivilegeHier::type hier,
                      TPrivilegeType::type ptype);

    ~CalcFileCrcAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _handle_calc_crc(HttpRequest* req, uint32_t* crc_value, int64_t* start_version,
                            int64_t* end_version, int32_t* rowset_count, int64_t* file_count);

private:
    BaseStorageEngine& _engine;
};

} // end namespace doris
