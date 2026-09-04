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

#include <gen_cpp/FrontendService_types.h>

#include "common/status.h"
#include "util/thrift_util.h"

namespace doris {

inline Status validate_report_exec_status_size(const TReportExecStatusParams& params,
                                               size_t thrift_limit) {
    ThriftSerializer serializer(false, 256);
    uint32_t serialized_size = 0;
    uint8_t* buffer = nullptr;
    RETURN_IF_ERROR(serializer.serialize(&params, &serialized_size, &buffer));
    // Include the args field header and RPC method/version/sequence envelope around the params.
    constexpr size_t rpc_envelope_bytes = 64;
    if (thrift_limit < rpc_envelope_bytes || serialized_size > thrift_limit - rpc_envelope_bytes) {
        return Status::InternalError(
                "ReportExecStatus exceeds the coordinator Thrift message limit");
    }
    return Status::OK();
}

} // namespace doris
