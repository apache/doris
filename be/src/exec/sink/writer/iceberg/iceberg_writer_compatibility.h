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
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

inline Status validate_iceberg_external_file_report_ack(const TQueryOptions& query_options) {
    if (!query_options.__isset.supports_external_file_report_ack ||
        !query_options.supports_external_file_report_ack) {
        // A pre-ACK coordinator cannot safely take ownership of files created by this sink.
        return Status::NotSupported(
                "Iceberg writes require a coordinator that acknowledges external-file reports");
    }
    return Status::OK();
}

} // namespace doris
