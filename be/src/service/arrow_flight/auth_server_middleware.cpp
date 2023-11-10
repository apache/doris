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

#include "service/arrow_flight/auth_server_middleware.h"

#include "service/arrow_flight/call_header_utils.h"

namespace doris {
namespace flight {

void NoOpHeaderAuthServerMiddleware::SendingHeaders(
        arrow::flight::AddCallHeaders* outgoing_headers) {
    outgoing_headers->AddHeader(kAuthHeader, std::string(kBearerPrefix) + kBearerDefaultToken);
}

arrow::Status NoOpHeaderAuthServerMiddlewareFactory::StartCall(
        const arrow::flight::CallInfo& info, const arrow::flight::ServerCallContext& context,
        std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) {
    std::string username, password;
    ParseBasicHeader(context.incoming_headers(), username, password);
    *middleware = std::make_shared<NoOpHeaderAuthServerMiddleware>();
    return arrow::Status::OK();
}

void NoOpBearerAuthServerMiddleware::SendingHeaders(
        arrow::flight::AddCallHeaders* outgoing_headers) {
    std::string bearer_token =
            FindKeyValPrefixInCallHeaders(_incoming_headers, kAuthHeader, kBearerPrefix);
    *_is_valid = (bearer_token == std::string(kBearerDefaultToken));
}

arrow::Status NoOpBearerAuthServerMiddlewareFactory::StartCall(
        const arrow::flight::CallInfo& info, const arrow::flight::ServerCallContext& context,
        std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) {
    *middleware = std::make_shared<NoOpBearerAuthServerMiddleware>(context.incoming_headers(),
                                                                   &_is_valid);
    return arrow::Status::OK();
}

} // namespace flight
} // namespace doris
