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

#include "service/arrow_flight/flight_sql_server_auth_handler.h"

#include <arrow/status.h>

namespace doris {
namespace flight {

FlightSqlServerAuthHandler::FlightSqlServerAuthHandler() = default;

FlightSqlServerAuthHandler::~FlightSqlServerAuthHandler() = default;

arrow::Status FlightSqlServerAuthHandler::Authenticate(
        const arrow::flight::ServerCallContext& context, arrow::flight::ServerAuthSender* outgoing,
        arrow::flight::ServerAuthReader* incoming) {
    std::string token;
    RETURN_NOT_OK(incoming->Read(&token));
    ARROW_ASSIGN_OR_RAISE(arrow::flight::BasicAuth incoming_auth,
                          arrow::flight::BasicAuth::Deserialize(token));
    if (incoming_auth.username == "" || incoming_auth.password == "") {
        return MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated, "Invalid token");
    }
    RETURN_NOT_OK(outgoing->Write(incoming_auth.username));
    return arrow::Status::OK();
}

arrow::Status FlightSqlServerAuthHandler::IsValid(const arrow::flight::ServerCallContext& context,
                                                  const std::string& token,
                                                  std::string* peer_identity) {
    if (token == "") {
        return MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated, "Invalid token");
    }
    *peer_identity = token;
    return arrow::Status::OK();
}

} // namespace flight
} // namespace doris
