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

#include "arrow/flight/sql/server.h"
#include "arrow/result.h"
#include "common/status.h"
#include "service/arrow_flight/arrow_flight_batch_reader.h"
#include "service/arrow_flight/auth_server_middleware.h"

namespace doris {
namespace flight {

class FlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
public:
    ~FlightSqlServer() override;

    static arrow::Result<std::shared_ptr<FlightSqlServer>> create();

    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
            const arrow::flight::ServerCallContext& context,
            const arrow::flight::sql::StatementQueryTicket& command) override;

    // A BE keeps no client session: it serves a result by its ticket, and the FE session that
    // issued the ticket already decided the catalog and database the query ran in. So `catalog`
    // and `schema`, the session options naming that context, are accepted here and change
    // nothing; any other option is refused with INVALID_NAME, since a BE has nothing it could
    // apply it to.
    //
    // A client still sends them: the Flight SQL JDBC driver, given `?catalog=internal` in its
    // URL, opens a connection to each endpoint location it reads a result from and sets
    // `catalog` there first. Answered with the default UNIMPLEMENTED, it failed every query
    // whose result a BE holds, while the FE's own results and metadata still read fine.
    arrow::Result<arrow::flight::SetSessionOptionsResult> SetSessionOptions(
            const arrow::flight::ServerCallContext& context,
            const arrow::flight::SetSessionOptionsRequest& request) override;

    Status init(int port);
    Status join();

private:
    class Impl;
    std::shared_ptr<Impl> _impl;
    bool _inited = false;

    std::shared_ptr<NoOpHeaderAuthServerMiddlewareFactory> _header_middleware;
    std::shared_ptr<NoOpBearerAuthServerMiddlewareFactory> _bearer_middleware;

    explicit FlightSqlServer(std::shared_ptr<Impl> impl);
};

} // namespace flight
} // namespace doris
