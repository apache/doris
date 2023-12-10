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

#include "service/arrow_flight/flight_sql_service.h"

#include <arrow/status.h>

#include "arrow/flight/sql/server.h"
#include "service/arrow_flight/arrow_flight_batch_reader.h"
#include "service/arrow_flight/flight_sql_info.h"
#include "service/backend_options.h"
#include "util/arrow/utils.h"
#include "util/uid_util.h"

namespace doris {
namespace flight {

class FlightSqlServer::Impl {
private:
    // Create a Ticket that combines a sql and a query ID.
    arrow::Result<arrow::flight::Ticket> encode_ticket(const std::string& sql,
                                                       const std::string& query_id) {
        std::string query = query_id;
        query += ':';
        query += sql;
        ARROW_ASSIGN_OR_RAISE(auto ticket, arrow::flight::sql::CreateStatementQueryTicket(query));
        return arrow::flight::Ticket {std::move(ticket)};
    }

    arrow::Result<std::pair<std::string, std::string>> decode_ticket(const std::string& ticket) {
        auto divider = ticket.find(':');
        if (divider == std::string::npos) {
            return arrow::Status::Invalid("Malformed ticket");
        }
        std::string query_id = ticket.substr(0, divider);
        std::string sql = ticket.substr(divider + 1);
        return std::make_pair(std::move(sql), std::move(query_id));
    }

public:
    explicit Impl() = default;

    ~Impl() = default;

    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
            const arrow::flight::ServerCallContext& context,
            const arrow::flight::sql::StatementQueryTicket& command) {
        ARROW_ASSIGN_OR_RAISE(auto pair, decode_ticket(command.statement_handle));
        const std::string& sql = pair.first;
        const std::string query_id = pair.second;
        TUniqueId queryid;
        parse_id(query_id, &queryid);

        auto statement = std::make_shared<QueryStatement>(queryid, sql);

        std::shared_ptr<ArrowFlightBatchReader> reader;
        ARROW_ASSIGN_OR_RAISE(reader, ArrowFlightBatchReader::Create(statement));

        return std::make_unique<arrow::flight::RecordBatchStream>(reader);
    }
};

FlightSqlServer::FlightSqlServer(std::shared_ptr<Impl> impl) : _impl(std::move(impl)) {}

arrow::Result<std::shared_ptr<FlightSqlServer>> FlightSqlServer::create() {
    std::shared_ptr<Impl> impl = std::make_shared<Impl>();

    std::shared_ptr<FlightSqlServer> result(new FlightSqlServer(std::move(impl)));
    for (const auto& id_to_result : GetSqlInfoResultMap()) {
        result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
    }

    return result;
}

FlightSqlServer::~FlightSqlServer() {
    static_cast<void>(join());
}

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> FlightSqlServer::DoGetStatement(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::StatementQueryTicket& command) {
    return _impl->DoGetStatement(context, command);
}

Status FlightSqlServer::init(int port) {
    if (port == -1) {
        LOG(INFO) << "Arrow Flight Service not start";
        return Status::OK();
    }
    _inited = true;
    arrow::flight::Location bind_location;
    RETURN_DORIS_STATUS_IF_ERROR(
            arrow::flight::Location::ForGrpcTcp(BackendOptions::get_service_bind_address(), port)
                    .Value(&bind_location));
    arrow::flight::FlightServerOptions flight_options(bind_location);

    // Not authenticated in BE flight server.
    // After the authentication between the ADBC Client and the FE flight server is completed,
    // the FE flight server will put the query id in the Ticket and send it back to the Client.
    // When the Client uses the Ticket to fetch data from the BE flight server, the BE flight
    // server will verify the query id, this step is equivalent to authentication.
    _header_middleware = std::make_shared<NoOpHeaderAuthServerMiddlewareFactory>();
    _bearer_middleware = std::make_shared<NoOpBearerAuthServerMiddlewareFactory>();
    flight_options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
    flight_options.middleware.push_back({"header-auth-server", _header_middleware});
    flight_options.middleware.push_back({"bearer-auth-server", _bearer_middleware});

    RETURN_DORIS_STATUS_IF_ERROR(Init(flight_options));
    LOG(INFO) << "Arrow Flight Service bind to host: " << BackendOptions::get_service_bind_address()
              << ", port: " << port;
    return Status::OK();
}

Status FlightSqlServer::join() {
    if (!_inited) {
        // Flight not inited, not need shutdown
        return Status::OK();
    }
    RETURN_DORIS_STATUS_IF_ERROR(Shutdown());
    return Status::OK();
}

} // namespace flight
} // namespace doris
