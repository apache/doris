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

#include "arrow/flight/sql/server.h"
#include "service/arrow_flight/flight_sql_info.h"
#include "service/arrow_flight/flight_sql_statement_batch_reader.h"
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
        LOG(INFO) << "DorisStatementBatchReader aaaaa";
        ARROW_ASSIGN_OR_RAISE(auto pair, decode_ticket(command.statement_handle));
        const std::string& sql = pair.first;
        const std::string query_id = pair.second;
        TUniqueId queryid;
        parse_id(query_id, &queryid);

        auto statement = std::make_shared<DorisStatement>(sql, queryid);

        std::shared_ptr<DorisStatementBatchReader> reader;
        ARROW_ASSIGN_OR_RAISE(reader, DorisStatementBatchReader::Create(statement));

        return std::make_unique<arrow::flight::RecordBatchStream>(reader);
    }
};

FlightSqlServer::FlightSqlServer(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

arrow::Result<std::shared_ptr<FlightSqlServer>> FlightSqlServer::Create() {
    std::shared_ptr<Impl> impl = std::make_shared<Impl>();

    std::shared_ptr<FlightSqlServer> result(new FlightSqlServer(std::move(impl)));
    for (const auto& id_to_result : GetSqlInfoResultMap()) {
        result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
    }

    return result;
}

FlightSqlServer::~FlightSqlServer() = default;

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> FlightSqlServer::DoGetStatement(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::StatementQueryTicket& command) {
    return impl_->DoGetStatement(context, command);
}

} // namespace flight
} // namespace doris
