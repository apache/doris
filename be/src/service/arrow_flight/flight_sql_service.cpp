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

#include <algorithm>
#include <random>
#include <arrow/status.h>
#include <cctz/time_zone.h>

#include "arrow/flight/sql/server.h"
#include "service/arrow_flight/arrow_flight_batch_reader.h"
#include "service/arrow_flight/arrow_flight_update.h"
#include "service/arrow_flight/flight_sql_info.h"
#include "service/backend_options.h"
#include "util/arrow/utils.h"
#include "util/uid_util.h"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/utils/arrow_column_to_doris_column.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/aggregate_functions/aggregate_function.h"

#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/runtime/vdatetime_value.h"

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

    clock_t _clock;
    int _cnt=0;

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

    arrow::Result<int64_t> DoPutCommandStatementUpdate(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::StatementUpdate& command) {
    const std::string& sql = command.query;
    ARROW_ASSIGN_OR_RAISE(auto statement, ArrowFlightUpdate::Create(sql));
    return statement->ExecuteUpdate();
  }

    arrow::Result<int64_t> DoPutPreparedStatementUpdate(
        const arrow::flight::ServerCallContext& context, const arrow::flight::sql::PreparedStatementUpdate& command,
        arrow::flight::FlightMessageReader* reader) {

        _cnt++;

        ARROW_ASSIGN_OR_RAISE(auto statement, ArrowFlightUpdate::Create("update"));
        
        while (true) {
            cctz::time_zone ctzz;
            TimezoneUtils::find_cctz_time_zone("UTC", ctzz);

            ARROW_ASSIGN_OR_RAISE(arrow::flight::FlightStreamChunk chunk, reader->Next());
            if (chunk.data == nullptr) break;

            const int64_t num_rows = chunk.data->num_rows();
            if (num_rows == 0) continue;
            // LOG(INFO) << "wuwenchi xxxx chunk.data.num_rows: " << num_rows;

            // statement->SetParameters(std::move(chunk.data));
            // ARROW_RETURN_NOT_OK(statement->SetParameters({std::move(chunk.data)}));

            std::vector<std::shared_ptr<arrow::RecordBatch>> datas;
            datas.push_back(std::move(chunk.data));
            auto end = std::remove_if(
                datas.begin(), datas.end(),
                [](const std::shared_ptr<arrow::RecordBatch>& batch) { return batch->num_rows() == 0; });
            datas.erase(end, datas.end());

            clock_t t1 = clock();
            int batch_size = datas.size();
            for (int i=0; i<batch_size; i++)
            {
                arrow::RecordBatch& batch = *datas[i];
                // 列个数
                int num_rows = batch.num_rows();
                int num_columns = batch.num_columns();
                // LOG(INFO) << "wuwenchi xxxx batch.num_rows:" << num_rows;
                // LOG(INFO) << "wuwenchi xxxx batch.num_columns:" << num_columns;
                for (int c = 0; c < num_columns; ++c) {
                    arrow::Array* column = batch.column(c).get();
                    
                    auto pt = doris::vectorized::arrow_type_to_primitive_type(column->type_id());
                    vectorized::DataTypePtr data_type = vectorized::DataTypeFactory::instance().create_data_type(pt, true);
                    vectorized::MutableColumnPtr data_column = data_type->create_column();
                    vectorized::ColumnWithTypeAndName column_with_name(std::move(data_column), data_type, "test_numeric_column");

                    Status st = doris::vectorized::arrow_column_to_doris_column(column, 0, column_with_name.column, column_with_name.type, num_rows, ctzz);

                    // for (int k=0; k<num_rows; k++) {
                    //     LOG(INFO) << "wuwenchi xxxx column_with_name.to_string : " << k << ": " << column_with_name.to_string(k);
                    // }
                }
            }

            clock_t t2 = clock();
            _clock += (t2 - t1);
            // LOG(INFO) << "wuwenchi xxxx time: " << t2 - t1;
            

            // {
            //     arrow::RecordBatch& batch = *datas[0];
            //     // 列个数
            //     int num_rows = batch.num_rows();
            //     int num_columns = batch.num_columns();
            //     LOG(INFO) << "wuwenchi xxxx batch.num_rows:" << num_rows;
            //     LOG(INFO) << "wuwenchi xxxx batch.num_columns:" << num_columns;
            //     for (int c = 0; c < num_columns; ++c) {
            //         arrow::Array* column = batch.column(c).get();
                    
            //         auto pt = doris::vectorized::arrow_type_to_primitive_type(column->type_id());
            //         vectorized::DataTypePtr data_type = vectorized::DataTypeFactory::instance().create_data_type(pt, true);
            //         vectorized::MutableColumnPtr data_column = data_type->create_column();
            //         vectorized::ColumnWithTypeAndName column_with_name(std::move(data_column), data_type, "test_numeric_column");

            //         Status st = doris::vectorized::arrow_column_to_doris_column(column, 0, column_with_name.column, column_with_name.type, num_rows, ctzz);

            //         for (int i=0; i<num_rows; i++) {
            //             LOG(INFO) << "wuwenchi xxxx column_with_name.to_string : " << i << ": " << column_with_name.to_string(i);
            //         }
            //     }
            // }

            // for (int i = 0; i < num_rows; ++i) {

            //     arrow::RecordBatch& batch = *datas[0];

            //     int nums = batch.num_columns();
            //     for (int c = 0; c < nums; ++c) {
            //         arrow::Array* column = batch.column(c).get();
                    
            //         auto pt = doris::vectorized::arrow_type_to_primitive_type(column->type_id());
            //         vectorized::DataTypePtr data_type = vectorized::DataTypeFactory::instance().create_data_type(pt, true);
            //         vectorized::MutableColumnPtr data_column = data_type->create_column();
            //         vectorized::ColumnWithTypeAndName column_with_name(std::move(data_column), data_type, "test_numeric_column");
            //         // static_cast<void>(doris::vectorized::arrow_column_to_doris_column(
            //         // column, 
            //         // 0, 
            //         // column_with_name.column,
            //         // column_with_name.type,
            //         // 13,
            //         // ctzz));
            //         Status st = doris::vectorized::arrow_column_to_doris_column(column, 0, column_with_name.column, column_with_name.type, 13, ctzz);
            //         std::cout << st << std::endl;
                    
            //     }
            // }

                // ARROW_RETURN_NOT_OK(statement->SetParameters({std::move(chunk.data)}));
                // for (int i = 0; i < num_rows; ++i) {
                // if (sqlite3_clear_bindings(stmt) != SQLITE_OK) {
                //     return Status::Invalid("Failed to reset bindings on row ", i, ": ",
                //                         sqlite3_errmsg(statement->db()));
                // }
                // // batch_index is always 0 since we're calling SetParameters
                // // with a single batch at a time
                // ARROW_RETURN_NOT_OK(statement->Bind(/*batch_index=*/0, i));
                // ARROW_RETURN_NOT_OK(callback());
                // }
        
            break;
        }
        return 10;
    }

    arrow::Result<arrow::flight::sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
            const arrow::flight::ServerCallContext& context,
            const arrow::flight::sql::ActionCreatePreparedStatementRequest& request) {

        LOG(INFO) << "CreatePreparedStatement";

        std::string handle = GenerateRandomString();
        arrow::FieldVector parameter_fields;
        // {
        //     parameter_fields.push_back(arrow::field("id", arrow::int32()));
        //     parameter_fields.push_back(arrow::field("val", arrow::int32()));
        // }
        
            parameter_fields.push_back(arrow::field("id", arrow::int32()));
            parameter_fields.push_back(arrow::field("val", arrow::utf8()));
            parameter_fields.push_back(arrow::field("val2", arrow::utf8()));
        
        std::shared_ptr<arrow::Schema> parameter_schema = arrow::schema(parameter_fields);

        return arrow::flight::sql::ActionCreatePreparedStatementResult{
            parameter_schema, parameter_schema, std::move(handle)};
    }

    std::default_random_engine gen_;

    std::string GenerateRandomString() {
        uint32_t length = 16;

        // MSVC doesn't support char types here
        std::uniform_int_distribution<uint16_t> dist(static_cast<uint16_t>('0'),
                                                    static_cast<uint16_t>('Z'));
        std::string ret(length, 0);
        // Don't generate symbols to simplify parsing in DecodeTransactionQuery
        auto get_random_char = [&]() {
        char res;
        while (true) {
            res = static_cast<char>(dist(gen_));
            if (res <= '9' || res >= 'A') break;
        }
        return res;
        };
        std::generate_n(ret.begin(), length, get_random_char);
        return ret;
    }

    arrow::Status ClosePreparedStatement(const arrow::flight::ServerCallContext& context,
                                const arrow::flight::sql::ActionClosePreparedStatementRequest& request) {
        // std::lock_guard<std::mutex> guard(mutex_);
        // const std::string& prepared_statement_handle = request.prepared_statement_handle;

        // auto search = prepared_statements_.find(prepared_statement_handle);
        // if (search != prepared_statements_.end()) {
        // prepared_statements_.erase(prepared_statement_handle);
        // } else {
        // return Status::Invalid("Prepared statement not found");
        // }

        LOG(INFO) << "ClosePreparedStatement";

        LOG(INFO) << "wuwenchi xxxx arrow time: " << _clock;
        LOG(INFO) << "wuwenchi xxxx cnt: " << _cnt;

        return arrow::Status::OK();
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

arrow::Result<int64_t> FlightSqlServer::DoPutCommandStatementUpdate(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::StatementUpdate& command) {
  return _impl->DoPutCommandStatementUpdate(context, command);
}


arrow::Result<int64_t> FlightSqlServer::DoPutPreparedStatementUpdate(
    const arrow::flight::ServerCallContext& context, const arrow::flight::sql::PreparedStatementUpdate& command,
    arrow::flight::FlightMessageReader* reader) {

  return _impl->DoPutPreparedStatementUpdate(context, command, reader);
}

arrow::Result<arrow::flight::sql::ActionCreatePreparedStatementResult> FlightSqlServer::CreatePreparedStatement(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::ActionCreatePreparedStatementRequest& request) {
    return _impl->CreatePreparedStatement(context, request);
}

arrow::Status FlightSqlServer::ClosePreparedStatement(const arrow::flight::ServerCallContext& context,
    const arrow::flight::sql::ActionClosePreparedStatementRequest& request) {

    return _impl->ClosePreparedStatement(context, request);
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
