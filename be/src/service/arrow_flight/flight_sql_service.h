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

namespace doris {
namespace flight {

class FlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
public:
    ~FlightSqlServer() override;

    static arrow::Result<std::shared_ptr<FlightSqlServer>> create();

    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
            const arrow::flight::ServerCallContext& context,
            const arrow::flight::sql::StatementQueryTicket& command) override;

    Status init(int port);
    Status join();

private:
    class Impl;
    std::shared_ptr<Impl> impl_;
    bool _inited = false;

    explicit FlightSqlServer(std::shared_ptr<Impl> impl);
};

} // namespace flight
} // namespace doris
