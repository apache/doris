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

#include <arrow/flight/client.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "service/arrow_flight/flight_sql_service.h"

namespace doris::flight {

// The SetSessionOptions action as a client sends it to a BE result endpoint over gRPC: the Flight
// SQL JDBC driver sets the `catalog` its URL names on every endpoint location before it reads the
// result there, and fails the read unless the answer carries no errors.
class FlightSqlSessionOptionsTest : public testing::Test {
protected:
    void SetUp() override {
        auto created = FlightSqlServer::create();
        ASSERT_TRUE(created.ok()) << created.status();
        _server = *created;
        ASSERT_TRUE(_server->init(0).ok());
        auto location = arrow::flight::Location::ForGrpcTcp("127.0.0.1", _server->port());
        ASSERT_TRUE(location.ok()) << location.status();
        auto client_options = arrow::flight::FlightClientOptions::Defaults();
        client_options.generic_options.emplace_back("grpc.enable_http_proxy", 0);
        auto connected = arrow::flight::FlightClient::Connect(*location, client_options);
        ASSERT_TRUE(connected.ok()) << connected.status();
        _client = std::move(*connected);
    }

    void TearDown() override {
        if (_client) {
            EXPECT_TRUE(_client->Close().ok());
        }
        if (_server) {
            EXPECT_TRUE(_server->join().ok());
        }
    }

    arrow::Result<arrow::flight::SetSessionOptionsResult> set(
            std::map<std::string, arrow::flight::SessionOptionValue> options) {
        arrow::flight::FlightCallOptions call_options;
        call_options.timeout = std::chrono::seconds(5);
        return _client->SetSessionOptions(
                call_options, arrow::flight::SetSessionOptionsRequest {std::move(options)});
    }

    std::shared_ptr<FlightSqlServer> _server;
    std::unique_ptr<arrow::flight::FlightClient> _client;
};

TEST_F(FlightSqlSessionOptionsTest, AcceptsCatalogAndSchemaWithoutErrors) {
    auto result = set({{"catalog", std::string("internal")}});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE(result->errors.empty());

    result = set({{"catalog", std::string("hive_catalog")}, {"schema", std::string("db1")}});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE(result->errors.empty());
}

TEST_F(FlightSqlSessionOptionsTest, RefusesEveryOtherOptionByName) {
    auto result = set({{"catalog", std::string("internal")},
                       {"time_zone", std::string("UTC")},
                       {"query_timeout", int64_t {60}}});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(2, result->errors.size());
    EXPECT_EQ(arrow::flight::SetSessionOptionErrorValue::kInvalidName,
              result->errors.at("time_zone").value);
    EXPECT_EQ(arrow::flight::SetSessionOptionErrorValue::kInvalidName,
              result->errors.at("query_timeout").value);
    // Option names are matched exactly, as the FE matches them.
    result = set({{"Catalog", std::string("internal")}});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(1, result->errors.size());
    EXPECT_EQ(arrow::flight::SetSessionOptionErrorValue::kInvalidName,
              result->errors.at("Catalog").value);
}

} // namespace doris::flight
