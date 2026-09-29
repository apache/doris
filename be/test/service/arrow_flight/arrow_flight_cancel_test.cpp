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
#include <arrow/flight/sql/server.h>
#include <brpc/server.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "exec/pipeline/dependency.h"
#include "load/channel/load_stream_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "service/arrow_flight/arrow_flight_batch_reader.h"
#include "service/arrow_flight/flight_sql_service.h"
#include "service/backend_options.h"
#include "service/internal_service.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/brpc_client_cache.h"

namespace doris::flight {

class ArrowFlightCancelTest : public testing::Test {
protected:
    void SetUp() override {
        _previous_mgr = std::exchange(ExecEnv::GetInstance()->_result_mgr, &_mgr);
        _state._batch_size = 1;
        _id.hi = 1;
        _id.lo = 2;
        auto schema = arrow::schema({arrow::field("value", arrow::int64())});
        std::shared_ptr<ResultBlockBufferBase> buffer;
        ASSERT_TRUE(_mgr.create_sender(_id, 16, &buffer, &_state, true, schema).ok());
        _buffer = std::dynamic_pointer_cast<ArrowFlightResultBlockBuffer>(buffer);
        _dep = Dependency::create_shared(0, 0, "Result", true);
        _buffer->set_dependency(_state.fragment_instance_id(), _dep);
        TNetworkAddress address;
        address.hostname = BackendOptions::get_localhost();
        _statement = std::make_shared<QueryStatement>(_id, address, "select value");
    }
    void TearDown() override { ExecEnv::GetInstance()->_result_mgr = _previous_mgr; }
    void fill_buffer() {
        auto block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
        ASSERT_TRUE(_buffer->add_batch(&_state, block).ok());
        ASSERT_FALSE(_dep->ready());
    }
    bool registered() {
        std::shared_ptr<ArrowFlightResultBlockBuffer> found;
        return _mgr.find_buffer(_id, found).ok();
    }
    MockRuntimeState _state;
    ResultBufferMgr _mgr;
    ResultBufferMgr* _previous_mgr = nullptr;
    TUniqueId _id;
    std::shared_ptr<ArrowFlightResultBlockBuffer> _buffer;
    std::shared_ptr<Dependency> _dep;
    std::shared_ptr<QueryStatement> _statement;
};

TEST_F(ArrowFlightCancelTest, EarlyCloseReleasesBackpressuredBuffer) {
    fill_buffer();
    auto result = ArrowFlightBatchLocalReader::Create(_statement);
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = *result;
    ASSERT_TRUE(reader->Close().ok());
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_dep->ready());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
    std::shared_ptr<Block> block;
    bool eos = false;
    EXPECT_FALSE(_buffer->get_arrow_batch(&block, &eos).ok());
    EXPECT_TRUE(reader->Close().ok());
}

TEST_F(ArrowFlightCancelTest, DestructionReleasesAbandonedBuffer) {
    fill_buffer();
    {
        auto reader = ArrowFlightBatchLocalReader::Create(_statement);
        ASSERT_TRUE(reader.ok()) << reader.status();
    }
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_dep->ready());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
}

TEST_F(ArrowFlightCancelTest, NormalEofIsNotCancellation) {
    bool fully_closed = false;
    ASSERT_TRUE(_buffer->close(_state.fragment_instance_id(), Status::OK(), 0, fully_closed).ok());
    auto result = ArrowFlightBatchLocalReader::Create(_statement);
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = *result;
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    EXPECT_EQ(batch, nullptr);
    EXPECT_TRUE(reader->Close().ok());
    std::shared_ptr<Block> block;
    bool eos = false;
    EXPECT_TRUE(_buffer->get_arrow_batch(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_FALSE(_state.get_query_ctx()->is_cancelled());
}

TEST_F(ArrowFlightCancelTest, CancellationInterruptsEmptyBufferWait) {
    std::atomic<bool> cancelled = false;
    std::promise<void> checked;
    std::atomic<int> checks = 0;
    auto result = ArrowFlightBatchLocalReader::Create(_statement, [&] {
        if (checks.fetch_add(1) == 3) {
            checked.set_value();
        }
        return cancelled.load();
    });
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = *result;
    auto fetch = std::async(std::launch::async, [&] {
        std::shared_ptr<arrow::RecordBatch> batch;
        return reader->ReadNext(&batch);
    });
    EXPECT_EQ(checked.get_future().wait_for(std::chrono::seconds(2)), std::future_status::ready);
    cancelled = true;
    const auto ready = fetch.wait_for(std::chrono::seconds(2));
    // Ensure a broken implementation fails instead of hanging the test runner.
    if (ready != std::future_status::ready) {
        _buffer->cancel(Status::Cancelled("test cleanup"));
    }
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_FALSE(fetch.get().ok());
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
}

TEST_F(ArrowFlightCancelTest, EmptyBufferReadReturnsForRetry) {
    auto fetch = std::async(std::launch::async, [&] {
        auto block = std::make_shared<Block>();
        bool eos = true;
        auto status = _buffer->get_arrow_batch(&block, &eos);
        EXPECT_EQ(block, nullptr);
        EXPECT_FALSE(eos);
        return status;
    });
    const auto ready = fetch.wait_for(std::chrono::seconds(2));
    // Bound the test even if an empty buffer incorrectly waits until query completion.
    if (ready != std::future_status::ready) {
        _buffer->cancel(Status::Cancelled("test cleanup"));
    }
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_TRUE(fetch.get().ok());
}

TEST_F(ArrowFlightCancelTest, EmptyWaitsDoNotFinishReader) {
    std::promise<void> retried;
    std::atomic<int> checks = 0;
    auto result = ArrowFlightBatchLocalReader::Create(_statement, [&] {
        if (checks.fetch_add(1) == 3) {
            retried.set_value();
        }
        return false;
    });
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = *result;
    std::shared_ptr<arrow::RecordBatch> batch;
    auto fetch = std::async(std::launch::async, [&] { return reader->ReadNext(&batch); });
    EXPECT_EQ(retried.get_future().wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_EQ(fetch.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    auto block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
    EXPECT_TRUE(_buffer->add_batch(&_state, block).ok());
    bool fully_closed = false;
    EXPECT_TRUE(_buffer->close(_state.fragment_instance_id(), Status::OK(), 2, fully_closed).ok());
    const auto ready = fetch.wait_for(std::chrono::seconds(2));
    if (ready != std::future_status::ready) {
        _buffer->cancel(Status::Cancelled("test cleanup"));
    }
    EXPECT_EQ(ready, std::future_status::ready);
    ASSERT_TRUE(fetch.get().ok());
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 2);
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    EXPECT_EQ(batch, nullptr);
    EXPECT_TRUE(reader->Close().ok());
    EXPECT_FALSE(_state.get_query_ctx()->is_cancelled());
}

TEST_F(ArrowFlightCancelTest, ConversionFailureCancelsQuery) {
    auto block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeString>({"bad"}));
    ASSERT_TRUE(_buffer->add_batch(&_state, block).ok());
    auto result = ArrowFlightBatchLocalReader::Create(_statement);
    ASSERT_TRUE(result.ok()) << result.status();
    std::shared_ptr<arrow::RecordBatch> batch;
    EXPECT_FALSE((*result)->ReadNext(&batch).ok());
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
}

TEST_F(ArrowFlightCancelTest, RealFlightRpcCancellationCleansUpQuery) {
    auto created = FlightSqlServer::create();
    ASSERT_TRUE(created.ok()) << created.status();
    auto server = *created;
    ASSERT_TRUE(server->init(0).ok());
    auto location = arrow::flight::Location::ForGrpcTcp("127.0.0.1", server->port());
    ASSERT_TRUE(location.ok()) << location.status();
    auto client_options = arrow::flight::FlightClientOptions::Defaults();
    client_options.generic_options.emplace_back("grpc.enable_http_proxy", 0);
    auto connected = arrow::flight::FlightClient::Connect(*location, client_options);
    ASSERT_TRUE(connected.ok()) << connected.status();
    auto client = std::move(*connected);
    auto handle = arrow::flight::sql::CreateStatementQueryTicket(
            print_id(_id) + "&" + BackendOptions::get_localhost() + "&" +
            std::to_string(config::brpc_port) + "&");
    ASSERT_TRUE(handle.ok()) << handle.status();
    arrow::flight::FlightCallOptions options;
    options.timeout = std::chrono::seconds(5);
    auto fetched = client->DoGet(options, arrow::flight::Ticket {*handle});
    ASSERT_TRUE(fetched.ok()) << fetched.status();
    auto reader = std::move(*fetched);
    reader->Cancel();
    for (int i = 0; i < 200 && (registered() || !_state.get_query_ctx()->is_cancelled()); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
    _buffer->cancel(Status::Cancelled("test cleanup"));
    reader.reset();
    EXPECT_TRUE(client->Close().ok());
    EXPECT_TRUE(server->join().ok());
}

class ArrowFlightRemoteCancelTest : public ArrowFlightCancelTest {
protected:
    void SetUp() override {
        ArrowFlightCancelTest::SetUp();
        _previous_cache = std::exchange(ExecEnv::GetInstance()->_internal_client_cache, &_cache);
        auto heavy = std::exchange(config::brpc_heavy_work_pool_threads, 1);
        auto light = std::exchange(config::brpc_light_work_pool_threads, 1);
        auto flight = std::exchange(config::brpc_arrow_flight_work_pool_threads, 2);
        _previous_load_mgr = std::move(ExecEnv::GetInstance()->_load_stream_mgr);
        ExecEnv::GetInstance()->_load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
        _service = std::make_unique<PInternalService>(ExecEnv::GetInstance());
        config::brpc_heavy_work_pool_threads = heavy;
        config::brpc_light_work_pool_threads = light;
        config::brpc_arrow_flight_work_pool_threads = flight;
        ASSERT_EQ(_server.AddService(_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(_server.Start("127.0.0.1:0", nullptr), 0);
        _statement->result_addr.hostname = "127.0.0.1";
        _statement->result_addr.port = _server.listen_address().port;
    }
    void TearDown() override {
        _buffer->cancel(Status::Cancelled("test cleanup"));
        _server.Stop(0);
        _server.Join();
        _service.reset();
        ExecEnv::GetInstance()->_load_stream_mgr = std::move(_previous_load_mgr);
        ExecEnv::GetInstance()->_internal_client_cache = _previous_cache;
        ArrowFlightCancelTest::TearDown();
    }
    BrpcClientCache<PBackendService_Stub> _cache;
    BrpcClientCache<PBackendService_Stub>* _previous_cache = nullptr;
    std::unique_ptr<LoadStreamMgr> _previous_load_mgr;
    std::unique_ptr<PInternalService> _service;
    brpc::Server _server;
};

TEST_F(ArrowFlightRemoteCancelTest, CloseReachesResultBackend) {
    fill_buffer();
    auto result = ArrowFlightBatchRemoteReader::Create(_statement);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE((*result)->Close().ok());
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_dep->ready());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
    EXPECT_TRUE((*result)->Close().ok());
}

TEST_F(ArrowFlightRemoteCancelTest, CancellationInterruptsPendingBrpcFetch) {
    std::atomic<bool> cancelled = false;
    auto result =
            ArrowFlightBatchRemoteReader::Create(_statement, [&] { return cancelled.load(); });
    ASSERT_TRUE(result.ok()) << result.status();
    auto fetch = std::async(std::launch::async, [&] {
        std::shared_ptr<arrow::RecordBatch> batch;
        return (*result)->ReadNext(&batch);
    });
    // Wait until the server has queued the fetch; cancelling before that misses the blocking path.
    bool waiting = false;
    for (int i = 0; i < 200; ++i) {
        {
            std::lock_guard lock(_buffer->_lock);
            waiting = !_buffer->_waiting_rpc.empty();
        }
        if (waiting) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(waiting);
    cancelled = true;
    auto ready = fetch.wait_for(std::chrono::seconds(3));
    if (ready != std::future_status::ready) {
        _buffer->cancel(Status::Cancelled("test cleanup"));
    }
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_FALSE(fetch.get().ok());
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
}

TEST_F(ArrowFlightRemoteCancelTest, FetchAfterCancellationStillCompletesRpc) {
    auto reader = ArrowFlightBatchRemoteReader::Create(_statement);
    ASSERT_TRUE(reader.ok()) << reader.status();
    ASSERT_TRUE((*reader)->Close().ok());
    auto stub = _cache.get_client(_statement->result_addr);
    ASSERT_NE(stub, nullptr);
    PFetchArrowDataRequest request;
    request.mutable_finst_id()->set_hi(_id.hi);
    request.mutable_finst_id()->set_lo(_id.lo);
    PFetchArrowDataResult response;
    brpc::Controller controller;
    controller.set_timeout_ms(1000);
    stub->fetch_arrow_data(&controller, &request, &response, nullptr);
    EXPECT_FALSE(controller.Failed()) << controller.ErrorText();
    EXPECT_TRUE(response.has_status());
    EXPECT_FALSE(Status::create(response.status()).ok());
}

TEST_F(ArrowFlightRemoteCancelTest, NormalEofDoesNotCancelQuery) {
    bool fully_closed = false;
    ASSERT_TRUE(_buffer->close(_state.fragment_instance_id(), Status::OK(), 0, fully_closed).ok());
    auto result = ArrowFlightBatchRemoteReader::Create(_statement);
    ASSERT_TRUE(result.ok()) << result.status();
    std::shared_ptr<arrow::RecordBatch> batch;
    EXPECT_TRUE((*result)->ReadNext(&batch).ok());
    EXPECT_EQ(batch, nullptr);
    EXPECT_TRUE((*result)->Close().ok());
    EXPECT_FALSE(_state.get_query_ctx()->is_cancelled());
}

} // namespace doris::flight
