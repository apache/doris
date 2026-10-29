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
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

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
#include "util/client_cache.h"
#include "util/dns_cache.h"

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
        address.port = config::brpc_port;
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

TEST_F(ArrowFlightCancelTest, CloseReleasesSiblingBuffersAfterContextExpires) {
    TUniqueId sibling_id = _id;
    ++sibling_id.lo;
    std::shared_ptr<ResultBlockBufferBase> sibling;
    ASSERT_TRUE(_mgr.create_sender(sibling_id, 16, &sibling, &_state, true,
                                   arrow::schema({arrow::field("value", arrow::int64())}))
                        .ok());
    auto arrow_sibling = std::dynamic_pointer_cast<ArrowFlightResultBlockBuffer>(sibling);
    auto block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt64>({1, 2}));
    ASSERT_TRUE(arrow_sibling->add_batch(&_state, block).ok());
    auto reader = ArrowFlightBatchLocalReader::Create(_statement);
    ASSERT_TRUE(reader.ok()) << reader.status();
    _state._query_ctx_uptr.reset();
    _state._query_ctx = nullptr;
    ASSERT_TRUE((*reader)->Close().ok());
    EXPECT_FALSE(registered());
    std::shared_ptr<ArrowFlightResultBlockBuffer> found;
    EXPECT_FALSE(_mgr.find_buffer(sibling_id, found).ok());
    EXPECT_TRUE(arrow_sibling->_result_batch_queue.empty());
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

class FlightCancelCoordinator : public FrontendServiceNull {
public:
    void cancelFlightQuery(TStatus& result, const TUniqueId& result_id) override {
        if (++calls <= rejections) {
            result.__set_status_code(TStatusCode::CANCELLED);
            return;
        }
        if (result_id != buffer_id) {
            result.__set_status_code(TStatusCode::NOT_FOUND);
            return;
        }
        PCancelPlanFragmentRequest request;
        request.mutable_finst_id()->set_hi(result_id.hi);
        request.mutable_finst_id()->set_lo(result_id.lo);
        request.mutable_query_id()->set_hi(query_id.hi);
        request.mutable_query_id()->set_lo(query_id.lo);
        Status::Cancelled("Flight stream aborted").to_protobuf(request.mutable_cancel_status());
        PCancelPlanFragmentResult response;
        brpc::Controller controller;
        controller.set_timeout_ms(1000);
        stub->cancel_plan_fragment(&controller, &request, &response, nullptr);
        if (controller.Failed()) {
            Status::RpcError(controller.ErrorText()).to_thrift(&result);
        } else {
            Status::create(response.status()).to_thrift(&result);
        }
    }
    TUniqueId buffer_id;
    TUniqueId query_id;
    std::shared_ptr<PBackendService_Stub> stub;
    std::atomic<int> calls {0};
    int rejections = 0;
};

class FlightCancelServerReady : public apache::thrift::server::TServerEventHandler {
public:
    void preServe() override { ready.set_value(); }
    std::promise<void> ready;
};

class FlightCancelResultService : public PInternalService {
public:
    explicit FlightCancelResultService(ExecEnv* env) : PInternalService(env) {}

    void cancel_plan_fragment(google::protobuf::RpcController* controller,
                              const PCancelPlanFragmentRequest* request,
                              PCancelPlanFragmentResult* result,
                              google::protobuf::Closure* done) override {
        if (!legacy_cancel) {
            PInternalService::cancel_plan_fragment(controller, request, result, done);
            return;
        }
        brpc::ClosureGuard guard(done);
        // Emulate the pre-change handler: only the existing query cancellation RPC is understood.
        _exec_env->fragment_mgr()->cancel_query(UniqueId(request->query_id()).to_thrift(),
                                                Status::create(request->cancel_status()));
        Status::OK().to_protobuf(result->mutable_status());
    }
    bool legacy_cancel = false;
};

class ArrowFlightRemoteCancelTest : public ArrowFlightCancelTest {
protected:
    void SetUp() override {
        ArrowFlightCancelTest::SetUp();
        static DNSCache dns_cache;
        _previous_dns_cache = std::exchange(ExecEnv::GetInstance()->_dns_cache, &dns_cache);
        _previous_cache = std::exchange(ExecEnv::GetInstance()->_internal_client_cache, &_cache);
        auto heavy = std::exchange(config::brpc_heavy_work_pool_threads, 1);
        auto light = std::exchange(config::brpc_light_work_pool_threads, 1);
        auto flight = std::exchange(config::brpc_arrow_flight_work_pool_threads, 2);
        _previous_load_mgr = std::move(ExecEnv::GetInstance()->_load_stream_mgr);
        ExecEnv::GetInstance()->_load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
        _service = std::make_unique<FlightCancelResultService>(ExecEnv::GetInstance());
        config::brpc_heavy_work_pool_threads = heavy;
        config::brpc_light_work_pool_threads = light;
        config::brpc_arrow_flight_work_pool_threads = flight;
        ASSERT_EQ(_server.AddService(_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(_server.Start("127.0.0.1:0", nullptr), 0);
        _statement->result_addr.hostname = "127.0.0.1";
        _statement->result_addr.port = _server.listen_address().port;
        _fragment_mgr =
                std::make_unique<MockFragmentManager>(_cancel_status, ExecEnv::GetInstance());
        _fragment_mgr->stop();
        _previous_fragment_mgr =
                std::exchange(ExecEnv::GetInstance()->_fragment_mgr, _fragment_mgr.get());
        _frontend_cache = std::make_unique<ClientCache<FrontendServiceClient>>();
        _previous_frontend_cache = std::exchange(ExecEnv::GetInstance()->_frontend_client_cache,
                                                 _frontend_cache.get());
        _coordinator = std::make_shared<FlightCancelCoordinator>();
        _coordinator->buffer_id = _id;
        _coordinator->query_id = _state.query_id();
        _coordinator->stub = _cache.get_client(_statement->result_addr);
        auto processor = std::make_shared<FrontendServiceProcessor>(_coordinator);
        auto socket = std::make_shared<apache::thrift::transport::TServerSocket>("127.0.0.1", 0);
        _fe_server = std::make_unique<apache::thrift::server::TThreadedServer>(
                processor, socket,
                std::make_shared<apache::thrift::transport::TBufferedTransportFactory>(),
                std::make_shared<apache::thrift::protocol::TBinaryProtocolFactory>());
        auto ready = std::make_shared<FlightCancelServerReady>();
        _fe_server->setServerEventHandler(ready);
        _fe_thread = std::thread([this] { _fe_server->serve(); });
        ASSERT_EQ(ready->ready.get_future().wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        TFrontendInfo frontend;
        frontend.coordinator_address.hostname = "127.0.0.1";
        frontend.coordinator_address.port = socket->getPort();
        _previous_frontends = ExecEnv::GetInstance()->_frontends;
        ExecEnv::GetInstance()->update_frontends({frontend});
    }
    void TearDown() override {
        _frontend_cache.reset();
        if (_fe_server) {
            _fe_server->stop();
            _fe_thread.join();
        }
        ExecEnv::GetInstance()->_dns_cache = _previous_dns_cache;
        ExecEnv::GetInstance()->_frontend_client_cache = _previous_frontend_cache;
        ExecEnv::GetInstance()->_frontends = _previous_frontends;
        ExecEnv::GetInstance()->_fragment_mgr = _previous_fragment_mgr;
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
    std::unique_ptr<FlightCancelResultService> _service;
    brpc::Server _server;
    DNSCache* _previous_dns_cache = nullptr;
    Status _cancel_status;
    std::unique_ptr<MockFragmentManager> _fragment_mgr;
    FragmentMgr* _previous_fragment_mgr = nullptr;
    std::unique_ptr<ClientCache<FrontendServiceClient>> _frontend_cache;
    ClientCache<FrontendServiceClient>* _previous_frontend_cache = nullptr;
    std::shared_ptr<FlightCancelCoordinator> _coordinator;
    std::unique_ptr<apache::thrift::server::TThreadedServer> _fe_server;
    std::thread _fe_thread;
    std::map<TNetworkAddress, FrontendInfo> _previous_frontends;
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

TEST_F(ArrowFlightRemoteCancelTest, CancelBypassesRejectedArrowWork) {
    fill_buffer();
    auto reader = ArrowFlightBatchRemoteReader::Create(_statement);
    ASSERT_TRUE(reader.ok()) << reader.status();
    _service->_arrow_flight_work_pool.shutdown();
    EXPECT_FALSE(_service->_arrow_flight_work_pool.try_offer([] {}));
    ASSERT_TRUE((*reader)->Close().ok());
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_dep->ready());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
    EXPECT_EQ(_coordinator->calls, 1);
}

TEST_F(ArrowFlightRemoteCancelTest, RetryCoordinatorApplicationError) {
    fill_buffer();
    auto reader = ArrowFlightBatchRemoteReader::Create(_statement);
    ASSERT_TRUE(reader.ok()) << reader.status();
    _coordinator->rejections = 1;
    ASSERT_TRUE((*reader)->Close().ok());
    EXPECT_EQ(_coordinator->calls, 2);
    EXPECT_FALSE(registered());
    EXPECT_TRUE(_state.get_query_ctx()->is_cancelled());
}

TEST_F(ArrowFlightRemoteCancelTest, OlderResultBackendReceivesQueryCancellation) {
    fill_buffer();
    auto reader = ArrowFlightBatchRemoteReader::Create(_statement);
    ASSERT_TRUE(reader.ok()) << reader.status();
    _service->legacy_cancel = true;
    ASSERT_TRUE((*reader)->Close().ok());
    EXPECT_EQ(_coordinator->calls, 1);
    EXPECT_FALSE(_cancel_status.ok());
    // Older result BEs still own their historical buffer cleanup policy.
    EXPECT_TRUE(registered());
}

TEST_F(ArrowFlightRemoteCancelTest, FinishedLocalEndpointStillReportsAbort) {
    auto statement = std::make_shared<QueryStatement>(*_statement);
    statement->result_addr.hostname = BackendOptions::get_localhost();
    statement->result_addr.port = config::brpc_port;
    fill_buffer();
    bool fully_closed = false;
    ASSERT_TRUE(_buffer->close(_state.fragment_instance_id(), Status::OK(), 2, fully_closed).ok());
    auto reader = ArrowFlightBatchLocalReader::Create(statement);
    ASSERT_TRUE(reader.ok()) << reader.status();
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE((*reader)->ReadNext(&batch).ok());
    ASSERT_NE(batch, nullptr);
    _state._query_ctx_uptr.reset();
    _state._query_ctx = nullptr;
    ASSERT_TRUE((*reader)->Close().ok());
    EXPECT_EQ(_coordinator->calls, 1);
    EXPECT_FALSE(_cancel_status.ok());
    EXPECT_FALSE(registered());
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

TEST_F(ArrowFlightRemoteCancelTest, SuccessfulFragmentStopPreservesUnreadResults) {
    fill_buffer();
    auto stub = _cache.get_client(_statement->result_addr);
    for (const auto& reason : {Status::Error<ErrorCode::LIMIT_REACH>("limit reached"),
                               Status::Error<ErrorCode::FINISHED>("finished")}) {
        PCancelPlanFragmentRequest request;
        request.mutable_finst_id()->set_hi(_id.hi);
        request.mutable_finst_id()->set_lo(_id.lo);
        request.mutable_query_id()->set_hi(_state.query_id().hi);
        request.mutable_query_id()->set_lo(_state.query_id().lo);
        reason.to_protobuf(request.mutable_cancel_status());
        PCancelPlanFragmentResult response;
        brpc::Controller controller;
        controller.set_timeout_ms(1000);
        stub->cancel_plan_fragment(&controller, &request, &response, nullptr);
        ASSERT_FALSE(controller.Failed()) << controller.ErrorText();
        EXPECT_TRUE(Status::create(response.status()).ok());
        EXPECT_TRUE(registered());
        EXPECT_FALSE(_buffer->_result_batch_queue.empty());
    }
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
