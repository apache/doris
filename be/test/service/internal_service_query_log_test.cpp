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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/query_log_context.h"
#include "load/channel/load_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "service/internal_service.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

class QueryRpcLogSink : public google::LogSink {
public:
    QueryRpcLogSink() { google::AddLogSink(this); }
    ~QueryRpcLogSink() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity severity, const char* /*full_filename*/,
              const char* /*base_filename*/, int /*line*/, const google::LogMessageTime& /*time*/,
              const char* message, std::size_t message_len) override {
        const std::string text(message, message_len);
        if (severity == google::GLOG_WARNING &&
            text.find("query-log-rpc-test") != std::string::npos) {
            std::lock_guard lock(_mutex);
            _identities.push_back(current_query_log_identity());
        }
    }

    std::vector<QueryLogIdentity> identities() {
        std::lock_guard lock(_mutex);
        return _identities;
    }

private:
    std::mutex _mutex;
    std::vector<QueryLogIdentity> _identities;
};

class QueryRpcDone : public google::protobuf::Closure {
public:
    void Run() override { ++calls; }
    std::atomic<int> calls {0};
};

} // namespace

class InternalServiceQueryLogTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_enabled = config::sys_log_enable_query_id;
        config::sys_log_enable_query_id = true;
        init_query_log_context();
        _scope.reset(QueryLogIdentity {});
        // As in InternalServiceLoadWorkPoolTest, avoid the production pool sizes in a unit test.
        for (auto* setting :
             {&config::brpc_heavy_work_pool_threads, &config::brpc_heavy_work_pool_max_queue_size,
              &config::brpc_light_work_pool_threads, &config::brpc_light_work_pool_max_queue_size,
              &config::brpc_peer_fetch_pool_threads, &config::brpc_peer_fetch_pool_max_queue_size,
              &config::brpc_arrow_flight_work_pool_threads,
              &config::brpc_arrow_flight_work_pool_max_queue_size,
              &config::brpc_load_light_work_pool_threads,
              &config::brpc_load_light_work_pool_max_queue_size}) {
            _saved_config.emplace_back(setting, *setting);
            *setting = 1;
        }
        config::brpc_arrow_flight_work_pool_max_queue_size = 2;
        _exec_env._load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
        _service = std::make_unique<PInternalService>(&_exec_env);
        // RPC handlers use the singleton. Reuse its manager if the test runner initialized one.
        _manager = ExecEnv::GetInstance()->result_mgr();
        if (_manager == nullptr) {
            _owned_manager = std::make_unique<ResultBufferMgr>();
            _manager = _owned_manager.get();
            ExecEnv::GetInstance()->_result_mgr = _manager;
        }
        _query_id.hi = 0x68694;
        _query_id.lo = 1;
        _buffer_id = _query_id;
        _buffer_id.lo = 9;
        {
            MockRuntimeState state;
            state._query_id = _query_id;
            state._fragment_instance_id = _buffer_id;
            ASSERT_TRUE(_manager->create_sender(_buffer_id, 16, &_buffer, &state, true).ok());
        }
        // The RPC must report the buffer's error even after its RuntimeState has gone away.
        _buffer->cancel(Status::InternalError("query-log-rpc-test"));
    }

    void TearDown() override {
        _exec_env.load_stream_mgr()->set_heavy_work_pool(nullptr);
        _service.reset();
        _manager->cancel(_buffer_id, Status::Cancelled("query log test cleanup"));
        _buffer.reset();
        if (_owned_manager) {
            _owned_manager->stop();
            ExecEnv::GetInstance()->_result_mgr = nullptr;
            _owned_manager.reset();
        }
        _exec_env._load_stream_mgr.reset();
        for (const auto& [setting, value] : _saved_config) {
            *setting = value;
        }
        config::sys_log_enable_query_id = _saved_enabled;
    }

    void check_failed_arrow_rpc(bool schema) {
        PFetchArrowDataRequest data_request;
        PFetchArrowDataResult data_response;
        PFetchArrowFlightSchemaRequest schema_request;
        PFetchArrowFlightSchemaResult schema_response;
        for (auto* finst_id :
             {data_request.mutable_finst_id(), schema_request.mutable_finst_id()}) {
            finst_id->set_hi(_buffer_id.hi);
            finst_id->set_lo(_buffer_id.lo);
        }
        TUniqueId caller;
        caller.hi = 5;
        caller.lo = 6;
        ScopedQueryLogContext unrelated {QueryLogIdentity(caller)};
        QueryRpcLogSink logs;
        QueryRpcDone done;
        if (schema) {
            _service->fetch_arrow_flight_schema(nullptr, &schema_request, &schema_response, &done);
        } else {
            _service->fetch_arrow_data(nullptr, &data_request, &data_response, &done);
        }
        std::promise<QueryLogIdentity> next_task;
        auto completed = next_task.get_future();
        EXPECT_TRUE(_service->_arrow_flight_work_pool.offer(
                [&] { next_task.set_value(current_query_log_identity()); }));
        // Fence the same single worker: responses, post-get_batch/get_schema logs and scope teardown
        // must finish before assertions or destruction of request/response/closure objects.
        const auto completion = completed.wait_for(std::chrono::seconds(10));
        _service->_arrow_flight_work_pool.shutdown();
        _service->_arrow_flight_work_pool.join();
        ASSERT_EQ(std::future_status::ready, completion);
        const auto next_task_identity = completed.get();
        EXPECT_EQ(1, done.calls.load());
        const auto& status = schema ? schema_response.status() : data_response.status();
        EXPECT_EQ(TStatusCode::INTERNAL_ERROR, status.status_code());
        ASSERT_EQ(1, status.error_msgs_size());
        EXPECT_NE(std::string::npos, status.error_msgs(0).find("query-log-rpc-test"));
        const auto identities = logs.identities();
        ASSERT_EQ(1, identities.size());
        EXPECT_EQ(_query_id.hi, identities[0].query_hi);
        EXPECT_EQ(_query_id.lo, identities[0].query_lo);
        EXPECT_EQ(0, identities[0].instance_hi);
        EXPECT_EQ(0, identities[0].instance_lo);
        EXPECT_EQ(0, next_task_identity.query_hi);
        EXPECT_EQ(0, next_task_identity.query_lo);
        EXPECT_EQ(0, next_task_identity.instance_hi);
        EXPECT_EQ(0, next_task_identity.instance_lo);
        EXPECT_EQ(caller.hi, current_query_log_identity().query_hi);
        EXPECT_EQ(caller.lo, current_query_log_identity().query_lo);
    }

private:
    bool _saved_enabled = false;
    ScopedQueryLogContext _scope;
    ExecEnv _exec_env;
    std::unique_ptr<PInternalService> _service;
    std::vector<std::pair<int32_t*, int32_t>> _saved_config;
    ResultBufferMgr* _manager = nullptr;
    std::unique_ptr<ResultBufferMgr> _owned_manager;
    TUniqueId _query_id;
    TUniqueId _buffer_id;
    std::shared_ptr<ResultBlockBufferBase> _buffer;
};

TEST_F(InternalServiceQueryLogTest, ArrowFetchErrorLogUsesQueryNotBufferId) {
    check_failed_arrow_rpc(false);
}

TEST_F(InternalServiceQueryLogTest, ArrowSchemaErrorLogUsesQueryNotBufferId) {
    check_failed_arrow_rpc(true);
}

} // namespace doris
