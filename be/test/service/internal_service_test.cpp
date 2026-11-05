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

#include "service/internal_service.h"

#include <arrow/type.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/config.h"
#include "load/channel/load_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/async_io.h"
#include "util/uid_util.h"

namespace doris {

namespace {

class CountingClosure final : public google::protobuf::Closure {
public:
    void Run() override { ++calls; }

    std::atomic<int> calls {0};
};

} // namespace

class InternalServiceFetchTest : public testing::Test {
protected:
    void SetUp() override {
        for (size_t i = 0; i < _pool_thread_configs.size(); ++i) {
            _saved_pool_threads[i] = std::exchange(*_pool_thread_configs[i], 1);
        }
        auto* env = ExecEnv::GetInstance();
        _saved_result_mgr = std::exchange(env->_result_mgr, &_result_mgr);
        _saved_load_stream_mgr = std::move(env->_load_stream_mgr);
        env->_load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
        _saved_btls_key = btls_key;
        _saved_io_key = AsyncIO::btls_io_ctx_key;
        _service = std::make_unique<PInternalService>(env);
    }

    void TearDown() override {
        _service.reset();
        btls_key = _saved_btls_key;
        AsyncIO::btls_io_ctx_key = _saved_io_key;
        auto* env = ExecEnv::GetInstance();
        env->_load_stream_mgr = std::move(_saved_load_stream_mgr);
        env->_result_mgr = _saved_result_mgr;
        _result_mgr.stop();
        for (size_t i = 0; i < _pool_thread_configs.size(); ++i) {
            *_pool_thread_configs[i] = _saved_pool_threads[i];
        }
    }

    void create_and_cancel_buffer(bool arrow_flight) {
        std::shared_ptr<ResultBlockBufferBase> buffer;
        auto schema = arrow_flight ? std::make_shared<arrow::Schema>(
                                             std::vector<std::shared_ptr<arrow::Field>> {})
                                   : nullptr;
        ASSERT_TRUE(
                _result_mgr.create_sender(_query_id, 1024, &buffer, &_state, arrow_flight, schema)
                        .ok());
        ASSERT_TRUE(_result_mgr.cancel(_query_id, Status::Cancelled("test cancellation")));
    }

    template <typename Request, typename Result>
    void check_missing_buffer() {
        Request request;
        request.mutable_finst_id()->set_hi(_query_id.hi);
        request.mutable_finst_id()->set_lo(_query_id.lo);
        Result result;
        CountingClosure done;
        if constexpr (std::is_same_v<Request, PFetchArrowDataRequest>) {
            _service->fetch_arrow_data(nullptr, &request, &result, &done);
            // Join the worker before inspecting the response or destroying RPC arguments.
            _service->_arrow_flight_work_pool.drain_and_shutdown();
        } else {
            _service->fetch_data(nullptr, &request, &result, &done);
        }
        EXPECT_EQ(done.calls.load(), 1);
        ASSERT_TRUE(result.has_status());
        EXPECT_EQ(result.status().status_code(), ErrorCode::INTERNAL_ERROR);
        ASSERT_EQ(result.status().error_msgs_size(), 1);
        EXPECT_NE(result.status().error_msgs(0).find("Result buffer not found, finst_id=" +
                                                     print_id(_query_id)),
                  std::string::npos);
    }

    RuntimeState _state;
    ResultBufferMgr _result_mgr;
    TUniqueId _query_id = UniqueId(123, 456).to_thrift();
    std::unique_ptr<PInternalService> _service;
    ResultBufferMgr* _saved_result_mgr = nullptr;
    std::unique_ptr<LoadStreamMgr> _saved_load_stream_mgr;
    bthread_key_t _saved_btls_key {};
    bthread_key_t _saved_io_key {};
    std::array<int32_t*, 4> _pool_thread_configs {
            &config::brpc_heavy_work_pool_threads, &config::brpc_peer_fetch_pool_threads,
            &config::brpc_light_work_pool_threads, &config::brpc_arrow_flight_work_pool_threads};
    std::array<int32_t, 4> _saved_pool_threads {};
};

TEST_F(InternalServiceFetchTest, FetchDataMissingBuffer) {
    check_missing_buffer<PFetchDataRequest, PFetchDataResult>();
}

TEST_F(InternalServiceFetchTest, FetchDataRemovedBuffer) {
    ASSERT_NO_FATAL_FAILURE(create_and_cancel_buffer(false));
    check_missing_buffer<PFetchDataRequest, PFetchDataResult>();
}

TEST_F(InternalServiceFetchTest, FetchArrowDataMissingBuffer) {
    check_missing_buffer<PFetchArrowDataRequest, PFetchArrowDataResult>();
}

TEST_F(InternalServiceFetchTest, FetchArrowDataRemovedBuffer) {
    ASSERT_NO_FATAL_FAILURE(create_and_cancel_buffer(true));
    check_missing_buffer<PFetchArrowDataRequest, PFetchArrowDataResult>();
}

} // namespace doris
