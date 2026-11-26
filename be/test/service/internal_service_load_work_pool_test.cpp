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
#include <future>
#include <memory>
#include <utility>
#include <vector>

#include "common/config.h"
#include "load/channel/load_stream_mgr.h"
#include "runtime/exec_env.h"
#include "service/internal_service.h"

namespace doris {
namespace {

// Hold every worker so routing and queue rejection can be checked without running
// storage handlers. Discard queued RPCs before releasing the worker at teardown.
class PausedLoadRpcPool {
public:
    explicit PausedLoadRpcPool(FifoThreadPool& pool) : _pool(pool) {
        auto resume = _resume.get_future().share();
        for (size_t i = 0; i < _pool._threads.size(); ++i) {
            auto started = std::make_shared<std::promise<void>>();
            auto ready = started->get_future();
            CHECK(_pool.try_offer([started, resume]() {
                started->set_value();
                resume.wait();
            }));
            ready.wait();
        }
    }

    ~PausedLoadRpcPool() {
        _pool.shutdown();
        _resume.set_value();
        _pool.join();
    }

private:
    FifoThreadPool& _pool;
    std::promise<void> _resume;
};

class LoadRpcCountingClosure : public google::protobuf::Closure {
public:
    void Run() override { ++calls; }
    std::atomic<int> calls {0};
};

} // namespace

class InternalServiceLoadWorkPoolTest : public testing::TestWithParam<int> {
protected:
    void SetUp() override {
        // Keep pools and queues small and restore configuration after each test.
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
        // Use a non-default value to verify that the cancellation pool honors configuration.
        config::brpc_load_light_work_pool_threads = 3;
        _exec_env._load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
        _service = std::make_unique<PInternalService>(&_exec_env);
        for (auto* pool : {&_service->_heavy_work_pool, &_service->_light_work_pool,
                           &_service->_load_light_work_pool}) {
            _paused_pools.push_back(std::make_unique<PausedLoadRpcPool>(*pool));
        }
    }

    void TearDown() override {
        _paused_pools.clear();
        _exec_env.load_stream_mgr()->set_heavy_work_pool(nullptr);
        _service.reset();
        _exec_env._load_stream_mgr.reset();
        for (const auto& [setting, value] : _saved_config) {
            *setting = value;
        }
    }

    ExecEnv _exec_env;
    std::unique_ptr<PInternalService> _service;
    std::vector<std::unique_ptr<PausedLoadRpcPool>> _paused_pools;
    std::vector<std::pair<int32_t*, int32_t>> _saved_config;
};

TEST_F(InternalServiceLoadWorkPoolTest, CancelBypassesFullHeavyPool) {
    EXPECT_EQ(_service->_load_light_work_pool.get_active_threads(), 3);
    ASSERT_TRUE(_service->_heavy_work_pool.try_offer([] {}));

    PTabletWriterCancelRequest request;
    PTabletWriterCancelResult response;
    LoadRpcCountingClosure done;
    _service->tablet_writer_cancel(nullptr, &request, &response, &done);
    EXPECT_EQ(done.calls.load(), 0);
    EXPECT_EQ(_service->_load_light_work_pool.get_queue_size(), 1);
    EXPECT_EQ(_service->_light_work_pool.get_queue_size(), 0);

    // Cancel's protobuf response is empty; queue rejection must still run the closure once.
    _service->tablet_writer_cancel(nullptr, &request, &response, &done);
    EXPECT_EQ(done.calls.load(), 1);
}

TEST_P(InternalServiceLoadWorkPoolTest, OpenAndAddBlockKeepUsingHeavyPool) {
    ASSERT_TRUE(_service->_load_light_work_pool.try_offer([] {}));

    PTabletWriterOpenRequest open_request;
    PTabletWriterOpenResult open_response;
    POpenLoadStreamRequest stream_request;
    POpenLoadStreamResponse stream_response;
    PTabletWriterAddBlockRequest block_request;
    PTabletWriterAddBlockResult block_response;
    LoadRpcCountingClosure done;
    auto submit = [&]() {
        switch (GetParam()) {
        case 0:
            _service->tablet_writer_open(nullptr, &open_request, &open_response, &done);
            break;
        case 1:
            _service->open_load_stream(nullptr, &stream_request, &stream_response, &done);
            break;
        case 2:
            _service->tablet_writer_add_block(nullptr, &block_request, &block_response, &done);
            break;
        }
    };

    submit();
    EXPECT_EQ(done.calls.load(), 0);
    EXPECT_EQ(_service->_heavy_work_pool.get_queue_size(), 1);
    EXPECT_EQ(_service->_light_work_pool.get_queue_size(), 0);

    submit();
    EXPECT_EQ(done.calls.load(), 1);
    const auto& status = GetParam() == 0   ? open_response.status()
                         : GetParam() == 1 ? stream_response.status()
                                           : block_response.status();
    EXPECT_EQ(status.status_code(), TStatusCode::CANCELLED);
    ASSERT_EQ(status.error_msgs_size(), 1);
    EXPECT_NE(status.error_msgs(0).find("brpc_heavy"), std::string::npos);
}

INSTANTIATE_TEST_SUITE_P(HeavyLoadRequests, InternalServiceLoadWorkPoolTest,
                         testing::Values(0, 1, 2));

TEST_F(InternalServiceLoadWorkPoolTest, StreamingCloseKeepsUsingHeavyPool) {
    EXPECT_EQ(_exec_env.load_stream_mgr()->heavy_work_pool(), &_service->_heavy_work_pool);
    EXPECT_NE(_exec_env.load_stream_mgr()->heavy_work_pool(), &_service->_load_light_work_pool);
}

} // namespace doris
