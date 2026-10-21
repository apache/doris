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

#include <chrono>
#include <future>
#include <utility>

#include "common/config.h"
#include "load/channel/load_stream_mgr.h"
#include "runtime/exec_env.h"
#include "service/internal_service.h"

namespace doris {
namespace {

class PrewarmClosure : public google::protobuf::Closure {
public:
    void Run() override { completed.set_value(); }
    std::promise<void> completed;
};

class LanceIndexPrewarmServiceTest : public testing::Test {
protected:
    void SetUp() override {
        auto heavy = std::exchange(config::brpc_heavy_work_pool_threads, 1);
        auto light = std::exchange(config::brpc_light_work_pool_threads, 1);
        auto peer = std::exchange(config::brpc_peer_fetch_pool_threads, 1);
        auto flight = std::exchange(config::brpc_arrow_flight_work_pool_threads, 1);
        auto* env = ExecEnv::GetInstance();
        previous_load_mgr = std::move(env->_load_stream_mgr);
        env->_load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
        service = std::make_unique<PInternalService>(env);
        config::brpc_heavy_work_pool_threads = heavy;
        config::brpc_light_work_pool_threads = light;
        config::brpc_peer_fetch_pool_threads = peer;
        config::brpc_arrow_flight_work_pool_threads = flight;
    }

    void TearDown() override {
        service.reset();
        ExecEnv::GetInstance()->_load_stream_mgr = std::move(previous_load_mgr);
    }

    std::unique_ptr<PInternalService> service;
    std::unique_ptr<LoadStreamMgr> previous_load_mgr;
};

TEST_F(LanceIndexPrewarmServiceTest, PrewarmDoesNotUseTheSharedHeavyWorker) {
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    service->_heavy_work_pool.try_offer([&, released] {
        entered.set_value();
        released.wait();
    });
    entered.get_future().wait();
    PLanceIndexPrewarmRequest request;
    request.set_timeout_ms(0);
    PLanceIndexPrewarmResponse response;
    PrewarmClosure done;
    auto completed = done.completed.get_future();
    service->prewarm_lance_index(nullptr, &request, &response, &done);
    auto result = completed.wait_for(std::chrono::seconds(2));
    release.set_value();
    completed.wait();
    EXPECT_EQ(std::future_status::ready, result);
    EXPECT_FALSE(Status::create(response.status()).ok());
}

TEST_F(LanceIndexPrewarmServiceTest, StalledPrewarmHasBoundedAdmissionAndExpiredQueueIsRejected) {
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    service->_lance_index_prewarm_pool.try_offer([&, released] {
        entered.set_value();
        released.wait();
    });
    entered.get_future().wait();
    PLanceIndexPrewarmRequest request;
    request.set_timeout_ms(0);
    PLanceIndexPrewarmResponse queued_response;
    PLanceIndexPrewarmResponse rejected_response;
    PrewarmClosure queued_done;
    PrewarmClosure rejected_done;
    auto queued = queued_done.completed.get_future();
    auto rejected = rejected_done.completed.get_future();
    service->prewarm_lance_index(nullptr, &request, &queued_response, &queued_done);
    service->prewarm_lance_index(nullptr, &request, &rejected_response, &rejected_done);
    auto rejection = rejected.wait_for(std::chrono::seconds(2));

    // Stalled native work must not occupy the shared RPC worker or create more prewarm workers.
    std::promise<void> heavy_done;
    service->_heavy_work_pool.try_offer([&] { heavy_done.set_value(); });
    auto heavy_future = heavy_done.get_future();
    auto heavy = heavy_future.wait_for(std::chrono::seconds(2));
    release.set_value();
    queued.wait();
    rejected.wait();
    heavy_future.wait();
    EXPECT_EQ(std::future_status::ready, rejection);
    EXPECT_EQ(std::future_status::ready, heavy);
    EXPECT_FALSE(Status::create(rejected_response.status()).ok());
    EXPECT_TRUE(Status::create(queued_response.status()).is<ErrorCode::TIMEOUT>());
}

} // namespace
} // namespace doris
