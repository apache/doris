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

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "load/channel/load_stream_mgr.h"
#include "runtime/exec_env.h"
#include "service/internal_service.h"
#include "util/time.h"

namespace doris {

class PointQueryRpcBatchTest : public testing::Test {
protected:
    PTabletKeyLookupBatchRequest request;
    PTabletKeyLookupBatchResponse response;
    int64_t now = 0;
    int calls = 0;
    bool cancelled = false;

    void add(int64_t tablet_id, uint32_t timeout_ms = 100) {
        auto* item = request.add_items();
        item->mutable_request()->set_tablet_id(tablet_id);
        item->set_remaining_timeout_ms(timeout_ms);
    }

    void execute(const std::function<Status(const PTabletKeyLookupRequest*,
                                            PTabletKeyLookupResponse*)>& handler) {
        PInternalService::_execute_tablet_fetch_data_batch(
                request, &response, 0,
                [&](const auto* req, auto* res) {
                    ++calls;
                    return handler(req, res);
                },
                [&]() { return cancelled; }, [&]() { return now; });
    }
};

TEST_F(PointQueryRpcBatchTest, ValidateCountRequiredFieldsAndTimeout) {
    EXPECT_FALSE(PInternalService::_validate_tablet_fetch_data_batch(request).ok());
    for (int i = 0; i < 8; ++i) {
        add(i);
        EXPECT_TRUE(PInternalService::_validate_tablet_fetch_data_batch(request).ok());
    }
    add(8);
    EXPECT_FALSE(PInternalService::_validate_tablet_fetch_data_batch(request).ok());
    request.mutable_items()->RemoveLast();
    request.mutable_items(0)->set_remaining_timeout_ms(0);
    EXPECT_FALSE(PInternalService::_validate_tablet_fetch_data_batch(request).ok());
    request.mutable_items(0)->set_remaining_timeout_ms(1);
    request.mutable_items(0)->clear_request();
    EXPECT_FALSE(PInternalService::_validate_tablet_fetch_data_batch(request).ok());
}

TEST_F(PointQueryRpcBatchTest, RejectOversizeEnvelope) {
    add(1);
    request.mutable_items(0)->mutable_request()->set_desc_tbl(std::string(1024 * 1024, 'x'));
    EXPECT_FALSE(PInternalService::_validate_tablet_fetch_data_batch(request).ok());
    request.mutable_items(0)->mutable_request()->set_desc_tbl(std::string(1024 * 1024 - 100, 'x'));
    EXPECT_TRUE(PInternalService::_validate_tablet_fetch_data_batch(request).ok());
}

TEST_F(PointQueryRpcBatchTest, KeepResultOrderAndIsolateLookupFailuresAndCacheMisses) {
    for (int i = 0; i < 4; ++i) {
        add(i);
    }
    execute([](const auto* req, auto* res) {
        switch (req->tablet_id()) {
        case 0:
            res->set_row_batch("partial result must be discarded");
            return Status::InternalError("lookup failed");
        case 1:
            res->set_need_resend_query_context(true);
            break;
        case 2:
            res->set_empty_batch(true);
            break;
        default:
            res->set_row_batch("last result");
        }
        return Status::OK();
    });
    ASSERT_EQ(4, calls);
    ASSERT_EQ(4, response.results_size());
    EXPECT_EQ(0, response.status().status_code());
    EXPECT_NE(0, response.results(0).status().status_code());
    EXPECT_FALSE(response.results(0).has_row_batch());
    EXPECT_TRUE(response.results(1).need_resend_query_context());
    EXPECT_TRUE(response.results(2).empty_batch());
    EXPECT_EQ("last result", response.results(3).row_batch());
    EXPECT_EQ(0, response.results(3).status().status_code());
}

TEST_F(PointQueryRpcBatchTest, IncludeQueueWaitAndPreviousLookupsInItemDeadlines) {
    add(0, 1);
    add(1, 10);
    add(2, 50);
    now = NANOS_PER_MILLIS;
    execute([&](const auto*, auto* res) {
        now += 10 * NANOS_PER_MILLIS;
        res->set_row_batch("row");
        return Status::OK();
    });
    EXPECT_EQ(2, calls);
    EXPECT_NE(0, response.results(0).status().status_code());
    EXPECT_NE(0, response.results(1).status().status_code());
    EXPECT_FALSE(response.results(1).has_row_batch());
    EXPECT_EQ(0, response.results(2).status().status_code());
    EXPECT_EQ("row", response.results(2).row_batch());
}

TEST_F(PointQueryRpcBatchTest, CancellationBeforeExecutionSkipsAllLookups) {
    add(0);
    add(1);
    cancelled = true;
    execute([](const auto*, auto*) { return Status::OK(); });
    EXPECT_EQ(0, calls);
    ASSERT_EQ(2, response.results_size());
    EXPECT_NE(0, response.results(0).status().status_code());
    EXPECT_NE(0, response.results(1).status().status_code());
}

TEST_F(PointQueryRpcBatchTest, CancellationDuringExecutionDiscardsPartialResults) {
    add(0);
    add(1);
    execute([&](const auto*, auto* res) {
        res->set_row_batch("row");
        cancelled = true;
        return Status::OK();
    });
    EXPECT_EQ(1, calls);
    EXPECT_FALSE(response.results(0).has_row_batch());
    EXPECT_NE(0, response.results(0).status().status_code());
    EXPECT_NE(0, response.results(1).status().status_code());
}

TEST_F(PointQueryRpcBatchTest, DorisExceptionIsAnItemError) {
    add(0);
    add(1);
    execute([](const auto* req, auto* res) {
        if (req->tablet_id() == 0) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "injected failure");
        }
        res->set_empty_batch(true);
        return Status::OK();
    });
    EXPECT_EQ(2, calls);
    EXPECT_NE(0, response.results(0).status().status_code());
    EXPECT_EQ(0, response.results(1).status().status_code());
}

TEST_F(PointQueryRpcBatchTest, RejectedRpcCompletesClosureOnceWithoutPartialResults) {
    auto heavy_threads = std::exchange(config::brpc_heavy_work_pool_threads, 1);
    auto peer_threads = std::exchange(config::brpc_peer_fetch_pool_threads, 1);
    auto light_threads = std::exchange(config::brpc_light_work_pool_threads, 1);
    auto arrow_threads = std::exchange(config::brpc_arrow_flight_work_pool_threads, 1);
    Defer restore {[&]() {
        config::brpc_heavy_work_pool_threads = heavy_threads;
        config::brpc_peer_fetch_pool_threads = peer_threads;
        config::brpc_light_work_pool_threads = light_threads;
        config::brpc_arrow_flight_work_pool_threads = arrow_threads;
    }};
    auto* exec_env = ExecEnv::GetInstance();
    auto original_stream_mgr =
            std::exchange(exec_env->_load_stream_mgr, std::make_unique<LoadStreamMgr>(1));
    Defer restore_stream_mgr {
            [&]() { exec_env->_load_stream_mgr = std::move(original_stream_mgr); }};
    PInternalService service(exec_env);
    service._light_work_pool.shutdown();
    add(1);
    class Closure final : public google::protobuf::Closure {
    public:
        void Run() override { ++runs; }
        int runs = 0;
    } done;
    brpc::Controller controller;
    service.tablet_fetch_data_batch(&controller, &request, &response, &done);
    EXPECT_EQ(1, done.runs);
    EXPECT_NE(0, response.status().status_code());
    EXPECT_EQ(0, response.results_size());
    EXPECT_TRUE(response.IsInitialized());

    response.Clear();
    request.Clear();
    service.tablet_fetch_data_batch(&controller, &request, &response, &done);
    EXPECT_EQ(2, done.runs);
    EXPECT_NE(0, response.status().status_code());
    EXPECT_EQ(0, response.results_size());
    EXPECT_TRUE(response.IsInitialized());
}

} // namespace doris
