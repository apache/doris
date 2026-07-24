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

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <utility>

#include "common/status.h"
#include "load/channel/load_stream_mgr.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "runtime/exec_env.h"
#include "service/internal_service.h"

namespace doris {

class InternalServiceStreamLoadStatusTest : public testing::Test {
public:
    void SetUp() override {
        _env = ExecEnv::GetInstance();
        _env->set_load_stream_mgr(std::make_unique<LoadStreamMgr>(1));
        _env->set_new_load_stream_mgr(NewLoadStreamMgr::create_unique());
    }

    void TearDown() override {
        _env->clear_new_load_stream_mgr();
        _env->clear_load_stream_mgr();
    }

protected:
    ExecEnv* _env = nullptr;
};

class MockClosure : public google::protobuf::Closure {
public:
    explicit MockClosure(std::function<void()> cb) : _cb(std::move(cb)) {}

    void Run() override { _cb(); }

private:
    std::function<void()> _cb;
};

TEST_F(InternalServiceStreamLoadStatusTest,
       report_stream_load_status_returns_error_for_unknown_id) {
    PInternalService service(_env);
    PReportStreamLoadStatusRequest request;
    PReportStreamLoadStatusResponse response;
    bool done = false;
    MockClosure closure([&done]() { done = true; });
    UniqueId load_id = UniqueId::gen_uid();
    request.mutable_load_id()->set_hi(load_id.hi);
    request.mutable_load_id()->set_lo(load_id.lo);
    Status::OK().to_protobuf(request.mutable_status());

    service.report_stream_load_status(nullptr, &request, &response, &closure);

    EXPECT_TRUE(done);
    Status result = Status::create(response.status());
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.to_string().find("unknown stream load id"), std::string::npos);
}

TEST_F(InternalServiceStreamLoadStatusTest, report_stream_load_status_uses_request_status) {
    PInternalService service(_env);
    auto ctx = std::make_shared<StreamLoadContext>(_env);
    ASSERT_TRUE(_env->new_load_stream_mgr()->put(ctx->id, ctx).ok());

    PReportStreamLoadStatusRequest request;
    PReportStreamLoadStatusResponse response;
    bool done = false;
    MockClosure closure([&done]() { done = true; });
    request.mutable_load_id()->set_hi(ctx->id.hi);
    request.mutable_load_id()->set_lo(ctx->id.lo);
    Status::InternalError("reported failure").to_protobuf(request.mutable_status());

    service.report_stream_load_status(nullptr, &request, &response, &closure);

    EXPECT_TRUE(done);
    Status response_status = Status::create(response.status());
    EXPECT_TRUE(response_status.ok());
    ASSERT_EQ(ctx->load_status_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    Status load_status = ctx->load_status_future.get();
    EXPECT_FALSE(load_status.ok());
    EXPECT_NE(load_status.to_string().find("reported failure"), std::string::npos);
}

} // namespace doris
