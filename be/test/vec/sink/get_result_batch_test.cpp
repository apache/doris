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

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <butil/iobuf.h>
#include <gen_cpp/Data_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include "vec/sink/varrow_flight_result_writer.h"
#include "vec/sink/vmysql_result_writer.h"

namespace doris::vectorized {

class GetResultBatchCtxTest : public ::testing::Test {
public:
    GetResultBatchCtxTest() = default;
    ~GetResultBatchCtxTest() = default;
};

class MockClosure : public google::protobuf::Closure {
public:
    MockClosure(std::function<void()> cb) : _cb(cb) {}
    void Run() override { _cb(); }

private:
    std::function<void()> _cb;
};

TEST_F(GetResultBatchCtxTest, TestGetResultBatchCtx) {
    PFetchDataResult result;
    bool done_flag = false;
    MockClosure done([&]() -> void { done_flag = true; });
    auto ctx = GetResultBatchCtx::create_shared(&result, &done);

    {
        // on_failure
        Status failure = Status::InternalError("Test");
        ctx->on_failure(failure);
        EXPECT_TRUE(done_flag);
        EXPECT_EQ(result.status().status_code(), ErrorCode::INTERNAL_ERROR);
        done_flag = false;
    }

    {
        // on_close
        int64_t packet_seq = 3;
        int64_t returned_rows = 4;
        ctx->on_close(packet_seq, returned_rows);
        EXPECT_TRUE(done_flag);
        EXPECT_EQ(result.query_statistics().returned_rows(), returned_rows);
        EXPECT_EQ(result.packet_seq(), packet_seq);
        done_flag = false;
    }

    {
        // on_data with 1 row
        uint8_t test_byte = 1;
        auto num_rows = 1;
        auto packet_seq = 0;
        auto tresult = std::make_shared<TFetchDataResult>();
        tresult->result_batch.rows.resize(num_rows);
        tresult->result_batch.rows[0].append((const char*)&test_byte, sizeof(test_byte));

        EXPECT_TRUE(ctx->on_data(tresult, packet_seq, nullptr).ok());
        EXPECT_TRUE(done_flag);
        EXPECT_EQ(result.empty_batch(), false);
        EXPECT_EQ(result.packet_seq(), packet_seq);
        EXPECT_EQ(result.status().status_code(), ErrorCode::OK);
        EXPECT_GT(result.row_batch().length(), 0) << result.row_batch();
        done_flag = false;
    }

    {
        // on_data with empty result
        auto packet_seq = 0;
        auto tresult = std::shared_ptr<TFetchDataResult>();
        EXPECT_TRUE(ctx->on_data(tresult, packet_seq, nullptr).ok());
        EXPECT_TRUE(done_flag);
        EXPECT_EQ(result.empty_batch(), true);
        EXPECT_EQ(result.packet_seq(), packet_seq);
        EXPECT_EQ(result.status().status_code(), ErrorCode::OK);
        EXPECT_EQ(result.row_batch().length(), 0) << result.row_batch();
        done_flag = false;
    }

    {
        // on_data with empty result
        ctx->_max_msg_size = 0;
        uint8_t test_byte = 1;
        auto num_rows = 1;
        auto packet_seq = 0;
        auto tresult = std::make_shared<TFetchDataResult>();
        tresult->result_batch.rows.resize(num_rows);
        tresult->result_batch.rows[0].append((const char*)&test_byte, sizeof(test_byte));

        EXPECT_TRUE(ctx->on_data(tresult, packet_seq, nullptr).ok());
        EXPECT_TRUE(done_flag);
        EXPECT_EQ(result.empty_batch(), true);
        EXPECT_EQ(result.packet_seq(), packet_seq);
        EXPECT_EQ(result.status().status_code(), ErrorCode::INTERNAL_ERROR);
        EXPECT_EQ(result.row_batch().length(), 0) << result.row_batch();
        done_flag = false;
    }
}

TEST_F(GetResultBatchCtxTest, TestGetArrowResultBatchCtx) {
    PFetchArrowDataResult result;
    auto ctx = GetArrowResultBatchCtx::create_shared(&result);

    {
        // on_failure
        Status failure = Status::InternalError("Test");
        ctx->on_failure(failure);
        EXPECT_EQ(result.status().status_code(), ErrorCode::INTERNAL_ERROR);
    }

    {
        // on_close
        int64_t packet_seq = 3;
        int64_t returned_rows = 4;
        ctx->on_close(packet_seq, returned_rows);
        EXPECT_EQ(result.packet_seq(), packet_seq);
        EXPECT_EQ(result.eos(), true);
    }
}

} // namespace doris::vectorized
