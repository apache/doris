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

#include <memory>

#include "common/status.h"
#include "io/fs/file_range_read_scheduler.h"
#include "runtime/query_context.h"
#include "testutil/mock/mock_query_context.h"
#include "util/threadpool.h"

namespace doris {
namespace {

ThreadPool* read_ahead_test_executor() {
    static std::unique_ptr<ThreadPool> executor = []() {
        std::unique_ptr<ThreadPool> result;
        Status status = ThreadPoolBuilder("QueryReadAheadTest")
                                .set_min_threads(1)
                                .set_max_threads(4)
                                .build(&result);
        DORIS_CHECK(status.ok());
        return result;
    }();
    return executor.get();
}

std::unique_ptr<io::FileRangeReadScheduler> create_range_scheduler() {
    io::FileRangeReadSchedulerOptions options {
            .max_bytes_per_query = 4096,
            .max_bytes_per_be = 8192,
    };
    std::unique_ptr<io::FileRangeReadScheduler> scheduler;
    EXPECT_TRUE(io::FileRangeReadScheduler::create(options, read_ahead_test_executor(), &scheduler)
                        .ok());
    return scheduler;
}

} // namespace

TEST(QueryContextReadAheadTest, SharesAndCancelsRangeContext) {
    auto scheduler = create_range_scheduler();
    auto query = MockQueryContext::create();

    auto first = query->get_or_create_file_range_read_context(scheduler.get());
    auto second = query->get_or_create_file_range_read_context(scheduler.get());
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first, second);
    EXPECT_FALSE(first->cancelled());

    query->cancel(Status::Cancelled("cancel read-ahead test query"));
    EXPECT_TRUE(first->cancelled());
}

TEST(QueryContextReadAheadTest, CreatesCancelledContextAfterQueryCancellation) {
    auto scheduler = create_range_scheduler();
    auto query = MockQueryContext::create();
    query->cancel(Status::Cancelled("cancel before creating read-ahead context"));

    auto range_context = query->get_or_create_file_range_read_context(scheduler.get());
    ASSERT_NE(range_context, nullptr);
    EXPECT_TRUE(range_context->cancelled());
}

} // namespace doris
