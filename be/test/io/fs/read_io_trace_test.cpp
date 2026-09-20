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

#include "io/fs/read_io_trace.h"

#include <gtest/gtest.h>

#include "io/fs/read_io_trace_test_util.h"
#include "io/io_common.h"

namespace doris::io {

TEST(ReadIOTraceTest, DisabledTracingEmitsNothingAndAllocatesNoIdentity) {
    ReadIOTraceCapture capture;
    config::enable_read_io_trace = false;
    EXPECT_EQ(ReadIOTrace::next_id(), 0);
    ReadIOTrace::record({.event = "test"});
    EXPECT_TRUE(capture.events("test").empty());
}

TEST(ReadIOTraceTest, EscapesIdentityAndBoundsLogVolume) {
    ReadIOTraceCapture capture;
    TUniqueId query;
    query.hi = 1;
    query.lo = 2;
    IOContext context;
    context.query_id = &query;
    context.read_trace_source = FileReadTraceSource::HOLE_FILL;
    const auto id = ReadIOTrace::next_id();
    ASSERT_GT(id, 0);
    ReadIOTrace::record({.event = "test",
                         .context = &context,
                         .file = "s3://bucket/a\"b\n",
                         .id = id,
                         .parent_id = 123,
                         .offset = 17,
                         .size = 29,
                         .time_ns = 456,
                         .start_ns = 400,
                         .bytes = 29});
    auto events = capture.events("test");
    ASSERT_EQ(events.size(), 1);
    EXPECT_STREQ(events[0]["file"].GetString(), "s3://bucket/a\"b\n");
    EXPECT_STREQ(events[0]["source"].GetString(), "hole_fill");
    EXPECT_EQ(events[0]["id"].GetUint64(), id);
    EXPECT_EQ(events[0]["parent_id"].GetUint64(), 123);
    EXPECT_EQ(events[0]["time_ns"].GetInt64(), 456);
    config::read_io_trace_max_events = static_cast<int64_t>(events[0]["seq"].GetUint64()) + 1;
    ReadIOTrace::record({.event = "test"});
    ReadIOTrace::record({.event = "test"});
    EXPECT_EQ(capture.events("test").size(), 2);
}

} // namespace doris::io
