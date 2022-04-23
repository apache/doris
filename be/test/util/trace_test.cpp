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

#include "util/trace.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <cctype>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include "gutil/macros.h"
#include "gutil/port.h"
#include "gutil/ref_counted.h"
#include "gutil/walltime.h"
#include "util/countdown_latch.h"
#include "util/scoped_cleanup.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"
#include "util/trace_metrics.h"

using rapidjson::Document;
using rapidjson::Value;
using std::string;
using std::thread;
using std::vector;

namespace doris {

class TraceTest : public ::testing::Test {};

// Replace all digits in 's' with the character 'X'.
static std::string XOutDigits(const string& s) {
    std::string ret;
    ret.reserve(s.size());
    for (char c : s) {
        if (isdigit(c)) {
            ret.push_back('X');
        } else {
            ret.push_back(c);
        }
    }
    return ret;
}

TEST_F(TraceTest, TestBasic) {
    scoped_refptr<Trace> t(new Trace);
    TRACE_TO(t, "hello $0, $1", "world", 12345);
    TRACE_TO(t, "goodbye $0, $1", "cruel world", 54321);

    std::string result = XOutDigits(t->DumpToString(Trace::NO_FLAGS));
    EXPECT_EQ(
            "XXXX XX:XX:XX.XXXXXX trace_test.cpp:XX] hello world, XXXXX\n"
            "XXXX XX:XX:XX.XXXXXX trace_test.cpp:XX] goodbye cruel world, XXXXX\n",
            result);
}

TEST_F(TraceTest, TestAttach) {
    scoped_refptr<Trace> traceA(new Trace);
    scoped_refptr<Trace> traceB(new Trace);
    {
        ADOPT_TRACE(traceA.get());
        EXPECT_EQ(traceA.get(), Trace::CurrentTrace());
        {
            ADOPT_TRACE(traceB.get());
            EXPECT_EQ(traceB.get(), Trace::CurrentTrace());
            TRACE("hello from traceB");
        }
        EXPECT_EQ(traceA.get(), Trace::CurrentTrace());
        TRACE("hello from traceA");
    }
    EXPECT_TRUE(Trace::CurrentTrace() == nullptr);
    TRACE("this goes nowhere");

    EXPECT_EQ("XXXX XX:XX:XX.XXXXXX trace_test.cpp:XX] hello from traceA\n",
              XOutDigits(traceA->DumpToString(Trace::NO_FLAGS)));
    EXPECT_EQ("XXXX XX:XX:XX.XXXXXX trace_test.cpp:XX] hello from traceB\n",
              XOutDigits(traceB->DumpToString(Trace::NO_FLAGS)));
}

TEST_F(TraceTest, TestChildTrace) {
    scoped_refptr<Trace> traceA(new Trace);
    scoped_refptr<Trace> traceB(new Trace);
    ADOPT_TRACE(traceA.get());
    traceA->AddChildTrace("child", traceB.get());
    TRACE("hello from traceA");
    {
        ADOPT_TRACE(traceB.get());
        TRACE("hello from traceB");
    }
    EXPECT_EQ(
            "XXXX XX:XX:XX.XXXXXX trace_test.cpp:XXX] hello from traceA\n"
            "Related trace 'child':\n"
            "XXXX XX:XX:XX.XXXXXX trace_test.cpp:XXX] hello from traceB\n",
            XOutDigits(traceA->DumpToString(Trace::NO_FLAGS)));
}

TEST_F(TraceTest, TestTraceMetrics) {
    scoped_refptr<Trace> trace(new Trace);
    trace->metrics()->Increment("foo", 10);
    trace->metrics()->Increment("bar", 10);
    for (int i = 0; i < 1000; i++) {
        trace->metrics()->Increment("baz", i);
    }
    EXPECT_EQ("{\"bar\":10,\"baz\":499500,\"foo\":10}", trace->MetricsAsJSON());

    {
        ADOPT_TRACE(trace.get());
        TRACE_COUNTER_SCOPE_LATENCY_US("test_scope_us");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    auto m = trace->metrics()->Get();
    EXPECT_GE(m["test_scope_us"], 80 * 1000);
}

} // namespace doris
