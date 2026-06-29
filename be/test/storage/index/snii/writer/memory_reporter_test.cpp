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

#include "snii/writer/memory_reporter.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/status.h"

using snii::writer::MemoryReporter;

TEST(SniiMemoryReporter, StartsAtZero) {
    MemoryReporter reporter; // null callback
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiMemoryReporter, TracksPositiveAndNegativeDeltas) {
    MemoryReporter reporter;
    reporter.report(+100);
    reporter.report(+50);
    EXPECT_EQ(reporter.current_bytes(), 150);
    reporter.report(-30);
    EXPECT_EQ(reporter.current_bytes(), 120);
    reporter.report(-120);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiMemoryReporter, NullCallbackIsSafe) {
    MemoryReporter reporter(nullptr); // explicit null ConsumeReleaseFn
    reporter.report(+10);
    reporter.report(-10);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiMemoryReporter, CallbackFiresWithSameDelta) {
    std::vector<int64_t> sink;
    MemoryReporter reporter([&sink](int64_t delta) { sink.push_back(delta); });
    reporter.report(+100);
    reporter.report(-40);
    ASSERT_EQ(sink.size(), 2U);
    EXPECT_EQ(sink[0], 100);
    EXPECT_EQ(sink[1], -40);
    EXPECT_EQ(reporter.current_bytes(), 60);
}

TEST(SniiMemoryReporter, CallbackSumMirrorsCurrentBytes) {
    int64_t external_total = 0;
    MemoryReporter reporter([&external_total](int64_t delta) { external_total += delta; });
    reporter.report(+100);
    reporter.report(+250);
    reporter.report(-75);
    reporter.report(+12);
    reporter.report(-200);
    EXPECT_EQ(external_total, reporter.current_bytes());
}

TEST(SniiMemoryReporter, ZeroDeltaIsNoOpButStillReports) {
    int fire_count = 0;
    MemoryReporter reporter([&fire_count](int64_t) { ++fire_count; });
    reporter.report(+100);
    EXPECT_EQ(reporter.current_bytes(), 100);
    reporter.report(0);
    EXPECT_EQ(reporter.current_bytes(), 100);
    EXPECT_EQ(fire_count, 2); // report(0) still fires the callback once
}
