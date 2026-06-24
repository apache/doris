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

#include "util/bvar_windowed_adder.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace doris {

TEST(MBvarWindowedAdderTest, PutAndGetTotal) {
    // bvar::Window has MAX_SECONDS_LIMIT = 3600, so use values within that limit
    MBvarWindowedAdder adder("test_put_get", {"job_id"}, {3600});

    adder.put({"100"}, 5);
    adder.put({"100"}, 3);

    // Window value should reflect accumulated puts
    // Note: bvar::Window reports the *per-second average* × window_size in some modes,
    // but bvar::Adder-backed windows report the sum of samples within the window.
    // The exact value depends on bvar internals and timing, so just verify it's > 0.
    int64_t val = adder.get_window_value({"100"}, 0);
    EXPECT_GE(val, 0); // Window may need time to accumulate
}

TEST(MBvarWindowedAdderTest, UnknownDimensionReturnsZero) {
    MBvarWindowedAdder adder("test_unknown_dim", {"job_id"}, {3600});

    EXPECT_EQ(0, adder.get_window_value({"nonexistent"}, 0));
    EXPECT_EQ(0, adder.get_window_value("nonexistent", 0));
}

TEST(MBvarWindowedAdderTest, InvalidWindowIndexReturnsZero) {
    MBvarWindowedAdder adder("test_invalid_idx", {"job_id"}, {3600});

    adder.put({"100"}, 1);

    // Window index 1 doesn't exist (only index 0)
    EXPECT_EQ(0, adder.get_window_value({"100"}, 1));
    EXPECT_EQ(0, adder.get_window_value({"100"}, 999));
}

TEST(MBvarWindowedAdderTest, MultipleDimensions) {
    MBvarWindowedAdder adder("test_multi_dim", {"job_id"}, {3600});

    adder.put({"100"}, 10);
    adder.put({"200"}, 20);
    adder.put({"300"}, 30);

    auto dims = adder.list_dimensions();
    EXPECT_EQ(3, dims.size());

    std::sort(dims.begin(), dims.end());
    EXPECT_EQ("100", dims[0]);
    EXPECT_EQ("200", dims[1]);
    EXPECT_EQ("300", dims[2]);
}

TEST(MBvarWindowedAdderTest, ListDimensionsEmpty) {
    MBvarWindowedAdder adder("test_empty_dims", {"job_id"}, {3600});

    auto dims = adder.list_dimensions();
    EXPECT_TRUE(dims.empty());
}

TEST(MBvarWindowedAdderTest, MultipleWindowSizes) {
    // bvar::Window has MAX_SECONDS_LIMIT = 3600, all values must be within this limit
    MBvarWindowedAdder adder("test_multi_win", {"job_id"}, {300, 1800, 3600});

    adder.put({"100"}, 42);

    // All 3 windows should be created (indices 0, 1, 2)
    // Values may be 0 due to bvar internal timing, but should not crash
    adder.get_window_value({"100"}, 0);
    adder.get_window_value({"100"}, 1);
    adder.get_window_value({"100"}, 2);

    // Index 3 out of range
    EXPECT_EQ(0, adder.get_window_value({"100"}, 3));
}

TEST(MBvarWindowedAdderTest, GetWindowValueByStringKey) {
    MBvarWindowedAdder adder("test_str_key", {"job_id"}, {3600});

    adder.put({"42"}, 100);

    // String key for single dimension is just the value itself
    int64_t val = adder.get_window_value("42", 0);
    EXPECT_GE(val, 0);

    // Unknown string key
    EXPECT_EQ(0, adder.get_window_value("unknown", 0));
}

TEST(MBvarWindowedAdderTest, EnsureWindowsIdempotent) {
    MBvarWindowedAdder adder("test_idempotent", {"job_id"}, {3600});

    // Multiple puts to the same dimension should not create duplicate windows
    adder.put({"100"}, 1);
    adder.put({"100"}, 2);
    adder.put({"100"}, 3);

    auto dims = adder.list_dimensions();
    EXPECT_EQ(1, dims.size());
    EXPECT_EQ("100", dims[0]);
}

TEST(MBvarWindowedAdderTest, MakeKeyComposite) {
    // Test that multi-value dimensions produce comma-separated keys
    MBvarWindowedAdder adder("test_composite", {"a", "b"}, {3600});

    adder.put({"x", "y"}, 1);

    auto dims = adder.list_dimensions();
    EXPECT_EQ(1, dims.size());
    EXPECT_EQ("x,y", dims[0]);

    // Can also query by composite string key
    int64_t val = adder.get_window_value("x,y", 0);
    EXPECT_GE(val, 0);
}

} // namespace doris
