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

#include "gutil/sysinfo.h"

#include <gtest/gtest.h>

namespace doris {

TEST(SysInfoTest, ReadMaxCpuIndexTest) {
    using base::ParseMaxCpuIndex;
    EXPECT_EQ(0, ParseMaxCpuIndex("0\n"));
    EXPECT_EQ(1, ParseMaxCpuIndex("1\n"));
    EXPECT_EQ(7, ParseMaxCpuIndex("0-7\n"));
    EXPECT_EQ(40, ParseMaxCpuIndex("0-7,30-40\n"));
    EXPECT_EQ(143, ParseMaxCpuIndex("2,4-127,128-143\n"));
    EXPECT_EQ(44, ParseMaxCpuIndex("44-44\n"));

    // Don't assume that ranges are in ascending order or non-overlapping,
    // just in case.
    EXPECT_EQ(8, ParseMaxCpuIndex("0-7,5-8\n"));
    EXPECT_EQ(7, ParseMaxCpuIndex("0-7,5-6\n"));
    EXPECT_EQ(3, ParseMaxCpuIndex("2-3,0-1\n"));
    EXPECT_EQ(3, ParseMaxCpuIndex("2-3,0\n"));
    EXPECT_EQ(3, ParseMaxCpuIndex("3,0-2\n"));

    // Invalid inputs.
    EXPECT_EQ(-1, ParseMaxCpuIndex(""));
    EXPECT_EQ(-1, ParseMaxCpuIndex("\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex(" "));
    EXPECT_EQ(-1, ParseMaxCpuIndex("a\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("0\n1\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("\n1\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("\n1"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("0-\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("-2\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("1-9qwerty\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("1-9,0-\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("1,2,3-\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("1,2,a-4\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("1,2,3@4\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("1,2,\n"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("3-2\n"));

    // Overflows in various positions.
    EXPECT_EQ(-1, ParseMaxCpuIndex("2147483648"));           // 2^31
    EXPECT_EQ(-1, ParseMaxCpuIndex("18446744073709551617")); // 2^64 + 1
    EXPECT_EQ(-1, ParseMaxCpuIndex("999999999999999999999999999999999999999999999999"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("0-2147483648"));           // 2^31
    EXPECT_EQ(-1, ParseMaxCpuIndex("0-18446744073709551617")); // 2^64 + 1
    EXPECT_EQ(-1, ParseMaxCpuIndex("0-999999999999999999999999999999999999999999999999"));
    EXPECT_EQ(-1, ParseMaxCpuIndex("2147483648-1"));           // 2^31
    EXPECT_EQ(-1, ParseMaxCpuIndex("18446744073709551617-1")); // 2^64 + 1
    EXPECT_EQ(-1, ParseMaxCpuIndex("999999999999999999999999999999999999999999999999-1"));
}

} // namespace doris
