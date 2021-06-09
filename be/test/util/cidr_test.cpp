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

#include "util/cidr.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "common/configbase.h"
#include "util/cpu_info.h"
#include "util/logging.h"

namespace doris {

TEST(CIDR, wrong_format) {
    CIDR cidr;
    EXPECT_FALSE(cidr.reset("192.168.17.0/"));
    EXPECT_FALSE(cidr.reset("192.168.17.0.1/20"));
    EXPECT_FALSE(cidr.reset("192.168.17.0.1/88"));
    EXPECT_FALSE(cidr.reset("192.168.17/"));
    EXPECT_FALSE(cidr.reset("192.168.a.a/24"));
    EXPECT_FALSE(cidr.reset("192.168.a.a/16"));
    EXPECT_FALSE(cidr.reset("192.168.0.0/a"));
}

TEST(CIDR, normal) {
    CIDR cidr;
    EXPECT_TRUE(cidr.reset("192.168.17.0/20"));
    EXPECT_TRUE(cidr.reset("10.10.10.10/20"));
    EXPECT_TRUE(cidr.reset("10.10.10.10/32"));
    EXPECT_TRUE(cidr.reset("10.10.10.10"));
}

TEST(CIDR, contains) {
    CIDR cidr;
    EXPECT_TRUE(cidr.reset("192.168.17.0/16"));
    EXPECT_TRUE(cidr.contains("192.168.88.99"));
    EXPECT_FALSE(cidr.contains("192.2.88.99"));
}

} // end namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
