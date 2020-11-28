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

#include "util/bit_util.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <boost/utility.hpp>
#include <iostream>

#include "common/config.h"
#include "util/cpu_info.h"
#include "util/logging.h"

namespace doris {

TEST(BitUtil, Ceil) {
    EXPECT_EQ(BitUtil::ceil(0, 1), 0);
    EXPECT_EQ(BitUtil::ceil(1, 1), 1);
    EXPECT_EQ(BitUtil::ceil(1, 2), 1);
    EXPECT_EQ(BitUtil::ceil(1, 8), 1);
    EXPECT_EQ(BitUtil::ceil(7, 8), 1);
    EXPECT_EQ(BitUtil::ceil(8, 8), 1);
    EXPECT_EQ(BitUtil::ceil(9, 8), 2);
}

TEST(BitUtil, Popcount) {
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
    EXPECT_EQ(BitUtil::popcount(0), 0);
    EXPECT_EQ(BitUtil::popcount_no_hw(0), 0);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
