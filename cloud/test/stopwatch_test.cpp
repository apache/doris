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

#include "common/stopwatch.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace doris::cloud;

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(StopWatchTest, SimpleTest) {
    {
        StopWatch s;
        s.start();
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        ASSERT_TRUE(s.elapsed_us() >= 1000);

        s.pause();
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        ASSERT_TRUE(s.elapsed_us() >= 1000 && s.elapsed_us() < 1500);

        s.resume();
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        ASSERT_TRUE(s.elapsed_us() >= 1000 && s.elapsed_us() < 2500);

        s.reset();
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        ASSERT_TRUE(s.elapsed_us() >= 1000 && s.elapsed_us() < 1500);
    }
}