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

#include "util/thread_group.h"

#include <gtest/gtest.h>
#include <sys/types.h>
#include <unistd.h>

#include <ostream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/basictypes.h"
#include "gutil/ref_counted.h"
#include "util/countdown_latch.h"
#include "util/runtime_profile.h"

using std::string;

namespace doris {

class ThreadGroupTest : public ::testing::Test {
public:
    virtual void SetUp() {}
    virtual void TearDown() {}
    void increment_count() {
        std::unique_lock<std::mutex> lock(mutex);
        ++count;
    }
    int count = 0;
    std::mutex mutex;
};

TEST_F(ThreadGroupTest, TestJoinALL) {
    ThreadGroup threads;
    for (int i = 0; i < 10; ++i) {
        threads.create_thread(&increment_count);
    }
    threads.join_all();
    EXPECT_EQ(10, count);
}

TEST_F(ThreadGroupTest, TestThreadIn) {
    ThreadGroup threads;
    std::thread* th = new std::thread(&increment_count);
    threads.add_thread(th);
    ASSERT_FALSE(threads.is_this_thread_in());
    threads.join_all();
    ThreadGroup threads2;
    std::thread* th2 = new std::thread(&increment_count);
    threads2.add_thread(th2);
    ASSERT_TRUE(threads2.is_thread_in(th2));
    threads2.remove_thread(th2);
    ASSERT_FALSE(threads2.is_thread_in(th2));
    th2->join();
    delete th2;
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
