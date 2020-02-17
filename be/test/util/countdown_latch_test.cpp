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

#include <functional>
#include <gtest/gtest.h>

#include "gutil/ref_counted.h"
#include "util/countdown_latch.h"
#include "util/monotime.h"
#include "util/thread.h"
#include "util/threadpool.h"

namespace doris {

static void decrement_latch(CountDownLatch* latch, int amount) {
    if (amount == 1) {
        latch->count_down();
        return;
    }
    latch->count_down(amount);
}

// Tests that we can decrement the latch by arbitrary amounts, as well
// as 1 by one.
TEST(TestCountDownLatch, TestLatch) {

    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("cdl-test").set_max_threads(1).build(&pool).ok());

    CountDownLatch latch(1000);

    // Decrement the count by 1 in another thread, this should not fire the
    // latch.
    ASSERT_TRUE(pool->submit_func(std::bind(decrement_latch, &latch, 1)).ok());
    ASSERT_FALSE(latch.wait_for(MonoDelta::FromMilliseconds(200)));
    ASSERT_EQ(999, latch.count());

    // Now decrement by 1000 this should decrement to 0 and fire the latch
    // (even though 1000 is one more than the current count).
    ASSERT_TRUE(pool->submit_func(std::bind(decrement_latch, &latch, 1000)).ok());
    latch.wait();
    ASSERT_EQ(0, latch.count());
}

// Test that resetting to zero while there are waiters lets the waiters
// continue.
TEST(TestCountDownLatch, TestResetToZero) {
    CountDownLatch cdl(100);
    scoped_refptr<Thread> t;
    ASSERT_TRUE(Thread::create("test", "cdl-test", &CountDownLatch::wait, &cdl, &t).ok());

    // Sleep for a bit until it's likely the other thread is waiting on the latch.
    SleepFor(MonoDelta::FromMilliseconds(10));
    cdl.reset(0);
    t->join();
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
