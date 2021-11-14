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

#include "util/thread.h"

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

class ThreadTest : public ::testing::Test {
public:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

// Join with a thread and emit warnings while waiting to join.
// This has to be manually verified.
TEST_F(ThreadTest, TestJoinAndWarn) {
    scoped_refptr<Thread> holder;
    Status status =
            Thread::create("test", "sleeper thread", SleepFor, MonoDelta::FromSeconds(1), &holder);
    ASSERT_TRUE(status.ok());
    status = ThreadJoiner(holder.get()).warn_after_ms(10).warn_every_ms(100).join();
    ASSERT_TRUE(status.ok());
}

TEST_F(ThreadTest, TestFailedJoin) {
    scoped_refptr<Thread> holder;
    Status status =
            Thread::create("test", "sleeper thread", SleepFor, MonoDelta::FromSeconds(1), &holder);
    ASSERT_TRUE(status.ok());
    status = ThreadJoiner(holder.get()).give_up_after_ms(50).join();
    ASSERT_TRUE(status.is_aborted());
}

static void TryJoinOnSelf() {
    Status s = ThreadJoiner(Thread::current_thread()).join();
    // Use CHECK instead of ASSERT because gtest isn't thread-safe.
    CHECK(s.is_invalid_argument());
}

// Try to join on the thread that is currently running.
TEST_F(ThreadTest, TestJoinOnSelf) {
    scoped_refptr<Thread> holder;
    ASSERT_TRUE(Thread::create("test", "test", TryJoinOnSelf, &holder).ok());
    holder->join();
    // Actual assertion is done by the thread spawned above.
}

TEST_F(ThreadTest, TestDoubleJoinIsNoOp) {
    scoped_refptr<Thread> holder;
    Status status =
            Thread::create("test", "sleeper thread", SleepFor, MonoDelta::FromSeconds(0), &holder);
    ASSERT_TRUE(status.ok());
    ThreadJoiner joiner(holder.get());
    status = joiner.join();
    ASSERT_TRUE(status.ok());
    status = joiner.join();
    ASSERT_TRUE(status.ok());
}

TEST_F(ThreadTest, ThreadStartBenchmark) {
    std::vector<scoped_refptr<Thread>> threads(1000);
    {
        int64_t thread_creation_ns = 0;
        {
            SCOPED_RAW_TIMER(&thread_creation_ns);
            for (auto& t : threads) {
                Status status = Thread::create("test", "TestCallOnExit", SleepFor,
                                            MonoDelta::FromSeconds(0), &t);
                ASSERT_TRUE(status.ok());
            }
        }
        std::cout << "create 1000 threads use:" << thread_creation_ns << "ns" << std::endl;
    }
    {
        int64_t thread_publish_tid_ns = 0;
        {
            SCOPED_RAW_TIMER(&thread_publish_tid_ns);
            for (auto& t : threads) {
                t->tid();
            }
        }
        std::cout << "1000 threads publish TIDS use:" << thread_publish_tid_ns << "ns" << std::endl;
    }

    for (auto& t : threads) {
        t->join();
    }
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
