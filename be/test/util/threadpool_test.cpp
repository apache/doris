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

#include "util/threadpool.h"

#include <gflags/gflags_declare.h>
#include <gtest/gtest-death-test.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest-test-part.h>
#include <sched.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "gutil/strings/substitute.h"
#include "util/barrier.h"
#include "util/countdown_latch.h"
#include "util/random.h"
#include "util/scoped_cleanup.h"
#include "util/spinlock.h"
#include "util/time.h"

using std::atomic;
using std::shared_ptr;
using std::string;
using std::thread;

using std::vector;

using strings::Substitute;

DECLARE_int32(thread_inject_start_latency_ms);

namespace doris {
using namespace ErrorCode;

static const char* kDefaultPoolName = "test";

class ThreadPoolTest : public ::testing::Test {
public:
    void SetUp() override { EXPECT_TRUE(ThreadPoolBuilder(kDefaultPoolName).build(&_pool).ok()); }

    Status rebuild_pool_with_builder(const ThreadPoolBuilder& builder) {
        return builder.build(&_pool);
    }

    Status rebuild_pool_with_min_max(int min_threads, int max_threads) {
        return ThreadPoolBuilder(kDefaultPoolName)
                .set_min_threads(min_threads)
                .set_max_threads(max_threads)
                .build(&_pool);
    }

protected:
    std::unique_ptr<ThreadPool> _pool;
};

TEST_F(ThreadPoolTest, TestNoTaskOpenClose) {
    EXPECT_TRUE(rebuild_pool_with_min_max(4, 4).ok());
    _pool->shutdown();
}

static void simple_task_method(int n, std::atomic<int32_t>* counter) {
    while (n--) {
        (*counter)++;
        if (n < 32 || n & 1) {
            sched_yield();
        } else {
            // g++ -Wextra warns on {} or {0}
            struct timespec rqtp = {0, 0};

            // POSIX says that timespec has tv_sec and tv_nsec
            // But it doesn't guarantee order or placement

            rqtp.tv_sec = 0;
            rqtp.tv_nsec = 1000;

            nanosleep(&rqtp, nullptr);
        }
    }
}

class SimpleTask : public Runnable {
public:
    SimpleTask(int n, std::atomic<int32_t>* counter) : _n(n), _counter(counter) {}

    void run() override { simple_task_method(_n, _counter); }

private:
    int _n;
    std::atomic<int32_t>* _counter;
};

TEST_F(ThreadPoolTest, TestSimpleTasks) {
    EXPECT_TRUE(rebuild_pool_with_min_max(4, 4).ok());

    std::atomic<int32_t> counter(0);
    std::shared_ptr<Runnable> task(new SimpleTask(15, &counter));

    EXPECT_TRUE(_pool->submit_func(std::bind(&simple_task_method, 10, &counter)).ok());
    EXPECT_TRUE(_pool->submit(task).ok());
    EXPECT_TRUE(_pool->submit_func(std::bind(&simple_task_method, 20, &counter)).ok());
    EXPECT_TRUE(_pool->submit(task).ok());
    _pool->wait();
    EXPECT_EQ(10 + 15 + 20 + 15, counter.load());
    _pool->shutdown();
}

class SlowTask : public Runnable {
public:
    explicit SlowTask(CountDownLatch* latch) : _latch(latch) {}

    void run() override { _latch->wait(); }

    static shared_ptr<Runnable> new_slow_task(CountDownLatch* latch) {
        return std::make_shared<SlowTask>(latch);
    }

private:
    CountDownLatch* _latch;
};

TEST_F(ThreadPoolTest, TestThreadPoolWithNoMinimum) {
    EXPECT_TRUE(rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName)
                                                  .set_min_threads(0)
                                                  .set_max_threads(3)
                                                  .set_idle_timeout(std::chrono::milliseconds(1)))
                        .ok());

    // There are no threads to start with.
    EXPECT_TRUE(_pool->num_threads() == 0);
    // We get up to 3 threads when submitting work.
    CountDownLatch latch(1);
    SCOPED_CLEANUP({ latch.count_down(); });
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_EQ(2, _pool->num_threads());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_EQ(3, _pool->num_threads());
    // The 4th piece of work gets queued.
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_EQ(3, _pool->num_threads());
    // Finish all work
    latch.count_down();
    _pool->wait();
    EXPECT_EQ(0, _pool->_active_threads);
    _pool->shutdown();
    EXPECT_EQ(0, _pool->num_threads());
}

TEST_F(ThreadPoolTest, TestThreadPoolWithNoMaxThreads) {
    // By default a threadpool's max_threads is set to the number of CPUs, so
    // this test submits more tasks than that to ensure that the number of CPUs
    // isn't some kind of upper bound.
    const int kNumCPUs = std::thread::hardware_concurrency();

    // Build a threadpool with no limit on the maximum number of threads.
    EXPECT_TRUE(rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName)
                                                  .set_max_threads(std::numeric_limits<int>::max()))
                        .ok());
    CountDownLatch latch(1);
    auto cleanup_latch = MakeScopedCleanup([&]() { latch.count_down(); });

    // submit tokenless tasks. Each should create a new thread.
    for (int i = 0; i < kNumCPUs * 2; i++) {
        EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    }
    EXPECT_EQ((kNumCPUs * 2), _pool->num_threads());

    // submit tasks on two tokens. Only two threads should be created.
    std::unique_ptr<ThreadPoolToken> t1 = _pool->new_token(ThreadPool::ExecutionMode::SERIAL);
    std::unique_ptr<ThreadPoolToken> t2 = _pool->new_token(ThreadPool::ExecutionMode::SERIAL);
    for (int i = 0; i < kNumCPUs * 2; i++) {
        ThreadPoolToken* t = (i % 2 == 0) ? t1.get() : t2.get();
        EXPECT_TRUE(t->submit(SlowTask::new_slow_task(&latch)).ok());
    }
    EXPECT_EQ((kNumCPUs * 2) + 2, _pool->num_threads());

    // submit more tokenless tasks. Each should create a new thread.
    for (int i = 0; i < kNumCPUs; i++) {
        EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    }
    EXPECT_EQ((kNumCPUs * 3) + 2, _pool->num_threads());

    latch.count_down();
    _pool->wait();
    _pool->shutdown();
}

// Regression test for a bug where a task is submitted exactly
// as a thread is about to exit. Previously this could hang forever.
TEST_F(ThreadPoolTest, TestRace) {
    alarm(60);
    auto cleanup = MakeScopedCleanup([]() {
        alarm(0); // Disable alarm on test exit.
    });
    EXPECT_TRUE(rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName)
                                                  .set_min_threads(0)
                                                  .set_max_threads(1)
                                                  .set_idle_timeout(std::chrono::microseconds(1)))
                        .ok());

    for (int i = 0; i < 500; i++) {
        CountDownLatch l(1);
        // CountDownLatch::count_down has multiple overloaded version,
        // so an cast is needed to use std::bind
        EXPECT_TRUE(_pool
                            ->submit_func(std::bind(
                                    (void(CountDownLatch::*)())(&CountDownLatch::count_down), &l))
                            .ok());
        l.wait();
        // Sleeping a different amount in each iteration makes it more likely to hit
        // the bug.
        std::this_thread::sleep_for(std::chrono::microseconds(i));
    }
}

TEST_F(ThreadPoolTest, TestVariableSizeThreadPool) {
    EXPECT_TRUE(rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName)
                                                  .set_min_threads(1)
                                                  .set_max_threads(4)
                                                  .set_idle_timeout(std::chrono::milliseconds(1)))
                        .ok());

    // There is 1 thread to start with.
    EXPECT_EQ(1, _pool->num_threads());
    // We get up to 4 threads when submitting work.
    CountDownLatch latch(1);
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_EQ(1, _pool->num_threads());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_EQ(2, _pool->num_threads());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_EQ(3, _pool->num_threads());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_EQ(4, _pool->num_threads());
    // The 5th piece of work gets queued.
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_EQ(4, _pool->num_threads());
    // Finish all work
    latch.count_down();
    _pool->wait();
    EXPECT_EQ(0, _pool->_active_threads);
    _pool->shutdown();
    EXPECT_EQ(0, _pool->num_threads());
}

TEST_F(ThreadPoolTest, TestMaxQueueSize) {
    EXPECT_TRUE(rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName)
                                                  .set_min_threads(1)
                                                  .set_max_threads(1)
                                                  .set_max_queue_size(1))
                        .ok());

    CountDownLatch latch(1);
    // We will be able to submit two tasks: one for max_threads == 1 and one for
    // max_queue_size == 1.
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    Status s = _pool->submit(SlowTask::new_slow_task(&latch));
    CHECK(s.is<SERVICE_UNAVAILABLE>()) << "Expected failure due to queue blowout:" << s.to_string();
    latch.count_down();
    _pool->wait();
    _pool->shutdown();
}

// Test that when we specify a zero-sized queue, the maximum number of threads
// running is used for enforcement.
TEST_F(ThreadPoolTest, TestZeroQueueSize) {
    const int kMaxThreads = 4;
    EXPECT_TRUE(rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName)
                                                  .set_max_queue_size(0)
                                                  .set_max_threads(kMaxThreads))
                        .ok());

    CountDownLatch latch(1);
    for (int i = 0; i < kMaxThreads; i++) {
        EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch)).ok());
    }
    Status s = _pool->submit(SlowTask::new_slow_task(&latch));
    EXPECT_TRUE(s.is<SERVICE_UNAVAILABLE>()) << s.to_string();
    latch.count_down();
    _pool->wait();
    _pool->shutdown();
}

// Test that a thread pool will crash if asked to run its own blocking
// functions in a pool thread.
//
// In a multi-threaded application, TSAN is unsafe to use following a fork().
// After a fork(), TSAN will:
// 1. Disable verification, expecting an exec() soon anyway, and
// 2. Die on future thread creation.
// For some reason, this test triggers behavior #2. We could disable it with
// the TSAN option die_after_fork=0, but this can (supposedly) lead to
// deadlocks, so we'll disable the entire test instead.
#ifndef THREAD_SANITIZER
TEST_F(ThreadPoolTest, TestDeadlocks) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
#ifdef NDEBUG
    const char* death_msg = "doris::ThreadPool::check_not_pool_thread_unlocked()";
#elif defined(__APPLE__)
    const char* death_msg =
            "_ZNSt3__1L8__invokeIRNS_6__bindIMN5doris10ThreadPoolEFvvEJPS3_EEEJEEEDTclscT_fp_"
            "spscT0_fp0_EEOS9_DpOSA_|6__bindIMN5doris10ThreadPoolEFvvEJPS3_"
            "EEEJEEEDTclclsr3stdE7declvalIT_EEspclsr3stdE7declvalIT0_EEEEOS9_DpOSA_";
#else
    const char* death_msg =
            "_ZNSt5_BindIFMN5doris10ThreadPoolEFvvEPS1_EE6__callIvJEJLm0EEEET_OSt5tupleIJDpT0_"
            "EESt12_Index_tupleIJXspT1_EEE";
#endif
    EXPECT_DEATH(
            {
                EXPECT_TRUE(rebuild_pool_with_min_max(1, 1).ok());
                EXPECT_TRUE(
                        _pool->submit_func(std::bind((&ThreadPool::shutdown), _pool.get())).ok());
                _pool->wait();
            },
            death_msg);

    EXPECT_DEATH(
            {
                EXPECT_TRUE(rebuild_pool_with_min_max(1, 1).ok());
                EXPECT_TRUE(_pool->submit_func(std::bind(&ThreadPool::wait, _pool.get())).ok());
                _pool->wait();
            },
            death_msg);
}
#endif

class SlowDestructorRunnable : public Runnable {
public:
    void run() override {}

    ~SlowDestructorRunnable() override {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
};

// Test that if a tasks's destructor is slow, it doesn't cause serialization of the tasks
// in the queue.
TEST_F(ThreadPoolTest, TestSlowDestructor) {
    EXPECT_TRUE(rebuild_pool_with_min_max(1, 20).ok());
    auto start = std::chrono::system_clock::now();
    for (int i = 0; i < 100; i++) {
        shared_ptr<Runnable> task(new SlowDestructorRunnable());
        EXPECT_TRUE(_pool->submit(std::move(task)).ok());
    }
    _pool->wait();
    EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() -
                                                               start)
                      .count(),
              5);
}

// For test cases that should run with both kinds of tokens.
class ThreadPoolTestTokenTypes : public ThreadPoolTest,
                                 public testing::WithParamInterface<ThreadPool::ExecutionMode> {};

INSTANTIATE_TEST_SUITE_P(Tokens, ThreadPoolTestTokenTypes,
                         ::testing::Values(ThreadPool::ExecutionMode::SERIAL,
                                           ThreadPool::ExecutionMode::CONCURRENT));

TEST_P(ThreadPoolTestTokenTypes, TestTokenSubmitAndWait) {
    std::unique_ptr<ThreadPoolToken> t = _pool->new_token(GetParam());
    int i = 0;
    Status status = t->submit_func([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        i++;
    });
    EXPECT_TRUE(status.ok());
    t->wait();
    EXPECT_EQ(1, i);
}

TEST_F(ThreadPoolTest, TestTokenSubmitsProcessedSerially) {
    std::unique_ptr<ThreadPoolToken> t = _pool->new_token(ThreadPool::ExecutionMode::SERIAL);
    int32_t seed = static_cast<int32_t>(GetCurrentTimeMicros());
    srand(seed);
    Random r(seed);
    string result;
    for (char c = 'a'; c < 'f'; c++) {
        // Sleep a little first so that there's a higher chance of out-of-order
        // appends if the submissions did execute in parallel.
        int sleep_ms = r.Next() % 5;
        Status status = t->submit_func([&result, c, sleep_ms]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            result += c;
        });
        EXPECT_TRUE(status.ok());
    }
    t->wait();
    EXPECT_EQ("abcde", result);
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenSubmitsProcessedConcurrently) {
    const int kNumTokens = 5;
    EXPECT_TRUE(rebuild_pool_with_builder(
                        ThreadPoolBuilder(kDefaultPoolName).set_max_threads(kNumTokens))
                        .ok());
    std::vector<std::unique_ptr<ThreadPoolToken>> tokens;

    // A violation to the tested invariant would yield a deadlock, so let's set
    // up an alarm to bail us out.
    alarm(60);
    SCOPED_CLEANUP({
        alarm(0); // Disable alarm on test exit.
    });
    std::shared_ptr<Barrier> b = std::make_shared<Barrier>(kNumTokens + 1);
    for (int i = 0; i < kNumTokens; i++) {
        tokens.emplace_back(_pool->new_token(GetParam()));
        EXPECT_TRUE(tokens.back()->submit_func([b]() { b->wait(); }).ok());
    }

    // This will deadlock if the above tasks weren't all running concurrently.
    b->wait();
}

TEST_F(ThreadPoolTest, TestTokenSubmitsNonSequential) {
    const int kNumSubmissions = 5;
    EXPECT_TRUE(rebuild_pool_with_builder(
                        ThreadPoolBuilder(kDefaultPoolName).set_max_threads(kNumSubmissions))
                        .ok());

    // A violation to the tested invariant would yield a deadlock, so let's set
    // up an alarm to bail us out.
    alarm(60);
    SCOPED_CLEANUP({
        alarm(0); // Disable alarm on test exit.
    });
    shared_ptr<Barrier> b = std::make_shared<Barrier>(kNumSubmissions + 1);
    std::unique_ptr<ThreadPoolToken> t = _pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    for (int i = 0; i < kNumSubmissions; i++) {
        EXPECT_TRUE(t->submit_func([b]() { b->wait(); }).ok());
    }

    // This will deadlock if the above tasks weren't all running concurrently.
    b->wait();
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenShutdown) {
    EXPECT_TRUE(
            rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName).set_max_threads(4)).ok());

    std::unique_ptr<ThreadPoolToken> t1(_pool->new_token(GetParam()));
    std::unique_ptr<ThreadPoolToken> t2(_pool->new_token(GetParam()));
    CountDownLatch l1(1);
    CountDownLatch l2(1);

    // A violation to the tested invariant would yield a deadlock, so let's set
    // up an alarm to bail us out.
    alarm(60);
    SCOPED_CLEANUP({
        alarm(0); // Disable alarm on test exit.
    });

    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(t1->submit_func([&]() { l1.wait(); }).ok());
    }
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(t2->submit_func([&]() { l2.wait(); }).ok());
    }

    // Unblock all of t1's tasks, but not t2's tasks.
    l1.count_down();

    // If this also waited for t2's tasks, it would deadlock.
    t1->shutdown();

    // We can no longer submit to t1 but we can still submit to t2.
    EXPECT_TRUE(t1->submit_func([]() {}).is<SERVICE_UNAVAILABLE>());
    EXPECT_TRUE(t2->submit_func([]() {}).ok());

    // Unblock t2's tasks.
    l2.count_down();
    t2->shutdown();
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenWaitForAll) {
    const int kNumTokens = 3;
    const int kNumSubmissions = 20;
    int32_t seed = static_cast<int32_t>(GetCurrentTimeMicros());
    srand(seed);
    Random r(seed);
    std::vector<std::unique_ptr<ThreadPoolToken>> tokens;
    for (int i = 0; i < kNumTokens; i++) {
        tokens.emplace_back(_pool->new_token(GetParam()));
    }

    atomic<int32_t> v(0);
    for (int i = 0; i < kNumSubmissions; i++) {
        // Sleep a little first to raise the likelihood of the test thread
        // reaching wait() before the submissions finish.
        int sleep_ms = r.Next() % 5;

        auto task = [&v, sleep_ms]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            v++;
        };

        // Half of the submissions will be token-less, and half will use a token.
        if (i % 2 == 0) {
            EXPECT_TRUE(_pool->submit_func(task).ok());
        } else {
            int token_idx = r.Next() % tokens.size();
            EXPECT_TRUE(tokens[token_idx]->submit_func(task).ok());
        }
    }
    _pool->wait();
    EXPECT_EQ(kNumSubmissions, v);
}

TEST_F(ThreadPoolTest, TestFuzz) {
    const int kNumOperations = 1000;
    int32_t seed = static_cast<int32_t>(GetCurrentTimeMicros());
    srand(seed);
    Random r(seed);
    std::vector<std::unique_ptr<ThreadPoolToken>> tokens;

    for (int i = 0; i < kNumOperations; i++) {
        // Operation distribution:
        //
        // - submit without a token: 40%
        // - submit with a randomly selected token: 35%
        // - Allocate a new token: 10%
        // - Wait on a randomly selected token: 7%
        // - shutdown a randomly selected token: 4%
        // - Deallocate a randomly selected token: 2%
        // - Wait for all submissions: 2%
        int op = r.Next() % 100;
        if (op < 40) {
            // submit without a token.
            int sleep_ms = r.Next() % 5;
            EXPECT_TRUE(_pool->submit_func([sleep_ms]() {
                                 // Sleep a little first to increase task overlap.
                                 std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                             }).ok());
        } else if (op < 75) {
            // submit with a randomly selected token.
            if (tokens.empty()) {
                continue;
            }
            int sleep_ms = r.Next() % 5;
            int token_idx = r.Next() % tokens.size();
            Status s = tokens[token_idx]->submit_func([sleep_ms]() {
                // Sleep a little first to increase task overlap.
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            });
            EXPECT_TRUE(s.ok() || s.is<SERVICE_UNAVAILABLE>());
        } else if (op < 85) {
            // Allocate a token with a randomly selected policy.
            ThreadPool::ExecutionMode mode = r.Next() % 2 ? ThreadPool::ExecutionMode::SERIAL
                                                          : ThreadPool::ExecutionMode::CONCURRENT;
            tokens.emplace_back(_pool->new_token(mode));
        } else if (op < 92) {
            // Wait on a randomly selected token.
            if (tokens.empty()) {
                continue;
            }
            int token_idx = r.Next() % tokens.size();
            tokens[token_idx]->wait();
        } else if (op < 96) {
            // shutdown a randomly selected token.
            if (tokens.empty()) {
                continue;
            }
            int token_idx = r.Next() % tokens.size();
            tokens[token_idx]->shutdown();
        } else if (op < 98) {
            // Deallocate a randomly selected token.
            if (tokens.empty()) {
                continue;
            }
            auto it = tokens.begin();
            int token_idx = r.Next() % tokens.size();
            std::advance(it, token_idx);
            tokens.erase(it);
        } else {
            // Wait on everything.
            EXPECT_LT(op, 100);
            EXPECT_GE(op, 98);
            _pool->wait();
        }
    }

    // Some test runs will shut down the pool before the tokens, and some won't.
    // Either way should be safe.
    if (r.Next() % 2 == 0) {
        _pool->shutdown();
    }
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenSubmissionsAdhereToMaxQueueSize) {
    EXPECT_TRUE(rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName)
                                                  .set_min_threads(1)
                                                  .set_max_threads(1)
                                                  .set_max_queue_size(1))
                        .ok());

    CountDownLatch latch(1);
    std::unique_ptr<ThreadPoolToken> t = _pool->new_token(GetParam());
    SCOPED_CLEANUP({ latch.count_down(); });
    // We will be able to submit two tasks: one for max_threads == 1 and one for
    // max_queue_size == 1.
    EXPECT_TRUE(t->submit(SlowTask::new_slow_task(&latch)).ok());
    EXPECT_TRUE(t->submit(SlowTask::new_slow_task(&latch)).ok());
    Status s = t->submit(SlowTask::new_slow_task(&latch));
    EXPECT_TRUE(s.is<SERVICE_UNAVAILABLE>());
}

TEST_F(ThreadPoolTest, TestTokenConcurrency) {
    const int kNumTokens = 20;
    const int kTestRuntimeSecs = 1;
    const int kCycleThreads = 2;
    const int kShutdownThreads = 2;
    const int kWaitThreads = 2;
    const int kSubmitThreads = 8;

    std::vector<shared_ptr<ThreadPoolToken>> tokens;
    int32_t seed = static_cast<int32_t>(GetCurrentTimeMicros());
    srand(seed);
    Random rng(seed);

    // Protects 'tokens' and 'rng'.
    SpinLock lock;

    // Fetch a token from 'tokens' at random.
    auto GetRandomToken = [&]() -> shared_ptr<ThreadPoolToken> {
        std::lock_guard<SpinLock> l(lock);
        int idx = rng.Uniform(kNumTokens);
        return tokens[idx];
    };

    // Preallocate all of the tokens.
    for (int i = 0; i < kNumTokens; i++) {
        ThreadPool::ExecutionMode mode;
        {
            std::lock_guard<SpinLock> l(lock);
            mode = rng.Next() % 2 ? ThreadPool::ExecutionMode::SERIAL
                                  : ThreadPool::ExecutionMode::CONCURRENT;
        }
        tokens.emplace_back(_pool->new_token(mode).release());
    }

    atomic<int64_t> total_num_tokens_cycled(0);
    atomic<int64_t> total_num_tokens_shutdown(0);
    atomic<int64_t> total_num_tokens_waited(0);
    atomic<int64_t> total_num_tokens_submitted(0);

    CountDownLatch latch(1);
    std::vector<thread> threads;

    for (int i = 0; i < kCycleThreads; i++) {
        // Pick a token at random and replace it.
        //
        // The replaced token is only destroyed when the last ref is dropped,
        // possibly by another thread.
        threads.emplace_back([&]() {
            int num_tokens_cycled = 0;
            while (latch.count()) {
                {
                    std::lock_guard<SpinLock> l(lock);
                    int idx = rng.Uniform(kNumTokens);
                    ThreadPool::ExecutionMode mode =
                            rng.Next() % 2 ? ThreadPool::ExecutionMode::SERIAL
                                           : ThreadPool::ExecutionMode::CONCURRENT;
                    tokens[idx] = shared_ptr<ThreadPoolToken>(_pool->new_token(mode).release());
                }
                num_tokens_cycled++;

                // Sleep a bit, otherwise this thread outpaces the other threads and
                // nothing interesting happens to most tokens.
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
            total_num_tokens_cycled += num_tokens_cycled;
        });
    }

    for (int i = 0; i < kShutdownThreads; i++) {
        // Pick a token at random and shut it down. Submitting a task to a shut
        // down token will return a ServiceUnavailable error.
        threads.emplace_back([&]() {
            int num_tokens_shutdown = 0;
            while (latch.count()) {
                GetRandomToken()->shutdown();
                num_tokens_shutdown++;
            }
            total_num_tokens_shutdown += num_tokens_shutdown;
        });
    }

    for (int i = 0; i < kWaitThreads; i++) {
        // Pick a token at random and wait for any outstanding tasks.
        threads.emplace_back([&]() {
            int num_tokens_waited = 0;
            while (latch.count()) {
                GetRandomToken()->wait();
                num_tokens_waited++;
            }
            total_num_tokens_waited += num_tokens_waited;
        });
    }

    for (int i = 0; i < kSubmitThreads; i++) {
        // Pick a token at random and submit a task to it.
        threads.emplace_back([&]() {
            int num_tokens_submitted = 0;
            int32_t seed = static_cast<int32_t>(GetCurrentTimeMicros());
            srand(seed);
            Random rng(seed);
            while (latch.count()) {
                int sleep_ms = rng.Next() % 5;
                Status s = GetRandomToken()->submit_func([sleep_ms]() {
                    // Sleep a little first so that tasks are running during other events.
                    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                });
                CHECK(s.ok() || s.is<SERVICE_UNAVAILABLE>());
                num_tokens_submitted++;
            }
            total_num_tokens_submitted += num_tokens_submitted;
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(kTestRuntimeSecs));
    latch.count_down();
    for (auto& t : threads) {
        t.join();
    }

    LOG(INFO) << strings::Substitute("Tokens cycled ($0 threads): $1", kCycleThreads,
                                     total_num_tokens_cycled.load());
    LOG(INFO) << strings::Substitute("Tokens shutdown ($0 threads): $1", kShutdownThreads,
                                     total_num_tokens_shutdown.load());
    LOG(INFO) << strings::Substitute("Tokens waited ($0 threads): $1", kWaitThreads,
                                     total_num_tokens_waited.load());
    LOG(INFO) << strings::Substitute("Tokens submitted ($0 threads): $1", kSubmitThreads,
                                     total_num_tokens_submitted.load());
}

static void MyFunc(int idx, int n) {
    LOG(INFO) << idx << ", " << std::this_thread::get_id() << " before sleep " << n << " seconds";
    sleep(n);
    LOG(INFO) << idx << ", " << std::this_thread::get_id() << " after sleep " << n << " seconds";
}

TEST_F(ThreadPoolTest, TestNormal) {
    std::unique_ptr<ThreadPool> thread_pool;
    static_cast<void>(ThreadPoolBuilder("my_pool")
                              .set_min_threads(0)
                              .set_max_threads(5)
                              .set_max_queue_size(10)
                              .set_idle_timeout(std::chrono::milliseconds(2000))
                              .build(&thread_pool));

    std::unique_ptr<ThreadPoolToken> token1 =
            thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT, 2);
    for (int i = 0; i < 10; i++) {
        static_cast<void>(token1->submit_func(std::bind(&MyFunc, i, 1)));
    }
    token1->wait();
    EXPECT_EQ(0, token1->num_tasks());

    std::unique_ptr<ThreadPoolToken> token2 =
            thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT, 20);
    for (int i = 0; i < 10; i++) {
        static_cast<void>(token2->submit_func(std::bind(&MyFunc, i, 1)));
    }
    token2->wait();
    EXPECT_EQ(0, token2->num_tasks());

    std::unique_ptr<ThreadPoolToken> token3 =
            thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT, 1);
    for (int i = 0; i < 10; i++) {
        static_cast<void>(token3->submit_func(std::bind(&MyFunc, i, 1)));
    }
    token3->wait();
    EXPECT_EQ(0, token3->num_tasks());

    std::unique_ptr<ThreadPoolToken> token4 =
            thread_pool->new_token(ThreadPool::ExecutionMode::SERIAL);
    for (int i = 0; i < 10; i++) {
        static_cast<void>(token4->submit_func(std::bind(&MyFunc, i, 1)));
    }
    token4->wait();
    EXPECT_EQ(0, token4->num_tasks());

    std::unique_ptr<ThreadPoolToken> token5 =
            thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT, 20);
    for (int i = 0; i < 10; i++) {
        static_cast<void>(token5->submit_func(std::bind(&MyFunc, i, 1)));
    }
    token5->shutdown();
    EXPECT_EQ(0, token5->num_tasks());
}

TEST_F(ThreadPoolTest, TestThreadPoolDynamicAdjustMaximumMinimum) {
    EXPECT_TRUE(rebuild_pool_with_builder(ThreadPoolBuilder(kDefaultPoolName)
                                                  .set_min_threads(3)
                                                  .set_max_threads(3)
                                                  .set_idle_timeout(std::chrono::milliseconds(1)))
                        .ok());

    EXPECT_EQ(3, _pool->min_threads());
    EXPECT_EQ(3, _pool->max_threads());
    EXPECT_EQ(3, _pool->num_threads());

    EXPECT_TRUE(!_pool->set_min_threads(4).ok());
    EXPECT_TRUE(!_pool->set_max_threads(2).ok());

    EXPECT_TRUE(_pool->set_min_threads(2).ok());
    EXPECT_EQ(2, _pool->min_threads());
    EXPECT_TRUE(_pool->set_max_threads(4).ok());
    EXPECT_EQ(4, _pool->max_threads());

    EXPECT_TRUE(_pool->set_min_threads(3).ok());
    EXPECT_EQ(3, _pool->min_threads());
    EXPECT_TRUE(_pool->set_max_threads(3).ok());
    EXPECT_EQ(3, _pool->max_threads());

    CountDownLatch latch_1(1);
    CountDownLatch latch_2(1);
    CountDownLatch latch_3(1);
    CountDownLatch latch_4(1);
    CountDownLatch latch_5(1);
    CountDownLatch latch_6(1);
    CountDownLatch latch_7(1);
    CountDownLatch latch_8(1);
    CountDownLatch latch_9(1);
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_1)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_2)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_3)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_4)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_5)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_6)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_7)).ok());
    EXPECT_EQ(3, _pool->num_threads());
    EXPECT_TRUE(_pool->set_max_threads(4).ok());
    EXPECT_EQ(4, _pool->max_threads());
    EXPECT_EQ(4, _pool->num_threads());
    EXPECT_TRUE(_pool->set_max_threads(5).ok());
    EXPECT_EQ(5, _pool->max_threads());
    EXPECT_EQ(5, _pool->num_threads());
    EXPECT_TRUE(_pool->set_max_threads(6).ok());
    EXPECT_EQ(6, _pool->max_threads());
    EXPECT_EQ(6, _pool->num_threads());
    EXPECT_TRUE(_pool->set_max_threads(4).ok());
    EXPECT_EQ(4, _pool->max_threads());
    latch_1.count_down();
    latch_2.count_down();
    latch_3.count_down();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(4, _pool->num_threads());

    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_8)).ok());
    EXPECT_TRUE(_pool->submit(SlowTask::new_slow_task(&latch_9)).ok());
    EXPECT_EQ(4, _pool->num_threads());

    EXPECT_TRUE(_pool->set_min_threads(2).ok());
    EXPECT_EQ(2, _pool->min_threads());

    latch_4.count_down();
    latch_5.count_down();
    latch_6.count_down();
    latch_7.count_down();
    latch_8.count_down();
    latch_9.count_down();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(2, _pool->num_threads());

    _pool->wait();
    _pool->shutdown();
    EXPECT_EQ(0, _pool->num_threads());
}

TEST_F(ThreadPoolTest, TestThreadTokenSerial) {
    std::unique_ptr<ThreadPool> thread_pool;
    static_cast<void>(ThreadPoolBuilder("my_pool")
                              .set_min_threads(0)
                              .set_max_threads(1)
                              .set_max_queue_size(10)
                              .set_idle_timeout(std::chrono::milliseconds(2000))
                              .build(&thread_pool));

    std::unique_ptr<ThreadPoolToken> token1 =
            thread_pool->new_token(ThreadPool::ExecutionMode::SERIAL, 2);
    static_cast<void>(token1->submit_func(std::bind(&MyFunc, 0, 1)));
    std::cout << "after submit 1" << std::endl;
    token1->wait();
    ASSERT_EQ(0, token1->num_tasks());
    for (int i = 0; i < 10; i++) {
        static_cast<void>(token1->submit_func(std::bind(&MyFunc, i, 1)));
    }
    std::cout << "after submit 1" << std::endl;
    token1->wait();
    ASSERT_EQ(0, token1->num_tasks());
}

TEST_F(ThreadPoolTest, TestThreadTokenConcurrent) {
    std::unique_ptr<ThreadPool> thread_pool;
    static_cast<void>(ThreadPoolBuilder("my_pool")
                              .set_min_threads(0)
                              .set_max_threads(1)
                              .set_max_queue_size(10)
                              .set_idle_timeout(std::chrono::milliseconds(2000))
                              .build(&thread_pool));

    std::unique_ptr<ThreadPoolToken> token1 =
            thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT, 2);
    for (int i = 0; i < 10; i++) {
        static_cast<void>(token1->submit_func(std::bind(&MyFunc, i, 1)));
    }
    std::cout << "after submit 1" << std::endl;
    token1->wait();
    ASSERT_EQ(0, token1->num_tasks());
}

} // namespace doris
