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

#include "util/blocking_queue.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "cpp/sync_point.h"
#include "util/defer_op.h"

namespace doris {

TEST(BlockingQueueTest, TestBasic) {
    int32_t i;
    BlockingQueue<int32_t> test_queue(5);
    EXPECT_TRUE(test_queue.blocking_put(1));
    EXPECT_TRUE(test_queue.blocking_put(2));
    EXPECT_TRUE(test_queue.blocking_put(3));
    EXPECT_TRUE(test_queue.blocking_get(&i));
    EXPECT_EQ(1, i);
    EXPECT_TRUE(test_queue.blocking_get(&i));
    EXPECT_EQ(2, i);
    EXPECT_TRUE(test_queue.blocking_get(&i));
    EXPECT_EQ(3, i);
}

TEST(BlockingQueueTest, TestGetFromShutdownQueue) {
    int64_t i;
    BlockingQueue<int64_t> test_queue(2);
    EXPECT_TRUE(test_queue.blocking_put(123));
    test_queue.shutdown();
    EXPECT_FALSE(test_queue.blocking_put(456));
    EXPECT_TRUE(test_queue.blocking_get(&i));
    EXPECT_EQ(123, i);
    EXPECT_FALSE(test_queue.blocking_get(&i));
}

class MultiThreadTest {
public:
    MultiThreadTest()
            : _iterations(10000),
              _nthreads(5),
              _queue(_iterations * _nthreads / 10),
              _num_inserters(_nthreads) {}

    void inserter_thread(int arg) {
        for (int i = 0; i < _iterations; ++i) {
            _queue.blocking_put(arg);
        }

        {
            std::lock_guard<std::mutex> guard(_lock);

            if (--_num_inserters == 0) {
                _queue.shutdown();
            }
        }
    }

    void RemoverThread() {
        for (int i = 0; i < _iterations; ++i) {
            int32_t arg;
            bool got = _queue.blocking_get(&arg);

            if (!got) {
                arg = -1;
            }

            {
                std::lock_guard<std::mutex> guard(_lock);
                _gotten[arg] = _gotten[arg] + 1;
            }
        }
    }

    void Run() {
        for (int i = 0; i < _nthreads; ++i) {
            _threads.push_back(std::shared_ptr<std::thread>(
                    new std::thread(std::bind(&MultiThreadTest::inserter_thread, this, i))));
            _threads.push_back(std::shared_ptr<std::thread>(
                    new std::thread(std::bind(&MultiThreadTest::RemoverThread, this))));
        }

        // We add an extra thread to ensure that there aren't enough elements in
        // the queue to go around.  This way, we test removal after shutdown.
        _threads.push_back(std::shared_ptr<std::thread>(
                new std::thread(std::bind(&MultiThreadTest::RemoverThread, this))));

        for (int i = 0; i < _threads.size(); ++i) {
            _threads[i]->join();
        }

        // Let's check to make sure we got what we should have.
        std::lock_guard<std::mutex> guard(_lock);

        for (int i = 0; i < _nthreads; ++i) {
            EXPECT_EQ(_iterations, _gotten[i]);
        }

        // And there were _nthreads * (_iterations + 1)  elements removed, but only
        // _nthreads * _iterations elements added.  So some removers hit the shutdown
        // case.
        EXPECT_EQ(_iterations, _gotten[-1]);
    }

private:
    typedef std::vector<std::shared_ptr<std::thread>> ThreadVector;

    int _iterations;
    int _nthreads;
    BlockingQueue<int32_t> _queue;
    // Lock for _gotten and _num_inserters.
    std::mutex _lock;
    // Map from inserter thread id to number of consumed elements from that id.
    // Ultimately, this should map each thread id to _insertions elements.
    // Additionally, if the blocking_get returns false, this increments id=-1.
    std::map<int32_t, int> _gotten;
    // All inserter and remover threads.
    ThreadVector _threads;
    // Number of inserters which haven't yet finished inserting.
    int _num_inserters;
};

TEST(BlockingQueueTest, TestMultipleThreads) {
    MultiThreadTest test;
    test.Run();
}

// Coordinates the queue threads through callbacks installed at the sync points below. The
// waiter callback runs while the queue mutex is still held, immediately before wait_for(). The
// operation callback runs after the peer operation has acquired that same mutex. Holding the
// first peer operation there past the wait timeout deterministically creates this ordering:
//
//   waiter times out -> peer changes the queue and unlocks -> waiter reacquires the mutex
//
// This is the ordering that made the old notifier and the timed waiter decrement the same waiter
// registration. Later peer operations are not blocked, so the test can verify whether a real
// waiter receives the notification that follows the corrupted registration.
class GetPutRaceCoordinator {
public:
    // The first callback is the waiter used to create the counter corruption. The second callback
    // is the waiter used to verify that the next notification is not lost.
    void get_waiter_registered() {
        std::lock_guard lock(_get_lock);
        ++_get_waiter_count;
        _get_cv.notify_all();
    }

    void put_waiter_registered() {
        std::lock_guard lock(_put_lock);
        ++_put_waiter_count;
        _put_cv.notify_all();
    }

    void block_first_get() {
        std::unique_lock lock(_get_lock);
        ++_get_operation_count;
        // Only the first get must hold the queue mutex across the put timeout. The later get must
        // proceed normally to exercise BlockingQueue's put notification decision.
        if (_get_operation_count != 1) {
            return;
        }
        _first_get_has_lock = true;
        _get_cv.notify_all();
        _get_cv.wait(lock, [&] { return _release_get; });
    }

    void block_first_put() {
        std::unique_lock lock(_put_lock);
        ++_put_operation_count;
        // Only the first put must hold the queue mutex across the get timeout. The later put must
        // proceed normally to exercise BlockingQueue's get notification decision.
        if (_put_operation_count != 1) {
            return;
        }
        _first_put_has_lock = true;
        _put_cv.notify_all();
        _put_cv.wait(lock, [&] { return _release_put; });
    }

    template <typename Rep, typename Period>
    bool wait_for_get_waiter_count(int expected,
                                   const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock lock(_get_lock);
        return _get_cv.wait_for(lock, timeout, [&] { return _get_waiter_count >= expected; });
    }

    template <typename Rep, typename Period>
    bool wait_for_put_waiter_count(int expected,
                                   const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock lock(_put_lock);
        return _put_cv.wait_for(lock, timeout, [&] { return _put_waiter_count >= expected; });
    }

    template <typename Rep, typename Period>
    bool wait_for_first_get_lock(const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock lock(_get_lock);
        return _get_cv.wait_for(lock, timeout, [&] { return _first_get_has_lock; });
    }

    template <typename Rep, typename Period>
    bool wait_for_first_put_lock(const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock lock(_put_lock);
        return _put_cv.wait_for(lock, timeout, [&] { return _first_put_has_lock; });
    }

    void release_blocked_get() {
        std::lock_guard lock(_get_lock);
        _release_get = true;
        _get_cv.notify_all();
    }

    void release_blocked_put() {
        std::lock_guard lock(_put_lock);
        _release_put = true;
        _put_cv.notify_all();
    }

private:
    // Coordinates get waiter registration and a get operation that holds the queue mutex. This
    // state is independent from the put path so each callback changes only get-related state.
    std::mutex _get_lock;
    std::condition_variable _get_cv;
    int _get_waiter_count = 0;
    int _get_operation_count = 0;
    bool _first_get_has_lock = false;
    bool _release_get = false;

    // Coordinates put waiter registration and a put operation that holds the queue mutex.
    std::mutex _put_lock;
    std::condition_variable _put_cv;
    int _put_waiter_count = 0;
    int _put_operation_count = 0;
    bool _first_put_has_lock = false;
    bool _release_put = false;
};

class BlockingQueueWaiterTest : public testing::Test {
protected:
    void SetUp() override {
        _sync_point = SyncPoint::get_instance();
        _sync_point->clear_all_call_backs();
        _sync_point->clear_trace();
        _sync_point->enable_processing();
    }

    void TearDown() override {
        _sync_point->disable_processing();
        _sync_point->clear_all_call_backs();
        _sync_point->clear_trace();
    }

    SyncPoint* _sync_point = nullptr;
};

// Verifies that a timed get remains the sole owner of its _get_waiting registration. The first
// phase forces try_put to hold the queue mutex after the get timeout expires; the old code made
// try_put and the timed get both decrement the same registration, wrapping the counter to
// SIZE_MAX. The second phase registers another get and verifies that a following try_put observes
// that waiter and wakes it immediately instead of leaving it blocked until the timeout fallback.
TEST_F(BlockingQueueWaiterTest, TimedGetNotificationRace) {
    // The first wait is deliberately short so the producer can hold the queue mutex past its
    // deadline. The second wait has a 30-second fallback, while the assertion waits only 5
    // seconds; therefore, a ready future proves that notify_one() woke it rather than its timeout.
    constexpr int64_t kTimedGetWaitTimeoutMs = 100;
    constexpr int64_t kNotifiedGetWaitTimeoutMs = 30 * 1000;
    constexpr auto kCoordinationTimeout = std::chrono::seconds(5);
    constexpr auto kGetNotificationDeadline = std::chrono::seconds(5);

    BlockingQueue<int32_t> get_queue(1);
    GetPutRaceCoordinator get_race;
    _sync_point->set_call_back("BlockingQueue::controlled_blocking_get::before_wait",
                               [&](auto&&) { get_race.get_waiter_registered(); });
    _sync_point->set_call_back("BlockingQueue::try_put::after_lock",
                               [&](auto&&) { get_race.block_first_put(); });

    std::promise<int32_t> timed_get_promise;
    auto timed_get_future = timed_get_promise.get_future();
    std::promise<bool> try_put_promise;
    auto try_put_future = try_put_promise.get_future();
    std::promise<int32_t> notified_get_promise;
    auto notified_get_future = notified_get_promise.get_future();
    std::vector<std::thread> get_threads;
    // Keep this guard after every promise and future declaration. On a fatal assertion it runs
    // first, releases either blocked callback, shuts down the queue, and joins all workers before
    // their referenced asynchronous state is destroyed.
    Defer cleanup {[&] {
        get_race.release_blocked_put();
        get_queue.shutdown();
        for (auto& thread : get_threads) {
            thread.join();
        }
    }};

    get_threads.emplace_back([&] {
        int32_t get_value = -1;
        get_queue.controlled_blocking_get(&get_value, kTimedGetWaitTimeoutMs);
        timed_get_promise.set_value(get_value);
    });
    ASSERT_TRUE(get_race.wait_for_get_waiter_count(1, kCoordinationTimeout));
    EXPECT_EQ(get_queue.get_waiting_count_for_test(), 1);
    EXPECT_EQ(get_queue.put_waiting_count_for_test(), 0);

    // Phase 1: timed_get has registered _get_waiting=1 and released the queue mutex in
    // wait_for(). try_put then acquires that mutex and is stopped by the callback. Keeping try_put
    // there for twice the get timeout guarantees that timed_get expires while it is unable to
    // reacquire the mutex.
    get_threads.emplace_back([&] { try_put_promise.set_value(get_queue.try_put(1)); });
    ASSERT_TRUE(get_race.wait_for_first_put_lock(kCoordinationTimeout));

    std::this_thread::sleep_for(std::chrono::milliseconds(2 * kTimedGetWaitTimeoutMs));
    get_race.release_blocked_put();
    // In the old implementation try_put decremented _get_waiting from 1 to 0 before notifying,
    // then timed_get reacquired the mutex and decremented the same registration again because
    // wait_for() returned timeout. The size_t counter consequently wrapped from 0 to SIZE_MAX. In
    // the fixed implementation only timed_get owns the registration, so the counter returns to
    // zero.
    ASSERT_EQ(try_put_future.wait_for(kCoordinationTimeout), std::future_status::ready);
    EXPECT_TRUE(try_put_future.get());
    ASSERT_EQ(timed_get_future.wait_for(kCoordinationTimeout), std::future_status::ready);
    EXPECT_EQ(timed_get_future.get(), 1);
    EXPECT_EQ(get_queue.get_waiting_count_for_test(), 0);
    EXPECT_EQ(get_queue.put_waiting_count_for_test(), 0);

    // Phase 2: in the old implementation notified_get changed _get_waiting from SIZE_MAX to 0. The
    // following try_put therefore observed no get waiter and skipped notify_one(), leaving
    // notified_get asleep until its 30-second fallback. With correct accounting notified_get
    // changes the counter from 0 to 1 and try_put wakes it within the 5-second notification
    // deadline.
    get_threads.emplace_back([&] {
        int32_t get_value = -1;
        get_queue.controlled_blocking_get(&get_value, kNotifiedGetWaitTimeoutMs);
        notified_get_promise.set_value(get_value);
    });
    ASSERT_TRUE(get_race.wait_for_get_waiter_count(2, kCoordinationTimeout));
    EXPECT_EQ(get_queue.get_waiting_count_for_test(), 1);
    EXPECT_EQ(get_queue.put_waiting_count_for_test(), 0);

    EXPECT_TRUE(get_queue.try_put(2));
    ASSERT_EQ(notified_get_future.wait_for(kGetNotificationDeadline), std::future_status::ready);
    EXPECT_EQ(notified_get_future.get(), 2);
    EXPECT_EQ(get_queue.get_waiting_count_for_test(), 0);
    EXPECT_EQ(get_queue.put_waiting_count_for_test(), 0);
}

// Verifies the symmetric rule for a timed put and its _put_waiting registration. The first phase
// forces blocking_get to hold the queue mutex after the put timeout expires; the old code made the
// get and timed put both decrement the registration and wrap the counter. The second phase
// registers another put, removes an item with blocking_get, and verifies that the put is notified
// immediately and can add its item without waiting for the timeout fallback.
TEST_F(BlockingQueueWaiterTest, TimedPutNotificationRace) {
    // As in TimedGetNotificationRace, the 30-second second wait and 5-second assertion deadline
    // distinguish a real notification from the periodic timeout fallback.
    constexpr int64_t kTimedPutWaitTimeoutMs = 100;
    constexpr int64_t kNotifiedPutWaitTimeoutMs = 30 * 1000;
    constexpr auto kCoordinationTimeout = std::chrono::seconds(5);
    constexpr auto kPutNotificationDeadline = std::chrono::seconds(5);

    BlockingQueue<int32_t> put_queue(1);
    ASSERT_TRUE(put_queue.try_put(0));

    GetPutRaceCoordinator put_race;
    _sync_point->set_call_back("BlockingQueue::controlled_blocking_put::before_wait",
                               [&](auto&&) { put_race.put_waiter_registered(); });
    _sync_point->set_call_back("BlockingQueue::controlled_blocking_get::after_lock",
                               [&](auto&&) { put_race.block_first_get(); });

    std::promise<bool> timed_put_promise;
    auto timed_put_future = timed_put_promise.get_future();
    std::promise<int32_t> blocking_get_promise;
    auto blocking_get_future = blocking_get_promise.get_future();
    std::promise<bool> notified_put_promise;
    auto notified_put_future = notified_put_promise.get_future();
    std::vector<std::thread> put_threads;
    // The guard is declared last so every failure path joins the workers before their promises and
    // futures are destroyed.
    Defer cleanup {[&] {
        put_race.release_blocked_get();
        put_queue.shutdown();
        for (auto& thread : put_threads) {
            thread.join();
        }
    }};

    put_threads.emplace_back([&] {
        timed_put_promise.set_value(put_queue.controlled_blocking_put(1, kTimedPutWaitTimeoutMs));
    });
    ASSERT_TRUE(put_race.wait_for_put_waiter_count(1, kCoordinationTimeout));
    EXPECT_EQ(put_queue.get_waiting_count_for_test(), 0);
    EXPECT_EQ(put_queue.put_waiting_count_for_test(), 1);

    // Phase 1 is the put-side mirror of the get race. timed_put registers _put_waiting=1 and waits
    // because the queue contains 0. blocking_get then acquires and holds the queue mutex until the
    // put timeout has expired, preventing timed_put from returning from wait_for().
    put_threads.emplace_back([&] {
        int32_t get_value = -1;
        put_queue.blocking_get(&get_value);
        blocking_get_promise.set_value(get_value);
    });
    ASSERT_TRUE(put_race.wait_for_first_get_lock(kCoordinationTimeout));

    std::this_thread::sleep_for(std::chrono::milliseconds(2 * kTimedPutWaitTimeoutMs));
    put_race.release_blocked_get();
    // Previously blocking_get decremented _put_waiting when it removed 0, and timed_put decremented
    // it again after reacquiring the mutex with a timeout result. The counter wrapped to SIZE_MAX
    // before timed_put put 1. With single-owner accounting timed_put performs the only decrement
    // and leaves the counter at zero.
    ASSERT_EQ(blocking_get_future.wait_for(kCoordinationTimeout), std::future_status::ready);
    EXPECT_EQ(blocking_get_future.get(), 0);
    ASSERT_EQ(timed_put_future.wait_for(kCoordinationTimeout), std::future_status::ready);
    EXPECT_TRUE(timed_put_future.get());
    EXPECT_EQ(put_queue.get_waiting_count_for_test(), 0);
    EXPECT_EQ(put_queue.put_waiting_count_for_test(), 0);

    // Phase 2: the old SIZE_MAX counter wrapped to zero when notified_put registered. The following
    // blocking_get therefore skipped notify_one(), so notified_put could not put 2 before the
    // 5-second deadline. The fixed counter is one, causing blocking_get to wake notified_put
    // immediately and allowing the final value check to finish.
    put_threads.emplace_back([&] {
        notified_put_promise.set_value(
                put_queue.controlled_blocking_put(2, kNotifiedPutWaitTimeoutMs));
    });
    ASSERT_TRUE(put_race.wait_for_put_waiter_count(2, kCoordinationTimeout));
    EXPECT_EQ(put_queue.get_waiting_count_for_test(), 0);
    EXPECT_EQ(put_queue.put_waiting_count_for_test(), 1);

    int32_t get_value = -1;
    EXPECT_TRUE(put_queue.blocking_get(&get_value));
    EXPECT_EQ(get_value, 1);
    ASSERT_EQ(notified_put_future.wait_for(kPutNotificationDeadline), std::future_status::ready);
    EXPECT_TRUE(notified_put_future.get());
    EXPECT_EQ(put_queue.get_waiting_count_for_test(), 0);
    EXPECT_EQ(put_queue.put_waiting_count_for_test(), 0);
    EXPECT_TRUE(put_queue.blocking_get(&get_value));
    EXPECT_EQ(get_value, 2);
}

} // namespace doris
