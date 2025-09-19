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

#include "vec/exec/executor/time_sharing/time_sharing_task_executor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <random>
#include <thread>

#include "vec/exec/executor/ticker.h"
#include "vec/exec/executor/time_sharing/time_sharing_task_handle.h"

namespace doris {
namespace vectorized {

class PhaseController {
public:
    explicit PhaseController(int parties) : _parties(parties), _count(parties), _generation(0) {}

    int arrive_and_await() {
        std::unique_lock<std::mutex> lock(_mutex);
        int current_generation = _generation;
        const int arrived_count = --_count;

        if (arrived_count == 0) {
            _count = _parties;
            _generation++;
            _cv.notify_all();
        } else {
            _cv.wait(lock, [this, current_generation] {
                return _generation > current_generation || _count == 0;
            });
        }
        return _generation;
    }

    void arrive_and_deregister() {
        std::unique_lock<std::mutex> lock(_mutex);
        _parties--;
        const int arrived_count = --_count;

        if (arrived_count == 0) {
            _count = _parties;
            _generation++;
            _cv.notify_all();
        } else {
            _cv.notify_all();
        }
    }

    void register_party() {
        std::lock_guard<std::mutex> lock(_mutex);
        _parties++;
        _count++;
        _cv.notify_all();
    }

    int parties() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _parties;
    }

    int count() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _count;
    }

    int generation() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _generation;
    }

private:
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    int _parties;
    int _count;
    int _generation;
};

class PhaseControllerTest : public testing::Test {
protected:
    const int THREAD_COUNT = 4;
    std::shared_ptr<PhaseController> controller;

    void SetUp() override { controller = std::make_shared<PhaseController>(THREAD_COUNT); }
};

TEST_F(PhaseControllerTest, basic_synchronization) {
    std::atomic<int> ready {0};
    std::vector<std::thread> threads;
    const int GENERATIONS = 3;

    for (int g = 0; g < GENERATIONS; ++g) {
        for (int i = 0; i < THREAD_COUNT; ++i) {
            threads.emplace_back([&, g] {
                if (ready.fetch_add(1) == THREAD_COUNT - 1) {
                    ready.store(0);
                } else {
                    while (ready.load() != 0)
                        ;
                }

                int gen = controller->arrive_and_await();
                EXPECT_EQ(gen, g + 1);
            });
        }

        for (auto& t : threads) t.join();
        threads.clear();

        EXPECT_EQ(controller->generation(), g + 1);
        EXPECT_EQ(controller->count(), THREAD_COUNT);
    }
}

TEST_F(PhaseControllerTest, dynamic_registration) {
    constexpr int NEW_PARTIES = 2;
    std::atomic<int> completed {0};
    std::vector<std::thread> threads;
    std::mutex cv_mtx;
    std::condition_variable cv;

    controller = std::make_shared<PhaseController>(THREAD_COUNT + 1);

    for (int i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back([&] {
            controller->arrive_and_await();
            controller->arrive_and_await();
            completed++;
        });
    }

    int main_gen = controller->arrive_and_await();
    EXPECT_EQ(main_gen, 1);

    std::vector<std::thread> new_threads;
    for (int i = 0; i < NEW_PARTIES; ++i) {
        new_threads.emplace_back([&] {
            {
                std::lock_guard<std::mutex> lk(cv_mtx);
                controller->register_party();
            }
            cv.notify_one();

            controller->arrive_and_await();
            completed++;
        });
    }

    {
        std::unique_lock<std::mutex> lk(cv_mtx);
        cv.wait(lk, [&] { return controller->parties() == THREAD_COUNT + 1 + NEW_PARTIES; });
    }

    main_gen = controller->arrive_and_await();
    EXPECT_EQ(main_gen, 2);

    for (auto& t : threads) t.join();
    for (auto& t : new_threads) t.join();

    EXPECT_EQ(completed.load(), THREAD_COUNT + NEW_PARTIES);
    EXPECT_EQ(controller->parties(), THREAD_COUNT + 1 + NEW_PARTIES);
}

class TestingTicker final : public Ticker {
public:
    TestingTicker() : time_(0) {}

    int64_t read() const override { return time_.load(std::memory_order_relaxed); }

    auto now() const { return time_.load(); }

    void increment(int64_t delta, std::chrono::nanoseconds unit) {
        if (delta < 0) {
            throw std::invalid_argument("delta is negative");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        time_ += unit.count() * delta;
    }

private:
    std::atomic<int64_t> time_;
    mutable std::mutex mutex_;
};

class TestingSplitRunner : public SplitRunner {
public:
    TestingSplitRunner(std::string name, std::shared_ptr<TestingTicker> ticker,
                       std::shared_ptr<PhaseController> global_controller,
                       std::shared_ptr<PhaseController> begin_controller,
                       std::shared_ptr<PhaseController> end_controller, int required_phases,
                       int quanta_time_ms)
            : _name(std::move(name)),
              _ticker(ticker),
              _global_controller(global_controller),
              _begin_controller(begin_controller),
              _end_controller(end_controller),
              _required_phases(required_phases),
              _quanta_time(quanta_time_ms),
              _completed_phases(0),
              _first_phase(-1),
              _last_phase(-1),
              _started(false) {
        _begin_controller->register_party();
        _end_controller->register_party();

        if (_global_controller->parties() == 0) {
            _global_controller->register_party();
        }
    }

    Status init() override { return Status::OK(); }

    Result<SharedListenableFuture<Void>> process_for(std::chrono::nanoseconds) override {
        _started = true;
        _ticker->increment(_quanta_time, std::chrono::milliseconds(1));
        std::promise<void> phase_promise;
        std::future<void> phase_future = phase_promise.get_future();

        _global_controller->arrive_and_await();
        int generation = _begin_controller->arrive_and_await();
        int expected = -1;
        _first_phase.compare_exchange_weak(expected, generation - 1, std::memory_order_relaxed);
        _last_phase.store(generation, std::memory_order_relaxed);

        _end_controller->arrive_and_await();

        if (++_completed_phases >= _required_phases) {
            _end_controller->arrive_and_deregister();
            _begin_controller->arrive_and_deregister();
            _global_controller->arrive_and_deregister();
            _completion_future.set_value(Void {});
        }

        return SharedListenableFuture<Void>::create_ready();
    }

    int completed_phases() const { return _completed_phases; }

    int first_phase() const { return _first_phase; }

    int last_phase() const { return _last_phase; }

    void close(const Status& status) override {}

    std::string get_info() const override { return ""; }

    bool is_finished() override { return _completion_future.is_ready(); }

    Status finished_status() override { return _completion_future.get_status(); }

    bool is_started() const { return _started.load(); }

private:
    std::string _name;
    std::shared_ptr<TestingTicker> _ticker;
    std::shared_ptr<PhaseController> _global_controller;
    std::shared_ptr<PhaseController> _begin_controller;
    std::shared_ptr<PhaseController> _end_controller;
    int _required_phases;
    int _quanta_time;

    std::atomic<int> _completed_phases;
    std::atomic<int> _first_phase;
    std::atomic<int> _last_phase;
    std::atomic<bool> _started;
    ListenableFuture<Void> _completion_future {};
};

class TimeSharingTaskExecutorTest : public testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}

private:
    template <typename Container>
    void assert_split_states(int end_index, const Container& splits) {
        for (int i = 0; i <= end_index; ++i) {
            bool actual = splits[i]->is_started();
            EXPECT_TRUE(actual) << "Split " << i << " should have started";
        }

        for (int i = end_index + 1; i < splits.size(); ++i) {
            bool actual = splits[i]->is_started();
            EXPECT_FALSE(actual) << "Split " << i << " should not have started";
        }
    }

    void wait_until_splits_start(const std::vector<std::shared_ptr<TestingSplitRunner>>& splits) {
        constexpr auto TIMEOUT = std::chrono::seconds(30);
        auto start = std::chrono::steady_clock::now();

        while (std::any_of(splits.begin(), splits.end(),
                           [](const auto& split) { return !split->is_started(); })) {
            if (std::chrono::steady_clock::now() - start > TIMEOUT) {
                throw std::runtime_error("Timeout waiting for split to start.");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
};

TEST_F(TimeSharingTaskExecutorTest, test_tasks_complete) {
    auto ticker = std::make_shared<TestingTicker>();

    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = 4;
    thread_config.min_thread_num = 4;
    TimeSharingTaskExecutor executor(thread_config, 8, 3, 4, ticker);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        ticker->increment(20, std::chrono::milliseconds(1));
        TaskId task_id("test_task");
        auto task_handle = TEST_TRY(executor.create_task(
                task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1), std::nullopt));

        auto begin_phase = std::make_shared<PhaseController>(1);
        auto verification_complete = std::make_shared<PhaseController>(1);

        // add two jobs
        auto split1 = std::make_shared<TestingSplitRunner>(
                "split1", ticker, std::make_shared<PhaseController>(0), begin_phase,
                verification_complete, 10, 0);
        auto split2 = std::make_shared<TestingSplitRunner>(
                "split2", ticker, std::make_shared<PhaseController>(0), begin_phase,
                verification_complete, 10, 0);
        executor.enqueue_splits(task_handle, true, {split1, split2});
        EXPECT_EQ(split1->completed_phases(), 0);
        EXPECT_EQ(split2->completed_phases(), 0);

        // verify worker have arrived but haven't processed yet
        begin_phase->arrive_and_await();
        ticker->increment(60, std::chrono::seconds(1));
        ticker->increment(600, std::chrono::seconds(1));

        verification_complete->arrive_and_await();

        // advance one phase and verify
        begin_phase->arrive_and_await();
        EXPECT_EQ(split1->completed_phases(), 1);
        EXPECT_EQ(split2->completed_phases(), 1);

        verification_complete->arrive_and_await();

        // add one more job
        auto split3 = std::make_shared<TestingSplitRunner>(
                "split3", ticker, std::make_shared<PhaseController>(0), begin_phase,
                verification_complete, 10, 0);
        executor.enqueue_splits(task_handle, false, {split3});

        // advance one phase and verify
        begin_phase->arrive_and_await();
        EXPECT_EQ(split1->completed_phases(), 2);
        EXPECT_EQ(split2->completed_phases(), 2);
        EXPECT_EQ(split3->completed_phases(), 0);
        verification_complete->arrive_and_await();

        // advance to the end of the first two task and verify
        begin_phase->arrive_and_await();
        for (int i = 0; i < 7; i++) {
            verification_complete->arrive_and_await();
            begin_phase->arrive_and_await();
            EXPECT_EQ(begin_phase->generation(), verification_complete->generation() + 1);
        }
        EXPECT_EQ(split1->completed_phases(), 10);
        EXPECT_EQ(split2->completed_phases(), 10);
        EXPECT_EQ(split3->completed_phases(), 8);
        verification_complete->arrive_and_await();

        // advance two more times and verify
        begin_phase->arrive_and_await();
        verification_complete->arrive_and_await();
        begin_phase->arrive_and_await();
        EXPECT_EQ(split1->completed_phases(), 10);
        EXPECT_EQ(split2->completed_phases(), 10);
        EXPECT_EQ(split3->completed_phases(), 10);
        verification_complete->arrive_and_await();

        EXPECT_EQ(split1->first_phase(), 0);
        EXPECT_EQ(split2->first_phase(), 0);
        EXPECT_EQ(split3->first_phase(), 2);

        EXPECT_EQ(split1->last_phase(), 10);
        EXPECT_EQ(split2->last_phase(), 10);
        EXPECT_EQ(split3->last_phase(), 12);

        // no splits remaining
        ticker->increment(610, std::chrono::seconds(1));
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, test_quanta_fairness) {
    constexpr int TOTAL_PHASES = 11;
    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();

    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = 1;
    thread_config.min_thread_num = 1;
    TimeSharingTaskExecutor executor(thread_config, 2, 3, 4, ticker);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        ticker->increment(20, std::chrono::milliseconds(1));

        TaskId short_task_id("short_quanta");
        TaskId long_task_id("long_quanta");
        auto short_handle = TEST_TRY(executor.create_task(
                short_task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                std::nullopt));

        auto long_handle = TEST_TRY(executor.create_task(
                long_task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                std::nullopt));

        auto end_controller = std::make_shared<PhaseController>(0);

        auto short_split = std::make_shared<TestingSplitRunner>(
                "short", ticker, std::make_shared<PhaseController>(0),
                std::make_shared<PhaseController>(0), end_controller, TOTAL_PHASES, 10);

        auto long_split = std::make_shared<TestingSplitRunner>(
                "long", ticker, std::make_shared<PhaseController>(0),
                std::make_shared<PhaseController>(0), end_controller, TOTAL_PHASES, 20);

        executor.enqueue_splits(short_handle, true, {short_split});
        executor.enqueue_splits(long_handle, true, {long_split});

        for (int i = 0; i < TOTAL_PHASES; ++i) {
            end_controller->arrive_and_await();
        }

        EXPECT_GE(short_split->completed_phases(), 7);
        EXPECT_LE(short_split->completed_phases(), 8);
        EXPECT_GE(long_split->completed_phases(), 3);
        EXPECT_LE(long_split->completed_phases(), 4);

        end_controller->arrive_and_deregister();

    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, test_level_movement) {
    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = 2;
    thread_config.min_thread_num = 2;
    TimeSharingTaskExecutor executor(thread_config, 2, 3, 4, ticker);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        ticker->increment(20, std::chrono::milliseconds(1));

        TaskId task_id("test_task");
        auto task_handle =
                std::static_pointer_cast<TimeSharingTaskHandle>(TEST_TRY(executor.create_task(
                        task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                        std::nullopt)));

        auto global_controller =
                std::make_shared<PhaseController>(3); // 2 taskExecutor threads + test thread
        const int quanta_time_ms = 500;
        const int phases_per_second = 1000 / quanta_time_ms;
        const int total_phases =
                MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS
                        [MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS.size() - 1] *
                phases_per_second;

        auto split1 = std::make_shared<TestingSplitRunner>(
                "split1", ticker, global_controller, std::make_shared<PhaseController>(0),
                std::make_shared<PhaseController>(0), total_phases, quanta_time_ms);
        auto split2 = std::make_shared<TestingSplitRunner>(
                "split2", ticker, global_controller, std::make_shared<PhaseController>(0),
                std::make_shared<PhaseController>(0), total_phases, quanta_time_ms);

        executor.enqueue_splits(task_handle, false, {split1, split2});

        int completed_phases = 0;
        for (int i = 0; i < MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS.size() - 1; ++i) {
            const int target_seconds = MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i + 1];
            while ((completed_phases / phases_per_second) < target_seconds) {
                global_controller->arrive_and_await();
                ++completed_phases;
            }

            EXPECT_EQ(task_handle->priority().level(), i + 1);
        }

        global_controller->arrive_and_deregister();
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, test_level_multipliers) {
    constexpr int TASK_THREADS = 6;
    constexpr int CONCURRENCY = 3;

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    std::shared_ptr<MultilevelSplitQueue> split_queue = std::make_shared<MultilevelSplitQueue>(2);
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = TASK_THREADS;
    thread_config.min_thread_num = TASK_THREADS;
    TimeSharingTaskExecutor executor(thread_config, CONCURRENCY, 3, 4, ticker, split_queue);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        ticker->increment(20, std::chrono::milliseconds(1));

        for (int i = 0;
             i < (sizeof(MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS) / sizeof(int) - 1); ++i) {
            TaskId task_id1("test1"), task_id2("test2"), task_id3("test3");
            auto task_handle1 = TEST_TRY(executor.create_task(
                    task_id1, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                    std::nullopt));
            auto task_handle2 = TEST_TRY(executor.create_task(
                    task_id2, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                    std::nullopt));
            auto task_handle3 = TEST_TRY(executor.create_task(
                    task_id3, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                    std::nullopt));

            // move task 0 to next level
            auto task0_job = std::make_shared<TestingSplitRunner>(
                    "task0", ticker, std::make_shared<PhaseController>(0),
                    std::make_shared<PhaseController>(0), std::make_shared<PhaseController>(0), 1,
                    MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i + 1] * 1000);
            executor.enqueue_splits(task_handle1, false, {task0_job});

            // move tasks 1 and 2 to this level
            auto task1_job = std::make_shared<TestingSplitRunner>(
                    "task1", ticker, std::make_shared<PhaseController>(0),
                    std::make_shared<PhaseController>(0), std::make_shared<PhaseController>(0), 1,
                    MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i] * 1000);
            auto task2_job = std::make_shared<TestingSplitRunner>(
                    "task2", ticker, std::make_shared<PhaseController>(0),
                    std::make_shared<PhaseController>(0), std::make_shared<PhaseController>(0), 1,
                    MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i] * 1000);
            executor.enqueue_splits(task_handle2, false, {task1_job});
            executor.enqueue_splits(task_handle3, false, {task2_job});

            // then, start new concurrencies for all tasks
            while (!task0_job->is_finished() || !task1_job->is_finished() ||
                   !task2_job->is_finished()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            auto global_controller =
                    std::make_shared<PhaseController>(7); // 6 taskExecutor threads + test thread
            const int phases_for_next_level = MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i + 1] -
                                              MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i];
            std::array<std::shared_ptr<TestingSplitRunner>, 6> splits;

            for (int j = 0; j < 6; ++j) {
                splits[j] = std::make_shared<TestingSplitRunner>(
                        "split" + std::to_string(j), ticker, global_controller,
                        std::make_shared<PhaseController>(0), std::make_shared<PhaseController>(0),
                        phases_for_next_level, 1000);
            }

            executor.enqueue_splits(task_handle1, false, {splits[0], splits[1]});
            executor.enqueue_splits(task_handle2, false, {splits[2], splits[3]});
            executor.enqueue_splits(task_handle3, false, {splits[4], splits[5]});

            // run all three concurrencies
            int lower_level_start = 0, higher_level_start = 0;
            for (int j = 2; j < 6; ++j) lower_level_start += splits[j]->completed_phases();
            for (int j = 0; j < 2; ++j) higher_level_start += splits[j]->completed_phases();

            while (std::any_of(splits.begin(), splits.end(),
                               [](auto& d) { return !d->is_finished(); })) {
                global_controller->arrive_and_await();

                int lower_level_end = 0, higher_level_end = 0;
                for (int j = 2; j < 6; ++j) lower_level_end += splits[j]->completed_phases();
                for (int j = 0; j < 2; ++j) higher_level_end += splits[j]->completed_phases();

                int lower_level_time = lower_level_end - lower_level_start;
                int higher_level_time = higher_level_end - higher_level_start;

                if (higher_level_time > 20) {
                    EXPECT_GT(lower_level_time, higher_level_time * 2 - 10);
                    EXPECT_LT(higher_level_time, lower_level_time * 2 + 10);
                }
            }

            global_controller->arrive_and_deregister();
            static_cast<void>(executor.remove_task(task_handle1));
            static_cast<void>(executor.remove_task(task_handle2));
            static_cast<void>(executor.remove_task(task_handle3));
        }
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, test_task_handle) {
    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = 4;
    thread_config.min_thread_num = 4;
    TimeSharingTaskExecutor executor(thread_config, 8, 3, 4, ticker);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        TaskId task_id("test_task");
        auto task_handle =
                std::static_pointer_cast<TimeSharingTaskHandle>(TEST_TRY(executor.create_task(
                        task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                        std::nullopt)));

        auto begin_phase = std::make_shared<PhaseController>(1);
        auto verification_complete = std::make_shared<PhaseController>(1);

        auto split1 = std::make_shared<TestingSplitRunner>(
                "split1", ticker, std::make_shared<PhaseController>(0), begin_phase,
                verification_complete, 10, 0);

        auto split2 = std::make_shared<TestingSplitRunner>(
                "split2", ticker, std::make_shared<PhaseController>(0), begin_phase,
                verification_complete, 10, 0);

        // force enqueue a split
        executor.enqueue_splits(task_handle, true, {split1});
        EXPECT_EQ(task_handle->running_leaf_splits(), 0);

        // normal enqueue a split
        executor.enqueue_splits(task_handle, false, {split2});
        EXPECT_EQ(task_handle->running_leaf_splits(), 1);

        // let the split continue to run
        begin_phase->arrive_and_deregister();
        verification_complete->arrive_and_deregister();

        while (!split1->is_finished() || !split2->is_finished()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, test_level_contribution_cap) {
    auto split_queue = std::make_shared<MultilevelSplitQueue>(2);
    auto handle0 = std::make_shared<TimeSharingTaskHandle>(
            TaskId("test0"), split_queue, []() { return 1.0; }, 1, std::chrono::seconds(1),
            std::nullopt);
    auto handle1 = std::make_shared<TimeSharingTaskHandle>(
            TaskId("test1"), split_queue, []() { return 1.0; }, 1, std::chrono::seconds(1),
            std::nullopt);

    for (size_t i = 0; i < MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS.size() - 1; ++i) {
        int64_t level_advance_nanos =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::seconds(MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i + 1] -
                                             MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i]))
                        .count();

        handle0->add_scheduled_nanos(level_advance_nanos);
        EXPECT_EQ(handle0->priority().level(), i + 1);

        handle1->add_scheduled_nanos(level_advance_nanos);
        EXPECT_EQ(handle1->priority().level(), i + 1);

        int64_t expected_level_nanos =
                2 * std::min(level_advance_nanos, MultilevelSplitQueue::LEVEL_CONTRIBUTION_CAP);
        EXPECT_EQ(split_queue->level_scheduled_time(i), expected_level_nanos);
        EXPECT_EQ(split_queue->level_scheduled_time(i + 1), 0);
    }
}

TEST_F(TimeSharingTaskExecutorTest, test_update_level_with_cap) {
    auto split_queue = std::make_shared<MultilevelSplitQueue>(2);
    auto handle0 = std::make_shared<TimeSharingTaskHandle>(
            TaskId("test0"), split_queue, []() { return 1.0; }, 1, std::chrono::seconds(1),
            std::nullopt);

    int64_t quanta_nanos =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::minutes(10)).count();
    handle0->add_scheduled_nanos(quanta_nanos);
    int64_t capped_nanos = std::min(quanta_nanos, MultilevelSplitQueue::LEVEL_CONTRIBUTION_CAP);

    for (size_t i = 0; i < MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS.size() - 1; ++i) {
        int64_t level_duration_nanos =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::seconds(MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i + 1] -
                                             MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i]))
                        .count();

        int64_t level_time_nanos = std::min(level_duration_nanos, capped_nanos);
        EXPECT_EQ(split_queue->level_scheduled_time(i), level_time_nanos);
        capped_nanos -= level_time_nanos;
    }
}

TEST_F(TimeSharingTaskExecutorTest, test_min_max_concurrency_per_task) {
    constexpr int MAX_DRIVERS_PER_TASK = 2;
    constexpr int TASK_THREADS = 4;
    constexpr int CONCURRENCY = 16;
    constexpr int BATCH_COUNT = 4;
    constexpr int SPLITS_PER_BATCH = 2;

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    std::shared_ptr<MultilevelSplitQueue> split_queue = std::make_shared<MultilevelSplitQueue>(2);
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = TASK_THREADS;
    thread_config.min_thread_num = TASK_THREADS;
    TimeSharingTaskExecutor executor(thread_config, CONCURRENCY, 1, MAX_DRIVERS_PER_TASK, ticker,
                                     split_queue);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        TaskId task_id("test_task");
        auto task_handle = TEST_TRY(executor.create_task(
                task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1), std::nullopt));

        // enqueue all batches of splits
        std::array<std::shared_ptr<TestingSplitRunner>, BATCH_COUNT * SPLITS_PER_BATCH> splits;
        std::array<std::shared_ptr<PhaseController>, BATCH_COUNT> batch_controllers;

        for (int batch = 0; batch < BATCH_COUNT; ++batch) {
            batch_controllers[batch] = std::make_shared<PhaseController>(0);
            batch_controllers[batch]->register_party();
            auto& controller = batch_controllers[batch];

            for (int i = 0; i < SPLITS_PER_BATCH; ++i) {
                splits[batch * SPLITS_PER_BATCH + i] = std::make_shared<TestingSplitRunner>(
                        "batch" + std::to_string(batch) + "_split" + std::to_string(i), ticker,
                        std::make_shared<PhaseController>(0), std::make_shared<PhaseController>(0),
                        controller, 1, 0);
            }

            executor.enqueue_splits(
                    task_handle, false,
                    {splits[batch * SPLITS_PER_BATCH], splits[batch * SPLITS_PER_BATCH + 1]});
        }

        // assert that the splits are processed in batches as expected
        for (int current_batch = 0; current_batch < BATCH_COUNT; ++current_batch) {
            // wait until the current batch starts
            wait_until_splits_start({splits[current_batch * SPLITS_PER_BATCH],
                                     splits[current_batch * SPLITS_PER_BATCH + 1]});
            // assert that only the splits including and up to the current batch are running and the rest haven't started yet
            assert_split_states(current_batch * SPLITS_PER_BATCH + 1, splits);
            // complete the current batch
            batch_controllers[current_batch]->arrive_and_deregister();
        }

        for (auto& split : splits) {
            while (!split->is_finished()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, test_user_specified_max_concurrencies_per_task) {
    constexpr int TASK_THREADS = 4;
    constexpr int CONCURRENCY = 16;
    constexpr int BATCH_COUNT = 4;

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    std::shared_ptr<MultilevelSplitQueue> split_queue = std::make_shared<MultilevelSplitQueue>(2);
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = TASK_THREADS;
    thread_config.min_thread_num = TASK_THREADS;
    TimeSharingTaskExecutor executor(thread_config, CONCURRENCY, 2, 4, ticker, split_queue);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        // overwrite the max concurrencies per task to be 1
        TaskId task_id("user_specified_task");
        auto task_handle = TEST_TRY(executor.create_task(
                task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                std::optional<int>(1)));

        // enqueue all batches of splits
        std::array<std::shared_ptr<TestingSplitRunner>, BATCH_COUNT> splits;
        std::array<std::shared_ptr<PhaseController>, BATCH_COUNT> batch_controllers;
        for (int batch = 0; batch < BATCH_COUNT; ++batch) {
            batch_controllers[batch] = std::make_shared<PhaseController>(0);
            batch_controllers[batch]->register_party();

            splits[batch] = std::make_shared<TestingSplitRunner>(
                    "batch" + std::to_string(batch), ticker, std::make_shared<PhaseController>(0),
                    std::make_shared<PhaseController>(0), batch_controllers[batch], 1, 0);

            executor.enqueue_splits(task_handle, false, {splits[batch]});
        }

        // assert that the splits are processed in batches as expected
        for (int current_batch = 0; current_batch < BATCH_COUNT; ++current_batch) {
            // wait until the current batch starts
            wait_until_splits_start({splits[current_batch]});
            // assert that only the splits including and up to the current batch are running and the rest haven't started yet
            assert_split_states(current_batch, splits);
            // complete the current batch
            batch_controllers[current_batch]->arrive_and_deregister();
        }

        for (auto& split : splits) {
            while (!split->is_finished()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest,
       test_min_concurrency_per_task_when_target_concurrency_increases) {
    constexpr int TASK_THREADS = 4;
    constexpr int CONCURRENCY = 1;

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    std::shared_ptr<MultilevelSplitQueue> split_queue = std::make_shared<MultilevelSplitQueue>(2);
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = TASK_THREADS;
    thread_config.min_thread_num = TASK_THREADS;
    TimeSharingTaskExecutor executor(thread_config, CONCURRENCY, 2, 2, ticker, split_queue);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        TaskId task_id("test_query");
        auto task_handle = TEST_TRY(executor.create_task(
                task_id, []() { return 0.0; }, 1, std::chrono::milliseconds(1),
                std::optional<int>(2)));

        // create 3 splits
        constexpr int BATCH_COUNT = 3;
        std::array<std::shared_ptr<TestingSplitRunner>, BATCH_COUNT> splits;
        std::array<std::shared_ptr<PhaseController>, BATCH_COUNT> batch_controllers;
        for (int batch = 0; batch < BATCH_COUNT; ++batch) {
            batch_controllers[batch] = std::make_shared<PhaseController>(0);
            batch_controllers[batch]->register_party();

            splits[batch] = std::make_shared<TestingSplitRunner>(
                    "split" + std::to_string(batch), ticker, std::make_shared<PhaseController>(0),
                    std::make_shared<PhaseController>(0), batch_controllers[batch], 1, 0);
        }

        executor.enqueue_splits(task_handle, false, {splits[0], splits[1], splits[2]});
        // wait until first split starts
        wait_until_splits_start({splits[0]});
        // remaining splits shouldn't start because initial split concurrency is 1
        assert_split_states(0, splits);

        // complete first split (SplitConcurrencyController for TaskHandle should increase concurrency since buffer is underutilized)
        batch_controllers[0]->arrive_and_deregister();

        // 2 remaining splits should be started
        wait_until_splits_start({splits[1], splits[2]});

        assert_split_states(2, splits);

        for (int batch = 1; batch < BATCH_COUNT; ++batch) {
            batch_controllers[batch]->arrive_and_deregister();
        }

        for (auto& split : splits) {
            while (!split->is_finished()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

} // namespace vectorized
} // namespace doris
