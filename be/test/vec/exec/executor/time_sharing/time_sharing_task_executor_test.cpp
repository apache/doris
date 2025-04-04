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
            // 重置计数器并进入下一代
            _count = _parties;
            _generation++;
            _cv.notify_all();
        } else {
            // 等待直到进入下一代
            _cv.wait(lock, [this, current_generation] {
                return _generation > current_generation || _count == 0;
            });
        }
        return _generation;
    }

    void arrive_and_deregister() {
        std::unique_lock<std::mutex> lock(_mutex);
        _parties--; // 永久减少参与方数量
        const int arrived_count = --_count;

        //        fprintf(stderr, "[PhaseController] 注销参与者，剩余参与方=%d\n", _parties);

        if (arrived_count == 0) {
            // 立即触发代际更新
            _count = _parties;
            _generation++;
            _cv.notify_all();
        } else {
            // 仅记录当前代际，不阻塞等待
            _cv.notify_all(); // 唤醒可能存在的等待线程
        }
    }

    void register_party() {
        std::lock_guard<std::mutex> lock(_mutex);
        _parties++;
        _count++;         // 立即生效新参与者
        _cv.notify_all(); // 唤醒可能存在的等待线程
        //fprintf(stderr, "[PhaseController] 注册新参与者，当前总参与方=%d\n", _parties);
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
    int _generation; // 新增代际计数器
};

// 线程安全的测试用Ticker
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
        //              _completion_future(_completion_promise.get_future()) {
        _begin_controller->register_party();
        _end_controller->register_party();

        if (_global_controller->parties() == 0) {
            _global_controller->register_party();
        }
    }

    Status init() override { return Status::OK(); }

    Result<SharedListenableFuture<Void>> process_for(std::chrono::nanoseconds) override {
        //        fprintf(stderr, "[%zu] TestingSplitRunner::process_for()\n",
        //                std::hash<std::thread::id> {}(std::this_thread::get_id()));
        _started = true;
        _ticker->increment(_quanta_time, std::chrono::milliseconds(1));
        // 创建新的promise/future对
        std::promise<void> phase_promise;
        std::future<void> phase_future = phase_promise.get_future();

        // Global phase synchronization
        //        fprintf(stderr,
        //                "[%zu] TestingSplitRunner::process_for() _global_controller.arrive_and_await(),"
        //                " _global_controller.count(): %d, _global_controller.parties(): %d;\n",
        //                std::hash<std::thread::id> {}(std::this_thread::get_id()),
        //                _global_controller->count(), _global_controller->parties());
        _global_controller->arrive_and_await();
        //        fprintf(stderr,
        //                "[%zu] TestingSplitRunner::process_for() _global_controller.arrive_and_await() "
        //                "finished;\n",
        //                std::hash<std::thread::id> {}(std::this_thread::get_id()));

        // Begin quanta phase
        //        static_cast<void>(_begin_controller);
        int generation = _begin_controller->arrive_and_await();
        int expected = -1;
        _first_phase.compare_exchange_weak(expected, generation - 1, std::memory_order_relaxed);
        _last_phase.store(generation, std::memory_order_relaxed);

        //        fprintf(stderr,
        //                "[%zu] TestingSplitRunner::process_for() _end_controller.arrive_and_await(),"
        //                " _end_controller.count(): %d, _end_controller.parties(): %d;\n",
        //                std::hash<std::thread::id> {}(std::this_thread::get_id()), _end_controller->count(),
        //                _end_controller->parties());

        // End quanta phase
        _end_controller->arrive_and_await();

        //        fprintf(stderr,
        //                "[%zu] TestingSplitRunner::process_for() _end_controller.arrive_and_await() "
        //                "finished;\n",
        //                std::hash<std::thread::id> {}(std::this_thread::get_id()));

        if (++_completed_phases >= _required_phases) {
            //            fprintf(stderr,
            //                    "[%zu] TestingSplitRunner::process_for() all completed: completed_phases=%d, "
            //                    "required=%d, global_count=%d, global_parties=%d\n",
            //                    std::hash<std::thread::id> {}(std::this_thread::get_id()),
            //                    _completed_phases.load(), _required_phases, _global_controller->count(),
            //                    _global_controller->parties());
            _end_controller->arrive_and_deregister();
            _begin_controller->arrive_and_deregister();
            _global_controller->arrive_and_deregister();
            // 仅当任务完成时设置主promise
            _completion_future.set_value(Void {});
        }

        //        fprintf(stderr,
        //                "[%zu] TestingSplitRunner::process_for() _completed_phases: %d,"
        //                " _required_phases: %d, _global_controller.count(): %d, "
        //                "_global_controller.parties(): %d;\n",
        //                std::hash<std::thread::id> {}(std::this_thread::get_id()), _completed_phases.load(),
        //                _required_phases, _global_controller->count(), _global_controller->parties());

        // 立即完成当前阶段的promise
        //        phase_promise.set_value();

        return SharedListenableFuture<Void>::create_ready();
    }

    int completed_phases() const { return _completed_phases; }

    int first_phase() const { return _first_phase; }

    int last_phase() const { return _last_phase; }

    void close(const Status& status) override {
        // Implementation needed
    }

    std::string get_info() const override {
        // Implementation needed
        return "";
    }

    bool is_finished() override {
        //        return _completion_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        return _completion_future.is_ready();
    }

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
    //    std::promise<void> _completion_promise;      // 用于整体完成通知
    //    std::shared_future<void> _completion_future; // 改为共享future
    ListenableFuture<Void> _completion_future {};
};

class TimeSharingTaskExecutorTest : public testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}

private:
    // 修改为模板函数以支持不同容器类型
    template <typename Container>
    void assert_split_states(int end_index, const Container& splits) {
        // 打印所有split的启动状态
        fprintf(stderr, "[验证] 所有split启动状态总览（共%zu个）:\n", splits.size());
        for (int i = 0; i < splits.size(); ++i) {
            fprintf(stderr, "  → split[%d] 启动状态: %s\n", i,
                    splits[i]->is_started() ? "true" : "false");
        }

        // 验证end_index之前的split都已启动
        fprintf(stderr, "\n[验证] 详细检查0-%d号split应已启动:\n", end_index);
        for (int i = 0; i <= end_index; ++i) {
            bool actual = splits[i]->is_started();
            fprintf(stderr, "  → split[%d] 预期启动=true，实际=%s\n", i, actual ? "true" : "false");
            EXPECT_TRUE(actual) << "Split " << i << " 应该已启动";
        }

        // 验证end_index之后的split未启动
        fprintf(stderr, "\n[验证] 详细检查%d-%zu号split应未启动:\n", end_index + 1,
                splits.size() - 1);
        for (int i = end_index + 1; i < splits.size(); ++i) {
            bool actual = splits[i]->is_started();
            fprintf(stderr, "  → split[%d] 预期启动=false，实际=%s\n", i,
                    actual ? "true" : "false");
            EXPECT_FALSE(actual) << "Split " << i << " 应该未启动";
        }

        fprintf(stderr, "[验证] 完整状态验证完成\n\n");
    }

    std::mt19937_64 _rng {std::random_device {}()};
};

// 辅助函数创建测试任务
//std::shared_ptr<TestingSplitRunner> create_testing_split(
//        std::shared_ptr<TestingTicker> ticker,
//        std::shared_ptr<PhaseController> begin,
//        std::shared_ptr<PhaseController> end,
//        int required_phases) {
//    return std::make_shared<TestingSplitRunner>(
//            "driver", ticker,
//            std::make_shared<PhaseController>(0), // global_controller
//            begin,
//            end,
//            required_phases,
//            0);
//}

// 修改原有测试用例（示例）
TEST_F(TimeSharingTaskExecutorTest, TestTasksComplete) {
    auto ticker = std::make_shared<TestingTicker>();
    //    const auto stuck_threshold = std::chrono::minutes(10);

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

        // 添加两个初始任务
        auto driver1 = std::make_shared<TestingSplitRunner>(
                "driver1", ticker,
                std::make_shared<PhaseController>(0), // global_controller
                begin_phase, verification_complete, 10, 0);
        auto driver2 = std::make_shared<TestingSplitRunner>(
                "driver2", ticker,
                std::make_shared<PhaseController>(0), // global_controller
                begin_phase, verification_complete, 10, 0);
        executor.enqueue_splits(task_handle, true, {driver1, driver2});

        // 验证初始状态
        EXPECT_EQ(driver1->completed_phases(), 0);
        EXPECT_EQ(driver2->completed_phases(), 0);

        // 触发第一阶段
        begin_phase->arrive_and_await();
        ticker->increment(60, std::chrono::seconds(1));

        // 验证停滞任务检测
        //        auto stuck_splits = executor.get_stuck_split_task_ids(stuck_threshold, [](auto&){ return true; });
        //        EXPECT_TRUE(stuck_splits.empty()) << "应无停滞任务";
        //        EXPECT_EQ(executor.get_runaway_split_count(), 0) << "失控任务计数应为0";

        // 触发任务超时
        ticker->increment(600, std::chrono::seconds(1));
        //        stuck_splits = executor.get_stuck_split_task_ids(stuck_threshold, [](auto&){ return true; });
        //        EXPECT_EQ(stuck_splits.size(), 1) << "应检测到1个停滞任务";
        //        EXPECT_EQ(*stuck_splits.begin(), task_id) << "停滞任务ID不匹配";
        //        EXPECT_EQ(executor.get_runaway_split_count(), 2) << "应检测到2个失控任务";
        verification_complete->arrive_and_await();

        // advance one phase and verify
        begin_phase->arrive_and_await();
        EXPECT_EQ(driver1->completed_phases(), 1);
        EXPECT_EQ(driver2->completed_phases(), 1);
        verification_complete->arrive_and_await();

        // add one more job
        auto driver3 = std::make_shared<TestingSplitRunner>(
                "driver3", ticker,
                std::make_shared<PhaseController>(0), // global_controller
                begin_phase, verification_complete, 10, 0);
        executor.enqueue_splits(task_handle, false, {driver3});
        // advance one phase and verify
        begin_phase->arrive_and_await();
        EXPECT_EQ(driver1->completed_phases(), 2);
        EXPECT_EQ(driver2->completed_phases(), 2);
        EXPECT_EQ(driver3->completed_phases(), 0);
        //        future1.get(1, SECONDS);
        //        future2.get(1, SECONDS);
        verification_complete->arrive_and_await();

        // advance to the end of the first two task and verify
        begin_phase->arrive_and_await();
        for (int i = 0; i < 7; i++) {
            verification_complete->arrive_and_await();
            begin_phase->arrive_and_await();
            EXPECT_EQ(begin_phase->generation(), verification_complete->generation() + 1);
        }
        EXPECT_EQ(driver1->completed_phases(), 10);
        EXPECT_EQ(driver2->completed_phases(), 10);
        EXPECT_EQ(driver3->completed_phases(), 8);
        //        future3.get(1, SECONDS);
        verification_complete->arrive_and_await();

        // advance two more times and verify
        begin_phase->arrive_and_await();
        verification_complete->arrive_and_await();
        begin_phase->arrive_and_await();
        EXPECT_EQ(driver1->completed_phases(), 10);
        EXPECT_EQ(driver2->completed_phases(), 10);
        EXPECT_EQ(driver3->completed_phases(), 10);
        //        future3.get(1, SECONDS);
        verification_complete->arrive_and_await();
        //
        EXPECT_EQ(driver1->first_phase(), 0);
        EXPECT_EQ(driver2->first_phase(), 0);
        EXPECT_EQ(driver3->first_phase(), 2);
        EXPECT_EQ(driver1->last_phase(), 10);
        EXPECT_EQ(driver2->last_phase(), 10);
        EXPECT_EQ(driver3->last_phase(), 12);

        // 清理后验证无滞留任务
        ticker->increment(610, std::chrono::seconds(1));
        //        stuck_splits = executor.get_stuck_split_task_ids(stuck_threshold, [](auto&){ return true; });
        //        EXPECT_TRUE(stuck_splits.empty()) << "最终应无停滞任务";
        //        EXPECT_EQ(executor.get_runaway_split_count(), 0) << "最终失控任务计数应为0";
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, TestQuantaFairness) {
    constexpr int TOTAL_PHASES = 11;
    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();

    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = 1;
    thread_config.min_thread_num = 1;
    TimeSharingTaskExecutor executor(thread_config, 2, 3, 4, ticker);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    ticker->increment(20, std::chrono::milliseconds(1));
    // 创建两个独立的任务句柄
    TaskId short_task_id("short_quanta");
    TaskId long_task_id("long_quanta");
    auto short_handle = TEST_TRY(executor.create_task(
            short_task_id, []() { return 0.0; }, // 优先级函数
            10,                                  // 初始并发数
            std::chrono::milliseconds(1), std::nullopt));

    auto long_handle = TEST_TRY(executor.create_task(
            long_task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1), std::nullopt));

    auto end_controller = std::make_shared<PhaseController>(0);

    auto short_split = std::make_shared<TestingSplitRunner>(
            "short", ticker, std::make_shared<PhaseController>(0),
            std::make_shared<PhaseController>(0), end_controller, TOTAL_PHASES, 10);

    auto long_split = std::make_shared<TestingSplitRunner>(
            "long", ticker, std::make_shared<PhaseController>(0),
            std::make_shared<PhaseController>(0), end_controller, TOTAL_PHASES, 20);

    // 添加任务到对应的句柄
    executor.enqueue_splits(short_handle, true, {short_split});
    executor.enqueue_splits(long_handle, true, {long_split});

    // 修改主线程同步逻辑
    for (int i = 0; i < TOTAL_PHASES; ++i) {
        end_controller->arrive_and_await();
    }

    //    fprintf(stderr, "验证阶段数\n");

    // 验证阶段数
    EXPECT_GE(short_split->completed_phases(), 7);
    EXPECT_LE(short_split->completed_phases(), 8);
    EXPECT_GE(long_split->completed_phases(), 3);
    EXPECT_LE(long_split->completed_phases(), 4);
    end_controller->arrive_and_deregister();
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, TestLevelMovement) {
    //    fprintf(stderr, "[%zu] TestLevelMovement\n",
    //            std::hash<std::thread::id> {}(std::this_thread::get_id()));

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = 2;
    thread_config.min_thread_num = 2;
    TimeSharingTaskExecutor executor(thread_config, 2, 3, 4, ticker);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        // 初始化时间
        ticker->increment(20, std::chrono::milliseconds(1));

        // 创建任务句柄
        TaskId task_id("test_task");
        auto task_handle =
                std::static_pointer_cast<TimeSharingTaskHandle>(TEST_TRY(executor.create_task(
                        task_id, []() { return 0.0; }, // 优先级函数
                        10,                            // 初始并发数
                        std::chrono::milliseconds(1), std::nullopt)));

        // 同步控制器
        auto global_controller = std::make_shared<PhaseController>(3); // 2个执行线程 + 主线程
        const int quanta_time_ms = 500;
        const int phases_per_second = 1000 / quanta_time_ms;
        const int total_phases =
                MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS
                        [MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS.size() - 1] *
                phases_per_second; // 假设最后一级阈值

        //        fprintf(stderr, "total_phases: %d\n", total_phases);
        // 创建测试任务
        auto driver1 = std::make_shared<TestingSplitRunner>(
                "driver1", ticker, global_controller, std::make_shared<PhaseController>(0),
                std::make_shared<PhaseController>(0), total_phases, quanta_time_ms);
        auto driver2 = std::make_shared<TestingSplitRunner>(
                "driver2", ticker, global_controller, std::make_shared<PhaseController>(0),
                std::make_shared<PhaseController>(0), total_phases, quanta_time_ms);

        // 添加任务
        executor.enqueue_splits(task_handle, false, {driver1, driver2});

        int completed_phases = 0;
        for (int i = 0; i < MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS.size() - 1; ++i) {
            const int target_seconds = MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i + 1];
            while ((completed_phases / phases_per_second) < target_seconds) {
                //                fprintf(stderr, "completed_phases: %d, phases_per_second: %d, target_seconds: %d\n",
                //                        completed_phases, phases_per_second, target_seconds);
                //                fprintf(stderr,
                //                        "[%zu] TestLevelMovement _global_controller.arrive_and_await()"
                //                        " _global_controller.count(): %d, _global_controller.parties(): %d;\n",
                //                        std::hash<std::thread::id> {}(std::this_thread::get_id()),
                //                        global_controller->count(), global_controller->parties());
                global_controller->arrive_and_await();
                //                fprintf(stderr,
                //                        "[%zu] TestLevelMovement _global_controller.arrive_and_await() finished;\n",
                //                        std::hash<std::thread::id> {}(std::this_thread::get_id()));
                ++completed_phases;
            }

            // 验证优先级级别
            EXPECT_EQ(task_handle->priority().level(), i + 1);
        }

        global_controller->arrive_and_deregister();
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

TEST_F(TimeSharingTaskExecutorTest, TestLevelMultipliers) {
    constexpr int TASK_THREADS = 6;
    constexpr int CONCURRENCY = 3;

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    std::shared_ptr<MultilevelSplitQueue> split_queue =
            std::make_shared<MultilevelSplitQueue>(2); // 初始化多级队列
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = TASK_THREADS;
    thread_config.min_thread_num = TASK_THREADS;
    TimeSharingTaskExecutor executor(thread_config, CONCURRENCY, 3, 4, ticker,
                                     std::chrono::milliseconds(60000), split_queue);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        ticker->increment(20, std::chrono::milliseconds(1));

        for (int i = 0;
             i < (sizeof(MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS) / sizeof(int) - 1); ++i) {
            // 创建三个任务句柄
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

            // 移动任务0到下一级别
            auto task0_job = std::make_shared<TestingSplitRunner>(
                    "task0", ticker, std::make_shared<PhaseController>(0),
                    std::make_shared<PhaseController>(0), std::make_shared<PhaseController>(0), 1,
                    MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i + 1] * 1000);
            executor.enqueue_splits(task_handle1, false, {task0_job});

            // 移动任务1和2到当前级别
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

            // 等待任务完成
            while (!task0_job->is_finished() || !task1_job->is_finished() ||
                   !task2_job->is_finished()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            //            fprintf(stderr, "start new task\n");
            // 启动新任务
            auto global_controller = std::make_shared<PhaseController>(7); // 6个执行线程 + 主线程
            const int phases_for_next_level = MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i + 1] -
                                              MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS[i];
            std::array<std::shared_ptr<TestingSplitRunner>, 6> drivers;

            for (int j = 0; j < 6; ++j) {
                drivers[j] = std::make_shared<TestingSplitRunner>(
                        "driver" + std::to_string(j), ticker, global_controller,
                        std::make_shared<PhaseController>(0), std::make_shared<PhaseController>(0),
                        phases_for_next_level, 1000);
            }

            executor.enqueue_splits(task_handle1, false, {drivers[0], drivers[1]});
            executor.enqueue_splits(task_handle2, false, {drivers[2], drivers[3]});
            executor.enqueue_splits(task_handle3, false, {drivers[4], drivers[5]});

            // 收集初始阶段数
            int lower_level_start = 0, higher_level_start = 0;
            for (int j = 2; j < 6; ++j) lower_level_start += drivers[j]->completed_phases();
            for (int j = 0; j < 2; ++j) higher_level_start += drivers[j]->completed_phases();

            // 运行所有任务
            while (std::any_of(drivers.begin(), drivers.end(),
                               [](auto& d) { return !d->is_finished(); })) {
                global_controller->arrive_and_await();

                int lower_level_end = 0, higher_level_end = 0;
                for (int j = 2; j < 6; ++j) lower_level_end += drivers[j]->completed_phases();
                for (int j = 0; j < 2; ++j) higher_level_end += drivers[j]->completed_phases();

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

TEST_F(TimeSharingTaskExecutorTest, TestTaskHandle) {
    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = 4;
    thread_config.min_thread_num = 4;
    TimeSharingTaskExecutor executor(thread_config, 8, 3, 4, ticker);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        // 创建任务句柄
        TaskId task_id("test_task");
        auto task_handle =
                std::static_pointer_cast<TimeSharingTaskHandle>(TEST_TRY(executor.create_task(
                        task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                        std::nullopt)));

        // 准备同步控制器
        auto begin_phase = std::make_shared<PhaseController>(1);
        auto verification_complete = std::make_shared<PhaseController>(1);

        // 创建测试任务
        auto driver1 = std::make_shared<TestingSplitRunner>(
                "driver1", ticker,
                std::make_shared<PhaseController>(0), // global_controller
                begin_phase, verification_complete,
                10, // required_phases
                0); // quanta_time

        auto driver2 = std::make_shared<TestingSplitRunner>(
                "driver2", ticker,
                std::make_shared<PhaseController>(0), // global_controller
                begin_phase, verification_complete,
                10, // required_phases
                0); // quanta_time

        //        // 强制入队split（不立即执行）
        //        executor.enqueue_splits(task_handle, true, {driver1});
        //        EXPECT_EQ(task_handle->get_running_leaf_splits(), 0)
        //                << "强制入队后应立即运行的任务数应为0";
        //
        //        // 正常入队split
        //        executor.enqueue_splits(task_handle, false, {driver2});
        //        EXPECT_EQ(task_handle->get_running_leaf_splits(), 1)
        //                << "正常入队后应立即运行的任务数应为1";

        //        // 正常入队split
        //        executor.enqueue_splits(task_handle, false, {driver2});
        //        EXPECT_EQ(task_handle->get_running_leaf_splits(), 2)
        //                << "正常入队后应立即运行的任务数应为2";

        executor.enqueue_splits(task_handle, true, {driver1});
        EXPECT_EQ(task_handle->running_leaf_splits(), 0) << "强制入队后应立即运行的任务数应为0";

        // 正常入队split
        executor.enqueue_splits(task_handle, false, {driver2});
        EXPECT_EQ(task_handle->running_leaf_splits(), 1) << "正常入队后应立即运行的任务数应为1";

        // 释放同步锁让任务继续执行
        begin_phase->arrive_and_deregister();
        verification_complete->arrive_and_deregister();

        // 等待任务完成
        while (!driver1->is_finished() || !driver2->is_finished()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    } catch (...) {
        executor.stop();
        throw;
    }
    executor.stop();
}

// 辅助函数实现（与之前测试保持一致）
void wait_until_splits_start(const std::vector<std::shared_ptr<TestingSplitRunner>>& splits) {
    constexpr auto TIMEOUT = std::chrono::seconds(30);
    auto start = std::chrono::steady_clock::now();

    while (std::any_of(splits.begin(), splits.end(),
                       [](const auto& split) { return !split->is_started(); })) {
        if (std::chrono::steady_clock::now() - start > TIMEOUT) {
            throw std::runtime_error("等待split启动超时");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

TEST_F(TimeSharingTaskExecutorTest, TestMinMaxDriversPerTask) {
    constexpr int MAX_DRIVERS_PER_TASK = 2;
    constexpr int TASK_THREADS = 4;
    constexpr int CONCURRENCY = 16;
    constexpr int BATCH_COUNT = 4;
    constexpr int SPLITS_PER_BATCH = 2;

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    std::shared_ptr<MultilevelSplitQueue> split_queue =
            std::make_shared<MultilevelSplitQueue>(2); // 初始化多级队列
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = TASK_THREADS;
    thread_config.min_thread_num = TASK_THREADS;
    TimeSharingTaskExecutor executor(thread_config, CONCURRENCY, 1, MAX_DRIVERS_PER_TASK, ticker,
                                     std::chrono::milliseconds(60000), split_queue);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        TaskId task_id("test_task");
        auto task_handle = TEST_TRY(executor.create_task(
                task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1), std::nullopt));

        std::array<std::shared_ptr<TestingSplitRunner>, BATCH_COUNT * SPLITS_PER_BATCH> splits;
        std::array<std::shared_ptr<PhaseController>, BATCH_COUNT> batch_controllers;

        // 准备4个批次的split，每批2个
        for (int batch = 0; batch < BATCH_COUNT; ++batch) {
            batch_controllers[batch] = std::make_shared<PhaseController>(0);
            batch_controllers[batch]->register_party();
            auto& controller = batch_controllers[batch];

            for (int i = 0; i < SPLITS_PER_BATCH; ++i) {
                splits[batch * SPLITS_PER_BATCH + i] = std::make_shared<TestingSplitRunner>(
                        "batch" + std::to_string(batch) + "_split" + std::to_string(i), ticker,
                        std::make_shared<PhaseController>(0), // global_controller
                        std::make_shared<PhaseController>(0), // begin_controller
                        controller,                           // end_controller
                        1,                                    // required_phases
                        0);                                   // quanta_time
            }

            executor.enqueue_splits(
                    task_handle, false,
                    {splits[batch * SPLITS_PER_BATCH], splits[batch * SPLITS_PER_BATCH + 1]});
        }

        //        fprintf(stderr, "wait and validation.\n");
        // 验证分批执行逻辑
        for (int current_batch = 0; current_batch < BATCH_COUNT; ++current_batch) {
            // 等待当前批次split启动
            //            fprintf(stderr, "wait_until_splits_start batch: %d\n", current_batch);
            wait_until_splits_start({splits[current_batch * SPLITS_PER_BATCH],
                                     splits[current_batch * SPLITS_PER_BATCH + 1]});
            //            fprintf(stderr, "wait_until_splits_start batch finished: %d\n", current_batch);

            // 替换原有的验证逻辑
            assert_split_states(current_batch * SPLITS_PER_BATCH + 1, splits);

            // 完成当前批次
            batch_controllers[current_batch]->arrive_and_deregister();
        }

        // 验证所有split最终完成
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

TEST_F(TimeSharingTaskExecutorTest, TestUserSpecifiedMaxDriversPerTask) {
    constexpr int TASK_THREADS = 4;
    constexpr int CONCURRENCY = 16;
    constexpr int BATCH_COUNT = 4;

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    std::shared_ptr<MultilevelSplitQueue> split_queue = std::make_shared<MultilevelSplitQueue>(2);
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = TASK_THREADS;
    thread_config.min_thread_num = TASK_THREADS;
    TimeSharingTaskExecutor executor(thread_config, CONCURRENCY, 2, 4, ticker,
                                     std::chrono::milliseconds(60000), split_queue);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        // 创建任务并覆盖最大驱动器数为1
        TaskId task_id("user_specified_task");
        auto task_handle = TEST_TRY(executor.create_task(
                task_id, []() { return 0.0; }, 10, std::chrono::milliseconds(1),
                std::optional<int>(1))); // 显式指定最大驱动器数

        std::array<std::shared_ptr<TestingSplitRunner>, BATCH_COUNT> splits;
        std::array<std::shared_ptr<PhaseController>, BATCH_COUNT> batch_controllers;

        // 准备4个批次的split
        for (int batch = 0; batch < BATCH_COUNT; ++batch) {
            batch_controllers[batch] = std::make_shared<PhaseController>(0);
            batch_controllers[batch]->register_party();

            splits[batch] = std::make_shared<TestingSplitRunner>(
                    "batch" + std::to_string(batch), ticker,
                    std::make_shared<PhaseController>(0), // global_controller
                    std::make_shared<PhaseController>(0), // begin_controller
                    batch_controllers[batch],             // end_controller
                    1,                                    // required_phases
                    0);                                   // quanta_time

            executor.enqueue_splits(task_handle, false, {splits[batch]});
        }

        // 验证分批执行逻辑
        for (int current_batch = 0; current_batch < BATCH_COUNT; ++current_batch) {
            // 等待当前批次split启动
            wait_until_splits_start({splits[current_batch]});

            // 验证当前批次状态
            assert_split_states(current_batch, splits);

            // 完成当前批次
            batch_controllers[current_batch]->arrive_and_deregister();
        }

        // 验证所有split最终完成
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

TEST_F(TimeSharingTaskExecutorTest, TestMinDriversPerTaskWhenTargetConcurrencyIncreases) {
    constexpr int TASK_THREADS = 4;
    constexpr int CONCURRENCY = 1;
    constexpr int BATCH_COUNT = 3;

    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
    std::shared_ptr<MultilevelSplitQueue> split_queue = std::make_shared<MultilevelSplitQueue>(2);
    TimeSharingTaskExecutor::ThreadConfig thread_config;
    thread_config.max_thread_num = TASK_THREADS;
    thread_config.min_thread_num = TASK_THREADS;
    TimeSharingTaskExecutor executor(thread_config, CONCURRENCY, 2, 2, ticker,
                                     std::chrono::milliseconds(60000), split_queue);
    ASSERT_TRUE(executor.init().ok());
    ASSERT_TRUE(executor.start().ok());

    try {
        // 创建任务并指定最小并发数
        TaskId task_id("test_query");
        auto task_handle = TEST_TRY(executor.create_task(
                task_id, []() { return 0.0; }, // 确保缓冲区未充分利用
                1,                             // 初始并发数
                std::chrono::milliseconds(1),
                std::optional<int>(2))); // 显式指定最小并发数

        std::array<std::shared_ptr<TestingSplitRunner>, BATCH_COUNT> splits;
        std::array<std::shared_ptr<PhaseController>, BATCH_COUNT> batch_controllers;

        // 创建3个split
        for (int batch = 0; batch < BATCH_COUNT; ++batch) {
            batch_controllers[batch] = std::make_shared<PhaseController>(0);
            batch_controllers[batch]->register_party();

            splits[batch] = std::make_shared<TestingSplitRunner>(
                    "split" + std::to_string(batch), ticker,
                    std::make_shared<PhaseController>(0), // global_controller
                    std::make_shared<PhaseController>(0), // begin_controller
                    batch_controllers[batch],             // end_controller
                    1,                                    // required_phases
                    0);                                   // quanta_time
        }

        // 批量添加所有split
        executor.enqueue_splits(task_handle, false, {splits[0], splits[1], splits[2]});

        // 等待第一个split启动
        wait_until_splits_start({splits[0]});

        // 验证只有第一个split启动
        assert_split_states(0, splits);

        // 完成第一个split（应触发并发数调整）
        batch_controllers[0]->arrive_and_deregister();

        // 等待剩余split启动
        wait_until_splits_start({splits[1], splits[2]});

        // 验证所有split已启动
        assert_split_states(2, splits);

        // 完成剩余split
        for (int batch = 1; batch < BATCH_COUNT; ++batch) {
            batch_controllers[batch]->arrive_and_deregister();
        }

        // 验证所有split完成
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

TEST_F(TimeSharingTaskExecutorTest, TestLeafSplitsSize) {}

//TEST_F(TimeSharingTaskExecutorTest, TaskRotation) {
//    constexpr int TASK_THREADS = 4;
//    constexpr int CONCURRENCY = 1;
//    constexpr int BATCH_COUNT = 3;
//
//    std::shared_ptr<TestingTicker> ticker = std::make_shared<TestingTicker>();
//    std::shared_ptr<MultilevelSplitQueue> split_queue = std::make_shared<MultilevelSplitQueue>(2);
//
//    TimeSharingTaskExecutor executor(TASK_THREADS, CONCURRENCY, 2, 2, ticker,
//                                     std::chrono::milliseconds(60000), split_queue);
//    ASSERT_TRUE(executor.init().ok());
//    ASSERT_TRUE(executor.start().ok());
//    // 创建三个任务
//    auto task1 = std::make_shared<TimeSharingTaskHandle>(
//            "task1", executor->_waiting_splits, []() { return 0.5; }, 1,
//            std::chrono::milliseconds(100), 2);
//    auto task2 = std::make_shared<TimeSharingTaskHandle>(
//            "task2", executor->_waiting_splits, []() { return 0.5; }, 1,
//            std::chrono::milliseconds(100), 2);
//    auto task3 = std::make_shared<TimeSharingTaskHandle>(
//            "task3", executor->_waiting_splits, []() { return 0.5; }, 1,
//            std::chrono::milliseconds(100), 2);
//
//    // 添加任务到执行器
//    executor->add_task("task1", task1);
//    executor->add_task("task2", task2);
//    executor->add_task("task3", task3);
//
//    // 添加分片到任务
//    auto split1 = std::make_shared<MockSplitRunner>();
//    auto split2 = std::make_shared<MockSplitRunner>();
//    auto split3 = std::make_shared<MockSplitRunner>();
//    task1->enqueue_split(split1);
//    task2->enqueue_split(split2);
//    task3->enqueue_split(split3);
//
//    // 测试任务轮转
//    {
//        std::lock_guard<std::mutex> guard(executor->_mutex);
//
//        // 第一次轮询，应该返回 task1 的分片
//        auto result1 = executor->_poll_next_split_worker(guard);
//        EXPECT_EQ(result1, split1);
//
//        // 第二次轮询，应该返回 task2 的分片
//        auto result2 = executor->_poll_next_split_worker(guard);
//        EXPECT_EQ(result2, split2);
//
//        // 第三次轮询，应该返回 task3 的分片
//        auto result3 = executor->_poll_next_split_worker(guard);
//        EXPECT_EQ(result3, split3);
//
//        // 第四次轮询，应该再次返回 task1 的分片（轮转）
//        auto result4 = executor->_poll_next_split_worker(guard);
//        EXPECT_EQ(result4, split1);
//    }
//
//    // 测试任务轮转时跳过没有分片的任务
//    {
//        std::lock_guard<std::mutex> guard(executor->_mutex);
//
//        // 清空 task2 的分片
//        task2->set_running_leaf_splits(2);
//
//        // 第一次轮询，应该返回 task3 的分片（跳过 task2）
//        auto result1 = executor->_poll_next_split_worker(guard);
//        EXPECT_EQ(result1, split3);
//
//        // 第二次轮询，应该返回 task1 的分片（跳过 task2）
//        auto result2 = executor->_poll_next_split_worker(guard);
//        EXPECT_EQ(result2, split1);
//    }
//}

class PhaseControllerTest : public testing::Test {
protected:
    const int THREAD_COUNT = 4;
    std::shared_ptr<PhaseController> controller;

    void SetUp() override { controller = std::make_shared<PhaseController>(THREAD_COUNT); }
};

// 基础同步测试
TEST_F(PhaseControllerTest, BasicSynchronization) {
    std::atomic<int> ready {0};
    std::vector<std::thread> threads;
    const int GENERATIONS = 3;

    for (int g = 0; g < GENERATIONS; ++g) {
        for (int i = 0; i < THREAD_COUNT; ++i) {
            threads.emplace_back([&, g] {
                // 同步线程启动
                if (ready.fetch_add(1) == THREAD_COUNT - 1) {
                    ready.store(0);
                } else {
                    while (ready.load() != 0)
                        ;
                }

                int gen = controller->arrive_and_await();
                EXPECT_EQ(gen, g + 1) << "代际计数错误";
            });
        }

        for (auto& t : threads) t.join();
        threads.clear();

        EXPECT_EQ(controller->generation(), g + 1);
        EXPECT_EQ(controller->count(), THREAD_COUNT) << "每代应重置计数器";
    }
}

// 动态参与者管理测试
TEST_F(PhaseControllerTest, DynamicRegistration) {
    constexpr int NEW_PARTIES = 2;
    std::atomic<int> completed {0};
    std::vector<std::thread> threads;
    std::mutex cv_mtx;
    std::condition_variable cv;

    // 修改初始参与方数（包含主线程）
    controller = std::make_shared<PhaseController>(THREAD_COUNT + 1);

    // 初始参与者 (4 threads + 主线程)
    for (int i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back([&] {
            // 第一阶段同步
            controller->arrive_and_await();
            //            fprintf(stderr, "[初始线程 %d] 第一阶段完成\n", idx);

            // 第二阶段同步
            controller->arrive_and_await();
            //            fprintf(stderr, "[初始线程 %d] 第二阶段完成\n", idx);

            completed++;
        });
    }

    // 主线程第一阶段同步
    //    fprintf(stderr, "主线程开始同步第一阶段\n");
    int main_gen = controller->arrive_and_await();
    fprintf(stderr, "第一阶段完成，当前代际=%d\n", main_gen);

    // 新注册参与者 (2 threads)
    std::vector<std::thread> new_threads;
    for (int i = 0; i < NEW_PARTIES; ++i) {
        new_threads.emplace_back([&] {
            {
                std::lock_guard<std::mutex> lk(cv_mtx);
                controller->register_party();
                //                fprintf(stderr, "[新线程 %d] 注册完成，当前参与方=%d\n", my_id,
                //                        controller->parties());
            }
            cv.notify_one();

            // 只参与第二阶段同步
            controller->arrive_and_await();
            completed++;
            //            fprintf(stderr, "[新线程 %d] 完成\n", my_id);
        });
    }

    // 等待新线程完成注册
    {
        std::unique_lock<std::mutex> lk(cv_mtx);
        cv.wait(lk, [&] { return controller->parties() == THREAD_COUNT + 1 + NEW_PARTIES; });
    }

    // 主线程触发第二阶段同步
    //    fprintf(stderr, "主线程开始同步第二阶段\n");
    main_gen = controller->arrive_and_await();
    //    fprintf(stderr, "第二阶段完成，当前代际=%d\n", main_gen);

    // 等待所有线程完成
    for (auto& t : threads) t.join();
    for (auto& t : new_threads) t.join();

    // 验证结果
    //    fprintf(stderr, "总完成数=%d (预期=%d)\n", completed.load(), THREAD_COUNT + NEW_PARTIES);
    //    fprintf(stderr, "当前参与方=%d (预期=%d)\n", controller->parties(),
    //            THREAD_COUNT + 1 + NEW_PARTIES);

    EXPECT_EQ(completed.load(), THREAD_COUNT + NEW_PARTIES);
    EXPECT_EQ(controller->parties(), THREAD_COUNT + 1 + NEW_PARTIES);
}

// // 注销参与者测试
// TEST_F(PhaseControllerTest, DeregistrationTest) {
//     std::vector<std::thread> threads;

//     // 第一个线程注销
//     threads.emplace_back([&] {
//         controller->arrive_and_deregister();
//         EXPECT_EQ(controller->parties(), THREAD_COUNT - 1);
//     });

//     // 其他线程正常完成
//     for (int i = 1; i < THREAD_COUNT; ++i) {
//         threads.emplace_back([&] {
//             controller->arrive_and_await();
//         });
//     }

//     for (auto& t : threads) t.join();

//     EXPECT_EQ(controller->parties(), THREAD_COUNT - 1);
//     EXPECT_EQ(controller->count(), THREAD_COUNT - 1) << "注销后计数器应更新";
// }

// // 混合操作压力测试
// TEST_F(PhaseControllerTest, ConcurrencyStressTest) {
//     const int TOTAL_OPERATIONS = 1000;
//     std::vector<std::thread> threads;
//     std::atomic<int> ops {0};

//     auto worker = [&] {
//         std::random_device rd;
//         std::mt19937 gen(rd());
//         std::uniform_int_distribution<> dist(0, 2);

//         while (ops.fetch_add(1) < TOTAL_OPERATIONS) {
//             switch (dist(gen)) {
//                 case 0:
//                     controller->arrive_and_await();
//                     break;
//                 case 1:
//                     controller->register_party();
//                     break;
//                 case 2:
//                     if (controller->parties() > 0) {
//                         controller->arrive_and_deregister();
//                     }
//                     break;
//             }
//         }
//     };

//     // 启动工作线程
//     for (int i = 0; i < 4; ++i) {
//         threads.emplace_back(worker);
//     }

//     // 验证最终状态一致性
//     for (auto& t : threads) t.join();

//     EXPECT_GE(controller->parties(), 0) << "参与者数不应为负";
//     EXPECT_GE(controller->count(), 0) << "计数器不应为负";
//     EXPECT_GE(controller->generation(), 0) << "代际计数不应回退";
// }

// // 边界条件测试
// TEST_F(PhaseControllerTest, EdgeCases) {
//     // 测试空控制器
//     auto empty_controller = std::make_shared<PhaseController>(0);
//     EXPECT_EQ(empty_controller->arrive_and_await(), 0);

//     // 测试单参与者
//     auto single_controller = std::make_shared<PhaseController>(1);
//     std::thread t([&] {
//         EXPECT_EQ(single_controller->arrive_and_await(), 1);
//     });
//     t.join();
//     EXPECT_EQ(single_controller->generation(), 1);

//     // 测试多次注销
//     auto controller = std::make_shared<PhaseController>(2);
//     controller->arrive_and_deregister();
//     controller->arrive_and_deregister();
//     EXPECT_EQ(controller->parties(), 0);
// }

} // namespace vectorized
} // namespace doris
