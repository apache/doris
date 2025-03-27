//// Licensed to the Apache Software Foundation (ASF) under one
//// or more contributor license agreements.  See the NOTICE file
//// distributed with this work for additional information
//// regarding copyright ownership.  The ASF licenses this file
//// to you under the Apache License, Version 2.0 (the
//// "License"); you may not use this file except in compliance
//// with the License.  You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing,
//// software distributed under the License is distributed on an
//// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//// KIND, either express or implied.  See the License for the
//// specific language governing permissions and limitations
//// under the License.
//
//#include <gtest/gtest.h>
//#include <atomic>
//#include <thread>
//#include "vec/exec/scan/MultilevelSplitQueue.h"
//
//namespace doris::vectorized {
//
//// 测试用SplitRunner实现
//class TestSplitRunner : public PrioritizedSplitRunner {
//public:
//    TestSplitRunner(Priority priority)
//        : PrioritizedSplitRunner(_global_cpu_time_micros, _global_scheduled_time_micros, _blocked_quanta_wall_time, _unblocked_quanta_wall_time), _priority(priority) {}
//
//    Priority getPriority() const override { return _priority; }
//    void updatePriority(Priority new_pri) { _priority = new_pri; }
//
//private:
//    Priority _priority;
//    CounterStats _global_cpu_time_micros;
//    CounterStats _global_scheduled_time_micros;
//
//    TimeStats _blocked_quanta_wall_time;
//    TimeStats _unblocked_quanta_wall_time;
//};
//
//class MultilevelSplitQueueTest : public testing::Test {
//protected:
//    void SetUp() override {
//        queue = std::make_unique<MultilevelSplitQueue>(2.0);
//    }
//
//    std::shared_ptr<TestSplitRunner> make_split(int level, int64_t pri) {
//        return std::make_shared<TestSplitRunner>(Priority(level, pri));
//    }
//
//    std::unique_ptr<MultilevelSplitQueue> queue;
//};
//
//TEST_F(MultilevelSplitQueueTest, BasicFunction) {
//    // 验证空队列
//    EXPECT_EQ(queue->size(), 0);
//
//    // 添加任务
//    auto split = make_split(0, 100);
//    queue->addSplit(split);
//    EXPECT_EQ(queue->size(), 1);
//
//    // 取出任务
//    auto task = queue->pollSplit();
//    ASSERT_NE(task, nullptr);
//    EXPECT_EQ(task->getPriority().getLevel(), 0);
//    EXPECT_EQ(queue->size(), 0);
//}
//
//TEST_F(MultilevelSplitQueueTest, LevelContributionCap) {
//    // 创建两个测试任务
//    auto split0 = make_split(0, 0);
//    auto split1 = make_split(0, 0);
//
//    // 获取级别阈值数组（假设实现中有这个静态成员）
//    const auto& levelThresholds = MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS;
//
//    int64_t scheduledNanos = 0;
//    for (size_t i = 0; i < levelThresholds.size() - 1; ++i) {
//        // 计算需要增加的时间量（秒转纳秒）
//        const int64_t levelAdvance =
//                (levelThresholds[i + 1] - levelThresholds[i]) * 1'000'000'000LL;
//        // 模拟任务累积时间
//        scheduledNanos += levelAdvance;
//        split0->updatePriority(queue->updatePriority(
//                split0->getPriority(), levelAdvance, scheduledNanos));
//        split1->updatePriority(queue->updatePriority(
//                split1->getPriority(), levelAdvance, scheduledNanos));
//
//        // 验证级别提升
//        EXPECT_EQ(split0->getPriority().getLevel(), i + 1);
//        EXPECT_EQ(split1->getPriority().getLevel(), i + 1);
//
//        // 验证级别累计时间（应用贡献上限）
//        const int64_t expected =
//                2 * std::min(levelAdvance, MultilevelSplitQueue::LEVEL_CONTRIBUTION_CAP);
//        EXPECT_EQ(queue->get_level_scheduled_time(i), expected);
//        EXPECT_EQ(queue->get_level_scheduled_time(i + 1), 0);
//    }
//}
//
//TEST_F(MultilevelSplitQueueTest, LevelContributionWithCap) {
//    // 创建队列（levelTimeMultiplier=2）
//    MultilevelSplitQueue queue(2.0);
//
//    // 创建测试任务并初始化优先级
//    auto split = make_split(0, 0);
//    queue.addSplit(split);
//
//    // 模拟运行10分钟（转换为纳秒）
//    constexpr int64_t quantaNanos = 10LL * 60 * 1'000'000'000; // 10分钟
//    int64_t scheduledNanos = 0;
//
//    // 更新优先级触发时间分配
//    auto task = std::static_pointer_cast<TestSplitRunner>(queue.pollSplit());
//    scheduledNanos += quantaNanos;
//    Priority new_pri = queue.updatePriority(
//            task->getPriority(),
//            quantaNanos,
//            scheduledNanos
//    );
//    task->updatePriority(new_pri);
//
//    // 计算受限制的时间贡献
//    int64_t cappedNanos = std::min(quantaNanos, MultilevelSplitQueue::LEVEL_CONTRIBUTION_CAP);
//    const auto& levels = MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS;
//
//    // 验证每个级别的时间分配
//    for (size_t i = 0; i < levels.size() - 1; ++i) {
//        // 计算当前级别时间阈值（转换为纳秒）
//        int64_t levelThreshold = static_cast<int64_t>((levels[i + 1] - levels[i])) * 1'000'000'000L;
//        int64_t expected = std::min(levelThreshold, cappedNanos);
//
//        // 验证当前级别累计时间
//        EXPECT_EQ(queue.get_level_scheduled_time(i), expected);
//        cappedNanos -= expected;
//    }
//
//    // 验证剩余时间分配到最后一个级别
////    EXPECT_EQ(queue.get_level_scheduled_time(levels.size() - 1), cappedNanos);
//}
//
//TEST_F(MultilevelSplitQueueTest, TimeCompensation) {
//    // 初始状态验证
//    EXPECT_EQ(queue->get_level_scheduled_time(2), 0);
//
//    int64_t scheduledNanos = 0L;
//
//    // 1. 创建并添加任务
//    auto split = make_split(2, 200);
//    queue->addSplit(split);
//
//    // 2. 取出任务（触发时间补偿）
//    auto task = std::static_pointer_cast<TestSplitRunner>(queue->pollSplit());
//    ASSERT_NE(task, nullptr);
//
//    // 3. 模拟任务运行5秒并更新优先级
//    const int64_t RUN_TIME = 5'000'000'000; // 5秒
//    scheduledNanos += RUN_TIME;
//    Priority new_pri = queue->updatePriority(
//        task->getPriority(),
//        RUN_TIME,
//        scheduledNanos
//    );
//    task->updatePriority(new_pri);
//
//    // 4. 重新加入队列（此时level2已有时间记录）
//    queue->addSplit(task);
//
//    // 5. 再次添加新任务触发补偿
//    auto new_split = make_split(2, 300);
//    queue->addSplit(new_split);
//
//    // 验证补偿逻辑
//    int64_t level2_time = queue->get_level_scheduled_time(2);
//    int64_t level0_time = queue->get_level0_target_time();
//
//    // 预期补偿 = (level0_time / 4) + 5秒
//    int64_t expected = (level0_time / 4) + RUN_TIME;
//
//    // 允许10%误差
//    EXPECT_GT(level2_time, expected - expected/10);
//    EXPECT_LT(level2_time, expected + expected/10);
//}
//
//TEST_F(MultilevelSplitQueueTest, PriorityUpdate) {
//    auto split = make_split(0, 0);
//    queue->addSplit(split);
//
//    // 模拟运行30秒
//    auto split_ptr = std::static_pointer_cast<TestSplitRunner>(queue->pollSplit());
//    ASSERT_NE(split_ptr, nullptr);
//    Priority new_pri = queue->updatePriority(
//        split_ptr->getPriority(),
//        30'000'000'000LL,  // 30秒
//        0
//    );
//
//    // 验证升级到level3
//    EXPECT_EQ(new_pri.getLevel(), 3);
//    EXPECT_GT(new_pri.getLevelPriority(), 30'000'000'000LL);
//}
//
//TEST_F(MultilevelSplitQueueTest, LevelSelection) {
//    // 准备不同级别的任务
//    queue->addSplit(make_split(0, 100)); // level0
//    queue->addSplit(make_split(3, 50));  // level3
//
//    // 多次轮询触发时间补偿
//    for (int i = 0; i < 3; ++i) {
//        auto task = queue->pollSplit();
//        queue->addSplit(task);
//    }
//
//    // 应优先选择level3
//    auto task = queue->pollSplit();
//    EXPECT_EQ(task->getPriority().getLevel(), 3);
//}
//
//TEST_F(MultilevelSplitQueueTest, Concurrency) {
//    constexpr int THREAD_NUM = 4;
//    constexpr int OPS = 1000;
//    std::vector<std::thread> workers;
//
//    for (int i = 0; i < THREAD_NUM; ++i) {
//        workers.emplace_back([&] {
//            for (int j = 0; j < OPS; ++j) {
//                auto split = make_split(j % 5, j);
//                queue->addSplit(split);
//                if (auto task = queue->pollSplit()) {
//                    // 模拟处理任务
//                    std::this_thread::sleep_for(std::chrono::microseconds(10));
//                }
//            }
//        });
//    }
//
//    for (auto& t : workers) t.join();
//    SUCCEED(); // 验证无崩溃
//}
//
//TEST_F(MultilevelSplitQueueTest, Interrupt) {
//    std::atomic<bool> stopped{false};
//    std::thread consumer([&] {
//        while (!stopped) {
//            if (auto task = queue->pollSplit()) {
//                // process task
//            }
//        }
//    });
//
//    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//    queue->interrupt();
//    stopped = true;
//    consumer.join();
//}
//
//} // namespace doris::vectorized