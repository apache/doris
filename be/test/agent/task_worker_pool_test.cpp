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

#include "agent/task_worker_pool.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "olap/storage_engine.h"

namespace doris {

using namespace std::chrono_literals;

TEST(TaskWorkerPoolTest, TaskWorkerPool) {
    std::atomic_int count {0};
    TaskWorkerPool workers("test", 2, [&](auto&& task) {
        std::this_thread::sleep_for(1s);
        ++count;
    });

    TAgentTaskRequest task;
    task.__set_signature(-1);
    workers.submit_task(task);
    workers.submit_task(task);
    workers.submit_task(task); // Pending and ignored when stop

    std::this_thread::sleep_for(200ms);
    workers.stop();

    workers.submit_task(task); // Ignore

    EXPECT_EQ(count.load(), 2);
}

TEST(TaskWorkerPoolTest, PriorTaskWorkerPool) {
    std::atomic_int normal_count {0};
    std::atomic_int high_prior_count {0};
    PriorTaskWorkerPool workers("test", 1, 1, [&](auto&& task) {
        if (task.priority == TPriority::NORMAL) {
            std::this_thread::sleep_for(1s);
            ++normal_count;
        } else {
            std::this_thread::sleep_for(200ms);
            ++high_prior_count;
        }
    });

    TAgentTaskRequest task;
    task.__set_signature(-1);
    task.__set_priority(TPriority::NORMAL);
    workers.submit_task(task);
    workers.submit_task(task);
    std::this_thread::sleep_for(200ms);

    task.__set_priority(TPriority::HIGH);
    // Normal pool is busy, but high prior pool should be idle
    workers.submit_task(task);
    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(normal_count.load(), 0);
    EXPECT_EQ(high_prior_count.load(), 1);

    std::this_thread::sleep_for(2s);
    EXPECT_EQ(normal_count.load(), 2);
    EXPECT_EQ(high_prior_count.load(), 1);
    // Both normal and high prior pool are idle
    workers.submit_task(task);
    workers.submit_task(task);

    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(normal_count.load(), 2);
    EXPECT_EQ(high_prior_count.load(), 3);

    workers.submit_task(task);
    workers.submit_task(task);
    workers.submit_task(task); // Pending and ignored when stop
    std::this_thread::sleep_for(100ms);
    workers.stop();

    EXPECT_EQ(normal_count.load(), 2);
    EXPECT_EQ(high_prior_count.load(), 5);

    workers.submit_task(task); // Ignore

    EXPECT_EQ(normal_count.load(), 2);
    EXPECT_EQ(high_prior_count.load(), 5);
}

TEST(TaskWorkerPoolTest, ReportWorkerPool) {
    StorageEngine engine({});
    ExecEnv::GetInstance()->set_storage_engine(&engine);
    Defer defer {[] { ExecEnv::GetInstance()->set_storage_engine(nullptr); }};

    TMasterInfo master_info;
    std::atomic_int count {0};
    ReportWorker worker("test", master_info, 1, [&] { ++count; });

    worker.notify(); // Not received heartbeat yet, igonre
    std::this_thread::sleep_for(100ms);

    master_info.network_address.__set_port(9030);
    worker.notify();
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(count.load(), 1);

    std::this_thread::sleep_for(1s);
    EXPECT_EQ(count.load(), 2);

    engine.notify_listener("test");
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(count.load(), 3);

    worker.stop();
    worker.notify(); // Ignore
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(count.load(), 3);
}

} // namespace doris
