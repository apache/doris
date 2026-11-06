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
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "agent/agent_server.h"
#include "cloud/cloud_storage_engine.h"
#include "common/config.h"
#include "cpp/sync_point.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"
#include "storage/options.h"
#include "storage/storage_engine.h"

namespace doris {

using namespace std::chrono_literals;

class CleanUDFCacheWorkerTest : public ::testing::Test {
protected:
    UserFunctionCache cache;
    UserFunctionCache* original_cache = nullptr;
    bool original_java_support = false;
    std::vector<TFinishTaskRequest> acknowledgements;

    void SetUp() override {
        original_cache = ExecEnv::GetInstance()->_user_function_cache;
        ExecEnv::GetInstance()->_user_function_cache = &cache;
        original_java_support = config::enable_java_support;
        config::enable_java_support = false;
        auto* sync_point = SyncPoint::get_instance();
        sync_point->set_call_back("TaskWorkerPool::finish_task", [this](auto&& args) {
            acknowledgements.push_back(*try_any_cast<const TFinishTaskRequest*>(args[0]));
            *try_any_cast<bool*>(args.back()) = true;
        });
        sync_point->enable_processing();
    }

    void TearDown() override {
        auto* sync_point = SyncPoint::get_instance();
        sync_point->disable_processing();
        sync_point->clear_call_back("TaskWorkerPool::finish_task");
        config::enable_java_support = original_java_support;
        ExecEnv::GetInstance()->_user_function_cache = original_cache;
    }

    TAgentTaskRequest request(int64_t signature, int64_t function_id = 938812) {
        TAgentTaskRequest req;
        req.__set_task_type(TTaskType::CLEAN_UDF_CACHE);
        req.__set_signature(signature);
        TCleanUDFCacheReq clean;
        clean.__set_function_id(function_id);
        clean.__set_function_signature("sum(INT)");
        req.__set_clean_udf_cache_req(clean);
        return req;
    }
};

TEST_F(CleanUDFCacheWorkerTest, OneShotCleanupDoesNotAcknowledge) {
    clean_udf_cache_callback(request(-1));
    EXPECT_TRUE(acknowledgements.empty());
}

TEST_F(CleanUDFCacheWorkerTest, TrackedCleanupCanRunAgainAfterLostAcknowledgement) {
    std::mutex mutex;
    std::condition_variable cv;
    int completed = 0;
    TaskWorkerPool workers("test_clean_udf", 1, [&](const auto& req) {
        clean_udf_cache_callback(req);
        {
            std::scoped_lock lock(mutex);
            ++completed;
        }
        cv.notify_all();
    });
    auto req = request(938812);
    ASSERT_TRUE(workers.submit_task(req).ok());
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, 5s, [&] { return completed == 1; }));
    }
    // A lost execution result must not prevent the same task from running again.
    ASSERT_TRUE(workers.submit_task(req).ok());
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, 5s, [&] { return completed == 2; }));
    }
    ASSERT_EQ(2, acknowledgements.size());
    for (const auto& ack : acknowledgements) {
        EXPECT_EQ(TTaskType::CLEAN_UDF_CACHE, ack.task_type);
        EXPECT_EQ(938812, ack.signature);
        EXPECT_EQ(TStatusCode::OK, ack.task_status.status_code);
    }
}

TEST_F(CleanUDFCacheWorkerTest, InvalidTrackedCleanupReportsFailure) {
    clean_udf_cache_callback(request(938813, 0));
    ASSERT_EQ(1, acknowledgements.size());
    EXPECT_EQ(938813, acknowledgements.front().signature);
    EXPECT_EQ(TStatusCode::INVALID_ARGUMENT, acknowledgements.front().task_status.status_code);
}

TEST(TaskWorkerPoolTest, TaskWorkerPool) {
    std::atomic_int count {0};
    TaskWorkerPool workers("test", 2, [&](auto&& task) {
        std::this_thread::sleep_for(1s);
        ++count;
    });

    TAgentTaskRequest task;
    task.__set_signature(-1);
    auto _ = workers.submit_task(task);
    _ = workers.submit_task(task);
    _ = workers.submit_task(task); // Pending and ignored when stop

    std::this_thread::sleep_for(200ms);
    workers.stop();

    _ = workers.submit_task(task); // Ignore

    EXPECT_EQ(count.load(), 2);
}

TEST(TaskWorkerPoolTest, PreSubmitCallback) {
    std::atomic_int callback_count {0};
    std::atomic_int pre_submit_count {0};
    TaskWorkerPool workers(
            "test", 1,
            [&](auto&& task) {
                std::this_thread::sleep_for(200ms);
                ++callback_count;
            },
            [&](auto&& task) { ++pre_submit_count; });

    TAgentTaskRequest task;
    task.__set_signature(-1);
    auto _ = workers.submit_task(task);
    _ = workers.submit_task(task);

    // pre_submit_callback is called synchronously before enqueue
    EXPECT_EQ(pre_submit_count.load(), 2);

    std::this_thread::sleep_for(600ms);
    workers.stop();
    EXPECT_EQ(callback_count.load(), 2);
    EXPECT_EQ(pre_submit_count.load(), 2);
}

TEST(TaskWorkerPoolTest, PreSubmitCallbackWithDedup) {
    std::atomic_int pre_submit_count {0};
    std::atomic_int callback_count {0};
    TaskWorkerPool workers(
            "test", 1,
            [&](auto&& task) {
                std::this_thread::sleep_for(500ms);
                ++callback_count;
            },
            [&](auto&& task) { ++pre_submit_count; });

    TAgentTaskRequest task;
    task.__set_task_type(TTaskType::ALTER);
    task.__set_signature(12345);
    auto _ = workers.submit_task(task);
    _ = workers.submit_task(task); // Should be deduped by register_task_info

    EXPECT_EQ(pre_submit_count.load(), 1); // Only called once, second was deduped

    std::this_thread::sleep_for(600ms);
    workers.stop();
    EXPECT_EQ(callback_count.load(), 1);
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
    auto _ = workers.submit_task(task);
    _ = workers.submit_task(task);
    std::this_thread::sleep_for(200ms);

    task.__set_priority(TPriority::HIGH);
    // Normal pool is busy, but high prior pool should be idle
    _ = workers.submit_task(task);
    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(normal_count.load(), 0);
    EXPECT_EQ(high_prior_count.load(), 1);

    std::this_thread::sleep_for(2s);
    EXPECT_EQ(normal_count.load(), 2);
    EXPECT_EQ(high_prior_count.load(), 1);
    // Both normal and high prior pool are idle
    _ = workers.submit_task(task);
    _ = workers.submit_task(task);

    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(normal_count.load(), 2);
    EXPECT_EQ(high_prior_count.load(), 3);

    workers.stop();

    EXPECT_EQ(normal_count.load(), 2);
    EXPECT_EQ(high_prior_count.load(), 3);

    _ = workers.submit_task(task); // Ignore

    EXPECT_EQ(normal_count.load(), 2);
    EXPECT_EQ(high_prior_count.load(), 3);
}

TEST(TaskWorkerPoolTest, ReportWorkerPool) {
    ExecEnv::GetInstance()->set_storage_engine(std::make_unique<StorageEngine>(EngineOptions {}));
    Defer defer {[] { ExecEnv::GetInstance()->set_storage_engine(nullptr); }};

    ClusterInfo cluster_info;
    std::atomic_int count {0};
    ReportWorker worker("test", &cluster_info, 1, [&] { ++count; });

    worker.notify(); // Not received heartbeat yet, ignore
    std::this_thread::sleep_for(100ms);

    cluster_info.master_fe_addr.__set_port(9030);
    worker.notify();
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(count.load(), 1);

    std::this_thread::sleep_for(1s);
    EXPECT_EQ(count.load(), 2);

    ExecEnv::GetInstance()->storage_engine().notify_listener("test");
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(count.load(), 3);

    worker.stop();
    worker.notify(); // Ignore
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(count.load(), 3);
}

TEST(AgentServerTest, CloudRegistersCleanUdfCacheWorker) {
    auto* exec_env = ExecEnv::GetInstance();
    auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
    auto* cloud_engine = engine.get();
    exec_env->set_storage_engine(std::move(engine));
    Defer defer {[exec_env] { exec_env->set_storage_engine(nullptr); }};

    ClusterInfo cluster_info;
    AgentServer agent_server(exec_env, &cluster_info);

    agent_server.cloud_start_workers(*cloud_engine, exec_env);

    EXPECT_TRUE(agent_server._workers.contains(TTaskType::CLEAN_UDF_CACHE));
}

} // namespace doris
