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
#include <thread>

#include "agent/agent_server.h"
#include "cloud/cloud_storage_engine.h"
#include "common/config.h"
#include "cpp/sync_point.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "udf/python/python_server.h"
#include "udf/python/python_udf_meta.h"

namespace doris {

using namespace std::chrono_literals;

class CleanUdfCacheCallbackTest : public testing::Test {
protected:
    void SetUp() override {
        _original_function_cache = ExecEnv::GetInstance()->_user_function_cache;
        ExecEnv::GetInstance()->_user_function_cache = &_function_cache;
        _original_java_support = config::enable_java_support;
        config::enable_java_support = false;
        auto* sync_point = SyncPoint::get_instance();
        sync_point->set_call_back(
                "finish_task:request",
                [this](auto&& args) {
                    const auto* finished = try_any_cast<const TFinishTaskRequest*>(args.at(0));
                    _finished.push_back(*finished);
                    _pool_existed_at_finish.push_back(has_pool(finished->signature));
                    *try_any_cast<bool*>(args.back()) = true;
                },
                &_finish_guard);
        sync_point->enable_processing();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->_user_function_cache = _original_function_cache;
        config::enable_java_support = _original_java_support;
        SyncPoint::get_instance()->disable_processing();
    }

    TAgentTaskRequest request(int64_t function_id, bool need_ack = true) {
        TCleanUDFCacheReq clean;
        clean.__set_function_id(function_id);
        clean.__set_function_signature("test_fn(INT)");
        if (need_ack) {
            clean.__set_need_ack(true);
        }
        TAgentTaskRequest req;
        req.__set_task_type(TTaskType::CLEAN_UDF_CACHE);
        // Old BEs never remove CLEAN_UDF_CACHE task signatures. Keep the wire signature untracked.
        req.__set_signature(-1);
        req.__set_clean_udf_cache_req(clean);
        return req;
    }

    void seed_pool(int64_t function_id) {
        PythonUDFMeta meta;
        meta.id = function_id;
        PythonServerManager::instance().set_process_pool_for_test(
                meta, PythonVersion("3.10.2", "", ""), {});
    }

    bool has_pool(int64_t function_id) {
        auto& manager = PythonServerManager::instance();
        std::scoped_lock lock(manager._pools_mutex);
        return manager._process_pools.contains(function_id);
    }

    bool _original_java_support;
    UserFunctionCache _function_cache;
    UserFunctionCache* _original_function_cache;
    SyncPoint::CallbackGuard _finish_guard;
    std::vector<TFinishTaskRequest> _finished;
    std::vector<bool> _pool_existed_at_finish;
};

TEST_F(CleanUdfCacheCallbackTest, AcknowledgesFunctionIdAfterPythonPoolIsDropped) {
    constexpr int64_t function_id = 94000001;
    seed_pool(function_id);
    ASSERT_TRUE(has_pool(function_id));
    auto req = request(function_id);
    clean_udf_cache_callback(req);
    EXPECT_FALSE(has_pool(function_id));
    ASSERT_EQ(1, _finished.size());
    EXPECT_FALSE(_pool_existed_at_finish.front());
    EXPECT_EQ(TTaskType::CLEAN_UDF_CACHE, _finished.front().task_type);
    EXPECT_EQ(function_id, _finished.front().signature);
    EXPECT_EQ(TStatusCode::OK, _finished.front().task_status.status_code);
}

TEST_F(CleanUdfCacheCallbackTest, RepeatedCleanupAcknowledgesTheSameFunctionId) {
    constexpr int64_t function_id = 94000002;
    auto req = request(function_id);
    clean_udf_cache_callback(req);
    clean_udf_cache_callback(req);
    ASSERT_EQ(2, _finished.size());
    for (const auto& finished : _finished) {
        EXPECT_EQ(function_id, finished.signature);
        EXPECT_EQ(TStatusCode::OK, finished.task_status.status_code);
    }
}

TEST_F(CleanUdfCacheCallbackTest, LegacyRequestsDropPythonPoolsWithoutAcknowledgement) {
    constexpr int64_t function_id = 94000003;
    for (bool explicit_false : {false, true}) {
        const int64_t current_id = function_id + explicit_false;
        seed_pool(current_id);
        auto req = request(current_id, false);
        if (explicit_false) {
            req.clean_udf_cache_req.__set_need_ack(false);
        }
        clean_udf_cache_callback(req);
        EXPECT_FALSE(has_pool(current_id));
    }
    EXPECT_TRUE(_finished.empty());
}

TEST_F(CleanUdfCacheCallbackTest, JavaFailureStillDropsPythonPoolAndAcknowledgesFailure) {
    constexpr int64_t function_id = 94000005;
    seed_pool(function_id);
    config::enable_java_support = true;
    SyncPoint::CallbackGuard java_guard;
    SyncPoint::get_instance()->set_call_back(
            "clean_udf_cache_callback:java_status",
            [](auto&& args) {
                auto* result = try_any_cast<std::pair<Status, bool>*>(args.back());
                result->first = Status::InternalError("test Java cleanup failure");
                result->second = true;
            },
            &java_guard);
    clean_udf_cache_callback(request(function_id));
    EXPECT_FALSE(has_pool(function_id));
    ASSERT_EQ(1, _finished.size());
    EXPECT_FALSE(_pool_existed_at_finish.front());
    EXPECT_NE(TStatusCode::OK, _finished.front().task_status.status_code);
    EXPECT_EQ(function_id, _finished.front().signature);
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
