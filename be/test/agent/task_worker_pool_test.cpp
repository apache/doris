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
#include <unordered_map>

#include "agent/agent_server.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_meta.h"

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

TEST(TaskWorkerPoolTest, CloudOwnerRefreshUsesEpochAndOldSenderOnlyInvalidates) {
    const bool old_rw_separation = config::enable_compaction_rw_separation;
    Defer restore_config {[&] { config::enable_compaction_rw_separation = old_rw_separation; }};
    config::enable_compaction_rw_separation = true;

    CloudStorageEngine engine(EngineOptions {});
    auto tablet_meta = std::make_shared<TabletMeta>(1, 2, 10001, 10002, 4, 5, TTabletSchema(), 6,
                                                    std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                    UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                    TCompressionType::LZ4F);
    auto tablet = std::make_shared<CloudTablet>(engine, std::move(tablet_meta));
    engine.tablet_mgr().put_tablet_for_UT(tablet);

    auto refresh_owner = [&](const std::string& cluster_id, int64_t time_ms,
                             const std::vector<int64_t>& epochs) {
        TMakeCloudTmpRsVisibleRequest make_visible;
        make_visible.__set_txn_id(123);
        make_visible.__set_tablet_ids({});
        make_visible.__set_partition_version_map({});
        make_visible.__set_version_update_time_ms(time_ms);
        make_visible.__set_load_cluster_id(cluster_id);
        make_visible.__set_last_active_tablet_ids({tablet->tablet_id()});
        if (!epochs.empty()) {
            make_visible.__set_last_active_epochs(epochs);
        }
        TAgentTaskRequest task;
        task.__set_make_cloud_tmp_rs_visible_req(make_visible);
        make_cloud_committed_rs_visible_callback(engine, task);
    };

    refresh_owner("cluster_b", 100, {5});
    EXPECT_EQ(tablet->last_active_cluster_id(), "cluster_b");
    EXPECT_EQ(tablet->last_active_time_ms(), 100);
    EXPECT_EQ(tablet->last_active_epoch(), 5);

    refresh_owner("cluster_stale", 200, {4});
    EXPECT_EQ(tablet->last_active_cluster_id(), "cluster_b");
    EXPECT_EQ(tablet->last_active_time_ms(), 100);
    EXPECT_EQ(tablet->last_active_epoch(), 5);

    tablet->last_sync_time_s = 123;
    refresh_owner("cluster_without_epoch", 300, {});
    EXPECT_EQ(tablet->last_active_cluster_id(), "cluster_b");
    EXPECT_EQ(tablet->last_active_epoch(), 5);
    EXPECT_EQ(tablet->last_sync_time_s, 0);
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
