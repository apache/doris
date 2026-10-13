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

#include "cloud/cloud_engine_calc_delete_bitmap_task.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "util/threadpool.h"

namespace doris {

TEST(CloudEngineCalcDeleteBitmapTaskTest, SubmissionFailureKeepsOriginalError) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(
            ThreadPoolBuilder("cloud_bitmap_rejected_test").set_max_threads(1).build(&pool).ok());
    CloudStorageEngine engine {EngineOptions()};
    engine._txn_delete_bitmap_cache = std::make_unique<CloudTxnDeleteBitmapCache>(1024 * 1024);
    ASSERT_TRUE(engine._txn_delete_bitmap_cache->init().ok());
    engine._calc_delete_bitmap_executor = std::make_unique<CalcDeleteBitmapExecutor>();
    engine._calc_delete_bitmap_executor->init("cloud_bitmap_background_test", 1, pool.get());
    SCOPED_ATTACH_TASK(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "cloud_bitmap_rejected_test"));
    pool->shutdown();

    TCalcDeleteBitmapRequest request;
    request.transaction_id = 1;
    TCalcDeleteBitmapPartitionInfo partition;
    partition.partition_id = 1;
    partition.version = 2;
    partition.tablet_ids = {10};
    request.partitions = {partition};
    std::vector<TTabletId> errors, successes;
    CloudEngineCalcDeleteBitmapTask task(engine, request, &errors, &successes);
    auto st = task.execute();
    EXPECT_TRUE(st.is<ErrorCode::SERVICE_UNAVAILABLE>());
    EXPECT_NE(st.to_string().find("cloud_bitmap_rejected_test"), std::string::npos);
    EXPECT_NE(st.to_string().find("shut down"), std::string::npos);
    EXPECT_EQ(errors, (std::vector<TTabletId> {10}));
    EXPECT_TRUE(successes.empty());

    errors.clear();
    CloudEngineCalcDeleteBitmapTask failed_task(engine, request, &errors, &successes);
    auto original = Status::InternalError("earlier tablet bitmap failure");
    failed_task.add_error_tablet_id(9, original);
    // Even when wait() fails, retain the tablet error already selected by the
    // cloud task instead of replacing it with the later submission failure.
    EXPECT_EQ(failed_task.execute().to_string(), original.to_string());
    EXPECT_EQ(errors, (std::vector<TTabletId> {9, 10}));
    EXPECT_TRUE(successes.empty());
}

TEST(CloudEngineCalcDeleteBitmapTaskTest, EmptyRowsetPublishUsesCachedWorkloadGroup) {
    std::unique_ptr<ThreadPool> default_pool;
    ASSERT_TRUE(ThreadPoolBuilder("empty_publish_default")
                        .set_max_threads(1)
                        .build(&default_pool)
                        .ok());
    auto wg = std::make_shared<WorkloadGroup>(
            WorkloadGroupInfo {.id = 68388, .name = "empty_publish_wg"});
    ASSERT_TRUE(ThreadPoolBuilder("empty_publish_wg")
                        .set_max_threads(1)
                        .build(&wg->_memtable_flush_pool)
                        .ok());
    CloudStorageEngine engine {EngineOptions()};
    engine._txn_delete_bitmap_cache = std::make_unique<CloudTxnDeleteBitmapCache>(1024 * 1024);
    ASSERT_TRUE(engine._txn_delete_bitmap_cache->init().ok());
    engine._calc_delete_bitmap_executor = std::make_unique<CalcDeleteBitmapExecutor>();
    engine._calc_delete_bitmap_executor->init("empty_publish_background", 1, default_pool.get());
    auto ctx = ResourceContext::create_shared();
    ctx->memory_context()->set_mem_tracker(MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "empty_publish_writer"));
    ctx->set_workload_group(wg);
    {
        SCOPED_ATTACH_TASK(ctx);
        engine.txn_delete_bitmap_cache().mark_empty_rowset(1, 10, INT64_MAX);
        engine.txn_delete_bitmap_cache().mark_empty_rowset(3, 10, INT64_MAX);
    }
    // The publish request itself has no workload group. The first empty
    // subtransaction has no owner, so routing must continue to the second one.
    SCOPED_ATTACH_TASK(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "empty_publish_request"));
    engine.txn_delete_bitmap_cache().mark_empty_rowset(2, 10, INT64_MAX);
    default_pool->shutdown();
    wg->_memtable_flush_pool->shutdown();
    // Distinct shutdown errors identify the pool selected by the real execute
    // path without requiring tablet metadata or meta-service RPCs.
    for (bool subtransactions : {false, true}) {
        TCalcDeleteBitmapRequest request;
        request.transaction_id = subtransactions ? 4 : 1;
        TCalcDeleteBitmapPartitionInfo partition;
        partition.partition_id = 1;
        partition.version = 2;
        partition.tablet_ids = {10};
        if (subtransactions) {
            partition.__set_sub_txn_ids({2, 3});
        }
        request.partitions = {partition};
        for (int retry = 0; retry < 2; ++retry) {
            std::vector<TTabletId> errors, successes;
            CloudEngineCalcDeleteBitmapTask task(engine, request, &errors, &successes);
            auto st = task.execute();
            EXPECT_TRUE(st.is<ErrorCode::SERVICE_UNAVAILABLE>());
            EXPECT_NE(st.to_string().find("empty_publish_wg"), std::string::npos);
            EXPECT_EQ(errors, (std::vector<TTabletId> {10}));
            EXPECT_TRUE(successes.empty());
        }
    }
}

} // namespace doris
