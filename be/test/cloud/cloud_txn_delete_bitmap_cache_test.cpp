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

#include "cloud/cloud_txn_delete_bitmap_cache.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>

#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/resource_context.h"

namespace doris {

TEST(CloudTxnDeleteBitmapCacheTest, ContextlessDeleteUsesDefaultWorkloadGroup) {
    CloudTxnDeleteBitmapCache cache(1024 * 1024);
    ASSERT_TRUE(cache.init().ok());
    auto wg = std::make_shared<WorkloadGroup>(
            WorkloadGroupInfo {.id = 68385, .name = "bitmap_cache_test"});
    auto ctx = ResourceContext::create_shared();
    ctx->memory_context()->set_mem_tracker(
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "bitmap_cache_test"));
    ctx->set_workload_group(wg);
    // Agent DELETE workers have no attached task context. A fresh thread also
    // verifies that a prior load's workload group is not retained after detach.
    std::thread worker([&] {
        SCOPED_INIT_THREAD_CONTEXT();
        EXPECT_FALSE(thread_context()->is_attach_task());
        {
            SCOPED_ATTACH_TASK(ctx);
            cache.set_tablet_txn_info(1, 10, std::make_shared<DeleteBitmap>(10), {}, nullptr, 0,
                                      nullptr);
            EXPECT_EQ(cache.get_workload_group(1, 10), wg);
        }
        EXPECT_FALSE(thread_context()->is_attach_task());
        cache.set_tablet_txn_info(2, 10, std::make_shared<DeleteBitmap>(10), {}, nullptr, 0,
                                  nullptr);
        EXPECT_EQ(cache.get_workload_group(2, 10), nullptr);
        EXPECT_EQ(cache.get_workload_group(1, 10), wg);
        // Replacing an entry from a contextless caller clears its old owner too.
        cache.set_tablet_txn_info(1, 10, std::make_shared<DeleteBitmap>(10), {}, nullptr, 0,
                                  nullptr);
        EXPECT_EQ(cache.get_workload_group(1, 10), nullptr);
        EXPECT_FALSE(thread_context()->is_attach_task());
    });
    worker.join();
}

TEST(CloudTxnDeleteBitmapCacheTest, EmptyRowsetOwnerIsRetainedUntilCleanup) {
    CloudTxnDeleteBitmapCache cache(1024 * 1024);
    ASSERT_TRUE(cache.init().ok());
    auto wg = std::make_shared<WorkloadGroup>(
            WorkloadGroupInfo {.id = 68386, .name = "empty_bitmap_cache_test"});
    std::weak_ptr<WorkloadGroup> weak_wg = wg;
    auto ctx = ResourceContext::create_shared();
    ctx->memory_context()->set_mem_tracker(MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "empty_bitmap_cache_test"));
    ctx->set_workload_group(wg);
    {
        SCOPED_ATTACH_TASK(ctx);
        cache.mark_empty_rowset(1, 10, INT64_MAX);
        cache.mark_empty_rowset(2, 10, INT64_MAX);
    }
    EXPECT_TRUE(cache.is_empty_rowset(1, 10));
    EXPECT_EQ(cache.get_workload_group(1, 10), wg);
    EXPECT_EQ(cache.get_workload_group(2, 10), wg);
    EXPECT_EQ(cache.get_workload_group(3, 10), nullptr);
    auto rowset_and_bitmap = cache.get_rowset_and_delete_bitmap(1, 10);
    ASSERT_TRUE(rowset_and_bitmap.has_value());
    EXPECT_EQ(rowset_and_bitmap->first, nullptr);
    EXPECT_EQ(rowset_and_bitmap->second, nullptr);
    ctx.reset();
    wg.reset();
    EXPECT_FALSE(weak_wg.expired());

    cache.remove_unused_tablet_txn_info(1, 10);
    EXPECT_FALSE(cache.is_empty_rowset(1, 10));
    EXPECT_EQ(cache.get_workload_group(1, 10), nullptr);
    EXPECT_FALSE(weak_wg.expired());
    {
        std::unique_lock lock(cache._rwlock);
        CloudTxnDeleteBitmapCache::TxnKey key(2, 10);
        // A stale expiration entry must not remove a renewed marker or its owner.
        cache._expiration_txn.emplace(0, key);
    }
    cache.remove_expired_tablet_txn_info();
    EXPECT_TRUE(cache.is_empty_rowset(2, 10));
    EXPECT_FALSE(weak_wg.expired());
    {
        std::unique_lock lock(cache._rwlock);
        CloudTxnDeleteBitmapCache::TxnKey key(2, 10);
        cache._empty_rowset_markers.at(key).txn_expiration = 0;
        cache._expiration_txn.emplace(0, key);
    }
    cache.remove_expired_tablet_txn_info();
    EXPECT_FALSE(cache.is_empty_rowset(2, 10));
    EXPECT_EQ(cache.get_workload_group(2, 10), nullptr);
    EXPECT_TRUE(weak_wg.expired());
}

TEST(CloudTxnDeleteBitmapCacheTest, ContextlessEmptyRowsetClearsPreviousOwner) {
    CloudTxnDeleteBitmapCache cache(1024 * 1024);
    ASSERT_TRUE(cache.init().ok());
    auto wg = std::make_shared<WorkloadGroup>(
            WorkloadGroupInfo {.id = 68387, .name = "contextless_empty_bitmap_cache_test"});
    auto ctx = ResourceContext::create_shared();
    ctx->memory_context()->set_mem_tracker(MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "contextless_empty_bitmap_cache_test"));
    ctx->set_workload_group(wg);
    std::thread worker([&] {
        SCOPED_INIT_THREAD_CONTEXT();
        {
            SCOPED_ATTACH_TASK(ctx);
            cache.mark_empty_rowset(1, 10, INT64_MAX);
            EXPECT_EQ(cache.get_workload_group(1, 10), wg);
        }
        EXPECT_FALSE(thread_context()->is_attach_task());
        cache.mark_empty_rowset(1, 10, INT64_MAX);
        cache.mark_empty_rowset(2, 10, INT64_MAX);
        EXPECT_EQ(cache.get_workload_group(1, 10), nullptr);
        EXPECT_EQ(cache.get_workload_group(2, 10), nullptr);
        EXPECT_TRUE(cache.is_empty_rowset(1, 10));
        EXPECT_TRUE(cache.is_empty_rowset(2, 10));
    });
    worker.join();
}

} // namespace doris
