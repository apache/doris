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

#include "common/kv_cache.h"

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

namespace doris::cloud {

TEST(KvCacheTest, BasicGetPut) {
    TabletIndexCache cache(100);

    TabletIndexPB pb;
    pb.set_db_id(1);
    pb.set_table_id(2);
    pb.set_index_id(3);
    pb.set_partition_id(4);
    pb.set_tablet_id(100);

    auto key = std::make_tuple("instance1", 100L);
    cache.put(key, pb);

    TabletIndexPB result;
    ASSERT_TRUE(cache.get(key, &result));
    EXPECT_EQ(result.tablet_id(), 100);
    EXPECT_EQ(result.db_id(), 1);
}

TEST(KvCacheTest, CacheMiss) {
    TabletIndexCache cache(100);
    auto key = std::make_tuple("instance1", 999L);
    TabletIndexPB result;
    ASSERT_FALSE(cache.get(key, &result));
}

TEST(KvCacheTest, LRUEviction) {
    TabletIndexCache cache(16);

    // Insert 50 items to ensure eviction (shard capacity is 16/16+1=2, total ~32)
    for (int i = 1; i <= 50; ++i) {
        TabletIndexPB pb;
        pb.set_tablet_id(i);
        cache.put(std::make_tuple("inst", (int64_t)i), pb);
    }

    // Verify some early items were evicted
    TabletIndexPB result;
    int miss_count = 0;
    for (int i = 1; i <= 20; ++i) {
        if (!cache.get(std::make_tuple("inst", (int64_t)i), &result)) {
            miss_count++;
        }
    }
    EXPECT_GT(miss_count, 0);
}

TEST(KvCacheTest, Invalidate) {
    TabletIndexCache cache(100);
    TabletIndexPB pb;
    pb.set_tablet_id(100);
    auto key = std::make_tuple("inst", 100L);
    cache.put(key, pb);

    TabletIndexPB result;
    ASSERT_TRUE(cache.get(key, &result));

    cache.invalidate(key);
    ASSERT_FALSE(cache.get(key, &result));
}

TEST(KvCacheTest, Clear) {
    TabletIndexCache cache(100);
    for (int i = 1; i <= 5; ++i) {
        TabletIndexPB pb;
        pb.set_tablet_id(i);
        cache.put(std::make_tuple("inst", (int64_t)i), pb);
    }
    EXPECT_EQ(cache.size(), 5);

    cache.clear();
    EXPECT_EQ(cache.size(), 0);
}

TEST(KvCacheTest, ConcurrentAccess) {
    TabletIndexCache cache(1000);
    std::vector<std::thread> threads;

    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&cache, t]() {
            for (int i = 0; i < 100; ++i) {
                TabletIndexPB pb;
                pb.set_tablet_id(t * 100 + i);
                cache.put(std::make_tuple("inst", (int64_t)(t * 100 + i)), pb);

                TabletIndexPB result;
                cache.get(std::make_tuple("inst", (int64_t)(t * 100 + i)), &result);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
}

} // namespace doris::cloud
