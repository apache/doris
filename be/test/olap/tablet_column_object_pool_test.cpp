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

#include "olap/tablet_column_object_pool.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>

namespace doris {

// Declare external variables
extern bvar::Adder<int64_t> g_tablet_column_cache_count;
extern bvar::Adder<int64_t> g_tablet_column_cache_hit_count;
extern bvar::Adder<int64_t> g_tablet_index_cache_count;
extern bvar::Adder<int64_t> g_tablet_index_cache_hit_count;

class TabletColumnObjectPoolTest : public testing::Test {
protected:
    void SetUp() override {
        // reset all counters to 0
        g_tablet_column_cache_count.reset();
        g_tablet_column_cache_hit_count.reset();
        g_tablet_index_cache_count.reset();
        g_tablet_index_cache_hit_count.reset();

        _pool = std::make_unique<TabletColumnObjectPool>(1024);
    }

    // Helper function to create a column PB
    std::string create_column_pb() {
        ColumnPB pb;
        pb.set_unique_id(123);
        pb.set_name("test_column");
        pb.set_type("TINYINT");
        std::string serialized;
        pb.SerializeToString(&serialized);
        return serialized;
    }

    // Helper function to create an index PB
    std::string create_index_pb() {
        TabletIndexPB pb;
        pb.set_index_id(456);
        pb.set_index_name("test_index");
        std::string serialized;
        pb.SerializeToString(&serialized);
        return serialized;
    }

    std::unique_ptr<TabletColumnObjectPool> _pool;
};

// Test column cache insertion and hit
TEST_F(TabletColumnObjectPoolTest, TestColumnCacheInsertAndHit) {
    std::string key = create_column_pb();

    // First insertion
    int64_t initial_count = g_tablet_column_cache_count.get_value();
    int64_t initial_hit_count = g_tablet_column_cache_hit_count.get_value();

    auto [handle1, ptr1] = _pool->insert(key);
    ASSERT_NE(nullptr, handle1);
    ASSERT_NE(nullptr, ptr1);
    ASSERT_EQ(initial_count + 1, g_tablet_column_cache_count.get_value());

    // Second insertion with same key should hit cache
    auto [handle2, ptr2] = _pool->insert(key);
    ASSERT_NE(nullptr, handle2);
    ASSERT_NE(nullptr, ptr2);
    ASSERT_EQ(ptr1, ptr2); // Should return same object
    ASSERT_EQ(initial_hit_count + 1, g_tablet_column_cache_hit_count.get_value());

    _pool->release(handle1);
    _pool->release(handle2);
}

// Test index cache insertion and hit
TEST_F(TabletColumnObjectPoolTest, TestIndexCacheInsertAndHit) {
    std::string key = create_index_pb();

    // First insertion
    int64_t initial_count = g_tablet_index_cache_count.get_value();
    int64_t initial_hit_count = g_tablet_index_cache_hit_count.get_value();

    auto [handle1, ptr1] = _pool->insert_index(key);
    ASSERT_NE(nullptr, handle1);
    ASSERT_NE(nullptr, ptr1);
    ASSERT_EQ(initial_count + 1, g_tablet_index_cache_count.get_value());

    // Second insertion with same key should hit cache
    auto [handle2, ptr2] = _pool->insert_index(key);
    ASSERT_NE(nullptr, handle2);
    ASSERT_NE(nullptr, ptr2);
    ASSERT_EQ(ptr1, ptr2); // Should return same object
    ASSERT_EQ(initial_hit_count + 1, g_tablet_index_cache_hit_count.get_value());

    _pool->release(handle1);
    _pool->release(handle2);
}

// Test cache eviction
TEST_F(TabletColumnObjectPoolTest, TestCacheEviction) {
    // Create a small cache
    TabletColumnObjectPool small_pool(2); // Only 2 entries

    std::string key1 = create_column_pb();
    std::string key2 = create_column_pb();
    std::string key3 = create_column_pb();

    // Insert first two entries
    auto [handle1, ptr1] = small_pool.insert(key1);
    auto [handle2, ptr2] = small_pool.insert(key2);

    // Release handle1 to allow eviction
    small_pool.release(handle1);

    // Insert third entry should evict first entry
    auto [handle3, ptr3] = small_pool.insert(key3);

    // Try to get first entry again - should be a new object
    auto [handle1_new, ptr1_new] = small_pool.insert(key1);
    ASSERT_NE(ptr1, ptr1_new); // Should be different objects

    small_pool.release(handle2);
    small_pool.release(handle3);
    small_pool.release(handle1_new);
}

// Test destructor behavior
TEST_F(TabletColumnObjectPoolTest, TestDestructor) {
    {
        TabletColumnObjectPool temp_pool(10);
        std::string key = create_column_pb();

        int64_t initial_count = g_tablet_column_cache_count.get_value();
        auto [handle, ptr] = temp_pool.insert(key);
        ASSERT_EQ(initial_count + 1, g_tablet_column_cache_count.get_value());

        temp_pool.release(handle);
    } // temp_pool destructor should be called here
    // Verify counter was decremented
    ASSERT_EQ(g_tablet_column_cache_count.get_value(), 0);
}

} // namespace doris

// int main(int argc, char** argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }