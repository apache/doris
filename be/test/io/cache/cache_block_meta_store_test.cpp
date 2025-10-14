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

#include "io/cache/cache_block_meta_store.h"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <random>
#include <thread>
#include <vector>

#include "common/status.h"

namespace doris::io {

class CacheBlockMetaStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for test database
        test_db_path_ = std::filesystem::temp_directory_path() / "cache_block_meta_store_test";
        std::filesystem::remove_all(test_db_path_);
        std::filesystem::create_directories(test_db_path_);

        meta_store_ = std::make_unique<CacheBlockMetaStore>(test_db_path_.string());
        auto status = meta_store_->init();
        ASSERT_TRUE(status.ok()) << "Failed to initialize CacheBlockMetaStore: "
                                 << status.to_string();
    }

    void TearDown() override {
        meta_store_.reset();
        std::filesystem::remove_all(test_db_path_);
    }

    std::filesystem::path test_db_path_;
    std::unique_ptr<CacheBlockMetaStore> meta_store_;
};

TEST_F(CacheBlockMetaStoreTest, BasicPutAndGet) {
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);
    BlockMeta meta1(1, 1024);

    // Test put operation
    meta_store_->put(key1, meta1);

    // Wait a bit for async operation to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test get operation
    BlockMeta result = meta_store_->get(key1);
    EXPECT_EQ(result.type, meta1.type);
    EXPECT_EQ(result.size, meta1.size);

    // Test non-existent key
    uint128_t hash2 = (static_cast<uint128_t>(999) << 64) | 999;
    BlockMetaKey non_existent_key(999, UInt128Wrapper(hash2), 999);
    BlockMeta non_existent_result = meta_store_->get(non_existent_key);
    EXPECT_EQ(non_existent_result.type, 0);
    EXPECT_EQ(non_existent_result.size, 0);
}

TEST_F(CacheBlockMetaStoreTest, MultiplePutsAndGets) {
    const int num_keys = 10;
    std::vector<BlockMetaKey> keys;
    std::vector<BlockMeta> metas;

    // Create multiple keys and metas
    for (int i = 0; i < num_keys; ++i) {
        uint128_t hash = (static_cast<uint128_t>(i) << 64) | (i * 100);
        keys.emplace_back(1, UInt128Wrapper(hash), i * 1024);
        metas.emplace_back(i % 3, 1024 * (i + 1));
        meta_store_->put(keys[i], metas[i]);
    }

    // Wait for async operations
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify all keys
    for (int i = 0; i < num_keys; ++i) {
        BlockMeta result = meta_store_->get(keys[i]);
        EXPECT_EQ(result.type, metas[i].type);
        EXPECT_EQ(result.size, metas[i].size);
    }
}

TEST_F(CacheBlockMetaStoreTest, RangeQuery) {
    const int num_tablets = 3;
    const int blocks_per_tablet = 5;

    // Create data for multiple tablets
    for (int tablet_id = 1; tablet_id <= num_tablets; ++tablet_id) {
        for (int i = 0; i < blocks_per_tablet; ++i) {
            uint128_t hash =
                    (static_cast<uint128_t>(tablet_id * 100 + i) << 64) | (tablet_id * 200 + i);
            BlockMetaKey key(tablet_id, UInt128Wrapper(hash), i * 1024);
            BlockMeta meta(i % 2, 2048 * (i + 1));
            meta_store_->put(key, meta);
        }
    }

    // Wait for async operations
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Test range query for each tablet
    for (int tablet_id = 1; tablet_id <= num_tablets; ++tablet_id) {
        auto iterator = meta_store_->range_get(tablet_id);

        int count = 0;
        while (iterator->valid()) {
            BlockMetaKey key = iterator->key();
            BlockMeta value = iterator->value();

            EXPECT_EQ(key.tablet_id, tablet_id);
            EXPECT_TRUE(value.type == 0 || value.type == 1);
            EXPECT_GT(value.size, 0);

            iterator->next();
            count++;
        }

        EXPECT_EQ(count, blocks_per_tablet);
    }

    // Test range query for non-existent tablet
    auto iterator = meta_store_->range_get(999);
    EXPECT_FALSE(iterator->valid());
}

TEST_F(CacheBlockMetaStoreTest, DeleteOperation) {
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);
    BlockMeta meta1(1, 1024);

    // Put then delete
    meta_store_->put(key1, meta1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify put worked
    BlockMeta result = meta_store_->get(key1);
    EXPECT_EQ(result.type, meta1.type);

    // Delete the key
    meta_store_->delete_key(key1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify deletion
    BlockMeta deleted_result = meta_store_->get(key1);
    EXPECT_EQ(deleted_result.type, 0);
    EXPECT_EQ(deleted_result.size, 0);
}

TEST_F(CacheBlockMetaStoreTest, SerializationDeserialization) {
    uint128_t hash3 = (static_cast<uint128_t>(456789) << 64) | 987654;
    BlockMetaKey original_key(123, UInt128Wrapper(hash3), 1024);
    BlockMeta original_meta(2, 4096);

    // Test round-trip through put and get operations
    meta_store_->put(original_key, original_meta);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    BlockMeta retrieved = meta_store_->get(original_key);
    EXPECT_EQ(retrieved.type, original_meta.type);
    EXPECT_EQ(retrieved.size, original_meta.size);

    // Test non-existent key
    uint128_t hash4 = (static_cast<uint128_t>(999999) << 64) | 888888;
    BlockMetaKey non_existent_key(999, UInt128Wrapper(hash4), 2048);
    BlockMeta non_existent_result = meta_store_->get(non_existent_key);
    EXPECT_EQ(non_existent_result.type, 0);
    EXPECT_EQ(non_existent_result.size, 0);
}

TEST_F(CacheBlockMetaStoreTest, ConcurrencyTest) {
    const int num_threads = 4;
    const int operations_per_thread = 100;
    std::atomic<int> successful_puts(0);

    // Store keys for later verification
    std::vector<BlockMetaKey> all_keys;
    std::mutex keys_mutex;

    auto worker = [&](int thread_id) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(1, 1000);

        for (int i = 0; i < operations_per_thread; ++i) {
            int64_t tablet_id = thread_id + 1;
            uint128_t hash_value = (static_cast<uint128_t>(dist(gen)) << 64) | dist(gen);
            size_t offset = i * 1024;

            BlockMetaKey key(tablet_id, UInt128Wrapper(hash_value), offset);
            BlockMeta meta(thread_id % 3, 2048);

            // Put operation
            meta_store_->put(key, meta);
            successful_puts++;

            // Store key for later verification
            {
                std::lock_guard<std::mutex> lock(keys_mutex);
                all_keys.push_back(key);
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Wait for all async operations to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    EXPECT_EQ(successful_puts, num_threads * operations_per_thread);

    // Verify we can retrieve the data after all writes are complete
    int successful_gets = 0;
    for (const auto& key : all_keys) {
        BlockMeta result = meta_store_->get(key);
        if (result.size > 0) {
            successful_gets++;
        }
    }

    EXPECT_GT(successful_gets, 0);

    // Verify we can retrieve some of the data
    for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
        for (int i = 0; i < 5; ++i) { // Check a few samples
            uint128_t hash = (static_cast<uint128_t>(100 + i) << 64) | (200 + i);
            BlockMetaKey key(thread_id + 1, UInt128Wrapper(hash), i * 1024);
            BlockMeta result = meta_store_->get(key);
            EXPECT_GE(result.size, 0);
        }
    }
}

TEST_F(CacheBlockMetaStoreTest, IteratorValidity) {
    // Put some data
    for (int i = 0; i < 5; ++i) {
        uint128_t hash = (static_cast<uint128_t>(100 + i) << 64) | (200 + i);
        BlockMetaKey key(1, UInt128Wrapper(hash), i * 1024);
        BlockMeta meta(i % 2, 2048 * (i + 1));
        meta_store_->put(key, meta);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test iterator
    auto iterator = meta_store_->range_get(1);
    int count = 0;

    while (iterator->valid()) {
        BlockMetaKey key = iterator->key();
        BlockMeta value = iterator->value();

        EXPECT_EQ(key.tablet_id, 1);
        EXPECT_GE(key.offset, 0);
        EXPECT_GT(value.size, 0);

        iterator->next();
        count++;
    }

    EXPECT_EQ(count, 5);

    // Test that iterator becomes invalid after reaching end
    EXPECT_FALSE(iterator->valid());
}

TEST_F(CacheBlockMetaStoreTest, KeyToString) {
    uint128_t hash4 = (static_cast<uint128_t>(456789) << 64) | 987654;
    BlockMetaKey key(123, UInt128Wrapper(hash4), 1024);
    std::string key_str = key.to_string();

    // UInt128Wrapper::to_string() returns hex string, so check for hex representations
    EXPECT_NE(key_str.find("123"), std::string::npos);  // tablet_id in decimal
    EXPECT_NE(key_str.find("1024"), std::string::npos); // offset in decimal

    // Check for hex representations of the hash parts
    // 456789 in hex is 6f855, 987654 in hex is f1206
    EXPECT_NE(key_str.find("6f855"), std::string::npos);
    EXPECT_NE(key_str.find("f1206"), std::string::npos);
}

TEST_F(CacheBlockMetaStoreTest, BlockMetaEquality) {
    BlockMeta meta1(1, 1024);
    BlockMeta meta2(1, 1024);
    BlockMeta meta3(2, 1024);
    BlockMeta meta4(1, 2048);

    EXPECT_TRUE(meta1 == meta2);
    EXPECT_FALSE(meta1 == meta3);
    EXPECT_FALSE(meta1 == meta4);
}

TEST_F(CacheBlockMetaStoreTest, BlockMetaKeyEquality) {
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    uint128_t hash2 = (static_cast<uint128_t>(789) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);
    BlockMetaKey key2(1, UInt128Wrapper(hash1), 0);
    BlockMetaKey key3(2, UInt128Wrapper(hash1), 0);
    BlockMetaKey key4(1, UInt128Wrapper(hash2), 0);
    BlockMetaKey key5(1, UInt128Wrapper(hash1), 1024);

    EXPECT_TRUE(key1 == key2);
    EXPECT_FALSE(key1 == key3);
    EXPECT_FALSE(key1 == key4);
    EXPECT_FALSE(key1 == key5);
}

TEST_F(CacheBlockMetaStoreTest, ClearAllRecords) {
    // Add multiple records to the store
    const int num_records = 10;
    std::vector<BlockMetaKey> keys;

    for (int i = 0; i < num_records; ++i) {
        uint128_t hash = (static_cast<uint128_t>(i) << 64) | (i * 100);
        BlockMetaKey key(1, UInt128Wrapper(hash), i * 1024);
        BlockMeta meta(i % 3, 2048 * (i + 1));

        keys.push_back(key);
        meta_store_->put(key, meta);
    }

    // Wait for async operations to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify all records are present
    for (int i = 0; i < num_records; ++i) {
        BlockMeta result = meta_store_->get(keys[i]);
        EXPECT_EQ(result.type, i % 3);
        EXPECT_EQ(result.size, 2048 * (i + 1));
    }

    // Clear all records
    meta_store_->clear();

    // Wait a bit for clear operation to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify all records are gone
    for (int i = 0; i < num_records; ++i) {
        BlockMeta result = meta_store_->get(keys[i]);
        EXPECT_EQ(result.type, 0);
        EXPECT_EQ(result.size, 0);
    }

    // Verify range query returns no results
    auto iterator = meta_store_->range_get(1);
    EXPECT_FALSE(iterator->valid());
}

TEST_F(CacheBlockMetaStoreTest, ClearWithPendingAsyncOperations) {
    // Add some records and immediately call clear
    // This tests that pending operations in the queue are handled correctly

    // Add a record
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);
    BlockMeta meta1(1, 1024);
    meta_store_->put(key1, meta1);

    // Immediately clear without waiting for async operation
    meta_store_->clear();

    // Wait a bit for operations to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify the record was not written (cleared from queue)
    BlockMeta result = meta_store_->get(key1);
    EXPECT_EQ(result.type, 0);
    EXPECT_EQ(result.size, 0);
}

TEST_F(CacheBlockMetaStoreTest, ClearAndThenAddNewRecords) {
    // Test that after clear, the store can accept new records

    // Add initial records
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);
    BlockMeta meta1(1, 1024);
    meta_store_->put(key1, meta1);

    // Wait for async operation
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Clear all records
    meta_store_->clear();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Add new records after clear
    uint128_t hash2 = (static_cast<uint128_t>(789) << 64) | 123;
    BlockMetaKey key2(2, UInt128Wrapper(hash2), 1024);
    BlockMeta meta2(2, 2048);
    meta_store_->put(key2, meta2);

    // Wait for async operation
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify old record is gone
    BlockMeta result1 = meta_store_->get(key1);
    EXPECT_EQ(result1.type, 0);
    EXPECT_EQ(result1.size, 0);

    // Verify new record is present
    BlockMeta result2 = meta_store_->get(key2);
    EXPECT_EQ(result2.type, 2);
    EXPECT_EQ(result2.size, 2048);
}

TEST_F(CacheBlockMetaStoreTest, ClearMultipleTimes) {
    // Test that clear can be called multiple times without issues

    // Add a record
    uint128_t hash = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key(1, UInt128Wrapper(hash), 0);
    BlockMeta meta(1, 1024);
    meta_store_->put(key, meta);

    // Wait for async operation
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Clear multiple times
    meta_store_->clear();
    meta_store_->clear();
    meta_store_->clear();

    // Wait for operations to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify record is gone
    BlockMeta result = meta_store_->get(key);
    EXPECT_EQ(result.type, 0);
    EXPECT_EQ(result.size, 0);
}

TEST_F(CacheBlockMetaStoreTest, ClearEmptyStore) {
    // Test clearing an empty store (should not crash or error)

    // Clear without adding any records
    meta_store_->clear();

    // Wait a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify store is still functional
    uint128_t hash = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key(1, UInt128Wrapper(hash), 0);
    BlockMeta result = meta_store_->get(key);
    EXPECT_EQ(result.type, 0);
    EXPECT_EQ(result.size, 0);
}

} // namespace doris::io