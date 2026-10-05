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
#include <optional>
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

        ASSERT_NE(meta_store_, nullptr);
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
    BlockMeta meta1(NORMAL, 1024, 3600);

    // Test put operation
    meta_store_->put(key1, meta1);

    // Wait a bit for async operation to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Test get operation
    auto result = meta_store_->get(key1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->type, meta1.type);
    EXPECT_EQ(result->size, meta1.size);
    EXPECT_EQ(result->ttl, meta1.ttl);

    // Test non-existent key
    uint128_t hash2 = (static_cast<uint128_t>(999) << 64) | 999;
    BlockMetaKey non_existent_key(999, UInt128Wrapper(hash2), 999);
    auto non_existent_result = meta_store_->get(non_existent_key);
    EXPECT_FALSE(non_existent_result.has_value());
}

TEST_F(CacheBlockMetaStoreTest, MultiplePutsAndGets) {
    const int num_keys = 10;
    std::vector<BlockMetaKey> keys;
    std::vector<BlockMeta> metas;

    // Create multiple keys and metas
    for (int i = 0; i < num_keys; ++i) {
        uint128_t hash = (static_cast<uint128_t>(i) << 64) | (i * 100);
        keys.emplace_back(1, UInt128Wrapper(hash), i * 1024);
        FileCacheType type = static_cast<FileCacheType>(i % 3);
        metas.emplace_back(type, 1024 * (i + 1), 3600 + i * 100);
        meta_store_->put(keys[i], metas[i]);
    }

    // Wait for async operations
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify all keys
    for (int i = 0; i < num_keys; ++i) {
        auto result = meta_store_->get(keys[i]);
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result->type, metas[i].type);
        EXPECT_EQ(result->size, metas[i].size);
        EXPECT_EQ(result->ttl, metas[i].ttl);
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
            FileCacheType type = static_cast<FileCacheType>(i % 2);
            BlockMeta meta(type, 2048 * (i + 1), 3600 + i * 100);
            meta_store_->put(key, meta);
        }
    }

    // Wait for async operations
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Test range query for each tablet
    for (int tablet_id = 1; tablet_id <= num_tablets; ++tablet_id) {
        auto iterator = meta_store_->range_get(tablet_id);
        ASSERT_NE(iterator, nullptr) << "Failed to create iterator for tablet " << tablet_id;

        int count = 0;
        while (iterator->valid()) {
            BlockMetaKey key = iterator->key();
            BlockMeta value = iterator->value();

            EXPECT_EQ(key.tablet_id, tablet_id);
            EXPECT_TRUE(value.type == DISPOSABLE || value.type == NORMAL);
            EXPECT_GT(value.size, 0);

            iterator->next();
            count++;
        }

        EXPECT_EQ(count, blocks_per_tablet);
    }

    // Test range query for non-existent tablet
    auto iterator = meta_store_->range_get(999);
    ASSERT_NE(iterator, nullptr) << "Failed to create iterator for non-existent tablet";
    EXPECT_FALSE(iterator->valid());
}

TEST_F(CacheBlockMetaStoreTest, DeleteOperation) {
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);
    BlockMeta meta1(NORMAL, 1024, 3600);

    // Put then delete
    meta_store_->put(key1, meta1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Verify put worked
    auto result = meta_store_->get(key1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->type, meta1.type);

    // Delete the key
    meta_store_->delete_key(key1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify deletion
    auto deleted_result = meta_store_->get(key1);
    EXPECT_FALSE(deleted_result.has_value());
}

TEST_F(CacheBlockMetaStoreTest, SerializationDeserialization) {
    uint128_t hash3 = (static_cast<uint128_t>(456789) << 64) | 987654;
    BlockMetaKey original_key(123, UInt128Wrapper(hash3), 1024);
    BlockMeta original_meta(INDEX, 4096, 7200);

    // Test round-trip through put and get operations
    meta_store_->put(original_key, original_meta);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    auto retrieved = meta_store_->get(original_key);
    EXPECT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->type, original_meta.type);
    EXPECT_EQ(retrieved->size, original_meta.size);
    EXPECT_EQ(retrieved->ttl, original_meta.ttl);

    // Test non-existent key
    uint128_t hash4 = (static_cast<uint128_t>(999999) << 64) | 888888;
    BlockMetaKey non_existent_key(999, UInt128Wrapper(hash4), 2048);
    auto non_existent_result = meta_store_->get(non_existent_key);
    EXPECT_FALSE(non_existent_result.has_value());
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
            FileCacheType type = static_cast<FileCacheType>(thread_id % 3);
            BlockMeta meta(type, 2048, 3600 + thread_id * 100 + i);

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
        auto result = meta_store_->get(key);
        if (result.has_value() && result->size > 0) {
            successful_gets++;
        }
    }

    EXPECT_GT(successful_gets, 0);

    // Verify we can retrieve some of the data
    for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
        for (int i = 0; i < 5; ++i) { // Check a few samples
            uint128_t hash = (static_cast<uint128_t>(100 + i) << 64) | (200 + i);
            BlockMetaKey key(thread_id + 1, UInt128Wrapper(hash), i * 1024);
            auto result = meta_store_->get(key);
            if (result.has_value()) {
                EXPECT_GE(result->size, 0);
                EXPECT_GE(result->ttl, 0);
            }
        }
    }
}

TEST_F(CacheBlockMetaStoreTest, IteratorValidity) {
    // Put some data
    for (int i = 0; i < 5; ++i) {
        uint128_t hash = (static_cast<uint128_t>(100 + i) << 64) | (200 + i);
        BlockMetaKey key(1, UInt128Wrapper(hash), i * 1024);
        FileCacheType type = static_cast<FileCacheType>(i % 2);
        BlockMeta meta(type, 2048 * (i + 1), 3600 + i * 100);
        meta_store_->put(key, meta);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test iterator
    auto iterator = meta_store_->range_get(1);
    ASSERT_NE(iterator, nullptr) << "Failed to create iterator for tablet 1";
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

TEST_F(CacheBlockMetaStoreTest, KeySerialization) {
    uint128_t hash4 = (static_cast<uint128_t>(456789) << 64) | 987654;
    BlockMetaKey key(123, UInt128Wrapper(hash4), 1024);

    // Test round-trip serialization
    std::string serialized = serialize_key(key);
    Status status;
    auto deserialized = deserialize_key(serialized, &status);

    EXPECT_TRUE(deserialized.has_value()) << "Failed to deserialize key: " << status.to_string();
    EXPECT_TRUE(status.ok()) << "Deserialization failed with status: " << status.to_string();
    EXPECT_EQ(deserialized->tablet_id, key.tablet_id);
    EXPECT_EQ(deserialized->hash, key.hash);
    EXPECT_EQ(deserialized->offset, key.offset);

    // Verify version byte
    EXPECT_EQ(serialized[0], 0x1);
}

TEST_F(CacheBlockMetaStoreTest, KeyOrder) {
    // Test that keys are properly ordered for the same tablet and hash
    uint128_t hash = (static_cast<uint128_t>(123) << 64) | 456;

    // Create keys with same tablet_id and hash, different offsets
    BlockMetaKey key1(1, UInt128Wrapper(hash), 1);
    BlockMetaKey key2(1, UInt128Wrapper(hash), 2);
    BlockMetaKey key3(1, UInt128Wrapper(hash), 100);

    // Serialize all keys
    std::string serialized1 = serialize_key(key1);
    std::string serialized2 = serialize_key(key2);
    std::string serialized3 = serialize_key(key3);

    // Verify that offset=1 comes before offset=2
    EXPECT_LT(serialized1, serialized2)
            << "Key with offset=1 should come before offset=2 for same tablet and hash";

    // Verify that offset=2 comes before offset=100
    EXPECT_LT(serialized2, serialized3)
            << "Key with offset=2 should come before offset=100 for same tablet and hash";

    // Verify that offset=1 comes before offset=100
    EXPECT_LT(serialized1, serialized3)
            << "Key with offset=1 should come before offset=100 for same tablet and hash";

    // Test with different tablet_ids
    BlockMetaKey key4(2, UInt128Wrapper(hash), 1);
    std::string serialized4 = serialize_key(key4);

    // Tablet 1 should come before tablet 2
    EXPECT_LT(serialized1, serialized4)
            << "Key with tablet_id=1 should come before tablet_id=2 for same hash and offset";

    // Test with different hashes but same tablet and offset
    uint128_t hash2 = (static_cast<uint128_t>(124) << 64) | 456;
    BlockMetaKey key5(1, UInt128Wrapper(hash2), 1);
    std::string serialized5 = serialize_key(key5);

    // Hash 123 should come before hash 124
    EXPECT_LT(serialized1, serialized5)
            << "Key with hash=123 should come before hash=124 for same tablet and offset";
}

TEST_F(CacheBlockMetaStoreTest, BlockMetaEquality) {
    BlockMeta meta1(NORMAL, 1024, 3600);
    BlockMeta meta2(NORMAL, 1024, 3600);
    BlockMeta meta3(INDEX, 1024, 3600);
    BlockMeta meta4(NORMAL, 2048, 3600);
    BlockMeta meta5(NORMAL, 1024, 7200);

    EXPECT_TRUE(meta1 == meta2);
    EXPECT_FALSE(meta1 == meta3);
    EXPECT_FALSE(meta1 == meta4);
    EXPECT_FALSE(meta1 == meta5);
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
        FileCacheType type = static_cast<FileCacheType>(i % 3);
        BlockMeta meta(type, 2048 * (i + 1), 3600 + i * 100);

        keys.push_back(key);
        meta_store_->put(key, meta);
    }

    // Wait for async operations to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify all records are present
    for (int i = 0; i < num_records; ++i) {
        auto result = meta_store_->get(keys[i]);
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(static_cast<int>(result->type), i % 3);
        EXPECT_EQ(result->size, 2048 * (i + 1));
        EXPECT_EQ(result->ttl, 3600 + i * 100);
    }

    // Clear all records
    meta_store_->clear();

    // Wait a bit for clear operation to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify all records are gone
    for (int i = 0; i < num_records; ++i) {
        auto result = meta_store_->get(keys[i]);
        EXPECT_FALSE(result.has_value());
    }

    // Verify range query returns no results
    auto iterator = meta_store_->range_get(1);
    ASSERT_NE(iterator, nullptr) << "Failed to create iterator for tablet 1";
    EXPECT_FALSE(iterator->valid());
}

TEST_F(CacheBlockMetaStoreTest, ClearWithPendingAsyncOperations) {
    // Add some records and immediately call clear
    // This tests that pending operations in the queue are handled correctly

    // Add a record
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);
    BlockMeta meta1(NORMAL, 1024, 3600);
    meta_store_->put(key1, meta1);

    // Immediately clear without waiting for async operation
    meta_store_->clear();

    // Wait a bit for operations to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify the record was not written (cleared from queue)
    auto result = meta_store_->get(key1);
    EXPECT_FALSE(result.has_value());
}

TEST_F(CacheBlockMetaStoreTest, ClearAndThenAddNewRecords) {
    // Test that after clear, the store can accept new records

    // Add initial records
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);
    BlockMeta meta1(NORMAL, 1024, 3600);
    meta_store_->put(key1, meta1);

    // Wait for async operation
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Clear all records
    meta_store_->clear();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Add new records after clear
    uint128_t hash2 = (static_cast<uint128_t>(789) << 64) | 123;
    BlockMetaKey key2(2, UInt128Wrapper(hash2), 1024);
    BlockMeta meta2(INDEX, 2048, 7200);
    meta_store_->put(key2, meta2);

    // Wait for async operation
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify old record is gone
    auto result1 = meta_store_->get(key1);
    EXPECT_FALSE(result1.has_value());

    // Verify new record is present
    auto result2 = meta_store_->get(key2);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->type, INDEX);
    EXPECT_EQ(result2->size, 2048);
    EXPECT_EQ(result2->ttl, 7200);
}

TEST_F(CacheBlockMetaStoreTest, ClearMultipleTimes) {
    // Test that clear can be called multiple times without issues

    // Add a record
    uint128_t hash = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key(1, UInt128Wrapper(hash), 0);
    BlockMeta meta(NORMAL, 1024, 3600);
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
    auto result = meta_store_->get(key);
    EXPECT_FALSE(result.has_value());
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
    auto result = meta_store_->get(key);
    EXPECT_FALSE(result.has_value());
}

TEST_F(CacheBlockMetaStoreTest, GetAllRecords) {
    // Add records from multiple tablets
    const int num_tablets = 3;
    const int blocks_per_tablet = 4;
    int total_records = num_tablets * blocks_per_tablet;

    for (int tablet_id = 1; tablet_id <= num_tablets; ++tablet_id) {
        for (int i = 0; i < blocks_per_tablet; ++i) {
            uint128_t hash =
                    (static_cast<uint128_t>(tablet_id * 100 + i) << 64) | (tablet_id * 200 + i);
            BlockMetaKey key(tablet_id, UInt128Wrapper(hash), i * 1024);
            BlockMeta meta(static_cast<FileCacheType>(i % 2), 2048 * (i + 1), 3600 + i * 100);
            meta_store_->put(key, meta);
        }
    }

    // Wait for async operations
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Test get_all method
    auto iterator = meta_store_->get_all();
    ASSERT_TRUE(iterator != nullptr);

    int count = 0;
    std::set<int64_t> tablet_ids_found;
    std::set<size_t> offsets_found;

    while (iterator->valid()) {
        BlockMetaKey key = iterator->key();
        BlockMeta value = iterator->value();

        // Verify key fields
        EXPECT_GT(key.tablet_id, 0);
        EXPECT_GE(key.offset, 0);
        EXPECT_TRUE(key.hash.value_ > 0);

        // Verify value fields
        EXPECT_TRUE(value.type == 0 || value.type == 1);
        EXPECT_GT(value.size, 0);
        EXPECT_GT(value.ttl, 0);

        // Track what we found
        tablet_ids_found.insert(key.tablet_id);
        offsets_found.insert(key.offset);

        iterator->next();
        count++;
    }

    // Verify we found all records
    EXPECT_EQ(count, total_records);

    // Verify we found records from all tablets
    EXPECT_EQ(tablet_ids_found.size(), num_tablets);
    for (int tablet_id = 1; tablet_id <= num_tablets; ++tablet_id) {
        EXPECT_TRUE(tablet_ids_found.find(tablet_id) != tablet_ids_found.end());
    }

    // Verify we found various offsets
    EXPECT_GE(offsets_found.size(), blocks_per_tablet);
}

TEST_F(CacheBlockMetaStoreTest, GetAllEmptyStore) {
    // Test get_all on empty store
    auto iterator = meta_store_->get_all();
    ASSERT_TRUE(iterator != nullptr);

    // Should be invalid immediately
    EXPECT_FALSE(iterator->valid());

    // Calling next should not crash
    iterator->next();
    EXPECT_FALSE(iterator->valid());
}

TEST_F(CacheBlockMetaStoreTest, GetAllAfterClear) {
    // Add some records
    for (int i = 0; i < 5; ++i) {
        uint128_t hash = (static_cast<uint128_t>(100 + i) << 64) | (200 + i);
        BlockMetaKey key(1, UInt128Wrapper(hash), i * 1024);
        BlockMeta meta(static_cast<FileCacheType>(i % 2), 2048 * (i + 1), 3600 + i * 100);
        meta_store_->put(key, meta);
    }

    // Wait for async operations with more reliable mechanism
    // Check that all records are actually written by querying each one
    int max_retries = 10;
    int successful_checks = 0;
    for (int retry = 0; retry < max_retries; ++retry) {
        successful_checks = 0;
        for (int i = 0; i < 5; ++i) {
            uint128_t hash = (static_cast<uint128_t>(100 + i) << 64) | (200 + i);
            BlockMetaKey key(1, UInt128Wrapper(hash), i * 1024);
            auto result = meta_store_->get(key);
            if (result.has_value()) {
                successful_checks++;
            }
        }
        if (successful_checks == 5) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // Verify all records are present using get_all()
    auto iterator1 = meta_store_->get_all();
    int count_before = 0;
    while (iterator1->valid()) {
        count_before++;
        iterator1->next();
    }
    EXPECT_EQ(count_before, 5) << "Expected 5 records but found " << count_before;

    // Clear all records
    meta_store_->clear();

    // Wait for clear operation to complete with verification
    max_retries = 10;
    for (int retry = 0; retry < max_retries; ++retry) {
        successful_checks = 0;
        for (int i = 0; i < 5; ++i) {
            uint128_t hash = (static_cast<uint128_t>(100 + i) << 64) | (200 + i);
            BlockMetaKey key(1, UInt128Wrapper(hash), i * 1024);
            auto result = meta_store_->get(key);
            if (!result.has_value()) {
                successful_checks++;
            }
        }
        if (successful_checks == 5) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // Verify no records after clear using get_all()
    auto iterator2 = meta_store_->get_all();
    int count_after = 0;
    while (iterator2->valid()) {
        count_after++;
        iterator2->next();
    }
    EXPECT_EQ(count_after, 0) << "Expected 0 records after clear but found " << count_after;
}

TEST_F(CacheBlockMetaStoreTest, GetAllIteratorValidity) {
    // Add multiple records
    for (int i = 0; i < 10; ++i) {
        uint128_t hash = (static_cast<uint128_t>(100 + i) << 64) | (200 + i);
        BlockMetaKey key(1, UInt128Wrapper(hash), i * 1024);
        BlockMeta meta(static_cast<FileCacheType>(i % 3), 2048 * (i + 1), 3600 + i * 100);
        meta_store_->put(key, meta);
    }

    // Wait for async operations
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Test iterator validity and navigation
    auto iterator = meta_store_->get_all();
    ASSERT_TRUE(iterator != nullptr);

    int count = 0;
    while (iterator->valid()) {
        BlockMetaKey key = iterator->key();
        BlockMeta value = iterator->value();

        // Verify consistency between key and value
        EXPECT_GE(key.offset, 0);
        EXPECT_GT(value.size, 0);

        iterator->next();
        ++count;
    }

    EXPECT_EQ(count, 10);
    EXPECT_FALSE(iterator->valid());
}

TEST_F(CacheBlockMetaStoreTest, MultipleOperationsSameKey) {
    uint128_t hash1 = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey key1(1, UInt128Wrapper(hash1), 0);

    // Multiple operations on same key
    BlockMeta meta1(FileCacheType::NORMAL, 1024, 3600);
    BlockMeta meta2(FileCacheType::INDEX, 2048, 7200);
    BlockMeta meta3(FileCacheType::TTL, 4096, 10800);

    // Put first value
    meta_store_->put(key1, meta1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Immediately query - should find first value
    auto result1 = meta_store_->get(key1);
    EXPECT_TRUE(result1.has_value());
    EXPECT_EQ(result1->type, 1);

    // Put second value (should override first in queue)
    meta_store_->put(key1, meta2);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Immediately query - should find second value
    auto result2 = meta_store_->get(key1);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->type, 2);

    // Put third value (should override second in queue)
    meta_store_->put(key1, meta3);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Immediately query - should find third value
    auto result3 = meta_store_->get(key1);
    EXPECT_TRUE(result3.has_value());
    EXPECT_EQ(result3->type, 3);

    // Delete operation (should override all puts in queue)
    meta_store_->delete_key(key1);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Immediately query - should find delete operation
    auto result4 = meta_store_->get(key1);
    EXPECT_FALSE(result4.has_value());
}

TEST_F(CacheBlockMetaStoreTest, ErrorHandling) {
    // Test error handling in deserialization functions

    // Test deserialize_key with invalid data
    Status status;
    auto invalid_key_result = deserialize_key("invalid_key_data", &status);
    EXPECT_FALSE(invalid_key_result.has_value());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Failed to decode") != std::string::npos);

    // Test deserialize_value with invalid data
    auto invalid_value_result = deserialize_value(std::string("invalid_value_data"), &status);
    EXPECT_FALSE(invalid_value_result.has_value());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Failed to deserialize value") != std::string::npos);

    // Test deserialize_value with empty string_view
    std::string_view empty_view;
    auto empty_value_result = deserialize_value(empty_view, &status);
    EXPECT_FALSE(empty_value_result.has_value());
    EXPECT_FALSE(status.ok());

    // Test successful deserialization
    uint128_t hash = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey valid_key(1, UInt128Wrapper(hash), 1024);
    std::string valid_key_str = serialize_key(valid_key);
    auto valid_key_result = deserialize_key(valid_key_str, &status);
    EXPECT_TRUE(valid_key_result.has_value());
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(valid_key_result->tablet_id, 1);
    EXPECT_EQ(valid_key_result->hash, valid_key.hash);
    EXPECT_EQ(valid_key_result->offset, 1024);

    // Test successful value deserialization
    BlockMeta valid_meta(NORMAL, 2048, 3600);
    std::string valid_meta_str = serialize_value(valid_meta);
    auto valid_meta_result = deserialize_value(valid_meta_str, &status);
    EXPECT_TRUE(valid_meta_result.has_value());
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(valid_meta_result->type, NORMAL);
    EXPECT_EQ(valid_meta_result->size, 2048);
    EXPECT_EQ(valid_meta_result->ttl, 3600);
}

TEST_F(CacheBlockMetaStoreTest, IteratorErrorHandling) {
    // Test error handling in iterators by manually inserting corrupted data

    // Create a valid key and meta first
    uint128_t hash = (static_cast<uint128_t>(123) << 64) | 456;
    BlockMetaKey valid_key(1, UInt128Wrapper(hash), 1024);
    BlockMeta valid_meta(NORMAL, 2048, 3600);

    // Put valid data
    meta_store_->put(valid_key, valid_meta);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test range_get iterator error handling
    auto iterator = meta_store_->range_get(1);
    ASSERT_NE(iterator, nullptr);

    // Iterate through valid data first
    int count = 0;
    while (iterator->valid()) {
        (void)iterator->key();   // unused variable
        (void)iterator->value(); // unused variable

        // Check that no errors occurred
        EXPECT_TRUE(iterator->get_last_key_error().ok());
        EXPECT_TRUE(iterator->get_last_value_error().ok());

        iterator->next();
        count++;
    }

    EXPECT_EQ(count, 1);

    // Test get_all iterator
    auto all_iterator = meta_store_->get_all();
    ASSERT_NE(all_iterator, nullptr);

    count = 0;
    while (all_iterator->valid()) {
        (void)all_iterator->key();   // unused variable
        (void)all_iterator->value(); // unused variable

        // Check that no errors occurred
        EXPECT_TRUE(all_iterator->get_last_key_error().ok());
        EXPECT_TRUE(all_iterator->get_last_value_error().ok());

        all_iterator->next();
        count++;
    }

    EXPECT_EQ(count, 1);
}

} // namespace doris::io