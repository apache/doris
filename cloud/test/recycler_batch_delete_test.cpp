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

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "recycler/obj_storage_client.h"

using namespace doris;

namespace doris::cloud {

// Mock ObjectListIterator for testing
class MockObjectListIterator : public ObjectListIterator {
public:
    MockObjectListIterator(std::vector<ObjectMeta> objects, int fail_after = -1)
            : objects_(std::move(objects)), fail_after_(fail_after) {}

    bool is_valid() override { return is_valid_; }

    bool has_next() override {
        if (!is_valid_) return false;
        return current_index_ < objects_.size();
    }

    std::optional<ObjectMeta> next() override {
        if (!is_valid_ || current_index_ >= objects_.size()) {
            return std::nullopt;
        }

        // Simulate iterator becoming invalid after certain number of calls
        if (fail_after_ >= 0 && static_cast<int>(current_index_) >= fail_after_) {
            is_valid_ = false;
            return std::nullopt;
        }

        return objects_[current_index_++];
    }

    void set_invalid() { is_valid_ = false; }

private:
    std::vector<ObjectMeta> objects_;
    size_t current_index_ = 0;
    bool is_valid_ = true;
    int fail_after_ = -1; // -1 means never fail
};

// Mock ObjStorageClient for testing delete_objects_recursively_
class MockObjStorageClient : public ObjStorageClient {
public:
    MockObjStorageClient(std::vector<ObjectMeta> objects, int iterator_fail_after = -1)
            : objects_(std::move(objects)), iterator_fail_after_(iterator_fail_after) {}

    ObjectStorageResponse put_object(ObjectStoragePathRef path, std::string_view stream) override {
        return {0};
    }

    ObjectStorageResponse head_object(ObjectStoragePathRef path, ObjectMeta* res) override {
        return {0};
    }

    std::unique_ptr<ObjectListIterator> list_objects(ObjectStoragePathRef path) override {
        return std::make_unique<MockObjectListIterator>(objects_, iterator_fail_after_);
    }

    ObjectStorageResponse delete_objects(const std::string& bucket, std::vector<std::string> keys,
                                         ObjClientOptions option) override {
        delete_calls_++;
        total_keys_deleted_ += keys.size();

        // Simulate delete failure if configured
        if (fail_delete_after_ >= 0 && delete_calls_ > fail_delete_after_) {
            return {-1, "simulated delete failure"};
        }

        return {0};
    }

    ObjectStorageResponse delete_object(ObjectStoragePathRef path) override { return {0}; }

    ObjectStorageResponse delete_objects_recursively(ObjectStoragePathRef path,
                                                     ObjClientOptions option,
                                                     int64_t expiration_time = 0) override {
        return delete_objects_recursively_(path, option, expiration_time, 1000);
    }

    ObjectStorageResponse get_life_cycle(const std::string& bucket,
                                         int64_t* expiration_days) override {
        return {0};
    }

    ObjectStorageResponse check_versioning(const std::string& bucket) override { return {0}; }

    ObjectStorageResponse abort_multipart_upload(ObjectStoragePathRef path,
                                                 const std::string& upload_id) override {
        return {0};
    }

    // Test helper methods
    int get_delete_calls() const { return delete_calls_; }
    size_t get_total_keys_deleted() const { return total_keys_deleted_; }
    void set_fail_delete_after(int n) { fail_delete_after_ = n; }

private:
    std::vector<ObjectMeta> objects_;
    int iterator_fail_after_ = -1;
    std::atomic<int> delete_calls_ {0};
    std::atomic<size_t> total_keys_deleted_ {0};
    int fail_delete_after_ = -1; // -1 means never fail
};

class RecyclerBatchDeleteTest : public testing::Test {
protected:
    void SetUp() override {
        thread_pool_ = std::make_shared<SimpleThreadPool>(4);
        thread_pool_->start();
    }

    void TearDown() override {
        if (thread_pool_) {
            thread_pool_->stop();
        }
    }

    std::vector<ObjectMeta> generate_objects(size_t count) {
        std::vector<ObjectMeta> objects;
        objects.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            objects.push_back(ObjectMeta {
                    .key = "test_key_" + std::to_string(i),
                    .size = 100,
                    .mtime_s = 0,
            });
        }
        return objects;
    }

    std::shared_ptr<SimpleThreadPool> thread_pool_;
};

// Test 1: Basic batch processing with multiple batches
TEST_F(RecyclerBatchDeleteTest, MultipleBatches) {
    // Save original config and set small batch size for testing
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 3; // 3 tasks per batch

    // Create 10 objects, with batch_size=2 (keys per task), max_tasks_per_batch=3
    // Expected: 10 objects / 2 keys per task = 5 tasks
    // 5 tasks / 3 tasks per batch = 2 batches (3 tasks + 2 tasks)
    auto objects = generate_objects(10);
    MockObjStorageClient client(objects);

    ObjClientOptions options;
    options.executor = thread_pool_;

    // Use batch_size=2 to create more tasks
    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 2);

    EXPECT_EQ(response.ret, 0);
    EXPECT_EQ(client.get_delete_calls(), 5);        // 10 objects / 2 = 5 delete calls
    EXPECT_EQ(client.get_total_keys_deleted(), 10); // All 10 keys deleted

    // Restore config
    config::recycler_max_tasks_per_batch = original_config;
}

// Test 2: Iterator becomes invalid during iteration
TEST_F(RecyclerBatchDeleteTest, IteratorInvalidMidway) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 100;

    // Create 20 objects but iterator fails after 10
    auto objects = generate_objects(20);
    MockObjStorageClient client(objects, 10); // fail_after=10

    ObjClientOptions options;
    options.executor = thread_pool_;

    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 5);

    // Should return error because iterator became invalid
    EXPECT_EQ(response.ret, -1);
    // Should have processed some objects before failure
    EXPECT_GT(client.get_total_keys_deleted(), 0);
    EXPECT_LT(client.get_total_keys_deleted(), 20);

    config::recycler_max_tasks_per_batch = original_config;
}

// Test 3: Delete operation fails (triggers cancel)
TEST_F(RecyclerBatchDeleteTest, DeleteFailureTriggersCancel) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 10;

    auto objects = generate_objects(30);
    MockObjStorageClient client(objects);
    client.set_fail_delete_after(2); // Fail after 2 successful deletes

    ObjClientOptions options;
    options.executor = thread_pool_;

    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 5);

    // Should return error because delete failed
    EXPECT_EQ(response.ret, -1);

    config::recycler_max_tasks_per_batch = original_config;
}

// Test 4: Empty object list
TEST_F(RecyclerBatchDeleteTest, EmptyObjectList) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 100;

    std::vector<ObjectMeta> empty_objects;
    MockObjStorageClient client(empty_objects);

    ObjClientOptions options;
    options.executor = thread_pool_;

    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 1000);

    EXPECT_EQ(response.ret, 0);
    EXPECT_EQ(client.get_delete_calls(), 0);
    EXPECT_EQ(client.get_total_keys_deleted(), 0);

    config::recycler_max_tasks_per_batch = original_config;
}

// Test 5: Objects less than batch_size
TEST_F(RecyclerBatchDeleteTest, ObjectsLessThanBatchSize) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 100;

    auto objects = generate_objects(5);
    MockObjStorageClient client(objects);

    ObjClientOptions options;
    options.executor = thread_pool_;

    // batch_size=1000, but only 5 objects
    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 1000);

    EXPECT_EQ(response.ret, 0);
    EXPECT_EQ(client.get_delete_calls(), 1); // All 5 keys in one delete call
    EXPECT_EQ(client.get_total_keys_deleted(), 5);

    config::recycler_max_tasks_per_batch = original_config;
}

// Test 6: Exact batch boundary
TEST_F(RecyclerBatchDeleteTest, ExactBatchBoundary) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 2; // 2 tasks per batch

    // 8 objects with batch_size=2 = 4 tasks
    // 4 tasks with max_tasks_per_batch=2 = exactly 2 batches
    auto objects = generate_objects(8);
    MockObjStorageClient client(objects);

    ObjClientOptions options;
    options.executor = thread_pool_;

    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 2);

    EXPECT_EQ(response.ret, 0);
    EXPECT_EQ(client.get_delete_calls(), 4); // 8 / 2 = 4 tasks
    EXPECT_EQ(client.get_total_keys_deleted(), 8);

    config::recycler_max_tasks_per_batch = original_config;
}

// Test 7: Invalid config value (negative)
TEST_F(RecyclerBatchDeleteTest, InvalidConfigNegative) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = -1; // Invalid negative value

    auto objects = generate_objects(10);
    MockObjStorageClient client(objects);

    ObjClientOptions options;
    options.executor = thread_pool_;

    // Should use default value 1000 and still work
    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 5);

    EXPECT_EQ(response.ret, 0);
    EXPECT_EQ(client.get_total_keys_deleted(), 10);

    config::recycler_max_tasks_per_batch = original_config;
}

// Test 8: Invalid config value (zero)
TEST_F(RecyclerBatchDeleteTest, InvalidConfigZero) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 0; // Invalid zero value

    auto objects = generate_objects(10);
    MockObjStorageClient client(objects);

    ObjClientOptions options;
    options.executor = thread_pool_;

    // Should use default value 1000 and still work
    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 5);

    EXPECT_EQ(response.ret, 0);
    EXPECT_EQ(client.get_total_keys_deleted(), 10);

    config::recycler_max_tasks_per_batch = original_config;
}

// Test 9: Expiration time filtering
TEST_F(RecyclerBatchDeleteTest, ExpirationTimeFiltering) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 100;

    std::vector<ObjectMeta> objects;
    // Create 10 objects: 5 with old mtime (should be deleted), 5 with new mtime (should be kept)
    for (int i = 0; i < 5; ++i) {
        objects.push_back(ObjectMeta {
                .key = "old_key_" + std::to_string(i),
                .size = 100,
                .mtime_s = 100, // Old timestamp
        });
    }
    for (int i = 0; i < 5; ++i) {
        objects.push_back(ObjectMeta {
                .key = "new_key_" + std::to_string(i),
                .size = 100,
                .mtime_s = 1000, // New timestamp
        });
    }

    MockObjStorageClient client(objects);

    ObjClientOptions options;
    options.executor = thread_pool_;

    // Set expiration_time=500, so only objects with mtime_s <= 500 should be deleted
    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 500, 1000);

    EXPECT_EQ(response.ret, 0);
    EXPECT_EQ(client.get_total_keys_deleted(), 5); // Only old objects deleted

    config::recycler_max_tasks_per_batch = original_config;
}

// Test 10: Iterator invalid at start (empty batch scenario)
TEST_F(RecyclerBatchDeleteTest, IteratorInvalidAtStart) {
    int32_t original_config = config::recycler_max_tasks_per_batch;
    config::recycler_max_tasks_per_batch = 100;

    // Iterator fails immediately (fail_after=0)
    auto objects = generate_objects(10);
    MockObjStorageClient client(objects, 0);

    ObjClientOptions options;
    options.executor = thread_pool_;

    auto response = client.delete_objects_recursively_(
            {.bucket = "test_bucket", .key = "test_prefix"}, options, 0, 5);

    // Should return error because iterator was invalid from the start
    EXPECT_EQ(response.ret, -1);
    EXPECT_EQ(client.get_delete_calls(), 0);

    config::recycler_max_tasks_per_batch = original_config;
}

} // namespace doris::cloud
