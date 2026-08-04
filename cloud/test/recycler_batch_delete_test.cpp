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
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/simple_thread_pool.h"
#include "cpp/client/obj_storage_client.h"
#include "recycler/s3_accessor.h"

namespace doris {
namespace {

class MockObjStorageBackend final : public ObjStorageBackend {
public:
    MockObjStorageBackend(std::vector<ObjectMeta> objects, size_t batch_size,
                          int iterator_fail_after = -1)
            : objects_(std::move(objects)),
              batch_size_(batch_size),
              iterator_fail_after_(iterator_fail_after) {}

    ObjectStorageUploadResponse create_multipart_upload(const ObjectStoragePathOptions&) override {
        return {.resp = ObjectStorageResponse::OK()};
    }
    ObjectStorageResponse put_object(const ObjectStoragePathOptions&, std::string_view) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions&, std::string_view,
                                            int) override {
        return {.resp = ObjectStorageResponse::OK()};
    }
    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions&, const std::vector<ObjectCompleteMultiPart>&) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions&) override {
        return {.resp = ObjectStorageResponse::OK()};
    }
    ObjectStorageResponse get_object(const ObjectStoragePathOptions&, void*, size_t, size_t,
                                     size_t*) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageListPage list_objects(const ObjectStoragePathOptions&,
                                       std::string_view continuation_token) override {
        const size_t index =
                continuation_token.empty()
                        ? 0
                        : static_cast<size_t>(std::stoull(std::string(continuation_token)));
        if (iterator_fail_after_ >= 0 && index >= static_cast<size_t>(iterator_fail_after_)) {
            return {.resp = {.status = {TStatusCode::INTERNAL_ERROR, "simulated list failure"}}};
        }
        ObjectStorageListPage page {.resp = ObjectStorageResponse::OK()};
        if (index < objects_.size()) {
            page.objects.emplace_back(objects_[index]);
            page.has_more = index + 1 < objects_.size();
            if (page.has_more) {
                page.continuation_token = std::to_string(index + 1);
            }
        }
        return page;
    }
    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions&,
                                         std::vector<std::string> keys) override {
        const int call = delete_calls_.fetch_add(1);
        if (fail_delete_after_.load() >= 0 && call >= fail_delete_after_.load()) {
            return {.status = {TStatusCode::INTERNAL_ERROR, "simulated delete failure"}};
        }
        std::lock_guard lock(deleted_keys_mutex_);
        deleted_keys_.insert(deleted_keys_.end(), keys.begin(), keys.end());
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_object(const ObjectStoragePathOptions&) override {
        return ObjectStorageResponse::OK();
    }
    std::string generate_presigned_url(const ObjectStoragePathOptions&, int64_t) override {
        return {};
    }
    ObjStorageCapabilities capabilities() const override {
        return {.max_delete_batch = batch_size_};
    }

    int delete_calls() const { return delete_calls_.load(); }
    const std::vector<std::string>& deleted_keys() const { return deleted_keys_; }
    void fail_delete() { fail_delete_after_ = 0; }

private:
    std::vector<ObjectMeta> objects_;
    size_t batch_size_;
    int iterator_fail_after_;
    std::atomic<int> delete_calls_ {0};
    std::atomic<int> fail_delete_after_ {-1};
    std::mutex deleted_keys_mutex_;
    std::vector<std::string> deleted_keys_;
};

class TestS3Accessor final : public cloud::S3Accessor {
public:
    using cloud::S3Accessor::make_recursive_delete_options;
};

class ScopedMaxTasksPerBatch {
public:
    explicit ScopedMaxTasksPerBatch(int32_t value)
            : original_(cloud::config::recycler_max_tasks_per_batch) {
        cloud::config::recycler_max_tasks_per_batch = value;
    }
    ~ScopedMaxTasksPerBatch() { cloud::config::recycler_max_tasks_per_batch = original_; }

private:
    int32_t original_;
};

class CountingRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    CountingRateLimitPolicy(size_t* get_requests, size_t* put_requests)
            : get_requests_(get_requests), put_requests_(put_requests) {}

    ObjStorageRateLimitToken acquire(ObjStorageRequestType type, size_t) const override {
        if (type == ObjStorageRequestType::GET) {
            ++*get_requests_;
        } else {
            ++*put_requests_;
        }
        return {};
    }

private:
    size_t* get_requests_;
    size_t* put_requests_;
};

std::vector<ObjectMeta> make_objects(size_t count) {
    std::vector<ObjectMeta> objects;
    for (size_t i = 0; i < count; ++i) {
        objects.push_back({
                .file_path = "test_key_" + std::to_string(i),
                .size = 100,
                .mtime_s = static_cast<int64_t>(i),
        });
    }
    return objects;
}

TEST(RecyclerBatchDeleteTest, UsesProviderBatchCapability) {
    auto backend = std::make_shared<MockObjStorageBackend>(make_objects(10), 3);
    ObjStorageClient client(backend);
    auto response = client.delete_objects_recursively({.bucket = "bucket", .prefix = "test_key_"});

    EXPECT_TRUE(response.ok());
    EXPECT_EQ(backend->delete_calls(), 4);
    EXPECT_EQ(backend->deleted_keys().size(), 10);
}

TEST(RecyclerBatchDeleteTest, CountsEveryDeleteObjectsBatch) {
    auto backend = std::make_shared<MockObjStorageBackend>(std::vector<ObjectMeta> {}, 3);
    size_t get_requests = 0;
    size_t put_requests = 0;
    ObjStorageClient client(
            backend, std::make_shared<CountingRateLimitPolicy>(&get_requests, &put_requests));

    std::vector<std::string> keys(7, "key");
    auto response = client.delete_objects({.bucket = "bucket"}, std::move(keys));

    EXPECT_TRUE(response.ok());
    EXPECT_EQ(get_requests, 0);
    EXPECT_EQ(put_requests, 3);
    EXPECT_EQ(backend->delete_calls(), 3);
}

TEST(RecyclerBatchDeleteTest, CountsEveryRecursiveListAndDeleteRequest) {
    auto backend = std::make_shared<MockObjStorageBackend>(make_objects(5), 2);
    size_t get_requests = 0;
    size_t put_requests = 0;
    ObjStorageClient client(
            backend, std::make_shared<CountingRateLimitPolicy>(&get_requests, &put_requests));

    auto response = client.delete_objects_recursively({.bucket = "bucket", .prefix = "test_key_"});

    EXPECT_TRUE(response.ok());
    EXPECT_EQ(get_requests, 5);
    EXPECT_EQ(put_requests, 3);
    EXPECT_EQ(backend->delete_calls(), 3);
}

TEST(RecyclerBatchDeleteTest, ProductionExecutorRunsMultipleTaskBatches) {
    ScopedMaxTasksPerBatch max_tasks_per_batch(2);
    auto pool = std::make_shared<cloud::SimpleThreadPool>(4, "recursive_delete_test");
    ASSERT_EQ(pool->start(), 0);

    auto options = TestS3Accessor::make_recursive_delete_options(0, pool);
    size_t executor_batches = 0;
    auto production_executor = std::move(options.executor);
    options.executor = [&executor_batches, production_executor = std::move(production_executor)](
                               std::vector<ObjStorageDeleteTask> tasks) mutable {
        ++executor_batches;
        return production_executor(std::move(tasks));
    };

    auto backend = std::make_shared<MockObjStorageBackend>(make_objects(10), 2);
    ObjStorageClient client(backend);
    auto response =
            client.delete_objects_recursively({.bucket = "bucket", .prefix = "test_key_"}, options);

    EXPECT_TRUE(response.ok());
    EXPECT_EQ(executor_batches, 3);
    EXPECT_EQ(backend->delete_calls(), 5);
    EXPECT_EQ(backend->deleted_keys().size(), 10);
    EXPECT_EQ(pool->stop(), 0);
}

TEST(RecyclerBatchDeleteTest, ProductionExecutorPropagatesCancellation) {
    ScopedMaxTasksPerBatch max_tasks_per_batch(3);
    auto pool = std::make_shared<cloud::SimpleThreadPool>(1, "recursive_delete_failure_test");
    ASSERT_EQ(pool->start(), 0);

    auto backend = std::make_shared<MockObjStorageBackend>(make_objects(6), 1);
    backend->fail_delete();
    ObjStorageClient client(backend);
    auto response = client.delete_objects_recursively(
            {.bucket = "bucket", .prefix = "test_key_"},
            TestS3Accessor::make_recursive_delete_options(0, pool));

    EXPECT_FALSE(response.ok());
    EXPECT_EQ(response.status.msg, "object storage batch deletion did not finish");
    EXPECT_EQ(backend->delete_calls(), 1);
    EXPECT_EQ(pool->stop(), 0);
}

TEST(RecyclerBatchDeleteTest, InvalidMaxTasksPerBatchUsesDefault) {
    auto pool = std::make_shared<cloud::SimpleThreadPool>(1, "recursive_delete_config_test");
    {
        ScopedMaxTasksPerBatch max_tasks_per_batch(0);
        auto options = TestS3Accessor::make_recursive_delete_options(0, pool);
        EXPECT_EQ(options.max_tasks_per_batch, 1000);
    }
    {
        ScopedMaxTasksPerBatch max_tasks_per_batch(-1);
        auto options = TestS3Accessor::make_recursive_delete_options(0, pool);
        EXPECT_EQ(options.max_tasks_per_batch, 1000);
    }
}

TEST(RecyclerBatchDeleteTest, FiltersByExpirationTime) {
    auto backend = std::make_shared<MockObjStorageBackend>(make_objects(10), 1000);
    ObjStorageClient client(backend);
    auto response = client.delete_objects_recursively({.bucket = "bucket", .prefix = "test_key_"},
                                                      {.expiration_time = 4});

    EXPECT_TRUE(response.ok());
    ASSERT_EQ(backend->deleted_keys().size(), 5);
    EXPECT_EQ(backend->deleted_keys().back(), "test_key_4");
}

TEST(RecyclerBatchDeleteTest, PropagatesListAndDeleteFailures) {
    auto list_failure_backend = std::make_shared<MockObjStorageBackend>(make_objects(10), 3, 2);
    ObjStorageClient list_failure(list_failure_backend);
    EXPECT_FALSE(
            list_failure.delete_objects_recursively({.bucket = "bucket", .prefix = "test_key_"})
                    .ok());

    auto delete_failure_backend = std::make_shared<MockObjStorageBackend>(make_objects(3), 3);
    delete_failure_backend->fail_delete();
    ObjStorageClient delete_failure(delete_failure_backend);
    EXPECT_FALSE(
            delete_failure.delete_objects_recursively({.bucket = "bucket", .prefix = "test_key_"})
                    .ok());
}

} // namespace
} // namespace doris
