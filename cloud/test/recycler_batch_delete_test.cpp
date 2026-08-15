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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/simple_thread_pool.h"
#include "cpp/obj-client/obj_storage_client.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "recycler/s3_accessor.h"

namespace doris {
namespace {

class MockObjStorageClient final : public ObjStorageClient {
public:
    MockObjStorageClient(std::vector<ObjectMeta> objects, size_t batch_size,
                         int iterator_fail_after = -1)
            : objects_(std::move(objects)),
              batch_size_(batch_size),
              iterator_fail_after_(iterator_fail_after) {}

    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath&) override {
        return {.resp = ObjStorageResponse::OK()};
    }
    ObjStorageResponse put_object(const ObjStoragePath&, std::string_view) override {
        return ObjStorageResponse::OK();
    }
    ObjStorageUploadResult upload_part(const ObjStoragePath&, const std::string&, std::string_view,
                                       int) override {
        return {.resp = ObjStorageResponse::OK()};
    }
    ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath&, const std::string&,
            const std::vector<ObjStorageCompletedPart>&) override {
        return ObjStorageResponse::OK();
    }
    ObjStorageHeadResult head_object(const ObjStoragePath&) override {
        return {.resp = ObjStorageResponse::OK()};
    }
    ObjStorageResponse get_object(const ObjStoragePath&, void*, size_t, size_t, size_t*) override {
        return ObjStorageResponse::OK();
    }
    ObjStorageListPageResult list_objects_page(const ObjStoragePath&,
                                               std::string_view continuation_token) override {
        ++list_calls_;
        const size_t index =
                continuation_token.empty()
                        ? 0
                        : static_cast<size_t>(std::stoull(std::string(continuation_token)));
        if (iterator_fail_after_ >= 0 && index >= static_cast<size_t>(iterator_fail_after_)) {
            return {.resp = {.status = {TStatusCode::INTERNAL_ERROR, "simulated list failure"}}};
        }
        ObjStorageListPageResult page {.resp = ObjStorageResponse::OK()};
        if (index < objects_.size()) {
            page.objects.emplace_back(objects_[index]);
            page.has_more = index + 1 < objects_.size();
            if (page.has_more) {
                page.continuation_token = std::to_string(index + 1);
            }
        }
        return page;
    }
    ObjStorageResponse delete_objects(const ObjStoragePath&,
                                      std::vector<std::string> keys) override {
        const int call = delete_calls_.fetch_add(1);
        if (fail_delete_after_.load() >= 0 && call >= fail_delete_after_.load()) {
            return {.status = {TStatusCode::INTERNAL_ERROR, "simulated delete failure"}};
        }
        std::lock_guard lock(deleted_keys_mutex_);
        deleted_keys_.insert(deleted_keys_.end(), keys.begin(), keys.end());
        return ObjStorageResponse::OK();
    }
    ObjStorageResponse delete_object(const ObjStoragePath&) override {
        return ObjStorageResponse::OK();
    }
    std::string generate_presigned_url(const ObjStoragePath&, int64_t) override { return {}; }
    ObjStorageCapabilities capabilities() const override {
        return {.max_delete_batch = batch_size_};
    }
    ObjStorageResponse get_lifecycle(const std::string&, int64_t*) override {
        return not_supported();
    }
    ObjStorageResponse check_versioning(const std::string&) override { return not_supported(); }
    ObjStorageResponse abort_multipart_upload(const ObjStoragePath&, const std::string&) override {
        return not_supported();
    }

    int delete_calls() const { return delete_calls_.load(); }
    int list_calls() const { return list_calls_.load(); }
    const std::vector<std::string>& deleted_keys() const { return deleted_keys_; }
    void fail_delete() { fail_delete_after_ = 0; }

private:
    static ObjStorageResponse not_supported() {
        return {
                .status = {TStatusCode::NOT_IMPLEMENTED_ERROR,
                           "operation is not supported by the recycler test client"},
                .http_code = 0,
        };
    }

    std::vector<ObjectMeta> objects_;
    size_t batch_size_;
    int iterator_fail_after_;
    std::atomic<int> list_calls_ {0};
    std::atomic<int> delete_calls_ {0};
    std::atomic<int> fail_delete_after_ {-1};
    std::mutex deleted_keys_mutex_;
    std::vector<std::string> deleted_keys_;
};

class CountingDeleteExecutor final : public ObjStorageDeleteExecutor {
public:
    CountingDeleteExecutor(std::shared_ptr<ObjStorageDeleteExecutor> inner, size_t* wait_calls)
            : inner_(std::move(inner)), wait_calls_(wait_calls) {}

    ObjStorageResponse submit(ObjStorageDeleteTask task) override {
        return inner_->submit(std::move(task));
    }

    ObjStorageResponse wait() override {
        ++*wait_calls_;
        return inner_->wait();
    }

private:
    std::shared_ptr<ObjStorageDeleteExecutor> inner_;
    size_t* wait_calls_;
};

class StreamingDeleteExecutor final : public ObjStorageDeleteExecutor {
public:
    StreamingDeleteExecutor(std::shared_ptr<MockObjStorageClient> client,
                            std::vector<int>* list_calls_at_submit)
            : client_(std::move(client)), list_calls_at_submit_(list_calls_at_submit) {}

    ObjStorageResponse submit(ObjStorageDeleteTask task) override {
        list_calls_at_submit_->push_back(client_->list_calls());
        return task();
    }

    ObjStorageResponse wait() override { return ObjStorageResponse::OK(); }

private:
    std::shared_ptr<MockObjStorageClient> client_;
    std::vector<int>* list_calls_at_submit_;
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

class CapturingLogSink final : public google::LogSink {
public:
    void send(google::LogSeverity, const char*, const char*, int, const google::LogMessageTime&,
              const char* message, std::size_t message_len) override {
        std::lock_guard lock(mutex_);
        messages_.emplace_back(message, message_len);
    }

    bool contains(std::string_view text) const {
        std::lock_guard lock(mutex_);
        for (const auto& message : messages_) {
            if (message.find(text) != std::string::npos) {
                return true;
            }
        }
        return false;
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::string> messages_;
};

class ScopedLogSink final {
public:
    explicit ScopedLogSink(google::LogSink* sink) : sink_(sink) { google::AddLogSink(sink_); }
    ~ScopedLogSink() { google::RemoveLogSink(sink_); }

private:
    google::LogSink* sink_;
};

class CountingRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    CountingRateLimitPolicy(size_t* get_requests, size_t* put_requests)
            : get_requests_(get_requests), put_requests_(put_requests) {}

    ObjStorageAdmission acquire(S3RateLimitType type, size_t) const override {
        if (type == S3RateLimitType::GET) {
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
                .key = "test_key_" + std::to_string(i),
                .size = 100,
                .mtime_s = static_cast<int64_t>(i),
        });
    }
    return objects;
}

TEST(RecyclerBatchDeleteTest, UsesProviderBatchCapability) {
    auto provider_client = std::make_shared<MockObjStorageClient>(make_objects(10), 3);
    auto response = delete_objects_recursively(provider_client,
                                               {.bucket = "bucket", .prefix = "test_key_"});

    EXPECT_TRUE(response.ok());
    EXPECT_EQ(provider_client->delete_calls(), 4);
    EXPECT_EQ(provider_client->deleted_keys().size(), 10);
}

TEST(RecyclerBatchDeleteTest, CountsDeleteObjectsOperationOnce) {
    auto inner_client = std::make_shared<MockObjStorageClient>(std::vector<ObjectMeta> {}, 3);
    size_t get_requests = 0;
    size_t put_requests = 0;
    RateLimitedObjStorageClient client(
            inner_client, std::make_shared<CountingRateLimitPolicy>(&get_requests, &put_requests));

    std::vector<std::string> keys(7, "key");
    auto response = client.delete_objects({.bucket = "bucket"}, std::move(keys));

    EXPECT_TRUE(response.ok());
    EXPECT_EQ(get_requests, 0);
    EXPECT_EQ(put_requests, 1);
    EXPECT_EQ(inner_client->delete_calls(), 1);
}

TEST(RecyclerBatchDeleteTest, RateLimitsEveryRecursiveDeleteRequest) {
    auto inner_client = std::make_shared<MockObjStorageClient>(make_objects(5), 2);
    size_t get_requests = 0;
    size_t put_requests = 0;
    auto client = std::make_shared<RateLimitedObjStorageClient>(
            inner_client, std::make_shared<CountingRateLimitPolicy>(&get_requests, &put_requests));

    auto response = delete_objects_recursively(client, {.bucket = "bucket", .prefix = "test_key_"});

    EXPECT_TRUE(response.ok());
    EXPECT_EQ(get_requests, 5);
    EXPECT_EQ(put_requests, 3);
    EXPECT_EQ(inner_client->list_calls(), 5);
    EXPECT_EQ(inner_client->delete_calls(), 3);
}

TEST(RecyclerBatchDeleteTest, ProductionExecutorRunsMultipleTaskBatches) {
    ScopedMaxTasksPerBatch max_tasks_per_batch(2);
    auto pool = std::make_shared<cloud::SimpleThreadPool>(4, "recursive_delete_test");
    ASSERT_EQ(pool->start(), 0);

    auto options = TestS3Accessor::make_recursive_delete_options(0, pool);
    size_t executor_batches = 0;
    options.executor =
            std::make_shared<CountingDeleteExecutor>(options.executor, &executor_batches);

    auto provider_client = std::make_shared<MockObjStorageClient>(make_objects(10), 2);
    auto response = delete_objects_recursively(
            provider_client, {.bucket = "bucket", .prefix = "test_key_"}, options);

    EXPECT_TRUE(response.ok());
    EXPECT_EQ(executor_batches, 3);
    EXPECT_EQ(provider_client->delete_calls(), 5);
    EXPECT_EQ(provider_client->deleted_keys().size(), 10);
    EXPECT_EQ(pool->stop(), 0);
}

TEST(RecyclerBatchDeleteTest, LogsBatchProgressAndFinalSummary) {
    CapturingLogSink logs;
    ScopedLogSink scoped_log_sink(&logs);
    auto provider_client = std::make_shared<MockObjStorageClient>(make_objects(5), 2);

    auto response =
            delete_objects_recursively(provider_client, {.bucket = "bucket", .prefix = "test_key_"},
                                       {.max_tasks_per_batch = 2});

    EXPECT_TRUE(response.ok());
    EXPECT_TRUE(
            logs.contains("delete objects under bucket/test_key_ batch 1 completed, "
                          "tasks_in_batch=2, total_deleted=4"));
    EXPECT_TRUE(
            logs.contains("delete objects under bucket/test_key_ batch 2 completed, "
                          "tasks_in_batch=1, total_deleted=5"));
    EXPECT_TRUE(
            logs.contains("delete objects under bucket/test_key_ finished, ret=0, "
                          "total_batches=2, num_deleted=5, error_count=0"));
}

TEST(RecyclerBatchDeleteTest, StreamsDeleteTasksWhileListing) {
    auto provider_client = std::make_shared<MockObjStorageClient>(make_objects(5), 1);
    std::vector<int> list_calls_at_submit;
    ObjStorageRecursiveDeleteOptions options {
            .max_tasks_per_batch = 1000,
            .executor = std::make_shared<StreamingDeleteExecutor>(provider_client,
                                                                  &list_calls_at_submit),
    };

    auto response = delete_objects_recursively(
            provider_client, {.bucket = "bucket", .prefix = "test_key_"}, options);

    EXPECT_TRUE(response.ok());
    ASSERT_EQ(list_calls_at_submit.size(), 5);
    EXPECT_EQ(list_calls_at_submit.front(), 1);
    EXPECT_EQ(provider_client->list_calls(), 5);
    EXPECT_EQ(provider_client->deleted_keys().size(), 5);
}

TEST(RecyclerBatchDeleteTest, ProductionExecutorStopsListingAfterCancellation) {
    ScopedMaxTasksPerBatch max_tasks_per_batch(3);
    auto pool = std::make_shared<cloud::SimpleThreadPool>(1, "recursive_delete_failure_test");
    ASSERT_EQ(pool->start(), 0);

    auto provider_client = std::make_shared<MockObjStorageClient>(make_objects(6), 1);
    provider_client->fail_delete();
    auto response =
            delete_objects_recursively(provider_client, {.bucket = "bucket", .prefix = "test_key_"},
                                       TestS3Accessor::make_recursive_delete_options(0, pool));

    EXPECT_FALSE(response.ok());
    EXPECT_EQ(response.status.msg, "object storage batch deletion did not finish");
    EXPECT_EQ(provider_client->list_calls(), 3);
    EXPECT_EQ(provider_client->delete_calls(), 1);
    EXPECT_EQ(pool->stop(), 0);
}

TEST(RecyclerBatchDeleteTest, InvalidMaxTasksPerBatchUsesDefault) {
    CapturingLogSink logs;
    ScopedLogSink scoped_log_sink(&logs);
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
    EXPECT_TRUE(
            logs.contains("recycler_max_tasks_per_batch=0 is not positive, using default 1000"));
    EXPECT_TRUE(
            logs.contains("recycler_max_tasks_per_batch=-1 is not positive, using default 1000"));
}

TEST(RecyclerBatchDeleteTest, FiltersByExpirationTime) {
    auto provider_client = std::make_shared<MockObjStorageClient>(make_objects(10), 1000);
    auto response = delete_objects_recursively(
            provider_client, {.bucket = "bucket", .prefix = "test_key_"}, {.expiration_time = 4});

    EXPECT_TRUE(response.ok());
    ASSERT_EQ(provider_client->deleted_keys().size(), 5);
    EXPECT_EQ(provider_client->deleted_keys().back(), "test_key_4");
}

TEST(RecyclerBatchDeleteTest, PropagatesListAndDeleteFailures) {
    CapturingLogSink logs;
    ScopedLogSink scoped_log_sink(&logs);
    auto list_failure_provider_client =
            std::make_shared<MockObjStorageClient>(make_objects(10), 3, 2);
    EXPECT_FALSE(delete_objects_recursively(list_failure_provider_client,
                                            {.bucket = "bucket", .prefix = "list_failure"})
                         .ok());

    auto delete_failure_provider_client =
            std::make_shared<MockObjStorageClient>(make_objects(7), 3);
    delete_failure_provider_client->fail_delete();
    auto delete_response = delete_objects_recursively(
            delete_failure_provider_client, {.bucket = "bucket", .prefix = "delete_failure"});
    EXPECT_FALSE(delete_response.ok());
    EXPECT_EQ(delete_response.status.msg, "simulated delete failure");
    EXPECT_EQ(delete_failure_provider_client->list_calls(), 3);
    EXPECT_EQ(delete_failure_provider_client->delete_calls(), 1);
    EXPECT_TRUE(logs.contains("delete objects under bucket/list_failure finished, ret="));
    EXPECT_TRUE(logs.contains("delete objects under bucket/delete_failure finished, ret="));
    EXPECT_TRUE(logs.contains("total_batches=1, num_deleted=3, error_count=1"));
}

} // namespace
} // namespace doris
