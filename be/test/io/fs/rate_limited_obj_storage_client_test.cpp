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

#include "io/fs/rate_limited_obj_storage_client.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "util/s3_rate_limiter_manager.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {

constexpr size_t kNoThrottleBytesPerSecond = 1ULL << 40;

// Provider-free fake: counts calls and reports a configurable read size.
class FakeObjStorageClient : public ObjStorageClient {
public:
    ObjectStorageUploadResponse create_multipart_upload(
            const ObjectStoragePathOptions& opts) override {
        ++calls;
        ++create_multipart_upload_calls;
        create_multipart_upload_provider_calls +=
                create_multipart_upload_provider_calls_per_logical_call;
        return {};
    }
    ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                     std::string_view stream) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }
    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions& opts,
                                            std::string_view stream, int part_num) override {
        ++calls;
        return {};
    }
    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const std::vector<ObjectCompleteMultiPart>& completed_parts) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }
    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts) override {
        ++calls;
        return {};
    }
    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read,
                                     size_t* size_return) override {
        ++calls;
        *size_return = actual_read_size;
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse list_objects(const ObjectStoragePathOptions& opts,
                                       std::vector<FileInfo>* files) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                         std::vector<std::string> objs) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_objects_recursively(
            const ObjectStoragePathOptions& opts) override {
        ++calls;
        ++delete_objects_recursively_calls;
        delete_objects_recursively_provider_calls +=
                delete_objects_recursively_provider_calls_per_logical_call;
        return ObjectStorageResponse::OK();
    }
    std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                       int64_t expiration_secs, const S3ClientConf& conf) override {
        ++calls;
        return "presigned";
    }

    int calls = 0;
    size_t actual_read_size = 0;
    int create_multipart_upload_calls = 0;
    int create_multipart_upload_provider_calls = 0;
    int create_multipart_upload_provider_calls_per_logical_call = 1;
    int delete_objects_recursively_calls = 0;
    int delete_objects_recursively_provider_calls = 0;
    int delete_objects_recursively_provider_calls_per_logical_call = 1;
};

struct RateLimiterConfigGuard {
    bool enable = config::enable_s3_rate_limiter;

    ~RateLimiterConfigGuard() {
        config::enable_s3_rate_limiter = enable;
        S3RateLimiterManager::instance().refresh();
    }
};

} // namespace

TEST(RateLimitedObjStorageClientTest, forwards_all_calls_when_disabled) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = false;

    auto fake = std::make_shared<FakeObjStorageClient>();
    RateLimitedObjStorageClient client(fake);
    ObjectStoragePathOptions opts {.bucket = "b", .key = "k"};

    size_t size_return = 0;
    EXPECT_EQ(0, client.create_multipart_upload(opts).resp.status.code);
    EXPECT_EQ(0, client.put_object(opts, "data").status.code);
    EXPECT_EQ(0, client.upload_part(opts, "data", 1).resp.status.code);
    EXPECT_EQ(0, client.complete_multipart_upload(opts, {}).status.code);
    EXPECT_EQ(0, client.head_object(opts).resp.status.code);
    EXPECT_EQ(0, client.get_object(opts, nullptr, 0, 4, &size_return).status.code);
    std::vector<FileInfo> files;
    EXPECT_EQ(0, client.list_objects(opts, &files).status.code);
    EXPECT_EQ(0, client.delete_objects(opts, {}).status.code);
    EXPECT_EQ(0, client.delete_object(opts).status.code);
    EXPECT_EQ(0, client.delete_objects_recursively(opts).status.code);
    EXPECT_EQ("presigned", client.generate_presigned_url(opts, 60, S3ClientConf {}));
    EXPECT_EQ(11, fake->calls);
}

TEST(RateLimitedObjStorageClientTest, get_rejected_by_count_limit_does_not_reach_inner) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 1);
    manager.qps_limiter(S3RateLimitType::PUT)->reset(0, 0, 0);

    auto fake = std::make_shared<FakeObjStorageClient>();
    RateLimitedObjStorageClient client(fake);
    ObjectStoragePathOptions opts {.bucket = "b", .key = "k"};

    EXPECT_EQ(0, client.head_object(opts).resp.status.code);
    EXPECT_EQ(1, fake->calls);

    auto resp = client.head_object(opts);
    EXPECT_NE(0, resp.resp.status.code);
    EXPECT_EQ(429, resp.resp.http_code);
    EXPECT_NE(std::string::npos, resp.resp.status.msg.find("exceeds request limit"));
    EXPECT_EQ(1, fake->calls); // rejected before reaching the provider

    // PUT uses an independent bucket and is unaffected.
    EXPECT_EQ(0, client.put_object(opts, "data").status.code);
    EXPECT_EQ(2, fake->calls);
}

TEST(RateLimitedObjStorageClientTest, get_object_settles_short_read) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 0);
    auto* bytes = manager.bytes_limiter(S3RateLimitType::GET);
    bytes->reset(kNoThrottleBytesPerSecond, kNoThrottleBytesPerSecond, 1000);

    auto fake = std::make_shared<FakeObjStorageClient>();
    fake->actual_read_size = 100; // short read: 600 requested, 100 returned
    RateLimitedObjStorageClient client(fake);
    ObjectStoragePathOptions opts {.bucket = "b", .key = "k"};

    size_t size_return = 0;
    EXPECT_EQ(0, client.get_object(opts, nullptr, 0, 600, &size_return).status.code);
    EXPECT_EQ(100, size_return);

    // Only 100 bytes remain cumulatively charged, so exactly 900 more are admitted.
    EXPECT_EQ(0, bytes->add(900));
    EXPECT_EQ(-1, bytes->add(1));
}

TEST(RateLimitedObjStorageClientTest, put_object_charges_payload_bytes) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    manager.qps_limiter(S3RateLimitType::PUT)->reset(0, 0, 0);
    auto* bytes = manager.bytes_limiter(S3RateLimitType::PUT);
    bytes->reset(kNoThrottleBytesPerSecond, kNoThrottleBytesPerSecond, 1000);

    auto fake = std::make_shared<FakeObjStorageClient>();
    RateLimitedObjStorageClient client(fake);
    ObjectStoragePathOptions opts {.bucket = "b", .key = "k"};

    std::string payload(600, 'x');
    EXPECT_EQ(0, client.put_object(opts, payload).status.code);
    EXPECT_EQ(0, bytes->add(400)); // exactly the cumulative count remainder
    EXPECT_EQ(-1, bytes->add(1));
}

TEST(RateLimitedObjStorageClientTest, recursive_delete_charges_one_put_qps) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 1);
    manager.qps_limiter(S3RateLimitType::PUT)->reset(0, 0, 1);
    manager.bytes_limiter(S3RateLimitType::PUT)->reset(0, 0, 0);

    auto fake = std::make_shared<FakeObjStorageClient>();
    fake->delete_objects_recursively_provider_calls_per_logical_call = 4;
    RateLimitedObjStorageClient client(fake);
    ObjectStoragePathOptions opts {.bucket = "b", .prefix = "p"};

    // Exhaust GET first. Recursive delete still succeeds because the logical API is PUT.
    EXPECT_EQ(0, client.head_object(opts).resp.status.code);
    EXPECT_EQ(0, client.delete_objects_recursively(opts).status.code);
    EXPECT_EQ(1, fake->delete_objects_recursively_calls);
    EXPECT_EQ(4, fake->delete_objects_recursively_provider_calls);

    auto resp = client.delete_objects_recursively(opts);
    EXPECT_NE(0, resp.status.code);
    EXPECT_EQ(429, resp.http_code);
    EXPECT_EQ(1, fake->delete_objects_recursively_calls);
    EXPECT_EQ(4, fake->delete_objects_recursively_provider_calls);
}

TEST(RateLimitedObjStorageClientTest, azure_noop_multipart_create_charges_one_put_qps) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    manager.qps_limiter(S3RateLimitType::PUT)->reset(0, 0, 1);
    manager.bytes_limiter(S3RateLimitType::PUT)->reset(0, 0, 0);

    auto fake = std::make_shared<FakeObjStorageClient>();
    // Azure implements create_multipart_upload as a provider-side no-op.
    fake->create_multipart_upload_provider_calls_per_logical_call = 0;
    RateLimitedObjStorageClient client(fake);
    ObjectStoragePathOptions opts {.bucket = "b", .key = "k"};

    EXPECT_EQ(0, client.create_multipart_upload(opts).resp.status.code);
    EXPECT_EQ(1, fake->create_multipart_upload_calls);
    EXPECT_EQ(0, fake->create_multipart_upload_provider_calls);

    auto resp = client.create_multipart_upload(opts);
    EXPECT_NE(0, resp.resp.status.code);
    EXPECT_EQ(429, resp.resp.http_code);
    EXPECT_EQ(1, fake->create_multipart_upload_calls);
    EXPECT_EQ(0, fake->create_multipart_upload_provider_calls);
}

TEST(RateLimitedObjStorageClientTest, presigned_url_bypasses_rate_limiters) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_qps = manager.qps_limiter(S3RateLimitType::GET);
    auto* put_qps = manager.qps_limiter(S3RateLimitType::PUT);
    get_qps->reset(0, 0, 1);
    put_qps->reset(0, 0, 1);
    EXPECT_EQ(0, get_qps->add(1));
    EXPECT_EQ(0, put_qps->add(1));

    auto fake = std::make_shared<FakeObjStorageClient>();
    RateLimitedObjStorageClient client(fake);
    ObjectStoragePathOptions opts {.bucket = "b", .key = "k"};

    EXPECT_EQ("presigned", client.generate_presigned_url(opts, 60, S3ClientConf {}));
    EXPECT_EQ(1, fake->calls);
}

} // namespace doris::io
