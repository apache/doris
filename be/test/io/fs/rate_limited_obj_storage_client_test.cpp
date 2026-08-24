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

#include "cpp/obj-client/rate_limited_obj_storage_client.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace doris {
namespace {

class FakeObjStorageClient final : public ObjStorageClient {
public:
    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath&) override {
        ++calls;
        ++create_multipart_upload_calls;
        create_multipart_upload_provider_calls +=
                create_multipart_upload_provider_calls_per_logical_call;
        return {};
    }

    ObjStorageResponse put_object(const ObjStoragePath&, std::string_view) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageUploadResult upload_part(const ObjStoragePath&, const std::string&, std::string_view,
                                       int) override {
        ++calls;
        return {};
    }

    ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath&, const std::string&,
            const std::vector<ObjStorageCompletedPart>&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageHeadResult head_object(const ObjStoragePath&) override {
        ++calls;
        return {};
    }

    ObjStorageResponse get_object(const ObjStoragePath&, void*, size_t, size_t,
                                  size_t* size_return) override {
        ++calls;
        *size_return = actual_read_size;
        if (fail_get) {
            return {.status = {TStatusCode::INTERNAL_ERROR, "simulated read failure"}};
        }
        return ObjStorageResponse::OK();
    }

    ObjStorageListPageResult list_objects_page(const ObjStoragePath&, std::string_view) override {
        ++calls;
        return {};
    }

    ObjStorageResponse delete_objects(const ObjStoragePath&, std::vector<std::string>) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageResponse delete_object(const ObjStoragePath&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    std::string generate_presigned_url(const ObjStoragePath&, int64_t) override {
        ++presigned_url_calls;
        return "url";
    }

    ObjStorageResponse get_lifecycle(const std::string&, int64_t*) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageResponse check_versioning(const std::string&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageResponse abort_multipart_upload(const ObjStoragePath&, const std::string&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageCapabilities capabilities() const override {
        return {.max_delete_batch = 17, .max_list_page = 23};
    }

    int calls = 0;
    int presigned_url_calls = 0;
    int create_multipart_upload_calls = 0;
    int create_multipart_upload_provider_calls = 0;
    int create_multipart_upload_provider_calls_per_logical_call = 1;
    size_t actual_read_size = 4;
    bool fail_get = false;
};

class RecordingRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    struct Request {
        Request(S3RateLimitType request_type, size_t bytes)
                : type(request_type), estimated_bytes(bytes) {}

        S3RateLimitType type;
        size_t estimated_bytes;
    };

    ObjStorageAdmission acquire(S3RateLimitType type, size_t estimated_bytes) const override {
        requests.push_back({type, estimated_bytes});
        if (reject) {
            return {.resp = ObjStorageResponse::rate_limit(TStatusCode::LIMIT_REACH, 429,
                                                           "rejected by test policy")};
        }
        return {.settle = [this](size_t actual_bytes) { settled_bytes.push_back(actual_bytes); }};
    }

    bool reject = false;
    mutable std::vector<Request> requests;
    mutable std::vector<size_t> settled_bytes;
};

void expect_rejected(const ObjStorageResponse& response) {
    EXPECT_EQ(response.status.code, static_cast<int>(TStatusCode::LIMIT_REACH));
    EXPECT_EQ(response.http_code, 429);
    EXPECT_EQ(response.status.msg, "rejected by test policy");
}

TEST(RateLimitedObjStorageClientTest, AppliesAdmissionPolicyToEveryClientOperation) {
    auto inner_client = std::make_shared<FakeObjStorageClient>();
    auto policy = std::make_shared<RecordingRateLimitPolicy>();
    auto client = std::make_shared<RateLimitedObjStorageClient>(inner_client, policy);
    ObjStoragePath opts {.bucket = "bucket", .key = "key"};

    EXPECT_TRUE(client->create_multipart_upload(opts).resp.ok());
    EXPECT_TRUE(client->put_object(opts, "abc").ok());
    EXPECT_TRUE(client->upload_part(opts, "upload-id", "part", 1).resp.ok());
    EXPECT_TRUE(client->complete_multipart_upload(opts, "upload-id", {}).ok());
    EXPECT_TRUE(client->head_object(opts).resp.ok());
    char buffer[10];
    size_t size_return = 0;
    EXPECT_TRUE(client->get_object(opts, buffer, 0, sizeof(buffer), &size_return).ok());
    EXPECT_EQ(size_return, 4);
    std::vector<ObjectMeta> objects;
    EXPECT_TRUE(client->list_objects(opts, &objects).ok());
    EXPECT_TRUE(client->delete_objects(opts, {"one", "two", "three"}).ok());
    EXPECT_TRUE(client->delete_object(opts).ok());
    EXPECT_TRUE(delete_objects_recursively(client, opts).ok());
    int64_t expiration_days = 0;
    EXPECT_TRUE(client->get_lifecycle("bucket", &expiration_days).ok());
    EXPECT_TRUE(client->check_versioning("bucket").ok());
    EXPECT_TRUE(client->abort_multipart_upload(opts, "upload-id").ok());
    EXPECT_EQ(client->generate_presigned_url(opts, 60), "url");
    EXPECT_EQ(client->capabilities().max_delete_batch, 17);
    EXPECT_EQ(client->capabilities().max_list_page, 23);

    const std::vector<RecordingRateLimitPolicy::Request> expected = {
            {S3RateLimitType::PUT, 0}, {S3RateLimitType::PUT, 3}, {S3RateLimitType::PUT, 4},
            {S3RateLimitType::PUT, 0}, {S3RateLimitType::GET, 0}, {S3RateLimitType::GET, 10},
            {S3RateLimitType::GET, 0}, {S3RateLimitType::PUT, 0}, {S3RateLimitType::PUT, 0},
            {S3RateLimitType::GET, 0}, {S3RateLimitType::GET, 0}, {S3RateLimitType::GET, 0},
            {S3RateLimitType::PUT, 0},
    };
    ASSERT_EQ(policy->requests.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(policy->requests[i].type, expected[i].type) << "request index " << i;
        EXPECT_EQ(policy->requests[i].estimated_bytes, expected[i].estimated_bytes)
                << "request index " << i;
    }
    EXPECT_EQ(policy->settled_bytes, std::vector<size_t>({4}));
    EXPECT_EQ(inner_client->calls, 13);
    EXPECT_EQ(inner_client->presigned_url_calls, 1);
}

TEST(RateLimitedObjStorageClientTest, RejectsEveryOperationBeforeDispatchingToInnerClient) {
    auto inner_client = std::make_shared<FakeObjStorageClient>();
    auto policy = std::make_shared<RecordingRateLimitPolicy>();
    policy->reject = true;
    auto client = std::make_shared<RateLimitedObjStorageClient>(inner_client, policy);
    ObjStoragePath opts {.bucket = "bucket", .key = "key"};

    expect_rejected(client->create_multipart_upload(opts).resp);
    expect_rejected(client->put_object(opts, "abc"));
    expect_rejected(client->upload_part(opts, "upload-id", "part", 1).resp);
    expect_rejected(client->complete_multipart_upload(opts, "upload-id", {}));
    expect_rejected(client->head_object(opts).resp);
    char buffer[10];
    size_t size_return = 0;
    expect_rejected(client->get_object(opts, buffer, 0, sizeof(buffer), &size_return));
    std::vector<ObjectMeta> objects;
    expect_rejected(client->list_objects(opts, &objects));
    expect_rejected(client->delete_objects(opts, {"one", "two"}));
    expect_rejected(client->delete_object(opts));
    expect_rejected(delete_objects_recursively(client, opts));
    int64_t expiration_days = 0;
    expect_rejected(client->get_lifecycle("bucket", &expiration_days));
    expect_rejected(client->check_versioning("bucket"));
    expect_rejected(client->abort_multipart_upload(opts, "upload-id"));

    EXPECT_EQ(inner_client->calls, 0);
    EXPECT_EQ(policy->requests.size(), 13);
    EXPECT_TRUE(policy->settled_bytes.empty());
}

TEST(RateLimitedObjStorageClientTest, DoesNotSettleFailedReads) {
    auto inner_client = std::make_shared<FakeObjStorageClient>();
    inner_client->fail_get = true;
    auto policy = std::make_shared<RecordingRateLimitPolicy>();
    RateLimitedObjStorageClient client(inner_client, policy);

    char buffer[10];
    size_t size_return = 0;
    auto response = client.get_object({.bucket = "bucket", .key = "key"}, buffer, 0, sizeof(buffer),
                                      &size_return);

    EXPECT_FALSE(response.ok());
    EXPECT_EQ(inner_client->calls, 1);
    ASSERT_EQ(policy->requests.size(), 1);
    EXPECT_EQ(policy->requests[0].type, S3RateLimitType::GET);
    EXPECT_EQ(policy->requests[0].estimated_bytes, sizeof(buffer));
    EXPECT_TRUE(policy->settled_bytes.empty());
}

TEST(RateLimitedObjStorageClientTest, PresignedUrlBypassesRejectedPolicy) {
    auto inner_client = std::make_shared<FakeObjStorageClient>();
    auto policy = std::make_shared<RecordingRateLimitPolicy>();
    policy->reject = true;
    RateLimitedObjStorageClient client(inner_client, policy);

    EXPECT_EQ(client.generate_presigned_url({.bucket = "bucket", .key = "key"}, 60), "url");
    EXPECT_EQ(inner_client->presigned_url_calls, 1);
    EXPECT_TRUE(policy->requests.empty());
}

TEST(RateLimitedObjStorageClientTest, MultipartCreateUsesOneLogicalAdmission) {
    auto inner_client = std::make_shared<FakeObjStorageClient>();
    inner_client->create_multipart_upload_provider_calls_per_logical_call = 2;
    auto policy = std::make_shared<RecordingRateLimitPolicy>();
    RateLimitedObjStorageClient client(inner_client, policy);

    EXPECT_TRUE(client.create_multipart_upload({.bucket = "bucket", .key = "key"}).resp.ok());
    ASSERT_EQ(policy->requests.size(), 1);
    EXPECT_EQ(policy->requests[0].type, S3RateLimitType::PUT);
    EXPECT_EQ(policy->requests[0].estimated_bytes, 0);
    EXPECT_EQ(inner_client->create_multipart_upload_calls, 1);
    EXPECT_EQ(inner_client->create_multipart_upload_provider_calls, 2);
}

} // namespace
} // namespace doris
