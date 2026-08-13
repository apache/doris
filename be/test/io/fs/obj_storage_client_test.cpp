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

#include "cpp/obj-client/obj_storage_client.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cpp/obj-client/rate_limited_obj_storage_client.h"

namespace doris {
namespace {

class FakeObjStorageClient final : public ObjStorageClient {
public:
    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath&) override {
        ++calls;
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
        *size_return = 4;
        return ObjStorageResponse::OK();
    }

    ObjStorageListPageResult list_objects_page(const ObjStoragePath&,
                                               std::string_view continuation_token) override {
        ++calls;
        ++list_page_calls;
        if (list_pages.empty()) {
            return {};
        }
        const size_t index =
                continuation_token.empty()
                        ? 0
                        : static_cast<size_t>(std::stoull(std::string(continuation_token)));
        ObjStorageListPageResult page {.resp = ObjStorageResponse::OK()};
        page.objects = list_pages[index];
        page.has_more = index + 1 < list_pages.size();
        if (page.has_more) {
            page.continuation_token = std::to_string(index + 1);
        }
        return page;
    }

    ObjStorageResponse delete_objects(const ObjStoragePath&, std::vector<std::string>) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageResponse delete_object(const ObjStoragePath&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageCapabilities capabilities() const override { return {.max_delete_batch = 2}; }

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

    int calls = 0;
    int presigned_url_calls = 0;
    int list_page_calls = 0;
    std::vector<std::vector<ObjectMeta>> list_pages;
};

class RecordingRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    struct Request {
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

TEST(ObjStorageClientTest, SupportsLazyAndEagerListing) {
    auto client = std::make_shared<FakeObjStorageClient>();
    client->list_pages = {
            {{.key = "first"}, {.key = "second"}},
            {{.key = "third"}},
    };
    ObjStoragePath opts {.bucket = "bucket", .prefix = "prefix"};

    auto iter = client->list_objects(opts);
    EXPECT_EQ(client->list_page_calls, 0);
    auto first = iter->next();
    ASSERT_TRUE(first.object.has_value());
    EXPECT_EQ(first.object->key, "first");
    EXPECT_EQ(client->list_page_calls, 1);
    auto second = iter->next();
    ASSERT_TRUE(second.object.has_value());
    EXPECT_EQ(second.object->key, "second");
    EXPECT_EQ(client->list_page_calls, 1);
    auto third = iter->next();
    ASSERT_TRUE(third.object.has_value());
    EXPECT_EQ(third.object->key, "third");
    EXPECT_EQ(client->list_page_calls, 2);

    std::vector<ObjectMeta> objects;
    EXPECT_TRUE(client->list_objects(opts, &objects).ok());
    ASSERT_EQ(objects.size(), 3);
    EXPECT_EQ(objects[0].key, "first");
    EXPECT_EQ(objects[1].key, "second");
    EXPECT_EQ(objects[2].key, "third");
    EXPECT_EQ(client->list_page_calls, 4);
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
    int64_t expiration_days = 0;
    EXPECT_TRUE(client->get_lifecycle("bucket", &expiration_days).ok());
    EXPECT_TRUE(client->check_versioning("bucket").ok());
    EXPECT_TRUE(client->abort_multipart_upload(opts, "upload-id").ok());
    EXPECT_EQ(client->generate_presigned_url(opts, 60), "url");

    const std::vector<RecordingRateLimitPolicy::Request> expected = {
            {S3RateLimitType::PUT, 0}, {S3RateLimitType::PUT, 3}, {S3RateLimitType::PUT, 4},
            {S3RateLimitType::PUT, 0}, {S3RateLimitType::GET, 0}, {S3RateLimitType::GET, 10},
            {S3RateLimitType::GET, 0}, {S3RateLimitType::PUT, 0}, {S3RateLimitType::PUT, 0},
            {S3RateLimitType::GET, 0}, {S3RateLimitType::GET, 0}, {S3RateLimitType::PUT, 0},
    };
    ASSERT_EQ(policy->requests.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(policy->requests[i].type, expected[i].type) << "request index " << i;
        EXPECT_EQ(policy->requests[i].estimated_bytes, expected[i].estimated_bytes)
                << "request index " << i;
    }
    EXPECT_EQ(policy->settled_bytes, std::vector<size_t>({4}));
    EXPECT_EQ(inner_client->calls, 12);
    EXPECT_EQ(inner_client->presigned_url_calls, 1);
}

TEST(RateLimitedObjStorageClientTest, RejectsBeforeDispatchingToInnerClient) {
    auto inner_client = std::make_shared<FakeObjStorageClient>();
    auto policy = std::make_shared<RecordingRateLimitPolicy>();
    policy->reject = true;
    RateLimitedObjStorageClient client(inner_client, policy);
    ObjStoragePath opts {.bucket = "bucket", .key = "key"};

    auto put_response = client.put_object(opts, "abc");
    EXPECT_EQ(put_response.status.code, static_cast<int>(TStatusCode::LIMIT_REACH));
    EXPECT_EQ(put_response.http_code, 429);
    auto head_response = client.head_object(opts);
    EXPECT_EQ(head_response.resp.status.code, static_cast<int>(TStatusCode::LIMIT_REACH));
    EXPECT_EQ(head_response.resp.http_code, 429);
    EXPECT_EQ(inner_client->calls, 0);
    EXPECT_EQ(policy->requests.size(), 2);
}

} // namespace
} // namespace doris
