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

#include "cpp/client/obj_storage_client.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace doris {
namespace {

class FakeObjStorageBackend final : public ObjStorageBackend {
public:
    ObjectStorageUploadResponse create_multipart_upload(const ObjectStoragePathOptions&) override {
        ++calls;
        return {};
    }

    ObjectStorageResponse put_object(const ObjectStoragePathOptions&, std::string_view) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }

    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions&, std::string_view,
                                            int) override {
        ++calls;
        return {};
    }

    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions&, const std::vector<ObjectCompleteMultiPart>&) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }

    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions&) override {
        ++calls;
        return {};
    }

    ObjectStorageResponse get_object(const ObjectStoragePathOptions&, void*, size_t, size_t,
                                     size_t* size_return) override {
        ++calls;
        *size_return = 4;
        return ObjectStorageResponse::OK();
    }

    ObjectStorageListPage list_objects(const ObjectStoragePathOptions&, std::string_view) override {
        ++calls;
        return {};
    }

    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions&,
                                         std::vector<std::string>) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }

    ObjectStorageResponse delete_object(const ObjectStoragePathOptions&) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }

    ObjStorageCapabilities capabilities() const override { return {.max_delete_batch = 2}; }

    std::string generate_presigned_url(const ObjectStoragePathOptions&, int64_t) override {
        ++presigned_url_calls;
        return "url";
    }

    ObjectStorageResponse get_life_cycle(const std::string&, int64_t*) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }

    ObjectStorageResponse check_versioning(const std::string&) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }

    ObjectStorageResponse abort_multipart_upload(const ObjectStoragePathOptions&,
                                                 const std::string&) override {
        ++calls;
        return ObjectStorageResponse::OK();
    }

    int calls = 0;
    int presigned_url_calls = 0;
};

class RecordingRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    struct Request {
        ObjStorageRequestType type;
        size_t estimated_bytes;
    };

    ObjStorageRateLimitToken acquire(ObjStorageRequestType type,
                                     size_t estimated_bytes) const override {
        requests.push_back({type, estimated_bytes});
        if (reject) {
            return {.resp = ObjectStorageResponse::rate_limit("rejected by test policy")};
        }
        return {.settle = [this](size_t actual_bytes) { settled_bytes.push_back(actual_bytes); }};
    }

    bool reject = false;
    mutable std::vector<Request> requests;
    mutable std::vector<size_t> settled_bytes;
};

TEST(ObjStorageClientTest, AppliesAdmissionPolicyToEveryBackendRequest) {
    auto backend = std::make_shared<FakeObjStorageBackend>();
    auto policy = std::make_shared<RecordingRateLimitPolicy>();
    ObjStorageClient client(backend, policy);
    ObjectStoragePathOptions opts {.bucket = "bucket", .key = "key"};

    EXPECT_TRUE(client.create_multipart_upload(opts).resp.ok());
    EXPECT_TRUE(client.put_object(opts, "abc").ok());
    EXPECT_TRUE(client.upload_part(opts, "part", 1).resp.ok());
    EXPECT_TRUE(client.complete_multipart_upload(opts, {}).ok());
    EXPECT_TRUE(client.head_object(opts).resp.ok());
    char buffer[10];
    size_t size_return = 0;
    EXPECT_TRUE(client.get_object(opts, buffer, 0, sizeof(buffer), &size_return).ok());
    EXPECT_EQ(size_return, 4);
    EXPECT_TRUE(client.list_objects(opts).resp.ok());
    EXPECT_TRUE(client.delete_objects(opts, {"one", "two", "three"}).ok());
    EXPECT_TRUE(client.delete_object(opts).ok());
    int64_t expiration_days = 0;
    EXPECT_TRUE(client.get_life_cycle("bucket", &expiration_days).ok());
    EXPECT_TRUE(client.check_versioning("bucket").ok());
    EXPECT_TRUE(client.abort_multipart_upload(opts, "upload-id").ok());
    EXPECT_EQ(client.generate_presigned_url(opts, 60), "url");

    const std::vector<RecordingRateLimitPolicy::Request> expected = {
            {ObjStorageRequestType::PUT, 0}, {ObjStorageRequestType::PUT, 3},
            {ObjStorageRequestType::PUT, 4}, {ObjStorageRequestType::PUT, 0},
            {ObjStorageRequestType::GET, 0}, {ObjStorageRequestType::GET, 10},
            {ObjStorageRequestType::GET, 0}, {ObjStorageRequestType::PUT, 0},
            {ObjStorageRequestType::PUT, 0}, {ObjStorageRequestType::PUT, 0},
            {ObjStorageRequestType::GET, 0}, {ObjStorageRequestType::GET, 0},
            {ObjStorageRequestType::PUT, 0},
    };
    ASSERT_EQ(policy->requests.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(policy->requests[i].type, expected[i].type) << "request index " << i;
        EXPECT_EQ(policy->requests[i].estimated_bytes, expected[i].estimated_bytes)
                << "request index " << i;
    }
    EXPECT_EQ(policy->settled_bytes, std::vector<size_t>({4}));
    EXPECT_EQ(backend->calls, 13);
    EXPECT_EQ(backend->presigned_url_calls, 1);
}

TEST(ObjStorageClientTest, RejectsBeforeDispatchingToBackend) {
    auto backend = std::make_shared<FakeObjStorageBackend>();
    auto policy = std::make_shared<RecordingRateLimitPolicy>();
    policy->reject = true;
    ObjStorageClient client(backend, policy);
    ObjectStoragePathOptions opts {.bucket = "bucket", .key = "key"};

    auto put_response = client.put_object(opts, "abc");
    EXPECT_EQ(put_response.status.code, static_cast<int>(TStatusCode::LIMIT_REACH));
    auto head_response = client.head_object(opts);
    EXPECT_EQ(head_response.resp.status.code, static_cast<int>(TStatusCode::LIMIT_REACH));
    EXPECT_EQ(backend->calls, 0);
    EXPECT_EQ(policy->requests.size(), 2);
}

} // namespace
} // namespace doris
