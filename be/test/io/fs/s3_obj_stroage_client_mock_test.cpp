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

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <fmt/format.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "cpp/obj-client/auth/gcp_workload_identity_token_provider.h"
#include "cpp/obj-client/obj_storage_client.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "gmock/gmock.h"
#include "io/fs/file_system.h"
#include "util/s3_util.h"
#include "util/string_util.h"

using namespace Aws::S3::Model;

namespace doris::io {
class MockS3Client : public Aws::S3::S3Client {
public:
    MockS3Client() {};

    MOCK_METHOD(Aws::S3::Model::ListObjectsV2Outcome, ListObjectsV2,
                (const Aws::S3::Model::ListObjectsV2Request& request), (const, override));
    MOCK_METHOD(Aws::S3::Model::DeleteObjectOutcome, DeleteObject,
                (const Aws::S3::Model::DeleteObjectRequest& request), (const, override));
    MOCK_METHOD(Aws::S3::Model::DeleteObjectsOutcome, DeleteObjects,
                (const Aws::S3::Model::DeleteObjectsRequest& request), (const, override));
};

class CountingGetRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    explicit CountingGetRateLimitPolicy(size_t* request_count) : request_count_(request_count) {}

    ObjStorageAdmission acquire(S3RateLimitType type, size_t) const override {
        EXPECT_EQ(type, S3RateLimitType::GET);
        ++*request_count_;
        return {};
    }

private:
    size_t* request_count_;
};

class S3ObjStorageClientMockTest : public testing::Test {
    static void SetUpTestSuite() { S3ClientFactory::instance(); };
    static void TearDownTestSuite() {};

private:
    static Aws::SDKOptions options;
};

Aws::SDKOptions S3ObjStorageClientMockTest::options {};

TEST_F(S3ObjStorageClientMockTest, list_objects_compatibility) {
    // If storage only supports ListObjectsV1, s3_obj_storage_client.list_objects
    // should return an error.
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto s3_obj_storage_client = std::make_shared<S3ObjStorageClient>(mock_s3_client);

    ListObjectsV2Result result;
    result.SetIsTruncated(true);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result)));

    std::vector<ObjectMeta> objects;
    auto response = s3_obj_storage_client->list_objects(
            {.bucket = "dummy-bucket", .key = "S3ObjStorageClientMockTest/list_objects_test"},
            &objects);

    EXPECT_TRUE(objects.empty());
    EXPECT_EQ(response.status.code, ErrorCode::INTERNAL_ERROR);
}

TEST_F(S3ObjStorageClientMockTest, gcp_workload_identity_bearer_token_applied) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    std::atomic<int> fetch_count = 0;
    auto token_provider = std::make_shared<GcpWorkloadIdentityTokenProvider>(
            [&](std::string* token, std::chrono::seconds* expires_in) {
                ++fetch_count;
                *token = "test-token";
                *expires_in = std::chrono::hours(1);
                return true;
            },
            std::chrono::steady_clock::now);
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client, {}, token_provider);

    std::vector<ObjectMeta> files;
    ListObjectsV2Result result;
    result.SetIsTruncated(false);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .Times(2)
            .WillRepeatedly([&](const ListObjectsV2Request& request) {
                const auto& headers = request.GetAdditionalCustomHeaders();
                auto header = headers.find("authorization");
                EXPECT_NE(header, headers.end());
                if (header != headers.end()) {
                    EXPECT_EQ(header->second, "Bearer test-token");
                }
                return ListObjectsV2Outcome(result);
            });

    auto response = s3_obj_storage_client.list_objects(
            {.bucket = "dummy-bucket",
             .prefix = "S3ObjStorageClientMockTest/gcp_workload_identity"},
            &files);
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // The token is cached: a second request must not refresh it again.
    response = s3_obj_storage_client.list_objects(
            {.bucket = "dummy-bucket",
             .prefix = "S3ObjStorageClientMockTest/gcp_workload_identity"},
            &files);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(fetch_count.load(), 1);
}

TEST_F(S3ObjStorageClientMockTest, reset_uses_complete_client_configuration) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    std::vector<S3ClientConf> created_configs;
    S3ClientFactory::instance().set_client_creator_for_test([&](const S3ClientConf& conf) {
        created_configs.push_back(conf);
        return std::make_shared<S3ObjStorageClient>(mock_s3_client);
    });

    S3ClientConf initial_conf {
            .endpoint = "https://s3.us-east-1.amazonaws.com",
            .region = "us-east-1",
            .ak = "access-key",
            .sk = "secret-key",
            .bucket = "bucket",
            .provider = io::ObjStorageProvider::AWS,
    };
    ObjClientHolder holder(initial_conf);
    EXPECT_TRUE(holder.init().ok());

    S3ClientConf workload_identity_conf {
            .endpoint = std::string(GCS_XML_ENDPOINT),
            .region = "us-central1",
            .bucket = "bucket",
            .provider = io::ObjStorageProvider::GCP,
            .need_override_endpoint = false,
            .cred_provider_type = CredProviderType::GcpWorkloadIdentity,
            .is_internal_bucket = true,
    };
    EXPECT_TRUE(holder.reset(workload_identity_conf).ok());
    S3ClientFactory::instance().clear_client_creator_for_test();

    ASSERT_EQ(created_configs.size(), 2);
    const auto& reset_conf = created_configs.back();
    EXPECT_EQ(reset_conf.endpoint, workload_identity_conf.endpoint);
    EXPECT_EQ(reset_conf.region, workload_identity_conf.region);
    EXPECT_EQ(reset_conf.provider, workload_identity_conf.provider);
    EXPECT_EQ(reset_conf.need_override_endpoint, workload_identity_conf.need_override_endpoint);
    EXPECT_EQ(reset_conf.cred_provider_type, workload_identity_conf.cred_provider_type);
    EXPECT_TRUE(reset_conf.ak.empty());
    EXPECT_TRUE(reset_conf.sk.empty());
}

TEST_F(S3ObjStorageClientMockTest, reset_distinguishes_addressing_and_endpoint_override) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    int created_clients = 0;
    S3ClientFactory::instance().set_client_creator_for_test([&](const S3ClientConf&) {
        ++created_clients;
        return std::make_shared<S3ObjStorageClient>(mock_s3_client);
    });

    S3ClientConf initial_conf {
            .endpoint = "https://s3.us-east-1.amazonaws.com",
            .region = "us-east-1",
            .ak = "access-key",
            .sk = "secret-key",
            .use_virtual_addressing = false,
            .need_override_endpoint = true,
    };
    ObjClientHolder holder(initial_conf);
    EXPECT_TRUE(holder.init().ok());

    S3ClientConf updated_conf = initial_conf;
    updated_conf.use_virtual_addressing = true;
    updated_conf.need_override_endpoint = false;
    EXPECT_TRUE(holder.reset(updated_conf).ok());
    S3ClientFactory::instance().clear_client_creator_for_test();

    EXPECT_EQ(created_clients, 2);
    EXPECT_EQ(holder.s3_client_conf(), updated_conf);
}

TEST_F(S3ObjStorageClientMockTest, explicit_credentials_override_stale_workload_identity) {
    cloud::ObjectStoreInfoPB info;
    info.set_endpoint("https://storage.googleapis.com");
    info.set_region("us-central1");
    info.set_bucket("bucket");
    info.set_provider(cloud::ObjectStoreInfoPB::GCP);
    info.set_cred_provider_type(cloud::CredProviderTypePB::GCP_WORKLOAD_IDENTITY);
    info.set_ak("access-key");
    info.set_sk("secret-key");

    auto conf = S3Conf::get_s3_conf(info);
    EXPECT_EQ(conf.client_conf.cred_provider_type, CredProviderType::Default);

    info.clear_ak();
    info.clear_sk();
    info.set_role_arn("arn:aws:iam::123456789012:role/test-role");
    conf = S3Conf::get_s3_conf(info);
    EXPECT_EQ(conf.client_conf.cred_provider_type, CredProviderType::InstanceProfile);
}

TEST_F(S3ObjStorageClientMockTest, gcp_workload_identity_delete_bearer_token_applied) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto token_provider = std::make_shared<GcpWorkloadIdentityTokenProvider>(
            [](std::string* token, std::chrono::seconds* expires_in) {
                *token = "test-token";
                *expires_in = std::chrono::hours(1);
                return true;
            },
            std::chrono::steady_clock::now);
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client, {}, token_provider);

    auto expect_bearer_token = [](const auto& request) {
        const auto& headers = request.GetAdditionalCustomHeaders();
        auto header = headers.find("authorization");
        ASSERT_NE(header, headers.end());
        EXPECT_EQ(header->second, "Bearer test-token");
    };

    DeleteObjectsResult delete_result;
    EXPECT_CALL(*mock_s3_client, DeleteObjects(testing::_))
            .WillOnce([&](const DeleteObjectsRequest& request) {
                expect_bearer_token(request);
                return DeleteObjectsOutcome(delete_result);
            });

    auto response = s3_obj_storage_client.delete_objects(
            {.bucket = "dummy-bucket"}, {"S3ObjStorageClientMockTest/delete-object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);
}

TEST_F(S3ObjStorageClientMockTest, gcp_workload_identity_token_refresh) {
    auto now = std::chrono::steady_clock::now();
    int fetch_count = 0;
    bool fetch_succeeds = true;
    GcpWorkloadIdentityTokenProvider token_provider(
            [&](std::string* token, std::chrono::seconds* expires_in) {
                ++fetch_count;
                if (!fetch_succeeds) {
                    return false;
                }
                *token = fmt::format("token-{}", fetch_count);
                *expires_in = std::chrono::hours(1);
                return true;
            },
            [&] { return now; });

    EXPECT_EQ(token_provider.get_token(), "token-1");
    EXPECT_EQ(token_provider.get_token(), "token-1");
    EXPECT_EQ(fetch_count, 1);

    now += std::chrono::minutes(56);
    EXPECT_EQ(token_provider.get_token(), "token-2");

    fetch_succeeds = false;
    now += std::chrono::minutes(56);
    EXPECT_EQ(token_provider.get_token(), "token-2");
    EXPECT_EQ(token_provider.get_token(), "token-2");
    EXPECT_EQ(fetch_count, 3);
    now += std::chrono::seconds(5);
    EXPECT_EQ(token_provider.get_token(), "token-2");
    EXPECT_EQ(fetch_count, 3);
    now += std::chrono::seconds(26);
    EXPECT_EQ(token_provider.get_token(), "token-2");
    EXPECT_EQ(fetch_count, 4);
    now += std::chrono::minutes(4);
    EXPECT_TRUE(token_provider.get_token().empty());
    EXPECT_EQ(fetch_count, 5);
}

TEST_F(S3ObjStorageClientMockTest, gcp_workload_identity_refresh_uses_valid_cached_token) {
    auto now = std::chrono::steady_clock::now();
    std::mutex mutex;
    std::condition_variable condition;
    bool refresh_started = false;
    bool finish_refresh = false;
    bool cached_read_finished = false;
    int fetch_count = 0;

    GcpWorkloadIdentityTokenProvider token_provider(
            [&](std::string* token, std::chrono::seconds* expires_in) {
                ++fetch_count;
                if (fetch_count > 1) {
                    std::unique_lock lock(mutex);
                    refresh_started = true;
                    condition.notify_all();
                    condition.wait(lock, [&] { return finish_refresh; });
                }
                *token = fmt::format("token-{}", fetch_count);
                *expires_in = std::chrono::hours(1);
                return true;
            },
            [&] { return now; });

    EXPECT_EQ(token_provider.get_token(), "token-1");
    now += std::chrono::minutes(56);

    std::string refreshed_token;
    std::thread refresh_thread([&] { refreshed_token = token_provider.get_token(); });
    {
        std::unique_lock lock(mutex);
        EXPECT_TRUE(
                condition.wait_for(lock, std::chrono::seconds(1), [&] { return refresh_started; }));
    }

    std::string cached_token;
    std::thread cached_read_thread([&] {
        cached_token = token_provider.get_token();
        std::lock_guard lock(mutex);
        cached_read_finished = true;
        condition.notify_all();
    });
    {
        std::unique_lock lock(mutex);
        EXPECT_TRUE(condition.wait_for(lock, std::chrono::seconds(1),
                                       [&] { return cached_read_finished; }));
        finish_refresh = true;
        condition.notify_all();
    }

    cached_read_thread.join();
    refresh_thread.join();
    EXPECT_EQ(cached_token, "token-1");
    EXPECT_EQ(refreshed_token, "token-2");
    EXPECT_EQ(fetch_count, 2);
}

TEST_F(S3ObjStorageClientMockTest, gcp_workload_identity_refresh_is_serialized) {
    std::atomic<int> fetch_count = 0;
    GcpWorkloadIdentityTokenProvider token_provider(
            [&](std::string* token, std::chrono::seconds* expires_in) {
                ++fetch_count;
                *token = "shared-token";
                *expires_in = std::chrono::hours(1);
                return true;
            },
            std::chrono::steady_clock::now);

    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&] { EXPECT_EQ(token_provider.get_token(), "shared-token"); });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(fetch_count.load(), 1);
}

TEST_F(S3ObjStorageClientMockTest, gcp_workload_identity_fails_closed_without_token) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto token_provider = std::make_shared<GcpWorkloadIdentityTokenProvider>(
            [](std::string*, std::chrono::seconds*) { return false; },
            std::chrono::steady_clock::now);
    S3ObjStorageClient client(mock_s3_client, {}, token_provider);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_)).Times(0);

    std::vector<ObjectMeta> objects;
    auto response = client.list_objects({.bucket = "dummy-bucket", .prefix = "prefix"}, &objects);
    EXPECT_EQ(response.status.code, ObjStorageStatus::NETWORK_ERROR);
    EXPECT_EQ(response.http_code, 0);
}

TEST_F(S3ObjStorageClientMockTest, gcp_workload_identity_presigned_url_unsupported) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto token_provider = std::make_shared<GcpWorkloadIdentityTokenProvider>(
            [](std::string* token, std::chrono::seconds* expires_in) {
                *token = "test-token";
                *expires_in = std::chrono::hours(1);
                return true;
            },
            std::chrono::steady_clock::now);
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client, {}, token_provider);

    EXPECT_TRUE(s3_obj_storage_client
                        .generate_presigned_url({.bucket = "dummy-bucket", .key = "object"}, 60)
                        .empty());
}

ListObjectsV2Result CreatePageResult(const std::string& nextToken,
                                     const std::vector<std::string>& keys, bool isTruncated) {
    ListObjectsV2Result result;
    result.SetIsTruncated(isTruncated);
    result.SetNextContinuationToken(nextToken);
    for (const auto& key : keys) {
        Object obj;
        obj.SetKey(key);
        result.AddContents(std::move(obj));
    }
    return result;
}

TEST_F(S3ObjStorageClientMockTest, list_objects_with_pagination) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    size_t get_request_count = 0;
    auto inner_client = std::make_shared<S3ObjStorageClient>(mock_s3_client);
    auto obj_storage_client = std::make_shared<RateLimitedObjStorageClient>(
            std::move(inner_client),
            std::make_shared<CountingGetRateLimitPolicy>(&get_request_count));
    std::string prefix = "S3ObjStorageClientMockTest/list_objects_with_pagination/";

    std::vector<std::vector<std::string>> pages = {
            {"key1", "key2"}, // page1
            {"key3", "key4"}, // page2
            {"key5"}          // page3
    };

    for (auto& page : pages) {
        for (auto& key : page) {
            key = prefix + key;
        }
    }

    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce([&](const ListObjectsV2Request& req) {
                // page1：no ContinuationToken
                EXPECT_FALSE(req.ContinuationTokenHasBeenSet());
                return Aws::S3::Model::ListObjectsV2Outcome(
                        CreatePageResult("token1", pages[0], true));
            })
            .WillOnce([&](const ListObjectsV2Request& req) {
                // page2: token1
                EXPECT_EQ(req.GetContinuationToken(), "token1");
                return ListObjectsV2Outcome(CreatePageResult("token2", pages[1], true));
            })
            .WillOnce([&](const ListObjectsV2Request& req) {
                // page3: token2
                EXPECT_EQ(req.GetContinuationToken(), "token2");
                return ListObjectsV2Outcome(CreatePageResult("", pages[2], false));
            });

    std::vector<ObjectMeta> objects;
    auto response = obj_storage_client->list_objects(
            {.bucket = "dummy-bucket",
             .key = "S3ObjStorageClientMockTest/list_objects_with_pagination"},
            &objects);

    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(objects.size(), 5);
    EXPECT_EQ(get_request_count, pages.size());
}

TEST_F(S3ObjStorageClientMockTest,
       delete_object_preserves_not_found_and_batch_uses_delete_objects) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto s3_obj_storage_client = std::make_shared<S3ObjStorageClient>(mock_s3_client);
    auto not_found = [](const DeleteObjectRequest&) {
        Aws::S3::S3Error error;
        error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
        error.SetMessage("object not found");
        error.SetRequestId("request-id");
        return DeleteObjectOutcome(std::move(error));
    };
    EXPECT_CALL(*mock_s3_client, DeleteObject(testing::_)).WillOnce(not_found);
    EXPECT_CALL(*mock_s3_client, DeleteObjects(testing::_))
            .WillOnce([](const DeleteObjectsRequest& request) {
                const auto& objects = request.GetDelete().GetObjects();
                EXPECT_EQ(objects.size(), 1);
                EXPECT_EQ(objects.front().GetKey(), "missing-object");
                return DeleteObjectsOutcome(DeleteObjectsResult {});
            });

    auto response = s3_obj_storage_client->delete_object(
            {.bucket = "dummy-bucket", .key = "missing-object"});
    EXPECT_EQ(response.status.code, ObjStorageStatus::NOT_FOUND);
    EXPECT_EQ(response.http_code, static_cast<int>(Aws::Http::HttpResponseCode::NOT_FOUND));
    EXPECT_EQ(response.request_id, "request-id");

    response =
            s3_obj_storage_client->delete_objects({.bucket = "dummy-bucket"}, {"missing-object"});
    EXPECT_TRUE(response.ok());
}

TEST_F(S3ObjStorageClientMockTest, test_ca_cert) {
    auto path = doris::get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    LOG(INFO) << "config:" << config::ca_cert_file_paths << " path:" << path;
    ASSERT_FALSE(path.empty());
}
} // namespace doris::io
