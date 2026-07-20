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
#include <condition_variable>
#include <mutex>
#include <thread>

#include "gmock/gmock.h"
#include "cpp/sync_point.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_obj_storage_client.h"
#include "util/s3_util.h"
#include "util/string_util.h"

using namespace Aws::S3::Model;

namespace doris::io {
class MockS3Client : public Aws::S3::S3Client {
public:
    MockS3Client() {};

    MOCK_METHOD(Aws::S3::Model::ListObjectsV2Outcome, ListObjectsV2,
                (const Aws::S3::Model::ListObjectsV2Request& request), (const, override));
    MOCK_METHOD(Aws::S3::Model::DeleteObjectsOutcome, DeleteObjects,
                (const Aws::S3::Model::DeleteObjectsRequest& request), (const, override));
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
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client);

    std::vector<io::FileInfo> files;

    ListObjectsV2Result result;
    result.SetIsTruncated(true);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result)));

    auto response = s3_obj_storage_client.list_objects(
            {.bucket = "dummy-bucket", .prefix = "S3ObjStorageClientMockTest/list_objects_test"},
            &files);

    EXPECT_EQ(response.status.code, ErrorCode::INTERNAL_ERROR);
    files.clear();
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
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client, token_provider);

    std::vector<io::FileInfo> files;
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

TEST_F(S3ObjStorageClientMockTest, gcp_workload_identity_token_is_refreshed_after_rate_limit) {
    auto now = std::chrono::steady_clock::now();
    int fetch_count = 0;
    auto token_provider = std::make_shared<GcpWorkloadIdentityTokenProvider>(
            [&](std::string* token, std::chrono::seconds* expires_in) {
                *token = fmt::format("token-{}", ++fetch_count);
                *expires_in = std::chrono::hours(1);
                return true;
            },
            [&] { return now; });
    EXPECT_EQ(token_provider->get_token(), "token-1");

    auto mock_s3_client = std::make_shared<MockS3Client>();
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client, token_provider);
    ListObjectsV2Result result;
    result.SetIsTruncated(false);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce([&](const ListObjectsV2Request& request) {
                const auto& headers = request.GetAdditionalCustomHeaders();
                auto header = headers.find("authorization");
                EXPECT_NE(header, headers.end());
                if (header != headers.end()) {
                    EXPECT_EQ(header->second, "Bearer token-2");
                }
                return ListObjectsV2Outcome(result);
            });

    bool enable_s3_rate_limiter = config::enable_s3_rate_limiter;
    config::enable_s3_rate_limiter = true;
    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("S3ObjStorageClient::after_rate_limit",
                              [&](auto&&) { now += std::chrono::minutes(56); });
    sync_point->enable_processing();

    std::vector<io::FileInfo> files;
    auto response = s3_obj_storage_client.list_objects(
            {.bucket = "dummy-bucket", .prefix = "S3ObjStorageClientMockTest/rate_limit"},
            &files);

    sync_point->disable_processing();
    sync_point->clear_all_call_backs();
    config::enable_s3_rate_limiter = enable_s3_rate_limiter;
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(fetch_count, 2);
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
            .provider = io::ObjStorageType::AWS,
    };
    ObjClientHolder holder(initial_conf);
    EXPECT_TRUE(holder.init().ok());

    S3ClientConf workload_identity_conf {
            .endpoint = std::string(GCS_XML_ENDPOINT),
            .region = "us-central1",
            .bucket = "bucket",
            .provider = io::ObjStorageType::GCP,
            .need_override_endpoint = false,
            .cred_provider_type = CredProviderType::GcpWorkloadIdentity,
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
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client, token_provider);

    auto expect_bearer_token = [](const auto& request) {
        const auto& headers = request.GetAdditionalCustomHeaders();
        auto header = headers.find("authorization");
        ASSERT_NE(header, headers.end());
        EXPECT_EQ(header->second, "Bearer test-token");
    };

    DeleteObjectsResult delete_result;
    EXPECT_CALL(*mock_s3_client, DeleteObjects(testing::_))
            .Times(2)
            .WillRepeatedly([&](const DeleteObjectsRequest& request) {
                expect_bearer_token(request);
                return DeleteObjectsOutcome(delete_result);
            });

    auto response = s3_obj_storage_client.delete_objects(
            {.bucket = "dummy-bucket"}, {"S3ObjStorageClientMockTest/delete-object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    ListObjectsV2Result list_result;
    list_result.SetIsTruncated(false);
    Object object;
    object.SetKey("S3ObjStorageClientMockTest/delete-recursively/object");
    list_result.AddContents(std::move(object));
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce([&](const ListObjectsV2Request& request) {
                expect_bearer_token(request);
                return ListObjectsV2Outcome(list_result);
            });

    response = s3_obj_storage_client.delete_objects_recursively(
            {.bucket = "dummy-bucket", .prefix = "S3ObjStorageClientMockTest/delete-recursively"});
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
    auto token_provider = std::make_shared<GcpWorkloadIdentityTokenProvider>(
            [](std::string*, std::chrono::seconds*) { return false; },
            std::chrono::steady_clock::now);
    ListObjectsV2Request request;
    apply_gcp_bearer_token(request, token_provider);

    const auto& headers = request.GetAdditionalCustomHeaders();
    auto header = headers.find("authorization");
    ASSERT_NE(header, headers.end());
    EXPECT_EQ(header->second, "Bearer unavailable");
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
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client, token_provider);

    S3ClientConf conf;
    auto result = s3_obj_storage_client.generate_presigned_url(
            {.bucket = "dummy-bucket", .key = "object"}, 60, conf);
    ASSERT_FALSE(result);
    EXPECT_TRUE(result.error().is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
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
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client);

    std::vector<std::vector<std::string>> pages = {
            {"key1", "key2"}, // page1
            {"key3", "key4"}, // page2
            {"key5"}          // page3
    };

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

    std::vector<io::FileInfo> files;
    auto response = s3_obj_storage_client.list_objects(
            {.bucket = "dummy-bucket",
             .prefix = "S3ObjStorageClientMockTest/list_objects_with_pagination"},
            &files);

    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 5);
    files.clear();
}

TEST_F(S3ObjStorageClientMockTest, test_ca_cert) {
    auto path = doris::get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    LOG(INFO) << "config:" << config::ca_cert_file_paths << " path:" << path;
    ASSERT_FALSE(path.empty());
}
} // namespace doris::io
