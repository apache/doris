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

#include "io/object_storage/obj_storage_client_factory.h"

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/GeneralHTTPCredentialsProvider.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationResult.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/GetBucketVersioningResult.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/aws_common.h"
#include "cpp/custom_aws_credentials_provider_chain.h"
#include "cpp/obj-storage/auth/aws_credential_factory.h"
#include "cpp/obj-storage/auth/gcp/gcp_auth.h"
#include "cpp/obj-storage/auth/gcp/gcp_s3_client.h"
#include "cpp/obj-storage/auth/gcp/gcs_signed_url.h"
#include "cpp/obj-storage/ca_bundle_cache.h"
#include "cpp/obj-storage/client/bos_obj_storage_client.h"
#include "cpp/obj-storage/client/cos_obj_storage_client.h"
#include "cpp/obj-storage/client/gcp_obj_storage_client.h"
#include "cpp/obj-storage/client/obs_obj_storage_client.h"
#include "cpp/obj-storage/client/oss_obj_storage_client.h"
#include "cpp/obj-storage/client/s3_obj_storage_client.h"
#include "cpp/obj-storage/client/tos_obj_storage_client.h"
#include "cpp/obj-storage/rate_limited_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "io/fs/err_utils.h"
#include "io/fs/obj_storage_file_system.h"
#include "io/object_storage/aws_client_settings.h"
#include "io/object_storage/obj_storage_config_parser.h"
#include "io/object_storage/obj_storage_rate_limiter_manager.h"
#include "testutil/container_credentials_endpoint.h"
#include "util/s3_uri.h"

#ifdef USE_AZURE
#include "cpp/obj-storage/client/azure_obj_storage_client.h"
#endif

namespace doris {

class S3ClientFactoryTest : public testing::Test {
protected:
    static void SetUpTestSuite() { ensure_aws_sdk_initialized(config::aws_log_level); }

    static AwsCredentialResult create_credentials_provider(
            const AwsConfig& conf, const std::string& ca_path = be_ca_bundle_path()) {
        auto options = AwsCredentialOptions::from_config(
                config::aws_credentials_provider_version, be_default_aws_client_configuration(),
                ca_path, conf, EmptyCredentialsBehavior::ANONYMOUS);
        return AwsCredentialFactory::create(options);
    }

    void TearDown() override {
        ObjStorageClientFactory::instance().clear_client_creator_for_test();
    }
};

namespace {

constexpr size_t kNoThrottleBytesPerSecond = 1ULL << 40;

AwsConfig make_factory_conf(std::string endpoint) {
    AwsConfig conf;
    conf.endpoint = std::move(endpoint);
    conf.region = "us-east-1";
    conf.cred_provider_type = CredProviderType::Anonymous;
    return conf;
}

AwsConfig make_hash_collision_conf(std::string endpoint, bool swapped) {
    auto conf = make_factory_conf(std::move(endpoint));
    conf.connect_timeout_ms = swapped ? 200 : 100;
    conf.request_timeout_ms = swapped ? 100 : 200;
    return conf;
}

template <typename Client, typename Config>
void check_s3_provider_client(const ObjStorageConfigBase& settings) {
    Config config {settings};
    auto result = ObjStorageClientFactory::instance().create(config);
    ASSERT_TRUE(result.has_value()) << result.error();
    const auto* client = result->get();
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(typeid(*client), typeid(Client));
}

class CloudModeConfigGuard {
public:
    explicit CloudModeConfigGuard(bool cloud_mode)
            : _deploy_mode(config::deploy_mode), _cloud_unique_id(config::cloud_unique_id) {
        config::deploy_mode = cloud_mode ? "cloud" : "";
        config::cloud_unique_id.clear();
    }

    ~CloudModeConfigGuard() {
        config::deploy_mode = _deploy_mode;
        config::cloud_unique_id = _cloud_unique_id;
    }

private:
    std::string _deploy_mode;
    std::string _cloud_unique_id;
};

class RateLimiterConfigGuard {
public:
    RateLimiterConfigGuard()
            : _enabled(config::enable_s3_rate_limiter),
              _qps_max_speed(_qps()->get_max_speed()),
              _qps_max_burst(_qps()->get_max_burst()),
              _qps_limit(_qps()->get_limit()),
              _bytes_max_speed(_bytes()->get_max_speed()),
              _bytes_max_burst(_bytes()->get_max_burst()),
              _bytes_limit(_bytes()->get_limit()) {}

    ~RateLimiterConfigGuard() {
        config::enable_s3_rate_limiter = _enabled;
        _qps()->reset(_qps_max_speed, _qps_max_burst, _qps_limit);
        _bytes()->reset(_bytes_max_speed, _bytes_max_burst, _bytes_limit);
    }

private:
    static S3RateLimiterHolder* _qps() {
        return ObjStorageRateLimiterManager::instance().qps_limiter(ObjStorageRateLimitType::GET);
    }
    static S3RateLimiterHolder* _bytes() {
        return ObjStorageRateLimiterManager::instance().bytes_limiter(ObjStorageRateLimitType::GET);
    }

    bool _enabled;
    size_t _qps_max_speed;
    size_t _qps_max_burst;
    size_t _qps_limit;
    size_t _bytes_max_speed;
    size_t _bytes_max_burst;
    size_t _bytes_limit;
};

GcpConfig make_gcp_native_conf(std::string impersonation_service_account = {}) {
    GcpConfig conf;
    conf.endpoint = "storage.googleapis.com";
    conf.region = "us-east1";
    conf.credential = GcpCredentialConfig {
            .provider_type = GcpCredentialProviderType::Default,
            .impersonation_service_account = std::move(impersonation_service_account),
    };
    return conf;
}

class FixedTokenGcpS3Client final : public GcpS3Client {
public:
    explicit FixedTokenGcpS3Client(std::string endpoint = "https://storage.googleapis.com")
            : GcpS3Client(GcpCredentialConfig {}, "", make_config(endpoint),
                          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true) {}

    using GcpS3Client::BuildHttpRequest;
    using GcpS3Client::authorize_request;

    std::optional<std::string> token = "test-gcp-token";
    mutable int token_requests = 0;

protected:
    std::optional<std::string> fetch_token() const override {
        ++token_requests;
        return token;
    }

private:
    static Aws::Client::ClientConfiguration make_config(const std::string& endpoint) {
        Aws::Client::ClientConfiguration config;
        config.endpointOverride = endpoint;
        return config;
    }
};

class SyncPointProcessingGuard {
public:
    SyncPointProcessingGuard() : _was_enabled(SyncPoint::get_instance()->get_enable()) {
        SyncPoint::get_instance()->enable_processing();
    }
    ~SyncPointProcessingGuard() {
        if (!_was_enabled) {
            SyncPoint::get_instance()->disable_processing();
        }
    }

private:
    bool _was_enabled;
};

} // namespace

TEST_F(S3ClientFactoryTest, CreatesClientForEachS3CompatibleProvider) {
    const auto settings = make_factory_conf("provider-dispatch.example.com");
    check_s3_provider_client<S3ObjStorageClient, AwsConfig>(settings);
    check_s3_provider_client<OssObjStorageClient, OssConfig>(settings);
    check_s3_provider_client<CosObjStorageClient, CosConfig>(settings);
    check_s3_provider_client<BosObjStorageClient, BosConfig>(settings);
    check_s3_provider_client<ObsObjStorageClient, ObsConfig>(settings);
    check_s3_provider_client<GcpObjStorageClient, GcpConfig>(settings);
    check_s3_provider_client<TosObjStorageClient, TosConfig>(settings);
}

TEST_F(S3ClientFactoryTest, CreatesS3CompatibleClientsFromProperties) {
    SyncPointProcessingGuard sync_point_guard;
    SyncPoint::CallbackGuard head_object_callback;
    SyncPoint::get_instance()->set_call_back(
            "s3_file_system::head_object",
            [](auto&& args) {
                auto result = try_any_cast_ret<Aws::S3::Model::HeadObjectOutcome>(args);
                Aws::S3::Model::HeadObjectResult head;
                head.SetContentLength(37);
                result->first = Aws::S3::Model::HeadObjectOutcome(std::move(head));
                result->second = true;
            },
            &head_object_callback);

    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    const struct {
        const char* provider;
        const char* endpoint;
        const std::type_info& expected_client;
    } cases[] = {
            {"OSS", "objects.example.com", typeid(OssObjStorageClient)},
            {"GCP", "storage.googleapis.com", typeid(GcpObjStorageClient)},
            {nullptr, "oss-cn-hangzhou.aliyuncs.com", typeid(OssObjStorageClient)},
            {" gCs ", "oss-cn-hangzhou.aliyuncs.com", typeid(GcpObjStorageClient)},
    };
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.provider ? entry.provider : "implicit OSS");
        std::map<std::string, std::string> props {{"AWS_ENDPOINT", entry.endpoint},
                                                  {"AWS_REGION", "region"},
                                                  {"AWS_ACCESS_KEY", "test-key"},
                                                  {"AWS_SECRET_KEY", "test-secret"}};
        if (entry.provider) {
            props["provider"] = entry.provider;
        }
        ObjStorageConfig parsed;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties(props, uri, &parsed).ok());
        auto result = ObjStorageClientFactory::instance().create(parsed);
        ASSERT_TRUE(result.has_value()) << result.error();
        const auto* client = result->get();
        ASSERT_NE(client, nullptr);
        EXPECT_EQ(typeid(*client), entry.expected_client);
        const auto head =
                (*result)->head_object({.bucket = uri.get_bucket(), .key = uri.get_key()});
        ASSERT_TRUE(head.resp.ok());
        EXPECT_EQ(37, head.file_size);
    }
}

TEST_F(S3ClientFactoryTest, CreatesAzureClientPerContainer) {
    AzureConfig config;
    config.endpoint = "https://account.blob.core.windows.net";
    config.region = "region";
    config.ak = "account";
    config.sk = "dGVzdA==";
    config.bucket = "first";
    auto& factory = ObjStorageClientFactory::instance();
    auto first = factory.create(config);
#ifdef USE_AZURE
    ASSERT_TRUE(first.has_value()) << first.error();
    EXPECT_NE(std::dynamic_pointer_cast<AzureObjStorageClient>(*first), nullptr);
    config.bucket = "second";
    auto second = factory.create(config);
    ASSERT_TRUE(second.has_value()) << second.error();
    EXPECT_NE(*first, *second);
#else
    ASSERT_FALSE(first.has_value());
    EXPECT_TRUE(first.error().is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
#endif
}

TEST_F(S3ClientFactoryTest, AzureIgnoresS3OptionsForTheSameContainer) {
    AzureConfig azure;
    azure.bucket = "container";
    azure.endpoint = "https://account.blob.core.windows.net";
    azure.region = "region";
    azure.ak = "account";
    azure.sk = "dGVzdA==";

    auto with_s3_options = azure;
    with_s3_options.need_override_endpoint = false;
    with_s3_options.use_virtual_addressing = false;
    with_s3_options.max_connections = 20;
    with_s3_options.request_timeout_ms = 1000;
    with_s3_options.connect_timeout_ms = 200;
    with_s3_options.token = "s3-token";
    with_s3_options.cred_provider_type = CredProviderType::InstanceProfile;
    with_s3_options.role_arn = "arn:aws:iam::123456789012:role/test";
    with_s3_options.external_id = "s3-external-id";

    EXPECT_NE(azure, with_s3_options);
    EXPECT_TRUE(same_storage_target(azure, with_s3_options));
    EXPECT_EQ(obj_storage_client_cache_key(azure), obj_storage_client_cache_key(with_s3_options));
    EXPECT_FALSE(validate_obj_storage_config(with_s3_options).has_value());
#ifdef USE_AZURE
    auto& factory = ObjStorageClientFactory::instance();
    auto first = factory.create(azure);
    auto second = factory.create(with_s3_options);
    ASSERT_TRUE(first.has_value()) << first.error();
    ASSERT_TRUE(second.has_value()) << second.error();
    EXPECT_EQ(*first, *second);
#endif

    with_s3_options.sk.clear();
    EXPECT_FALSE(validate_obj_storage_config(with_s3_options).has_value());
    auto without_role = with_s3_options;
    without_role.role_arn.clear();
    EXPECT_TRUE(validate_obj_storage_config(without_role).has_value());

    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [client](const ObjStorageConfig&) { return client; });
    io::ObjStorageClientHolder holder(with_s3_options);
    ASSERT_TRUE(holder.init().ok());
    EXPECT_FALSE(holder.reset(without_role).ok());
    EXPECT_EQ(holder.config(), ObjStorageConfig {with_s3_options});
}

TEST_F(S3ClientFactoryTest, SharesS3ClientAcrossFilesystemLocationsAndPolicies) {
    auto config = make_factory_conf("cache-between-buckets.example.com");
    config.bucket = "first";
    auto other = config;
    other.bucket = "second";
    other.prefix = "warehouse";
    other.sse_enabled = true;
    other.is_internal_bucket = true;

    auto& factory = ObjStorageClientFactory::instance();
    auto first = factory.create(config);
    auto second = factory.create(other);
    ASSERT_TRUE(first.has_value()) << first.error();
    ASSERT_TRUE(second.has_value()) << second.error();
    EXPECT_EQ(*first, *second);
}

TEST_F(S3ClientFactoryTest, DistinguishesHashCollisions) {
    auto external_conf =
            make_hash_collision_conf("cloud-rate-limit-hash-collision.example.com", false);
    auto internal_conf =
            make_hash_collision_conf("cloud-rate-limit-hash-collision.example.com", true);
    ASSERT_EQ(ObjStorageClientHash {}(external_conf), ObjStorageClientHash {}(internal_conf));
    ASSERT_NE(external_conf, internal_conf);

    auto& factory = ObjStorageClientFactory::instance();
    auto external_result = factory.create(external_conf);
    auto internal_result = factory.create(internal_conf);
    ASSERT_TRUE(external_result.has_value()) << external_result.error();
    ASSERT_TRUE(internal_result.has_value()) << internal_result.error();
    auto external_client = std::move(external_result).value();
    auto internal_client = std::move(internal_result).value();

    EXPECT_NE(external_client, internal_client);
    auto cached_external = factory.create(external_conf);
    auto cached_internal = factory.create(internal_conf);
    ASSERT_TRUE(cached_external.has_value()) << cached_external.error();
    ASSERT_TRUE(cached_internal.has_value()) << cached_internal.error();
    EXPECT_EQ(cached_external.value(), external_client);
    EXPECT_EQ(cached_internal.value(), internal_client);
}

TEST_F(S3ClientFactoryTest, ObjClientHolderResetDistinguishesHashCollisions) {
    auto external_conf =
            make_hash_collision_conf("s3-client-holder-hash-collision.example.com", false);
    auto internal_conf =
            make_hash_collision_conf("s3-client-holder-hash-collision.example.com", true);
    ASSERT_EQ(ObjStorageClientHash {}(external_conf), ObjStorageClientHash {}(internal_conf));

    CloudModeConfigGuard cloud_mode_guard(true);
    auto external_client =
            std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    auto internal_client =
            std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    int create_count = 0;
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageConfig& conf) -> std::shared_ptr<io::ObjStorageClient> {
                ++create_count;
                return conf == ObjStorageConfig {internal_conf} ? internal_client : external_client;
            });

    io::ObjStorageClientHolder holder(external_conf);
    ASSERT_TRUE(holder.init().ok());
    EXPECT_EQ(create_count, 1);
    EXPECT_EQ(holder.get(), external_client);

    ASSERT_TRUE(holder.reset(internal_conf).ok());
    EXPECT_EQ(create_count, 2);
    EXPECT_EQ(holder.get(), internal_client);
    EXPECT_EQ(holder.config(), ObjStorageConfig {internal_conf});
}

TEST_F(S3ClientFactoryTest, SelectsRateLimiterByDeploymentAndBucketType) {
    RateLimiterConfigGuard rate_limiter_guard;
    SyncPointProcessingGuard sync_point_guard;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard create_client_callback;
    sync_point->set_call_back(
            "s3_client_factory::create",
            [](auto&& args) {
                auto result = try_any_cast_ret<doris::ObjStorageClientBuildResult>(args);
                result->second = true;
            },
            &create_client_callback);
    SyncPoint::CallbackGuard head_object_callback;
    sync_point->set_call_back(
            "s3_file_system::head_object",
            [](auto&& args) {
                auto result = try_any_cast_ret<Aws::S3::Model::HeadObjectOutcome>(args);
                result->first =
                        Aws::S3::Model::HeadObjectOutcome(Aws::S3::Model::HeadObjectResult {});
                result->second = true;
            },
            &head_object_callback);

    config::enable_s3_rate_limiter = true;
    auto check_selection = [&](bool cloud_mode, bool internal_bucket, bool expect_limited,
                               std::string endpoint) {
        CloudModeConfigGuard cloud_mode_guard(cloud_mode);
        auto& manager = ObjStorageRateLimiterManager::instance();
        manager.qps_limiter(ObjStorageRateLimitType::GET)->reset(0, 0, 1);
        manager.bytes_limiter(ObjStorageRateLimitType::GET)->reset(0, 0, 0);

        auto conf = make_factory_conf(std::move(endpoint));
        conf.is_internal_bucket = internal_bucket;
        io::ObjStorageClientHolder holder(conf);
        ASSERT_TRUE(holder.init().ok());
        auto client = holder.get();
        EXPECT_TRUE(client->head_object({.bucket = "bucket", .key = "key"}).resp.ok());
        auto second = client->head_object({.bucket = "bucket", .key = "key"});
        if (expect_limited) {
            EXPECT_EQ(second.resp.status.code, static_cast<int>(ErrorCode::EXCEEDED_LIMIT));
            EXPECT_EQ(second.resp.http_code, 0);
            EXPECT_NE(second.resp.status.msg.find("s3 get request exceeds QPS limit"),
                      std::string::npos);
        } else {
            EXPECT_TRUE(second.resp.ok());
        }
    };

    // In cloud mode both resources share one raw client but independently bind their policy.
    check_selection(false, false, true, "non-cloud-external-rate-limit.example.com");
    check_selection(true, true, true, "shared-cloud-rate-limit.example.com");
    check_selection(true, false, false, "shared-cloud-rate-limit.example.com");
}

TEST_F(S3ClientFactoryTest, RateLimitResponseDistinguishesBytesFromProviderThrottling) {
    RateLimiterConfigGuard rate_limiter_guard;
    SyncPointProcessingGuard sync_point_guard;
    SyncPoint::CallbackGuard create_client_callback;
    SyncPoint::get_instance()->set_call_back(
            "s3_client_factory::create",
            [](auto&& args) {
                auto result = try_any_cast_ret<doris::ObjStorageClientBuildResult>(args);
                result->second = true;
            },
            &create_client_callback);

    CloudModeConfigGuard cloud_mode_guard(false);
    config::enable_s3_rate_limiter = true;
    auto& manager = ObjStorageRateLimiterManager::instance();
    manager.qps_limiter(ObjStorageRateLimitType::GET)->reset(0, 0, 0);
    manager.bytes_limiter(ObjStorageRateLimitType::GET)
            ->reset(kNoThrottleBytesPerSecond, kNoThrottleBytesPerSecond, 1);

    io::ObjStorageClientHolder holder(
            make_factory_conf("be-bytes-rate-limit-response.example.com"));
    ASSERT_TRUE(holder.init().ok());
    auto client = holder.get();
    char buffer[2];
    size_t size_return = 0;
    auto response = client->get_object({.bucket = "bucket", .key = "key"}, buffer, 0,
                                       sizeof(buffer), &size_return);

    EXPECT_EQ(response.status.code, static_cast<int>(ErrorCode::EXCEEDED_LIMIT));
    EXPECT_EQ(response.http_code, 0);
    EXPECT_NE(response.status.msg.find("s3 get request exceeds bytes limit"), std::string::npos);
}

TEST_F(S3ClientFactoryTest, ObjClientHolderResetRefreshesNativeCredential) {
    auto initial_conf = make_gcp_native_conf();
    auto rotated_conf = make_gcp_native_conf("rotated@my-project.iam.gserviceaccount.com");
    std::vector<ObjStorageConfig> created_confs;
    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageConfig& conf) -> std::shared_ptr<io::ObjStorageClient> {
                created_confs.push_back(conf);
                return client;
            });

    io::ObjStorageClientHolder holder(initial_conf);
    ASSERT_TRUE(holder.init().ok());
    ASSERT_TRUE(holder.reset(rotated_conf).ok());

    ASSERT_EQ(created_confs.size(), 2);
    EXPECT_EQ(created_confs.back(), ObjStorageConfig {rotated_conf});
    EXPECT_EQ(holder.config(), ObjStorageConfig {rotated_conf});
}

TEST_F(S3ClientFactoryTest, CredentialRotationPreservesQuotaAndInFlightClient) {
    CloudModeConfigGuard cloud_mode_guard(true);
    RateLimiterConfigGuard rate_limiter_guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = ObjStorageRateLimiterManager::instance();
    manager.qps_limiter(ObjStorageRateLimitType::GET)->reset(0, 0, 1);
    manager.bytes_limiter(ObjStorageRateLimitType::GET)->reset(0, 0, 0);

    auto conf = make_factory_conf("rotate.example.com");
    auto raw = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageConfig&) { return raw; });
    conf.is_internal_bucket = true;
    io::ObjStorageClientHolder holder(conf);
    ASSERT_TRUE(holder.init().ok());
    auto old_client = holder.get();
    SyncPointProcessingGuard sync_point_guard;
    SyncPoint::CallbackGuard head_callback;
    SyncPoint::get_instance()->set_call_back(
            "s3_file_system::head_object",
            [](auto&& args) {
                auto result = try_any_cast_ret<Aws::S3::Model::HeadObjectOutcome>(args);
                result->first =
                        Aws::S3::Model::HeadObjectOutcome(Aws::S3::Model::HeadObjectResult {});
                result->second = true;
            },
            &head_callback);
    EXPECT_TRUE(old_client->head_object({.bucket = "bucket", .key = "key"}).resp.ok());

    conf.ak = "new-ak";
    EXPECT_FALSE(holder.reset(conf).ok());
    EXPECT_EQ(holder.get(), old_client);
    EXPECT_TRUE(storage_config_base(holder.config()).ak.empty());

    conf.sk = "new-sk";
    ASSERT_TRUE(holder.reset(conf).ok());
    EXPECT_EQ(holder.config(), ObjStorageConfig {conf});
    EXPECT_NE(holder.get(), old_client);
    for (const auto& client : {old_client, holder.get()}) {
        auto response = client->head_object({.bucket = "bucket", .key = "key"}).resp;
        EXPECT_EQ(response.status.code, static_cast<int>(ErrorCode::EXCEEDED_LIMIT));
    }
}

TEST_F(S3ClientFactoryTest, ObjClientHolderKeepsClientWhenTargetChanges) {
    auto native_conf = make_gcp_native_conf();
    AwsConfig s3_conf {native_conf};
    s3_conf.credential = std::monostate {};

    std::vector<ObjStorageConfig> created_confs;
    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageConfig& conf) -> std::shared_ptr<io::ObjStorageClient> {
                created_confs.push_back(conf);
                return client;
            });

    io::ObjStorageClientHolder holder(s3_conf);
    ASSERT_TRUE(holder.init().ok());
    auto old_client = holder.get();
    EXPECT_FALSE(holder.reset(native_conf).ok());
    EXPECT_EQ(holder.get(), old_client);
    EXPECT_EQ(holder.config(), ObjStorageConfig {s3_conf});
    EXPECT_EQ(created_confs.size(), 1);
}

TEST_F(S3ClientFactoryTest, ObjClientHolderResetSwitchesNativeAndStaticCredentials) {
    auto native_conf = make_gcp_native_conf();
    auto static_conf = native_conf;
    static_conf.credential = std::monostate {};
    static_conf.ak = "access-key";
    static_conf.sk = "secret-key";

    std::vector<ObjStorageConfig> created_confs;
    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageConfig& conf) -> std::shared_ptr<io::ObjStorageClient> {
                created_confs.push_back(conf);
                return client;
            });

    io::ObjStorageClientHolder holder(native_conf);
    ASSERT_TRUE(holder.init().ok());
    ASSERT_TRUE(holder.reset(static_conf).ok());
    ASSERT_TRUE(std::holds_alternative<std::monostate>(
            std::get<GcpConfig>(holder.config()).credential));
    EXPECT_EQ(storage_config_base(holder.config()).ak, "access-key");

    ASSERT_TRUE(holder.reset(native_conf).ok());
    ASSERT_EQ(created_confs.size(), 3);
    EXPECT_EQ(created_confs[1], ObjStorageConfig {static_conf});
    EXPECT_EQ(created_confs[2], ObjStorageConfig {native_conf});
    EXPECT_EQ(holder.config(), ObjStorageConfig {native_conf});
}

TEST_F(S3ClientFactoryTest, CredentialTypeNamesAreExplicit) {
    CredentialConfig none;
    CredentialConfig gcp = GcpCredentialConfig {};

    EXPECT_EQ(credential_type_name(none), "none");
    EXPECT_EQ(credential_type_name(gcp), "gcp");
}

TEST_F(S3ClientFactoryTest, GcpBearerHeaderSurvivesAnonymousAwsSigning) {
    FixedTokenGcpS3Client client;
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket("bucket");
    request.SetKey("key");
    auto http_request = Aws::Http::CreateHttpRequest(
            Aws::Http::URI("https://storage.googleapis.com/bucket/key"),
            Aws::Http::HttpMethod::HTTP_GET,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);

    ASSERT_FALSE(client.authorize_request(request, request.GetBucket()).has_value());
    client.BuildHttpRequest(request, http_request);
    EXPECT_EQ(http_request->GetHeaderValue("Authorization"), "Bearer test-gcp-token");

    // AWSAuthV4Signer reads this before it checks for anonymous credentials.
    http_request->SetServiceSpecificParameters(
            std::make_shared<Aws::Http::ServiceSpecificParameters>());
    Aws::Client::AWSAuthV4Signer signer(
            std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(), "s3", "us-east-1");
    EXPECT_TRUE(signer.SignRequest(*http_request));
    EXPECT_EQ(http_request->GetHeaderValue("Authorization"), "Bearer test-gcp-token");
}

TEST_F(S3ClientFactoryTest, GcpRejectsUntrustedEndpointsBeforeFetchingToken) {
    for (const auto* endpoint :
         {"https://attacker.example", "http://storage.googleapis.com",
          "https://storage.googleapis.com.attacker.example",
          "https://storage.googleapis.com@attacker.example", "https://storage.googleapis.com:8443",
          "https://bucket.storage.googleapis.com",
          "https://bucket.us-central1-storage.googleapis.com",
          "https://bucket.storage.us-central1.rep.googleapis.com", ""}) {
        FixedTokenGcpS3Client client(endpoint);
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket("bucket");
        request.SetKey("key");
        auto outcome = client.GetObject(request);
        ASSERT_FALSE(outcome.IsSuccess()) << endpoint;
        EXPECT_EQ(outcome.GetError().GetExceptionName(), "GcpEndpointError");
        EXPECT_EQ(client.token_requests, 0);
    }
    FixedTokenGcpS3Client client;
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket("arn:aws:s3:us-east-1:123456789012:accesspoint/example");
    EXPECT_FALSE(client.GetObject(request).IsSuccess());
    EXPECT_EQ(client.token_requests, 0);
}

TEST_F(S3ClientFactoryTest, GcpAcceptsGoogleStorageEndpoints) {
    for (const auto* endpoint :
         {"https://storage.googleapis.com", "https://storage.googleapis.com:443/",
          "https://us-central1-storage.googleapis.com",
          "https://storage.us-central1.rep.googleapis.com"}) {
        EXPECT_TRUE(is_valid_gcp_storage_endpoint(endpoint)) << endpoint;
    }
}

TEST_F(S3ClientFactoryTest, GcpErrorLogPresigningUsesUploadedKey) {
    for (const std::string prefix : {"", "instance-prefix"}) {
        GcpConfig conf;
        conf.bucket = "test-bucket";
        conf.prefix = prefix;
        io::ObjStorageFileSystem fs(std::move(conf), "gcp-error-log-test");
        const std::string expected_key =
                prefix.empty() ? "error_log/id" : "instance-prefix/error_log/id";
        io::Path upload_path;
        ASSERT_TRUE(fs.absolute_path("error_log/id", upload_path).ok());
        S3URI upload_uri(upload_path.native());
        ASSERT_TRUE(upload_uri.parse().ok());
        ASSERT_EQ(upload_uri.get_key(), expected_key);

        fs.client_holder()->_client = std::make_shared<S3ObjStorageClient>(
                nullptr, ObjStorageEndpointInfo {},
                [&](const ObjStoragePath& path, int64_t expiration_secs) {
                    EXPECT_EQ(path.key, upload_uri.get_key());
                    GcsV4SignedUrlOptions options {
                            .endpoint = "https://storage.googleapis.com",
                            .bucket = path.bucket,
                            .key = path.key,
                            .signer_email = "signer@test.iam.gserviceaccount.com",
                            .expiration_secs = expiration_secs,
                    };
                    auto result = build_gcs_v4_signed_url(
                            options, std::chrono::system_clock::time_point {},
                            [](std::string_view) {
                                return GcsSignBlobResult {.signature = "test"};
                            });
                    EXPECT_TRUE(result.ok()) << result.error;
                    return result.signed_url;
                });
        EXPECT_TRUE(fs.generate_presigned_url("error_log/id", 60, false)
                            .starts_with("https://storage.googleapis.com/test-bucket/" +
                                         expected_key + "?"));
    }
}

TEST_F(S3ClientFactoryTest, GcpTokenFailureRejectsEveryObjectStorageOperation) {
    FixedTokenGcpS3Client client;
    for (const auto& token : {std::optional<std::string> {}, std::optional<std::string> {""}}) {
        client.token = token;
        auto check = [](const auto& outcome) {
            ASSERT_FALSE(outcome.IsSuccess());
            EXPECT_EQ(outcome.GetError().GetExceptionName(), "GcpAuthenticationError");
            EXPECT_EQ(outcome.GetError().GetResponseCode(),
                      Aws::Http::HttpResponseCode::UNAUTHORIZED);
            EXPECT_FALSE(outcome.GetError().ShouldRetry());
            EXPECT_EQ(doris::s3fs_error(outcome.GetError(), "GCS request failed").code,
                      ObjStorageStatus::PERMISSION_DENIED);
        };
        check(client.AbortMultipartUpload(Aws::S3::Model::AbortMultipartUploadRequest {}));
        check(client.CompleteMultipartUpload(Aws::S3::Model::CompleteMultipartUploadRequest {}));
        check(client.CreateMultipartUpload(Aws::S3::Model::CreateMultipartUploadRequest {}));
        check(client.DeleteObject(Aws::S3::Model::DeleteObjectRequest {}));
        check(client.DeleteObjects(Aws::S3::Model::DeleteObjectsRequest {}));
        check(client.GetBucketLifecycleConfiguration(
                Aws::S3::Model::GetBucketLifecycleConfigurationRequest {}));
        check(client.GetBucketVersioning(Aws::S3::Model::GetBucketVersioningRequest {}));
        check(client.GetObject(Aws::S3::Model::GetObjectRequest {}));
        check(client.HeadObject(Aws::S3::Model::HeadObjectRequest {}));
        check(client.ListObjectsV2(Aws::S3::Model::ListObjectsV2Request {}));
        check(client.PutObject(Aws::S3::Model::PutObjectRequest {}));
        check(client.UploadPart(Aws::S3::Model::UploadPartRequest {}));
    }
}

TEST_F(S3ClientFactoryTest, UnauthorizedObjectStoreResponseIsPermissionDenied) {
    Aws::S3::S3Error error;
    error.SetResponseCode(Aws::Http::HttpResponseCode::UNAUTHORIZED);
    error.SetExceptionName("InvalidAuthenticationCredentials");
    error.SetMessage("missing or invalid bearer token");

    auto status = doris::s3fs_error(error, "GCS request failed");
    EXPECT_EQ(status.code, ObjStorageStatus::PERMISSION_DENIED);
}

TEST_F(S3ClientFactoryTest, GcpServiceAccountEmailValidation) {
    const std::vector<std::string_view> valid_emails = {
            "target@my-project.iam.gserviceaccount.com",
            "123456789-compute@developer.gserviceaccount.com",
            "my-project@appspot.gserviceaccount.com",
    };
    for (auto email : valid_emails) {
        EXPECT_TRUE(is_valid_gcp_service_account_email(email)) << email;
    }

    const std::vector<std::string_view> invalid_emails = {
            "",
            "@my-project.iam.gserviceaccount.com",
            "target@.iam.gserviceaccount.com",
            "target@@my-project.iam.gserviceaccount.com",
            "target@evil.developer.gserviceaccount.com",
            "target@evil.appspot.gserviceaccount.com",
            "target@my-project.iam.gserviceaccount.com.evil.example",
            "target account@my-project.iam.gserviceaccount.com",
    };
    for (auto email : invalid_emails) {
        EXPECT_FALSE(is_valid_gcp_service_account_email(email)) << email;
    }
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProvider) {
    AwsConfig anonymous_conf;
    AwsConfig ak_sk_conf;
    ak_sk_conf.ak = "ak";
    ak_sk_conf.sk = "sk";

    AwsConfig role_conf1;
    role_conf1.cred_provider_type = CredProviderType::InstanceProfile;

    AwsConfig role_conf2;
    role_conf2.cred_provider_type = CredProviderType::InstanceProfile;
    role_conf2.role_arn = "role_arn";
    role_conf2.external_id = "external_id";

    AwsConfig web_identity_conf;
    web_identity_conf.cred_provider_type = CredProviderType::WebIdentity;

    config::aws_credentials_provider_version = "v2";
    {
        auto provider_v2 = create_credentials_provider(anonymous_conf).provider;
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }
    {
        auto provider_v2 = create_credentials_provider(ak_sk_conf).provider;
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }

    {
        auto provider_v2 = create_credentials_provider(role_conf1).provider;
        auto instance_profile_v2 =
                std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(
                        provider_v2);
        ASSERT_NE(instance_profile_v2, nullptr);
    }

    {
        auto provider_v2 = create_credentials_provider(role_conf2).provider;
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }

    {
        auto provider_v2 = create_credentials_provider(web_identity_conf).provider;
        auto web_identity_v2 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
                        provider_v2);
        ASSERT_NE(web_identity_v2, nullptr);
    }

    config::aws_credentials_provider_version = "v1";
    {
        auto provider_v1 = create_credentials_provider(anonymous_conf).provider;
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = create_credentials_provider(ak_sk_conf).provider;
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = create_credentials_provider(role_conf1).provider;
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(
                        provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = create_credentials_provider(role_conf2).provider;
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    config::aws_credentials_provider_version = "v2";
}

TEST_F(S3ClientFactoryTest, RefreshCaCertForCredentialsProvider) {
    CaBundleCache cache;
    auto ca_cert_file_path = std::filesystem::temp_directory_path() /
                             ("doris_s3_client_factory_ca_" + std::to_string(getpid()) + ".pem");
    std::filesystem::remove(ca_cert_file_path);
    const auto ca_path = ca_cert_file_path.string();

    AwsConfig role_conf;
    role_conf.cred_provider_type = CredProviderType::InstanceProfile;
    role_conf.role_arn = "role_arn";
    auto provider_without_ca = create_credentials_provider(role_conf, cache.path(ca_path)).provider;

    {
        std::ofstream ca_cert_file(ca_cert_file_path);
        ca_cert_file << "test CA bundle";
    }
    auto provider_with_ca = create_credentials_provider(role_conf, cache.path(ca_path)).provider;

    std::filesystem::remove(ca_cert_file_path);
    auto refreshed_ca_cert_file_path = cache.path(ca_path);

    EXPECT_NE(provider_without_ca, nullptr);
    EXPECT_NE(provider_with_ca, nullptr);
    EXPECT_EQ(refreshed_ca_cert_file_path, ca_cert_file_path.string());
}

TEST_F(S3ClientFactoryTest, SetS3ClientDefaultHttpScheme) {
    Aws::Client::ClientConfiguration client_config;
    client_config.endpointOverride = "example.com:9000";

    set_s3_client_default_http_scheme(client_config, "http");
    EXPECT_EQ(client_config.endpointOverride, "example.com:9000");
    EXPECT_EQ(client_config.scheme, Aws::Http::Scheme::HTTP);

    set_s3_client_default_http_scheme(client_config, "https");
    EXPECT_EQ(client_config.endpointOverride, "example.com:9000");
    EXPECT_EQ(client_config.scheme, Aws::Http::Scheme::HTTPS);

    client_config.endpointOverride = "http://example.com:9000";
    client_config.scheme = Aws::Http::Scheme::HTTP;
    set_s3_client_default_http_scheme(client_config, "https");
    EXPECT_EQ(client_config.endpointOverride, "http://example.com:9000");
    EXPECT_EQ(client_config.scheme, Aws::Http::Scheme::HTTP);

    client_config.endpointOverride = "https://example.com:9000";
    client_config.scheme = Aws::Http::Scheme::HTTPS;
    set_s3_client_default_http_scheme(client_config, "http");
    EXPECT_EQ(client_config.endpointOverride, "https://example.com:9000");
    EXPECT_EQ(client_config.scheme, Aws::Http::Scheme::HTTPS);
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfRoleArnProviderType) {
    std::map<std::string, std::string> properties {
            {"AWS_ENDPOINT", "s3.us-west-2.amazonaws.com"},
            {"AWS_REGION", "us-west-2"},
            {"AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role"},
    };

    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    ObjStorageConfig s3_conf;
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    ASSERT_EQ(storage_config_base(s3_conf).endpoint, properties.at("AWS_ENDPOINT"));
    ASSERT_EQ(std::get<AwsConfig>(s3_conf).cred_provider_type, CredProviderType::Default);

    properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = "WEB_IDENTITY";
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    ASSERT_EQ(std::get<AwsConfig>(s3_conf).cred_provider_type, CredProviderType::WebIdentity);
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfProviderTypeMatrix) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    std::map<std::string, std::string> base_properties {
            {"AWS_ENDPOINT", "s3.us-west-2.amazonaws.com"},
            {"AWS_REGION", "us-west-2"},
    };

    struct TestCase {
        const char* provider_type;
        CredProviderType expected;
    };

    std::vector<TestCase> cases = {
            {"DEFAULT", CredProviderType::Default},
            {"ENV", CredProviderType::Env},
            {"SYSTEM_PROPERTIES", CredProviderType::SystemProperties},
            {"WEB_IDENTITY", CredProviderType::WebIdentity},
            {"CONTAINER", CredProviderType::Container},
            {"INSTANCE_PROFILE", CredProviderType::InstanceProfile},
            {"ANONYMOUS", CredProviderType::Anonymous},
    };

    for (const auto& test_case : cases) {
        ObjStorageConfig s3_conf;
        auto properties = base_properties;
        properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = test_case.provider_type;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok())
                << "provider_type=" << test_case.provider_type;
        ASSERT_EQ(std::get<AwsConfig>(s3_conf).cred_provider_type, test_case.expected)
                << "provider_type=" << test_case.provider_type;
    }
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfGcpCredentialModes) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    std::map<std::string, std::string> base_properties {
            {"AWS_ENDPOINT", "storage.googleapis.com"},
            {"AWS_REGION", "us-east1"},
            {"provider", "GCP"},
    };

    struct TestCase {
        const char* provider_type;
        const char* impersonation_service_account;
        GcpCredentialProviderType expected_provider_type;
    };

    // Both ADC and Compute Engine metadata credentials may be used directly or
    // as the source credential for service-account impersonation.
    std::vector<TestCase> cases = {
            {"DEFAULT", "", GcpCredentialProviderType::Default},
            {"default", "", GcpCredentialProviderType::Default},
            {"DEFAULT", "target@my-project.iam.gserviceaccount.com",
             GcpCredentialProviderType::Default},
            {"COMPUTE_ENGINE", "", GcpCredentialProviderType::ComputeEngine},
            {" compute_engine ", "", GcpCredentialProviderType::ComputeEngine},
            {"COMPUTE_ENGINE", "target@my-project.iam.gserviceaccount.com",
             GcpCredentialProviderType::ComputeEngine},
    };

    for (const auto& test_case : cases) {
        auto properties = base_properties;
        properties["gs.credential_provider_type"] = test_case.provider_type;
        if (test_case.impersonation_service_account[0] != '\0') {
            properties["gs.impersonation_service_account"] =
                    test_case.impersonation_service_account;
        }

        ObjStorageConfig s3_conf;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok())
                << "provider_type=" << test_case.provider_type;
        ASSERT_EQ(storage_provider(s3_conf), io::ObjStorageProvider::GCP);
        ASSERT_TRUE(std::holds_alternative<GcpCredentialConfig>(
                std::get<GcpConfig>(s3_conf).credential));
        const auto& credential =
                std::get<GcpCredentialConfig>(std::get<GcpConfig>(s3_conf).credential);
        EXPECT_EQ(credential.provider_type, test_case.expected_provider_type);
        EXPECT_EQ(credential.impersonation_service_account,
                  test_case.impersonation_service_account);
    }
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfGcpDefaultImpersonation) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());
    std::map<std::string, std::string> properties {
            {"AWS_ENDPOINT", "storage.googleapis.com"},
            {"AWS_REGION", "us-east1"},
            {"provider", "GCP"},
            {"gs.impersonation_service_account", "target@my-project.iam.gserviceaccount.com"},
    };
    ObjStorageConfig s3_conf;
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    const auto& credential = std::get<GcpCredentialConfig>(std::get<GcpConfig>(s3_conf).credential);
    EXPECT_EQ(credential.provider_type, GcpCredentialProviderType::Default);
    EXPECT_EQ(credential.impersonation_service_account,
              "target@my-project.iam.gserviceaccount.com");

    properties["gs.impersonation_service_account"] = "";
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    const auto& direct_credential =
            std::get<GcpCredentialConfig>(std::get<GcpConfig>(s3_conf).credential);
    EXPECT_EQ(direct_credential.provider_type, GcpCredentialProviderType::Default);
    EXPECT_TRUE(direct_credential.impersonation_service_account.empty());
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfRejectsConflictingGcpCredentials) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    std::map<std::string, std::string> base_properties {
            {"AWS_ENDPOINT", "storage.googleapis.com"},
            {"AWS_REGION", "us-east1"},
            {"provider", "GCP"},
            {"gs.credential_provider_type", "DEFAULT"},
    };

    const std::vector<std::pair<std::string, std::string>> conflicts = {
            {"AWS_ACCESS_KEY", "ak"},
            {"AWS_SECRET_KEY", "sk"},
            {"AWS_TOKEN", "token"},
            {"AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role"},
            {"AWS_EXTERNAL_ID", "external-id"},
            {"AWS_CREDENTIALS_PROVIDER_TYPE", "INSTANCE_PROFILE"},
    };
    for (const auto& [key, value] : conflicts) {
        auto properties = base_properties;
        properties[key] = value;
        ObjStorageConfig s3_conf;
        auto status = ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf);
        ASSERT_FALSE(status.ok()) << key;
        EXPECT_NE(status.to_string().find("cannot be combined"), std::string::npos) << key;
    }
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfRejectsInvalidGcpCredentialModes) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    std::map<std::string, std::string> base_properties {
            {"AWS_ENDPOINT", "storage.googleapis.com"},
            {"AWS_REGION", "us-east1"},
            {"provider", "GCP"},
    };
    for (const auto& provider_type : {"ADC", "", "unknown"}) {
        auto properties = base_properties;
        properties["gs.credential_provider_type"] = provider_type;
        ObjStorageConfig s3_conf;
        auto status = ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf);
        ASSERT_FALSE(status.ok()) << provider_type;
        EXPECT_NE(status.to_string().find("gs.credential_provider_type"), std::string::npos)
                << provider_type;
        EXPECT_NE(status.to_string().find("DEFAULT"), std::string::npos) << provider_type;
        EXPECT_NE(status.to_string().find("COMPUTE_ENGINE"), std::string::npos) << provider_type;
    }

    for (const auto& account : {" ", "invalid-account"}) {
        auto properties = base_properties;
        properties["gs.impersonation_service_account"] = account;
        ObjStorageConfig s3_conf;
        ASSERT_FALSE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    }
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfRejectsGcpCredentialWithOtherProviders) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    for (const auto& provider : {"S3", "AWS", "AZURE"}) {
        std::map<std::string, std::string> properties {
                {"AWS_ENDPOINT", "storage.googleapis.com"},
                {"AWS_REGION", "us-east1"},
                {"provider", provider},
                {"gs.credential_provider_type", "DEFAULT"},
        };
        ObjStorageConfig s3_conf;
        auto status = ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf);
        ASSERT_FALSE(status.ok()) << provider;
        EXPECT_NE(status.to_string().find("provider=GCP"), std::string::npos) << provider;
    }
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfCredentialValidation) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    std::map<std::string, std::string> base_properties {
            {"AWS_ENDPOINT", "s3.us-west-2.amazonaws.com"},
            {"AWS_REGION", "us-west-2"},
    };
    {
        auto properties = base_properties;
        properties["AWS_ACCESS_KEY"] = "ak";
        ObjStorageConfig s3_conf;
        ASSERT_FALSE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        properties["AWS_SECRET_KEY"] = "sk";
        ObjStorageConfig s3_conf;
        ASSERT_FALSE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        ObjStorageConfig s3_conf;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        ObjStorageConfig s3_conf;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        properties["AWS_ACCESS_KEY"] = "ak";
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        ObjStorageConfig s3_conf;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        properties["AWS_SECRET_KEY"] = "sk";
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        ObjStorageConfig s3_conf;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties(properties, s3_uri, &s3_conf).ok());
    }
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2ProviderTypeWithoutRoleArn) {
    config::aws_credentials_provider_version = "v2";

    AwsConfig default_conf;
    default_conf.cred_provider_type = CredProviderType::Default;
    auto provider = create_credentials_provider(default_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(provider), nullptr);

    AwsConfig env_conf;
    env_conf.cred_provider_type = CredProviderType::Env;
    provider = create_credentials_provider(env_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::EnvironmentAWSCredentialsProvider>(provider),
              nullptr);

    AwsConfig sys_conf;
    sys_conf.cred_provider_type = CredProviderType::SystemProperties;
    provider = create_credentials_provider(sys_conf).provider;
    ASSERT_NE(
            std::dynamic_pointer_cast<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(provider),
            nullptr);

    AwsConfig web_identity_conf;
    web_identity_conf.cred_provider_type = CredProviderType::WebIdentity;
    provider = create_credentials_provider(web_identity_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
                      provider),
              nullptr);

    {
        // The environment is pinned rather than merely topped up, so that this case asserts the
        // same thing whether or not the machine running it happens to be inside a container.
        ContainerCredentialsEnvGuard env;
        env.set_ecs_task_role("/v2/credentials/mock");
        AwsConfig container_conf;
        container_conf.cred_provider_type = CredProviderType::Container;
        provider = create_credentials_provider(container_conf).provider;
        ASSERT_NE(as_valid_http_provider(provider), nullptr);
    }

    AwsConfig instance_profile_conf;
    instance_profile_conf.cred_provider_type = CredProviderType::InstanceProfile;
    provider = create_credentials_provider(instance_profile_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(provider),
              nullptr);

    AwsConfig anonymous_conf;
    anonymous_conf.cred_provider_type = CredProviderType::Anonymous;
    provider = create_credentials_provider(anonymous_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider),
              nullptr);
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2WithRoleArnAlwaysAssumeRole) {
    config::aws_credentials_provider_version = "v2";

    std::vector<CredProviderType> provider_types = {
            CredProviderType::Default,          CredProviderType::Env,
            CredProviderType::SystemProperties, CredProviderType::WebIdentity,
            CredProviderType::Container,        CredProviderType::InstanceProfile,
            CredProviderType::Anonymous,        CredProviderType::Simple,
    };

    for (auto provider_type : provider_types) {
        AwsConfig conf;
        conf.cred_provider_type = provider_type;
        conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
        conf.external_id = "external-id";
        auto provider = create_credentials_provider(conf).provider;
        ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider),
                  nullptr);
    }
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2SimpleWithoutAkSkUsesDefaultChain) {
    config::aws_credentials_provider_version = "v2";

    AwsConfig conf;
    conf.cred_provider_type = CredProviderType::Simple;
    auto result = create_credentials_provider(conf);

    ASSERT_TRUE(result);
    EXPECT_NE(std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(result.provider),
              nullptr);
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderAkSkTakePrecedenceOverRoleArn) {
    AwsConfig conf;
    conf.ak = "ak";
    conf.sk = "sk";
    conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
    conf.external_id = "external-id";
    conf.cred_provider_type = CredProviderType::InstanceProfile;

    config::aws_credentials_provider_version = "v2";
    auto provider_v2 = create_credentials_provider(conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v2),
              nullptr);

    config::aws_credentials_provider_version = "v1";
    auto provider_v1 = create_credentials_provider(conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v1),
              nullptr);

    config::aws_credentials_provider_version = "v2";
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV1RoleArnDefaultFallback) {
    config::aws_credentials_provider_version = "v1";

    AwsConfig conf;
    conf.cred_provider_type = CredProviderType::Default;
    conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
    auto provider = create_credentials_provider(conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider),
              nullptr);

    config::aws_credentials_provider_version = "v2";
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV1PartialCredentialsUseDefaultChain) {
    config::aws_credentials_provider_version = "v1";

    for (bool provide_access_key : {false, true}) {
        AwsConfig conf;
        conf.cred_provider_type = CredProviderType::Default;
        conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
        conf.ak = provide_access_key ? "ak" : "";
        conf.sk = provide_access_key ? "" : "sk";

        auto provider = create_credentials_provider(conf).provider;
        EXPECT_NE(
                std::dynamic_pointer_cast<Aws::Auth::DefaultAWSCredentialsProviderChain>(provider),
                nullptr);
    }

    config::aws_credentials_provider_version = "v2";
}

namespace {

// The provider is looked up inside the chain rather than constructed directly,
// because what is under test is how CustomAwsCredentialsProviderChain wires the
// environment.
Aws::Auth::GeneralHTTPCredentialsProvider* find_http_provider(
        const CustomAwsCredentialsProviderChain& chain) {
    for (const auto& provider : chain.GetProviders()) {
        auto* http_provider =
                dynamic_cast<Aws::Auth::GeneralHTTPCredentialsProvider*>(provider.get());
        if (http_provider != nullptr) {
            return http_provider;
        }
    }
    return nullptr;
}

} // namespace

// EKS Pod Identity supplies the credential-endpoint token as a file that the
// kubelet rotates in place, and never as a plain environment variable. The chain
// must therefore hand the provider the token path, so that every refresh picks
// up the current contents. Passing the value read at construction time works
// until the first rotation and then fails with AccessDenied.
TEST_F(S3ClientFactoryTest, CustomChainReadsRotatedTokenFileForPodIdentity) {
    ContainerCredentialsEndpoint endpoint;
    ASSERT_TRUE(endpoint.start());

    ContainerCredentialsEnvGuard env;
    const std::string token_path = env.token_file_path("custom_chain");
    env.write_token_file(token_path, "token-one");
    env.set_pod_identity(endpoint.url(), token_path);

    CustomAwsCredentialsProviderChain chain;
    auto* provider = find_http_provider(chain);
    ASSERT_NE(provider, nullptr) << "no GeneralHTTPCredentialsProvider was added to the chain";

    // Each GetAWSCredentials() drives one HTTP GET to the endpoint: the provider re-reads the token
    // file, sends it as the Authorization header, and parses the credentials from the reply. The
    // handler records the header it saw, so auth_headers() reports what actually went over the wire.
    const auto first_credentials = provider->GetAWSCredentials();
    EXPECT_EQ(first_credentials.GetAWSAccessKeyId(), "AKIDTEST");
    EXPECT_EQ(first_credentials.GetSessionToken(), "SESSIONTEST");

    // Rotate the file the way the kubelet does. The second call refetches rather than serving its
    // cache only because the handler reports an already-expired Expiration.
    env.write_token_file(token_path, "token-two");
    provider->GetAWSCredentials();

    // GE rather than EQ: the credentials client retries and the handler records every attempt, so an
    // exact count would make a retry look like a bug. front/back keep the assertion at full strength
    // because every request before the rewrite carries token-one and every one after carries token-two.
    const auto auth_headers = endpoint.auth_headers();
    ASSERT_GE(auth_headers.size(), 2u);
    EXPECT_EQ(auth_headers.front(), "token-one");
    EXPECT_EQ(auth_headers.back(), "token-two");
}

// The same wiring has to hold when CONTAINER is asked for by name rather than reached through the
// default chain, which is the whole point of a public provider mode. Reading only
// AWS_CONTAINER_CREDENTIALS_RELATIVE_URI leaves this provider with an empty URI, no HTTP client and
// no credentials on every standard EKS deployment.
TEST_F(S3ClientFactoryTest, ContainerProviderTypeReadsRotatedTokenFileForPodIdentity) {
    config::aws_credentials_provider_version = "v2";

    ContainerCredentialsEndpoint endpoint;
    ASSERT_TRUE(endpoint.start());

    ContainerCredentialsEnvGuard env;
    const std::string token_path = env.token_file_path("container_type");
    env.write_token_file(token_path, "token-one");
    env.set_pod_identity(endpoint.url(), token_path);

    AwsConfig conf;
    conf.cred_provider_type = CredProviderType::Container;
    auto provider = as_valid_http_provider(create_credentials_provider(conf).provider);
    ASSERT_NE(provider, nullptr) << "CONTAINER did not yield a usable container credentials "
                                    "provider for AWS_CONTAINER_CREDENTIALS_FULL_URI";

    const auto first_credentials = provider->GetAWSCredentials();
    EXPECT_EQ(first_credentials.GetAWSAccessKeyId(), "AKIDTEST");
    EXPECT_EQ(first_credentials.GetSessionToken(), "SESSIONTEST");

    env.write_token_file(token_path, "token-two");
    provider->GetAWSCredentials();

    const auto auth_headers = endpoint.auth_headers();
    ASSERT_GE(auth_headers.size(), 2u);
    EXPECT_EQ(auth_headers.front(), "token-one");
    EXPECT_EQ(auth_headers.back(), "token-two");
}

TEST_F(S3ClientFactoryTest, ContainerProviderTypeForwardsInlineAuthorizationToken) {
    config::aws_credentials_provider_version = "v2";

    ContainerCredentialsEndpoint endpoint;
    ASSERT_TRUE(endpoint.start());

    ContainerCredentialsEnvGuard env;
    env.set_inline_token_endpoint(endpoint.url(), "inline-token");

    AwsConfig conf;
    conf.cred_provider_type = CredProviderType::Container;
    auto provider = as_valid_http_provider(create_credentials_provider(conf).provider);
    ASSERT_NE(provider, nullptr) << "CONTAINER did not yield a usable container credentials "
                                    "provider for an inline authorization token";

    EXPECT_EQ(provider->GetAWSCredentials().GetAWSAccessKeyId(), "AKIDTEST");

    const auto auth_headers = endpoint.auth_headers();
    ASSERT_GE(auth_headers.size(), 1u);
    EXPECT_EQ(auth_headers.front(), "inline-token");
}

TEST_F(S3ClientFactoryTest, ContainerProviderTypeStillHonoursEcsRelativeUri) {
    config::aws_credentials_provider_version = "v2";

    ContainerCredentialsEnvGuard env;
    env.set_ecs_task_role("/v2/credentials/mock");

    AwsConfig conf;
    conf.cred_provider_type = CredProviderType::Container;
    EXPECT_NE(as_valid_http_provider(create_credentials_provider(conf).provider), nullptr)
            << "CONTAINER did not yield a usable container credentials provider for "
               "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
}

// With neither URI exported there is nothing to talk to, and the provider says so. This pins the
// discriminator the two tests above rely on: IsValid() really does distinguish a wired provider
// from an inert one, so their assertions cannot pass vacuously.
TEST_F(S3ClientFactoryTest, ContainerProviderTypeIsUnusableWithoutAnyUri) {
    config::aws_credentials_provider_version = "v2";

    // Constructing the guard is the whole setup: it clears all four AWS_CONTAINER_* variables, so
    // this runs as if on a host that is not a container at all.
    ContainerCredentialsEnvGuard env;

    AwsConfig conf;
    conf.cred_provider_type = CredProviderType::Container;
    auto provider = create_credentials_provider(conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::GeneralHTTPCredentialsProvider>(provider),
              nullptr);
    EXPECT_EQ(as_valid_http_provider(provider), nullptr);
}

// With a role_arn the CONTAINER provider is not handed to the S3 client directly: it becomes the
// base provider the STS client authenticates its AssumeRole call with. AwsCredentialFactory is
// asked for that base provider here - a v2 request with no role_arn returns it unwrapped - so the
// assertion is about the base itself rather than the STS wrapper around it.
TEST_F(S3ClientFactoryTest, ContainerProviderTypeIsUsableAsStsBaseProvider) {
    ContainerCredentialsEndpoint endpoint;
    ASSERT_TRUE(endpoint.start());

    ContainerCredentialsEnvGuard env;
    const std::string token_path = env.token_file_path("sts_base");
    env.write_token_file(token_path, "token-one");
    env.set_pod_identity(endpoint.url(), token_path);

    AwsCredentialOptions options;
    options.version = AwsCredentialProviderVersion::V2;
    options.provider_type = CredProviderType::Container;
    // role_arn stays empty on purpose: that is what makes create() hand back the base provider
    // itself instead of the STS wrapper built on top of it.
    auto base_provider = as_valid_http_provider(AwsCredentialFactory::create(options).provider);
    ASSERT_NE(base_provider, nullptr)
            << "CONTAINER did not yield a usable STS base credentials provider";

    EXPECT_EQ(base_provider->GetAWSCredentials().GetAWSAccessKeyId(), "AKIDTEST");
    const auto auth_headers = endpoint.auth_headers();
    ASSERT_FALSE(auth_headers.empty())
            << "the STS base provider never contacted the credentials endpoint";
    EXPECT_EQ(auth_headers.back(), "token-one");
}

} // namespace doris
