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

#include "cpp/obj-storage/client/obj_storage_client_factory.h"

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/GeneralHTTPCredentialsProvider.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/S3Client.h>
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
#include <gen_cpp/AgentService_types.h>
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
#include <variant>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/custom_aws_credentials_provider_chain.h"
#include "cpp/obj-storage/auth/aws/aws_credential_factory.h"
#include "cpp/obj-storage/auth/ca_bundle_cache.h"
#include "cpp/obj-storage/auth/gcp/gcp_auth.h"
#include "cpp/obj-storage/auth/gcp/gcp_oauth_s3_client.h"
#include "cpp/obj-storage/auth/gcp/gcs_signed_url.h"
#include "cpp/obj-storage/client/bos_obj_storage_client.h"
#include "cpp/obj-storage/client/cos_obj_storage_client.h"
#include "cpp/obj-storage/client/gcp_obj_storage_client.h"
#include "cpp/obj-storage/client/obs_obj_storage_client.h"
#include "cpp/obj-storage/client/oss_obj_storage_client.h"
#include "cpp/obj-storage/client/s3_obj_storage_client.h"
#include "cpp/obj-storage/client/s3_util.h"
#include "cpp/obj-storage/client/tos_obj_storage_client.h"
#include "cpp/obj-storage/parser/obj_storage_config_parser.h"
#include "cpp/obj-storage/rate_limit/rate_limited_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "io/fs/err_utils.h"
#include "io/fs/obj_storage_file_system.h"
#include "io/object_storage/be_obj_storage_config_parser.h"
#include "io/object_storage/obj_storage_client_holder.h"
#include "io/object_storage/obj_storage_rate_limiter_manager.h"
#include "testutil/container_credentials_endpoint.h"
#include "util/s3_uri.h"

#ifdef USE_AZURE
#include <azure/core/http/curl_transport.hpp>

#include "cpp/obj-storage/client/azure_obj_storage_client.h"
#endif

namespace doris {

class S3ClientFactoryTest : public testing::Test {
protected:
    using Properties = std::map<std::string, std::string>;

    static AwsCredentialResult create_credentials_provider(
            Properties properties, const std::string& ca_path = be_ca_bundle_path()) {
        properties.emplace("AWS_ENDPOINT", "https://s3.us-east-1.amazonaws.com");
        properties.emplace("AWS_REGION", "us-east-1");
        S3URI uri("s3://credentials/test");
        if (auto status = uri.parse(); !status.ok()) {
            return {.error = status.to_string()};
        }
        auto parsed = ObjStorageConfigParser::from_properties(
                properties, uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        if (auto* error = std::get_if<std::string>(&parsed)) {
            return {.error = *error};
        }
        auto resource = std::get<ObjStorageResourceConfig>(std::move(parsed));
        apply_be_obj_storage_defaults(resource.client);
        auto& config = std::get<AwsObjStorageConfig>(resource.client);
        config.connection.ca_cert_file = ca_path;
        return AwsCredentialFactory::create(config.auth, config.connection);
    }

    std::string previous_version = config::aws_credentials_provider_version;

    void TearDown() override {
        config::aws_credentials_provider_version = previous_version;
        ObjStorageClientFactory::instance().clear_client_creator_for_test();
    }
};

namespace {

constexpr size_t kNoThrottleBytesPerSecond = 1ULL << 40;

AwsObjStorageConfig make_factory_conf(std::string endpoint) {
    return {.connection = {.endpoint = std::move(endpoint)}, .region = "us-east-1"};
}

AwsObjStorageConfig make_timeout_conf(std::string endpoint, bool swapped) {
    auto config = make_factory_conf(std::move(endpoint));
    config.connection.connect_timeout = std::chrono::milliseconds(swapped ? 200 : 100);
    config.connection.request_timeout = std::chrono::milliseconds(swapped ? 100 : 200);
    return config;
}

template <typename Client, typename Config>
void check_s3_provider_client() {
    Config config;
    config.connection.endpoint = "https://objects.example.com";
    config.region = "us-east-1";
    auto result = ObjStorageClientFactory::instance().create(config);
    ASSERT_NE(result.client, nullptr) << result.error;
    auto* client = result.client.get();
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

GcpObjStorageConfig make_gcp_native_conf(std::optional<std::string> account = std::nullopt) {
    return {.connection = {.endpoint = "https://storage.googleapis.com"},
            .auth = GcpNativeObjStorageAuth {.impersonation_service_account = std::move(account)},
            .region = "us-east1"};
}

class FixedTokenGcpOAuthS3Client final : public GcpOAuthS3Client {
public:
    explicit FixedTokenGcpOAuthS3Client(std::string endpoint = "https://storage.googleapis.com")
            : GcpOAuthS3Client(GcpCredentialConfig {}, "", make_config(endpoint),
                               Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true) {}

    using GcpOAuthS3Client::BuildHttpRequest;
    using GcpOAuthS3Client::authorize_request;

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
    check_s3_provider_client<S3ObjStorageClient, AwsObjStorageConfig>();
    check_s3_provider_client<OssObjStorageClient, OssObjStorageConfig>();
    check_s3_provider_client<CosObjStorageClient, CosObjStorageConfig>();
    check_s3_provider_client<BosObjStorageClient, BosObjStorageConfig>();
    check_s3_provider_client<ObsObjStorageClient, ObsObjStorageConfig>();
    check_s3_provider_client<GcpObjStorageClient, GcpObjStorageConfig>();
    check_s3_provider_client<TosObjStorageClient, TosObjStorageConfig>();
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
            {nullptr, "oss-cn-hangzhou.aliyuncs.com", typeid(S3ObjStorageClient)},
            {" gCs ", "oss-cn-hangzhou.aliyuncs.com", typeid(GcpObjStorageClient)},
    };
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.provider ? entry.provider : "default S3");
        std::map<std::string, std::string> props {{"AWS_ENDPOINT", entry.endpoint},
                                                  {"AWS_REGION", "region"},
                                                  {"AWS_ACCESS_KEY", "test-key"},
                                                  {"AWS_SECRET_KEY", "test-secret"}};
        if (entry.provider) {
            props["provider"] = entry.provider;
        }
        auto parsed_result = ObjStorageConfigParser::from_properties(
                props, uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(parsed_result))
                << std::get<std::string>(parsed_result);
        auto parsed = std::get<ObjStorageResourceConfig>(std::move(parsed_result));
        apply_be_obj_storage_defaults(parsed.client);
        auto result =
                ObjStorageClientFactory::instance().create(parsed.client, true, parsed.bucket);
        ASSERT_NE(result.client, nullptr) << result.error;
        const auto* client = result.client.get();
        EXPECT_EQ(typeid(*client), entry.expected_client);
        const auto head =
                result.client->head_object({.bucket = uri.get_bucket(), .key = uri.get_key()});
        ASSERT_TRUE(head.resp.ok());
        EXPECT_EQ(37, head.file_size);
    }
}

TEST_F(S3ClientFactoryTest, LegacyS3TimeoutsPreserveSdkDefaults) {
    const auto defaults = be_default_aws_client_configuration();
    for (int timeout_ms : {-1, 0, 5000}) {
        SCOPED_TRACE(timeout_ms);
        Properties properties {{"AWS_ENDPOINT", "https://endpoint"},
                               {"AWS_REGION", "region"},
                               {"AWS_REQUEST_TIMEOUT_MS", std::to_string(timeout_ms)},
                               {"AWS_CONNECTION_TIMEOUT_MS", std::to_string(timeout_ms)}};
        TS3StorageParam param;
        param.__set_provider(TObjStorageType::AWS);
        param.__set_bucket("bucket");
        param.__set_endpoint("https://endpoint");
        param.__set_region("region");
        param.__set_request_timeout_ms(timeout_ms);
        param.__set_conn_timeout_ms(timeout_ms);
        for (const auto& parsed : {parse_be_obj_storage_config(properties, "bucket"),
                                   parse_be_obj_storage_config(param)}) {
            ASSERT_TRUE(parsed.has_value()) << parsed.error();
            const auto& config = std::get<AwsObjStorageConfig>(parsed->client);
            const auto options =
                    make_s3_sdk_config(config.connection, config.region, config.retry_slow_down);
            EXPECT_EQ(options.requestTimeoutMs, timeout_ms > 0 ? timeout_ms : 30000);
            EXPECT_EQ(options.connectTimeoutMs,
                      timeout_ms > 0 ? timeout_ms : defaults.connectTimeoutMs);
        }
    }
}

#ifdef USE_AZURE
TEST_F(S3ClientFactoryTest, AzureLegacyConnectTimeoutKeepsSdkDefault) {
    TS3StorageParam param;
    param.__set_provider(TObjStorageType::AZURE);
    param.__set_bucket("container");
    param.__set_endpoint("https://account.blob.core.windows.net");
    param.__set_ak("account");
    param.__set_sk("dGVzdA==");
    // Omitted Thrift fields still carry the 1000ms default sent by existing resources.
    EXPECT_EQ(param.conn_timeout_ms, 1000);
    SyncPointProcessingGuard sync_point_guard;
    for (int timeout_ms : {param.conn_timeout_ms, 0, 3000}) {
        SCOPED_TRACE(timeout_ms);
        param.__set_conn_timeout_ms(timeout_ms);
        for (bool with_ca : {false, true}) {
            SCOPED_TRACE(with_ca);
            auto parsed = parse_be_obj_storage_config(param);
            ASSERT_TRUE(parsed.has_value()) << parsed.error();
            auto& config = std::get<AzureObjStorageConfig>(parsed->client);
            config.connection.ca_cert_file =
                    with_ca ? std::make_optional<std::string>("/test/ca.pem") : std::nullopt;
            bool checked_options = false;
            SyncPoint::CallbackGuard options_callback;
            SyncPoint::get_instance()->set_call_back(
                    "AzureObjStorageClient::client_options",
                    [&](auto&& args) {
                        checked_options = true;
                        const auto* options =
                                try_any_cast<Azure::Storage::Blobs::BlobClientOptions*>(args[0]);
                        if (with_ca) {
                            auto transport =
                                    std::dynamic_pointer_cast<Azure::Core::Http::CurlTransport>(
                                            options->Transport.Transport);
                            ASSERT_NE(transport, nullptr);
                            EXPECT_EQ(transport->m_options.CAInfo, "/test/ca.pem");
                            EXPECT_EQ(transport->m_options.ConnectionTimeout,
                                      std::chrono::minutes(5));
                        } else {
                            EXPECT_EQ(options->Transport.Transport, nullptr);
                        }
                    },
                    &options_callback);
            auto built = AzureObjStorageClient::create(config);
            ASSERT_NE(built.client, nullptr) << built.error;
            EXPECT_TRUE(checked_options);
        }
    }
}
#endif

TEST_F(S3ClientFactoryTest, AzureResourcesCacheServiceClientsPerContainer) {
    AzureObjStorageConfig azure {
            .connection = {.endpoint = "https://account.blob.core.windows.net"},
            .auth = {.account_name = "account", .credential = AzureSharedKeyConfig {"dGVzdA=="}}};
    ObjStorageResourceConfig first {.bucket = "one", .client = azure};
    ObjStorageResourceConfig second {.bucket = "two", .prefix = "data", .client = azure};
    auto& factory = ObjStorageClientFactory::instance();
    auto result = factory.create(first.client, true, first.bucket);
#ifdef USE_AZURE
    ASSERT_NE(result.client, nullptr) << result.error;
    EXPECT_NE(std::dynamic_pointer_cast<AzureObjStorageClient>(result.client), nullptr);
    auto reused = factory.create(first.client, true, first.bucket);
    ASSERT_NE(reused.client, nullptr) << reused.error;
    EXPECT_EQ(result.client, reused.client);
    auto separate = factory.create(second.client, true, second.bucket);
    ASSERT_NE(separate.client, nullptr) << separate.error;
    EXPECT_NE(result.client, separate.client);
#else
    ASSERT_EQ(result.client, nullptr);
    EXPECT_TRUE(result.unsupported);
    EXPECT_FALSE(result.error.empty());
#endif
}

TEST_F(S3ClientFactoryTest, AzureDiscardsS3AddressingAndRoleProperties) {
    S3URI uri("s3://container/blob");
    ASSERT_TRUE(uri.parse().ok());
    Properties props {{"provider", "AZURE"},
                      {"AWS_ENDPOINT", "https://account.blob.core.windows.net"},
                      {"AWS_ACCESS_KEY", "account"},
                      {"AWS_SECRET_KEY", "dGVzdA=="}};
    auto first_result = ObjStorageConfigParser::from_properties(
            props, uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(first_result))
            << std::get<std::string>(first_result);
    auto first = std::get<ObjStorageResourceConfig>(std::move(first_result));
    apply_be_obj_storage_defaults(first.client);
    props.insert({{"AWS_ROLE_ARN", "unused"},
                  {"AWS_EXTERNAL_ID", "unused"},
                  {"AWS_REGION", "dummy_region"},
                  {"use_path_style", "true"},
                  {"AWS_NEED_OVERRIDE_ENDPOINT", "false"}});
    auto second_result = ObjStorageConfigParser::from_properties(
            props, uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(second_result))
            << std::get<std::string>(second_result);
    auto second = std::get<ObjStorageResourceConfig>(std::move(second_result));
    apply_be_obj_storage_defaults(second.client);
    EXPECT_EQ(first.client, second.client);
    auto invalid = std::get<AzureObjStorageConfig>(second.client);
    std::get<AzureSharedKeyConfig>(invalid.auth.credential).account_key.clear();
    EXPECT_EQ(ObjStorageClientFactory::instance().create(invalid).client, nullptr);
}

TEST_F(S3ClientFactoryTest, OssPublicSigningPreservesLegacyAwsAuthentication) {
    class RecordingSignedUrlClient final : public S3ObjStorageClient {
    public:
        RecordingSignedUrlClient() : S3ObjStorageClient(nullptr) {}

        std::string generate_presigned_url(const ObjStoragePath& path,
                                           int64_t expiration_secs) override {
            signed_path = path;
            signed_expiration_secs = expiration_secs;
            return "signed-public-url";
        }

        ObjStoragePath signed_path;
        int64_t signed_expiration_secs = 0;
    };

    for (bool assume_role : {false, true}) {
        SCOPED_TRACE(assume_role);
        auto config = make_factory_conf("https://oss-cn-beijing-internal.aliyuncs.com");
        config.connection.ca_cert_file = "/test/ca-bundle.pem";
        config.connection.max_connections = 123;
        config.connection.connect_timeout = std::chrono::milliseconds(456);
        config.connection.request_timeout = std::chrono::milliseconds(789);
        config.connection.max_retries = 4;
        config.auth.source = AwsCredentialChainType::Environment;
        config.auth.implementation = AwsCredentialImplementation::V2;
        config.use_path_style = true;
        config.retry_slow_down = true;
        if (assume_role) {
            config.auth.assume_role =
                    AwsAssumeRole {.role_arn = "arn:aws:iam::123456789012:role/legacy-oss",
                                   .external_id = "legacy-external-id",
                                   .request_timeout = std::chrono::milliseconds(321)};
        }
        auto expected = config;
        expected.connection.endpoint = "https://oss-cn-beijing.aliyuncs.com";

        // Capture the factory input instead of acquiring ENV credentials or contacting STS.
        // The public endpoint must not reclassify this historical S3 vault as OSS auth.
        std::optional<ObjStorageClientConfig> signing_config;
        auto recording_client = std::make_shared<RecordingSignedUrlClient>();
        ObjStorageClientFactory::instance().set_client_creator_for_test(
                [&](const ObjStorageClientConfig& created) {
                    signing_config = created;
                    return recording_client;
                });
        auto sign = make_oss_public_presigned_url_generator(config);
        ASSERT_TRUE(sign);
        EXPECT_EQ(sign({.bucket = "legacy-bucket", .key = "errors/load-id"}, 600),
                  "signed-public-url");
        ASSERT_TRUE(signing_config.has_value());
        ASSERT_TRUE(std::holds_alternative<AwsObjStorageConfig>(*signing_config));
        EXPECT_EQ(std::get<AwsObjStorageConfig>(*signing_config), expected);
        EXPECT_EQ(config.connection.endpoint, "https://oss-cn-beijing-internal.aliyuncs.com");
        EXPECT_EQ(recording_client->signed_path.bucket, "legacy-bucket");
        EXPECT_EQ(recording_client->signed_path.key, "errors/load-id");
        EXPECT_EQ(recording_client->signed_expiration_secs, 600);
    }
}

TEST_F(S3ClientFactoryTest, ReusesS3ClientsWithinEachBucket) {
    ObjStorageResourceConfig first {.bucket = "first",
                                    .client = make_factory_conf("https://shared.example.com")};
    auto other = first;
    other.prefix = "warehouse";
    other.sse_enabled = true;
    other.is_internal_bucket = true;
    auto& factory = ObjStorageClientFactory::instance();
    auto raw = factory.create(first.client, true, first.bucket);
    auto reused = factory.create(other.client, true, other.bucket);
    ASSERT_NE(raw.client, nullptr) << raw.error;
    ASSERT_NE(reused.client, nullptr) << reused.error;
    EXPECT_EQ(raw.client, reused.client);

    other.bucket = "second";
    auto separate = factory.create(other.client, true, other.bucket);
    ASSERT_NE(separate.client, nullptr) << separate.error;
    EXPECT_NE(raw.client, separate.client);
    EXPECT_EQ(factory.create(other.client, true, other.bucket).client, separate.client);

    auto bypassed = factory.create(first.client, /*use_cache=*/false, first.bucket);
    ASSERT_NE(bypassed.client, nullptr) << bypassed.error;
    EXPECT_NE(raw.client, bypassed.client);
    EXPECT_EQ(factory.create(first.client, true, first.bucket).client, raw.client);
}

TEST_F(S3ClientFactoryTest, CacheFlagControlsClientReuse) {
    auto& factory = ObjStorageClientFactory::instance();
    auto config = make_factory_conf("https://cache-policy-reuse.example.com");

    auto cached = factory.create(config);
    ASSERT_NE(cached.client, nullptr) << cached.error;
    auto reused = factory.create(config, /*use_cache=*/true);
    ASSERT_NE(reused.client, nullptr) << reused.error;
    EXPECT_EQ(reused.client, cached.client);

    auto bypassed = factory.create(config, /*use_cache=*/false);
    ASSERT_NE(bypassed.client, nullptr) << bypassed.error;
    EXPECT_NE(bypassed.client, cached.client);
    auto bypassed_again = factory.create(config, /*use_cache=*/false);
    ASSERT_NE(bypassed_again.client, nullptr) << bypassed_again.error;
    EXPECT_NE(bypassed_again.client, bypassed.client);

    // Disabling the cache must leave an existing cached client in place.
    auto still_cached = factory.create(config, /*use_cache=*/true);
    ASSERT_NE(still_cached.client, nullptr) << still_cached.error;
    EXPECT_EQ(still_cached.client, cached.client);

    // A bypassed first creation must not populate an empty cache entry either.
    config.connection.endpoint = "https://cache-policy-empty.example.com";
    auto uncached = factory.create(config, /*use_cache=*/false);
    ASSERT_NE(uncached.client, nullptr) << uncached.error;
    auto fresh_cached = factory.create(config, /*use_cache=*/true);
    ASSERT_NE(fresh_cached.client, nullptr) << fresh_cached.error;
    EXPECT_NE(fresh_cached.client, uncached.client);
    auto fresh_reused = factory.create(config);
    ASSERT_NE(fresh_reused.client, nullptr) << fresh_reused.error;
    EXPECT_EQ(fresh_reused.client, fresh_cached.client);
}

TEST_F(S3ClientFactoryTest, CacheFlagValidatesBeforeCreating) {
    auto& factory = ObjStorageClientFactory::instance();
    int create_count = 0;
    factory.set_client_creator_for_test(
            [&](const ObjStorageClientConfig&) -> std::shared_ptr<io::ObjStorageClient> {
                ++create_count;
                return std::make_shared<io::S3ObjStorageClient>(
                        std::shared_ptr<Aws::S3::S3Client> {});
            });
    auto config = make_factory_conf("https://cache-policy-validation.example.com");
    config.connection.max_connections = 0;
    for (bool use_cache : {true, false}) {
        auto invalid = factory.create(config, use_cache);
        EXPECT_EQ(invalid.client, nullptr);
        EXPECT_EQ(invalid.error, "max_connections must be positive");
        EXPECT_FALSE(invalid.unsupported);
    }
    EXPECT_EQ(create_count, 0);

    config.connection.max_connections = 1;
    for (bool use_cache : {true, false}) {
        auto valid = factory.create(config, use_cache);
        ASSERT_NE(valid.client, nullptr) << valid.error;
    }
    EXPECT_EQ(create_count, 2);
}

TEST_F(S3ClientFactoryTest, DistinguishesTimeoutOptions) {
    auto external_conf = make_timeout_conf("cloud-rate-limit-timeout.example.com", false);
    auto internal_conf = make_timeout_conf("cloud-rate-limit-timeout.example.com", true);
    ASSERT_NE(external_conf, internal_conf);

    auto& factory = ObjStorageClientFactory::instance();
    auto external_result = factory.create(external_conf);
    auto internal_result = factory.create(internal_conf);
    ASSERT_NE(external_result.client, nullptr) << external_result.error;
    ASSERT_NE(internal_result.client, nullptr) << internal_result.error;
    auto external_client = std::move(external_result.client);
    auto internal_client = std::move(internal_result.client);

    EXPECT_NE(external_client, internal_client);
    auto cached_external = factory.create(external_conf);
    auto cached_internal = factory.create(internal_conf);
    ASSERT_NE(cached_external.client, nullptr) << cached_external.error;
    ASSERT_NE(cached_internal.client, nullptr) << cached_internal.error;
    EXPECT_EQ(cached_external.client, external_client);
    EXPECT_EQ(cached_internal.client, internal_client);
}

TEST_F(S3ClientFactoryTest, ObjClientHolderResetDistinguishesTimeoutOptions) {
    auto external_conf = make_timeout_conf("s3-client-holder-timeout.example.com", false);
    auto internal_conf = make_timeout_conf("s3-client-holder-timeout.example.com", true);

    CloudModeConfigGuard cloud_mode_guard(true);
    auto external_client =
            std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    auto internal_client =
            std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    int create_count = 0;
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageClientConfig& conf) -> std::shared_ptr<io::ObjStorageClient> {
                ++create_count;
                return conf == ObjStorageClientConfig {internal_conf} ? internal_client
                                                                      : external_client;
            });

    io::ObjStorageClientHolder holder(external_conf);
    ASSERT_TRUE(holder.init().ok());
    EXPECT_EQ(create_count, 1);
    EXPECT_EQ(holder.get(), external_client);

    ASSERT_TRUE(holder.reset(internal_conf).ok());
    EXPECT_EQ(create_count, 2);
    EXPECT_EQ(holder.get(), internal_client);
    EXPECT_EQ(holder.config(), ObjStorageClientConfig {internal_conf});
}

TEST_F(S3ClientFactoryTest, ObjClientHolderKeepsBucketScopeWhenCredentialsChange) {
    auto config = make_factory_conf("https://bucket-holder-cache.example.com");
    io::ObjStorageClientHolder first(config, nullptr, "first");
    io::ObjStorageClientHolder second(config, nullptr, "second");
    ASSERT_TRUE(first.init().ok());
    ASSERT_TRUE(second.init().ok());
    auto first_client = first.get();
    auto second_client = second.get();
    EXPECT_NE(first_client, second_client);
    auto& factory = ObjStorageClientFactory::instance();
    EXPECT_EQ(first_client, factory.create(config, true, "first").client);

    config.auth.source = AccessKeyObjStorageAuth {.access_key = "new-ak", .secret_key = "new-sk"};
    ASSERT_TRUE(first.reset(config).ok());
    EXPECT_NE(first.get(), first_client);
    EXPECT_EQ(first.get(), factory.create(config, true, "first").client);
    auto separate = factory.create(config, true, "second");
    ASSERT_NE(separate.client, nullptr) << separate.error;
    EXPECT_NE(first.get(), separate.client);
    EXPECT_EQ(second.get(), second_client);
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
        io::ObjStorageClientHolder holder(
                conf, ObjStorageRateLimiterManager::policy_for(internal_bucket));
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

    io::ObjStorageClientHolder holder(make_factory_conf("be-bytes-rate-limit-response.example.com"),
                                      ObjStorageRateLimiterManager::policy_for(false));
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
    std::vector<ObjStorageClientConfig> created_confs;
    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageClientConfig& conf) -> std::shared_ptr<io::ObjStorageClient> {
                created_confs.push_back(conf);
                return client;
            });

    io::ObjStorageClientHolder holder(initial_conf);
    ASSERT_TRUE(holder.init().ok());
    ASSERT_TRUE(holder.reset(rotated_conf).ok());

    ASSERT_EQ(created_confs.size(), 2);
    EXPECT_EQ(created_confs.back(), ObjStorageClientConfig {rotated_conf});
    EXPECT_EQ(holder.config(), ObjStorageClientConfig {rotated_conf});
}

TEST_F(S3ClientFactoryTest, AzureLegacyPoolSizeAllowsHolderCredentialRotation) {
    TS3StorageParam param;
    param.__set_provider(TObjStorageType::AZURE);
    param.__set_endpoint("https://account.blob.core.windows.net");
    param.__set_bucket("container");
    param.__set_ak("account");
    param.__set_sk("old-key");
    param.__set_max_conn(100);
    auto parse = [&] {
        return ObjStorageConfigParser::from_storage_param(param, AwsCredentialImplementation::V2,
                                                          EmptyCredentialsBehavior::ANONYMOUS);
    };
    int create_count = 0;
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageClientConfig&) {
                ++create_count;
                return std::make_shared<S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
            });

    auto initial = parse();
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(initial));
    auto holder = std::make_shared<io::ObjStorageClientHolder>(
            std::get<ObjStorageResourceConfig>(initial).client);
    ASSERT_TRUE(holder->init().ok());
    auto existing_holder = holder;
    auto old_client = holder->get();

    param.__set_sk("new-key");
    auto rotated = parse();
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(rotated));
    const auto& rotated_conf = std::get<ObjStorageResourceConfig>(rotated).client;
    ASSERT_TRUE(holder->reset(rotated_conf).ok());
    EXPECT_EQ(create_count, 2);
    EXPECT_NE(existing_holder->get(), old_client);
    EXPECT_EQ(existing_holder->config(), rotated_conf);
    EXPECT_EQ(std::get<AzureSharedKeyConfig>(
                      std::get<AzureObjStorageConfig>(existing_holder->config()).auth.credential)
                      .account_key,
              "new-key");
    EXPECT_FALSE(std::get<AzureObjStorageConfig>(rotated_conf).connection.max_connections);
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
            [&](const ObjStorageClientConfig&) { return raw; });
    io::ObjStorageClientHolder holder(conf, ObjStorageRateLimiterManager::policy_for(true));
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

    conf.auth.source = AccessKeyObjStorageAuth {.access_key = "new-ak"};
    EXPECT_FALSE(holder.reset(conf).ok());
    EXPECT_EQ(holder.get(), old_client);
    EXPECT_TRUE(std::holds_alternative<AnonymousObjStorageAuth>(
            std::get<AwsObjStorageConfig>(holder.config()).auth.source));

    std::get<AccessKeyObjStorageAuth>(conf.auth.source).secret_key = "new-sk";
    ASSERT_TRUE(holder.reset(conf).ok());
    EXPECT_EQ(holder.config(), ObjStorageClientConfig {conf});
    EXPECT_NE(holder.get(), old_client);
    for (const auto& client : {old_client, holder.get()}) {
        auto response = client->head_object({.bucket = "bucket", .key = "key"}).resp;
        EXPECT_EQ(response.status.code, static_cast<int>(ErrorCode::EXCEEDED_LIMIT));
    }
}

TEST_F(S3ClientFactoryTest, ObjClientHolderKeepsClientWhenTargetChanges) {
    auto native_conf = make_gcp_native_conf();
    AwsObjStorageConfig s3_conf {.connection = native_conf.connection,
                                 .region = native_conf.region};
    // A provider-only switch at the same endpoint is supported. Change the
    // physical target to exercise rejection without replacing the existing client.
    s3_conf.connection.endpoint = "https://different-target.example.com";

    std::vector<ObjStorageClientConfig> created_confs;
    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageClientConfig& conf) -> std::shared_ptr<io::ObjStorageClient> {
                created_confs.push_back(conf);
                return client;
            });

    io::ObjStorageClientHolder holder(s3_conf);
    ASSERT_TRUE(holder.init().ok());
    auto old_client = holder.get();
    EXPECT_FALSE(holder.reset(native_conf).ok());
    EXPECT_EQ(holder.get(), old_client);
    EXPECT_EQ(holder.config(), ObjStorageClientConfig {s3_conf});
    EXPECT_EQ(created_confs.size(), 1);
}

TEST_F(S3ClientFactoryTest, ObjClientHolderResetSwitchesNativeAndStaticCredentials) {
    auto native_conf = make_gcp_native_conf();
    auto static_conf = native_conf;
    static_conf.auth = GcpHmacObjStorageAuth {"access-key", "secret-key", {}};

    std::vector<ObjStorageClientConfig> created_confs;
    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    ObjStorageClientFactory::instance().set_client_creator_for_test(
            [&](const ObjStorageClientConfig& conf) -> std::shared_ptr<io::ObjStorageClient> {
                created_confs.push_back(conf);
                return client;
            });

    io::ObjStorageClientHolder holder(native_conf);
    ASSERT_TRUE(holder.init().ok());
    ASSERT_TRUE(holder.reset(static_conf).ok());
    const auto config = holder.config();
    const auto& auth = std::get<GcpObjStorageConfig>(config).auth;
    EXPECT_EQ(std::get<GcpHmacObjStorageAuth>(auth).access_id, "access-key");

    ASSERT_TRUE(holder.reset(native_conf).ok());
    ASSERT_EQ(created_confs.size(), 3);
    EXPECT_EQ(created_confs[1], ObjStorageClientConfig {static_conf});
    EXPECT_EQ(created_confs[2], ObjStorageClientConfig {native_conf});
    EXPECT_EQ(holder.config(), ObjStorageClientConfig {native_conf});
}

TEST_F(S3ClientFactoryTest, GcpBearerHeaderSurvivesAnonymousAwsSigning) {
    FixedTokenGcpOAuthS3Client client;
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
        FixedTokenGcpOAuthS3Client client(endpoint);
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket("bucket");
        request.SetKey("key");
        auto outcome = client.GetObject(request);
        ASSERT_FALSE(outcome.IsSuccess()) << endpoint;
        EXPECT_EQ(outcome.GetError().GetExceptionName(), "GcpEndpointError");
        EXPECT_EQ(client.token_requests, 0);
    }
    FixedTokenGcpOAuthS3Client client;
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
        ObjStorageResourceConfig conf {
                .bucket = "test-bucket", .prefix = prefix, .client = make_gcp_native_conf()};
        auto raw_client = std::make_shared<GcpObjStorageClient>(
                nullptr, ObjStorageEndpointInfo {},
                [](const ObjStoragePath& path, int64_t expiration_secs) {
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
        ObjStorageClientFactory::instance().set_client_creator_for_test(
                [raw_client](const ObjStorageClientConfig&) { return raw_client; });
        auto fs = TEST_TRY(io::ObjStorageFileSystem::create(std::move(conf), "gcp-error-log-test"));
        const std::string expected_key =
                prefix.empty() ? "error_log/id" : "instance-prefix/error_log/id";
        io::Path upload_path;
        ASSERT_TRUE(fs->absolute_path("error_log/id", upload_path).ok());
        S3URI upload_uri(upload_path.native());
        ASSERT_TRUE(upload_uri.parse().ok());
        ASSERT_EQ(upload_uri.get_key(), expected_key);

        EXPECT_TRUE(fs->generate_presigned_url("error_log/id", 60, false)
                            .starts_with("https://storage.googleapis.com/test-bucket/" +
                                         expected_key + "?"));
    }
}

TEST_F(S3ClientFactoryTest, GcpTokenFailureRejectsEveryObjectStorageOperation) {
    FixedTokenGcpOAuthS3Client client;
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
    Properties anonymous_conf;
    Properties ak_sk_conf;
    ak_sk_conf["AWS_ACCESS_KEY"] = "ak";
    ak_sk_conf["AWS_SECRET_KEY"] = "sk";

    Properties role_conf1;
    role_conf1["AWS_CREDENTIALS_PROVIDER_TYPE"] = "INSTANCE_PROFILE";

    Properties role_conf2;
    role_conf2["AWS_CREDENTIALS_PROVIDER_TYPE"] = "INSTANCE_PROFILE";
    role_conf2["AWS_ROLE_ARN"] = "role_arn";
    role_conf2["AWS_EXTERNAL_ID"] = "external_id";

    Properties web_identity_conf;
    web_identity_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "WEB_IDENTITY";

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

    Properties role_conf;
    role_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "INSTANCE_PROFILE";
    role_conf["AWS_ROLE_ARN"] = "role_arn";
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

TEST_F(S3ClientFactoryTest, EffectiveAwsEndpointControlsSdkScheme) {
    for (const std::string scheme : {"http", "https"}) {
        for (const std::string endpoint :
             {"example.com:9000", "http://example.com:9000", "https://example.com:9000"}) {
            ObjStorageClientConfig client = make_factory_conf(endpoint);
            normalize_obj_storage_endpoint(client, scheme);
            const auto& aws = std::get<AwsObjStorageConfig>(client);
            auto sdk = make_s3_sdk_config(aws.connection, aws.region, aws.retry_slow_down,
                                          aws.use_sdk_endpoint, aws.sdk_endpoint_http);
            const auto expected = endpoint.find("://") == std::string::npos
                                          ? scheme + "://" + endpoint
                                          : endpoint;
            EXPECT_EQ(aws.connection.endpoint, expected);
            EXPECT_EQ(sdk.endpointOverride.c_str(), expected);
            EXPECT_EQ(sdk.scheme, expected.starts_with("http://") ? Aws::Http::Scheme::HTTP
                                                                  : Aws::Http::Scheme::HTTPS);
        }

        S3URI uri("s3://bucket/key");
        ASSERT_TRUE(uri.parse().ok());
        Properties properties {{"AWS_REGION", "us-east-1"},
                               {"AWS_ENDPOINT", "unused.example.com"},
                               {"AWS_NEED_OVERRIDE_ENDPOINT", "false"}};
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
                << std::get<std::string>(resource_result);
        auto resource = std::get<ObjStorageResourceConfig>(std::move(resource_result));
        apply_be_obj_storage_defaults(resource.client);
        normalize_obj_storage_endpoint(resource.client, scheme);
        const auto& aws = std::get<AwsObjStorageConfig>(resource.client);
        ASSERT_TRUE(aws.use_sdk_endpoint);
        EXPECT_TRUE(aws.connection.endpoint.empty());
        auto sdk = make_s3_sdk_config(aws.connection, aws.region, aws.retry_slow_down,
                                      aws.use_sdk_endpoint, aws.sdk_endpoint_http);
        EXPECT_TRUE(sdk.endpointOverride.empty());
        EXPECT_EQ(sdk.scheme,
                  scheme == "http" ? Aws::Http::Scheme::HTTP : Aws::Http::Scheme::HTTPS);
    }
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfRoleArnProviderType) {
    std::map<std::string, std::string> properties {
            {"AWS_ENDPOINT", "s3.us-west-2.amazonaws.com"},
            {"AWS_REGION", "us-west-2"},
            {"AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role"},
    };

    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    config::aws_credentials_provider_version = "v2";
    auto resource_result = ObjStorageConfigParser::from_properties(
            properties, s3_uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
            << std::get<std::string>(resource_result);
    auto resource = std::get<ObjStorageResourceConfig>(std::move(resource_result));
    apply_be_obj_storage_defaults(resource.client);
    const auto& aws = std::get<AwsObjStorageConfig>(resource.client);
    ASSERT_TRUE(aws.auth.assume_role);
    EXPECT_EQ(aws.auth.assume_role->role_arn, properties["AWS_ROLE_ARN"]);
    EXPECT_EQ(std::get<AwsCredentialChainType>(aws.auth.source), AwsCredentialChainType::Default);

    properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = "WEB_IDENTITY";
    resource_result = ObjStorageConfigParser::from_properties(
            properties, s3_uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
            << std::get<std::string>(resource_result);
    resource = std::get<ObjStorageResourceConfig>(std::move(resource_result));
    apply_be_obj_storage_defaults(resource.client);
    EXPECT_EQ(std::get<AwsCredentialChainType>(
                      std::get<AwsObjStorageConfig>(resource.client).auth.source),
              AwsCredentialChainType::WebIdentity);
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
        AwsCredentialSource expected;
    };

    std::vector<TestCase> cases = {
            {"DEFAULT", AwsCredentialChainType::Default},
            {"ENV", AwsCredentialChainType::Environment},
            {"SYSTEM_PROPERTIES", AwsCredentialChainType::Profile},
            {"WEB_IDENTITY", AwsCredentialChainType::WebIdentity},
            {"CONTAINER", AwsCredentialChainType::Container},
            {"INSTANCE_PROFILE", AwsCredentialChainType::InstanceProfile},
            {"ANONYMOUS", AnonymousObjStorageAuth {}},
    };

    config::aws_credentials_provider_version = "v2";
    for (const auto& test_case : cases) {
        auto properties = base_properties;
        properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = test_case.provider_type;
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
                << std::get<std::string>(resource_result);
        auto resource = std::get<ObjStorageResourceConfig>(std::move(resource_result));
        apply_be_obj_storage_defaults(resource.client);
        ASSERT_EQ(std::get<AwsObjStorageConfig>(resource.client).auth.source, test_case.expected)
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
        GcpCredentialSource expected_provider_type;
    };

    // Both ADC and Compute Engine metadata credentials may be used directly or
    // as the source credential for service-account impersonation.
    std::vector<TestCase> cases = {
            {"DEFAULT", "", GcpCredentialSource::Default},
            {"default", "", GcpCredentialSource::Default},
            {"DEFAULT", "target@my-project.iam.gserviceaccount.com", GcpCredentialSource::Default},
            {"COMPUTE_ENGINE", "", GcpCredentialSource::ComputeEngine},
            {" compute_engine ", "", GcpCredentialSource::ComputeEngine},
            {"COMPUTE_ENGINE", "target@my-project.iam.gserviceaccount.com",
             GcpCredentialSource::ComputeEngine},
    };

    for (const auto& test_case : cases) {
        auto properties = base_properties;
        properties["gs.credential_provider_type"] = test_case.provider_type;
        if (test_case.impersonation_service_account[0] != '\0') {
            properties["gs.impersonation_service_account"] =
                    test_case.impersonation_service_account;
        }

        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
                << std::get<std::string>(resource_result);
        auto resource = std::get<ObjStorageResourceConfig>(std::move(resource_result));
        apply_be_obj_storage_defaults(resource.client);
        ASSERT_EQ(storage_provider(resource.client), io::ObjStorageProvider::GCP);
        ASSERT_TRUE(std::holds_alternative<GcpNativeObjStorageAuth>(
                std::get<GcpObjStorageConfig>(resource.client).auth));
        const auto& credential = std::get<GcpNativeObjStorageAuth>(
                std::get<GcpObjStorageConfig>(resource.client).auth);
        EXPECT_EQ(credential.source, test_case.expected_provider_type);
        EXPECT_EQ(credential.impersonation_service_account.value_or(""),
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
    auto resource_result = ObjStorageConfigParser::from_properties(
            properties, s3_uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
            << std::get<std::string>(resource_result);
    auto resource = std::get<ObjStorageResourceConfig>(std::move(resource_result));
    apply_be_obj_storage_defaults(resource.client);
    const auto& credential =
            std::get<GcpNativeObjStorageAuth>(std::get<GcpObjStorageConfig>(resource.client).auth);
    EXPECT_EQ(credential.source, GcpCredentialSource::Default);
    EXPECT_EQ(credential.impersonation_service_account,
              "target@my-project.iam.gserviceaccount.com");

    properties["gs.impersonation_service_account"] = "";
    resource_result = ObjStorageConfigParser::from_properties(
            properties, s3_uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
            << std::get<std::string>(resource_result);
    resource = std::get<ObjStorageResourceConfig>(std::move(resource_result));
    apply_be_obj_storage_defaults(resource.client);
    const auto& direct_credential =
            std::get<GcpNativeObjStorageAuth>(std::get<GcpObjStorageConfig>(resource.client).auth);
    EXPECT_EQ(direct_credential.source, GcpCredentialSource::Default);
    EXPECT_FALSE(direct_credential.impersonation_service_account);
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
        auto error = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<std::string>(error)) << key;
        EXPECT_NE(std::get<std::string>(error).find("cannot be combined"), std::string::npos)
                << key;
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
        auto error = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<std::string>(error)) << provider_type;
        EXPECT_NE(std::get<std::string>(error).find("gs.credential_provider_type"),
                  std::string::npos)
                << provider_type;
        EXPECT_NE(std::get<std::string>(error).find("DEFAULT"), std::string::npos) << provider_type;
        EXPECT_NE(std::get<std::string>(error).find("COMPUTE_ENGINE"), std::string::npos)
                << provider_type;
    }

    for (const auto& account : {" ", "invalid-account"}) {
        auto properties = base_properties;
        properties["gs.impersonation_service_account"] = account;
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<std::string>(resource_result));
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
        auto error = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<std::string>(error)) << provider;
        EXPECT_NE(std::get<std::string>(error).find("provider=GCP"), std::string::npos) << provider;
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
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<std::string>(resource_result));
    }

    {
        auto properties = base_properties;
        properties["AWS_SECRET_KEY"] = "sk";
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<std::string>(resource_result));
    }

    {
        auto properties = base_properties;
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
                << std::get<std::string>(resource_result);
    }

    {
        auto properties = base_properties;
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
                << std::get<std::string>(resource_result);
    }

    {
        auto properties = base_properties;
        properties["AWS_ACCESS_KEY"] = "ak";
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
                << std::get<std::string>(resource_result);
    }

    {
        auto properties = base_properties;
        properties["AWS_SECRET_KEY"] = "sk";
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        auto resource_result = ObjStorageConfigParser::from_properties(
                properties, s3_uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(resource_result))
                << std::get<std::string>(resource_result);
    }
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2ProviderTypeWithoutRoleArn) {
    config::aws_credentials_provider_version = "v2";

    Properties default_conf;
    default_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "DEFAULT";
    auto provider = create_credentials_provider(default_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(provider), nullptr);

    Properties env_conf;
    env_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "ENV";
    provider = create_credentials_provider(env_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::EnvironmentAWSCredentialsProvider>(provider),
              nullptr);

    Properties sys_conf;
    sys_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "SYSTEM_PROPERTIES";
    provider = create_credentials_provider(sys_conf).provider;
    ASSERT_NE(
            std::dynamic_pointer_cast<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(provider),
            nullptr);

    Properties web_identity_conf;
    web_identity_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "WEB_IDENTITY";
    provider = create_credentials_provider(web_identity_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
                      provider),
              nullptr);

    {
        // The environment is pinned rather than merely topped up, so that this case asserts the
        // same thing whether or not the machine running it happens to be inside a container.
        ContainerCredentialsEnvGuard env;
        env.set_ecs_task_role("/v2/credentials/mock");
        Properties container_conf;
        container_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "CONTAINER";
        provider = create_credentials_provider(container_conf).provider;
        ASSERT_NE(as_valid_http_provider(provider), nullptr);
    }

    Properties instance_profile_conf;
    instance_profile_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "INSTANCE_PROFILE";
    provider = create_credentials_provider(instance_profile_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(provider),
              nullptr);

    Properties anonymous_conf;
    anonymous_conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "ANONYMOUS";
    provider = create_credentials_provider(anonymous_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider),
              nullptr);
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2WithRoleArnAlwaysAssumeRole) {
    config::aws_credentials_provider_version = "v2";

    const std::vector<std::string> provider_types = {"DEFAULT",           "ENV",
                                                     "SYSTEM_PROPERTIES", "WEB_IDENTITY",
                                                     "CONTAINER",         "INSTANCE_PROFILE",
                                                     "ANONYMOUS",         "SIMPLE"};

    for (const auto& provider_type : provider_types) {
        Properties conf;
        conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = provider_type;
        conf["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        conf["AWS_EXTERNAL_ID"] = "external-id";
        auto provider = create_credentials_provider(conf).provider;
        ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider),
                  nullptr);
    }
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2SimpleWithoutAkSkUsesDefaultChain) {
    config::aws_credentials_provider_version = "v2";

    Properties conf;
    conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "SIMPLE";
    auto result = create_credentials_provider(conf);

    ASSERT_TRUE(result);
    EXPECT_NE(std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(result.provider),
              nullptr);
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderAkSkTakePrecedenceOverRoleArn) {
    Properties conf;
    conf["AWS_ACCESS_KEY"] = "ak";
    conf["AWS_SECRET_KEY"] = "sk";
    conf["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
    conf["AWS_EXTERNAL_ID"] = "external-id";
    conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "INSTANCE_PROFILE";

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

    Properties conf;
    conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "DEFAULT";
    conf["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
    auto provider = create_credentials_provider(conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider),
              nullptr);

    config::aws_credentials_provider_version = "v2";
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV1PartialCredentialsUseDefaultChain) {
    config::aws_credentials_provider_version = "v1";

    for (bool provide_access_key : {false, true}) {
        Properties conf;
        conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "DEFAULT";
        conf["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        conf["AWS_ACCESS_KEY"] = provide_access_key ? "ak" : "";
        conf["AWS_SECRET_KEY"] = provide_access_key ? "" : "sk";

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

    Properties conf;
    conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "CONTAINER";
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

    Properties conf;
    conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "CONTAINER";
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

    Properties conf;
    conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "CONTAINER";
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

    Properties conf;
    conf["AWS_CREDENTIALS_PROVIDER_TYPE"] = "CONTAINER";
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

    config::aws_credentials_provider_version = "v2";
    Properties properties {{"AWS_CREDENTIALS_PROVIDER_TYPE", "CONTAINER"}};
    // role_arn stays empty on purpose: that is what makes create() hand back the base provider
    // itself instead of the STS wrapper built on top of it.
    auto base_provider = as_valid_http_provider(create_credentials_provider(properties).provider);
    ASSERT_NE(base_provider, nullptr)
            << "CONTAINER did not yield a usable STS base credentials provider";

    EXPECT_EQ(base_provider->GetAWSCredentials().GetAWSAccessKeyId(), "AKIDTEST");
    const auto auth_headers = endpoint.auth_headers();
    ASSERT_FALSE(auth_headers.empty())
            << "the STS base provider never contacted the credentials endpoint";
    EXPECT_EQ(auth_headers.back(), "token-one");
}

} // namespace doris
