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

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/aws_common.h"
#include "cpp/custom_aws_credentials_provider_chain.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "io/fs/s3_file_system.h"
#include "util/s3_rate_limiter_manager.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

class S3ClientFactoryTest : public testing::Test {
    FRIEND_TEST(S3ClientFactoryTest, S3ClientFactory);

protected:
    void TearDown() override { S3ClientFactory::instance().clear_client_creator_for_test(); }
};

namespace {

constexpr size_t kNoThrottleBytesPerSecond = 1ULL << 40;

S3ClientConf make_factory_conf(std::string endpoint, bool is_internal_bucket) {
    S3ClientConf conf;
    conf.endpoint = std::move(endpoint);
    conf.region = "us-east-1";
    conf.cred_provider_type = CredProviderType::Anonymous;
    conf.is_internal_bucket = is_internal_bucket;
    return conf;
}

S3ClientConf make_hash_collision_conf(std::string endpoint, bool is_internal_bucket) {
    auto conf = make_factory_conf(std::move(endpoint), is_internal_bucket);
    conf.use_virtual_addressing = !is_internal_bucket;
    return conf;
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
        return S3RateLimiterManager::instance().qps_limiter(S3RateLimitType::GET);
    }
    static S3RateLimiterHolder* _bytes() {
        return S3RateLimiterManager::instance().bytes_limiter(S3RateLimitType::GET);
    }

    bool _enabled;
    size_t _qps_max_speed;
    size_t _qps_max_burst;
    size_t _qps_limit;
    size_t _bytes_max_speed;
    size_t _bytes_max_burst;
    size_t _bytes_limit;
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

TEST_F(S3ClientFactoryTest, DistinguishesHashCollisions) {
    auto external_conf =
            make_hash_collision_conf("cloud-rate-limit-hash-collision.example.com", false);
    auto internal_conf =
            make_hash_collision_conf("cloud-rate-limit-hash-collision.example.com", true);
    ASSERT_EQ(external_conf.get_hash(), internal_conf.get_hash());
    ASSERT_NE(external_conf, internal_conf);

    auto& factory = S3ClientFactory::instance();
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
    ASSERT_EQ(external_conf.get_hash(), internal_conf.get_hash());

    auto external_client =
            std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    auto internal_client =
            std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    int create_count = 0;
    S3ClientFactory::instance().set_client_creator_for_test(
            [&](const S3ClientConf& conf) -> std::shared_ptr<io::ObjStorageClient> {
                ++create_count;
                return conf.is_internal_bucket ? internal_client : external_client;
            });

    io::ObjClientHolder holder(external_conf);
    ASSERT_TRUE(holder.init().ok());
    EXPECT_EQ(create_count, 1);
    EXPECT_EQ(holder.get(), external_client);

    ASSERT_TRUE(holder.reset(internal_conf).ok());
    EXPECT_EQ(create_count, 2);
    EXPECT_EQ(holder.get(), internal_client);
    EXPECT_EQ(holder.s3_client_conf(), internal_conf);
}

TEST_F(S3ClientFactoryTest, SelectsRateLimiterByDeploymentAndBucketType) {
    RateLimiterConfigGuard rate_limiter_guard;
    SyncPointProcessingGuard sync_point_guard;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard create_client_callback;
    sync_point->set_call_back(
            "s3_client_factory::create",
            [](auto&& args) {
                auto result = try_any_cast_ret<std::shared_ptr<io::S3ObjStorageClient>>(args);
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
        auto& manager = S3RateLimiterManager::instance();
        manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 1);
        manager.bytes_limiter(S3RateLimitType::GET)->reset(0, 0, 0);

        auto result = S3ClientFactory::instance().create(
                make_factory_conf(std::move(endpoint), internal_bucket));
        ASSERT_TRUE(result.has_value()) << result.error();
        auto client = std::move(result).value();
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

    check_selection(false, false, true, "non-cloud-external-rate-limit.example.com");
    check_selection(true, true, true, "cloud-internal-rate-limit.example.com");
    check_selection(true, false, false, "cloud-external-no-rate-limit.example.com");
}

TEST_F(S3ClientFactoryTest, RateLimitResponseDistinguishesBytesFromProviderThrottling) {
    RateLimiterConfigGuard rate_limiter_guard;
    SyncPointProcessingGuard sync_point_guard;
    SyncPoint::CallbackGuard create_client_callback;
    SyncPoint::get_instance()->set_call_back(
            "s3_client_factory::create",
            [](auto&& args) {
                auto result = try_any_cast_ret<std::shared_ptr<io::S3ObjStorageClient>>(args);
                result->second = true;
            },
            &create_client_callback);

    CloudModeConfigGuard cloud_mode_guard(false);
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 0);
    manager.bytes_limiter(S3RateLimitType::GET)
            ->reset(kNoThrottleBytesPerSecond, kNoThrottleBytesPerSecond, 1);

    auto result = S3ClientFactory::instance().create(
            make_factory_conf("be-bytes-rate-limit-response.example.com", false));
    ASSERT_TRUE(result.has_value()) << result.error();
    auto client = std::move(result).value();
    char buffer[2];
    size_t size_return = 0;
    auto response = client->get_object({.bucket = "bucket", .key = "key"}, buffer, 0,
                                       sizeof(buffer), &size_return);

    EXPECT_EQ(response.status.code, static_cast<int>(ErrorCode::EXCEEDED_LIMIT));
    EXPECT_EQ(response.http_code, 0);
    EXPECT_NE(response.status.msg.find("s3 get request exceeds bytes limit"), std::string::npos);
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProvider) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    S3ClientConf anonymous_conf;
    S3ClientConf ak_sk_conf;
    ak_sk_conf.ak = "ak";
    ak_sk_conf.sk = "sk";

    S3ClientConf role_conf1;
    role_conf1.cred_provider_type = CredProviderType::InstanceProfile;

    S3ClientConf role_conf2;
    role_conf2.cred_provider_type = CredProviderType::InstanceProfile;
    role_conf2.role_arn = "role_arn";
    role_conf2.external_id = "external_id";

    S3ClientConf web_identity_conf;
    web_identity_conf.cred_provider_type = CredProviderType::WebIdentity;

    config::aws_credentials_provider_version = "v2";
    {
        auto provider_v2 = factory.create_aws_credentials_provider(anonymous_conf).provider;
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }
    {
        auto provider_v2 = factory.create_aws_credentials_provider(ak_sk_conf).provider;
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }

    {
        auto provider_v2 = factory.create_aws_credentials_provider(role_conf1).provider;
        auto instance_profile_v2 =
                std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(
                        provider_v2);
        ASSERT_NE(instance_profile_v2, nullptr);
    }

    {
        auto provider_v2 = factory.create_aws_credentials_provider(role_conf2).provider;
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }

    {
        auto provider_v2 = factory.create_aws_credentials_provider(web_identity_conf).provider;
        auto web_identity_v2 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
                        provider_v2);
        ASSERT_NE(web_identity_v2, nullptr);
    }

    config::aws_credentials_provider_version = "v1";
    {
        auto provider_v1 = factory.create_aws_credentials_provider(anonymous_conf).provider;
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = factory.create_aws_credentials_provider(ak_sk_conf).provider;
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = factory.create_aws_credentials_provider(role_conf1).provider;
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(
                        provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = factory.create_aws_credentials_provider(role_conf2).provider;
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    config::aws_credentials_provider_version = "v2";
}

TEST_F(S3ClientFactoryTest, RefreshCaCertForCredentialsProvider) {
    auto& factory = S3ClientFactory::instance();
    auto old_ca_cert_file_paths = config::ca_cert_file_paths;
    std::string old_ca_cert_file_path;
    {
        std::lock_guard lock(factory._ca_cert_lock);
        old_ca_cert_file_path = std::exchange(factory._ca_cert_file_path, "");
    }

    auto ca_cert_file_path = std::filesystem::temp_directory_path() /
                             ("doris_s3_client_factory_ca_" + std::to_string(getpid()) + ".pem");
    std::filesystem::remove(ca_cert_file_path);
    config::ca_cert_file_paths = ca_cert_file_path.string();

    S3ClientConf role_conf;
    role_conf.cred_provider_type = CredProviderType::InstanceProfile;
    role_conf.role_arn = "role_arn";
    auto provider_without_ca = factory.create_aws_credentials_provider(role_conf).provider;

    {
        std::ofstream ca_cert_file(ca_cert_file_path);
        ca_cert_file << "test CA bundle";
    }
    auto provider_with_ca = factory.create_aws_credentials_provider(role_conf).provider;

    std::string refreshed_ca_cert_file_path;
    {
        std::lock_guard lock(factory._ca_cert_lock);
        refreshed_ca_cert_file_path = factory._ca_cert_file_path;
        factory._ca_cert_file_path = std::move(old_ca_cert_file_path);
    }
    config::ca_cert_file_paths = std::move(old_ca_cert_file_paths);
    std::filesystem::remove(ca_cert_file_path);

    EXPECT_NE(provider_without_ca, nullptr);
    EXPECT_NE(provider_with_ca, nullptr);
    EXPECT_EQ(refreshed_ca_cert_file_path, ca_cert_file_path.string());
}

TEST_F(S3ClientFactoryTest, SetS3ClientDefaultHttpScheme) {
    S3ClientFactory::instance();
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

    S3Conf s3_conf;
    ASSERT_TRUE(S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    ASSERT_EQ(s3_conf.client_conf.endpoint, properties.at("AWS_ENDPOINT"));
    ASSERT_EQ(s3_conf.client_conf.cred_provider_type, CredProviderType::Default);

    properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = "WEB_IDENTITY";
    ASSERT_TRUE(S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    ASSERT_EQ(s3_conf.client_conf.cred_provider_type, CredProviderType::WebIdentity);
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
        S3Conf s3_conf;
        auto properties = base_properties;
        properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = test_case.provider_type;
        ASSERT_TRUE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok())
                << "provider_type=" << test_case.provider_type;
        ASSERT_EQ(s3_conf.client_conf.cred_provider_type, test_case.expected)
                << "provider_type=" << test_case.provider_type;
    }
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfS3ExpressValidation) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    std::map<std::string, std::string> properties {
            {"AWS_ENDPOINT", "s3express-control.us-west-2.amazonaws.com"},
            {"AWS_REGION", "us-west-2"},
            {"provider", "s3express"},
    };
    S3Conf s3_conf;
    ASSERT_TRUE(S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    EXPECT_EQ(s3_conf.client_conf.provider, io::ObjStorageProvider::S3EXPRESS);
    EXPECT_TRUE(s3_conf.client_conf.use_virtual_addressing);

    properties["use_path_style"] = "true";
    EXPECT_FALSE(S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());

    properties["use_path_style"] = "false";
    properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = "ANONYMOUS";
    EXPECT_FALSE(S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
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
        S3Conf s3_conf;
        ASSERT_FALSE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        properties["AWS_SECRET_KEY"] = "sk";
        S3Conf s3_conf;
        ASSERT_FALSE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        S3Conf s3_conf;
        ASSERT_TRUE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        S3Conf s3_conf;
        ASSERT_TRUE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        properties["AWS_ACCESS_KEY"] = "ak";
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        S3Conf s3_conf;
        ASSERT_TRUE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    }

    {
        auto properties = base_properties;
        properties["AWS_SECRET_KEY"] = "sk";
        properties["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/test-role";
        S3Conf s3_conf;
        ASSERT_TRUE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    }
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2ProviderTypeWithoutRoleArn) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v2";

    S3ClientConf default_conf;
    default_conf.cred_provider_type = CredProviderType::Default;
    auto provider = factory.create_aws_credentials_provider(default_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(provider), nullptr);

    S3ClientConf env_conf;
    env_conf.cred_provider_type = CredProviderType::Env;
    provider = factory.create_aws_credentials_provider(env_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::EnvironmentAWSCredentialsProvider>(provider),
              nullptr);

    S3ClientConf sys_conf;
    sys_conf.cred_provider_type = CredProviderType::SystemProperties;
    provider = factory.create_aws_credentials_provider(sys_conf).provider;
    ASSERT_NE(
            std::dynamic_pointer_cast<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(provider),
            nullptr);

    S3ClientConf web_identity_conf;
    web_identity_conf.cred_provider_type = CredProviderType::WebIdentity;
    provider = factory.create_aws_credentials_provider(web_identity_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
                      provider),
              nullptr);

    const char* old_container_uri = std::getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
    if (old_container_uri == nullptr) {
        setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/mock", 1);
    }
    S3ClientConf container_conf;
    container_conf.cred_provider_type = CredProviderType::Container;
    provider = factory.create_aws_credentials_provider(container_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::TaskRoleCredentialsProvider>(provider), nullptr);
    if (old_container_uri == nullptr) {
        unsetenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
    }

    S3ClientConf instance_profile_conf;
    instance_profile_conf.cred_provider_type = CredProviderType::InstanceProfile;
    provider = factory.create_aws_credentials_provider(instance_profile_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(provider),
              nullptr);

    S3ClientConf anonymous_conf;
    anonymous_conf.cred_provider_type = CredProviderType::Anonymous;
    provider = factory.create_aws_credentials_provider(anonymous_conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider),
              nullptr);
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2WithRoleArnAlwaysAssumeRole) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v2";

    std::vector<CredProviderType> provider_types = {
            CredProviderType::Default,          CredProviderType::Env,
            CredProviderType::SystemProperties, CredProviderType::WebIdentity,
            CredProviderType::Container,        CredProviderType::InstanceProfile,
            CredProviderType::Anonymous,        CredProviderType::Simple,
    };

    for (auto provider_type : provider_types) {
        S3ClientConf conf;
        conf.cred_provider_type = provider_type;
        conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
        conf.external_id = "external-id";
        auto provider = factory.create_aws_credentials_provider(conf).provider;
        ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider),
                  nullptr);
    }
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV2SimpleWithoutAkSkUsesDefaultChain) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v2";

    S3ClientConf conf;
    conf.cred_provider_type = CredProviderType::Simple;
    auto result = factory.create_aws_credentials_provider(conf);

    ASSERT_TRUE(result);
    EXPECT_NE(std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(result.provider),
              nullptr);
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderAkSkTakePrecedenceOverRoleArn) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    S3ClientConf conf;
    conf.ak = "ak";
    conf.sk = "sk";
    conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
    conf.external_id = "external-id";
    conf.cred_provider_type = CredProviderType::InstanceProfile;

    config::aws_credentials_provider_version = "v2";
    auto provider_v2 = factory.create_aws_credentials_provider(conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v2),
              nullptr);

    config::aws_credentials_provider_version = "v1";
    auto provider_v1 = factory.create_aws_credentials_provider(conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v1),
              nullptr);

    config::aws_credentials_provider_version = "v2";
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV1RoleArnDefaultFallback) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v1";

    S3ClientConf conf;
    conf.cred_provider_type = CredProviderType::Default;
    conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
    auto provider = factory.create_aws_credentials_provider(conf).provider;
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider),
              nullptr);

    config::aws_credentials_provider_version = "v2";
}

TEST_F(S3ClientFactoryTest, AwsCredentialsProviderV1PartialCredentialsUseDefaultChain) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v1";

    for (bool provide_access_key : {false, true}) {
        S3ClientConf conf;
        conf.cred_provider_type = CredProviderType::Default;
        conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
        conf.ak = provide_access_key ? "ak" : "";
        conf.sk = provide_access_key ? "" : "sk";

        auto provider = factory.create_aws_credentials_provider(conf).provider;
        EXPECT_NE(
                std::dynamic_pointer_cast<Aws::Auth::DefaultAWSCredentialsProviderChain>(provider),
                nullptr);
    }

    config::aws_credentials_provider_version = "v2";
}

} // namespace doris
