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
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "cpp/client/s3_obj_storage_backend.h"
#include "cpp/custom_aws_credentials_provider_chain.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

class S3ClientFactoryTest : public testing::Test {
    FRIEND_TEST(S3ClientFactoryTest, S3ClientFactory);

protected:
    void TearDown() override { S3ClientFactory::instance().clear_client_creator_for_test(); }
};

namespace {

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

    auto external_backend =
            std::make_shared<io::S3ObjStorageBackend>(std::shared_ptr<Aws::S3::S3Client> {});
    auto internal_backend =
            std::make_shared<io::S3ObjStorageBackend>(std::shared_ptr<Aws::S3::S3Client> {});
    auto external_client = std::make_shared<io::ObjStorageClient>(external_backend);
    auto internal_client = std::make_shared<io::ObjStorageClient>(internal_backend);
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

} // namespace doris
