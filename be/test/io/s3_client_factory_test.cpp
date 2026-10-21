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
#include <aws/core/auth/GeneralHTTPCredentialsProvider.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "cpp/aws_common.h"
#include "cpp/custom_aws_credentials_provider_chain.h"
#include "cpp/obj-client/auth/gcp/gcp_auth.h"
#include "cpp/obj-client/auth/gcp/gcp_s3_client.h"
#include "io/fs/err_utils.h"
#include "io/fs/rate_limited_obj_storage_client.h"
#include "io/fs/s3_obj_storage_client.h"
#include "testutil/container_credentials_endpoint.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

class S3ClientFactoryTest : public testing::Test {
    FRIEND_TEST(S3ClientFactoryTest, S3ClientFactory);

protected:
    void TearDown() override { S3ClientFactory::instance().clear_client_creator_for_test(); }
};

namespace {

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

S3ClientConf make_gcp_native_conf(std::string impersonation_service_account = {}) {
    auto conf = make_factory_conf("storage.googleapis.com", true);
    conf.provider = io::ObjStorageType::GCP;
    conf.region = "us-east1";
    conf.cred_provider_type = CredProviderType::Default;
    conf.credential = GcpCredentialConfig {
            .provider_type = GcpCredentialProviderType::Default,
            .impersonation_service_account = std::move(impersonation_service_account),
    };
    return conf;
}

class FixedTokenGcpS3Client final : public GcpS3Client {
public:
    FixedTokenGcpS3Client()
            : GcpS3Client(GcpCredentialConfig {}, "", Aws::Client::ClientConfiguration {},
                          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true) {}

    using GcpS3Client::BuildHttpRequest;

protected:
    std::optional<std::string> fetch_token() const override { return "test-gcp-token"; }
};

} // namespace

TEST_F(S3ClientFactoryTest, WrapsAllClientsInNonCloudMode) {
    CloudModeConfigGuard guard(false);
    auto& factory = S3ClientFactory::instance();

    auto external_client =
            factory.create(make_factory_conf("non-cloud-external-rate-limit.example.com", false));
    auto internal_client =
            factory.create(make_factory_conf("non-cloud-internal-rate-limit.example.com", true));

    ASSERT_NE(external_client, nullptr);
    ASSERT_NE(internal_client, nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<io::RateLimitedObjStorageClient>(external_client), nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<io::RateLimitedObjStorageClient>(internal_client), nullptr);
}

TEST_F(S3ClientFactoryTest, WrapsOnlyInternalClientsInCloudModeAndDistinguishesHashCollisions) {
    CloudModeConfigGuard guard(true);
    auto external_conf =
            make_hash_collision_conf("cloud-rate-limit-hash-collision.example.com", false);
    auto internal_conf =
            make_hash_collision_conf("cloud-rate-limit-hash-collision.example.com", true);
    ASSERT_EQ(external_conf.get_hash(), internal_conf.get_hash());
    ASSERT_NE(external_conf, internal_conf);

    auto& factory = S3ClientFactory::instance();
    auto external_client = factory.create(external_conf);
    auto internal_client = factory.create(internal_conf);

    ASSERT_NE(external_client, nullptr);
    ASSERT_NE(internal_client, nullptr);
    EXPECT_EQ(std::dynamic_pointer_cast<io::RateLimitedObjStorageClient>(external_client), nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<io::RateLimitedObjStorageClient>(internal_client), nullptr);
    EXPECT_NE(external_client, internal_client);
    EXPECT_EQ(factory.create(external_conf), external_client);
    EXPECT_EQ(factory.create(internal_conf), internal_client);
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

TEST_F(S3ClientFactoryTest, ObjClientHolderResetRefreshesNativeCredential) {
    auto initial_conf = make_gcp_native_conf();
    auto rotated_conf = make_gcp_native_conf("rotated@my-project.iam.gserviceaccount.com");
    std::vector<S3ClientConf> created_confs;
    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    S3ClientFactory::instance().set_client_creator_for_test(
            [&](const S3ClientConf& conf) -> std::shared_ptr<io::ObjStorageClient> {
                created_confs.push_back(conf);
                return client;
            });

    io::ObjClientHolder holder(initial_conf);
    ASSERT_TRUE(holder.init().ok());
    ASSERT_TRUE(holder.reset(rotated_conf).ok());

    ASSERT_EQ(created_confs.size(), 2);
    EXPECT_EQ(created_confs.back(), rotated_conf);
    EXPECT_EQ(holder.s3_client_conf(), rotated_conf);
}

TEST_F(S3ClientFactoryTest, ObjClientHolderResetSwitchesNativeAndStaticCredentials) {
    auto native_conf = make_gcp_native_conf();
    auto static_conf = native_conf;
    static_conf.credential = std::monostate {};
    static_conf.ak = "access-key";
    static_conf.sk = "secret-key";

    std::vector<S3ClientConf> created_confs;
    auto client = std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
    S3ClientFactory::instance().set_client_creator_for_test(
            [&](const S3ClientConf& conf) -> std::shared_ptr<io::ObjStorageClient> {
                created_confs.push_back(conf);
                return client;
            });

    io::ObjClientHolder holder(native_conf);
    ASSERT_TRUE(holder.init().ok());
    ASSERT_TRUE(holder.reset(static_conf).ok());
    ASSERT_TRUE(std::holds_alternative<std::monostate>(holder.s3_client_conf().credential));
    EXPECT_EQ(holder.s3_client_conf().ak, "access-key");

    ASSERT_TRUE(holder.reset(native_conf).ok());
    ASSERT_EQ(created_confs.size(), 3);
    EXPECT_EQ(created_confs[1], static_conf);
    EXPECT_EQ(created_confs[2], native_conf);
    EXPECT_EQ(holder.s3_client_conf(), native_conf);
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

TEST_F(S3ClientFactoryTest, UnauthorizedObjectStoreResponseIsPermissionDenied) {
    Aws::S3::S3Error error;
    error.SetResponseCode(Aws::Http::HttpResponseCode::UNAUTHORIZED);
    error.SetExceptionName("InvalidAuthenticationCredentials");
    error.SetMessage("missing or invalid bearer token");

    auto status = io::s3fs_error(error, "GCS request failed");
    EXPECT_EQ(status.code(), ErrorCode::PERMISSION_DENIED);
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
        auto provider_v2 = factory.get_aws_credentials_provider(anonymous_conf);
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }
    {
        auto provider_v2 = factory.get_aws_credentials_provider(ak_sk_conf);
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }

    {
        auto provider_v2 = factory.get_aws_credentials_provider(role_conf1);
        auto instance_profile_v2 =
                std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(
                        provider_v2);
        ASSERT_NE(instance_profile_v2, nullptr);
    }

    {
        auto provider_v2 = factory.get_aws_credentials_provider(role_conf2);
        auto custom_chain_v2 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider_v2);
        ASSERT_NE(custom_chain_v2, nullptr);
    }

    {
        auto provider_v2 = factory.get_aws_credentials_provider(web_identity_conf);
        auto web_identity_v2 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
                        provider_v2);
        ASSERT_NE(web_identity_v2, nullptr);
    }

    config::aws_credentials_provider_version = "v1";
    {
        auto provider_v1 = factory.get_aws_credentials_provider(anonymous_conf);
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = factory.get_aws_credentials_provider(ak_sk_conf);
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = factory.get_aws_credentials_provider(role_conf1);
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(
                        provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    {
        auto provider_v1 = factory.get_aws_credentials_provider(role_conf2);
        auto default_chain_v1 =
                std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider_v1);
        ASSERT_NE(default_chain_v1, nullptr);
    }

    config::aws_credentials_provider_version = "v2";
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

        S3Conf s3_conf;
        ASSERT_TRUE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok())
                << "provider_type=" << test_case.provider_type;
        ASSERT_EQ(s3_conf.client_conf.provider, io::ObjStorageType::GCP);
        ASSERT_TRUE(std::holds_alternative<GcpCredentialConfig>(s3_conf.client_conf.credential));
        const auto& credential = std::get<GcpCredentialConfig>(s3_conf.client_conf.credential);
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
    S3Conf s3_conf;
    ASSERT_TRUE(S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    const auto& credential = std::get<GcpCredentialConfig>(s3_conf.client_conf.credential);
    EXPECT_EQ(credential.provider_type, GcpCredentialProviderType::Default);
    EXPECT_EQ(credential.impersonation_service_account,
              "target@my-project.iam.gserviceaccount.com");

    properties["gs.impersonation_service_account"] = "";
    ASSERT_TRUE(S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    const auto& direct_credential = std::get<GcpCredentialConfig>(s3_conf.client_conf.credential);
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
        S3Conf s3_conf;
        auto status = S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf);
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
        S3Conf s3_conf;
        auto status = S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf);
        ASSERT_FALSE(status.ok()) << provider_type;
        EXPECT_NE(status.to_string().find("gs.credential_provider_type"), std::string::npos)
                << provider_type;
        EXPECT_NE(status.to_string().find("DEFAULT"), std::string::npos) << provider_type;
        EXPECT_NE(status.to_string().find("COMPUTE_ENGINE"), std::string::npos) << provider_type;
    }

    for (const auto& account : {" ", "invalid-account"}) {
        auto properties = base_properties;
        properties["gs.impersonation_service_account"] = account;
        S3Conf s3_conf;
        ASSERT_FALSE(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf).ok());
    }
}

TEST_F(S3ClientFactoryTest, ConvertPropertiesToS3ConfRejectsGcpCredentialWithAwsProvider) {
    S3URI s3_uri("s3://test-bucket/test-prefix");
    ASSERT_TRUE(s3_uri.parse().ok());

    for (const auto& provider : {"S3", "AWS"}) {
        std::map<std::string, std::string> properties {
                {"AWS_ENDPOINT", "storage.googleapis.com"},
                {"AWS_REGION", "us-east1"},
                {"provider", provider},
                {"gs.credential_provider_type", "DEFAULT"},
        };
        S3Conf s3_conf;
        auto status = S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf);
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
    auto provider = factory.get_aws_credentials_provider(default_conf);
    ASSERT_NE(std::dynamic_pointer_cast<CustomAwsCredentialsProviderChain>(provider), nullptr);

    S3ClientConf env_conf;
    env_conf.cred_provider_type = CredProviderType::Env;
    provider = factory.get_aws_credentials_provider(env_conf);
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::EnvironmentAWSCredentialsProvider>(provider),
              nullptr);

    S3ClientConf sys_conf;
    sys_conf.cred_provider_type = CredProviderType::SystemProperties;
    provider = factory.get_aws_credentials_provider(sys_conf);
    ASSERT_NE(
            std::dynamic_pointer_cast<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(provider),
            nullptr);

    S3ClientConf web_identity_conf;
    web_identity_conf.cred_provider_type = CredProviderType::WebIdentity;
    provider = factory.get_aws_credentials_provider(web_identity_conf);
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
                      provider),
              nullptr);

    {
        // The environment is pinned rather than merely topped up, so that this case asserts the
        // same thing whether or not the machine running it happens to be inside a container.
        ContainerCredentialsEnvGuard env;
        env.set_ecs_task_role("/v2/credentials/mock");
        S3ClientConf container_conf;
        container_conf.cred_provider_type = CredProviderType::Container;
        provider = factory.get_aws_credentials_provider(container_conf);
        ASSERT_NE(as_valid_http_provider(provider), nullptr);
    }

    S3ClientConf instance_profile_conf;
    instance_profile_conf.cred_provider_type = CredProviderType::InstanceProfile;
    provider = factory.get_aws_credentials_provider(instance_profile_conf);
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::InstanceProfileCredentialsProvider>(provider),
              nullptr);

    S3ClientConf anonymous_conf;
    anonymous_conf.cred_provider_type = CredProviderType::Anonymous;
    provider = factory.get_aws_credentials_provider(anonymous_conf);
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
            CredProviderType::Anonymous,
    };

    for (auto provider_type : provider_types) {
        S3ClientConf conf;
        conf.cred_provider_type = provider_type;
        conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
        conf.external_id = "external-id";
        auto provider = factory.get_aws_credentials_provider(conf);
        ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::STSAssumeRoleCredentialsProvider>(provider),
                  nullptr);
    }
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
    auto provider_v2 = factory.get_aws_credentials_provider(conf);
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::SimpleAWSCredentialsProvider>(provider_v2),
              nullptr);

    config::aws_credentials_provider_version = "v1";
    auto provider_v1 = factory.get_aws_credentials_provider(conf);
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
    auto provider = factory.get_aws_credentials_provider(conf);
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(provider),
              nullptr);

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
    (void)S3ClientFactory::instance();

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
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v2";

    ContainerCredentialsEndpoint endpoint;
    ASSERT_TRUE(endpoint.start());

    ContainerCredentialsEnvGuard env;
    const std::string token_path = env.token_file_path("container_type");
    env.write_token_file(token_path, "token-one");
    env.set_pod_identity(endpoint.url(), token_path);

    S3ClientConf conf;
    conf.cred_provider_type = CredProviderType::Container;
    auto provider = as_valid_http_provider(factory.get_aws_credentials_provider(conf));
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
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v2";

    ContainerCredentialsEndpoint endpoint;
    ASSERT_TRUE(endpoint.start());

    ContainerCredentialsEnvGuard env;
    env.set_inline_token_endpoint(endpoint.url(), "inline-token");

    S3ClientConf conf;
    conf.cred_provider_type = CredProviderType::Container;
    auto provider = as_valid_http_provider(factory.get_aws_credentials_provider(conf));
    ASSERT_NE(provider, nullptr) << "CONTAINER did not yield a usable container credentials "
                                    "provider for an inline authorization token";

    EXPECT_EQ(provider->GetAWSCredentials().GetAWSAccessKeyId(), "AKIDTEST");

    const auto auth_headers = endpoint.auth_headers();
    ASSERT_GE(auth_headers.size(), 1u);
    EXPECT_EQ(auth_headers.front(), "inline-token");
}

TEST_F(S3ClientFactoryTest, ContainerProviderTypeStillHonoursEcsRelativeUri) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v2";

    ContainerCredentialsEnvGuard env;
    env.set_ecs_task_role("/v2/credentials/mock");

    S3ClientConf conf;
    conf.cred_provider_type = CredProviderType::Container;
    EXPECT_NE(as_valid_http_provider(factory.get_aws_credentials_provider(conf)), nullptr)
            << "CONTAINER did not yield a usable container credentials provider for "
               "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
}

// With neither URI exported there is nothing to talk to, and the provider says so. This pins the
// discriminator the two tests above rely on: IsValid() really does distinguish a wired provider
// from an inert one, so their assertions cannot pass vacuously.
TEST_F(S3ClientFactoryTest, ContainerProviderTypeIsUnusableWithoutAnyUri) {
    S3ClientFactory& factory = S3ClientFactory::instance();
    config::aws_credentials_provider_version = "v2";

    // Constructing the guard is the whole setup: it clears all four AWS_CONTAINER_* variables, so
    // this runs as if on a host that is not a container at all.
    ContainerCredentialsEnvGuard env;

    S3ClientConf conf;
    conf.cred_provider_type = CredProviderType::Container;
    auto provider = factory.get_aws_credentials_provider(conf);
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::GeneralHTTPCredentialsProvider>(provider),
              nullptr);
    EXPECT_EQ(as_valid_http_provider(provider), nullptr);
}

} // namespace doris
