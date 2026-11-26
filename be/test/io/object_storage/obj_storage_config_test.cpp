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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <initializer_list>
#include <map>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include "common/config.h"
#include "cpp/obj-storage/parser/obj_storage_config_parser.h"
#include "io/object_storage/be_obj_storage_config_parser.h"
#include "util/s3_uri.h"

namespace doris {

TEST(ObjStorageConfigTest, CredentialChangesPreserveTarget) {
    AwsObjStorageConfig aws;
    aws.region = "region";
    aws.connection.endpoint = "https://endpoint";
    aws.auth.source = AccessKeyObjStorageAuth {"ak", "sk", "token"};
    auto update = aws;
    std::get<AccessKeyObjStorageAuth>(update.auth.source).session_token = "rotated";
    EXPECT_TRUE(same_client_target(aws, update));
    EXPECT_NE(aws, update);
    update.region = "another-region";
    EXPECT_FALSE(same_client_target(aws, update));
    update = aws;
    update.connection.ca_cert_file = "/etc/ssl/ca.pem";
    EXPECT_TRUE(same_client_target(aws, update));
    EXPECT_NE(aws, update);
    update.connection.endpoint = "another-endpoint";
    EXPECT_FALSE(same_client_target(aws, update));
}

TEST(ObjStorageConfigTest, ResourceFieldsDoNotChangeClientIdentity) {
    AzureObjStorageConfig azure;
    azure.connection.endpoint = "https://account.blob.core.windows.net";
    azure.auth = {"account", AzureSharedKeyConfig {"key"}};
    ObjStorageResourceConfig first {.bucket = "container-1", .prefix = "/root/", .client = azure};
    auto second = first;
    second.bucket = "container-2";
    second.prefix = "archive";
    second.is_internal_bucket = true;
    second.sse_enabled = true;
    EXPECT_EQ(first.normalized_prefix(), "root");
    EXPECT_EQ(first.client, second.client);
    EXPECT_EQ(ObjStorageClientHash {}(first.client), ObjStorageClientHash {}(second.client));
    std::get<AzureObjStorageConfig>(second.client).auth.account_name = "other-account";
    EXPECT_FALSE(same_client_target(first.client, second.client));
}

TEST(ObjStorageConfigTest, LegacyResourceWithoutProviderUsesAws) {
    TS3StorageParam param;
    param.__set_bucket("legacy");
    param.__set_root_path("/tablets");
    param.__set_endpoint("https://s3.example");
    param.__set_region("region");
    auto parsed_result = ObjStorageConfigParser::from_storage_param(
            param, aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(parsed_result))
            << std::get<std::string>(parsed_result);
    auto parsed = std::get<ObjStorageResourceConfig>(std::move(parsed_result));
    EXPECT_EQ(parsed.bucket, "legacy");
    EXPECT_EQ(parsed.prefix, "/tablets");
    const auto& aws = std::get<AwsObjStorageConfig>(parsed.client);
    EXPECT_EQ(aws.connection.endpoint, "https://s3.example");
    EXPECT_EQ(aws.connection.max_connections, 50);
    EXPECT_EQ(aws.connection.request_timeout, std::chrono::milliseconds(3000));
    EXPECT_EQ(aws.connection.connect_timeout, std::chrono::milliseconds(1000));
}

TEST(ObjStorageConfigTest, UnknownThriftUsesAwsWhileVaultInfersEndpoint) {
    const struct {
        const char* endpoint;
        ObjStorageProvider expected;
    } cases[] = {
            {"oss-cn-hangzhou.aliyuncs.com", ObjStorageProvider::OSS},
            {"https://bucket.OSS-CN-HANGZHOU-INTERNAL.aliyuncs.com:443/path",
             ObjStorageProvider::OSS},
            {"s3.cn-hangzhou.aliyuncs.com", ObjStorageProvider::OSS},
            {"cn-shanghai.oss-dls.aliyuncs.com", ObjStorageProvider::AWS},
            {"ecs.cn-hangzhou.aliyuncs.com", ObjStorageProvider::AWS},
            {"bucket.cos.ap-guangzhou.myqcloud.com", ObjStorageProvider::COS},
            {"obs.cn-north-4.myhuaweicloud.com", ObjStorageProvider::OBS},
            {"bj.bcebos.com", ObjStorageProvider::BOS},
            {"tos-s3-cn-beijing.volces.com", ObjStorageProvider::TOS},
            {"account.blob.core.windows.net", ObjStorageProvider::AZURE},
            {"https://storage.googleapis.com", ObjStorageProvider::GCP},
            {"https://objects.example.com", ObjStorageProvider::AWS},
            {"https://oss-cn-hangzhou.aliyuncs.com.example.com", ObjStorageProvider::AWS},
            {"https://oss-cn-hangzhou.aliyuncs.com@objects.example.com", ObjStorageProvider::AWS},
            {"https://objects.example.com/oss-cn-hangzhou.aliyuncs.com", ObjStorageProvider::AWS},
    };
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.endpoint);
        TS3StorageParam param;
        param.__set_bucket("bucket");
        param.__set_endpoint(entry.endpoint);
        auto thrift_result = ObjStorageConfigParser::from_storage_param(
                param, aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(thrift_result))
                << std::get<std::string>(thrift_result);
        auto thrift = std::get<ObjStorageResourceConfig>(std::move(thrift_result));
        EXPECT_EQ(ObjStorageProvider::AWS, storage_provider(thrift.client));
        cloud::ObjectStoreInfoPB vault;
        vault.set_bucket("bucket");
        vault.set_endpoint(entry.endpoint);
        auto pb_result = ObjStorageConfigParser::from_vault(
                vault, aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(pb_result))
                << std::get<std::string>(pb_result);
        auto pb = std::get<ObjStorageResourceConfig>(std::move(pb_result));
        EXPECT_EQ(entry.expected, storage_provider(pb.client));
    }
}

TEST(ObjStorageConfigTest, UnknownThriftProviderPreservesLegacyCredentialChain) {
    TS3StorageParam param;
    param.__set_bucket("private-bucket");
    param.__set_endpoint("https://oss-cn-hangzhou.aliyuncs.com");
    param.__set_region("cn-hangzhou");
    for (auto mode : {TCredProviderType::DEFAULT, TCredProviderType::ENV}) {
        param.__set_cred_provider_type(mode);
        auto parsed = ObjStorageConfigParser::from_storage_param(
                param, AwsCredentialImplementation::V2, EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(parsed));
        auto& client = std::get<ObjStorageResourceConfig>(parsed).client;
        ASSERT_TRUE(std::holds_alternative<AwsObjStorageConfig>(client));
        auto& source = std::get<AwsObjStorageConfig>(client).auth.source;
        ASSERT_TRUE(std::holds_alternative<AwsCredentialChainType>(source));
        EXPECT_EQ(std::get<AwsCredentialChainType>(source),
                  mode == TCredProviderType::ENV ? AwsCredentialChainType::Environment
                                                 : AwsCredentialChainType::Default);
    }
}

TEST(ObjStorageConfigTest, MissingPropertyProviderPreservesAwsCredentialChain) {
    const struct {
        const char* name;
        AwsCredentialChainType expected;
    } modes[] = {{"ENV", AwsCredentialChainType::Environment},
                 {"DEFAULT", AwsCredentialChainType::Default}};
    for (const char* endpoint :
         {"oss-cn-hangzhou.aliyuncs.com", "cos.ap-guangzhou.myqcloud.com",
          "obs.cn-north-4.myhuaweicloud.com", "bj.bcebos.com", "tos-s3-cn-beijing.volces.com",
          "https://storage.googleapis.com", "account.blob.core.windows.net"}) {
        SCOPED_TRACE(endpoint);
        for (const auto& mode : modes) {
            SCOPED_TRACE(mode.name);
            // FE's generic S3 provider emits these fields without a provider hint,
            // including when users select S3 explicitly at another cloud's endpoint.
            auto result = ObjStorageConfigParser::from_properties(
                    {{"AWS_ENDPOINT", endpoint},
                     {"AWS_REGION", "region"},
                     {"AWS_ACCESS_KEY", ""},
                     {"AWS_SECRET_KEY", ""},
                     {"AWS_CREDENTIALS_PROVIDER_TYPE", mode.name}},
                    "bucket", AwsCredentialImplementation::V2, EmptyCredentialsBehavior::ANONYMOUS);
            ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result))
                    << std::get<std::string>(result);
            const auto& resource = std::get<ObjStorageResourceConfig>(result);
            ASSERT_TRUE(std::holds_alternative<AwsObjStorageConfig>(resource.client));
            const auto& aws = std::get<AwsObjStorageConfig>(resource.client);
            EXPECT_EQ(aws.auth.source, AwsCredentialSource {mode.expected});
            EXPECT_EQ(aws.connection.endpoint, endpoint);
            EXPECT_FALSE(aws.validate());
        }
    }
}

TEST(ObjStorageConfigTest, ExplicitPropertyProviderPreservesGcpAndAzureAuthentication) {
    auto gcp_result = ObjStorageConfigParser::from_properties(
            {{"provider", "GCP"},
             {"AWS_ENDPOINT", "https://storage.googleapis.com"},
             {"AWS_REGION", "us-east1"},
             {"gs.credential_provider_type", "COMPUTE_ENGINE"}},
            "bucket", AwsCredentialImplementation::V2, EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(gcp_result))
            << std::get<std::string>(gcp_result);
    const auto& gcp_resource = std::get<ObjStorageResourceConfig>(gcp_result);
    ASSERT_TRUE(std::holds_alternative<GcpObjStorageConfig>(gcp_resource.client));
    const auto& gcp = std::get<GcpObjStorageConfig>(gcp_resource.client);
    ASSERT_TRUE(std::holds_alternative<GcpNativeObjStorageAuth>(gcp.auth));
    EXPECT_EQ(std::get<GcpNativeObjStorageAuth>(gcp.auth).source,
              GcpCredentialSource::ComputeEngine);
    EXPECT_FALSE(gcp.validate());

    auto azure_result = ObjStorageConfigParser::from_properties(
            {{"provider", "azure"},
             {"AWS_ENDPOINT", "https://account.blob.core.windows.net"},
             {"AWS_ACCESS_KEY", "account"},
             {"AWS_SECRET_KEY", "key"}},
            "container", AwsCredentialImplementation::V2, EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(azure_result))
            << std::get<std::string>(azure_result);
    const auto& azure_resource = std::get<ObjStorageResourceConfig>(azure_result);
    ASSERT_TRUE(std::holds_alternative<AzureObjStorageConfig>(azure_resource.client));
    const auto& azure = std::get<AzureObjStorageConfig>(azure_resource.client);
    EXPECT_EQ(azure.auth.account_name, "account");
    EXPECT_EQ(std::get<AzureSharedKeyConfig>(azure.auth.credential).account_key, "key");
    EXPECT_FALSE(azure.validate());
}

TEST(ObjStorageConfigTest, ParsesLegacyAwsProperties) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    std::map<std::string, std::string> props {{"AWS_ACCESS_KEY", "ak"},
                                              {"AWS_SECRET_KEY", "sk"},
                                              {"AWS_TOKEN", "token"},
                                              {"aws_endpoint", "endpoint"},
                                              {"AWS_REGION", "region"},
                                              {"AWS_NEED_OVERRIDE_ENDPOINT", "false"},
                                              {"AWS_MAX_CONNECTIONS", "42"},
                                              {"AWS_REQUEST_TIMEOUT_MS", "2345"},
                                              {"AWS_CONNECTION_TIMEOUT_MS", "678"},
                                              {"USE_PATH_STYLE", "true"},
                                              {"AWS_ROLE_ARN", "role"},
                                              {"AWS_EXTERNAL_ID", "external"},
                                              {"AWS_CREDENTIALS_PROVIDER_TYPE", "ENV"}};
    auto parsed_result = ObjStorageConfigParser::from_properties(
            props, uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(parsed_result))
            << std::get<std::string>(parsed_result);
    auto parsed = std::get<ObjStorageResourceConfig>(std::move(parsed_result));
    apply_be_obj_storage_defaults(parsed.client);
    EXPECT_EQ(parsed.bucket, "bucket");
    const auto& aws = std::get<AwsObjStorageConfig>(parsed.client);
    EXPECT_TRUE(aws.use_sdk_endpoint);
    EXPECT_TRUE(aws.use_path_style);
    EXPECT_TRUE(aws.connection.endpoint.empty());
    EXPECT_EQ(aws.region, "region");
    EXPECT_EQ(aws.connection.max_connections, 42);
    EXPECT_EQ(aws.connection.request_timeout, std::chrono::milliseconds(2345));
    EXPECT_EQ(aws.connection.connect_timeout, std::chrono::milliseconds(678));
    EXPECT_EQ(std::get<AccessKeyObjStorageAuth>(aws.auth.source),
              (AccessKeyObjStorageAuth {"ak", "sk", "token"}));
    EXPECT_FALSE(aws.auth.assume_role);
    props["aws_endpoint"] = "unused-second-override";
    auto second_result = ObjStorageConfigParser::from_properties(
            props, uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(second_result))
            << std::get<std::string>(second_result);
    auto second = std::get<ObjStorageResourceConfig>(std::move(second_result));
    apply_be_obj_storage_defaults(second.client);
    EXPECT_EQ(parsed.client, second.client);
    props["provider"] = "BOS";
    auto unsupported = ObjStorageConfigParser::from_properties(
            props, uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<std::string>(unsupported));
    EXPECT_NE(std::get<std::string>(unsupported).find("requires provider=AWS"), std::string::npos);
}

TEST(ObjStorageConfigTest, NonAwsCredentialRestrictionsApplyToEveryInputFormat) {
    const struct {
        const char* name;
        TObjStorageType::type thrift;
        cloud::ObjectStoreInfoPB::Provider pb;
    } providers[] = {
            {"OSS", TObjStorageType::OSS, cloud::ObjectStoreInfoPB::OSS},
            {"COS", TObjStorageType::COS, cloud::ObjectStoreInfoPB::COS},
            {"BOS", TObjStorageType::BOS, cloud::ObjectStoreInfoPB::BOS},
            {"OBS", TObjStorageType::OBS, cloud::ObjectStoreInfoPB::OBS},
            {"TOS", TObjStorageType::TOS, cloud::ObjectStoreInfoPB::TOS},
            {"GCP", TObjStorageType::GCP, cloud::ObjectStoreInfoPB::GCP},
    };
    for (const auto& provider : providers) {
        SCOPED_TRACE(provider.name);
        ObjStorageProperties properties {{"provider", provider.name},
                                         {"AWS_ENDPOINT", "endpoint"},
                                         {"AWS_REGION", "region"}};
        TS3StorageParam param;
        param.__set_provider(provider.thrift);
        param.__set_bucket("bucket");
        param.__set_endpoint("endpoint");
        param.__set_region("region");
        cloud::ObjectStoreInfoPB vault;
        vault.set_provider(provider.pb);
        vault.set_bucket("bucket");
        vault.set_endpoint("endpoint");
        vault.set_region("region");
        // GCS historically supports Recycler's default HMAC chain. The other
        // access-key providers must keep anonymous access for an empty key pair.
        for (const auto& result :
             {ObjStorageConfigParser::from_properties(properties, "bucket",
                                                      AwsCredentialImplementation::V1,
                                                      EmptyCredentialsBehavior::DEFAULT_CHAIN),
              ObjStorageConfigParser::from_storage_param(param, AwsCredentialImplementation::V1,
                                                         EmptyCredentialsBehavior::DEFAULT_CHAIN),
              ObjStorageConfigParser::from_vault(vault, AwsCredentialImplementation::V1,
                                                 EmptyCredentialsBehavior::DEFAULT_CHAIN)}) {
            ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result))
                    << std::get<std::string>(result);
            std::visit(
                    [](const auto& client) {
                        using C = std::decay_t<decltype(client)>;
                        if constexpr (std::is_same_v<C, GcpObjStorageConfig>) {
                            ASSERT_TRUE(std::holds_alternative<GcpDefaultHmacObjStorageAuth>(
                                    client.auth));
                            EXPECT_EQ(std::get<GcpDefaultHmacObjStorageAuth>(client.auth)
                                              .implementation,
                                      AwsCredentialImplementation::V1);
                        } else if constexpr (!std::is_same_v<C, AwsObjStorageConfig> &&
                                             !std::is_same_v<C, AzureObjStorageConfig>) {
                            EXPECT_TRUE(
                                    std::holds_alternative<AnonymousObjStorageAuth>(client.auth));
                        }
                    },
                    std::get<ObjStorageResourceConfig>(result).client);
        }
        properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = "ENV";
        properties["AWS_ROLE_ARN"] = "legacy-role";
        properties["AWS_EXTERNAL_ID"] = "legacy-external-id";
        param.__set_cred_provider_type(TCredProviderType::ENV);
        param.__set_role_arn("legacy-role");
        param.__set_external_id("legacy-external-id");
        vault.set_cred_provider_type(cloud::CredProviderTypePB::ENV);
        vault.set_role_arn("legacy-role");
        vault.set_external_id("legacy-external-id");
        for (const auto& result :
             {ObjStorageConfigParser::from_properties(properties, "bucket",
                                                      AwsCredentialImplementation::V1,
                                                      EmptyCredentialsBehavior::ANONYMOUS),
              ObjStorageConfigParser::from_storage_param(param, AwsCredentialImplementation::V1,
                                                         EmptyCredentialsBehavior::ANONYMOUS),
              ObjStorageConfigParser::from_vault(vault, AwsCredentialImplementation::V1,
                                                 EmptyCredentialsBehavior::ANONYMOUS)}) {
            ASSERT_TRUE(std::holds_alternative<std::string>(result));
            EXPECT_EQ(std::get<std::string>(result),
                      "AWS credential chains and AssumeRole are not supported for this provider");
        }
        // Historical role fields are harmless once FE has supplied actual keys.
        properties["AWS_ACCESS_KEY"] = "sts-ak";
        properties["AWS_SECRET_KEY"] = "sts-sk";
        properties["AWS_TOKEN"] = "sts-token";
        param.__set_ak("sts-ak");
        param.__set_sk("sts-sk");
        param.__set_token("sts-token");
        vault.set_ak("sts-ak");
        vault.set_sk("sts-sk");
        for (const auto& result :
             {ObjStorageConfigParser::from_properties(properties, "bucket",
                                                      AwsCredentialImplementation::V2,
                                                      EmptyCredentialsBehavior::DEFAULT_CHAIN),
              ObjStorageConfigParser::from_storage_param(param, AwsCredentialImplementation::V2,
                                                         EmptyCredentialsBehavior::DEFAULT_CHAIN),
              ObjStorageConfigParser::from_vault(vault, AwsCredentialImplementation::V2,
                                                 EmptyCredentialsBehavior::DEFAULT_CHAIN)}) {
            ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result))
                    << std::get<std::string>(result);
            std::visit(
                    [](const auto& client) {
                        using C = std::decay_t<decltype(client)>;
                        if constexpr (std::is_same_v<C, GcpObjStorageConfig>) {
                            EXPECT_TRUE(std::holds_alternative<GcpHmacObjStorageAuth>(client.auth));
                        } else if constexpr (!std::is_same_v<C, AwsObjStorageConfig> &&
                                             !std::is_same_v<C, AzureObjStorageConfig>) {
                            EXPECT_TRUE(
                                    std::holds_alternative<AccessKeyObjStorageAuth>(client.auth));
                        }
                    },
                    std::get<ObjStorageResourceConfig>(result).client);
        }
    }
}

TEST(ObjStorageConfigTest, ProviderParsersRejectUnsupportedCredentialsAndPreserveStaticKeys) {
    auto check = [](const char* provider, auto parse) {
        SCOPED_TRACE(provider);
        using C = std::variant_alternative_t<0, decltype(parse(ObjStorageProperties {}))>;
        using Keys = std::conditional_t<std::is_same_v<C, GcpObjStorageConfig>,
                                        GcpHmacObjStorageAuth, AccessKeyObjStorageAuth>;
        for (const char* mode :
             {static_cast<const char*>(nullptr), "", "DEFAULT", "SIMPLE", "ANONYMOUS"}) {
            ObjStorageProperties properties;
            if (mode) {
                properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = mode;
            }
            auto result = parse(properties);
            ASSERT_TRUE(std::holds_alternative<C>(result)) << std::get<std::string>(result);
            if constexpr (std::is_same_v<C, GcpObjStorageConfig>) {
                EXPECT_EQ(std::holds_alternative<GcpDefaultHmacObjStorageAuth>(
                                  std::get<C>(result).auth),
                          !mode || std::string_view(mode) != "ANONYMOUS");
            } else {
                EXPECT_TRUE(
                        std::holds_alternative<AnonymousObjStorageAuth>(std::get<C>(result).auth));
            }
        }
        const std::pair<ObjStorageProperties, const char*> invalid[] = {
                {{{"AWS_ACCESS_KEY", "ak"}}, "Incomplete access key credentials"},
                {{{"AWS_SECRET_KEY", "sk"}}, "Incomplete access key credentials"},
                {{{"AWS_ACCESS_KEY", "ak"}, {"AWS_ROLE_ARN", "role"}},
                 "Incomplete access key credentials"},
                {{{"AWS_TOKEN", "token"}}, "Session token requires access key credentials"},
                {{{"AWS_CREDENTIALS_PROVIDER_TYPE", "ENV"}},
                 "AWS credential chains and AssumeRole are not supported for this provider"},
                {{{"AWS_CREDENTIALS_PROVIDER_TYPE", "INSTANCE_PROFILE"}},
                 "AWS credential chains and AssumeRole are not supported for this provider"},
                {{{"AWS_CREDENTIALS_PROVIDER_TYPE", "WEB_IDENTITY"}},
                 "AWS credential chains and AssumeRole are not supported for this provider"},
                {{{"AWS_ROLE_ARN", "role"}},
                 "AWS credential chains and AssumeRole are not supported for this provider"},
                {{{"AWS_EXTERNAL_ID", "external"}},
                 "AWS credential chains and AssumeRole are not supported for this provider"},
        };
        for (auto [properties, message] : invalid) {
            auto result = parse(properties);
            ASSERT_TRUE(std::holds_alternative<std::string>(result));
            EXPECT_EQ(std::get<std::string>(result), message);
            properties["AWS_ACCESS_KEY"] = "ak";
            properties["AWS_SECRET_KEY"] = "sk";
            properties["AWS_TOKEN"] = "token";
            result = parse(properties);
            ASSERT_TRUE(std::holds_alternative<C>(result)) << std::get<std::string>(result);
            const auto& auth = std::get<C>(result).auth;
            ASSERT_TRUE(std::holds_alternative<Keys>(auth));
            EXPECT_EQ(std::get<Keys>(auth).session_token, "token");
        }
    };
    check("OSS", parse_oss_obj_storage_config);
    check("COS", parse_cos_obj_storage_config);
    check("BOS", parse_bos_obj_storage_config);
    check("OBS", parse_obs_obj_storage_config);
    check("TOS", parse_tos_obj_storage_config);
    check("GCP", [](const auto& props) { return parse_gcp_obj_storage_config(props); });
}

TEST(ObjStorageConfigTest, AwsEmptyCredentialsKeepHostPolicyAcrossInputFormats) {
    TS3StorageParam param;
    param.__set_provider(TObjStorageType::AWS);
    param.__set_bucket("bucket");
    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::S3);
    vault.set_bucket("bucket");
    for (auto implementation : {AwsCredentialImplementation::V1, AwsCredentialImplementation::V2}) {
        for (auto empty :
             {EmptyCredentialsBehavior::ANONYMOUS, EmptyCredentialsBehavior::DEFAULT_CHAIN}) {
            const bool anonymous = implementation == AwsCredentialImplementation::V1 &&
                                   empty == EmptyCredentialsBehavior::ANONYMOUS;
            for (const auto& result :
                 {ObjStorageConfigParser::from_properties({{"provider", "AWS"}}, "bucket",
                                                          implementation, empty),
                  ObjStorageConfigParser::from_storage_param(param, implementation, empty),
                  ObjStorageConfigParser::from_vault(vault, implementation, empty)}) {
                ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result))
                        << std::get<std::string>(result);
                const auto& auth = std::get<AwsObjStorageConfig>(
                                           std::get<ObjStorageResourceConfig>(result).client)
                                           .auth;
                EXPECT_EQ(auth.implementation, implementation);
                EXPECT_EQ(std::holds_alternative<AnonymousObjStorageAuth>(auth.source), anonymous);
                if (!anonymous) {
                    EXPECT_EQ(std::get<AwsCredentialChainType>(auth.source),
                              AwsCredentialChainType::Default);
                }
            }
        }
    }
}

TEST(ObjStorageConfigTest, BeAdaptersApplyDefaultsAcrossInputFormats) {
    ObjStorageProperties properties {
            {"PrOvIdEr", "  aWs "}, {"aws_endpoint", "https://endpoint"}, {"AWS_REGION", "region"}};
    TS3StorageParam thrift;
    thrift.__set_provider(TObjStorageType::AWS);
    thrift.__set_bucket("bucket");
    thrift.__set_endpoint("https://endpoint");
    thrift.__set_region("region");
    thrift.__set_max_conn(-1);
    thrift.__set_request_timeout_ms(-1);
    thrift.__set_conn_timeout_ms(-1);
    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::S3);
    vault.set_bucket("bucket");
    vault.set_endpoint("https://endpoint");
    vault.set_region("region");
    for (const auto& result :
         {parse_be_obj_storage_config(properties, "bucket"), parse_be_obj_storage_config(thrift),
          parse_be_obj_storage_config(vault)}) {
        ASSERT_TRUE(result.has_value()) << result.error();
        const auto& resource = result.value();
        const auto& aws = std::get<AwsObjStorageConfig>(resource.client);
        EXPECT_EQ(resource.bucket, "bucket");
        EXPECT_EQ(aws.connection.endpoint, "https://endpoint");
        EXPECT_EQ(aws.connection.max_connections, 102400);
        EXPECT_EQ(aws.connection.request_timeout, std::chrono::seconds(30));
        EXPECT_EQ(aws.connection.max_retries, config::max_s3_client_retry);
        EXPECT_EQ(aws.auth.implementation,
                  aws_credential_implementation(config::aws_credentials_provider_version));
        const AwsCredentialSource expected_source =
                aws.auth.implementation == AwsCredentialImplementation::V1
                        ? AwsCredentialSource {AnonymousObjStorageAuth {}}
                        : AwsCredentialSource {AwsCredentialChainType::Default};
        EXPECT_EQ(aws.auth.source, expected_source);
    }

    properties["AWS_MAX_CONNECTIONS"] = "37";
    properties["AWS_REQUEST_TIMEOUT_MS"] = "0";
    auto explicit_settings = parse_be_obj_storage_config(properties, "bucket");
    ASSERT_TRUE(explicit_settings.has_value()) << explicit_settings.error();
    const auto& connection = obj_storage_connection(explicit_settings->client);
    EXPECT_EQ(connection.max_connections, 37);
    EXPECT_EQ(connection.request_timeout, std::chrono::seconds(30));
}

TEST(ObjStorageConfigTest, BeAdaptersReturnParseErrorsAsStatus) {
    TS3StorageParam thrift;
    cloud::ObjectStoreInfoPB vault;
    for (const auto& result :
         {parse_be_obj_storage_config({}, ""), parse_be_obj_storage_config(thrift),
          parse_be_obj_storage_config(vault)}) {
        ASSERT_FALSE(result.has_value());
        EXPECT_TRUE(result.error().is<ErrorCode::INVALID_ARGUMENT>());
        EXPECT_NE(result.error().to_string().find("bucket"), std::string::npos);
    }
}

TEST(ObjStorageConfigTest, GcpDefaultHmacKeepsHostPolicyAcrossInputFormats) {
    ObjStorageProperties props {{"provider", "GCP"},
                                {"AWS_ENDPOINT", "https://storage.googleapis.com"},
                                {"AWS_REGION", "us-east1"}};
    TS3StorageParam param;
    param.__set_provider(TObjStorageType::GCP);
    param.__set_bucket("bucket");
    param.__set_endpoint("https://storage.googleapis.com");
    param.__set_region("us-east1");
    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::GCP);
    vault.set_bucket("bucket");
    vault.set_endpoint(param.endpoint);
    vault.set_region(param.region);
    for (auto implementation : {AwsCredentialImplementation::V1, AwsCredentialImplementation::V2}) {
        for (auto empty :
             {EmptyCredentialsBehavior::ANONYMOUS, EmptyCredentialsBehavior::DEFAULT_CHAIN}) {
            // FE omits DEFAULT from Thrift; properties can also carry it explicitly.
            for (const char* mode : {"", "DEFAULT", "ANONYMOUS"}) {
                props["AWS_CREDENTIALS_PROVIDER_TYPE"] = mode;
                param.__isset.cred_provider_type = false;
                vault.clear_cred_provider_type();
                if (std::string_view(mode) == "ANONYMOUS") {
                    param.__set_cred_provider_type(TCredProviderType::ANONYMOUS);
                    vault.set_cred_provider_type(cloud::CredProviderTypePB::ANONYMOUS);
                }
                for (auto parsed :
                     {ObjStorageConfigParser::from_properties(props, "bucket", implementation,
                                                              empty),
                      ObjStorageConfigParser::from_storage_param(param, implementation, empty),
                      ObjStorageConfigParser::from_vault(vault, implementation, empty)}) {
                    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(parsed));
                    auto& client = std::get<GcpObjStorageConfig>(
                            std::get<ObjStorageResourceConfig>(parsed).client);
                    const bool anonymous = implementation == AwsCredentialImplementation::V1
                                                   ? empty == EmptyCredentialsBehavior::ANONYMOUS
                                                   : std::string_view(mode) == "ANONYMOUS";
                    EXPECT_EQ(std::holds_alternative<AnonymousObjStorageAuth>(client.auth),
                              anonymous);
                    if (!anonymous) {
                        ASSERT_TRUE(
                                std::holds_alternative<GcpDefaultHmacObjStorageAuth>(client.auth));
                        EXPECT_EQ(
                                std::get<GcpDefaultHmacObjStorageAuth>(client.auth).implementation,
                                implementation);
                    }
                    EXPECT_FALSE(client.validate());
                }
            }
        }
    }
}

TEST(ObjStorageConfigTest, BeVaultKeepsLegacyUnencryptedUploads) {
    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::S3);
    vault.set_bucket("bucket");
    vault.set_endpoint("http://minio:9000");
    vault.set_region("us-east-1");
    vault.set_ak("ak");
    vault.set_sk("sk");
    vault.set_sse_enabled(true);
    auto common = ObjStorageConfigParser::from_vault(vault, AwsCredentialImplementation::V2,
                                                     EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(common));
    EXPECT_TRUE(std::get<ObjStorageResourceConfig>(common).sse_enabled);
    auto be = parse_be_obj_storage_config(vault);
    ASSERT_TRUE(be.has_value()) << be.error();
    EXPECT_FALSE(be->sse_enabled);
}

TEST(ObjStorageConfigTest, GcpSigningTimeoutParticipatesInClientIdentity) {
    ObjStorageProperties properties {{"provider", "GCP"},
                                     {"AWS_ENDPOINT", "https://storage.googleapis.com"},
                                     {"AWS_REGION", "us-east1"},
                                     {"gs.credential_provider_type", "COMPUTE_ENGINE"}};
    auto defaults = parse_be_obj_storage_config(properties, "bucket");
    ASSERT_TRUE(defaults.has_value()) << defaults.error();
    properties["AWS_REQUEST_TIMEOUT_MS"] = "30000";
    auto explicit_timeout = parse_be_obj_storage_config(properties, "bucket");
    ASSERT_TRUE(explicit_timeout.has_value()) << explicit_timeout.error();
    EXPECT_EQ(obj_storage_connection(defaults->client),
              obj_storage_connection(explicit_timeout->client));
    EXPECT_TRUE(same_client_target(defaults->client, explicit_timeout->client));
    EXPECT_EQ(std::get<GcpObjStorageConfig>(defaults->client).signing_timeout,
              std::chrono::seconds(10));
    EXPECT_EQ(std::get<GcpObjStorageConfig>(explicit_timeout->client).signing_timeout,
              std::chrono::seconds(30));
    EXPECT_NE(defaults->client, explicit_timeout->client);
    EXPECT_NE(ObjStorageClientHash {}(defaults->client),
              ObjStorageClientHash {}(explicit_timeout->client));
    auto reapplied = defaults->client;
    apply_be_obj_storage_defaults(reapplied);
    EXPECT_EQ(reapplied, defaults->client);
}

TEST(ObjStorageConfigTest, LegacyZeroPoolSizeAndTimeoutsUseHostDefaults) {
    ObjStorageProperties properties {{"AWS_ENDPOINT", "https://endpoint"},
                                     {"AWS_REGION", "region"},
                                     {"AWS_MAX_CONNECTIONS", "0"},
                                     {"AWS_REQUEST_TIMEOUT_MS", "0"},
                                     {"AWS_CONNECTION_TIMEOUT_MS", "0"}};
    TS3StorageParam thrift;
    thrift.__set_provider(TObjStorageType::AWS);
    thrift.__set_bucket("bucket");
    thrift.__set_endpoint("https://endpoint");
    thrift.__set_region("region");
    thrift.__set_max_conn(0);
    thrift.__set_request_timeout_ms(0);
    thrift.__set_conn_timeout_ms(0);

    for (const auto& result :
         {ObjStorageConfigParser::from_properties(properties, "bucket",
                                                  AwsCredentialImplementation::V2,
                                                  EmptyCredentialsBehavior::ANONYMOUS),
          ObjStorageConfigParser::from_storage_param(thrift, AwsCredentialImplementation::V2,
                                                     EmptyCredentialsBehavior::ANONYMOUS)}) {
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result))
                << std::get<std::string>(result);
        const auto& resource = std::get<ObjStorageResourceConfig>(result);
        EXPECT_FALSE(obj_storage_connection(resource.client).max_connections);
        EXPECT_FALSE(obj_storage_connection(resource.client).request_timeout);
        EXPECT_FALSE(obj_storage_connection(resource.client).connect_timeout);
        EXPECT_FALSE(validate_obj_storage_config(resource.client));
    }
    for (const auto& result :
         {parse_be_obj_storage_config(properties, "bucket"), parse_be_obj_storage_config(thrift)}) {
        ASSERT_TRUE(result.has_value()) << result.error();
        const auto& connection = obj_storage_connection(result->client);
        EXPECT_EQ(connection.max_connections, 102400);
        EXPECT_EQ(connection.request_timeout, std::chrono::seconds(30));
        EXPECT_FALSE(connection.connect_timeout);
        EXPECT_FALSE(validate_obj_storage_config(result->client));
    }
}

TEST(ObjStorageConfigTest, TypedConfigRejectsZeroPoolSize) {
    AwsObjStorageConfig config {.connection = {.endpoint = "https://endpoint"}, .region = "region"};
    EXPECT_FALSE(config.validate());
    config.connection.max_connections = 0;
    auto error = config.validate();
    ASSERT_TRUE(error);
    EXPECT_EQ(*error, "max_connections must be positive");
}

TEST(ObjStorageConfigTest, EffectiveTimeoutTreatsLegacyZeroAsDefault) {
    ObjStorageProperties props {{"AWS_ENDPOINT", "endpoint"}, {"AWS_REGION", "region"}};
    auto common_result = ObjStorageConfigParser::from_properties(
            props, "bucket", AwsCredentialImplementation::V1, EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(common_result))
            << std::get<std::string>(common_result);
    auto common = std::get<ObjStorageResourceConfig>(std::move(common_result));
    const auto& connection = obj_storage_connection(common.client);
    EXPECT_EQ(connection.endpoint, "endpoint");
    EXPECT_FALSE(connection.request_timeout);
    EXPECT_FALSE(connection.max_connections);
    EXPECT_FALSE(connection.max_retries);
    EXPECT_FALSE(connection.ca_cert_file);
    auto parsed = common;
    apply_be_obj_storage_defaults(parsed.client);
    EXPECT_EQ(obj_storage_connection(parsed.client).request_timeout, std::chrono::seconds(30));
    EXPECT_EQ(obj_storage_connection(parsed.client).max_connections, 102400);
    EXPECT_EQ(obj_storage_connection(parsed.client).max_retries, config::max_s3_client_retry);
    props["AWS_REQUEST_TIMEOUT_MS"] = "0";
    common_result = ObjStorageConfigParser::from_properties(
            props, "bucket", AwsCredentialImplementation::V1, EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(common_result))
            << std::get<std::string>(common_result);
    common = std::get<ObjStorageResourceConfig>(std::move(common_result));
    EXPECT_FALSE(obj_storage_connection(common.client).request_timeout);
    auto zero = common;
    apply_be_obj_storage_defaults(zero.client);
    EXPECT_EQ(obj_storage_connection(zero.client).request_timeout, std::chrono::seconds(30));
    EXPECT_EQ(parsed.client, zero.client);
}

TEST(ObjStorageConfigTest, SharedParserUsesEquivalentAuthenticationAcrossProtocols) {
    ObjStorageProperties properties {
            {"PrOvIdEr", "  aWs "},   {"aws_endpoint", "https://endpoint"},
            {"AWS_REGION", "region"}, {"AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT"},
            {"AWS_ROLE_ARN", "role"}, {"AWS_EXTERNAL_ID", "external"}};
    auto from_properties_result = ObjStorageConfigParser::from_properties(
            properties, "bucket", AwsCredentialImplementation::V2,
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(from_properties_result))
            << std::get<std::string>(from_properties_result);
    auto from_properties = std::get<ObjStorageResourceConfig>(std::move(from_properties_result));

    TS3StorageParam param;
    param.__set_provider(TObjStorageType::AWS);
    param.__set_endpoint("https://endpoint");
    param.__set_region("region");
    param.__set_bucket("bucket");
    param.__set_root_path("warehouse");
    param.__set_cred_provider_type(TCredProviderType::DEFAULT);
    param.__set_role_arn("role");
    param.__set_external_id("external");
    // Vault has no connection fields. The legacy -1 sentinel leaves the same
    // fields unspecified in Thrift, ready for either host to supply its defaults.
    param.__set_max_conn(-1);
    param.__set_request_timeout_ms(-1);
    param.__set_conn_timeout_ms(-1);
    auto from_thrift_result = ObjStorageConfigParser::from_storage_param(
            param, AwsCredentialImplementation::V2, EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(from_thrift_result))
            << std::get<std::string>(from_thrift_result);
    auto from_thrift = std::get<ObjStorageResourceConfig>(std::move(from_thrift_result));

    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::S3);
    vault.set_endpoint("https://endpoint");
    vault.set_region("region");
    vault.set_bucket("bucket");
    vault.set_prefix("warehouse");
    vault.set_cred_provider_type(cloud::CredProviderTypePB::DEFAULT);
    vault.set_role_arn("role");
    vault.set_external_id("external");
    auto from_vault_result = ObjStorageConfigParser::from_vault(
            vault, AwsCredentialImplementation::V2, EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(from_vault_result))
            << std::get<std::string>(from_vault_result);
    auto from_vault = std::get<ObjStorageResourceConfig>(std::move(from_vault_result));

    EXPECT_EQ(from_properties.client, from_thrift.client);
    EXPECT_EQ(from_properties.client, from_vault.client);
    EXPECT_EQ(from_thrift.prefix, "warehouse");
    EXPECT_EQ(from_vault.prefix, "warehouse");
    EXPECT_TRUE(from_properties.prefix.empty());
    EXPECT_FALSE(from_vault.is_internal_bucket);
    const auto& aws = std::get<AwsObjStorageConfig>(from_vault.client);
    EXPECT_EQ(std::get<AwsCredentialChainType>(aws.auth.source), AwsCredentialChainType::Default);
    ASSERT_TRUE(aws.auth.assume_role);
    EXPECT_EQ(aws.auth.assume_role->role_arn, "role");
    EXPECT_FALSE(aws.auth.assume_role->request_timeout);
}

TEST(ObjStorageConfigTest, SharedParserReturnsErrorsFromEachInputFormat) {
    auto property_error = ObjStorageConfigParser::from_properties(
            {{"AWS_REQUEST_TIMEOUT_MS", "invalid"}}, "new-bucket", AwsCredentialImplementation::V2,
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<std::string>(property_error));
    EXPECT_NE(std::get<std::string>(property_error).find("AWS_REQUEST_TIMEOUT_MS"),
              std::string::npos);

    TS3StorageParam param;
    param.__set_bucket("new-bucket");
    param.__set_cred_provider_type(static_cast<TCredProviderType::type>(-1));
    auto thrift_error = ObjStorageConfigParser::from_storage_param(
            param, AwsCredentialImplementation::V2, EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<std::string>(thrift_error));
    EXPECT_NE(std::get<std::string>(thrift_error).find("unknown credential provider"),
              std::string::npos);

    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::GCP);
    vault.set_bucket("new-bucket");
    vault.set_ak("ak");
    vault.set_sk("sk");
    vault.mutable_credential()->mutable_gcp_credential();
    auto vault_error = ObjStorageConfigParser::from_vault(vault, AwsCredentialImplementation::V2,
                                                          EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<std::string>(vault_error));
    EXPECT_NE(std::get<std::string>(vault_error).find("combined"), std::string::npos);
}

TEST(ObjStorageConfigTest, InvalidNumericPropertyIsReportedBeforeMissingBucket) {
    S3URI uri("key-without-bucket");
    ASSERT_TRUE(uri.parse().ok());
    for (const char* name :
         {"AWS_MAX_CONNECTIONS", "AWS_REQUEST_TIMEOUT_MS", "AWS_CONNECTION_TIMEOUT_MS"}) {
        for (const char* value : {"", "x", "10ms", "-2"}) {
            auto parsed = ObjStorageConfigParser::from_properties(
                    {{name, value}}, uri.get_bucket(),
                    aws_credential_implementation(config::aws_credentials_provider_version),
                    EmptyCredentialsBehavior::ANONYMOUS);
            ASSERT_TRUE(std::holds_alternative<std::string>(parsed));
            EXPECT_NE(std::get<std::string>(parsed).find(name), std::string_view::npos);
        }
    }
    auto parsed = ObjStorageConfigParser::from_properties(
            {}, uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<std::string>(parsed));
    EXPECT_NE(std::get<std::string>(parsed).find("bucket is not specified"),
              std::string_view::npos);
}

TEST(ObjStorageConfigTest, AzureIgnoresLegacyThriftPoolSize) {
    TS3StorageParam thrift;
    thrift.__set_provider(TObjStorageType::AZURE);
    thrift.__set_bucket("container");
    thrift.__set_endpoint("https://account.blob.core.windows.net");
    thrift.__set_ak("account");
    thrift.__set_sk("key");
    for (int pool_size : {50, 100, 0, -1, -2}) {
        SCOPED_TRACE(pool_size);
        thrift.__set_max_conn(pool_size);
        auto result = ObjStorageConfigParser::from_storage_param(
                thrift, aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result))
                << std::get<std::string>(result);
        const auto& resource = std::get<ObjStorageResourceConfig>(result);
        const auto& azure = std::get<AzureObjStorageConfig>(resource.client);
        EXPECT_FALSE(azure.connection.max_connections);
        EXPECT_FALSE(azure.validate());
    }
}

TEST(ObjStorageConfigTest, AzurePreservesLegacyPoolSizePropertyParsing) {
    ObjStorageProperties props {{"provider", "azure"},
                                {"AWS_ENDPOINT", "https://account.blob.core.windows.net"},
                                {"AWS_ACCESS_KEY", "account"},
                                {"AWS_SECRET_KEY", "key"}};
    // The old conversion accepted a numeric prefix; Azure ignored the resulting pool size.
    for (const char* value : {"50", "100", "0", "-1", "-2", "100unused"}) {
        SCOPED_TRACE(value);
        props["AWS_MAX_CONNECTIONS"] = value;
        auto result = ObjStorageConfigParser::from_properties(
                props, "container",
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result))
                << std::get<std::string>(result);
        const auto& resource = std::get<ObjStorageResourceConfig>(result);
        const auto& azure = std::get<AzureObjStorageConfig>(resource.client);
        EXPECT_FALSE(azure.connection.max_connections);
        EXPECT_FALSE(azure.validate());
    }
    for (const char* value : {"", "x", "2147483648", "+100"}) {
        SCOPED_TRACE(value);
        props["AWS_MAX_CONNECTIONS"] = value;
        auto result = ObjStorageConfigParser::from_properties(
                props, "container",
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<std::string>(result));
        EXPECT_NE(std::get<std::string>(result).find("AWS_MAX_CONNECTIONS"),
                  std::string_view::npos);
    }
}

TEST(ObjStorageConfigTest, AzureTypedConfigStillRejectsPoolSize) {
    AzureObjStorageConfig config {
            .connection = {.endpoint = "https://account.blob.core.windows.net"},
            .auth = {.account_name = "account", .credential = AzureSharedKeyConfig {"key"}}};
    EXPECT_FALSE(config.validate());
    config.connection.max_connections = 100;
    auto error = config.validate();
    ASSERT_TRUE(error);
    EXPECT_EQ(*error, "Azure does not expose a connection pool size");
}

TEST(ObjStorageConfigTest, RejectsConflictingNativeGcpCredentialsInEachProtocol) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    auto props = ObjStorageConfigParser::from_properties(
            {{"provider", "gcp"},
             {"gs.credential_provider_type", "DEFAULT"},
             {"AWS_ACCESS_KEY", "ak"},
             {"AWS_SECRET_KEY", "sk"}},
            uri.get_bucket(),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<std::string>(props));
    EXPECT_NE(std::get<std::string>(props).find("combined"), std::string_view::npos);

    TS3StorageParam thrift;
    thrift.__set_provider(TObjStorageType::GCP);
    thrift.__set_bucket("bucket");
    thrift.__set_role_arn("role");
    TCredential credential;
    credential.__set_gcp_credential(TGcpCredential {});
    thrift.__set_credential(credential);
    auto from_thrift = ObjStorageConfigParser::from_storage_param(
            thrift, aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<std::string>(from_thrift));
    EXPECT_NE(std::get<std::string>(from_thrift).find("combined"), std::string_view::npos);

    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::GCP);
    vault.set_bucket("bucket");
    vault.set_cred_provider_type(cloud::CredProviderTypePB::ENV);
    vault.mutable_credential()->mutable_gcp_credential();
    auto pb = ObjStorageConfigParser::from_vault(
            vault, aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS);
    ASSERT_TRUE(std::holds_alternative<std::string>(pb));
    EXPECT_NE(std::get<std::string>(pb).find("combined"), std::string_view::npos);
}

TEST(ObjStorageConfigTest, RejectsMalformedAddressingFlags) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    for (const char* key : {"use_path_style", "AWS_NEED_OVERRIDE_ENDPOINT"}) {
        auto parsed = ObjStorageConfigParser::from_properties(
                {{key, "invalid"}}, uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<std::string>(parsed));
        EXPECT_NE(std::get<std::string>(parsed).find(key), std::string_view::npos);
    }
}

TEST(ObjStorageConfigTest, ExplicitProviderWinsOverEndpoint) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    for (const char* provider : {"AWS", "S3"}) {
        auto parsed_result = ObjStorageConfigParser::from_properties(
                {{"provider", provider}, {"AWS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com"}},
                uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(parsed_result))
                << std::get<std::string>(parsed_result);
        auto parsed = std::get<ObjStorageResourceConfig>(std::move(parsed_result));
        EXPECT_EQ(storage_provider(parsed.client), ObjStorageProvider::AWS);
    }
}

TEST(ObjStorageConfigTest, ParsesProviderTypesFromAllInputFormats) {
    struct {
        const char* name;
        TObjStorageType::type thrift;
        cloud::ObjectStoreInfoPB::Provider pb;
        ObjStorageProvider expected;
    } cases[] = {
            {"AWS", TObjStorageType::AWS, cloud::ObjectStoreInfoPB::S3, ObjStorageProvider::AWS},
            {"OSS", TObjStorageType::OSS, cloud::ObjectStoreInfoPB::OSS, ObjStorageProvider::OSS},
            {"COS", TObjStorageType::COS, cloud::ObjectStoreInfoPB::COS, ObjStorageProvider::COS},
            {"BOS", TObjStorageType::BOS, cloud::ObjectStoreInfoPB::BOS, ObjStorageProvider::BOS},
            {"OBS", TObjStorageType::OBS, cloud::ObjectStoreInfoPB::OBS, ObjStorageProvider::OBS},
            {"GCP", TObjStorageType::GCP, cloud::ObjectStoreInfoPB::GCP, ObjStorageProvider::GCP},
            {"TOS", TObjStorageType::TOS, cloud::ObjectStoreInfoPB::TOS, ObjStorageProvider::TOS},
            {"AZURE", TObjStorageType::AZURE, cloud::ObjectStoreInfoPB::AZURE,
             ObjStorageProvider::AZURE},
    };
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.name);
        std::map<std::string, std::string> props {
                {"provider", entry.name}, {"AWS_ENDPOINT", "endpoint"}, {"AWS_REGION", "region"},
                {"AWS_ACCESS_KEY", "ak"}, {"AWS_SECRET_KEY", "sk"},     {"use_path_style", "true"}};
        auto from_props_result = ObjStorageConfigParser::from_properties(
                props, uri.get_bucket(),
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(from_props_result))
                << std::get<std::string>(from_props_result);
        auto from_props = std::get<ObjStorageResourceConfig>(std::move(from_props_result));
        apply_be_obj_storage_defaults(from_props.client);
        TS3StorageParam param;
        param.__set_provider(entry.thrift);
        param.__set_bucket("bucket");
        param.__set_endpoint("endpoint");
        param.__set_region("region");
        param.__set_ak("ak");
        param.__set_sk("sk");
        param.__set_use_path_style(true);
        auto thrift_result = ObjStorageConfigParser::from_storage_param(
                param, aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(thrift_result))
                << std::get<std::string>(thrift_result);
        auto thrift = std::get<ObjStorageResourceConfig>(std::move(thrift_result));
        apply_be_obj_storage_defaults(thrift.client);
        cloud::ObjectStoreInfoPB info;
        info.set_provider(entry.pb);
        info.set_bucket("bucket");
        info.set_endpoint("endpoint");
        info.set_region("region");
        info.set_ak("ak");
        info.set_sk("sk");
        info.set_use_path_style(true);
        info.set_sse_enabled(true);
        auto pb_result = ObjStorageConfigParser::from_vault(
                info, aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(pb_result))
                << std::get<std::string>(pb_result);
        auto pb = std::get<ObjStorageResourceConfig>(std::move(pb_result));
        apply_be_obj_storage_defaults(pb.client);
        EXPECT_FALSE(pb.is_internal_bucket);
        EXPECT_TRUE(pb.sse_enabled);
        EXPECT_FALSE(thrift.is_internal_bucket);
        EXPECT_FALSE(from_props.is_internal_bucket);
        for (const auto* resource : {&from_props, &thrift, &pb}) {
            EXPECT_EQ(resource->bucket, "bucket");
            EXPECT_EQ(storage_provider(resource->client), entry.expected);
            EXPECT_FALSE(validate_obj_storage_config(resource->client));
            EXPECT_TRUE(obj_storage_connection(resource->client).endpoint.ends_with("://endpoint"));
        }
    }
}

} // namespace doris
