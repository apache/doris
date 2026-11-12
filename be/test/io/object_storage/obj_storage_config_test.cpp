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

#include <chrono>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/config.h"
#include "cpp/obj-storage/obj_storage_config_parser.h"
#include "io/object_storage/obj_storage_config_parser.h"
#include "util/s3_uri.h"

namespace doris {
namespace {
using namespace std::chrono_literals;
using Properties = std::map<std::string, std::string>;
using Vault = cloud::ObjectStoreInfoPB;

TEST(ObjStorageConfigTest, CredentialChangesPreserveTargetAndReplaceClient) {
    AwsObjStorageConfig aws;
    aws.connection.endpoint = "https://endpoint";
    aws.region = "region";
    aws.auth.source = AccessKeyObjStorageAuth {"ak", "sk", "token"};
    auto update = aws;
    std::get<AccessKeyObjStorageAuth>(update.auth.source).session_token = "rotated";
    EXPECT_TRUE(same_client_target(aws, update));
    std::unordered_map<ObjStorageClientConfig, int, ObjStorageClientHash> cache {{aws, 1}};
    EXPECT_FALSE(cache.contains(update));
    update = aws;
    update.connection.endpoint = "https://another-endpoint";
    EXPECT_FALSE(same_client_target(aws, update));
    update = aws;
    update.region = "another-region";
    EXPECT_FALSE(same_client_target(aws, update));
    update = aws;
    update.use_sdk_endpoint = true;
    EXPECT_FALSE(same_client_target(aws, update));
    GcpObjStorageConfig gcp;
    gcp.connection = aws.connection;
    gcp.region = aws.region;
    EXPECT_FALSE(same_client_target(aws, gcp));
}

TEST(ObjStorageConfigTest, ResourceLocationIsIndependentOfEveryClient) {
    S3URI first_uri("s3://first/path/to/object");
    S3URI second_uri("s3://second/other-object");
    ASSERT_TRUE(first_uri.parse().ok());
    ASSERT_TRUE(second_uri.parse().ok());
    for (const char* provider : {"AWS", "Azure"}) {
        SCOPED_TRACE(provider);
        Properties p {{"provider", provider},
                      {"AWS_ENDPOINT", "endpoint"},
                      {"AWS_REGION", "region"},
                      {"AWS_ACCESS_KEY", "account"},
                      {"AWS_SECRET_KEY", "secret"}};
        auto first = ObjStorageConfigParser::from_properties(p, first_uri);
        auto second = ObjStorageConfigParser::from_properties(p, second_uri);
        ASSERT_TRUE(first) << first.error().msg();
        ASSERT_TRUE(second) << second.error().msg();
        EXPECT_EQ(first->bucket, "first");
        EXPECT_EQ(second->bucket, "second");
        EXPECT_TRUE(first->prefix.empty());
        second->prefix = "/new-root/";
        second->is_internal_bucket = true;
        second->sse_enabled = true;
        EXPECT_EQ(second->normalized_prefix(), "new-root");
        EXPECT_NE(*first, *second);
        EXPECT_EQ(first->client, second->client);
        EXPECT_EQ(ObjStorageClientHash {}(first->client), ObjStorageClientHash {}(second->client));
    }
}

TEST(ObjStorageConfigTest, InfersMissingProviderFromEndpointHost) {
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
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.endpoint);
        auto props =
                ObjStorageConfigParser::from_properties({{"AWS_ENDPOINT", entry.endpoint}}, uri);
        ASSERT_TRUE(props) << props.error().msg();
        EXPECT_EQ(storage_provider(props->client), entry.expected);
        TS3StorageParam param;
        param.__set_bucket("bucket");
        param.__set_endpoint(entry.endpoint);
        auto thrift = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(thrift) << thrift.error().msg();
        EXPECT_EQ(storage_provider(thrift->client), entry.expected);
        param.__set_provider(TObjStorageType::UNKNOWN);
        thrift = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(thrift) << thrift.error().msg();
        EXPECT_EQ(storage_provider(thrift->client), entry.expected);
        Vault info;
        info.set_bucket("bucket");
        info.set_endpoint(entry.endpoint);
        ASSERT_FALSE(info.has_provider());
        auto vault = ObjStorageConfigParser::from_vault(info);
        ASSERT_TRUE(vault) << vault.error().msg();
        EXPECT_EQ(storage_provider(vault->client), entry.expected);
    }
}

TEST(ObjStorageConfigTest, ParsesAllProvidersFromAllInputFormats) {
    const struct {
        const char* name;
        TObjStorageType::type thrift;
        Vault::Provider vault;
        ObjStorageProvider expected;
    } cases[] = {
            {"AWS", TObjStorageType::AWS, Vault::S3, ObjStorageProvider::AWS},
            {"S3", TObjStorageType::AWS, Vault::S3, ObjStorageProvider::AWS},
            {"OSS", TObjStorageType::OSS, Vault::OSS, ObjStorageProvider::OSS},
            {"COS", TObjStorageType::COS, Vault::COS, ObjStorageProvider::COS},
            {"BOS", TObjStorageType::BOS, Vault::BOS, ObjStorageProvider::BOS},
            {"OBS", TObjStorageType::OBS, Vault::OBS, ObjStorageProvider::OBS},
            {"GCS", TObjStorageType::GCP, Vault::GCP, ObjStorageProvider::GCP},
            {"TOS", TObjStorageType::TOS, Vault::TOS, ObjStorageProvider::TOS},
            {"Azure", TObjStorageType::AZURE, Vault::AZURE, ObjStorageProvider::AZURE},
    };
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    const char* endpoint = "https://oss-cn-hangzhou.aliyuncs.com";
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.name);
        Properties p {{"provider", entry.name}, {"aws_endpoint", endpoint},
                      {"AWS_REGION", "region"}, {"AWS_ACCESS_KEY", "ak"},
                      {"AWS_SECRET_KEY", "sk"}, {"USE_PATH_STYLE", "true"}};
        auto properties = ObjStorageConfigParser::from_properties(p, uri);
        TS3StorageParam param;
        param.__set_provider(entry.thrift);
        param.__set_bucket("bucket");
        param.__set_root_path("/root");
        param.__set_endpoint(endpoint);
        param.__set_region("region");
        param.__set_ak("ak");
        param.__set_sk("sk");
        param.__set_use_path_style(true);
        auto thrift = ObjStorageConfigParser::from_storage_param(param);
        Vault info;
        info.set_provider(entry.vault);
        info.set_bucket("bucket");
        info.set_prefix("/root");
        info.set_endpoint(endpoint);
        info.set_region("region");
        info.set_ak("ak");
        info.set_sk("sk");
        info.set_use_path_style(true);
        info.set_sse_enabled(true);
        auto vault = ObjStorageConfigParser::from_vault(info);
        ASSERT_TRUE(properties) << properties.error().msg();
        ASSERT_TRUE(thrift) << thrift.error().msg();
        ASSERT_TRUE(vault) << vault.error().msg();
        EXPECT_TRUE(properties->prefix.empty());
        EXPECT_EQ(thrift->prefix, "/root");
        EXPECT_FALSE(thrift->is_internal_bucket);
        EXPECT_FALSE(thrift->sse_enabled);
        EXPECT_EQ(vault->prefix, "/root");
        EXPECT_TRUE(vault->is_internal_bucket);
        EXPECT_TRUE(vault->sse_enabled);
        EXPECT_EQ(properties->client, vault->client);
        for (const auto* resource : {&*properties, &*thrift, &*vault}) {
            EXPECT_EQ(resource->bucket, "bucket");
            EXPECT_EQ(storage_provider(resource->client), entry.expected);
            EXPECT_EQ(obj_storage_connection(resource->client).endpoint, endpoint);
            EXPECT_FALSE(validate_obj_storage_config(resource->client));
            std::visit(
                    [](const auto& c) {
                        using C = std::decay_t<decltype(c)>;
                        if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
                            EXPECT_EQ(c.auth.account_name, "ak");
                            EXPECT_EQ(c.auth.account_key, "sk");
                        } else {
                            EXPECT_EQ(c.region, "region");
                            EXPECT_TRUE(c.use_path_style);
                            if constexpr (std::is_same_v<C, GcpObjStorageConfig>) {
                                EXPECT_EQ(std::get<GcpHmacObjStorageAuth>(c.auth).access_id, "ak");
                            } else if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                                EXPECT_EQ(
                                        std::get<AccessKeyObjStorageAuth>(c.auth.source).access_key,
                                        "ak");
                            } else {
                                EXPECT_EQ(std::get<AccessKeyObjStorageAuth>(c.auth).access_key,
                                          "ak");
                            }
                        }
                    },
                    resource->client);
        }
        const auto& t = obj_storage_connection(thrift->client);
        EXPECT_EQ(t.request_timeout, 3000ms);
        EXPECT_EQ(t.connect_timeout, 1000ms);
        if (entry.expected != ObjStorageProvider::AZURE) {
            EXPECT_EQ(t.max_connections, 50);
            EXPECT_EQ(obj_storage_connection(vault->client).max_connections, 102400);
            EXPECT_EQ(obj_storage_connection(vault->client).request_timeout, 30s);
        } else {
            EXPECT_FALSE(t.max_connections);
        }
    }
}

TEST(ObjStorageConfigTest, AwsSdkEndpointIgnoresOverrideAndPreservesScheme) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    Properties p {{"provider", "AWS"},
                  {"AWS_ENDPOINT", "unused"},
                  {"AWS_REGION", "region"},
                  {"AWS_NEED_OVERRIDE_ENDPOINT", "false"}};
    auto first = ObjStorageConfigParser::from_properties(p, uri);
    ASSERT_TRUE(first) << first.error().msg();
    const auto& aws = std::get<AwsObjStorageConfig>(first->client);
    EXPECT_TRUE(aws.use_sdk_endpoint);
    EXPECT_TRUE(aws.connection.endpoint.empty());
    p["AWS_ENDPOINT"] = "another-unused";
    auto second = ObjStorageConfigParser::from_properties(p, uri);
    ASSERT_TRUE(second) << second.error().msg();
    EXPECT_EQ(first->client, second->client);
    EXPECT_TRUE(same_client_target(first->client, second->client));
    normalize_obj_storage_endpoint(first->client, "http");
    EXPECT_TRUE(aws.sdk_endpoint_http);
    for (const char* provider : {"GCP", "BOS", "COS", "TOS", "OSS", "OBS"}) {
        SCOPED_TRACE(provider);
        p["provider"] = provider;
        EXPECT_FALSE(ObjStorageConfigParser::from_properties(p, uri));
    }
}

TEST(ObjStorageConfigTest, ReportsFormatAndResourceErrors) {
    S3URI uri("key-without-bucket");
    ASSERT_TRUE(uri.parse().ok());
    for (const char* name :
         {"AWS_MAX_CONNECTIONS", "AWS_REQUEST_TIMEOUT_MS", "AWS_CONNECTION_TIMEOUT_MS"}) {
        for (const char* invalid : {"bad", "12suffix", "-2", "2147483648", ""}) {
            SCOPED_TRACE(invalid);
            auto parsed = ObjStorageConfigParser::from_properties({{name, invalid}}, uri);
            ASSERT_FALSE(parsed);
            EXPECT_NE(parsed.error().msg().find(name), std::string_view::npos);
        }
    }
    auto parsed = ObjStorageConfigParser::from_properties({}, uri);
    ASSERT_FALSE(parsed);
    EXPECT_NE(parsed.error().msg().find("bucket is not specified"), std::string_view::npos);
    EXPECT_FALSE(ObjStorageConfigParser::from_storage_param(TS3StorageParam {}));
    EXPECT_FALSE(ObjStorageConfigParser::from_vault(Vault {}));
}

TEST(ObjStorageConfigTest, ExplicitZeroTimeoutSurvivesDefaulting) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    auto parsed = ObjStorageConfigParser::from_properties({{"AWS_REQUEST_TIMEOUT_MS", "0"},
                                                           {"AWS_MAX_CONNECTIONS", "42"},
                                                           {"AWS_CONNECTION_TIMEOUT_MS", "0"}},
                                                          uri);
    ASSERT_TRUE(parsed) << parsed.error().msg();
    auto& c = obj_storage_connection(parsed->client);
    EXPECT_EQ(c.request_timeout, 0ms);
    EXPECT_EQ(c.connect_timeout, 0ms);
    EXPECT_EQ(c.max_connections, 42);
    c.ca_cert_file = "/chosen-ca.pem";
    c.max_retries = 0;
    apply_be_obj_storage_defaults(parsed->client);
    EXPECT_EQ(c.ca_cert_file, "/chosen-ca.pem");
    EXPECT_EQ(c.max_retries, 0);
}

TEST(ObjStorageConfigTest, AwsAuthenticationIsSelectedUsingImplementation) {
    const auto original = config::aws_credentials_provider_version;
    struct RestoreVersion {
        std::string value;
        ~RestoreVersion() { config::aws_credentials_provider_version = value; }
    } restore {original};
    TS3StorageParam param;
    param.__set_bucket("bucket");
    param.__set_role_arn("role");
    param.__set_external_id("external");
    for (const char* version : {"v1", "v2"}) {
        SCOPED_TRACE(version);
        config::aws_credentials_provider_version = version;
        param.__isset.cred_provider_type = false;
        auto omitted = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(omitted) << omitted.error().msg();
        auto& auth = std::get<AwsObjStorageConfig>(omitted->client).auth;
        if (std::string_view(version) == "v1") {
            EXPECT_EQ(auth.implementation, AwsCredentialImplementation::V1);
            EXPECT_TRUE(std::holds_alternative<AnonymousObjStorageAuth>(auth.source));
            EXPECT_FALSE(auth.assume_role);
        } else {
            EXPECT_EQ(auth.implementation, AwsCredentialImplementation::V2);
            EXPECT_EQ(std::get<AwsCredentialChainType>(auth.source),
                      AwsCredentialChainType::Default);
            EXPECT_TRUE(auth.assume_role);
        }
        param.__set_cred_provider_type(TCredProviderType::DEFAULT);
        auto explicit_default = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(explicit_default) << explicit_default.error().msg();
        EXPECT_EQ(omitted->client, explicit_default->client);
        // FE's role path explicitly selects INSTANCE_PROFILE when the user omitted the source.
        param.__set_cred_provider_type(TCredProviderType::INSTANCE_PROFILE);
        auto instance = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(instance) << instance.error().msg();
        const auto& role = std::get<AwsObjStorageConfig>(instance->client).auth;
        EXPECT_EQ(std::get<AwsCredentialChainType>(role.source),
                  AwsCredentialChainType::InstanceProfile);
        ASSERT_TRUE(role.assume_role);
        EXPECT_EQ(role.assume_role->role_arn, "role");
        EXPECT_EQ(role.assume_role->external_id, "external");
        EXPECT_EQ(role.assume_role->request_timeout,
                  std::chrono::milliseconds(config::aws_client_request_timeout_ms));
        param.__set_ak("ak");
        param.__set_sk("sk");
        param.__set_token("token");
        auto keys = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(keys) << keys.error().msg();
        const auto& static_auth = std::get<AwsObjStorageConfig>(keys->client).auth;
        EXPECT_FALSE(static_auth.assume_role);
        EXPECT_EQ(std::get<AccessKeyObjStorageAuth>(static_auth.source).session_token, "token");
        param.__set_ak("");
        param.__set_sk("");
        param.__set_token("");
    }
}

TEST(ObjStorageConfigTest, GcpNativeAuthValidatesWireConflicts) {
    TS3StorageParam param;
    param.__set_bucket("bucket");
    TCredential credential;
    TGcpCredential native;
    native.__set_credential_provider_type(TGcpCredentialProviderType::COMPUTE_ENGINE);
    credential.__set_gcp_credential(native);
    param.__set_credential(credential);
    param.__set_provider(TObjStorageType::GCP);
    Vault info;
    info.set_bucket("bucket");
    info.set_provider(Vault::GCP);
    info.mutable_credential()->mutable_gcp_credential()->set_credential_provider_type(
            cloud::GcpCredentialPB::COMPUTE_ENGINE);
    auto thrift = ObjStorageConfigParser::from_storage_param(param);
    auto vault = ObjStorageConfigParser::from_vault(info);
    ASSERT_TRUE(thrift) << thrift.error().msg();
    ASSERT_TRUE(vault) << vault.error().msg();
    for (const auto* cfg : {&*thrift, &*vault}) {
        EXPECT_EQ(std::get<GcpNativeObjStorageAuth>(std::get<GcpObjStorageConfig>(cfg->client).auth)
                          .source,
                  GcpCredentialSource::ComputeEngine);
    }
    param.__set_role_arn("aws-role");
    info.set_role_arn("aws-role");
    EXPECT_FALSE(ObjStorageConfigParser::from_storage_param(param));
    EXPECT_FALSE(ObjStorageConfigParser::from_vault(info));
    param.__set_role_arn("");
    info.clear_role_arn();
    param.__set_provider(TObjStorageType::OSS);
    info.set_provider(Vault::OSS);
    EXPECT_FALSE(ObjStorageConfigParser::from_storage_param(param));
    EXPECT_FALSE(ObjStorageConfigParser::from_vault(info));
}
} // namespace
} // namespace doris
