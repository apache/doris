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

#include "io/object_storage/obj_storage_config_parser.h"
#include "util/s3_uri.h"

namespace doris {

TEST(ObjStorageConfigTest, CredentialChangesPreserveTarget) {
    AwsConfig conf;
    conf.endpoint = "endpoint";
    conf.region = "region";
    auto update = conf;
    update.ak = "new-ak";
    update.sk = "new-sk";
    update.token = "new-token";
    EXPECT_TRUE(same_storage_target(conf, update));
    EXPECT_NE(obj_storage_client_cache_key(conf), obj_storage_client_cache_key(update));

    update.endpoint = "another-endpoint";
    EXPECT_FALSE(same_storage_target(conf, update));
    update = conf;
    update.region = "another-region";
    EXPECT_FALSE(same_storage_target(conf, update));
    update = conf;
    update.need_override_endpoint = false;
    EXPECT_FALSE(same_storage_target(conf, update));
    GcpConfig gcp;
    gcp.endpoint = conf.endpoint;
    gcp.region = conf.region;
    EXPECT_FALSE(same_storage_target(conf, gcp));
}

TEST(ObjStorageConfigTest, AzureClientIsBoundToItsContainer) {
    AzureConfig conf;
    conf.bucket = "first";
    conf.endpoint = "endpoint";
    conf.region = "region";
    auto update = conf;
    update.sk = "new-sk";
    EXPECT_TRUE(same_storage_target(conf, update));
    update.bucket = "second";
    EXPECT_FALSE(same_storage_target(conf, update));
    AwsConfig aws;
    aws.bucket = conf.bucket;
    aws.endpoint = conf.endpoint;
    aws.region = conf.region;
    EXPECT_FALSE(same_storage_target(conf, aws));
}

TEST(ObjStorageConfigTest, BucketIsPartOfClientIdentityOnlyForAzure) {
    S3URI first_uri("s3://first/path");
    S3URI second_uri("s3://second/path");
    ASSERT_TRUE(first_uri.parse().ok());
    ASSERT_TRUE(second_uri.parse().ok());
    std::map<std::string, std::string> props {{"AWS_ENDPOINT", "endpoint"},
                                              {"AWS_REGION", "region"}};
    ObjStorageConfig first;
    ObjStorageConfig second;
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(props, first_uri, &first).ok());
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(props, second_uri, &second).ok());
    const auto first_key = obj_storage_client_cache_key(first);
    const auto second_key = obj_storage_client_cache_key(second);
    EXPECT_EQ(first_key, second_key);
    EXPECT_EQ(ObjStorageClientHash {}(first_key), ObjStorageClientHash {}(second_key));
    EXPECT_NE(storage_config_base(first).bucket, storage_config_base(second).bucket);

    props["provider"] = "azure";
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(props, first_uri, &first).ok());
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(props, second_uri, &second).ok());
    EXPECT_EQ("first", std::get<AzureConfig>(first).bucket);
    EXPECT_NE(obj_storage_client_cache_key(first), obj_storage_client_cache_key(second));
}

TEST(ObjStorageConfigTest, LegacyResourceWithoutProviderUsesAws) {
    TS3StorageParam param;
    param.provider = TObjStorageType::UNKNOWN;
    param.bucket = "legacy";
    param.root_path = "/tablets";
    param.endpoint = "s3.example";
    const auto config = ObjStorageConfigParser::from_storage_param(param);
    ASSERT_TRUE(config.has_value()) << config.error().msg();
    const auto* aws = std::get_if<AwsConfig>(&*config);
    ASSERT_NE(aws, nullptr);
    EXPECT_EQ("legacy", aws->bucket);
    EXPECT_EQ("/tablets", aws->prefix);
    EXPECT_EQ("s3.example", aws->endpoint);
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
        ObjStorageConfig parsed;
        ASSERT_TRUE(
                ObjStorageConfigParser::from_properties(
                        {{"AWS_ENDPOINT", entry.endpoint}, {"AWS_REGION", "region"}}, uri, &parsed)
                        .ok());
        EXPECT_EQ(entry.expected, storage_provider(parsed));

        TS3StorageParam param;
        param.endpoint = entry.endpoint;
        auto resource = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(resource.has_value()) << resource.error().msg();
        EXPECT_EQ(entry.expected, storage_provider(*resource));
        param.__set_provider(TObjStorageType::UNKNOWN);
        resource = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(resource.has_value()) << resource.error().msg();
        EXPECT_EQ(entry.expected, storage_provider(*resource));

        cloud::ObjectStoreInfoPB info;
        info.set_endpoint(entry.endpoint);
        ASSERT_FALSE(info.has_provider());
        auto vault = ObjStorageConfigParser::from_vault(info);
        ASSERT_TRUE(vault.has_value()) << vault.error().msg();
        EXPECT_EQ(entry.expected, storage_provider(*vault));
    }
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
    ObjStorageConfig parsed;
    ASSERT_TRUE(ObjStorageConfigParser::from_properties(props, uri, &parsed).ok());
    const auto* aws = std::get_if<AwsConfig>(&parsed);
    ASSERT_NE(aws, nullptr);
    EXPECT_EQ("bucket", aws->bucket);
    EXPECT_EQ("endpoint", aws->endpoint);
    EXPECT_EQ("region", aws->region);
    EXPECT_EQ("ak", aws->ak);
    EXPECT_EQ("sk", aws->sk);
    EXPECT_EQ("token", aws->token);
    EXPECT_FALSE(aws->need_override_endpoint);
    EXPECT_FALSE(aws->use_virtual_addressing);
    EXPECT_EQ(42, aws->max_connections);
    EXPECT_EQ(2345, aws->request_timeout_ms);
    EXPECT_EQ(678, aws->connect_timeout_ms);
    EXPECT_EQ("role", aws->role_arn);
    EXPECT_EQ("external", aws->external_id);
    EXPECT_EQ(CredProviderType::Env, aws->cred_provider_type);
}

TEST(ObjStorageConfigTest, PropertiesRetainExistingConnectionAndResetFilesystemPath) {
    GcpConfig original;
    original.endpoint = "storage.googleapis.com";
    original.region = "us-east1";
    original.bucket = "previous-bucket";
    original.prefix = "previous-prefix";
    original.use_virtual_addressing = false;
    original.need_override_endpoint = false;
    original.max_connections = 80;
    ObjStorageConfig parsed = original;
    S3URI uri("s3://new-bucket/new-key");
    ASSERT_TRUE(uri.parse().ok());
    ASSERT_TRUE(ObjStorageConfigParser::from_properties({}, uri, &parsed).ok());
    const auto* config = std::get_if<GcpConfig>(&parsed);
    ASSERT_NE(config, nullptr);
    EXPECT_EQ(original.endpoint, config->endpoint);
    EXPECT_EQ(original.region, config->region);
    EXPECT_EQ(original.max_connections, config->max_connections);
    EXPECT_FALSE(config->need_override_endpoint);
    EXPECT_EQ("new-bucket", config->bucket);
    EXPECT_TRUE(config->prefix.empty());
    EXPECT_TRUE(config->use_virtual_addressing);
}

TEST(ObjStorageConfigTest, InvalidNumericPropertyIsReportedBeforeMissingBucket) {
    S3URI uri("key-without-bucket");
    ASSERT_TRUE(uri.parse().ok());
    ObjStorageConfig parsed;
    for (auto name :
         {"AWS_MAX_CONNECTIONS", "AWS_REQUEST_TIMEOUT_MS", "AWS_CONNECTION_TIMEOUT_MS"}) {
        SCOPED_TRACE(name);
        auto status = ObjStorageConfigParser::from_properties({{name, "bad"}}, uri, &parsed);
        ASSERT_FALSE(status.ok());
        EXPECT_NE(std::string_view::npos, status.msg().find(name));
    }
    auto status = ObjStorageConfigParser::from_properties({}, uri, &parsed);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(std::string_view::npos, status.msg().find("bucket is not specified"));
}

TEST(ObjStorageConfigTest, ExplicitAwsTakesPrecedenceOverOssEndpoint) {
    const char* endpoint = "oss-cn-hangzhou.aliyuncs.com";
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    for (const char* provider : {"AWS", "S3"}) {
        SCOPED_TRACE(provider);
        ObjStorageConfig parsed;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties({{"provider", provider},
                                                             {"AWS_ENDPOINT", endpoint},
                                                             {"AWS_REGION", "region"}},
                                                            uri, &parsed)
                            .ok());
        EXPECT_TRUE(std::holds_alternative<AwsConfig>(parsed));
    }

    TS3StorageParam param;
    param.endpoint = endpoint;
    param.__set_provider(TObjStorageType::AWS);
    auto resource = ObjStorageConfigParser::from_storage_param(param);
    ASSERT_TRUE(resource.has_value()) << resource.error().msg();
    EXPECT_TRUE(std::holds_alternative<AwsConfig>(*resource));

    cloud::ObjectStoreInfoPB info;
    info.set_endpoint(endpoint);
    info.set_provider(cloud::ObjectStoreInfoPB::S3);
    auto vault = ObjStorageConfigParser::from_vault(info);
    ASSERT_TRUE(vault.has_value()) << vault.error().msg();
    EXPECT_TRUE(std::holds_alternative<AwsConfig>(*vault));
}

TEST(ObjStorageConfigTest, SwitchingProviderDropsAwsRoleAndRetainsConnection) {
    AwsConfig aws;
    aws.endpoint = "shared-endpoint";
    aws.region = "region";
    aws.role_arn = "aws-role";
    aws.external_id = "aws-external-id";
    aws.max_connections = 23;
    ObjStorageConfig parsed = aws;
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());

    ASSERT_TRUE(ObjStorageConfigParser::from_properties({{"provider", "BOS"}}, uri, &parsed).ok());
    const auto& bos = std::get<BosConfig>(parsed);
    EXPECT_EQ(aws.endpoint, bos.endpoint);
    EXPECT_EQ(aws.max_connections, bos.max_connections);
    ASSERT_TRUE(ObjStorageConfigParser::from_properties({{"provider", "AWS"}}, uri, &parsed).ok());
    const auto& reparsed = std::get<AwsConfig>(parsed);
    EXPECT_TRUE(reparsed.role_arn.empty());
    EXPECT_TRUE(reparsed.external_id.empty());
    EXPECT_EQ(aws.max_connections, reparsed.max_connections);
}

TEST(ObjStorageConfigTest, WireParsersRejectGcpCredentialConflicts) {
    TS3StorageParam resource;
    TCredential credential;
    credential.__set_gcp_credential(TGcpCredential {});
    resource.__set_credential(credential);
    cloud::ObjectStoreInfoPB vault;
    vault.mutable_credential()->mutable_gcp_credential();

    resource.__set_provider(TObjStorageType::GCP);
    vault.set_provider(cloud::ObjectStoreInfoPB::GCP);
    ASSERT_TRUE(ObjStorageConfigParser::from_storage_param(resource).has_value());
    ASSERT_TRUE(ObjStorageConfigParser::from_vault(vault).has_value());

    resource.__set_role_arn("aws-role");
    vault.set_role_arn("aws-role");
    EXPECT_FALSE(ObjStorageConfigParser::from_storage_param(resource).has_value());
    EXPECT_FALSE(ObjStorageConfigParser::from_vault(vault).has_value());

    resource.__set_role_arn("");
    vault.clear_role_arn();
    resource.__set_provider(TObjStorageType::OSS);
    vault.set_provider(cloud::ObjectStoreInfoPB::OSS);
    EXPECT_FALSE(ObjStorageConfigParser::from_storage_param(resource).has_value());
    EXPECT_FALSE(ObjStorageConfigParser::from_vault(vault).has_value());
}

TEST(ObjStorageConfigTest, ParsesProviderTypesFromAllInputFormats) {
    struct {
        const char* name;
        TObjStorageType::type thrift;
        cloud::ObjectStoreInfoPB::Provider pb;
        ObjStorageConfig expected;
    } cases[] = {
            {"AWS", TObjStorageType::AWS, cloud::ObjectStoreInfoPB::S3, AwsConfig {}},
            {"OSS", TObjStorageType::OSS, cloud::ObjectStoreInfoPB::OSS, OssConfig {}},
            {"COS", TObjStorageType::COS, cloud::ObjectStoreInfoPB::COS, CosConfig {}},
            {"BOS", TObjStorageType::BOS, cloud::ObjectStoreInfoPB::BOS, BosConfig {}},
            {"OBS", TObjStorageType::OBS, cloud::ObjectStoreInfoPB::OBS, ObsConfig {}},
            {"GCP", TObjStorageType::GCP, cloud::ObjectStoreInfoPB::GCP, GcpConfig {}},
            {"TOS", TObjStorageType::TOS, cloud::ObjectStoreInfoPB::TOS, TosConfig {}},
            {"AZURE", TObjStorageType::AZURE, cloud::ObjectStoreInfoPB::AZURE, AzureConfig {}},
    };
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.name);
        std::map<std::string, std::string> props {{"provider", entry.name},
                                                  {"AWS_ENDPOINT", "endpoint"},
                                                  {"AWS_REGION", "region"},
                                                  {"AWS_ACCESS_KEY", "ak"},
                                                  {"AWS_SECRET_KEY", "sk"},
                                                  {"use_path_style", "true"},
                                                  {"AWS_CREDENTIALS_PROVIDER_TYPE", "ENV"}};
        ObjStorageConfig from_props;
        ASSERT_TRUE(ObjStorageConfigParser::from_properties(props, uri, &from_props).ok());

        TS3StorageParam param;
        param.__set_provider(entry.thrift);
        param.__set_cred_provider_type(TCredProviderType::ENV);
        param.bucket = "bucket";
        param.endpoint = "endpoint";
        param.region = "region";
        param.ak = "ak";
        param.sk = "sk";
        param.use_path_style = true;
        const auto from_resource = ObjStorageConfigParser::from_storage_param(param);

        cloud::ObjectStoreInfoPB info;
        info.set_provider(entry.pb);
        info.set_cred_provider_type(cloud::CredProviderTypePB::ENV);
        info.set_bucket("bucket");
        info.set_endpoint("endpoint");
        info.set_region("region");
        info.set_ak("ak");
        info.set_sk("sk");
        info.set_use_path_style(true);
        const auto from_vault = ObjStorageConfigParser::from_vault(info);

        ASSERT_TRUE(from_resource.has_value()) << from_resource.error().msg();
        ASSERT_TRUE(from_vault.has_value()) << from_vault.error().msg();
        for (const auto* parsed : std::initializer_list<const ObjStorageConfig*> {
                     &from_props, &*from_resource, &*from_vault}) {
            ASSERT_EQ(entry.expected.index(), parsed->index());
            const auto& config = storage_config_base(*parsed);
            EXPECT_EQ("endpoint", config.endpoint);
            EXPECT_EQ("region", config.region);
            EXPECT_EQ("ak", config.ak);
            EXPECT_EQ("sk", config.sk);
            EXPECT_EQ("bucket", config.bucket);
            std::visit(
                    [](const auto& provider) {
                        if constexpr (requires { provider.use_virtual_addressing; }) {
                            EXPECT_FALSE(provider.use_virtual_addressing);
                            EXPECT_EQ(CredProviderType::Env, provider.cred_provider_type);
                        }
                    },
                    *parsed);
        }
    }
}

} // namespace doris
