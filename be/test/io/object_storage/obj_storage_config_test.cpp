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

#include "common/config.h"
#include "cpp/obj-storage/obj_storage_properties.h"
#include "io/object_storage/obj_storage_config_parser.h"
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
    azure.auth = {"account", "key"};
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
    auto parsed = ObjStorageConfigParser::from_storage_param(param);
    ASSERT_TRUE(parsed.has_value()) << parsed.error();
    EXPECT_EQ(parsed->bucket, "legacy");
    EXPECT_EQ(parsed->prefix, "/tablets");
    const auto& aws = std::get<AwsObjStorageConfig>(parsed->client);
    EXPECT_EQ(aws.connection.endpoint, "https://s3.example");
    EXPECT_EQ(aws.connection.max_connections, 50);
    EXPECT_EQ(aws.connection.request_timeout, std::chrono::milliseconds(3000));
    EXPECT_EQ(aws.connection.connect_timeout, std::chrono::milliseconds(1000));
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
        auto parsed = ObjStorageConfigParser::from_properties(
                {{"AWS_ENDPOINT", entry.endpoint}, {"AWS_REGION", "region"}}, uri);
        ASSERT_TRUE(parsed.has_value()) << parsed.error();
        EXPECT_EQ(entry.expected, storage_provider(parsed->client));
        TS3StorageParam param;
        param.__set_bucket("bucket");
        param.__set_endpoint(entry.endpoint);
        auto thrift = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(thrift.has_value()) << thrift.error();
        EXPECT_EQ(entry.expected, storage_provider(thrift->client));
        cloud::ObjectStoreInfoPB vault;
        vault.set_bucket("bucket");
        vault.set_endpoint(entry.endpoint);
        auto pb = ObjStorageConfigParser::from_vault(vault);
        ASSERT_TRUE(pb.has_value()) << pb.error();
        EXPECT_EQ(entry.expected, storage_provider(pb->client));
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
    auto parsed = ObjStorageConfigParser::from_properties(props, uri);
    ASSERT_TRUE(parsed.has_value()) << parsed.error();
    EXPECT_EQ(parsed->bucket, "bucket");
    const auto& aws = std::get<AwsObjStorageConfig>(parsed->client);
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
    auto second = ObjStorageConfigParser::from_properties(props, uri);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(parsed->client, second->client);
    props["provider"] = "BOS";
    auto unsupported = ObjStorageConfigParser::from_properties(props, uri);
    ASSERT_FALSE(unsupported.has_value());
    EXPECT_NE(unsupported.error().msg().find("requires provider=AWS"), std::string::npos);
}

TEST(ObjStorageConfigTest, NonAwsRolePreservesLegacySdkAuthentication) {
    using Vault = cloud::ObjectStoreInfoPB;
    Vault info;
    info.set_provider(Vault::BOS);
    info.set_bucket("bucket");
    info.set_region("region");
    info.set_endpoint("endpoint");
    info.set_role_arn("role");
    info.set_external_id("external");
    info.set_cred_provider_type(cloud::CredProviderTypePB::ENV);
    ObjStorageResourceConfig parsed;
    ASSERT_FALSE(parse_vault_storage_config(info, AwsCredentialImplementation::V2,
                                            EmptyCredentialsBehavior::ANONYMOUS, &parsed));
    const auto& chain =
            std::get<S3CredentialChain>(std::get<BosObjStorageConfig>(parsed.client).auth);
    EXPECT_EQ(std::get<AwsCredentialChainType>(chain.source), AwsCredentialChainType::Environment);
    ASSERT_TRUE(chain.assume_role);
    EXPECT_EQ(chain.assume_role->role_arn, "role");
    EXPECT_EQ(chain.assume_role->external_id, "external");
    info.set_ak("sts-ak");
    info.set_sk("sts-sk");
    ASSERT_FALSE(parse_vault_storage_config(info, AwsCredentialImplementation::V2,
                                            EmptyCredentialsBehavior::ANONYMOUS, &parsed));
    EXPECT_TRUE(std::holds_alternative<AccessKeyObjStorageAuth>(
            std::get<BosObjStorageConfig>(parsed.client).auth));
}

TEST(ObjStorageConfigTest, EffectiveTimeoutPreservesExplicitZero) {
    S3URI uri("s3://bucket/file");
    ASSERT_TRUE(uri.parse().ok());
    ObjStorageProperties props {{"AWS_ENDPOINT", "endpoint"}, {"AWS_REGION", "region"}};
    auto parsed = ObjStorageConfigParser::from_properties(props, uri);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(obj_storage_connection(parsed->client).request_timeout, std::chrono::seconds(30));
    props["AWS_REQUEST_TIMEOUT_MS"] = "0";
    auto zero = ObjStorageConfigParser::from_properties(props, uri);
    ASSERT_TRUE(zero.has_value());
    EXPECT_EQ(obj_storage_connection(zero->client).request_timeout, std::chrono::milliseconds(0));
    EXPECT_NE(parsed->client, zero->client);
}

TEST(ObjStorageConfigTest, InvalidNumericPropertyIsReportedBeforeMissingBucket) {
    S3URI uri("key-without-bucket");
    ASSERT_TRUE(uri.parse().ok());
    for (const char* name :
         {"AWS_MAX_CONNECTIONS", "AWS_REQUEST_TIMEOUT_MS", "AWS_CONNECTION_TIMEOUT_MS"}) {
        for (const char* value : {"x", "10ms", "-2"}) {
            auto parsed = ObjStorageConfigParser::from_properties({{name, value}}, uri);
            ASSERT_FALSE(parsed.has_value());
            EXPECT_NE(parsed.error().msg().find(name), std::string_view::npos);
        }
    }
    auto parsed = ObjStorageConfigParser::from_properties({}, uri);
    ASSERT_FALSE(parsed.has_value());
    EXPECT_NE(parsed.error().msg().find("bucket is not specified"), std::string_view::npos);
}

TEST(ObjStorageConfigTest, AzureAcceptsLegacyThriftDefaultAndRejectsExplicitPoolSize) {
    TS3StorageParam thrift;
    thrift.__set_provider(TObjStorageType::AZURE);
    thrift.__set_bucket("container");
    thrift.__set_max_conn(50);
    auto legacy = ObjStorageConfigParser::from_storage_param(thrift);
    ASSERT_TRUE(legacy.has_value());
    EXPECT_FALSE(std::get<AzureObjStorageConfig>(legacy->client).connection.max_connections);

    thrift.__set_max_conn(100);
    auto unsupported = ObjStorageConfigParser::from_storage_param(thrift);
    ASSERT_FALSE(unsupported.has_value());
    EXPECT_NE(unsupported.error().msg().find("AWS_MAX_CONNECTIONS"), std::string_view::npos);

    S3URI uri("s3://container/key");
    ASSERT_TRUE(uri.parse().ok());
    auto specified = ObjStorageConfigParser::from_properties(
            {{"provider", "azure"}, {"AWS_MAX_CONNECTIONS", "50"}}, uri);
    ASSERT_FALSE(specified.has_value());
    EXPECT_NE(specified.error().msg().find("AWS_MAX_CONNECTIONS"), std::string_view::npos);
}

TEST(ObjStorageConfigTest, RejectsConflictingNativeGcpCredentialsInEachProtocol) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    auto props =
            ObjStorageConfigParser::from_properties({{"provider", "gcp"},
                                                     {"gs.credential_provider_type", "DEFAULT"},
                                                     {"AWS_ACCESS_KEY", "ak"},
                                                     {"AWS_SECRET_KEY", "sk"}},
                                                    uri);
    ASSERT_FALSE(props.has_value());
    EXPECT_NE(props.error().msg().find("combined"), std::string_view::npos);

    TS3StorageParam thrift;
    thrift.__set_provider(TObjStorageType::GCP);
    thrift.__set_bucket("bucket");
    thrift.__set_role_arn("role");
    TCredential credential;
    credential.__set_gcp_credential(TGcpCredential {});
    thrift.__set_credential(credential);
    auto from_thrift = ObjStorageConfigParser::from_storage_param(thrift);
    ASSERT_FALSE(from_thrift.has_value());
    EXPECT_NE(from_thrift.error().msg().find("combined"), std::string_view::npos);

    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::GCP);
    vault.set_bucket("bucket");
    vault.set_cred_provider_type(cloud::CredProviderTypePB::ENV);
    vault.mutable_credential()->mutable_gcp_credential();
    auto pb = ObjStorageConfigParser::from_vault(vault);
    ASSERT_FALSE(pb.has_value());
    EXPECT_NE(pb.error().msg().find("combined"), std::string_view::npos);
}

TEST(ObjStorageConfigTest, RejectsMalformedAddressingFlags) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    for (const char* key : {"use_path_style", "AWS_NEED_OVERRIDE_ENDPOINT"}) {
        auto parsed = ObjStorageConfigParser::from_properties({{key, "invalid"}}, uri);
        ASSERT_FALSE(parsed.has_value());
        EXPECT_NE(parsed.error().msg().find(key), std::string_view::npos);
    }
}

TEST(ObjStorageConfigTest, ExplicitProviderWinsOverEndpoint) {
    S3URI uri("s3://bucket/key");
    ASSERT_TRUE(uri.parse().ok());
    for (const char* provider : {"AWS", "S3"}) {
        auto parsed = ObjStorageConfigParser::from_properties(
                {{"provider", provider}, {"AWS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com"}}, uri);
        ASSERT_TRUE(parsed.has_value());
        EXPECT_EQ(storage_provider(parsed->client), ObjStorageProvider::AWS);
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
        auto from_props = ObjStorageConfigParser::from_properties(props, uri);
        ASSERT_TRUE(from_props.has_value()) << from_props.error();
        TS3StorageParam param;
        param.__set_provider(entry.thrift);
        param.__set_bucket("bucket");
        param.__set_endpoint("endpoint");
        param.__set_region("region");
        param.__set_ak("ak");
        param.__set_sk("sk");
        param.__set_use_path_style(true);
        auto thrift = ObjStorageConfigParser::from_storage_param(param);
        ASSERT_TRUE(thrift.has_value()) << thrift.error();
        cloud::ObjectStoreInfoPB info;
        info.set_provider(entry.pb);
        info.set_bucket("bucket");
        info.set_endpoint("endpoint");
        info.set_region("region");
        info.set_ak("ak");
        info.set_sk("sk");
        info.set_use_path_style(true);
        info.set_sse_enabled(true);
        auto pb = ObjStorageConfigParser::from_vault(info);
        ASSERT_TRUE(pb.has_value()) << pb.error();
        EXPECT_TRUE(pb->is_internal_bucket);
        EXPECT_TRUE(pb->sse_enabled);
        EXPECT_FALSE(thrift->is_internal_bucket);
        EXPECT_FALSE(from_props->is_internal_bucket);
        for (const auto* resource : {&*from_props, &*thrift, &*pb}) {
            EXPECT_EQ(resource->bucket, "bucket");
            EXPECT_EQ(storage_provider(resource->client), entry.expected);
            EXPECT_FALSE(validate_obj_storage_config(resource->client));
            EXPECT_TRUE(obj_storage_connection(resource->client).endpoint.ends_with("://endpoint"));
        }
    }
}

} // namespace doris
