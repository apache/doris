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

#include "cpp/custom_aws_credentials_provider_chain.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

class S3ClientFactoryTest : public testing::Test {
    FRIEND_TEST(S3ClientFactoryTest, S3ClientFactory);
};

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
        auto web_identity_v2 = std::dynamic_pointer_cast<
                Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(provider_v2);
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

} // namespace doris
