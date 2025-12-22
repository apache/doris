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
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <gtest/gtest.h>

#include "cpp/custom_aws_credentials_provider_chain.h"
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

} // namespace doris