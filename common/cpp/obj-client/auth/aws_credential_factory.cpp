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

#include "aws_credential_factory.h"

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/sts/STSClient.h>

#include "cpp/custom_aws_credentials_provider_chain.h"

namespace doris {
namespace {

using Provider = Aws::Auth::AWSCredentialsProvider;

std::shared_ptr<Provider> create_v2_base_provider(CredProviderType type) {
    switch (type) {
    case CredProviderType::Env:
        return std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();
    case CredProviderType::SystemProperties:
        return std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>();
    case CredProviderType::WebIdentity:
        return std::make_shared<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>();
    case CredProviderType::Container:
        return create_container_credentials_provider();
    case CredProviderType::Anonymous:
        return std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>();
    case CredProviderType::Default:
    case CredProviderType::Simple:
        return std::make_shared<CustomAwsCredentialsProviderChain>();
    case CredProviderType::InstanceProfile:
        return std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
    }
    __builtin_unreachable();
}

AwsCredentialResult assume_role(const AwsCredentialOptions& options,
                                std::shared_ptr<Provider> base_provider) {
    auto sts_client =
            std::make_shared<Aws::STS::STSClient>(base_provider, options.sts_client_config);
    return {
            .provider = std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
                    options.role_arn, Aws::String(), options.external_id,
                    Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, std::move(sts_client)),
    };
}

} // namespace

AwsCredentialResult AwsCredentialFactory::create(const AwsCredentialOptions& options) {
    const bool has_access_key = !options.access_key.empty();
    const bool has_secret_key = !options.secret_key.empty();

    if (has_access_key && has_secret_key) {
        Aws::Auth::AWSCredentials credentials(options.access_key, options.secret_key);
        if (!options.session_token.empty()) {
            credentials.SetSessionToken(options.session_token);
        }
        return {
                .provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
                        std::move(credentials)),
        };
    }

    if (options.version == AwsCredentialProviderVersion::V1) {
        if (options.provider_type == CredProviderType::InstanceProfile) {
            auto base = std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
            return options.role_arn.empty() ? AwsCredentialResult {.provider = std::move(base)}
                                            : assume_role(options, std::move(base));
        }
        if (!has_access_key && !has_secret_key &&
            options.empty_credentials == EmptyCredentialsBehavior::ANONYMOUS) {
            return {
                    .provider = std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(),
            };
        }
        return {
                .provider = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>(),
        };
    }

    auto base = create_v2_base_provider(options.provider_type);
    return options.role_arn.empty() ? AwsCredentialResult {.provider = std::move(base)}
                                    : assume_role(options, std::move(base));
}

} // namespace doris
