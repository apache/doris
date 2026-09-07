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

#include "aws_common.h"

#include <aws/core/auth/GeneralHTTPCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <glog/logging.h>

namespace doris {

namespace {
const char CONTAINER_CREDENTIALS_PROVIDER_TAG[] = "ContainerCredentialsProvider";
} // namespace

CredProviderType cred_provider_type_from_pb(cloud::CredProviderTypePB cred_provider_type) {
    switch (cred_provider_type) {
    case cloud::CredProviderTypePB::DEFAULT:
        return CredProviderType::Default;
    case cloud::CredProviderTypePB::SIMPLE:
        return CredProviderType::Simple;
    case cloud::CredProviderTypePB::INSTANCE_PROFILE:
        return CredProviderType::InstanceProfile;
    case cloud::CredProviderTypePB::ENV:
        return CredProviderType::Env;
    case cloud::CredProviderTypePB::SYSTEM_PROPERTIES:
        return CredProviderType::SystemProperties;
    case cloud::CredProviderTypePB::WEB_IDENTITY:
        return CredProviderType::WebIdentity;
    case cloud::CredProviderTypePB::CONTAINER:
        return CredProviderType::Container;
    case cloud::CredProviderTypePB::ANONYMOUS:
        return CredProviderType::Anonymous;
    default:
        __builtin_unreachable();
        LOG(WARNING) << "Invalid CredProviderTypePB value: " << cred_provider_type
                     << ", use default instead.";
        return CredProviderType::Default;
    }
}

CredProviderType cred_provider_type_from_string(const std::string& type) {
    if (type.empty() || type == "DEFAULT") {
        return CredProviderType::Default;
    }
    if (type == "SIMPLE") {
        return CredProviderType::Simple;
    }
    if (type == "INSTANCE_PROFILE") {
        return CredProviderType::InstanceProfile;
    }
    if (type == "ENV") {
        return CredProviderType::Env;
    }
    if (type == "SYSTEM_PROPERTIES") {
        return CredProviderType::SystemProperties;
    }
    if (type == "WEB_IDENTITY") {
        return CredProviderType::WebIdentity;
    }
    if (type == "CONTAINER") {
        return CredProviderType::Container;
    }
    if (type == "ANONYMOUS") {
        return CredProviderType::Anonymous;
    }
    LOG(WARNING) << "Unknown credentials provider type: " << type << ", use default instead.";
    return CredProviderType::Default;
}

bool container_credentials_available() {
    return !Aws::Environment::GetEnv(AWS_CONTAINER_CREDENTIALS_RELATIVE_URI).empty() ||
           !Aws::Environment::GetEnv(AWS_CONTAINER_CREDENTIALS_FULL_URI).empty();
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> create_container_credentials_provider() {
    const auto relative_uri = Aws::Environment::GetEnv(AWS_CONTAINER_CREDENTIALS_RELATIVE_URI);
    const auto absolute_uri = Aws::Environment::GetEnv(AWS_CONTAINER_CREDENTIALS_FULL_URI);
    const auto token = Aws::Environment::GetEnv(AWS_CONTAINER_AUTHORIZATION_TOKEN);
    const auto token_path = Aws::Environment::GetEnv(AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE);

    // Both URIs are forwarded and the provider decides between them: a non-empty relative URI wins
    // and is resolved against the ECS agent's address, otherwise the full URI is used as-is. This
    // is the same precedence the AWS SDK's own default chain applies.
    //
    // Both token forms are forwarded for the same reason. The endpoint authenticates every fetch
    // with a bearer token, which the provider takes either inline or as a file path, and given a
    // path it re-reads the file before each fetch. ECS sets only the inline variable, EKS Pod
    // Identity sets only the file one - so forwarding the path is what makes the Authorization header
    // non-empty under Pod Identity, and what keeps it valid once the kubelet rotates the file.
    //
    // NOTE: The header file names its third parameter authTokenFilePath and its fourth authToken,
    // but the implementation binds them the other way round. The header is the side that is wrong,
    // not the definition. This is reported as aws/aws-sdk-cpp#3143, fixed by
    // aws/aws-sdk-cpp#3162.
    auto provider = Aws::MakeShared<Aws::Auth::GeneralHTTPCredentialsProvider>(
            CONTAINER_CREDENTIALS_PROVIDER_TAG, relative_uri, absolute_uri, token, token_path);

    const bool uses_relative_uri = !relative_uri.empty();
    const char* const uri_var = uses_relative_uri ? AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
                                                  : AWS_CONTAINER_CREDENTIALS_FULL_URI;
    const auto& uri = uses_relative_uri ? relative_uri : absolute_uri;

    if (relative_uri.empty() && absolute_uri.empty()) {
        LOG(WARNING) << "Container credentials provider has no endpoint to call and will return no "
                        "credentials: neither "
                     << AWS_CONTAINER_CREDENTIALS_RELATIVE_URI << " nor "
                     << AWS_CONTAINER_CREDENTIALS_FULL_URI << " is set.";
    } else {
        LOG(INFO)
                << "Created container credentials provider from " << uri_var << ": [" << uri
                << "] with a" << (token.empty() ? "n empty" : " non-empty")
                << " inline authorization token and a"
                << (token_path.empty() ? "n empty" : " non-empty")
                << " authorization token file path: [" << token_path
                << "]. If credentials come back empty, raise aws_log_level to 3 or higher for the "
                   "SDK's own reason.";
    }
    return provider;
}

std::string get_valid_ca_cert_path(const std::vector<std::string>& ca_cert_file_paths) {
    for (const auto& path : ca_cert_file_paths) {
        if (std::filesystem::exists(path)) {
            return path;
        }
    }
    return "";
}

void set_s3_client_default_http_scheme(Aws::Client::ClientConfiguration& client_config,
                                       const std::string& scheme) {
    if (client_config.endpointOverride.starts_with("http://") ||
        client_config.endpointOverride.starts_with("https://")) {
        return;
    }
    client_config.scheme = scheme == "http" ? Aws::Http::Scheme::HTTP : Aws::Http::Scheme::HTTPS;
}
} // namespace doris
