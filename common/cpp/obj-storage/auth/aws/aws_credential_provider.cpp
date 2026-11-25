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

#include "cpp/obj-storage/auth/aws/aws_credential_provider.h"

#include <aws/core/auth/GeneralHTTPCredentialsProvider.h>
#include <aws/core/platform/Environment.h>
#include <glog/logging.h>

namespace doris {

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
    using Provider = Aws::Auth::GeneralHTTPCredentialsProvider;
    using Aws::Environment::GetEnv;
    return !GetEnv(Provider::AWS_CONTAINER_CREDENTIALS_RELATIVE_URI).empty() ||
           !GetEnv(Provider::AWS_CONTAINER_CREDENTIALS_FULL_URI).empty();
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> create_container_credentials_provider() {
    using Provider = Aws::Auth::GeneralHTTPCredentialsProvider;
    using Aws::Environment::GetEnv;
    // SDK 1.11.221's header swaps the token and token-file parameter names; follow the implementation.
    return std::make_shared<Provider>(GetEnv(Provider::AWS_CONTAINER_CREDENTIALS_RELATIVE_URI),
                                      GetEnv(Provider::AWS_CONTAINER_CREDENTIALS_FULL_URI),
                                      GetEnv(Provider::AWS_CONTAINER_AUTHORIZATION_TOKEN),
                                      GetEnv(Provider::AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE));
}

} // namespace doris
