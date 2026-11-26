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

#pragma once

#include <memory>
#include <string>

namespace Aws::Auth {
class AWSCredentialsProvider;
}

namespace doris {

//AWS Credentials Provider Type
enum class CredProviderType {
    Default = 0,
    Simple = 1,
    InstanceProfile = 2,
    Env = 3,
    SystemProperties = 4,
    WebIdentity = 5,
    Container = 6,
    Anonymous = 7
};

CredProviderType cred_provider_type_from_string(const std::string& type);

// Builds the ECS / EKS provider from AWS_CONTAINER_* environment variables.
// The SDK handles URI precedence and re-reads the authorization token file on credential refresh.
std::shared_ptr<Aws::Auth::AWSCredentialsProvider> create_container_credentials_provider();

// True when the runtime advertises a container credentials endpoint through either URI variable.
bool container_credentials_available();

enum class EmptyCredentialsBehavior { ANONYMOUS, DEFAULT_CHAIN };

} // namespace doris
