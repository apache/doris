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

#include <gen_cpp/cloud.pb.h>

#include <filesystem>
#include <memory>

namespace Aws::Auth {
class AWSCredentialsProvider;
}

namespace Aws::Client {
struct ClientConfiguration;
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

// The environment variables through which a container runtime advertises its credentials
// endpoint. ECS exports a relative URI, which is a path resolved against the ECS agent's fixed
// link-local address, and optionally an inline authorization token. EKS Pod Identity exports a
// full URI, which is a complete URL, and an authorization token file that the kubelet rotates in
// place. A runtime never sets both URIs, so code that reads only one of them silently gets
// nothing on the other platform.
inline constexpr char AWS_CONTAINER_CREDENTIALS_RELATIVE_URI[] =
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
inline constexpr char AWS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
inline constexpr char AWS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
inline constexpr char AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE[] =
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE";

CredProviderType cred_provider_type_from_pb(cloud::CredProviderTypePB cred_provider_type);

CredProviderType cred_provider_type_from_string(const std::string& type);

// Builds the credentials provider that talks to a container runtime's credentials endpoint, from
// the four AWS_CONTAINER_* environment variables above. Every caller that asks for container credentials
// - the default provider chain, and the explicit CONTAINER/ECS provider modes of the BE S3 client
// factory, the cloud recycler and the Kafka MSK IAM signer - has to support both ECS and EKS Pod
// Identity, so none of them can afford to read a subset of the variables.
std::shared_ptr<Aws::Auth::AWSCredentialsProvider> create_container_credentials_provider();

// True when the runtime advertises a container credentials endpoint through either URI variable.
bool container_credentials_available();

std::string get_valid_ca_cert_path(const std::vector<std::string>& ca_cert_file_paths);

// Configures the default S3 client transport scheme for endpoints without an explicit scheme.
void set_s3_client_default_http_scheme(Aws::Client::ClientConfiguration& client_config,
                                       const std::string& scheme);

} // namespace doris