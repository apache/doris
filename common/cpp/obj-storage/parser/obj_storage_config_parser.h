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

#include <map>
#include <string>
#include <variant>

#include "cpp/obj-storage/auth/aws/aws_credential_provider.h"
#include "cpp/obj-storage/parser/obj_storage_config.h"

namespace doris {

class TS3StorageParam;
class TAzureClientConfig;

using ObjStorageProperties = std::map<std::string, std::string>;

template <class T>
using ObjStorageConfigParseResult = std::variant<T, std::string>;

AwsCredentialImplementation aws_credential_implementation(std::string_view version);
void normalize_obj_storage_endpoint(ObjStorageClientConfig& client, std::string_view scheme);
cloud::ObjectStoreInfoPB::Provider storage_provider_from_endpoint(std::string_view endpoint);
cloud::ObjectStoreInfoPB::Provider vault_storage_provider(const cloud::ObjectStoreInfoPB& info);

// Provider parsers consume the existing, normalized property keys. Key presence
// distinguishes omitted values from explicit empty values where required.
ObjStorageConfigParseResult<AwsObjStorageConfig> parse_aws_obj_storage_config(
        const ObjStorageProperties& properties, AwsCredentialImplementation implementation);
ObjStorageConfigParseResult<OssObjStorageConfig> parse_oss_obj_storage_config(
        const ObjStorageProperties& properties);
ObjStorageConfigParseResult<CosObjStorageConfig> parse_cos_obj_storage_config(
        const ObjStorageProperties& properties);
ObjStorageConfigParseResult<BosObjStorageConfig> parse_bos_obj_storage_config(
        const ObjStorageProperties& properties);
ObjStorageConfigParseResult<ObsObjStorageConfig> parse_obs_obj_storage_config(
        const ObjStorageProperties& properties);
ObjStorageConfigParseResult<TosObjStorageConfig> parse_tos_obj_storage_config(
        const ObjStorageProperties& properties);
ObjStorageConfigParseResult<GcpObjStorageConfig> parse_gcp_obj_storage_config(
        const ObjStorageProperties& properties,
        AwsCredentialImplementation implementation = AwsCredentialImplementation::V2);
ObjStorageConfigParseResult<AzureObjStorageConfig> parse_azure_obj_storage_config(
        const ObjStorageProperties& properties);

// Converts supported wire formats and dispatches to the provider parsers above.
// Credential implementation and empty-credential defaults apply to AWS and legacy GCS HMAC.
// Hosts apply their connection settings after parsing succeeds.
class ObjStorageConfigParser {
public:
    static ObjStorageConfigParseResult<ObjStorageResourceConfig> from_properties(
            const ObjStorageProperties& properties, std::string bucket,
            AwsCredentialImplementation implementation,
            EmptyCredentialsBehavior aws_empty_credentials);
    static ObjStorageConfigParseResult<ObjStorageResourceConfig> from_storage_param(
            const TS3StorageParam& param, AwsCredentialImplementation implementation,
            EmptyCredentialsBehavior aws_empty_credentials);

    // Standalone Azure parameter conversion. The caller supplies an endpoint and container;
    // location resolution and host-specific defaults remain outside the shared parser.
    static ObjStorageConfigParseResult<ObjStorageResourceConfig> from_azure_client_config(
            const TAzureClientConfig& config);

    // Credentials must already be decrypted by the host's metadata boundary.
    static ObjStorageConfigParseResult<ObjStorageResourceConfig> from_vault(
            const cloud::ObjectStoreInfoPB& info, AwsCredentialImplementation implementation,
            EmptyCredentialsBehavior aws_empty_credentials);
};

} // namespace doris
