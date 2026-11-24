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

#include "gcp_obj_storage_client.h"

#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/ClientConfiguration.h>

#include "cpp/obj-storage/auth/aws/aws_credential_factory.h"
#include "cpp/obj-storage/auth/gcp/gcp_oauth_s3_client.h"
#include "cpp/obj-storage/auth/gcp/gcs_signed_url_provider.h"
#include "cpp/obj-storage/client/s3_util.h"
#include "cpp/obj-storage/parser/obj_storage_config.h"

namespace doris {

GcpObjStorageClient::GcpObjStorageClient(std::shared_ptr<Aws::S3::S3Client> sdk,
                                         ObjStorageEndpointInfo endpoint, Signer signer)
        : S3ObjStorageClient(std::move(sdk), std::move(endpoint), false),
          _signer(std::move(signer)) {}

ObjStorageClientBuildResult GcpObjStorageClient::create(const GcpObjStorageConfig& config) {
    auto options = make_s3_sdk_config(config.connection, config.region, config.retry_slow_down);
    ObjStorageEndpointInfo endpoint {.endpoint = config.connection.endpoint};
    if (auto* native = std::get_if<GcpNativeObjStorageAuth>(&config.auth)) {
        GcpCredentialConfig credential {
                .provider_type = native->source == GcpCredentialSource::ComputeEngine
                                         ? GcpCredentialProviderType::ComputeEngine
                                         : GcpCredentialProviderType::Default,
                .impersonation_service_account =
                        native->impersonation_service_account.value_or("")};
        auto ca_file = config.connection.ca_cert_file.value_or("");
        auto sdk = std::make_shared<GcpOAuthS3Client>(
                credential, ca_file, options,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, !config.use_path_style);
        auto signer = make_gcs_signed_url_generator(
                credential, endpoint.endpoint, config.signing_timeout.count(), std::move(ca_file));
        return {.client = std::make_shared<GcpObjStorageClient>(std::move(sdk), std::move(endpoint),
                                                                std::move(signer))};
    }

    AccessKeyOrAnonymousObjStorageAuth source = AnonymousObjStorageAuth {};
    if (auto* key = std::get_if<GcpHmacObjStorageAuth>(&config.auth)) {
        source = AccessKeyObjStorageAuth {key->access_id, key->secret_key, key->session_token};
    }
    // The default HMAC chain is distinct from both explicit keys and native OAuth.
    auto* chain = std::get_if<GcpDefaultHmacObjStorageAuth>(&config.auth);
    auto credentials =
            chain ? AwsCredentialFactory::create(
                            AwsObjStorageAuthConfig {.source = AwsCredentialChainType::Default,
                                                     .implementation = chain->implementation},
                            config.connection)
                  : AwsCredentialFactory::create(source);
    if (!credentials) {
        return {.error = std::move(credentials.error)};
    }
    auto sdk =
            create_s3_sdk_client(std::move(credentials.provider), options, config.use_path_style);
    return {.client = std::make_shared<GcpObjStorageClient>(std::move(sdk), std::move(endpoint))};
}

std::string GcpObjStorageClient::generate_presigned_url(const ObjStoragePath& path,
                                                        int64_t expiration_secs) {
    return _signer ? _signer(path, expiration_secs)
                   : S3ObjStorageClient::generate_presigned_url(path, expiration_secs);
}

} // namespace doris
