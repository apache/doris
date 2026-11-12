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

#include "cpp/obj-storage/auth/gcp/gcp_s3_client.h"
#include "cpp/obj-storage/auth/gcp/gcs_signed_url_provider.h"
#include "s3_transport.h"

namespace doris {

ObjStorageClientBuildResult GcpObjStorageClient::create(const GcpObjStorageConfig& config) {
    auto options = s3_transport_options(config.connection, config.region, config.retry_slow_down);
    ObjStorageEndpointInfo endpoint {.endpoint = config.connection.endpoint};
    if (auto* native = std::get_if<GcpNativeObjStorageAuth>(&config.auth)) {
        GcpCredentialConfig credential {
                .provider_type = native->source == GcpCredentialSource::ComputeEngine
                                         ? GcpCredentialProviderType::ComputeEngine
                                         : GcpCredentialProviderType::Default,
                .impersonation_service_account =
                        native->impersonation_service_account.value_or("")};
        auto ca_file = config.connection.ca_cert_file.value_or("");
        auto sdk = std::make_shared<GcpS3Client>(
                credential, ca_file, options,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, !config.use_path_style);
        auto signer = make_gcs_signed_url_generator(
                credential, endpoint.endpoint,
                config.connection.request_timeout.value_or(std::chrono::seconds(10)).count(),
                std::move(ca_file));
        return {.client = std::make_shared<GcpObjStorageClient>(std::move(sdk), std::move(endpoint),
                                                                std::move(signer))};
    }

    S3CompatibleObjStorageAuth source = AnonymousObjStorageAuth {};
    if (auto* key = std::get_if<GcpHmacObjStorageAuth>(&config.auth)) {
        source = AccessKeyObjStorageAuth {key->access_id, key->secret_key, key->session_token};
    } else if (auto* chain = std::get_if<S3CredentialChain>(&config.auth)) {
        source = *chain;
    }
    auto credentials = AwsCredentialFactory::create(source, config.connection);
    if (!credentials) {
        return {.error = std::move(credentials.error)};
    }
    auto sdk = make_s3_transport(std::move(credentials.provider), options, config.use_path_style);
    return {.client = std::make_shared<GcpObjStorageClient>(std::move(sdk), std::move(endpoint))};
}

std::string GcpObjStorageClient::generate_presigned_url(const ObjStoragePath& path,
                                                        int64_t expiration_secs) {
    return _signer ? _signer(path, expiration_secs)
                   : S3CompatibleObjStorageClient::generate_presigned_url(path, expiration_secs);
}
} // namespace doris
