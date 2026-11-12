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

#include "cpp/obj-storage/auth/gcp/gcp_s3_client.h"
#include "cpp/obj-storage/auth/gcp/gcs_signed_url_provider.h"
#include "cpp/obj-storage/client/s3_sdk.h"
#include "cpp/obj-storage/obj_storage_config.h"
#include "cpp/sync_point.h"

namespace doris {

ObjStorageClientBuildResult GcpObjStorageClient::create(const GcpObjStorageConfig& config) {
    ObjStorageEndpointInfo endpoint {.endpoint = config.connection.endpoint};
    TEST_SYNC_POINT_RETURN_WITH_VALUE(
            "s3_client_factory::create",
            (ObjStorageClientBuildResult {
                    .client = std::make_shared<GcpObjStorageClient>(
                            std::make_shared<Aws::S3::S3Client>(), endpoint)}));
    auto sdk_config = s3_sdk_config(config.connection, config.region, config.retry_slow_down);
    if (const auto* native = std::get_if<GcpNativeObjStorageAuth>(&config.auth)) {
        GcpCredentialConfig credential {
                .provider_type = native->source == GcpCredentialSource::ComputeEngine
                                         ? GcpCredentialProviderType::ComputeEngine
                                         : GcpCredentialProviderType::Default,
                .impersonation_service_account =
                        native->impersonation_service_account.value_or("")};
        auto ca_file = config.connection.ca_cert_file.value_or("");
        auto sdk = std::make_shared<GcpS3Client>(
                credential, ca_file, sdk_config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, !config.use_path_style);
        auto signer = make_gcs_signed_url_generator(
                credential, config.connection.endpoint,
                config.connection.request_timeout.value_or(std::chrono::seconds(10)).count(),
                std::move(ca_file));
        return {.client = std::make_shared<GcpObjStorageClient>(std::move(sdk), std::move(endpoint),
                                                                std::move(signer))};
    }
    AccessKeyOrAnonymousObjStorageAuth auth;
    if (const auto* hmac = std::get_if<GcpHmacObjStorageAuth>(&config.auth)) {
        auth = AccessKeyObjStorageAuth {hmac->access_id, hmac->secret_key, hmac->session_token};
    } else if (const auto* chain = std::get_if<ObjStorageCredentialChain>(&config.auth)) {
        auth = *chain;
    }
    auto [sdk, error] =
            make_s3_sdk_client(sdk_config, config.use_path_style,
                               AwsCredentialOptions::from_auth(auth, config.connection));
    if (!sdk) {
        return {.error = std::move(error)};
    }
    return {.client = std::make_shared<GcpObjStorageClient>(std::move(sdk), endpoint)};
}

std::string GcpObjStorageClient::generate_presigned_url(const ObjStoragePath& opts,
                                                        int64_t expiration_secs) {
    if (_signer) {
        return _signer(opts, expiration_secs);
    }
    return S3CompatibleObjStorageClient::generate_presigned_url(opts, expiration_secs);
}

} // namespace doris
