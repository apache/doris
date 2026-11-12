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
#include "cpp/obj-storage/client/s3_sdk.h"
#include "cpp/obj-storage/obj_storage_config.h"
#include "cpp/sync_point.h"

namespace doris {

ObjStorageClientBuildResult GcpObjStorageClient::create(const GcpConfig& config) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE(
            "s3_client_factory::create",
            (ObjStorageClientBuildResult {
                    .client = std::make_shared<GcpObjStorageClient>(
                            std::make_shared<Aws::S3::S3Client>(), endpoint_info(config))}));
    if (const auto& gcp = config.credential) {
        // Both providers can read the ADC file. Initialize object I/O before signing.
        auto sdk = std::make_shared<GcpS3Client>(
                *gcp, config.s3.ca_cert_path, s3_sdk_config(config),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                config.use_virtual_addressing);
        ObjStorageSignedUrlGenerator signer;
        if (config.gcs_signer) {
            signer = config.gcs_signer(*gcp, config.endpoint, config.request_timeout_ms,
                                       config.s3.ca_cert_path);
        }
        return {.client = std::make_shared<GcpObjStorageClient>(
                        std::move(sdk), endpoint_info(config), std::move(signer))};
    }
    auto [sdk, error] = make_s3_sdk_client(config, AwsCredentialOptions::from_config(config));
    if (!sdk) {
        return {.error = std::move(error)};
    }
    return {.client = std::make_shared<GcpObjStorageClient>(std::move(sdk), endpoint_info(config))};
}

std::string GcpObjStorageClient::generate_presigned_url(const ObjStoragePath& opts,
                                                        int64_t expiration_secs) {
    if (_signer) {
        return _signer(opts, expiration_secs);
    }
    return S3CompatibleObjStorageClient::generate_presigned_url(opts, expiration_secs);
}

} // namespace doris
