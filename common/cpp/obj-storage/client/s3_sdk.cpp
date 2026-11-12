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

#include "s3_sdk.h"

#include <aws/s3/S3Client.h>

#include <algorithm>

#include "cpp/aws_common.h"
#include "cpp/obj-storage/obj_storage_config.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/sync_point.h"

namespace doris {

Aws::Client::ClientConfiguration s3_sdk_config(const S3CompatibleConfig& config) {
    auto sdk = default_aws_client_configuration();
    if (config.need_override_endpoint) {
        sdk.endpointOverride = config.endpoint;
    }
    sdk.region = config.region;
    if (!config.s3.ca_cert_path.empty()) {
        sdk.caFile = config.s3.ca_cert_path;
    }
    if (config.max_connections > 0) {
        sdk.maxConnections = static_cast<unsigned int>(config.max_connections);
    } else if (config.s3.min_connections) {
        sdk.maxConnections = static_cast<unsigned int>(
                std::max<int64_t>(sdk.maxConnections, *config.s3.min_connections));
    } else {
        constexpr unsigned int default_max_connections = 102400;
        sdk.maxConnections = default_max_connections;
    }
    sdk.requestTimeoutMs = config.request_timeout_ms > 0 ? config.request_timeout_ms : 30000;
    if (config.connect_timeout_ms > 0) {
        sdk.connectTimeoutMs = config.connect_timeout_ms;
    }
    set_s3_client_default_http_scheme(sdk, config.s3.http_scheme);
    sdk.retryStrategy = std::make_shared<S3CustomRetryStrategy>(config.s3.max_retries,
                                                                config.s3.retry_slow_down);
    return sdk;
}

ObjStorageEndpointInfo endpoint_info(const ObjStorageConfigBase& config) {
    return {.endpoint = config.endpoint, .ak = config.ak, .sk = config.sk};
}

std::pair<std::shared_ptr<Aws::S3::S3Client>, std::string> make_s3_sdk_client(
        const S3CompatibleConfig& config, const AwsCredentialOptions& credential_options) {
    auto credentials = AwsCredentialFactory::create(credential_options);
    if (!credentials) {
        return {nullptr, std::move(credentials.error)};
    }
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageClient::credentials_provider", &credentials.provider);
    return {std::make_shared<Aws::S3::S3Client>(
                    std::move(credentials.provider), s3_sdk_config(config),
                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                    config.use_virtual_addressing),
            {}};
}

} // namespace doris
