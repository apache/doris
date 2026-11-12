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

#include "cpp/aws_common.h"
#include "cpp/obj-storage/obj_storage_config.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/sync_point.h"

namespace doris {

Aws::Client::ClientConfiguration s3_sdk_config(const ObjStorageConnectionConfig& connection,
                                               const std::string& region, bool retry_slow_down,
                                               bool use_sdk_endpoint, bool sdk_endpoint_http) {
    auto sdk = default_aws_client_configuration();
    if (!use_sdk_endpoint) {
        sdk.endpointOverride = connection.endpoint;
    }
    sdk.scheme =
            sdk_endpoint_http || (!use_sdk_endpoint && connection.endpoint.starts_with("http://"))
                    ? Aws::Http::Scheme::HTTP
                    : Aws::Http::Scheme::HTTPS;
    sdk.region = region;
    if (connection.ca_cert_file) {
        sdk.caFile = *connection.ca_cert_file;
    }
    if (connection.max_connections) {
        sdk.maxConnections = static_cast<unsigned>(*connection.max_connections);
    }
    if (connection.request_timeout) {
        sdk.requestTimeoutMs = connection.request_timeout->count();
    }
    if (connection.connect_timeout) {
        sdk.connectTimeoutMs = connection.connect_timeout->count();
    }
    if (connection.max_retries) {
        sdk.retryStrategy =
                std::make_shared<S3CustomRetryStrategy>(*connection.max_retries, retry_slow_down);
    }
    return sdk;
}

std::pair<std::shared_ptr<Aws::S3::S3Client>, std::string> make_s3_sdk_client(
        const Aws::Client::ClientConfiguration& sdk, bool use_path_style,
        const AwsCredentialOptions& credential_options) {
    auto credentials = AwsCredentialFactory::create(credential_options);
    if (!credentials) {
        return {nullptr, std::move(credentials.error)};
    }
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageClient::credentials_provider", &credentials.provider);
    return {std::make_shared<Aws::S3::S3Client>(
                    std::move(credentials.provider), sdk,
                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, !use_path_style),
            {}};
}

} // namespace doris
