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

#include "s3_transport.h"

#include <aws/core/auth/signer/AWSAuthV4Signer.h>

#include "cpp/aws_common.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/sync_point.h"

namespace doris {

Aws::Client::ClientConfiguration s3_transport_options(const ObjStorageConnectionConfig& connection,
                                                      const std::string& region,
                                                      bool retry_slow_down, bool automatic_endpoint,
                                                      bool automatic_http) {
    auto options = default_aws_client_configuration();
    if (!automatic_endpoint) {
        options.endpointOverride = connection.endpoint;
    }
    options.region = region;
    const bool http =
            automatic_endpoint ? automatic_http : connection.endpoint.starts_with("http://");
    options.scheme = http ? Aws::Http::Scheme::HTTP : Aws::Http::Scheme::HTTPS;
    if (connection.ca_cert_file) {
        options.caFile = *connection.ca_cert_file;
    }
    if (connection.max_connections) {
        options.maxConnections = static_cast<unsigned>(*connection.max_connections);
    }
    if (connection.request_timeout) {
        options.requestTimeoutMs = connection.request_timeout->count();
    }
    if (connection.connect_timeout) {
        options.connectTimeoutMs = connection.connect_timeout->count();
    }
    if (connection.max_retries) {
        options.retryStrategy =
                std::make_shared<S3CustomRetryStrategy>(*connection.max_retries, retry_slow_down);
    }
    return options;
}

std::shared_ptr<Aws::S3::S3Client> make_s3_transport(
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials,
        const Aws::Client::ClientConfiguration& options, bool path_style) {
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageClient::credentials_provider", &credentials);
    return std::make_shared<Aws::S3::S3Client>(
            std::move(credentials), options,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, !path_style);
}

} // namespace doris
