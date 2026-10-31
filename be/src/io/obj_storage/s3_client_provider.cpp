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

#include "io/obj_storage/s3_client_provider.h"

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>

#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/aws_common.h"
#include "cpp/obj-client/auth/gcp/gcp_token_provider.h"
#include "cpp/obj-client/auth/obj_s3_client_factory.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/sync_point.h"
#include "io/fs/gcs_signed_url_provider.h"
#include "io/obj_storage/obj_client_rate_limit.h"
#include "util/s3_util.h"

namespace doris::io {

Result<std::shared_ptr<ObjStorageClient>> S3ClientProvider::get_or_create(
        const ObjClientConfig& config, const S3ClientConfig& s3) {
    if (auto cached = _cache.find(config)) {
        return cached;
    }

    auto client_result = _build(config, s3);
    if (!client_result.has_value()) {
        return ResultError(std::move(client_result).error());
    }
    auto obj_client = apply_rate_limit_policy(std::move(client_result).value(), config.usage);
    return _cache.publish(config, obj_client);
}

Result<std::shared_ptr<ObjStorageClient>> S3ClientProvider::_build(
        const ObjClientConfig& client_config, const S3ClientConfig& s3) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE(
            "s3_client_factory::create",
            std::make_shared<S3ObjStorageClient>(std::make_shared<Aws::S3::S3Client>(),
                                                 ObjStorageEndpointInfo {}));
    Aws::Client::ClientConfiguration aws_config = S3ClientFactory::getClientConfiguration();
    if (s3.need_override_endpoint) {
        aws_config.endpointOverride = s3.endpoint;
    }
    aws_config.region = s3.region;

    auto ca_cert_file_path = S3ClientFactory::instance().ca_cert_file_path();
    if (!ca_cert_file_path.empty()) {
        aws_config.caFile = ca_cert_file_path;
    }

    const auto& connection = client_config.connection;
    if (connection.max_connections > 0) {
        aws_config.maxConnections = connection.max_connections;
    } else {
        aws_config.maxConnections = 102400;
    }

    aws_config.requestTimeoutMs = 30000;
    if (connection.request_timeout_ms > 0) {
        aws_config.requestTimeoutMs = connection.request_timeout_ms;
    }

    if (connection.connect_timeout_ms > 0) {
        aws_config.connectTimeoutMs = connection.connect_timeout_ms;
    }

    set_s3_client_default_http_scheme(aws_config, config::s3_client_http_scheme);

    aws_config.retryStrategy = std::make_shared<S3CustomRetryStrategy>(
            config::max_s3_client_retry /*scaleFactor = 25*/, /*retry_slow_down=*/true);

    auto credentials = S3ClientFactory::instance().create_aws_credentials_provider(s3.auth);
    if (!credentials) {
        return ResultError(Status::InvalidArgument("failed to create AWS credential provider: {}",
                                                   credentials.error));
    }
    auto new_client = make_s3_client(
            s3.auth.credential,
            S3ClientBuildContext {
                    .fallback_provider = std::move(credentials.provider),
                    .config = std::move(aws_config),
                    .payload_signing_policy =
                            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                    .use_virtual_addressing = s3.use_virtual_addressing,
                    .ca_cert_path = ca_cert_file_path,
            });

    S3ObjStorageClient::SignedUrlGenerator signed_url_generator;
    if (const auto* gcp_credential = std::get_if<GcpCredentialConfig>(&s3.auth.credential)) {
        // signBlob is authorized by the source identity, not by the optional
        // impersonated target identity. Keep one refreshable cloud-platform
        // token provider for the lifetime of the S3 client.
        auto signing_caller_credential = *gcp_credential;
        signing_caller_credential.impersonation_service_account.clear();
        auto token_provider = std::make_shared<GcpTokenProvider>(
                signing_caller_credential, ca_cert_file_path, GcpTokenScope::CloudPlatform);
        signed_url_generator = [endpoint = s3.endpoint,
                                request_timeout_ms = client_config.connection.request_timeout_ms,
                                credential = *gcp_credential, ca_cert_file_path,
                                token_provider = std::move(token_provider)](
                                       const ObjStoragePath& path, int64_t expiration_secs) {
            std::string signed_url;
            auto status = generate_gcs_v4_signed_url({.endpoint = endpoint,
                                                      .bucket = path.bucket,
                                                      .key = path.key,
                                                      .expiration_secs = expiration_secs,
                                                      .request_timeout_ms = request_timeout_ms,
                                                      .ca_cert_file_path = ca_cert_file_path},
                                                     credential, token_provider, &signed_url);
            if (!status.ok()) {
                LOG(WARNING) << "failed to generate GCS V4 signed URL: " << status;
                return std::string {};
            }
            return signed_url;
        };
    }
    auto provider_client = std::make_shared<S3ObjStorageClient>(
            std::move(new_client),
            ObjStorageEndpointInfo {
                    .endpoint = s3.endpoint, .ak = s3.auth.access_key, .sk = s3.auth.secret_key},
            std::move(signed_url_generator));
    LOG_INFO("create one s3 client with {}", client_config.to_string());
    return provider_client;
}

} // namespace doris::io
