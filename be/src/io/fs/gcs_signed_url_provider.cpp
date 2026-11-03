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

#include "io/fs/gcs_signed_url_provider.h"

#include <fmt/format.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <string_view>
#include <utility>

#include "cpp/obj-client/auth/gcp/gcp_token_provider.h"
#include "cpp/obj-client/auth/gcp/gcs_signed_url.h"
#include "service/http/http_client.h"
#include "util/string_util.h"
#include "util/url_coding.h"

namespace doris::io {
namespace {

constexpr std::string_view IAM_CREDENTIALS_ENDPOINT =
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";
constexpr std::string_view DEFAULT_METADATA_HOST = "metadata.google.internal";
constexpr std::string_view METADATA_SERVICE_ACCOUNT_EMAIL_PATH =
        "/computeMetadata/v1/instance/service-accounts/default/email";
constexpr int64_t METADATA_REQUEST_TIMEOUT_MS = 1000;

std::string iam_error_message(const rapidjson::Document& document) {
    if (!document.IsObject() || !document.HasMember("error") || !document["error"].IsObject()) {
        return "unparseable error response";
    }
    const auto& error = document["error"];
    std::string message;
    if (error.HasMember("status") && error["status"].IsString()) {
        message = error["status"].GetString();
    }
    if (error.HasMember("message") && error["message"].IsString()) {
        if (!message.empty()) {
            message.append(": ");
        }
        message.append(error["message"].GetString());
    }
    if (message.empty()) {
        return "error response did not contain status or message";
    }
    constexpr size_t MAX_ERROR_MESSAGE_SIZE = 1024;
    if (message.size() > MAX_ERROR_MESSAGE_SIZE) {
        message.resize(MAX_ERROR_MESSAGE_SIZE);
        message.append("...");
    }
    return message;
}

bool is_unreserved(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' ||
           c == '_' || c == '.' || c == '~';
}

std::string percent_encode(std::string_view value) {
    constexpr char HEX[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(value.size());
    for (unsigned char c : value) {
        if (is_unreserved(c)) {
            encoded.push_back(static_cast<char>(c));
            continue;
        }
        encoded.push_back('%');
        encoded.push_back(HEX[c >> 4]);
        encoded.push_back(HEX[c & 0x0F]);
    }
    return encoded;
}

Status call_iam_sign_blob(std::string_view access_token, std::string_view service_account,
                          std::string_view string_to_sign, int64_t request_timeout_ms,
                          std::string* signature) {
    std::string encoded_payload;
    base64_encode(std::string(string_to_sign), &encoded_payload);
    std::string request_body = fmt::format(R"({{"payload":"{}"}})", encoded_payload);
    std::string endpoint =
            std::string(IAM_CREDENTIALS_ENDPOINT) + percent_encode(service_account) + ":signBlob";

    HttpClient client;
    // Keep the response body for non-2xx replies so IAM permission and
    // service-account errors are actionable to operators.
    RETURN_IF_ERROR(client.init(endpoint, false));
    client.set_authorization("Bearer " + std::string(access_token));
    client.set_content_type("application/json");
    client.set_timeout_ms(request_timeout_ms > 0 ? request_timeout_ms : 10000);

    std::string response;
    RETURN_IF_ERROR(client.execute_post_request(request_body, &response));

    rapidjson::Document document;
    document.Parse(response.data(), response.size());
    const auto http_status = client.get_http_status();
    if (http_status < 200 || http_status >= 300) {
        return Status::HttpError("IAM signBlob failed with HTTP {}: {}", http_status,
                                 iam_error_message(document));
    }
    if (document.HasParseError() || !document.IsObject() || !document.HasMember("signedBlob") ||
        !document["signedBlob"].IsString()) {
        return Status::InternalError("IAM signBlob returned an invalid response");
    }
    if (!base64_decode(document["signedBlob"].GetString(), signature)) {
        return Status::InternalError("IAM signBlob returned an invalid base64 signature");
    }
    return Status::OK();
}

Status fetch_metadata_service_account_email(int64_t request_timeout_ms, std::string* email) {
    const char* configured_host = std::getenv("GCE_METADATA_HOST");
    std::string_view metadata_host = configured_host != nullptr && configured_host[0] != '\0'
                                             ? configured_host
                                             : DEFAULT_METADATA_HOST;
    HttpClient client;
    RETURN_IF_ERROR(client.init(
            fmt::format("http://{}{}", metadata_host, METADATA_SERVICE_ACCOUNT_EMAIL_PATH)));
    client.set_header("Metadata-Flavor", "Google");
    client.set_timeout_ms(request_timeout_ms > 0
                                  ? std::min(request_timeout_ms, METADATA_REQUEST_TIMEOUT_MS)
                                  : METADATA_REQUEST_TIMEOUT_MS);

    std::string response;
    RETURN_IF_ERROR(client.execute(&response));
    auto resolved_email = trim(response);
    if (!is_valid_gcp_service_account_email(resolved_email)) {
        return Status::InternalError(
                "GCP metadata server returned an invalid service account email");
    }
    email->assign(resolved_email);
    return Status::OK();
}

} // namespace

Status generate_gcs_v4_signed_url(const GcsV4SignedUrlProviderOptions& options,
                                  const GcpCredentialConfig& credential,
                                  const std::shared_ptr<GcpTokenProvider>& token_provider,
                                  std::string* signed_url) {
    if (signed_url == nullptr) {
        return Status::InvalidArgument("signed_url output must not be null");
    }
    signed_url->clear();

    std::string signer_email;
    if (!credential.impersonation_service_account.empty()) {
        signer_email = credential.impersonation_service_account;
    } else if (credential.provider_type == GcpCredentialProviderType::ComputeEngine) {
        RETURN_IF_ERROR(
                fetch_metadata_service_account_email(options.request_timeout_ms, &signer_email));
    } else {
        return Status::InvalidArgument(
                "GCS V4 signing with DEFAULT credentials requires "
                "gs.impersonation_service_account; use COMPUTE_ENGINE to resolve the VM service "
                "account from metadata");
    }
    if (token_provider == nullptr) {
        return Status::InternalError("GCS V4 signing token provider is not initialized");
    }

    try {
        auto token = token_provider->get_token();
        if (!token.has_value()) {
            return Status::InternalError("failed to obtain OAuth token for IAM signBlob");
        }

        auto result = build_gcs_v4_signed_url(
                {.endpoint = options.endpoint,
                 .bucket = options.bucket,
                 .key = options.key,
                 .signer_email = signer_email,
                 .expiration_secs = options.expiration_secs},
                std::chrono::system_clock::now(), [&](std::string_view string_to_sign) {
                    std::string signature;
                    auto status = call_iam_sign_blob(*token, signer_email, string_to_sign,
                                                     options.request_timeout_ms, &signature);
                    if (!status.ok()) {
                        return GcsSignBlobResult {.error = status.to_string()};
                    }
                    return GcsSignBlobResult {.signature = std::move(signature)};
                });
        if (!result.ok()) {
            return Status::InternalError("failed to build GCS V4 signed URL: {}", result.error);
        }
        *signed_url = std::move(result.signed_url);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("failed to initialize GCS URL signer: {}", e.what());
    }
}

} // namespace doris::io
