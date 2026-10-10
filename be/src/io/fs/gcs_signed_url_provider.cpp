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

#include <exception>
#include <string_view>
#include <utility>

#include "cpp/obj-client/auth/gcp/gcp_token_provider.h"
#include "cpp/obj-client/auth/gcp/gcs_signed_url.h"
#include "cpp/sync_point.h"
#include "service/http/http_client.h"
#include "util/url_coding.h"

namespace doris::io {
namespace {

constexpr std::string_view IAM_CREDENTIALS_ENDPOINT =
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";

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
                          const std::string& ca_cert_file_path, std::string* signature) {
    std::string encoded_payload;
    base64_encode(std::string(string_to_sign), &encoded_payload);
    std::string request_body = fmt::format(R"({{"payload":"{}"}})", encoded_payload);
    std::string endpoint =
            std::string(IAM_CREDENTIALS_ENDPOINT) + percent_encode(service_account) + ":signBlob";

    HttpClient client;
    // Keep the response body for non-2xx replies so IAM permission and
    // service-account errors are actionable to operators.
    RETURN_IF_ERROR(client.init(endpoint, false, HttpClient::AuthTokenMode::NONE));
    if (!ca_cert_file_path.empty()) {
        RETURN_IF_ERROR(client.set_ca_cert_file(ca_cert_file_path));
    }
    client.set_authorization("Bearer " + std::string(access_token));
    client.set_content_type("application/json");
    client.set_timeout_ms(request_timeout_ms > 0 ? request_timeout_ms : 10000);

    TEST_SYNC_POINT_RETURN_WITH_VALUE("GcsV4Signer::sign_blob", Status::OK(), access_token,
                                      service_account, string_to_sign, signature);
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

} // namespace

Status resolve_gcs_signer_email(const GcpCredentialConfig& credential,
                                const GcpTokenProvider& token_provider, std::string* signer_email) {
    *signer_email = credential.impersonation_service_account.empty()
                            ? token_provider.get_service_account_email()
                            : credential.impersonation_service_account;
    if (!is_valid_gcp_service_account_email(*signer_email)) {
        return Status::InvalidArgument(
                "GCS V4 signing requires a service account identity; configure "
                "gs.impersonation_service_account for credentials without one");
    }
    return Status::OK();
}

Status generate_gcs_v4_signed_url(const GcsV4SignedUrlProviderOptions& options,
                                  const GcpCredentialConfig& credential,
                                  const std::shared_ptr<GcpTokenProvider>& token_provider,
                                  std::string* signed_url) {
    if (signed_url == nullptr) {
        return Status::InvalidArgument("signed_url output must not be null");
    }
    signed_url->clear();

    if (token_provider == nullptr) {
        return Status::InternalError("GCS V4 signing token provider is not initialized");
    }

    try {
        auto token = token_provider->get_token();
        if (!token.has_value()) {
            return Status::InternalError("failed to obtain OAuth token for IAM signBlob");
        }

        // Resolve identity from the same credentials that supplied the token. DEFAULT may
        // select a VM, a key file, or another ADC source; never infer it from VM presence.
        std::string signer_email;
        RETURN_IF_ERROR(resolve_gcs_signer_email(credential, *token_provider, &signer_email));

        auto result = build_gcs_v4_signed_url(
                {.endpoint = options.endpoint,
                 .bucket = options.bucket,
                 .key = options.key,
                 .signer_email = signer_email,
                 .expiration_secs = options.expiration_secs},
                std::chrono::system_clock::now(), [&](std::string_view string_to_sign) {
                    std::string signature;
                    auto status = call_iam_sign_blob(*token, signer_email, string_to_sign,
                                                     options.request_timeout_ms,
                                                     options.ca_cert_file_path, &signature);
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
