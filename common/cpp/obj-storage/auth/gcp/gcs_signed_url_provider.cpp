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

#include "gcs_signed_url_provider.h"

#include <absl/strings/escaping.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <google/cloud/internal/curl_handle.h>
#include <rapidjson/document.h>

#include <exception>
#include <new>
#include <string_view>
#include <utility>

#include "cpp/obj-storage/auth/gcp/gcp_token_provider.h"
#include "cpp/obj-storage/auth/gcp/gcs_signed_url.h"
#include "cpp/obj-storage/client/obj_storage_client.h"
#include "cpp/sync_point.h"

namespace doris {
using google::cloud::StatusCode;
using SdkStatus = google::cloud::Status;
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

size_t append_iam_response(char* data, size_t size, size_t count, void* output) noexcept {
    // libcurl supplies size == 1 for body data; bound the memory used for IAM error replies.
    auto& response = *static_cast<std::string*>(output);
    constexpr size_t max_response = 1024 * 1024;
    if (size != 1 || count > max_response - response.size()) {
        return 0;
    }
    try {
        response.append(data, count);
    } catch (const std::bad_alloc&) {
        return 0;
    }
    return count;
}

SdkStatus call_iam_sign_blob(std::string_view access_token, std::string_view service_account,
                             std::string_view string_to_sign, int64_t request_timeout_ms,
                             const std::string& ca_cert_file_path, std::string* signature) {
    namespace rest = google::cloud::rest_internal;
    rest::CurlHandle http;
    auto escaped = http.MakeEscapedString(std::string(service_account));
    if (!escaped) {
        return SdkStatus {StatusCode::kResourceExhausted, "cannot encode the IAM signer account"};
    }
    std::string endpoint = std::string(IAM_CREDENTIALS_ENDPOINT) + escaped.get() + ":signBlob";
    auto payload = absl::Base64Escape({string_to_sign.data(), string_to_sign.size()});
    std::string body = fmt::format(R"({{"payload":"{}"}})", payload);
    std::string token(access_token);
    std::string response;
    rest::CurlHeaders headers(curl_slist_append(nullptr, "Content-Type: application/json"));
    if (!headers) {
        return SdkStatus {StatusCode::kResourceExhausted, "cannot allocate signBlob headers"};
    }
    for (const auto& status :
         {http.SetOption(CURLOPT_URL, endpoint.c_str()),
          http.SetOption(CURLOPT_POSTFIELDS, body.c_str()),
          http.SetOption(CURLOPT_HTTPHEADER, headers.get()),
          http.SetOption(CURLOPT_HTTPAUTH, static_cast<long>(CURLAUTH_BEARER)),
          http.SetOption(CURLOPT_XOAUTH2_BEARER, token.c_str()),
          http.SetOption(CURLOPT_TIMEOUT_MS, static_cast<long>(request_timeout_ms)),
          http.SetOption(CURLOPT_NOSIGNAL, 1L),
          http.SetOption(CURLOPT_WRITEFUNCTION, &append_iam_response),
          http.SetOption(CURLOPT_WRITEDATA, &response)}) {
        if (!status.ok()) {
            return status;
        }
    }
    if (!ca_cert_file_path.empty()) {
        auto status = http.SetOption(CURLOPT_CAINFO, ca_cert_file_path.c_str());
        if (!status.ok()) {
            return status;
        }
    }
    TEST_SYNC_POINT_RETURN_WITH_VALUE("GcsV4Signer::sign_blob", SdkStatus {}, access_token,
                                      service_account, string_to_sign, signature,
                                      &ca_cert_file_path);
    auto sent = http.EasyPerform();
    if (!sent.ok()) {
        return sent;
    }
    rapidjson::Document document;
    document.Parse(response.data(), response.size());
    const auto http_status = http.GetResponseCode();
    if (http_status < 200 || http_status >= 300) {
        return SdkStatus {StatusCode::kUnknown,
                          fmt::format("IAM signBlob failed with HTTP {}: {}", http_status,
                                      iam_error_message(document))};
    }
    if (document.HasParseError() || !document.IsObject() || !document.HasMember("signedBlob") ||
        !document["signedBlob"].IsString()) {
        return SdkStatus(StatusCode::kInternal, "IAM signBlob returned an invalid response");
    }
    if (!absl::Base64Unescape(document["signedBlob"].GetString(), signature)) {
        return SdkStatus(StatusCode::kInternal,
                         "IAM signBlob returned an invalid base64 signature");
    }
    return {};
}

} // namespace

std::function<std::string(const ObjStoragePath&, int64_t)> make_gcs_signed_url_generator(
        const GcpCredentialConfig& credential, std::string endpoint, int64_t request_timeout_ms,
        std::string ca_cert_path) {
    // signBlob uses the source identity even when object I/O impersonates a target.
    auto source = credential;
    source.impersonation_service_account.clear();
    auto provider =
            std::make_shared<GcpTokenProvider>(source, ca_cert_path, GcpTokenScope::CloudPlatform);
    return [credential, endpoint = std::move(endpoint), request_timeout_ms,
            ca_cert_path = std::move(ca_cert_path),
            provider = std::move(provider)](const ObjStoragePath& path, int64_t expiration_secs) {
        std::string url;
        auto status = generate_gcs_v4_signed_url({.endpoint = endpoint,
                                                  .bucket = path.bucket,
                                                  .key = path.key,
                                                  .expiration_secs = expiration_secs,
                                                  .request_timeout_ms = request_timeout_ms,
                                                  .ca_cert_file_path = ca_cert_path},
                                                 credential, provider, &url);
        if (!status.ok()) {
            LOG(WARNING) << "failed to generate GCS V4 signed URL: " << status;
            return std::string {};
        }
        return url;
    };
}

SdkStatus resolve_gcs_signer_email(const GcpCredentialConfig& credential,
                                   const GcpTokenProvider& token_provider,
                                   std::string* signer_email) {
    *signer_email = credential.impersonation_service_account.empty()
                            ? token_provider.get_service_account_email()
                            : credential.impersonation_service_account;
    if (!is_valid_gcp_service_account_email(*signer_email)) {
        return SdkStatus(StatusCode::kInvalidArgument,
                         "GCS V4 signing requires a service account identity; configure "
                         "gs.impersonation_service_account for credentials without one");
    }
    return {};
}

SdkStatus generate_gcs_v4_signed_url(const GcsV4SignedUrlProviderOptions& options,
                                     const GcpCredentialConfig& credential,
                                     const std::shared_ptr<GcpTokenProvider>& token_provider,
                                     std::string* signed_url) {
    if (signed_url == nullptr) {
        return SdkStatus(StatusCode::kInvalidArgument, "signed_url output must not be null");
    }
    signed_url->clear();

    if (token_provider == nullptr) {
        return SdkStatus(StatusCode::kInternal, "GCS V4 signing token provider is not initialized");
    }

    try {
        auto token = token_provider->get_token();
        if (!token.has_value()) {
            return SdkStatus(StatusCode::kInternal,
                             "failed to obtain OAuth token for IAM signBlob");
        }

        // Resolve identity from the same credentials that supplied the token. DEFAULT may
        // select a VM, a key file, or another ADC source; never infer it from VM presence.
        std::string signer_email;
        auto resolved = resolve_gcs_signer_email(credential, *token_provider, &signer_email);
        if (!resolved.ok()) {
            return resolved;
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
                                                     options.request_timeout_ms,
                                                     options.ca_cert_file_path, &signature);
                    if (!status.ok()) {
                        return GcsSignBlobResult {.error = status.message()};
                    }
                    return GcsSignBlobResult {.signature = std::move(signature)};
                });
        if (!result.ok()) {
            return SdkStatus(StatusCode::kInternal,
                             "failed to build GCS V4 signed URL: " + result.error);
        }
        *signed_url = std::move(result.signed_url);
        return {};
    } catch (const std::exception& e) {
        return SdkStatus(StatusCode::kInternal,
                         fmt::format("failed to initialize GCS URL signer: {}", e.what()));
    }
}

} // namespace doris
