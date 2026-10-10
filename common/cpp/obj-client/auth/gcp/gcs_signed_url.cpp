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

#include "cpp/obj-client/auth/gcp/gcs_signed_url.h"

#include <fmt/format.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <utility>

#include "cpp/obj-client/auth/gcp/gcp_auth.h"

namespace doris {
namespace {

constexpr std::string_view SIGNING_ALGORITHM = "GOOG4-RSA-SHA256";
constexpr std::string_view SIGNED_HEADERS = "host";
constexpr std::string_view SIGNING_REGION = "auto";
constexpr std::string_view SIGNING_SERVICE = "storage";
constexpr std::string_view SIGNING_TERMINATOR = "goog4_request";
constexpr int64_t MAX_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;

struct Endpoint {
    std::string scheme;
    std::string authority;
};

bool is_unreserved(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' ||
           c == '_' || c == '.' || c == '~';
}

std::string percent_encode(std::string_view value, bool preserve_slash) {
    constexpr char HEX[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(value.size());
    for (unsigned char c : value) {
        if (is_unreserved(c) || (preserve_slash && c == '/')) {
            encoded.push_back(static_cast<char>(c));
            continue;
        }
        encoded.push_back('%');
        encoded.push_back(HEX[c >> 4]);
        encoded.push_back(HEX[c & 0x0F]);
    }
    return encoded;
}

std::string hex_encode(std::string_view value) {
    constexpr char HEX[] = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(value.size() * 2);
    for (unsigned char c : value) {
        encoded.push_back(HEX[c >> 4]);
        encoded.push_back(HEX[c & 0x0F]);
    }
    return encoded;
}

std::string sha256_hex(std::string_view value) {
    std::array<unsigned char, SHA256_DIGEST_LENGTH> digest {};
    SHA256(reinterpret_cast<const unsigned char*>(value.data()), value.size(), digest.data());
    return hex_encode(
            std::string_view(reinterpret_cast<const char*>(digest.data()), digest.size()));
}

std::optional<std::string> parse_endpoint(std::string endpoint, Endpoint* parsed) {
    if (endpoint.empty()) {
        return "GCS endpoint must not be empty";
    }
    auto scheme_end = endpoint.find("://");
    if (scheme_end == std::string::npos) {
        parsed->scheme = "https";
    } else {
        parsed->scheme = endpoint.substr(0, scheme_end);
        endpoint.erase(0, scheme_end + 3);
    }
    if (!is_valid_gcp_storage_endpoint(parsed->scheme + "://" + endpoint)) {
        return "GCS signed URL requires a Google Storage HTTPS service-base endpoint without a "
               "bucket";
    }
    if (endpoint.ends_with('/')) {
        endpoint.pop_back();
    }
    // HTTP clients omit the default TLS port from Host. Sign the same authority.
    if (endpoint.ends_with(":443")) {
        endpoint.resize(endpoint.size() - 4);
    }
    parsed->authority = std::move(endpoint);
    return std::nullopt;
}

std::optional<std::string> format_signing_time(std::chrono::system_clock::time_point now,
                                               std::string* timestamp, std::string* date) {
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm utc {};
#if defined(_WIN32)
    if (gmtime_s(&utc, &time) != 0) {
        return "failed to convert GCS signing time to UTC";
    }
#else
    if (gmtime_r(&time, &utc) == nullptr) {
        return "failed to convert GCS signing time to UTC";
    }
#endif
    char timestamp_buffer[17];
    char date_buffer[9];
    if (std::strftime(timestamp_buffer, sizeof(timestamp_buffer), "%Y%m%dT%H%M%SZ", &utc) == 0 ||
        std::strftime(date_buffer, sizeof(date_buffer), "%Y%m%d", &utc) == 0) {
        return "failed to format GCS signing time";
    }
    *timestamp = timestamp_buffer;
    *date = date_buffer;
    return std::nullopt;
}

} // namespace

GcsV4SignedUrlResult build_gcs_v4_signed_url(const GcsV4SignedUrlOptions& options,
                                             std::chrono::system_clock::time_point now,
                                             const GcsSignBlobFunction& sign_blob) {
    if (options.bucket.empty()) {
        return {.error = "GCS bucket must not be empty"};
    }
    if (!is_valid_gcp_service_account_email(options.signer_email)) {
        return {.error = "invalid GCS signing service account email"};
    }
    if (options.expiration_secs <= 0 || options.expiration_secs > MAX_EXPIRATION_SECONDS) {
        return {.error = fmt::format("GCS signed URL expiration must be in [1, {}] seconds",
                                     MAX_EXPIRATION_SECONDS)};
    }
    if (!sign_blob) {
        return {.error = "GCS signBlob callback must not be empty"};
    }

    Endpoint endpoint;
    if (auto error = parse_endpoint(options.endpoint, &endpoint); error.has_value()) {
        return {.error = std::move(*error)};
    }
    std::string timestamp;
    std::string date;
    if (auto error = format_signing_time(now, &timestamp, &date); error.has_value()) {
        return {.error = std::move(*error)};
    }

    const std::string canonical_uri =
            "/" + percent_encode(options.bucket, false) + "/" + percent_encode(options.key, true);
    const std::string credential_scope =
            fmt::format("{}/{}/{}/{}", date, SIGNING_REGION, SIGNING_SERVICE, SIGNING_TERMINATOR);
    const std::string credential = options.signer_email + "/" + credential_scope;
    const std::string canonical_query = fmt::format(
            "X-Goog-Algorithm={}&X-Goog-Credential={}&X-Goog-Date={}&"
            "X-Goog-Expires={}&X-Goog-SignedHeaders={}",
            SIGNING_ALGORITHM, percent_encode(credential, false), timestamp,
            options.expiration_secs, SIGNED_HEADERS);
    const std::string canonical_request =
            fmt::format("GET\n{}\n{}\nhost:{}\n\n{}\nUNSIGNED-PAYLOAD", canonical_uri,
                        canonical_query, endpoint.authority, SIGNED_HEADERS);
    const std::string string_to_sign = fmt::format("{}\n{}\n{}\n{}", SIGNING_ALGORITHM, timestamp,
                                                   credential_scope, sha256_hex(canonical_request));

    auto sign_result = sign_blob(string_to_sign);
    if (!sign_result.ok()) {
        return {.error = std::move(sign_result.error)};
    }
    if (sign_result.signature.empty()) {
        return {.error = "signBlob returned an empty signature"};
    }

    return {.signed_url = fmt::format("{}://{}{}?{}&X-Goog-Signature={}", endpoint.scheme,
                                      endpoint.authority, canonical_uri, canonical_query,
                                      hex_encode(sign_result.signature))};
}

} // namespace doris
