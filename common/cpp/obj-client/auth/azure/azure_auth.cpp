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

#include "cpp/obj-client/auth/azure/azure_auth.h"

#include <algorithm>
#include <functional>
#include <string>

namespace doris {
namespace {

std::optional<std::string> validate_method(std::string_view account,
                                           const AzureSharedKeyConfig& shared_key) {
    // Either both are provided or both are empty. Unlike the flat ak/sk rule of S3 there is no
    // role_arn exemption: Azure has no role authentication, so SharedKey always needs the
    // account name and the key together.
    if (!account.empty() && shared_key.account_key.empty()) {
        return "Invalid azure conf, empty account key";
    }
    if (!shared_key.account_key.empty() && account.empty()) {
        return "Invalid azure conf, empty account name";
    }
    return std::nullopt;
}

std::optional<std::string> validate_method(std::string_view /*account*/,
                                           const AzureSasConfig& sas) {
    if (sas.token.empty()) {
        return "Invalid azure conf, empty SAS token";
    }
    return std::nullopt;
}

std::optional<std::string> validate_method(std::string_view /*account*/,
                                           const AzureClientSecretConfig& client_secret) {
    if (client_secret.tenant_id.empty()) {
        return "Invalid azure conf, empty tenant_id for client secret";
    }
    if (client_secret.client_id.empty()) {
        return "Invalid azure conf, empty client_id for client secret";
    }
    if (client_secret.client_secret.empty()) {
        return "Invalid azure conf, empty client_secret for client secret";
    }
    return std::nullopt;
}

int sas_hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

// Decodes %XX escapes into *decoded. SAS values never encode a space as '+', so '+' is kept
// as is.
std::optional<std::string> sas_url_decode(std::string_view value, std::string* decoded) {
    decoded->reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] != '%') {
            decoded->push_back(value[i]);
            continue;
        }
        if (i + 2 >= value.size() || sas_hex_value(value[i + 1]) < 0 ||
            sas_hex_value(value[i + 2]) < 0) {
            return "Azure SAS token has an invalid escape";
        }
        decoded->push_back(
                static_cast<char>(sas_hex_value(value[i + 1]) * 16 + sas_hex_value(value[i + 2])));
        i += 2;
    }
    return std::nullopt;
}

// Parses value[pos, pos + n) as decimal digits.
bool sas_parse_digits(std::string_view value, size_t pos, size_t n, int64_t* out) {
    if (pos + n > value.size()) {
        return false;
    }
    int64_t result = 0;
    for (size_t i = pos; i < pos + n; ++i) {
        if (value[i] < '0' || value[i] > '9') {
            return false;
        }
        result = result * 10 + (value[i] - '0');
    }
    *out = result;
    return true;
}

// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's days_from_civil).
// Done by hand so that the result never depends on the process time zone.
int64_t sas_days_from_civil(int64_t year, int64_t month, int64_t day) {
    year -= month <= 2 ? 1 : 0;
    const int64_t era = (year >= 0 ? year : year - 399) / 400;
    const int64_t year_of_era = year - era * 400;
    const int64_t shifted_month = month > 2 ? month - 3 : month + 9;
    const int64_t day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    const int64_t day_of_era =
            year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    return era * 146097 + day_of_era - 719468;
}

// Parses the UTC forms of the SAS "se" field into Unix milliseconds:
// YYYY-MM-DD, YYYY-MM-DDThh:mmZ, YYYY-MM-DDThh:mm:ssZ and YYYY-MM-DDThh:mm:ss.fffffffZ.
AzureAuthExpiry parse_sas_time(std::string_view value) {
    const AzureAuthExpiry invalid {
            .expires_at_ms = std::nullopt,
            .error = "Azure SAS token has an invalid expiry '" + std::string(value) + "'",
    };
    int64_t year = 0;
    int64_t month = 0;
    int64_t day = 0;
    if (value.size() < 10 || value[4] != '-' || value[7] != '-' ||
        !sas_parse_digits(value, 0, 4, &year) || !sas_parse_digits(value, 5, 2, &month) ||
        !sas_parse_digits(value, 8, 2, &day) || month < 1 || month > 12 || day < 1 || day > 31) {
        return invalid;
    }
    int64_t hour = 0;
    int64_t minute = 0;
    int64_t second = 0;
    int64_t millis = 0;
    if (value.size() > 10) {
        if (value[10] != 'T' || value.back() != 'Z') {
            return invalid;
        }
        std::string_view time = value.substr(11, value.size() - 12);
        if (time.size() < 5 || time[2] != ':' || !sas_parse_digits(time, 0, 2, &hour) ||
            !sas_parse_digits(time, 3, 2, &minute) || hour > 23 || minute > 59) {
            return invalid;
        }
        if (time.size() > 5) {
            if (time.size() < 8 || time[5] != ':' || !sas_parse_digits(time, 6, 2, &second) ||
                second > 59) {
                return invalid;
            }
        }
        if (time.size() > 8) {
            // Fractional seconds, e.g. ".1234567"; keep millisecond precision.
            // Only the first three digits are kept, so the others are checked without being
            // accumulated, which would overflow int64_t for a long fraction.
            std::string_view fraction = time.substr(9);
            if (time[8] != '.' || fraction.empty() ||
                !std::all_of(fraction.begin(), fraction.end(),
                             [](char c) { return c >= '0' && c <= '9'; })) {
                return invalid;
            }
            std::string padded(fraction.substr(0, std::min<size_t>(3, fraction.size())));
            padded.resize(3, '0');
            sas_parse_digits(padded, 0, 3, &millis);
        }
    }
    int64_t seconds =
            ((sas_days_from_civil(year, month, day) * 24 + hour) * 60 + minute) * 60 + second;
    return {.expires_at_ms = seconds * 1000 + millis, .error = std::nullopt};
}

AzureAuthExpiry expiry_of(const AzureSharedKeyConfig& /*shared_key*/) {
    return {};
}

AzureAuthExpiry expiry_of(const AzureSasConfig& sas) {
    auto token_expiry = azure_sas_token_expiry(sas.token);
    if (token_expiry.error.has_value()) {
        return token_expiry;
    }
    std::optional<int64_t> expiry = sas.expires_at_ms;
    if (token_expiry.expires_at_ms.has_value()) {
        expiry =
                std::min(expiry.value_or(*token_expiry.expires_at_ms), *token_expiry.expires_at_ms);
    }
    return {.expires_at_ms = expiry, .error = std::nullopt};
}

AzureAuthExpiry expiry_of(const AzureClientSecretConfig& /*client_secret*/) {
    // The SDK credential obtains and refreshes the access token, so nothing here can expire.
    return {};
}

void hash_method(uint64_t* hash, std::string_view account, const AzureSharedKeyConfig& shared_key) {
    // Hash account + key together, as ak + sk for S3, so swapping them changes the result.
    std::string combined(account);
    combined += shared_key.account_key;
    *hash ^= std::hash<std::string> {}(combined);
}

void hash_method(uint64_t* hash, std::string_view /*account*/, const AzureSasConfig& sas) {
    *hash ^= std::hash<std::string> {}(sas.token);
    *hash ^= static_cast<uint64_t>(sas.expires_at_ms.value_or(0));
}

void hash_method(uint64_t* hash, std::string_view /*account*/,
                 const AzureClientSecretConfig& client_secret) {
    // Hash the identity triple together so that swapping fields does not collide.
    *hash ^= std::hash<std::string> {}(client_secret.tenant_id + client_secret.client_id +
                                       client_secret.client_secret);
    *hash ^= std::hash<std::string> {}(client_secret.authority_host);
}

std::string_view method_name(const AzureSharedKeyConfig&) {
    return "shared_key";
}

std::string_view method_name(const AzureSasConfig&) {
    return "sas";
}

std::string_view method_name(const AzureClientSecretConfig&) {
    return "client_secret";
}

std::string method_account_key(const AzureSharedKeyConfig& shared_key) {
    return shared_key.account_key;
}

std::string method_account_key(const AzureSasConfig& /*sas*/) {
    return "";
}

std::string method_account_key(const AzureClientSecretConfig& /*client_secret*/) {
    return "";
}

} // namespace

std::optional<std::string> validate_azure_auth(std::string_view account,
                                               const AzureAuthConfig& auth) {
    return std::visit([&](const auto& method) { return validate_method(account, method); }, auth);
}

AzureAuthExpiry azure_auth_expiry(const AzureAuthConfig& auth) {
    return std::visit([](const auto& method) { return expiry_of(method); }, auth);
}

std::optional<std::string> validate_azure_auth_expiry(const AzureAuthConfig& auth, int64_t now_ms) {
    auto expiry = azure_auth_expiry(auth);
    if (expiry.error.has_value()) {
        return expiry.error;
    }
    if (expiry.expires_at_ms.has_value() && *expiry.expires_at_ms <= now_ms) {
        return "Azure SAS credential expired at " + std::to_string(*expiry.expires_at_ms) +
               " ms (now " + std::to_string(now_ms) + " ms); request a new credential";
    }
    return std::nullopt;
}

AzureAuthExpiry azure_sas_token_expiry(std::string_view token) {
    if (!token.empty() && token.front() == '?') {
        token.remove_prefix(1);
    }
    std::optional<int64_t> expiry;
    size_t begin = 0;
    while (begin <= token.size()) {
        size_t end = token.find('&', begin);
        if (end == std::string_view::npos) {
            end = token.size();
        }
        std::string_view param = token.substr(begin, end - begin);
        size_t eq = param.find('=');
        if (eq != std::string_view::npos && param.substr(0, eq) == "se") {
            if (expiry.has_value()) {
                return {.expires_at_ms = std::nullopt,
                        .error = "Azure SAS token has more than one expiry field"};
            }
            std::string decoded;
            if (auto error = sas_url_decode(param.substr(eq + 1), &decoded); error.has_value()) {
                return {.expires_at_ms = std::nullopt, .error = std::move(error)};
            }
            auto parsed = parse_sas_time(decoded);
            if (parsed.error.has_value()) {
                return parsed;
            }
            expiry = parsed.expires_at_ms;
        }
        begin = end + 1;
    }
    return {.expires_at_ms = expiry, .error = std::nullopt};
}

void hash_combine_azure_auth(uint64_t* hash, std::string_view account,
                             const AzureAuthConfig& auth) {
    *hash ^= auth.index();
    std::visit([&](const auto& method) { hash_method(hash, account, method); }, auth);
}

std::string_view azure_auth_type_name(const AzureAuthConfig& auth) {
    return std::visit([](const auto& method) { return method_name(method); }, auth);
}

std::string azure_auth_account_key(const AzureAuthConfig& auth) {
    return std::visit([](const auto& method) { return method_account_key(method); }, auth);
}

} // namespace doris
