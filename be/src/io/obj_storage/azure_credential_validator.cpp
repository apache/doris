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

#include "io/obj_storage/azure_credential_validator.h"

#include <algorithm>
#include <string>
#include <variant>

namespace doris::io {
namespace {

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

// Decodes %XX escapes. SAS values never encode a space as '+', so '+' is kept as is.
Result<std::string> sas_url_decode(std::string_view value) {
    std::string decoded;
    decoded.reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] != '%') {
            decoded.push_back(value[i]);
            continue;
        }
        if (i + 2 >= value.size() || sas_hex_value(value[i + 1]) < 0 ||
            sas_hex_value(value[i + 2]) < 0) {
            return ResultError(
                    Status::InvalidArgument<false>("Azure SAS token has an invalid escape"));
        }
        decoded.push_back(
                static_cast<char>(sas_hex_value(value[i + 1]) * 16 + sas_hex_value(value[i + 2])));
        i += 2;
    }
    return decoded;
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
Result<int64_t> parse_sas_time(std::string_view value) {
    auto invalid = [&]() {
        return ResultError(Status::InvalidArgument<false>(
                "Azure SAS token has an invalid expiry '{}'", std::string(value)));
    };
    int64_t year = 0;
    int64_t month = 0;
    int64_t day = 0;
    if (value.size() < 10 || value[4] != '-' || value[7] != '-' ||
        !sas_parse_digits(value, 0, 4, &year) || !sas_parse_digits(value, 5, 2, &month) ||
        !sas_parse_digits(value, 8, 2, &day) || month < 1 || month > 12 || day < 1 || day > 31) {
        return invalid();
    }
    int64_t hour = 0;
    int64_t minute = 0;
    int64_t second = 0;
    int64_t millis = 0;
    if (value.size() > 10) {
        if (value[10] != 'T' || value.back() != 'Z') {
            return invalid();
        }
        std::string_view time = value.substr(11, value.size() - 12);
        if (time.size() < 5 || time[2] != ':' || !sas_parse_digits(time, 0, 2, &hour) ||
            !sas_parse_digits(time, 3, 2, &minute) || hour > 23 || minute > 59) {
            return invalid();
        }
        if (time.size() > 5) {
            if (time.size() < 8 || time[5] != ':' || !sas_parse_digits(time, 6, 2, &second) ||
                second > 59) {
                return invalid();
            }
        }
        if (time.size() > 8) {
            // Fractional seconds, e.g. ".1234567"; keep millisecond precision.
            std::string_view fraction = time.substr(9);
            int64_t digits = 0;
            if (time[8] != '.' || fraction.empty() ||
                !sas_parse_digits(fraction, 0, fraction.size(), &digits)) {
                return invalid();
            }
            std::string padded(fraction.substr(0, std::min<size_t>(3, fraction.size())));
            padded.resize(3, '0');
            sas_parse_digits(padded, 0, 3, &millis);
        }
    }
    int64_t seconds =
            ((sas_days_from_civil(year, month, day) * 24 + hour) * 60 + minute) * 60 + second;
    return seconds * 1000 + millis;
}

Status validate_azure_credential_expiry(const AzureSharedKeyConfig& /*shared_key*/,
                                        int64_t /*now_ms*/) {
    return Status::OK();
}

Status validate_azure_credential_expiry(const AzureClientSecretConfig& /*client_secret*/,
                                        int64_t /*now_ms*/) {
    // The SDK credential obtains and refreshes the access token, so nothing here can expire.
    return Status::OK();
}

Status validate_azure_credential_expiry(const AzureSasConfig& sas, int64_t now_ms) {
    auto token_expiry = DORIS_TRY(AzureCredentialValidator::sas_expiry_ms(sas.token));
    std::optional<int64_t> expiry = sas.expires_at_ms;
    if (token_expiry.has_value()) {
        expiry = std::min(expiry.value_or(*token_expiry), *token_expiry);
    }
    if (expiry.has_value() && *expiry <= now_ms) {
        return Status::InvalidArgument<false>(
                "Azure SAS credential expired at {} ms (now {} ms); request a new credential",
                *expiry, now_ms);
    }
    return Status::OK();
}

} // namespace

Status AzureCredentialValidator::validate(const AzureAuthConfig& auth, int64_t now_ms) {
    return std::visit([&](const auto& a) { return validate_azure_credential_expiry(a, now_ms); },
                      auth);
}

Result<std::optional<int64_t>> AzureCredentialValidator::sas_expiry_ms(std::string_view token) {
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
                return ResultError(Status::InvalidArgument<false>(
                        "Azure SAS token has more than one expiry field"));
            }
            auto decoded = sas_url_decode(param.substr(eq + 1));
            if (!decoded.has_value()) {
                return ResultError(std::move(decoded).error());
            }
            auto parsed = parse_sas_time(decoded.value());
            if (!parsed.has_value()) {
                return ResultError(std::move(parsed).error());
            }
            expiry = parsed.value();
        }
        begin = end + 1;
    }
    return expiry;
}

} // namespace doris::io
