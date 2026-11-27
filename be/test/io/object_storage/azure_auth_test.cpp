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

#include "cpp/obj-storage/auth/azure/azure_auth.h"

#include <gtest/gtest.h>

namespace doris {
namespace {
constexpr int64_t SAS_EXPIRY_MS = 1792053000000;
AzureClientSecretConfig client_secret_config() {
    return {.tenant_id = "tenant",
            .client_id = "client",
            .client_secret = "secret",
            .authority_host = ""};
}
} // namespace

TEST(AzureAuthExpiryTest, ParsesSasExpiryForms) {
    auto parse = [](std::string_view token) {
        auto result = azure_sas_token_expiry(token);
        EXPECT_FALSE(result.error.has_value()) << *result.error;
        return result.error.has_value() ? std::optional<int64_t>(-1) : result.expires_at_ms;
    };
    EXPECT_EQ(parse("sv=2024&se=2026-10-15T08:30:00Z&sig=abc"), SAS_EXPIRY_MS);
    EXPECT_EQ(parse("se=2026-10-15T08%3A30%3A00Z"), SAS_EXPIRY_MS);
    EXPECT_EQ(parse("?se=2026-10-15T08:30Z"), SAS_EXPIRY_MS);
    EXPECT_EQ(parse("se=2026-10-15T08:30:00.1234567Z"), SAS_EXPIRY_MS + 123);
    // A fraction longer than int64_t can hold keeps millisecond precision as well.
    EXPECT_EQ(parse("se=2026-10-15T08:30:00.12345678901234567890123Z"), SAS_EXPIRY_MS + 123);
    EXPECT_EQ(parse("se=2026-10-15"), 1792022400000);
    EXPECT_EQ(parse("se=1999-12-31T23:59:59Z"), 946684799000);
    EXPECT_FALSE(parse("sv=2024&sig=abc").has_value());

    EXPECT_TRUE(azure_sas_token_expiry("se=2026-10-15&se=2026-10-16").error.has_value());
    EXPECT_TRUE(azure_sas_token_expiry("se=2026-13-01").error.has_value());
    EXPECT_TRUE(azure_sas_token_expiry("se=2026-10-15T08:30:00").error.has_value());
    EXPECT_TRUE(azure_sas_token_expiry("se=2026-10-15T08%3").error.has_value());
    EXPECT_TRUE(azure_sas_token_expiry("se=2026-10-15T08:30:00.12a4Z").error.has_value());
}

TEST(AzureAuthExpiryTest, ParsesNumericTimezoneOffsets) {
    for (const std::string& value :
         {"2026-10-15T08:30:00+00:00", "2026-10-15T08:30:00-00:00", "2026-10-15T14:00+05:30",
          "2026-10-15T05:00:00-03:30", "2026-10-16T08:29:00+23:59", "2026-10-14T08:31:00-23:59",
          "2026-10-15T08%3A30%3A00%2B00%3A00"}) {
        auto result = azure_sas_token_expiry("se=" + value);
        ASSERT_FALSE(result.error.has_value()) << *result.error;
        EXPECT_EQ(result.expires_at_ms, SAS_EXPIRY_MS) << value;
    }
    EXPECT_EQ(azure_sas_token_expiry("se=2026-10-15T14:00:00.1234567%2B05:30").expires_at_ms,
              SAS_EXPIRY_MS + 123);
    // Conversion crosses a year boundary without changing the signed token.
    AzureSasConfig sas {.token = "se=2000-01-01T00:59:59%2B01:00&sig=abc",
                        .expires_at_ms = std::nullopt};
    EXPECT_EQ(azure_auth_expiry(sas).expires_at_ms, 946684799000);
    EXPECT_FALSE(validate_azure_auth_expiry(sas, 946684798999));
    EXPECT_TRUE(validate_azure_auth_expiry(sas, 946684799000));
    EXPECT_EQ(sas.token, "se=2000-01-01T00:59:59%2B01:00&sig=abc");
}

TEST(AzureAuthExpiryTest, RejectsInvalidTimezoneOffsets) {
    for (const std::string& suffix :
         {"+24:00", "-24:00", "+00:60", "-00:60", "+1:00", "+01:0", "+0100", "+01", "+aa:00",
          "+00:aa", " 01:00", "+01:00Z", "Z+01:00", "+01:00extra"}) {
        auto result = azure_sas_token_expiry("se=2026-10-15T08:30:00" + suffix);
        ASSERT_TRUE(result.error.has_value()) << suffix;
        EXPECT_EQ(*result.error, "Azure SAS token has an invalid expiry");
        EXPECT_FALSE(result.expires_at_ms.has_value());
    }
    EXPECT_TRUE(azure_sas_token_expiry("se=2026-10-15T+01:00").error);
}

TEST(AzureAuthExpiryTest, RejectsExpiredSas) {
    AzureSasConfig sas {.token = "se=2026-10-15T08:30:00Z&sig=abc", .expires_at_ms = std::nullopt};
    EXPECT_FALSE(validate_azure_auth_expiry(sas, SAS_EXPIRY_MS - 1).has_value());
    EXPECT_TRUE(validate_azure_auth_expiry(sas, SAS_EXPIRY_MS).has_value());

    // The earlier of the upstream expiry and the token expiry applies.
    sas.expires_at_ms = SAS_EXPIRY_MS - 1000;
    EXPECT_TRUE(validate_azure_auth_expiry(sas, SAS_EXPIRY_MS - 1).has_value());
    EXPECT_EQ(azure_auth_expiry(sas).expires_at_ms, SAS_EXPIRY_MS - 1000);

    // Without any known expiry there is nothing to reject locally.
    AzureSasConfig no_expiry {.token = "sig=abc", .expires_at_ms = std::nullopt};
    EXPECT_FALSE(validate_azure_auth_expiry(no_expiry, SAS_EXPIRY_MS).has_value());

    // A malformed expiry is an error, not "never expires".
    AzureSasConfig malformed {.token = "se=2026-13-01&sig=abc", .expires_at_ms = std::nullopt};
    EXPECT_TRUE(validate_azure_auth_expiry(malformed, SAS_EXPIRY_MS - 1).has_value());

    EXPECT_FALSE(
            validate_azure_auth_expiry(AzureSharedKeyConfig {"key"}, SAS_EXPIRY_MS).has_value());
    EXPECT_FALSE(azure_auth_expiry(client_secret_config()).expires_at_ms.has_value());
}

TEST(AzureAuthExpiryTest, RequiresFieldsOfEachMethod) {
    EXPECT_FALSE(validate_azure_auth(
                         "", AzureSasConfig {.token = "sig=abc", .expires_at_ms = std::nullopt})
                         .has_value());
    EXPECT_TRUE(validate_azure_auth("", AzureSasConfig {.token = "", .expires_at_ms = std::nullopt})
                        .has_value());
    EXPECT_FALSE(validate_azure_auth("", client_secret_config()).has_value());
    auto no_tenant = client_secret_config();
    no_tenant.tenant_id.clear();
    EXPECT_TRUE(validate_azure_auth("", no_tenant).has_value());

    // Names and the endpoint-info key never expose a token or a secret.
    AzureAuthConfig sas = AzureSasConfig {.token = "sig=abc", .expires_at_ms = std::nullopt};
    EXPECT_EQ(azure_auth_type_name(sas), "sas");
    EXPECT_EQ(azure_auth_type_name(client_secret_config()), "client_secret");
}

TEST(AzureAuthExpiryTest, RejectsInvalidCalendarDatesWithoutEchoingInputs) {
    for (const std::string& value : {"2025-02-29", "2026-02-30", "2026-04-31", "secret-value"}) {
        auto result = azure_sas_token_expiry("se=" + value);
        ASSERT_TRUE(result.error.has_value());
        EXPECT_EQ(result.error->find(value), std::string::npos);
    }
    EXPECT_FALSE(azure_sas_token_expiry("se=2024-02-29").error);
    EXPECT_TRUE(azure_sas_token_expiry("se").error);
    EXPECT_TRUE(azure_sas_token_expiry("se=2026-10-15&s%65=2026-10-16").error);
    EXPECT_EQ(azure_sas_token_expiry("s%65=2026-10-15T08:30:00Z").expires_at_ms, SAS_EXPIRY_MS);
}

TEST(AzureAuthExpiryTest, RejectsEmptyQueryAndFragmentInSas) {
    for (const std::string& token : {"?", "sig=secret#fragment", "sig=secret\n"}) {
        EXPECT_TRUE(validate_azure_auth("account", AzureSasConfig {token, std::nullopt}));
    }
}

TEST(AzureAuthExpiryTest, ValidatesClientSecretAuthority) {
    auto credential = client_secret_config();
    for (const std::string& authority :
         {"", "https://login.microsoftonline.com/", "https://login.microsoftonline.us"}) {
        credential.authority_host = authority;
        EXPECT_FALSE(validate_azure_auth("account", credential));
    }
    for (const std::string& authority :
         {"http://login.example", "https://", "https://user@host", "https://host/path",
          "https://host?q=x", "https://host#x"}) {
        credential.authority_host = authority;
        EXPECT_TRUE(validate_azure_auth("account", credential));
    }
}

} // namespace doris
