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

#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "io/obj_storage/azure_config_codec.h"
#include "io/obj_storage/azure_config_validator.h"
#include "io/obj_storage/azure_credential_validator.h"
#include "io/obj_storage/obj_storage_config.h"
#include "io/obj_storage/obj_storage_config_resolver.h"

namespace doris::io {
namespace {

// 2026-10-15T08:30:00Z
constexpr int64_t SAS_EXPIRY_MS = 1792053000000;

TAzureClientConfig make_config(TAzureCredential credential) {
    TAzureClientConfig config;
    config.__set_endpoint("https://acct.blob.core.windows.net");
    config.__set_account_name("acct");
    config.__set_credential(std::move(credential));
    return config;
}

TAzureCredential shared_key_credential(std::string key) {
    TAzureSharedKey shared_key;
    shared_key.__set_account_key(std::move(key));
    TAzureCredential credential;
    credential.__set_shared_key(shared_key);
    return credential;
}

TAzureCredential sas_credential(std::string token) {
    TAzureSas sas;
    sas.__set_token(std::move(token));
    TAzureCredential credential;
    credential.__set_sas(sas);
    return credential;
}

TAzureCredential client_secret_credential() {
    TAzureClientSecret secret;
    secret.__set_tenant_id("tenant");
    secret.__set_client_id("client");
    secret.__set_client_secret("secret");
    secret.__set_authority_host("https://login.microsoftonline.com/");
    TAzureCredential credential;
    credential.__set_client_secret(secret);
    return credential;
}

AzureClientConfig make_azure(AzureAuthConfig auth) {
    return AzureClientConfig {
            .endpoint = "https://acct.blob.core.windows.net",
            .account = "acct",
            .container = "container",
            .auth = std::move(auth),
    };
}

} // namespace

TEST(AzureConfigCodecTest, DecodesEachCredential) {
    auto shared_key = AzureConfigCodec::decode(make_config(shared_key_credential("key")), "c1");
    ASSERT_TRUE(shared_key.has_value()) << shared_key.error();
    EXPECT_EQ(shared_key->endpoint, "https://acct.blob.core.windows.net");
    EXPECT_EQ(shared_key->account, "acct");
    EXPECT_EQ(shared_key->container, "c1");
    ASSERT_TRUE(std::holds_alternative<AzureSharedKeyConfig>(shared_key->auth));
    EXPECT_EQ(std::get<AzureSharedKeyConfig>(shared_key->auth).account_key, "key");

    auto sas_config = make_config(sas_credential("?sv=2024&se=2026-10-15&sig=abc"));
    sas_config.credential.sas.__set_expires_at_ms(42);
    auto sas = AzureConfigCodec::decode(sas_config, "c1");
    ASSERT_TRUE(sas.has_value()) << sas.error();
    ASSERT_TRUE(std::holds_alternative<AzureSasConfig>(sas->auth));
    // The leading '?' is dropped so the token can be appended to the container URL as is.
    EXPECT_EQ(std::get<AzureSasConfig>(sas->auth).token, "sv=2024&se=2026-10-15&sig=abc");
    EXPECT_EQ(std::get<AzureSasConfig>(sas->auth).expires_at_ms, std::optional<int64_t>(42));

    auto secret = AzureConfigCodec::decode(make_config(client_secret_credential()), "c1");
    ASSERT_TRUE(secret.has_value()) << secret.error();
    ASSERT_TRUE(std::holds_alternative<AzureClientSecretConfig>(secret->auth));
    const auto& client_secret = std::get<AzureClientSecretConfig>(secret->auth);
    EXPECT_EQ(client_secret.tenant_id, "tenant");
    EXPECT_EQ(client_secret.client_id, "client");
    EXPECT_EQ(client_secret.client_secret, "secret");
    EXPECT_EQ(client_secret.authority_host, "https://login.microsoftonline.com/");
}

TEST(AzureConfigCodecTest, RequiresExactlyOneCredential) {
    TAzureClientConfig no_credential;
    no_credential.__set_endpoint("https://acct.blob.core.windows.net");
    EXPECT_FALSE(AzureConfigCodec::decode(no_credential, "c1").has_value());

    // Set the second member directly: a generated union setter may clear the other member,
    // while a peer can still put two members on the wire.
    auto two_credentials = make_config(shared_key_credential("key"));
    two_credentials.credential.sas.__set_token("sig=abc");
    two_credentials.credential.__isset.sas = true;
    EXPECT_FALSE(AzureConfigCodec::decode(two_credentials, "c1").has_value());
}

TEST(AzureConfigCodecTest, ConfiguredContainerMustMatchLocation) {
    auto config = make_config(shared_key_credential("key"));
    config.__set_container("c1");

    auto same = AzureConfigCodec::decode(config, "c1");
    ASSERT_TRUE(same.has_value()) << same.error();
    EXPECT_EQ(same->container, "c1");

    auto without_location = AzureConfigCodec::decode(config, "");
    ASSERT_TRUE(without_location.has_value()) << without_location.error();
    EXPECT_EQ(without_location->container, "c1");

    EXPECT_FALSE(AzureConfigCodec::decode(config, "c2").has_value());
}

TEST(AzureConfigValidatorTest, RequiresFieldsOfTheSelectedCredential) {
    EXPECT_TRUE(AzureConfigValidator::validate(make_azure(AzureSharedKeyConfig {"key"})).ok());

    auto no_account = make_azure(AzureSharedKeyConfig {"key"});
    no_account.account.clear();
    EXPECT_FALSE(AzureConfigValidator::validate(no_account).ok());
    EXPECT_FALSE(AzureConfigValidator::validate(make_azure(AzureSharedKeyConfig {""})).ok());

    // SAS and client secret do not sign with the account name, so it may be absent.
    auto sas = make_azure(AzureSasConfig {.token = "sig=abc", .expires_at_ms = std::nullopt});
    sas.account.clear();
    EXPECT_TRUE(AzureConfigValidator::validate(sas).ok());
    EXPECT_FALSE(AzureConfigValidator::validate(
                         make_azure(AzureSasConfig {.token = "", .expires_at_ms = std::nullopt}))
                         .ok());

    AzureClientSecretConfig secret {.tenant_id = "tenant",
                                    .client_id = "client",
                                    .client_secret = "secret",
                                    .authority_host = ""};
    EXPECT_TRUE(AzureConfigValidator::validate(make_azure(secret)).ok());
    secret.tenant_id.clear();
    EXPECT_FALSE(AzureConfigValidator::validate(make_azure(secret)).ok());

    auto no_endpoint = make_azure(AzureSharedKeyConfig {"key"});
    no_endpoint.endpoint.clear();
    EXPECT_FALSE(AzureConfigValidator::validate(no_endpoint).ok());
    auto no_container = make_azure(AzureSharedKeyConfig {"key"});
    no_container.container.clear();
    EXPECT_FALSE(AzureConfigValidator::validate(no_container).ok());
}

TEST(AzureConfigValidatorTest, AccountMustMatchStandardEndpoint) {
    auto other_account = make_azure(AzureSharedKeyConfig {"key"});
    other_account.endpoint = "https://other.blob.core.windows.net";
    EXPECT_FALSE(AzureConfigValidator::validate(other_account).ok());

    auto case_differs = make_azure(AzureSharedKeyConfig {"key"});
    case_differs.endpoint = "https://ACCT.dfs.core.windows.net/";
    EXPECT_TRUE(AzureConfigValidator::validate(case_differs).ok());

    // A custom endpoint names no account in its host.
    auto custom = make_azure(AzureSharedKeyConfig {"key"});
    custom.endpoint = "http://127.0.0.1:10000/acct";
    EXPECT_TRUE(AzureConfigValidator::validate(custom).ok());
}

TEST(AzureCredentialValidatorTest, ParsesSasExpiryForms) {
    auto parse = [](std::string_view token) {
        auto result = AzureCredentialValidator::sas_expiry_ms(token);
        EXPECT_TRUE(result.has_value()) << result.error();
        return result.has_value() ? *result : std::optional<int64_t>(-1);
    };
    EXPECT_EQ(parse("sv=2024&se=2026-10-15T08:30:00Z&sig=abc"), SAS_EXPIRY_MS);
    EXPECT_EQ(parse("se=2026-10-15T08%3A30%3A00Z"), SAS_EXPIRY_MS);
    EXPECT_EQ(parse("?se=2026-10-15T08:30Z"), SAS_EXPIRY_MS);
    EXPECT_EQ(parse("se=2026-10-15T08:30:00.1234567Z"), SAS_EXPIRY_MS + 123);
    EXPECT_EQ(parse("se=2026-10-15"), 1792022400000);
    EXPECT_EQ(parse("se=1999-12-31T23:59:59Z"), 946684799000);
    EXPECT_FALSE(parse("sv=2024&sig=abc").has_value());

    EXPECT_FALSE(
            AzureCredentialValidator::sas_expiry_ms("se=2026-10-15&se=2026-10-16").has_value());
    EXPECT_FALSE(AzureCredentialValidator::sas_expiry_ms("se=2026-13-01").has_value());
    EXPECT_FALSE(AzureCredentialValidator::sas_expiry_ms("se=2026-10-15T08:30:00").has_value());
    EXPECT_FALSE(AzureCredentialValidator::sas_expiry_ms("se=2026-10-15T08%3").has_value());
}

TEST(AzureCredentialValidatorTest, RejectsExpiredSas) {
    AzureSasConfig sas {.token = "se=2026-10-15T08:30:00Z&sig=abc", .expires_at_ms = std::nullopt};
    EXPECT_TRUE(AzureCredentialValidator::validate(sas, SAS_EXPIRY_MS - 1).ok());
    EXPECT_FALSE(AzureCredentialValidator::validate(sas, SAS_EXPIRY_MS).ok());

    // The earlier of the upstream expiry and the token expiry applies.
    sas.expires_at_ms = SAS_EXPIRY_MS - 1000;
    EXPECT_FALSE(AzureCredentialValidator::validate(sas, SAS_EXPIRY_MS - 1).ok());

    // Without any known expiry there is nothing to reject locally.
    AzureSasConfig no_expiry {.token = "sig=abc", .expires_at_ms = std::nullopt};
    EXPECT_TRUE(AzureCredentialValidator::validate(no_expiry, SAS_EXPIRY_MS).ok());

    EXPECT_TRUE(
            AzureCredentialValidator::validate(AzureSharedKeyConfig {"key"}, SAS_EXPIRY_MS).ok());
}

TEST(ObjStorageConfigResolverTest, ResolvesTypedAzureConfig) {
    auto config = ObjStorageConfigResolver::from_azure_client_config(
            make_config(sas_credential("sig=abc")), "c1");
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->usage, ClientUsage::EXTERNAL_ACCESS);
    EXPECT_EQ(config->provider(), ObjStorageProvider::AZURE);
    ASSERT_TRUE(std::holds_alternative<AzureClientConfig>(config->backend));
    EXPECT_EQ(std::get<AzureClientConfig>(config->backend).container, "c1");

    // A structurally invalid config fails here, before any client is built.
    auto missing_tenant = make_config(client_secret_credential());
    missing_tenant.credential.client_secret.__set_tenant_id("");
    EXPECT_FALSE(
            ObjStorageConfigResolver::from_azure_client_config(missing_tenant, "c1").has_value());
}

TEST(ObjClientConfigTest, AzureCredentialsAreMaskedAndPartOfIdentity) {
    ObjClientConfig sas {.connection = {},
                         .usage = ClientUsage::EXTERNAL_ACCESS,
                         .backend = make_azure(AzureSasConfig {.token = "sig=secret-token",
                                                               .expires_at_ms = std::nullopt})};
    ObjClientConfig other_sas = sas;
    std::get<AzureClientConfig>(other_sas.backend).auth =
            AzureSasConfig {.token = "sig=other-token", .expires_at_ms = std::nullopt};
    EXPECT_NE(sas, other_sas);
    EXPECT_EQ(sas.to_string().find("secret-token"), std::string::npos);
    EXPECT_NE(sas.to_string().find("auth=sas"), std::string::npos);

    ObjClientConfig secret {
            .connection = {},
            .usage = ClientUsage::EXTERNAL_ACCESS,
            .backend = make_azure(AzureClientSecretConfig {.tenant_id = "tenant",
                                                           .client_id = "client",
                                                           .client_secret = "client-secret-value",
                                                           .authority_host = ""})};
    EXPECT_EQ(secret.to_string().find("client-secret-value"), std::string::npos);
    EXPECT_NE(secret, sas);
}

} // namespace doris::io
