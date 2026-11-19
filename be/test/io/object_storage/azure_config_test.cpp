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

#include <gen_cpp/ObjectStorage_types.h>
#include <gtest/gtest.h>

#include "cpp/obj-storage/parser/obj_storage_config_parser.h"

namespace doris {
namespace {
TAzureClientConfig azure_config() {
    TAzureSharedKey key;
    key.__set_account_key("shared-secret");
    TAzureCredential credential;
    credential.__set_shared_key(key);
    TAzureClientConfig config;
    config.__set_endpoint("https://acct.blob.core.windows.net");
    config.__set_account_name("acct");
    config.__set_container("container");
    config.__set_credential(credential);
    return config;
}

TAzureCredential sas_credential() {
    TAzureSas sas;
    sas.__set_token("?sv=2024-11-04&se=2099-10-15T08%3A30%3A00Z&sig=sas-secret");
    TAzureCredential credential;
    credential.__set_sas(sas);
    return credential;
}

TAzureCredential oauth_credential() {
    TAzureClientSecret secret;
    secret.__set_tenant_id("tenant");
    secret.__set_client_id("client");
    secret.__set_client_secret("oauth-secret");
    secret.__set_authority_host("https://login.chinacloudapi.cn/");
    TAzureCredential credential;
    credential.__set_client_secret(secret);
    return credential;
}

void expect_error(const TAzureClientConfig& config) {
    auto result = ObjStorageConfigParser::from_azure_client_config(config);
    ASSERT_TRUE(std::holds_alternative<std::string>(result));
    for (const auto* value : {"shared-secret", "sas-secret", "oauth-secret", "endpoint-secret"}) {
        EXPECT_EQ(std::get<std::string>(result).find(value), std::string::npos);
    }
}
} // namespace

TEST(AzureConfigTest, SharedKeyPreservesExplicitParametersWithoutHostDefaults) {
    auto config = azure_config();
    auto result = ObjStorageConfigParser::from_azure_client_config(config);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result));
    const auto& resource = std::get<ObjStorageResourceConfig>(result);
    EXPECT_EQ(resource.bucket, "container");
    EXPECT_TRUE(resource.prefix.empty());
    const auto& azure = std::get<AzureObjStorageConfig>(resource.client);
    EXPECT_EQ(azure.connection.endpoint, config.endpoint);
    EXPECT_EQ(azure.auth.account_name, "acct");
    EXPECT_EQ(std::get<AzureSharedKeyConfig>(azure.auth.credential).account_key, "shared-secret");
    EXPECT_FALSE(azure.connection.ca_cert_file.has_value());
    EXPECT_FALSE(azure.connection.max_retries.has_value());
}

TEST(AzureConfigTest, SasNormalizesTokenAndPreservesOptionalExpiry) {
    auto config = azure_config();
    config.account_name.clear();
    config.__set_credential(sas_credential());
    for (bool include_expiry : {false, true}) {
        if (include_expiry) {
            config.credential.sas.__set_expires_at_ms(4095793800000);
        }
        auto result = ObjStorageConfigParser::from_azure_client_config(config);
        ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result));
        const auto& azure =
                std::get<AzureObjStorageConfig>(std::get<ObjStorageResourceConfig>(result).client);
        const auto& sas = std::get<AzureSasConfig>(azure.auth.credential);
        EXPECT_EQ(sas.token, config.credential.sas.token.substr(1));
        EXPECT_EQ(sas.expires_at_ms,
                  include_expiry ? std::optional<int64_t>(4095793800000) : std::nullopt);
    }
    config.credential.sas.__set_expires_at_ms(1);
    expect_error(config);
    config.__set_credential(sas_credential());
    config.credential.sas.__set_token("se=not-a-date&sig=sas-secret");
    expect_error(config);
}

TEST(AzureConfigTest, SasPreservesSignedNumericTimezoneOffset) {
    auto config = azure_config();
    config.__set_credential(sas_credential());
    config.credential.sas.__set_token("?se=2099-01-01T00%3A00%3A00%2B00%3A00&sig=sas-secret");
    auto result = ObjStorageConfigParser::from_azure_client_config(config);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result));
    const auto& azure =
            std::get<AzureObjStorageConfig>(std::get<ObjStorageResourceConfig>(result).client);
    const auto& sas = std::get<AzureSasConfig>(azure.auth.credential);
    EXPECT_EQ(sas.token, config.credential.sas.token.substr(1));
    EXPECT_EQ(azure_auth_expiry(sas).expires_at_ms, 4070908800000);
}

TEST(AzureConfigTest, ClientSecretPreservesParametersAndRequiresHttps) {
    auto config = azure_config();
    config.account_name.clear();
    config.__set_credential(oauth_credential());
    auto result = ObjStorageConfigParser::from_azure_client_config(config);
    ASSERT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(result));
    const auto& azure =
            std::get<AzureObjStorageConfig>(std::get<ObjStorageResourceConfig>(result).client);
    const auto& secret = std::get<AzureClientSecretConfig>(azure.auth.credential);
    EXPECT_EQ(secret.tenant_id, "tenant");
    EXPECT_EQ(secret.client_id, "client");
    EXPECT_EQ(secret.client_secret, "oauth-secret");
    EXPECT_EQ(secret.authority_host, "https://login.chinacloudapi.cn/");
    config.__set_endpoint("http://acct.blob.core.windows.net");
    expect_error(config);
}

TEST(AzureConfigTest, RejectsMissingOrAmbiguousCredentials) {
    auto config = azure_config();
    config.credential.shared_key.account_key.clear();
    expect_error(config);
    config = azure_config();
    config.account_name.clear();
    expect_error(config);
    config = azure_config();
    config.__isset.credential = false;
    expect_error(config);
    config = azure_config();
    config.credential.__isset.shared_key = false;
    expect_error(config);
    config = azure_config();
    // Generated C++ can represent malformed wire input with multiple union alternatives.
    config.credential.__isset.sas = true;
    expect_error(config);
    config.__set_credential(sas_credential());
    config.credential.sas.token.clear();
    expect_error(config);
    for (int field = 0; field < 3; ++field) {
        config.__set_credential(oauth_credential());
        auto& secret = config.credential.client_secret;
        (field == 0   ? secret.tenant_id
         : field == 1 ? secret.client_id
                      : secret.client_secret)
                .clear();
        expect_error(config);
    }
}

TEST(AzureConfigTest, RequiresExplicitContainerAndCleanServiceEndpoint) {
    auto config = azure_config();
    config.container.clear();
    expect_error(config);
    for (const auto* endpoint :
         {"", "acct.blob.core.windows.net", "file:///endpoint-secret",
          "https://acct.blob.core.windows.net?sig=endpoint-secret", "https://endpoint-secret@host",
          "https://acct.blob.core.windows.net#endpoint-secret", "https://", "https:///path"}) {
        config = azure_config();
        config.__set_endpoint(endpoint);
        expect_error(config);
    }
    config = azure_config();
    config.__set_endpoint("http://127.0.0.1:10000/acct");
    EXPECT_TRUE(std::holds_alternative<ObjStorageResourceConfig>(
            ObjStorageConfigParser::from_azure_client_config(config)));
}

} // namespace doris
