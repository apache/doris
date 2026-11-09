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

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace doris {

// Azure Blob authentication shared by BE and Recycler. Each process turns its own input
// (BE: FE properties, Thrift, vaults; Recycler: storage metadata) into AzureAuthConfig, and
// AzureAuthFactory (azure_auth_factory.h) turns it into SDK credentials. This header is
// SDK-free so that configuration code can use it when Azure is not built.
//
// The storage account name is not part of the authentication: it names the storage target
// (the endpoint host), so it is passed next to the config where a method needs it.
//
// Where a credential comes from, and who renews it, is not decided here: a source that renews
// a SAS (e.g. an Iceberg REST catalog) replaces the config, and the caller replaces its client.

// Signs every request with the storage account key.
struct AzureSharedKeyConfig {
    std::string account_key;

    bool operator==(const AzureSharedKeyConfig&) const = default;
};

// A shared access signature appended to every request URL.
struct AzureSasConfig {
    // SAS query string without the leading '?'.
    std::string token;
    // Expiry given by the upstream credential source, in Unix milliseconds, when known. The
    // expiry encoded in the token ("se") is checked as well; the earlier one applies.
    std::optional<int64_t> expires_at_ms;

    bool operator==(const AzureSasConfig&) const = default;
};

// Entra ID client credentials flow. The SDK credential requests and refreshes the access
// token itself, from authority_host.
struct AzureClientSecretConfig {
    std::string tenant_id;
    std::string client_id;
    std::string client_secret;
    // Empty means the SDK default authority (Azure public cloud).
    std::string authority_host;

    bool operator==(const AzureClientSecretConfig&) const = default;
};

// One alternative per authentication method, so two methods can never be carried together.
// To add a method: add its config here, then its overloads in azure_auth.cpp and its branch in
// AzureAuthFactory::create. The visitors take no catch-all, so a missing branch fails to
// compile.
using AzureAuthConfig = std::variant<AzureSharedKeyConfig, AzureSasConfig, AzureClientSecretConfig>;

// The fields each method requires for the storage account `account`; returns the error, or
// nullopt when `auth` is complete. SharedKey needs the account name and the key together
// (both empty is anonymous access), SAS a token, and client secret the tenant, the client id
// and the secret. BE checks its inputs with it. Recycler does not call it:
// S3Conf::from_obj_store_info loads the SharedKey pair complete or not at all.
std::optional<std::string> validate_azure_auth(std::string_view account,
                                               const AzureAuthConfig& auth);

// When a credential expires, in Unix milliseconds, or why that cannot be read.
struct AzureAuthExpiry {
    // Unset when the method does not expire or no expiry is known.
    std::optional<int64_t> expires_at_ms;
    // Set when the expiry cannot be read, e.g. a SAS token with a malformed "se".
    std::optional<std::string> error;
};

// The expiry of `auth`: for a SAS, the earlier of the upstream expiry and the token's "se"
// field. SharedKey does not expire, and the SDK refreshes the access token of a client secret.
AzureAuthExpiry azure_auth_expiry(const AzureAuthConfig& auth);

// An error if `auth` has a known expiry at or before now_ms, or an unreadable one. For example,
// with expires_at_ms = 10:00 and se=09:30, a SAS is rejected from 09:30 on. A local check only
// turns an expired credential into a clear error before any request; the service enforces
// expiry on every request anyway.
std::optional<std::string> validate_azure_auth_expiry(const AzureAuthConfig& auth, int64_t now_ms);

// The "se" field of a SAS query string, with or without the leading '?'. Accepts the ISO 8601
// UTC forms Azure issues, URL-encoded or not: "2026-10-15", "2026-10-15T08:30Z",
// "2026-10-15T08:30:00Z", "2026-10-15T08:30:00.1234567Z".
AzureAuthExpiry azure_sas_token_expiry(std::string_view token);

// Mixes the account and the secrets of `auth` into `hash`, for client cache keys.
void hash_combine_azure_auth(uint64_t* hash, std::string_view account, const AzureAuthConfig& auth);

// The method name, for logs and error messages; it never contains a secret.
std::string_view azure_auth_type_name(const AzureAuthConfig& auth);

// The account key of a SharedKey config, empty for any other method. ObjStorageEndpointInfo
// reports it as sk, which AzureObjStorageClient falls back to for presigned URLs, so a SAS
// token or a client secret never appears there.
std::string azure_auth_account_key(const AzureAuthConfig& auth);

} // namespace doris
