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

// Signs every request with the storage account key.
struct AzureSharedKeyConfig {
    std::string account_key;

    bool operator==(const AzureSharedKeyConfig&) const = default;
};

// One alternative per authentication method, so two methods can never be carried together.
// To add a method: add its config here, then its overloads in azure_auth.cpp and its branch in
// AzureAuthFactory::create. The visitors take no catch-all, so a missing branch fails to
// compile.
using AzureAuthConfig = std::variant<AzureSharedKeyConfig>;

// The fields each method requires for the storage account `account`; returns the error, or
// nullopt when `auth` is complete. BE checks its inputs with it. Recycler does not call it:
// S3Conf::from_obj_store_info loads the SharedKey pair complete or not at all.
std::optional<std::string> validate_azure_auth(std::string_view account,
                                               const AzureAuthConfig& auth);

// Mixes the account and the secrets of `auth` into `hash`, for client cache keys.
void hash_combine_azure_auth(uint64_t* hash, std::string_view account, const AzureAuthConfig& auth);

// The method name, for logs and error messages; it never contains a secret.
std::string_view azure_auth_type_name(const AzureAuthConfig& auth);

// The account key of a SharedKey config, empty for any other method. ObjStorageEndpointInfo
// reports it as sk.
std::string azure_auth_account_key(const AzureAuthConfig& auth);

} // namespace doris
