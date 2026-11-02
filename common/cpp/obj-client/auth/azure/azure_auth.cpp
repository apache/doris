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

#include <functional>

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

void hash_method(uint64_t* hash, std::string_view account, const AzureSharedKeyConfig& shared_key) {
    // Hash account + key together, as ak + sk for S3, so swapping them changes the result.
    std::string combined(account);
    combined += shared_key.account_key;
    *hash ^= std::hash<std::string> {}(combined);
}

std::string_view method_name(const AzureSharedKeyConfig&) {
    return "shared_key";
}

std::string method_account_key(const AzureSharedKeyConfig& shared_key) {
    return shared_key.account_key;
}

} // namespace

std::optional<std::string> validate_azure_auth(std::string_view account,
                                               const AzureAuthConfig& auth) {
    return std::visit([&](const auto& method) { return validate_method(account, method); }, auth);
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
