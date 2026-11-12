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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "cpp/obj-client/auth/gcp/gcp_auth.h"
#include "cpp/obj-client/auth/obj_credential_common.h"

namespace doris {

class TCredential;

namespace cloud {
class ObjectStoreInfoPB;
}

// This abstraction keeps provider-specific passwordless authentication out of
// S3 utilities and provides one runtime credential model shared by BE and Cloud.
// To add another provider (for example OSS or Azure):
//  1. add <Provider>CredentialConfig in its own <provider>_auth.h;
//  2. add that type to this variant; and
//  3. implement the corresponding hash/validate visitors, client builder, and
//     protocol/property conversions.
// The explicit visitors intentionally make an incomplete extension fail to compile.
using CredentialConfig = std::variant<std::monostate, GcpCredentialConfig>;

void hash_combine_credential(uint64_t* hash, const CredentialConfig& credential);

std::string_view credential_type_name(const CredentialConfig& credential);

std::optional<std::string> validate_obj_credential_config(
        const CredentialConfig& credential, const ObjCredentialValidationContext& context);

std::optional<std::string> parse_obj_credential_properties(
        const ObjCredentialPropertyGetter& get_property, CredentialConfig* credential);

template <typename Compare, typename Allocator>
std::optional<std::string> parse_obj_credential_properties(
        const std::map<std::string, std::string, Compare, Allocator>& properties,
        CredentialConfig* credential) {
    return parse_obj_credential_properties(make_property_getter(properties), credential);
}

void convert_obj_credential(const cloud::ObjectStoreInfoPB& source, CredentialConfig* credential);
void convert_obj_credential(const TCredential& source, CredentialConfig* credential);

} // namespace doris
