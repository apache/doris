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

#include "cpp/obj-client/auth/obj_credential.h"

#include <array>
#include <functional>
#include <utility>

namespace doris {
namespace {

struct CredentialParseResult {
    CredentialConfig credential {};
    std::optional<std::string> error = std::nullopt;
};

// Adapt provider-owned parsers to the generic registry. A provider parser
// returns no credential when none of its properties are present and an error
// for a partially configured credential.
CredentialParseResult parse_gcp_properties(const ObjCredentialPropertyGetter& get_property) {
    auto result = parse_gcp_credential_properties(get_property);
    if (result.error.has_value()) {
        return {.error = std::move(result.error)};
    }
    if (!result.credential.has_value()) {
        return {};
    }
    return {.credential = std::move(*result.credential)};
}

using CredentialPropertyParser =
        CredentialParseResult (*)(const ObjCredentialPropertyGetter& get_property);

// Register exactly one parser per non-monostate CredentialConfig alternative.
// The loop below also rejects properties from multiple credential providers.
// Add parse_oss_credential_properties/parse_azure_credential_properties here
// when those passwordless modes are introduced.
constexpr std::array CREDENTIAL_PROPERTY_PARSERS {
        CredentialPropertyParser {parse_gcp_properties},
};

static_assert(CREDENTIAL_PROPERTY_PARSERS.size() + 1 == std::variant_size_v<CredentialConfig>);

// These visitors are the runtime extension points. Do not add a catch-all
// template overload: explicit overloads make a new CredentialConfig type fail
// to compile until its hashing and validation are implemented.
struct CredentialHashVisitor {
    uint64_t* hash;

    void operator()(const std::monostate&) const {}

    void operator()(const GcpCredentialConfig& credential) const {
        *hash ^= static_cast<size_t>(credential.provider_type) + 1;
        *hash ^= std::hash<std::string> {}(credential.impersonation_service_account);
    }
};

struct CredentialValidationVisitor {
    const ObjCredentialValidationContext& context;

    std::optional<std::string> operator()(const std::monostate&) const { return std::nullopt; }

    std::optional<std::string> operator()(const GcpCredentialConfig& credential) const {
        if (context.provider != ObjCredentialProvider::Gcp) {
            return "GCP credentials require provider=GCP";
        }
        if (context.has_conflicting_credentials()) {
            return "GCP credentials cannot be combined with other credentials";
        }
        return validate_gcp_credential(credential);
    }
};

struct CredentialTypeNameVisitor {
    std::string_view operator()(const std::monostate&) const { return "none"; }
    std::string_view operator()(const GcpCredentialConfig&) const { return "gcp"; }
};

} // namespace

void hash_combine_credential(uint64_t* hash, const CredentialConfig& credential) {
    *hash ^= credential.index();
    std::visit(CredentialHashVisitor {hash}, credential);
}

std::string_view credential_type_name(const CredentialConfig& credential) {
    return std::visit(CredentialTypeNameVisitor {}, credential);
}

std::optional<std::string> validate_obj_credential_config(
        const CredentialConfig& credential, const ObjCredentialValidationContext& context) {
    return std::visit(CredentialValidationVisitor {context}, credential);
}

std::optional<std::string> parse_obj_credential_properties(
        const ObjCredentialPropertyGetter& get_property, CredentialConfig* credential) {
    CredentialConfig selected;

    for (auto parser : CREDENTIAL_PROPERTY_PARSERS) {
        auto result = parser(get_property);
        if (result.error.has_value()) {
            return std::move(result.error);
        }
        if (std::holds_alternative<std::monostate>(result.credential)) {
            continue;
        }
        if (!std::holds_alternative<std::monostate>(selected)) {
            return "multiple object storage credential types are configured";
        }
        selected = std::move(result.credential);
    }

    if (!std::holds_alternative<std::monostate>(selected)) {
        *credential = std::move(selected);
    }
    return std::nullopt;
}

} // namespace doris
