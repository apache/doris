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

#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace doris {

// Credential-layer provider identity. Storage and protocol enums are converted
// to this type only at their respective validation boundaries.
enum class ObjCredentialProvider {
    Unknown,
    Aws,
    Azure,
    Bos,
    Cos,
    Gcp,
    Obs,
    Oss,
    Tos,
};

// TODO: Add AwsCredentialConfig to CredentialConfig so AWS uses the same
// provider-neutral path. The AWS-specific role ARN, external ID, and credential-
// provider fields remain outside this abstraction for compatibility with existing
// configurations and protocol clients. AK/SK and session tokens are common static
// credentials for S3-compatible providers and are not part of this migration.
struct ObjCredentialValidationContext {
    ObjCredentialProvider provider = ObjCredentialProvider::Unknown;
    std::string_view ak {};
    std::string_view sk {};
    std::string_view token {};
    bool has_aws_role_arn = false;
    bool has_aws_external_id = false;
    bool has_aws_credential_provider = false;
    bool has_encrypted_access_keys = false;

    bool has_static_credentials() const {
        return !ak.empty() || !sk.empty() || !token.empty() || has_encrypted_access_keys;
    }

    bool has_aws_authentication() const {
        return has_aws_role_arn || has_aws_external_id || has_aws_credential_provider;
    }

    // A provider-native credential is mutually exclusive with both common
    // static credentials and provider-specific authentication fields.
    bool has_conflicting_credentials() const {
        return has_static_credentials() || has_aws_authentication();
    }
};

// Return an owned string so provider parsers never depend on a caller's storage
// or on the lifetime of a temporary normalized property value.
using ObjCredentialPropertyGetter = std::function<std::optional<std::string>(std::string_view)>;

template <typename PropertyMap>
ObjCredentialPropertyGetter make_property_getter(const PropertyMap& properties) {
    // Preserve the caller's comparator (including StringCaseMap's
    // case-insensitive lookup) instead of normalizing property names here.
    return [&properties](std::string_view key) -> std::optional<std::string> {
        auto it = properties.find(std::string(key));
        if (it == properties.end()) {
            return std::nullopt;
        }
        return it->second;
    };
}

} // namespace doris
