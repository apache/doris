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

#include "common/auth/obj_credential.h"

#include <gen_cpp/cloud.pb.h>

#include <string_view>

#include "cpp/obj-storage/auth/gcp/gcp_auth.h"

namespace doris::cloud {
namespace {

GcpCredentialConfig to_gcp_credential_config(const GcpCredentialPB& credential) {
    GcpCredentialConfig config;
    switch (credential.credential_provider_type()) {
    case GcpCredentialPB::DEFAULT:
        config.provider_type = GcpCredentialProviderType::Default;
        break;
    case GcpCredentialPB::COMPUTE_ENGINE:
        config.provider_type = GcpCredentialProviderType::ComputeEngine;
        break;
    }
    config.impersonation_service_account = credential.impersonation_service_account();
    return config;
}

bool has_static_credentials(const ObjectStoreInfoPB& obj) {
    return !obj.ak().empty() || !obj.sk().empty() || obj.has_encryption_info();
}

bool has_aws_authentication(const ObjectStoreInfoPB& obj) {
    return !obj.role_arn().empty() || !obj.external_id().empty() || obj.has_cred_provider_type();
}

// A provider-native credential supersedes both common static credentials and
// provider-specific authentication fields outside the credential envelope.
void clear_legacy_credentials(ObjectStoreInfoPB* obj) {
    obj->clear_ak();
    obj->clear_sk();
    obj->clear_encryption_info();
    obj->clear_cred_provider_type();
    obj->clear_role_arn();
    obj->clear_external_id();
}

std::optional<std::string> validate_required_storage_fields(const ObjectStoreInfoPB& obj,
                                                            std::string_view provider) {
    if (obj.bucket().empty() || obj.endpoint().empty() || obj.region().empty()) {
        return std::string(provider) + " storage conf requires bucket, endpoint and region";
    }
    return std::nullopt;
}

std::optional<std::string> validate_gcp_obj_credential(const ObjectStoreInfoPB& obj) {
    const auto& credential = obj.credential().gcp_credential();
    if (!credential.has_credential_provider_type()) {
        return "GCP credential provider type is required";
    }
    if (auto error = validate_required_storage_fields(obj, "GCP"); error.has_value()) {
        return error;
    }

    if (!obj.has_provider() || obj.provider() != ObjectStoreInfoPB::GCP) {
        return "GCP credentials require provider=GCP";
    }
    if (has_static_credentials(obj) || has_aws_authentication(obj)) {
        return "GCP credentials cannot be combined with other credentials";
    }
    return validate_gcp_credential(to_gcp_credential_config(credential));
}

} // namespace

bool has_obj_credential(const ObjectStoreInfoPB& obj) {
    return obj.has_credential();
}

std::optional<std::string> validate_obj_credential(const ObjectStoreInfoPB& obj) {
    if (!obj.has_credential()) {
        return "credential is not set";
    }

    // Add one provider-specific branch for every new field in
    // ObjectStoreCredentialPB. Reject multiple fields before dispatch when a
    // second provider is introduced.
    if (obj.credential().has_gcp_credential()) {
        return validate_gcp_obj_credential(obj);
    }
    return "unsupported object storage credential";
}

std::optional<std::string> validate_obj_authentication(const ObjectStoreInfoPB& obj) {
    if (has_obj_credential(obj)) {
        return validate_obj_credential(obj);
    }
    if (obj.has_role_arn() && obj.role_arn().empty()) {
        return "AWS role ARN cannot be empty";
    }

    if (has_static_credentials(obj) && has_aws_authentication(obj)) {
        return "access keys cannot be combined with AWS role or credential provider";
    }
    if (!has_aws_authentication(obj)) {
        return std::nullopt;
    }
    if (!obj.has_provider() || obj.provider() != ObjectStoreInfoPB::S3) {
        return "AWS role and credential provider require provider=S3";
    }
    if (obj.has_external_id() && obj.external_id().size() > 0 && obj.role_arn().empty()) {
        return "AWS external ID requires a role ARN";
    }
    return std::nullopt;
}

std::optional<std::string> validate_and_normalize_obj_credential(ObjectStoreInfoPB* obj) {
    if (auto error = validate_obj_credential(*obj); error.has_value()) {
        return error;
    }
    clear_legacy_credentials(obj);
    return std::nullopt;
}

std::optional<std::string> apply_obj_credential(const ObjectStoreInfoPB& update,
                                                ObjectStoreInfoPB* target) {
    if (!has_obj_credential(update)) {
        return "credential is not set";
    }
    if (has_static_credentials(update) || has_aws_authentication(update)) {
        return "credentials cannot be combined with other credentials";
    }

    // Merge into a copy first so a rejected alter never partially mutates target.
    ObjectStoreInfoPB candidate = *target;
    copy_obj_credential(update, &candidate);
    if (update.credential().has_gcp_credential()) {
        const auto& patch = update.credential().gcp_credential();
        if (!patch.has_credential_provider_type() && !patch.has_impersonation_service_account()) {
            return "GCP credential update must specify a provider type or service account";
        }
        if (!target->credential().has_gcp_credential() && !patch.has_credential_provider_type() &&
            patch.impersonation_service_account().empty()) {
            return "clearing impersonation requires an existing native GCP credential; "
                   "specify a provider type to replace HMAC credentials";
        }
        auto* credential = candidate.mutable_credential()->mutable_gcp_credential();
        if (target->credential().has_gcp_credential()) {
            credential->CopyFrom(target->credential().gcp_credential());
            credential->MergeFrom(patch);
        }
        // Only a new native credential needs a default; an existing source is
        // preserved when the patch changes just the impersonation target.
        if (!credential->has_credential_provider_type()) {
            credential->set_credential_provider_type(GcpCredentialPB::DEFAULT);
        }
        if (credential->impersonation_service_account().empty()) {
            credential->clear_impersonation_service_account();
        }
    }
    clear_legacy_credentials(&candidate);
    if (auto error = validate_obj_credential(candidate); error.has_value()) {
        return error;
    }

    clear_legacy_credentials(target);
    copy_obj_credential(candidate, target);
    return std::nullopt;
}

void copy_obj_credential(const ObjectStoreInfoPB& source, ObjectStoreInfoPB* target) {
    if (&source == target) {
        return;
    }
    target->clear_credential();
    if (source.has_credential()) {
        target->mutable_credential()->CopyFrom(source.credential());
    }
}

} // namespace doris::cloud
