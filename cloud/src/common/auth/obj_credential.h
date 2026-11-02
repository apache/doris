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

#include <optional>
#include <string>

namespace doris::cloud {

// This helper keeps provider-specific credential branches out of MetaService.
// Provider authentication implementations belong in common/cpp/obj-client/auth; this
// Cloud layer owns vault-field validation, compatibility cleanup, and protobuf
// persistence mutations.

class ObjectStoreInfoPB;

// Whether a provider-native credential envelope is present.
bool has_obj_credential(const ObjectStoreInfoPB& obj);

// TODO: Move AWS authentication fields into ObjectStoreInfoPB.credential so AWS
// follows the same validation and mutation path. Keep cred_provider_type,
// role_arn, and external_id in their current fields for now to preserve
// compatibility with stored vault metadata and existing clients. AK/SK remain
// the common static-credential representation for S3-compatible providers.

// Validates the active ObjectStoreInfoPB.credential case without modifying obj.
// Add a provider-specific validation case when extending the credential envelope.
std::optional<std::string> validate_obj_credential(const ObjectStoreInfoPB& obj);

// Validates the complete authentication state after create/alter mutations. In
// addition to provider-native credentials, this rejects cross-provider AWS
// authentication and mutually exclusive authentication modes.
std::optional<std::string> validate_obj_authentication(const ObjectStoreInfoPB& obj);

// Used by create paths: validates first, then removes common static credentials
// and provider-specific authentication fields that must not coexist with the
// selected provider-native credential.
std::optional<std::string> validate_and_normalize_obj_credential(ObjectStoreInfoPB* obj);

// Used by alter paths: merges explicitly present GCP fields into the stored
// credential, with an empty service account clearing impersonation. Validates
// the result against target's storage fields before changing authentication.
std::optional<std::string> apply_obj_credential(const ObjectStoreInfoPB& update,
                                                ObjectStoreInfoPB* target);

// Replaces only the credential envelope. An unset source clears target's credential;
// all non-credential fields are preserved.
void copy_obj_credential(const ObjectStoreInfoPB& source, ObjectStoreInfoPB* target);

} // namespace doris::cloud
