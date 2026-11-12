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

#include <gen_cpp/cloud.pb.h>

#include <map>

#include "cpp/aws_common.h"
#include "cpp/obj-storage/obj_storage_config.h"

namespace doris {

using ObjStorageProperties = std::map<std::string, std::string>;

AwsCredentialImplementation aws_credential_implementation(std::string_view version);
void normalize_obj_storage_endpoint(ObjStorageClientConfig& client, std::string_view scheme);
cloud::ObjectStoreInfoPB::Provider storage_provider_from_endpoint(std::string_view endpoint);
cloud::ObjectStoreInfoPB::Provider vault_storage_provider(const cloud::ObjectStoreInfoPB& info);

// Accepts canonical wire keys. Protocol adapters own alias and enum conversion.
std::optional<std::string> parse_obj_storage_client_config(
        cloud::ObjectStoreInfoPB::Provider provider, const ObjStorageProperties& properties,
        AwsCredentialImplementation implementation, EmptyCredentialsBehavior empty_credentials,
        ObjStorageClientConfig* result);

// Both hosts pass plaintext credentials; the Recycler decrypts persisted keys first.
std::optional<std::string> parse_vault_storage_config(const cloud::ObjectStoreInfoPB& info,
                                                      AwsCredentialImplementation implementation,
                                                      EmptyCredentialsBehavior empty_credentials,
                                                      ObjStorageResourceConfig* result);

} // namespace doris
