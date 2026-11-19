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

#include <map>
#include <string>

#include "common/status.h"
#include "cpp/obj-storage/parser/obj_storage_config.h"

namespace Aws::Client {
struct ClientConfiguration;
}

namespace doris {

class TS3StorageParam;
namespace cloud {
class ObjectStoreInfoPB;
}

// Parse legacy inputs with BE credential defaults, convert parse errors to Status, and apply
// BE connection settings. Vault credentials must be decrypted by the metadata caller first.
Result<ObjStorageResourceConfig> parse_be_obj_storage_config(
        const std::map<std::string, std::string>& properties, std::string bucket);
Result<ObjStorageResourceConfig> parse_be_obj_storage_config(const TS3StorageParam& param);
Result<ObjStorageResourceConfig> parse_be_obj_storage_config(const cloud::ObjectStoreInfoPB& info);

// Apply BE defaults after parsing, before client validation and cache lookup.
void apply_be_obj_storage_defaults(ObjStorageClientConfig& client);

std::string be_ca_bundle_path();
Aws::Client::ClientConfiguration be_default_aws_client_configuration();

} // namespace doris
