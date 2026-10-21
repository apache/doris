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
#include "io/obj_storage/obj_storage_config.h"

namespace doris {
class TAzureClientConfig;
class TS3StorageParam;
namespace cloud {
class ObjectStoreInfoPB;
} // namespace cloud
} // namespace doris

namespace doris::io {
struct ObjStorageUri;

// The single place that turns every input source into typed object storage configuration.
// After it, factories, holders, file systems and readers never read raw properties, Thrift or
// Protobuf parameters again.
//
// The flat sources (FE properties, TS3StorageParam, ObjectStoreInfoPB) share one mapping into
// the backend alternatives: provider AZURE becomes AzureClientConfig, which has always reused
// ak/sk/bucket as the SharedKey account/key and the container; every other provider becomes
// S3ClientConfig with that provider as its vendor. Fields the selected backend never read are
// dropped, e.g. an Azure source's region, token and role_arn.
class ObjStorageConfigResolver {
public:
    // FE properties of a FILE_S3 request (AWS_* keys; provider=azure selects Azure) for the
    // file at `location`, whose bucket becomes the binding's bucket. Validated like before:
    // endpoint and region are required, and ak/sk must come together unless a role is
    // assumed.
    static Result<ObjStorageConfig> from_properties(
            const std::map<std::string, std::string>& properties, const ObjStorageUri& location);

    // A cold-storage resource sent by FE.
    static ObjStorageConfig from_storage_param(const TS3StorageParam& param);

    // A storage vault in cloud mode, i.e. a Doris internal bucket.
    static ObjStorageConfig from_object_store_info(const cloud::ObjectStoreInfoPB& info);

    // Typed Azure input sent by FE. Unlike the flat sources it can carry SAS and client secret
    // credentials. location is the file being accessed: its container fills or must match the
    // configured one (see AzureConfigCodec::decode), and an account named by its host must
    // match the configured account, e.g. abfss://c1@other.dfs.core.windows.net/k with
    // account_name "acct" is rejected instead of sending acct's credentials for another
    // account's path. The result is validated, so a missing or conflicting field fails here
    // instead of at the first request.
    static Result<ObjClientConfig> from_azure_client_config(const TAzureClientConfig& config,
                                                            const ObjStorageUri& location);

    // Merges a refreshed resource or vault configuration into the one a holder uses. The
    // storage target stays: S3 keeps its vendor, endpoint, region and endpoint override, Azure
    // keeps its endpoint. Credentials, connection options, usage, S3 addressing and the Azure
    // account and container come from `update`, as the flat-field merge always did. A
    // different backend is a different storage target, which needs a new binding rather than
    // a reset, so it is rejected.
    static Result<ObjClientConfig> merge_update(const ObjClientConfig& current,
                                                const ObjClientConfig& update);
};

} // namespace doris::io
