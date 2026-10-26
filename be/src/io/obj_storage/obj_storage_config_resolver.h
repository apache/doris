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
    // the property rules (a region for every provider) and then validate_obj_client_config.
    static Result<ObjStorageConfig> from_properties(
            const std::map<std::string, std::string>& properties, const ObjStorageUri& location);

    // A cold-storage resource sent by FE.
    static ObjStorageConfig from_storage_param(const TS3StorageParam& param);

    // A storage vault in cloud mode, i.e. a Doris internal bucket.
    static ObjStorageConfig from_object_store_info(const cloud::ObjectStoreInfoPB& info);
};

} // namespace doris::io
