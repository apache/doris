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

#include <string_view>

#include "common/status.h"
#include "io/obj_storage/obj_storage_config.h"

namespace doris {
struct S3ClientConf;
class TAzureClientConfig;
} // namespace doris

namespace doris::io {

// The single place that turns the legacy flat S3ClientConf into the typed client
// configuration. Every input source (FE properties, TS3StorageParam, ObjectStoreInfoPB)
// still produces an S3ClientConf today, so they all reach the client providers through
// this function and share one mapping.
class ObjStorageConfigResolver {
public:
    // The caller must have validated conf with the legacy rules (S3ClientFactory::create
    // does). The mapping itself cannot fail: every legacy field either belongs to the
    // selected backend or was never used by it.
    //
    // Example: provider=AZURE, ak="acct", sk="key", bucket="c1", region="r" becomes
    // AzureClientConfig{account="acct", container="c1", auth=SharedKey{"key"}}; region,
    // token and role_arn are dropped because the Azure client never read them.
    static ObjClientConfig from_client_conf(const S3ClientConf& conf);

    // Typed Azure input sent by FE. Unlike the legacy conf it can carry SAS and client secret
    // credentials. location_container is the container of the file being accessed; see
    // AzureConfigCodec::decode. The result is validated, so a missing or conflicting field
    // fails here instead of at the first request.
    static Result<ObjClientConfig> from_azure_client_config(const TAzureClientConfig& config,
                                                            std::string_view location_container);
};

} // namespace doris::io
