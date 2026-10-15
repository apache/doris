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

#include <memory>

#include "common/status.h"
#include "io/obj_storage/azure_client_provider.h"
#include "io/obj_storage/obj_storage_config.h"
#include "io/obj_storage/s3_client_provider.h"

namespace doris::io {

// Entry for getting a BE native object storage client. It only dispatches on the backend
// alternative; validation, construction and caching belong to the selected provider.
class ObjClientFactory {
public:
    static ObjClientFactory& instance();

    Result<std::shared_ptr<ObjStorageClient>> get_or_create(const ObjClientConfig& config);

private:
    ObjClientFactory();

    S3ClientProvider _s3;
    AzureClientProvider _azure;
};

// Wraps a newly built client with the BE shared rate limiter when its usage requires it.
// Every provider calls this before publishing to its cache, so all backends follow one
// policy: in cloud mode only internal storage (storage vaults) is limited, otherwise all
// clients are.
std::shared_ptr<ObjStorageClient> apply_rate_limit_policy(std::shared_ptr<ObjStorageClient> client,
                                                          ClientUsage usage);

} // namespace doris::io
