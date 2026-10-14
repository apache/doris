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
#include "io/obj_storage/obj_storage_client_cache.h"
#include "io/obj_storage/obj_storage_config.h"

namespace doris::io {

// Builds, caches and reuses Azure Blob SDK clients. The header is SDK-free so that the
// factory compiles with BUILD_AZURE off; _build reports NotSupported in that case.
class AzureClientProvider {
public:
    // config.backend must hold azure, which is passed separately so the caller's
    // dispatch and this provider cannot disagree on the alternative.
    Result<std::shared_ptr<ObjStorageClient>> get_or_create(const ObjStorageClientConfig& config,
                                                            const AzureClientConfig& azure);

private:
    Result<std::shared_ptr<ObjStorageClient>> _build(const AzureClientConfig& azure);

    ObjStorageClientCache _cache;
};

} // namespace doris::io
