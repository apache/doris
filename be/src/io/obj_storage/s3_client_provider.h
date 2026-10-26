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
#include "io/obj_storage/obj_client_cache.h"
#include "io/obj_storage/obj_storage_config.h"

namespace doris::io {

// Builds, caches and reuses AWS SDK clients for S3 and S3-compatible vendors.
class S3ClientProvider {
public:
    // config.backend must hold s3, which is passed separately so the caller's dispatch
    // and this provider cannot disagree on the alternative.
    Result<std::shared_ptr<ObjStorageClient>> get_or_create(const ObjClientConfig& config,
                                                            const S3ClientConfig& s3);

private:
    Result<std::shared_ptr<ObjStorageClient>> _build(const ObjClientConfig& client_config,
                                                     const S3ClientConfig& s3);

    ObjClientCache _cache;
};

} // namespace doris::io
