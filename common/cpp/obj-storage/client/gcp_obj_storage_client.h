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

#include <functional>

#include "cpp/obj-storage/client/s3_compatible_obj_storage_client.h"

namespace doris {
struct GcpObjStorageConfig;
class GcpObjStorageClient final : public S3CompatibleObjStorageClient {
public:
    static ObjStorageClientBuildResult create(const GcpObjStorageConfig& config);
    using Signer = std::function<std::string(const ObjStoragePath&, int64_t)>;
    GcpObjStorageClient(std::shared_ptr<Aws::S3::S3Client> sdk,
                        ObjStorageEndpointInfo endpoint = {}, Signer signer = {})
            : S3CompatibleObjStorageClient(std::move(sdk), std::move(endpoint), false),
              _signer(std::move(signer)) {}
    std::string generate_presigned_url(const ObjStoragePath&, int64_t) override;

private:
    Signer _signer;
};

} // namespace doris
