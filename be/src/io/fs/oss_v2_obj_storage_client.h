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

#ifdef USE_OSS

#include <memory>
#include <string>
#include <vector>

#include "io/fs/obj_storage_client.h"

// Forward declare OSS SDK types to avoid heavy header in .h
namespace AlibabaCloud {
namespace OSS {
class OssClient;
} // namespace OSS
} // namespace AlibabaCloud

namespace doris::io {

// OSSv2ObjStorageClient implements ObjStorageClient using the native Alibaba Cloud OSS SDK.
//
// This is used for all non-cloud-vault OSS paths where previously the S3-compatible AWS SDK
// was used (cold/tiered storage, external tables, load/export, CREATE RESOURCE OSS).
// It is plugged into S3ClientFactory::create() when provider == ObjStorageType::OSS.
//
// Cloud vault OSS continues to use OSSFileSystem → OSSClientHolder directly (unchanged).
class OSSv2ObjStorageClient final : public ObjStorageClient {
public:
    OSSv2ObjStorageClient(std::shared_ptr<AlibabaCloud::OSS::OssClient> client, std::string bucket);
    ~OSSv2ObjStorageClient() override = default;

    ObjectStorageUploadResponse create_multipart_upload(
            const ObjectStoragePathOptions& opts) override;

    ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                     std::string_view stream) override;

    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions& opts,
                                            std::string_view stream, int part_num) override;

    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const std::vector<ObjectCompleteMultiPart>& completed_parts) override;

    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts) override;

    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read,
                                     size_t* size_return) override;

    ObjectStorageResponse list_objects(const ObjectStoragePathOptions& opts,
                                       std::vector<FileInfo>* files) override;

    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                         std::vector<std::string> objs) override;

    ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) override;

    ObjectStorageResponse delete_objects_recursively(const ObjectStoragePathOptions& opts) override;

    std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                       int64_t expiration_secs, const S3ClientConf& conf) override;

private:
    // Returns bucket: opts.bucket if set, else falls back to _bucket
    const std::string& resolve_bucket(const ObjectStoragePathOptions& opts) const {
        return opts.bucket.empty() ? _bucket : opts.bucket;
    }

    std::shared_ptr<AlibabaCloud::OSS::OssClient> _client;
    std::string _bucket;
};

} // namespace doris::io

#endif // USE_OSS
