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

#include "io/fs/obj_storage_client.h"

namespace doris::io {

// Decorator that applies the process-wide S3 GET/PUT QPS and bandwidth rate limiters
// in front of any ObjStorageClient. This is the single place where rate limiting is
// wired into the object storage path: provider clients (S3, Azure, future GCP, ...)
// contain no rate limiting code, and S3ClientFactory decides at construction time
// whether to wrap a client (internal storage-vault buckets) or return it bare
// (external buckets: S3 load, TVF, external catalogs in cloud mode).
//
// Each public API call is charged once against the QPS bucket, and data-carrying
// calls additionally reserve their payload size from the bytes bucket (reconciled
// with the actually transferred size for reads). Note that APIs which internally
// paginate (list_objects, delete_objects_recursively) are charged once per logical
// call, not once per underlying HTTP request.
class RateLimitedObjStorageClient final : public ObjStorageClient {
public:
    explicit RateLimitedObjStorageClient(std::shared_ptr<ObjStorageClient> inner)
            : _inner(std::move(inner)) {}
    ~RateLimitedObjStorageClient() override = default;

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
    std::shared_ptr<ObjStorageClient> _inner;
};

} // namespace doris::io
