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

#include <optional>

#include "io/fs/file_system.h"
#include "io/fs/path.h"
namespace doris {
class Status;
namespace io {

enum class ObjStorageType : uint8_t {
    AZURE = 0,
    BOS,
    COS,
    OSS,
    OBS,
    GCP,
    S3,
};

struct ObjectStoragePathOptions {
    Path path;
    std::string bucket;                   // blob container in azure
    std::string key;                      // blob name
    std::string prefix;                   // for batch delete and recursive delete
    std::optional<std::string> upload_id; // only used for S3 upload
};

struct ObjectCompleteMultiParts {};

struct ObjectStorageResponse {
    Status status; // Azure的异常里面http的信息等都有，如果是S3的话则用s3fs_error
    std::optional<std::string> upload_id;
    std::optional<std::string> etag;
};

struct ObjectStorageHeadResponse {
    Status status;
    long long file_size {0};
};

// wrapper class owned by concret fs
class ObjStorageClient {
public:
    virtual ~ObjStorageClient() = default;
    virtual ObjectStorageResponse create_multipart_upload(const ObjectStoragePathOptions& opts) = 0;
    virtual ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                             std::string_view stream) = 0;
    virtual ObjectStorageResponse upload_part(const ObjectStoragePathOptions& opts,
                                              std::string_view stream, int partNum) = 0;
    virtual ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const ObjectCompleteMultiParts& completed_parts) = 0;
    virtual ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts) = 0;
    virtual ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                             size_t offset, size_t bytes_read,
                                             size_t* size_return) = 0;
    virtual ObjectStorageResponse list_objects(const ObjectStoragePathOptions& opts,
                                               std::vector<FileInfo>* files) = 0;
    virtual ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                                 std::vector<std::string> objs) = 0;
    virtual ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) = 0;
    virtual ObjectStorageResponse delete_objects_recursively(
            const ObjectStoragePathOptions& opts) = 0;
};
} // namespace io
} // namespace doris
