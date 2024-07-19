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
struct S3ClientConf;
namespace io {

// Names are in lexico order.
enum class ObjStorageType : uint8_t {
    UNKNOWN = 0,
    AWS = 1,
    AZURE,
    BOS,
    COS,
    OSS,
    OBS,
    GCP,
};

struct ObjectStoragePathOptions {
    Path path = "";
    std::string bucket = std::string();                  // blob container in azure
    std::string key = std::string();                     // blob name in azure
    std::string prefix = std::string();                  // for batch delete and recursive delete
    std::optional<std::string> upload_id = std::nullopt; // only used for S3 upload
};

struct ObjectCompleteMultiPart {
    int part_num = 0;
    std::string etag = std::string();
};

struct ObjectStorageStatus {
    int code = 0;
    std::string msg = std::string();
};

// We only store error code along with err_msg instead of Status to unify BE and recycler's error handle logic
struct ObjectStorageResponse {
    ObjectStorageStatus status {};
    int http_code {200};
    std::string request_id = std::string();
    static ObjectStorageResponse OK() {
        // clang-format off
        return {
                .status { .code = 0, },
                .http_code = 200,
        };
        // clang-format on
    }
};

struct ObjectStorageUploadResponse {
    ObjectStorageResponse resp {};
    std::optional<std::string> upload_id = std::nullopt;
    std::optional<std::string> etag = std::nullopt;
};

struct ObjectStorageHeadResponse {
    ObjectStorageResponse resp {};
    long long file_size {0};
};

class ObjStorageClient {
public:
    virtual ~ObjStorageClient() = default;
    // Create a multi-part upload request. On AWS-compatible systems, it will return an upload ID, but not on Azure.
    // The input parameters should include the bucket and key for the object storage.
    virtual ObjectStorageUploadResponse create_multipart_upload(
            const ObjectStoragePathOptions& opts) = 0;
    // To directly upload a piece of data to object storage and generate a user-visible file.
    // You need to clearly specify the bucket and key
    virtual ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                             std::string_view stream) = 0;
    // To upload a part of a large file to object storage as a temporary file, which is not visible to the user
    // The temporary file's ID is the value of the part_num passed in
    // You need to specify the bucket and key along with the upload_id if it's AWS-compatible system
    // For the same bucket and key, as well as the same part_num, it will directly replace the original temporary file.
    virtual ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions& opts,
                                                    std::string_view stream, int part_num) = 0;
    // To combine the previously uploaded multiple file parts into a complete file, the file name is the name of the key passed in.
    // If it is an AWS-compatible system, the upload_id needs to be included.
    // After a successful execution, the large file can be accessed in the object storage
    virtual ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const std::vector<ObjectCompleteMultiPart>& completed_parts) = 0;
    // According to the passed bucket and key, it will access whether the corresponding file exists in the object storage.
    // If it exists, it will return the corresponding file size
    virtual ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts) = 0;
    // According to the bucket and key, it finds the corresponding file in the object storage
    // and starting from the offset, it reads bytes_read bytes into the buffer, with size_return recording the actual number of bytes read
    virtual ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                             size_t offset, size_t bytes_read,
                                             size_t* size_return) = 0;
    // According to the passed bucket and prefix, it traverses and retrieves all files under the prefix, and returns the name and file size of all files.
    // **Notice**: The files returned by this function contains the full key in object storage.
    virtual ObjectStorageResponse list_objects(const ObjectStoragePathOptions& opts,
                                               std::vector<FileInfo>* files) = 0;
    // According to the bucket and prefix specified by the user, it performs batch deletion based on the object names in the object array.
    virtual ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                                 std::vector<std::string> objs) = 0;
    // Delete the file named key in the object storage bucket.
    virtual ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) = 0;
    // According to the prefix, recursively delete all files under the prefix.
    virtual ObjectStorageResponse delete_objects_recursively(
            const ObjectStoragePathOptions& opts) = 0;
    // Return a presigned URL for users to access the object
    virtual std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                               int64_t expiration_secs,
                                               const S3ClientConf& conf) = 0;
};
} // namespace io
} // namespace doris
