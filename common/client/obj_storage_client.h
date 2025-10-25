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

#include <gen_cpp/Status_types.h>

#include <filesystem>
#include <optional>
#include <string_view>
#include <vector>

namespace doris {
class Status;
struct S3ClientConf;

using Path = std::filesystem::path;

inline Path operator/(Path&& lhs, const Path& rhs) {
    return std::move(lhs /= rhs);
}

// Names are in lexico order.
enum class ObjStorageType : uint8_t {
    UNKNOWN = 0,
    AWS = 1,
    AZURE = 2,
    BOS = 3,
    COS = 4,
    OSS = 5,
    OBS = 6,
    GCP = 7,
    TOS = 8,
};

/// eg:
///     s3://bucket1/path/to/file.txt
/// _full_path: s3://bucket1/path/to/file.txt
/// _bucket: bucket1
/// _key:    path/to/file.txt
struct ObjectStoragePathOptions {
    Path full_path = "";
    std::string bucket;                                  // blob container in azure
    std::string key;                                     // blob name in azure
    std::optional<std::string> upload_id = std::nullopt; // only used for S3 upload
};

struct ObjectClientConfig {
    std::string endpoint;
    std::string ak;
    std::string sk;
};

struct ObjectMeta {
    std::string file_path;
    int64_t size {0};
    int64_t mtime_s {0};
};

struct ObjectCompleteMultiPart {
    int part_num = 0;
    std::string etag;
};

struct ObjectStorageStatus {
    enum Code : int {
        UNDEFINED = -1,
        OK = 0,
        NOT_FOUND = 1,
        RATE_LIMIT = 2,
    };

    ObjectStorageStatus(int r = OK, std::string msg = "") : code(r), msg(std::move(msg)) {}
    // clang-format off
    int code {OK}; // To unify the error handle logic with BE, we'd better use the same error code as BE
    // clang-format on
    std::string msg;
};

// We only store error code along with err_msg instead of Status to unify BE and recycler's error handle logic
struct ObjectStorageResponse {
    ObjectStorageStatus status {0, ""};
    int http_code {200};
    std::string request_id {};
    static ObjectStorageResponse OK() {
        // clang-format off
        return {
                .status = ObjectStorageStatus{0, ""},
                .http_code = 200,
                .request_id = ""
        };
        // clang-format on
    }
};

struct ObjectStorageRateLimitResponse : public ObjectStorageResponse {
    ObjectStorageRateLimitResponse() {
        status.code = ObjectStorageStatus::RATE_LIMIT;
        status.msg = "Rate limit exceeded";
        http_code = 429;
    }
};

struct ObjectStorageUploadResponse {
    ObjectStorageResponse resp;
    std::optional<std::string> upload_id = std::nullopt;
    std::optional<std::string> etag = std::nullopt;
};

struct ObjectStorageHeadResponse {
    ObjectStorageResponse resp;
    long long file_size {0};
};

struct ObjectStorageListResponse {
    ObjectStorageResponse resp;
    std::optional<ObjectMeta> results_ = std::nullopt;
};

class ObjectListIterator {
public:
    virtual ~ObjectListIterator() = default;
    virtual bool is_valid() { return is_valid_; }
    virtual ObjectStorageResponse has_next() = 0;
    virtual ObjectStorageListResponse next() {
        std::optional<ObjectMeta> res;
        auto resp = has_next();
        if (resp.status.code != TStatusCode::OK) {
            return ObjectStorageListResponse {.resp = std::move(resp), .results_ = {}};
        }

        // clang-format off
        if (results_.empty()) {
            return ObjectStorageListResponse {
                    .resp = ObjectStorageResponse {
                                .status = ObjectStorageStatus {TStatusCode::INTERNAL_ERROR,"results_ is empty"},
                                .http_code = 0,
                                .request_id = ""},
                    .results_ = {}};
        }
        // clang-format on

        res = std::move(results_.back());
        results_.pop_back();
        return ObjectStorageListResponse {.resp = ObjectStorageResponse::OK(),
                                          .results_ = {std::move(*res)}};
    }

protected:
    std::vector<ObjectMeta> results_;
    bool is_valid_ {true};
    bool has_more_ {true};
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
    virtual std::unique_ptr<ObjectListIterator> list_objects(
            const ObjectStoragePathOptions& path) = 0;

    // According to the bucket and prefix specified by the user, it performs batch deletion based on the object names in the object array.
    virtual ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                                 std::vector<std::string> objs) = 0;
    // Delete the file named key in the object storage bucket.
    virtual ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) = 0;
    // According to the prefix, recursively delete all files under the prefix.
    virtual ObjectStorageResponse delete_objects_recursively(const ObjectStoragePathOptions& opts,
                                                             const std::string& prefix) = 0;
    // Return a presigned URL for users to access the object
    virtual std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                               int64_t expiration_secs) = 0;

    // Get the objects' expiration time on the bucket
    virtual ObjectStorageResponse get_life_cycle(const std::string& endpoint,
                                                 const std::string& bucket,
                                                 int64_t* expiration_days) = 0;

    // Check if the objects' versioning is on or off
    // returns 0 when versioning is on, otherwise versioning is off or check failed
    virtual ObjectStorageResponse check_versioning(const std::string& endpoint_,
                                                   const std::string& bucket) = 0;

    virtual ObjectStorageResponse abort_multipart_upload(const ObjectStoragePathOptions& path,
                                                         const std::string& upload_id) = 0;
};
} // namespace doris
