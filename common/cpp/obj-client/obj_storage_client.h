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

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace doris {
// Explicit values are stable; provider conversions must map by name.
enum class ObjStorageProvider : uint8_t {
    UNKNOWN = 0,
    AWS = 1,
    AZURE = 2,
    BOS = 3,
    COS = 4,
    OSS = 5,
    OBS = 6,
    GCP = 7,
    TOS = 8,
    S3EXPRESS = 9,
};

/// eg:
///     s3://bucket1/path/to/file.txt
/// path:   s3://bucket1/path/to/file.txt
/// bucket: bucket1
/// key:    path/to/file.txt
struct ObjStoragePath {
    std::filesystem::path path = "";
    std::string bucket {}; // blob container in azure
    std::string key {};    // blob name in azure
    std::string prefix {}; // for list and recursive delete
};

struct ObjStorageEndpointInfo {
    std::string endpoint {};
    std::string ak {};
    std::string sk {};
    std::string tls_debug_context {};
};

struct ObjectMeta {
    std::string key {};
    int64_t size {0};
    int64_t mtime_s {0};
};

struct ObjStorageCompletedPart {
    int part_num = 0;
    std::string etag {};
    std::optional<std::string> checksum_crc32c = std::nullopt;
};

struct ObjStorageStatus {
    enum Code : int {
        OK = TStatusCode::OK,
        INTERNAL_ERROR = TStatusCode::INTERNAL_ERROR,
        LIMIT_REACH = TStatusCode::LIMIT_REACH,
        NOT_FOUND = TStatusCode::NOT_FOUND,
        END_OF_FILE = TStatusCode::END_OF_FILE,
        IO_ERROR = TStatusCode::IO_ERROR,
        NETWORK_ERROR = TStatusCode::NETWORK_ERROR,
        // Keep the legacy BE error code for access-denied object storage responses.
        PERMISSION_DENIED = -256,
    };

    ObjStorageStatus(int r = OK, std::string msg = "") : code(r), msg(std::move(msg)) {}
    // clang-format off
    int code {OK}; // To unify the error handle logic with BE, we'd better use the same error code as BE
    // clang-format on
    std::string msg;
};

// We only store error code along with err_msg instead of Status to unify BE and recycler's error handle logic
struct ObjStorageResponse {
    ObjStorageStatus status {ObjStorageStatus::OK, ""};
    int http_code {200};
    std::string request_id {};
    static ObjStorageResponse OK() {
        // clang-format off
        return {
                .status = ObjStorageStatus{ObjStorageStatus::OK, ""},
                .http_code = 200,
                .request_id = ""
        };
        // clang-format on
    }

    static ObjStorageResponse rate_limit(int status_code, int http_code, std::string message) {
        return {
                .status = ObjStorageStatus {status_code, std::move(message)},
                .http_code = http_code,
        };
    }

    bool ok() const { return status.code == ObjStorageStatus::OK; }
};

// Convert a provider HTTP response code into the object-storage status domain. A non-positive
// response code means that no HTTP response was received.
ObjStorageStatus obj_storage_status_from_http_code(int http_code, std::string message);

struct ObjStorageUploadResult {
    ObjStorageResponse resp = ObjStorageResponse::OK();
    std::optional<std::string> upload_id = std::nullopt;
    std::optional<std::string> etag = std::nullopt;
    std::optional<std::string> checksum_crc32c = std::nullopt;
};

struct ObjStorageHeadResult {
    ObjStorageResponse resp = ObjStorageResponse::OK();
    long long file_size {0};
};

struct ObjStorageListResult {
    ObjStorageResponse resp = ObjStorageResponse::OK();
    std::optional<ObjectMeta> object = std::nullopt;
};

struct ObjStorageListPageResult {
    ObjStorageResponse resp = ObjStorageResponse::OK();
    std::vector<ObjectMeta> objects {};
    std::string continuation_token {};
    bool has_more = false;
};

struct ObjStorageCapabilities {
    size_t max_delete_batch = 1;
    size_t max_list_page = 1000;
};

using ObjStorageDeleteTask = std::function<ObjStorageResponse()>;

// Implementations must enqueue tasks immediately and remain reusable after wait() completes.
// Recycler implements this in cloud/src/recycler/s3_accessor.cpp; BE uses the synchronous fallback.
class ObjStorageDeleteExecutor {
public:
    virtual ~ObjStorageDeleteExecutor() = default;
    virtual ObjStorageResponse submit(ObjStorageDeleteTask task) = 0;
    virtual ObjStorageResponse wait() = 0;
};

struct ObjStorageRecursiveDeleteOptions {
    int64_t expiration_time = 0;
    size_t max_tasks_per_batch = 1;
    std::shared_ptr<ObjStorageDeleteExecutor> executor {};
};

class ObjStorageListIterator;
class RateLimitedObjStorageClient;

// Provider implementations are in common/cpp/obj-client/s3_obj_storage_client.cpp and
// common/cpp/obj-client/azure_obj_storage_client.cpp.
// Clients are shared-owned so a lazy list iterator can keep the complete decorator chain alive.
class ObjStorageClient : public std::enable_shared_from_this<ObjStorageClient> {
public:
    ObjStorageClient() = default;
    virtual ~ObjStorageClient() = default;

    ObjStorageClient(const ObjStorageClient&) = delete;
    ObjStorageClient& operator=(const ObjStorageClient&) = delete;
    // Create a multipart upload request. The returned token may be provider-issued or local and
    // identifies this writer's parts.
    // The input parameters should include the bucket and key for the object storage.
    virtual ObjStorageUploadResult create_multipart_upload(const ObjStoragePath& opts) = 0;
    // To directly upload a piece of data to object storage and generate a user-visible file.
    // You need to clearly specify the bucket and key
    virtual ObjStorageResponse put_object(const ObjStoragePath& opts, std::string_view stream) = 0;
    // Upload one part of a large object without making the object visible to users. upload_id is
    // the provider-issued or local writer token returned by create_multipart_upload.
    // Reusing the same bucket, key, upload_id, and part_num replaces that writer's staged part.
    virtual ObjStorageUploadResult upload_part(const ObjStoragePath& opts,
                                               const std::string& upload_id,
                                               std::string_view stream, int part_num) = 0;
    // Combine the parts belonging to upload_id into the key passed in opts. After this succeeds,
    // the complete object is visible in object storage.
    virtual ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath& opts, const std::string& upload_id,
            const std::vector<ObjStorageCompletedPart>& completed_parts) = 0;
    // According to the passed bucket and key, it will access whether the corresponding file exists in the object storage.
    // If it exists, it will return the corresponding file size
    virtual ObjStorageHeadResult head_object(const ObjStoragePath& opts) = 0;
    // According to the bucket and key, it finds the corresponding file in the object storage
    // and starting from the offset, it reads bytes_read bytes into the buffer, with size_return recording the actual number of bytes read
    virtual ObjStorageResponse get_object(const ObjStoragePath& opts, void* buffer, size_t offset,
                                          size_t bytes_read, size_t* size_return) = 0;
    // Return a lazy iterator that fetches one provider page at a time.
    virtual std::unique_ptr<ObjStorageListIterator> list_objects(const ObjStoragePath& opts);
    // Collect all objects by consuming the lazy iterator. This preserves the eager BE API.
    // **Notice**: The files returned by this function contain the full key in object storage.
    virtual ObjStorageResponse list_objects(const ObjStoragePath& opts,
                                            std::vector<ObjectMeta>* objects);

    // According to the bucket and prefix specified by the user, it performs batch deletion based on the object names in the object array.
    virtual ObjStorageResponse delete_objects(const ObjStoragePath& opts,
                                              std::vector<std::string> objs) = 0;
    // Delete the file named key in the object storage bucket.
    virtual ObjStorageResponse delete_object(const ObjStoragePath& opts) = 0;
    virtual ObjStorageCapabilities capabilities() const = 0;
    // Return a presigned URL for users to access the object
    virtual std::string generate_presigned_url(const ObjStoragePath& opts,
                                               int64_t expiration_secs) = 0;

    // Get the objects' expiration time on the bucket
    virtual ObjStorageResponse get_lifecycle(const std::string& bucket,
                                             int64_t* expiration_days) = 0;

    // Check if the objects' versioning is on or off
    // returns 0 when versioning is on, otherwise versioning is off or check failed
    virtual ObjStorageResponse check_versioning(const std::string& bucket) = 0;

    virtual ObjStorageResponse abort_multipart_upload(const ObjStoragePath& path,
                                                      const std::string& upload_id) = 0;

protected:
    // Fetch at most one page. One call corresponds to exactly one provider request.
    virtual ObjStorageListPageResult list_objects_page(const ObjStoragePath& opts,
                                                       std::string_view continuation_token) = 0;

private:
    friend class ObjStorageListIterator;
    friend class RateLimitedObjStorageClient;
};

// A client-side iterator returned by ObjStorageClient. It requests one fixed-size page at a time.
class ObjStorageListIterator {
public:
    ObjStorageListIterator(std::shared_ptr<ObjStorageClient> client, ObjStoragePath opts)
            : client_(std::move(client)), opts_(std::move(opts)) {}

    bool is_valid() const { return is_valid_; }
    ObjStorageResponse has_next();
    ObjStorageListResult next();

private:
    std::shared_ptr<ObjStorageClient> client_;
    ObjStoragePath opts_;
    std::vector<ObjectMeta> objects_;
    size_t next_index_ = 0;
    std::string continuation_token_;
    bool has_more_ = true;
    bool is_valid_ = true;
};

// Provider-independent recursive deletion shared by concrete clients. Passing the complete client
// decorator chain keeps it alive for asynchronous delete tasks and applies policy per list page and
// delete batch.
ObjStorageResponse delete_objects_recursively(std::shared_ptr<ObjStorageClient> client,
                                              const ObjStoragePath& path,
                                              const ObjStorageRecursiveDeleteOptions& options = {});
} // namespace doris

// Keep the BE namespace spelling source-compatible while the implementation is
// shared with Recycler in `doris`.
namespace doris::io {
using ::doris::ObjStorageCapabilities;
using ::doris::ObjStorageClient;
using ::doris::ObjStorageCompletedPart;
using ::doris::ObjStorageDeleteExecutor;
using ::doris::ObjStorageDeleteTask;
using ::doris::ObjStorageEndpointInfo;
using ::doris::ObjStorageHeadResult;
using ::doris::ObjStorageListIterator;
using ::doris::ObjStorageListPageResult;
using ::doris::ObjStorageListResult;
using ::doris::ObjStoragePath;
using ::doris::ObjStorageProvider;
using ::doris::ObjStorageRecursiveDeleteOptions;
using ::doris::ObjStorageResponse;
using ::doris::ObjStorageStatus;
using ::doris::ObjStorageUploadResult;
using ::doris::ObjectMeta;
using ::doris::delete_objects_recursively;
} // namespace doris::io
