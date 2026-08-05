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
/// path:   s3://bucket1/path/to/file.txt
/// bucket: bucket1
/// key:    path/to/file.txt
struct ObjectStoragePathOptions {
    std::filesystem::path path = "";
    std::string bucket {};                               // blob container in azure
    std::string key {};                                  // blob name in azure
    std::string prefix {};                               // for list and recursive delete
    std::optional<std::string> upload_id = std::nullopt; // only used for S3 upload
};

struct ObjectClientConfig {
    std::string endpoint {};
    std::string ak {};
    std::string sk {};
    std::string tls_debug_context {};
};

struct ObjectMeta {
    std::string file_path {};
    int64_t size {0};
    int64_t mtime_s {0};
};

struct ObjectCompleteMultiPart {
    int part_num = 0;
    std::string etag {};
};

struct ObjectStorageStatus {
    enum Code : int {
        UNDEFINED = -1,
        OK = TStatusCode::OK,
        NOT_FOUND = TStatusCode::NOT_FOUND,
        END_OF_FILE = TStatusCode::END_OF_FILE,
        RATE_LIMIT = TStatusCode::LIMIT_REACH,
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

    static ObjectStorageResponse rate_limit(std::string message) {
        return {
                .status = ObjectStorageStatus {ObjectStorageStatus::RATE_LIMIT, std::move(message)},
                .http_code = 429,
        };
    }

    bool ok() const { return status.code == ObjectStorageStatus::OK; }
};

enum class ObjStorageRequestType {
    GET,
    PUT,
};

inline constexpr int32_t OBJECT_LIST_PAGE_SIZE = 1000;

// One admission result for one object-storage backend request. `settle` is used by
// byte-aware limiters to refund a short read after the call completes.
struct ObjStorageRateLimitToken {
    ObjectStorageResponse resp = ObjectStorageResponse::OK();
    std::function<void(size_t)> settle {};

    void settle_bytes(size_t actual_bytes) const {
        if (settle) {
            settle(actual_bytes);
        }
    }
};

class ObjStorageRateLimitPolicy {
public:
    virtual ~ObjStorageRateLimitPolicy() = default;
    virtual ObjStorageRateLimitToken acquire(ObjStorageRequestType type,
                                             size_t estimated_bytes) const = 0;
};

struct ObjectStorageUploadResponse {
    ObjectStorageResponse resp = ObjectStorageResponse::OK();
    std::optional<std::string> upload_id = std::nullopt;
    std::optional<std::string> etag = std::nullopt;
};

struct ObjectStorageHeadResponse {
    ObjectStorageResponse resp = ObjectStorageResponse::OK();
    long long file_size {0};
};

struct ObjectStorageListResponse {
    ObjectStorageResponse resp = ObjectStorageResponse::OK();
    std::optional<ObjectMeta> results_ = std::nullopt;
};

struct ObjectStorageListPage {
    ObjectStorageResponse resp = ObjectStorageResponse::OK();
    std::vector<ObjectMeta> objects {};
    std::string continuation_token {};
    bool has_more = false;
};

struct ObjStorageCapabilities {
    size_t max_delete_batch = 1;
};

using ObjStorageDeleteTask = std::function<ObjectStorageResponse()>;

// A streaming executor for recursive deletion. submit() must enqueue the task immediately so the
// producer is subject to the executor's queue backpressure while it continues listing. wait()
// completes the current synchronization batch and prepares the executor for the next one.
struct ObjStorageDeleteExecutor {
    std::function<ObjectStorageResponse(ObjStorageDeleteTask)> submit {};
    std::function<ObjectStorageResponse()> wait {};

    explicit operator bool() const { return submit && wait; }
};

struct RecursiveDeleteOptions {
    int64_t expiration_time = 0;
    size_t max_tasks_per_batch = 1;
    ObjStorageDeleteExecutor executor {};
};

class ObjStorageBackend {
public:
    virtual ~ObjStorageBackend() = default;
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
    // Return at most one page of objects. One call corresponds to exactly one backend request.
    // **Notice**: The files returned by this function contain the full key in object storage.
    virtual ObjectStorageListPage list_objects(const ObjectStoragePathOptions& path,
                                               std::string_view continuation_token) = 0;

    // According to the bucket and prefix specified by the user, it performs batch deletion based on the object names in the object array.
    virtual ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                                 std::vector<std::string> objs) = 0;
    // Delete the file named key in the object storage bucket.
    virtual ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) = 0;
    virtual ObjStorageCapabilities capabilities() const { return {}; }
    // Return a presigned URL for users to access the object
    virtual std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                               int64_t expiration_secs) = 0;

    // Get the objects' expiration time on the bucket
    virtual ObjectStorageResponse get_life_cycle(const std::string& /*bucket*/,
                                                 int64_t* /*expiration_days*/) {
        return {.status = {TStatusCode::NOT_IMPLEMENTED_ERROR,
                           "object storage lifecycle is not supported"},
                .http_code = 0};
    }

    // Check if the objects' versioning is on or off
    // returns 0 when versioning is on, otherwise versioning is off or check failed
    virtual ObjectStorageResponse check_versioning(const std::string& /*bucket*/) {
        return {.status = {TStatusCode::NOT_IMPLEMENTED_ERROR,
                           "object storage versioning is not supported"},
                .http_code = 0};
    }

    virtual ObjectStorageResponse abort_multipart_upload(const ObjectStoragePathOptions& /*path*/,
                                                         const std::string& /*upload_id*/) {
        return {.status = {TStatusCode::NOT_IMPLEMENTED_ERROR,
                           "aborting multipart uploads is not supported"},
                .http_code = 0};
    }
};

// The only object-storage interface exposed to upper layers. It combines a backend implementation
// with an optional runtime policy, so backends cannot accidentally bypass rate limiting.
class ObjStorageClient final {
public:
    explicit ObjStorageClient(
            std::shared_ptr<ObjStorageBackend> backend,
            std::shared_ptr<const ObjStorageRateLimitPolicy> rate_limit_policy = nullptr)
            : backend_(std::move(backend)), rate_limit_policy_(std::move(rate_limit_policy)) {}

    ObjectStorageUploadResponse create_multipart_upload(const ObjectStoragePathOptions& opts);
    ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts, std::string_view stream);
    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions& opts,
                                            std::string_view stream, int part_num);
    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const std::vector<ObjectCompleteMultiPart>& completed_parts);
    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts);
    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read, size_t* size_return);
    ObjectStorageListPage list_objects(const ObjectStoragePathOptions& opts,
                                       std::string_view continuation_token = {});
    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                         std::vector<std::string> objs);
    ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts);
    ObjectStorageResponse delete_objects_recursively(
            const ObjectStoragePathOptions& opts,
            const RecursiveDeleteOptions& options = RecursiveDeleteOptions {});
    ObjStorageCapabilities capabilities() const { return backend_->capabilities(); }
    std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                       int64_t expiration_secs);
    ObjectStorageResponse get_life_cycle(const std::string& bucket, int64_t* expiration_days);
    ObjectStorageResponse check_versioning(const std::string& bucket);
    ObjectStorageResponse abort_multipart_upload(const ObjectStoragePathOptions& opts,
                                                 const std::string& upload_id);

private:
    ObjStorageRateLimitToken acquire(ObjStorageRequestType type, size_t estimated_bytes = 0) const;

    std::shared_ptr<ObjStorageBackend> backend_;
    std::shared_ptr<const ObjStorageRateLimitPolicy> rate_limit_policy_;
};

// A client-side iterator above ObjStorageClient. It requests one fixed-size page at a time, so
// every ObjStorageClient::list_objects call maps to one rate-limit admission and one SDK request.
class ObjectListIterator {
public:
    ObjectListIterator(std::shared_ptr<ObjStorageClient> client, ObjectStoragePathOptions opts)
            : client_(std::move(client)), opts_(std::move(opts)) {}

    bool is_valid() const { return is_valid_; }
    ObjectStorageResponse has_next();
    ObjectStorageListResponse next();

private:
    std::shared_ptr<ObjStorageClient> client_;
    ObjectStoragePathOptions opts_;
    std::vector<ObjectMeta> objects_;
    size_t next_index_ = 0;
    std::string continuation_token_;
    bool has_more_ = true;
    bool is_valid_ = true;
};
} // namespace doris

// Keep the BE namespace spelling source-compatible while the implementation is
// shared with Recycler in `doris`.
namespace doris::io {
using ::doris::ObjStorageCapabilities;
using ::doris::ObjStorageClient;
using ::doris::ObjStorageDeleteExecutor;
using ::doris::ObjStorageDeleteTask;
using ::doris::ObjStorageBackend;
using ::doris::ObjStorageRateLimitPolicy;
using ::doris::ObjStorageRateLimitToken;
using ::doris::ObjStorageRequestType;
using ::doris::ObjStorageType;
using ::doris::ObjectClientConfig;
using ::doris::ObjectCompleteMultiPart;
using ::doris::ObjectListIterator;
using ::doris::ObjectMeta;
using ::doris::ObjectStorageHeadResponse;
using ::doris::ObjectStorageListPage;
using ::doris::ObjectStorageListResponse;
using ::doris::ObjectStoragePathOptions;
using ::doris::ObjectStorageResponse;
using ::doris::ObjectStorageStatus;
using ::doris::ObjectStorageUploadResponse;
using ::doris::RecursiveDeleteOptions;
} // namespace doris::io
