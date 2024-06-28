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
#include <memory>
#include <string>
#include <vector>

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace doris::cloud {

struct ObjectMeta {
    std::string path; // Relative path to accessor prefix
    int64_t size {0};
    int64_t last_modify_second {0};
};

enum class AccessorType {
    S3,
    HDFS,
    AZURE,
};

// TODO(plat1ko): Redesign `Accessor` interface to adapt to storage vaults other than S3 style
class ObjStoreAccessor {
public:
    explicit ObjStoreAccessor(AccessorType type) : type_(type) {}
    virtual ~ObjStoreAccessor() = default;

    AccessorType type() const { return type_; }

    // root path
    virtual const std::string& path() const = 0;

    // returns 0 for success otherwise error
    virtual int init() = 0;

    // returns 0 for success otherwise error
    virtual int delete_objects_by_prefix(const std::string& relative_path) = 0;

    // returns 0 for success otherwise error
    virtual int delete_objects(const std::vector<std::string>& relative_paths) = 0;

    // returns 0 for success otherwise error
    virtual int delete_object(const std::string& relative_path) = 0;

    // for test
    // returns 0 for success otherwise error
    virtual int put_object(const std::string& relative_path, const std::string& content) = 0;

    // returns 0 for success otherwise error
    virtual int list(const std::string& relative_path, std::vector<ObjectMeta>* files) = 0;

    // return 0 if object exists, 1 if object is not found, negative for error
    virtual int exist(const std::string& relative_path) = 0;

private:
    const AccessorType type_;
};

struct ObjectStoragePathOptions {
    std::string bucket; // blob container in azure
    std::string key;    // blob name
    std::string prefix; // for batch delete and recursive delete
    std::string_view endpoint;
};

struct ObjectStorageDeleteExpiredOptions {
    ObjectStoragePathOptions path_opts;
    std::function<std::string(const std::string& path)> relative_path_factory;
};

struct ObjectCompleteMultiParts {};

struct ObjectStorageResponse {
    ObjectStorageResponse(int r = 0, std::string msg = "") : ret(r), error_msg(std::move(msg)) {}
    // clang-format off
    int ret {0}; // To unify the error handle logic with BE, we'd better use the same error code as BE
    // clang-format on
    std::string error_msg;
    static ObjectStorageResponse OK() { return {0}; }
};

// wrapper class owned by concret fs
class ObjStorageClient {
public:
    virtual ~ObjStorageClient() = default;
    // To directly upload a piece of data to object storage and generate a user-visible file.
    // You need to clearly specify the bucket and key
    virtual ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                             std::string_view stream) = 0;
    // According to the passed bucket and key, it will access whether the corresponding file exists in the object storage.
    // If it exists, it will return the corresponding file size
    virtual ObjectStorageResponse head_object(const ObjectStoragePathOptions& opts) = 0;
    // According to the passed bucket and prefix, it traverses and retrieves all files under the prefix, and returns the name and file size of all files.
    // **Attention**: The ObjectMeta contains the full key in object storage
    virtual ObjectStorageResponse list_objects(const ObjectStoragePathOptions& opts,
                                               std::vector<ObjectMeta>* files) = 0;
    // According to the bucket and prefix specified by the user, it performs batch deletion based on the object names in the object array.
    virtual ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                                 std::vector<std::string> objs) = 0;
    // Delete the file named key in the object storage bucket.
    virtual ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) = 0;
    // According to the prefix, recursively delete all files under the prefix.
    virtual ObjectStorageResponse delete_objects_recursively(
            const ObjectStoragePathOptions& opts) = 0;
    // Delete all the objects under the prefix which expires before the expired_time
    virtual ObjectStorageResponse delete_expired(const ObjectStorageDeleteExpiredOptions& opts,
                                                 int64_t expired_time) = 0;
    // Get the objects' expiration time on the bucket
    virtual ObjectStorageResponse get_life_cycle(const ObjectStoragePathOptions& opts,
                                                 int64_t* expiration_days) = 0;
    // Check if the objects' versioning is on or off
    virtual ObjectStorageResponse check_versioning(const ObjectStoragePathOptions& opts) = 0;

    virtual const std::shared_ptr<Aws::S3::S3Client>& s3_client() = 0;
};

} // namespace doris::cloud
