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
#include <optional>
#include <string>
#include <vector>

namespace doris::cloud {

struct ObjectStoragePathRef {
    const std::string& bucket;
    const std::string& key;
};

struct ObjectStorageResponse {
    ObjectStorageResponse(int r = 0, std::string msg = "") : ret(r), error_msg(std::move(msg)) {}
    // clang-format off
    int ret {0}; // To unify the error handle logic with BE, we'd better use the same error code as BE
    // clang-format on
    std::string error_msg;
};

struct ObjectMeta {
    std::string key;
    int64_t size {0};
    int64_t mtime_s {0};
};

class ObjectListIterator {
public:
    virtual ~ObjectListIterator() = default;
    virtual bool is_valid() = 0;
    virtual bool has_next() = 0;
    virtual std::optional<ObjectMeta> next() = 0;
};

class ObjStorageClient {
public:
    ObjStorageClient() = default;
    virtual ~ObjStorageClient() = default;

    ObjStorageClient(const ObjStorageClient&) = delete;
    ObjStorageClient& operator=(const ObjStorageClient&) = delete;

    virtual ObjectStorageResponse put_object(ObjectStoragePathRef path,
                                             std::string_view stream) = 0;

    // If it exists, it will return the corresponding object meta
    virtual ObjectStorageResponse head_object(ObjectStoragePathRef path, ObjectMeta* res) = 0;

    // According to the passed bucket and prefix, it traverses and retrieves all files under the prefix, and returns the name and file size of all files.
    // **Attention**: The ObjectMeta contains the full key in object storage
    virtual std::unique_ptr<ObjectListIterator> list_objects(ObjectStoragePathRef path) = 0;

    // According to the bucket and prefix specified by the user, it performs batch deletion based on the object names in the object array.
    virtual ObjectStorageResponse delete_objects(const std::string& bucket,
                                                 std::vector<std::string> keys) = 0;

    // Delete the file named key in the object storage bucket.
    virtual ObjectStorageResponse delete_object(ObjectStoragePathRef path) = 0;

    // According to the prefix, recursively delete all objects under the prefix.
    // If `expiration_time` > 0, only delete objects with mtime earlier than `expiration_time`.
    virtual ObjectStorageResponse delete_objects_recursively(ObjectStoragePathRef path,
                                                             int64_t expiration_time = 0) = 0;

    // Get the objects' expiration time on the bucket
    virtual ObjectStorageResponse get_life_cycle(const std::string& bucket,
                                                 int64_t* expiration_days) = 0;

    // Check if the objects' versioning is on or off
    // returns 0 when versioning is on, otherwise versioning is off or check failed
    virtual ObjectStorageResponse check_versioning(const std::string& bucket) = 0;

protected:
    ObjectStorageResponse delete_objects_recursively_(ObjectStoragePathRef path,
                                                      int64_t expiration_time, size_t batch_size);
};

} // namespace doris::cloud
