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
#include <string>
#include <vector>

namespace doris::cloud {

struct ObjectMeta {
    std::string path; // Relative path to accessor prefix
    int64_t size {0};
    int64_t last_modify_second {0};
};

enum class AccessorType {
    S3,
    HDFS,
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
    ObjectStorageResponse(int r) : ret(r) {}
    int ret {0};
};

// wrapper class owned by concret fs
class ObjStorageClient {
public:
    virtual ~ObjStorageClient() = default;
    virtual ObjectStorageResponse PutObject(const ObjectStoragePathOptions& opts,
                                            std::string_view stream) = 0;
    virtual ObjectStorageResponse HeadObject(const ObjectStoragePathOptions& opts) = 0;
    virtual ObjectStorageResponse ListObjects(const ObjectStoragePathOptions& opts,
                                              std::vector<ObjectMeta>* files) = 0;
    virtual ObjectStorageResponse DeleteObjects(const ObjectStoragePathOptions& opts,
                                                std::vector<std::string> objs) = 0;
    virtual ObjectStorageResponse DeleteObject(const ObjectStoragePathOptions& opts) = 0;
    virtual ObjectStorageResponse RecursiveDelete(const ObjectStoragePathOptions& opts) = 0;
    virtual ObjectStorageResponse DeleteExpired(const ObjectStorageDeleteExpiredOptions& opts,
                                                int64_t expired_time) = 0;
    virtual ObjectStorageResponse GetLifeCycle(const ObjectStoragePathOptions& opts,
                                               int64_t* expiration_days) = 0;

    virtual ObjectStorageResponse CheckVersioning(const ObjectStoragePathOptions& opts) = 0;
};

} // namespace doris::cloud
