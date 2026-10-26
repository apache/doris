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
#include <utility>
#include <vector>

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace doris::cloud {

struct FileMeta {
    std::string path; // Relative path to accessor prefix
    int64_t size {0};
    int64_t mtime_s {0};
};

enum class AccessorType : uint8_t {
    MOCK,
    S3,
    HDFS,
};

class ListIterator {
public:
    virtual ~ListIterator() = default;
    virtual bool is_valid() = 0;
    virtual bool has_next() = 0;
    virtual std::optional<FileMeta> next() = 0;
};

// Where the objects of an accessor live: `container` is the S3 endpoint and bucket or the HDFS
// fs_name, `prefix` the directory in it without surrounding '/'. Credentials and client confs are
// not part of it, vaults on one bucket and prefix with different credentials share a root.
struct StorageRoot {
    std::string container;
    std::string prefix;

    bool operator==(const StorageRoot&) const = default;
};

class StorageVaultAccessor {
public:
    StorageVaultAccessor(AccessorType type, std::string location, StorageRoot root)
            : type_(type), location_(std::move(location)), root_(std::move(root)) {}

    virtual ~StorageVaultAccessor() = default;

    StorageVaultAccessor(const StorageVaultAccessor&) = delete;
    StorageVaultAccessor& operator=(const StorageVaultAccessor&) = delete;

    AccessorType type() const { return type_; }

    const std::string& uri() const { return uri_; }

    // The storage this accessor lists: equal locations see the same objects. It keeps the whole
    // configuration that may select a namespace, unnormalized, e.g. S3 credentials (a gateway may
    // map one bucket name per tenant) and HDFS confs in order. A false difference only costs a
    // second listing.
    const std::string& location() const { return location_; }

    const StorageRoot& root() const { return root_; }

    // If `expiration_time` > 0, only delete objects with mtime earlier than `expiration_time`.
    // returns 0 for success otherwise error
    virtual int delete_prefix(const std::string& path_prefix, int64_t expiration_time = 0) = 0;

    // returns 0 for success otherwise error
    virtual int delete_directory(const std::string& dir_path) = 0;

    // Delete all files in the storage vault.
    // If `expiration_time` > 0, only delete objects with mtime earlier than `expiration_time`.
    // returns 0 for success otherwise error
    virtual int delete_all(int64_t expiration_time = 0) = 0;

    // returns 0 for success otherwise error
    virtual int delete_files(const std::vector<std::string>& paths) = 0;

    // returns 0 for success otherwise error
    virtual int delete_file(const std::string& path) = 0;

    // List directory recursively
    // returns 0 for success (even if directory does not exist), otherwise error
    virtual int list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) = 0;

    // List all files in the storage vault.
    // returns 0 for success otherwise error
    virtual int list_all(std::unique_ptr<ListIterator>* res) = 0;

    // for test
    // returns 0 for success otherwise error
    virtual int put_file(const std::string& path, const std::string& content) = 0;

    // return 0 if file exists, 1 if file is not found, negative for error
    virtual int exists(const std::string& path) = 0;

    virtual int abort_multipart_upload(const std::string& path, const std::string& upload_id) = 0;

private:
    const AccessorType type_;
    const std::string location_;
    const StorageRoot root_;

protected:
    std::string uri_;
};

} // namespace doris::cloud
