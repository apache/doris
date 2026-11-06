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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "io/obj_storage/obj_client_holder.h"
#include "io/obj_storage/obj_storage_config.h"
#include "util/s3_util.h"

namespace doris::io {

// File system for object storage (S3 and S3-compatible stores, Azure Blob), bound to the bucket
// (Azure: container) and root prefix of one ObjStorageConfig. Backend differences live in the
// client the holder provides. It accepts 2 kinds of path:
//  1. Full path, e.g. s3://bucket/path/to/file.txt (ObjStorageUri::parse_s3).
//     In this case, the root prefix is not used.
//  2. only key: path/to/file.txt
//      In this case, the final key will be "prefix + path/to/file.txt"
class S3FileSystem final : public RemoteFileSystem {
public:
    static Result<std::shared_ptr<S3FileSystem>> create(ObjStorageConfig config, std::string id);

    // Applies a refreshed configuration of the same storage resource to `fs`:
    //  - The same backend is reset in place (ObjClientHolder::reset), and `fs` is returned.
    //  - Another backend with the same bucket and prefix rebinds the holder (ObjClientHolder::
    //    rebind), and `fs` is returned. A rowset resolves its file system once and keeps it, so
    //    only the existing holder reaches every rowset, including after the change is reverted.
    //  - Another backend with another bucket or prefix builds a new file system with the same
    //    id, as the next BE start would build it. FE freezes both under a storage policy, so
    //    this only happens to a resource that no storage policy references.
    // FE allows a change of backend through ALTER RESOURCE, but it is not a supported way to move
    // a resource; this only keeps a BE from silently staying on the old backend.
    static Result<std::shared_ptr<S3FileSystem>> refresh_resource(std::shared_ptr<S3FileSystem> fs,
                                                                  ObjStorageConfig config);

    // Applies a refreshed configuration of the storage vault this file system serves. The backend
    // of a vault cannot be altered, so another backend means that the meta service handed the id
    // of a dropped vault to a new one. Everything that keeps this file system would then write
    // into the dropped vault's bucket, so its holder is invalidated (ObjClientHolder::invalidate)
    // and the error is returned; a BE restart builds the new vault's file system.
    Status refresh_vault(const ObjClientConfig& config);

    ~S3FileSystem() override;

    const std::shared_ptr<ObjClientHolder>& client_holder() const { return _client; }

    const std::string& bucket() const { return _bucket; }
    const std::string& prefix() const { return _prefix; }

    std::string generate_presigned_url(const Path& path, int64_t expiration_secs,
                                       bool is_public_endpoint) const;

protected:
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;
    Status open_file_internal(const Path& file, FileReaderSPtr* reader,
                              const FileReaderOptions& opts) override;
    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override;
    Status delete_file_impl(const Path& file) override;
    Status delete_directory_impl(const Path& dir) override;
    Status batch_delete_impl(const std::vector<Path>& files) override;
    Status exists_impl(const Path& path, bool* res) const override;
    Status file_size_impl(const Path& file, int64_t* file_size) const override;
    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override;
    Status rename_impl(const Path& orig_name, const Path& new_name) override;

    Status upload_impl(const Path& local_file, const Path& remote_file) override;
    Status batch_upload_impl(const std::vector<Path>& local_files,
                             const std::vector<Path>& remote_files) override;
    Status download_impl(const Path& remote_file, const Path& local_file) override;

    Status absolute_path(const Path& path, Path& abs_path) const override {
        if (path.string().find("://") != std::string::npos) {
            // the path is with schema, which means this is a full path like:
            // s3://bucket/path/to/file.txt
            // so no need to concat with prefix
            abs_path = path;
        } else {
            // path with no schema
            abs_path = _prefix / path;
        }
        return Status::OK();
    }

private:
    S3FileSystem(ObjStorageConfig config, std::string id);

    Status init();

    Result<std::string> _get_key(const Path& full_path) const;

    // For error msg
    std::string full_s3_path(std::string_view key) const;

    std::string _bucket;
    std::string _prefix;
    std::shared_ptr<ObjClientHolder> _client;
};

} // namespace doris::io
