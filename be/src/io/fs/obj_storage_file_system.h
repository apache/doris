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

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "io/object_storage/obj_storage_client_holder.h"

namespace doris::io {

// Adapts a bucket and prefix to the RemoteFileSystem interface. Readers and writers
// share the active client so a storage resource can refresh its credentials.
class ObjStorageFileSystem final : public RemoteFileSystem {
public:
    static Result<std::shared_ptr<ObjStorageFileSystem>> create(ObjStorageConfig storage_config,
                                                                std::string id);

    ~ObjStorageFileSystem() override;

    const std::shared_ptr<ObjStorageClientHolder>& client_holder() const { return _client; }

    bool same_target(const ObjStorageConfig& config) const;

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
    ObjStorageFileSystem(ObjStorageConfig storage_config, std::string id);

    Status init();

    // For error msg
    std::string full_object_path(std::string_view key) const;

    std::string _bucket;
    std::string _prefix;
    std::shared_ptr<ObjStorageClientHolder> _client;
};

} // namespace doris::io
