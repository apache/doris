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

#include <stdint.h>

#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "util/s3_util.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3
namespace Aws::Utils::Threading {
class PooledThreadExecutor;
} // namespace Aws::Utils::Threading

namespace doris {
namespace io {
struct FileInfo;

// File system for S3 compatible object storage
// When creating S3FileSystem, all required info should be set in S3Conf,
// such as ak, sk, region, endpoint, bucket.
// And the root_path of S3FileSystem is s3_conf.prefix.
// When using S3FileSystem, it accepts 2 kinds of path:
//  1. Full path: s3://bucket/path/to/file.txt
//      In this case, the root_path is not used.
//  2. only key: path/to/file.txt
//      In this case, the final key will be "prefix + path/to/file.txt"
//
// This class is thread-safe.(Except `set_xxx` method)
class S3FileSystem final : public RemoteFileSystem {
public:
    static Status create(S3Conf s3_conf, std::string id, std::shared_ptr<S3FileSystem>* fs);
    ~S3FileSystem() override;
    // Guarded by external lock.
    void set_conf(S3Conf s3_conf) { _s3_conf = std::move(s3_conf); }

    std::shared_ptr<Aws::S3::S3Client> get_client() const {
        std::lock_guard lock(_client_mu);
        return _client;
    }

protected:
    Status connect_impl() override;
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;
    Status open_file_internal(const FileDescription& fd, const Path& abs_path,
                              FileReaderSPtr* reader) override;
    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override;
    Status delete_file_impl(const Path& file) override;
    Status delete_directory_impl(const Path& dir) override;
    Status batch_delete_impl(const std::vector<Path>& files) override;
    Status exists_impl(const Path& path, bool* res) const override;
    Status file_size_impl(const Path& file, int64_t* file_size) const override;
    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override;
    Status rename_impl(const Path& orig_name, const Path& new_name) override;
    Status rename_dir_impl(const Path& orig_name, const Path& new_name) override;

    Status upload_impl(const Path& local_file, const Path& remote_file) override;
    Status batch_upload_impl(const std::vector<Path>& local_files,
                             const std::vector<Path>& remote_files) override;
    Status direct_upload_impl(const Path& remote_file, const std::string& content) override;
    Status upload_with_checksum_impl(const Path& local_file, const Path& remote_file,
                                     const std::string& checksum) override;
    Status download_impl(const Path& remote_file, const Path& local_file) override;
    Status direct_download_impl(const Path& remote_file, std::string* content) override;

    Path absolute_path(const Path& path) const override {
        if (path.string().find("://") != std::string::npos) {
            // the path is with schema, which means this is a full path like:
            // s3://bucket/path/to/file.txt
            // so no need to concat with prefix
            return path;
        } else {
            // path with no schema
            return _root_path / path;
        }
    }

private:
    S3FileSystem(S3Conf&& s3_conf, std::string&& id);

    template <typename AwsOutcome>
    std::string error_msg(const std::string& key, const AwsOutcome& outcome) const;
    std::string error_msg(const std::string& key, const std::string& err) const;
    /// copy file from src to dst
    Status copy(const Path& src, const Path& dst);
    /// copy dir from src to dst
    Status copy_dir(const Path& src, const Path& dst);
    Status get_key(const Path& path, std::string* key) const;

private:
    S3Conf _s3_conf;
    // TODO(cyx): We can use std::atomic<std::shared_ptr> since c++20.
    mutable std::mutex _client_mu;
    std::shared_ptr<Aws::S3::S3Client> _client;
    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> _executor;
};

} // namespace io
} // namespace doris
