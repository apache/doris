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
#include <shared_mutex>
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

namespace doris::io {
class ObjStorageClient;
// In runtime, AK and SK may be modified, and the original `S3Client` instance will be replaced.
// The `S3FileReader` cached by the `Segment` must hold a shared `ObjClientHolder` in order to
// access S3 data with latest AK SK.
class ObjClientHolder {
public:
    explicit ObjClientHolder(S3ClientConf conf);
    ~ObjClientHolder();

    Status init();

    // Update s3 conf and reset client if `conf` is different. This method is threadsafe.
    Status reset(const S3ClientConf& conf);

    std::shared_ptr<ObjStorageClient> get() const {
        std::shared_lock lock(_mtx);
        return _client;
    }

    Result<int64_t> object_file_size(const std::string& bucket, const std::string& key) const;

    // For error msg
    std::string full_s3_path(std::string_view bucket, std::string_view key) const;

    const S3ClientConf& s3_client_conf() { return _conf; }

private:
    mutable std::shared_mutex _mtx;
    std::shared_ptr<ObjStorageClient> _client;
    S3ClientConf _conf;
};

// File system for S3 compatible object storage
// When creating S3FileSystem, all required info should be set in S3Conf,
// such as ak, sk, region, endpoint, bucket.
// And the root_path of S3FileSystem is s3_conf.prefix.
// When using S3FileSystem, it accepts 2 kinds of path:
//  1. Full path: s3://bucket/path/to/file.txt
//      In this case, the root_path is not used.
//  2. only key: path/to/file.txt
//      In this case, the final key will be "prefix + path/to/file.txt"
class S3FileSystem final : public RemoteFileSystem {
public:
    static Result<std::shared_ptr<S3FileSystem>> create(S3Conf s3_conf, std::string id);

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
    S3FileSystem(S3Conf s3_conf, std::string id);

    Status init();

    // For error msg
    std::string full_s3_path(std::string_view key) const;

    std::string _bucket;
    std::string _prefix;
    std::shared_ptr<ObjClientHolder> _client;
};

} // namespace doris::io
