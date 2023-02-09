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

#include <mutex>

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

// This class is thread-safe.(Except `set_xxx` method)
class S3FileSystem final : public RemoteFileSystem {
public:
    static std::shared_ptr<S3FileSystem> create(S3Conf s3_conf, std::string id);

    ~S3FileSystem() override;

    Status create_file(const Path& path, FileWriterPtr* writer) override;

    Status create_file_impl(const Path& path, FileWriterPtr* writer);

    Status open_file(const Path& path, FileReaderSPtr* reader, IOContext* io_ctx) override;

    Status open_file_impl(const Path& path, FileReaderSPtr* reader, IOContext* io_ctx);

    Status delete_file(const Path& path) override;

    Status delete_file_impl(const Path& path);

    Status create_directory(const Path& path) override;

    // Delete all objects start with path.
    Status delete_directory(const Path& path) override;

    Status delete_directory_impl(const Path& path);

    Status link_file(const Path& src, const Path& dest) override;

    Status exists(const Path& path, bool* res) const override;

    Status exists_impl(const Path& path, bool* res) const;

    Status file_size(const Path& path, size_t* file_size) const override;

    Status file_size_impl(const Path& path, size_t* file_size) const;

    Status list(const Path& path, std::vector<Path>* files) override;

    Status upload(const Path& local_path, const Path& dest_path) override;

    Status upload_impl(const Path& local_path, const Path& dest_path);

    Status batch_upload(const std::vector<Path>& local_paths,
                        const std::vector<Path>& dest_paths) override;

    Status batch_upload_impl(const std::vector<Path>& local_paths,
                             const std::vector<Path>& dest_paths);

    Status connect() override;

    Status connect_impl();

    std::shared_ptr<Aws::S3::S3Client> get_client() const {
        std::lock_guard lock(_client_mu);
        return _client;
    }

    // Guarded by external lock.
    void set_conf(S3Conf s3_conf) { _s3_conf = std::move(s3_conf); }

    std::string get_key(const Path& path) const;

private:
    S3FileSystem(S3Conf&& s3_conf, std::string&& id);

private:
    S3Conf _s3_conf;

    // FIXME(cyx): We can use std::atomic<std::shared_ptr> since c++20.
    std::shared_ptr<Aws::S3::S3Client> _client;
    mutable std::mutex _client_mu;

    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> _executor;
};

} // namespace io
} // namespace doris
