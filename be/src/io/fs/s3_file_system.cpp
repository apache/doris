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

#include "io/fs/s3_file_system.h"

#include <fmt/format.h>
#include <stddef.h>

#include <algorithm>

#include "common/compiler_util.h" // IWYU pragma: keep
// IWYU pragma: no_include <bits/chrono.h>
#include <aws/core/utils/threading/Executor.h>

#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <fstream> // IWYU pragma: keep
#include <future>
#include <memory>
#include <sstream>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_common.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_writer.h"
#include "io/fs/s3_obj_storage_client.h"
#include "util/bvar_helper.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {
#ifndef CHECK_S3_CLIENT
#define CHECK_S3_CLIENT(client)                               \
    if (!client) {                                            \
        return Status::InternalError("init s3 client error"); \
    }
#endif

Result<std::string> get_key(const Path& full_path) {
    // FIXME(plat1ko): Check bucket in full path and support relative path
    S3URI uri(full_path.native());
    RETURN_IF_ERROR_RESULT(uri.parse());
    return uri.get_key();
}

// TODO(plat1ko): AwsTransferManager will be deprecated
std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor>& default_executor() {
    static auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
            "default", config::s3_transfer_executor_pool_size);
    return executor;
}

} // namespace

ObjClientHolder::ObjClientHolder(S3ClientConf conf) : _conf(std::move(conf)) {}

ObjClientHolder::~ObjClientHolder() = default;

Status ObjClientHolder::init() {
    auto client = S3ClientFactory::instance().create(_conf);
    if (!client) {
        return Status::InternalError("failed to init s3 client with conf {}", _conf.to_string());
    }

    _client = std::make_shared<S3ObjStorageClient>(std::move(client));
    return Status::OK();
}

Status ObjClientHolder::reset(const S3ClientConf& conf) {
    S3ClientConf reset_conf;
    {
        std::shared_lock lock(_mtx);
        if (conf.ak == _conf.ak && conf.sk == _conf.sk && conf.token == _conf.token) {
            return Status::OK(); // Same conf
        }

        reset_conf = _conf;
        reset_conf.ak = conf.ak;
        reset_conf.sk = conf.sk;
        reset_conf.token = conf.token;
        // Should check endpoint here?
    }

    auto client = S3ClientFactory::instance().create(reset_conf);
    if (!client) {
        return Status::InternalError("failed to init s3 client with conf {}", conf.to_string());
    }

    LOG(INFO) << "reset s3 client with new conf: " << conf.to_string();

    {
        std::lock_guard lock(_mtx);
        _client = std::make_shared<S3ObjStorageClient>(std::move(client));
        _conf = std::move(reset_conf);
    }

    return Status::OK();
}

Result<int64_t> ObjClientHolder::object_file_size(const std::string& bucket,
                                                  const std::string& key) const {
    auto client = get();
    if (!client) {
        return ResultError(Status::InternalError("init s3 client error"));
    }

    auto resp = client->head_object({
            .bucket = bucket,
            .key = key,
    });

    if (!resp.status.ok()) {
        return ResultError(
                s3fs_error(std::move(resp.status),
                           fmt::format("failed to head s3 file {}", full_s3_path(bucket, key))));
    }

    return resp.file_size;
}

std::string ObjClientHolder::full_s3_path(std::string_view bucket, std::string_view key) const {
    return fmt::format("{}/{}/{}", _conf.endpoint, bucket, key);
}

std::string S3FileSystem::full_s3_path(std::string_view key) const {
    return _client->full_s3_path(_bucket, key);
}

Result<std::shared_ptr<S3FileSystem>> S3FileSystem::create(S3Conf s3_conf, std::string id) {
    std::shared_ptr<S3FileSystem> fs(new S3FileSystem(std::move(s3_conf), std::move(id)));
    RETURN_IF_ERROR_RESULT(fs->init());
    return fs;
}

S3FileSystem::S3FileSystem(S3Conf s3_conf, std::string id)
        : RemoteFileSystem(s3_conf.prefix, std::move(id), FileSystemType::S3),
          _bucket(std::move(s3_conf.bucket)),
          _prefix(std::move(s3_conf.prefix)),
          _client(std::make_shared<ObjClientHolder>(std::move(s3_conf.client_conf))) {
    // FIXME(plat1ko): Normalize prefix
    // remove the first and last '/'
    if (!_prefix.empty()) {
        size_t start = _prefix.find_first_not_of('/');
        if (start == std::string::npos) {
            _prefix = "";
        } else {
            size_t end = _prefix.find_last_not_of('/');
            if (start > 0 || end < _prefix.size() - 1) {
                _prefix = _prefix.substr(start, end - start + 1);
            }
        }
    }
}

Status S3FileSystem::init() {
    return _client->init();
}

S3FileSystem::~S3FileSystem() = default;

Status S3FileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                      const FileWriterOptions* opts) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto key = DORIS_TRY(get_key(file));
    *writer = std::make_unique<S3FileWriter>(_client, _bucket, std::move(key), opts);
    return Status::OK();
}

Status S3FileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                        const FileReaderOptions& opts) {
    auto key = DORIS_TRY(get_key(file));
    *reader = DORIS_TRY(S3FileReader::create(_client, _bucket, key, opts.file_size));
    return Status::OK();
}

Status S3FileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    return Status::OK();
}

Status S3FileSystem::delete_file_impl(const Path& file) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    auto key = DORIS_TRY(get_key(file));

    auto resp = client->delete_object({.bucket = _bucket, .key = key});

    if (resp.status.ok() || resp.status.is<ErrorCode::NOT_FOUND>()) {
        return Status::OK();
    }
    return s3fs_error(std::move(resp.status),
                      fmt::format("failed to delete file {}", full_s3_path(key)));
}

Status S3FileSystem::delete_directory_impl(const Path& dir) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    auto prefix = DORIS_TRY(get_key(dir));
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    return client
            ->delete_objects_recursively({
                    .path = full_s3_path(prefix),
                    .bucket = _bucket,
                    .prefix = prefix,
            })
            .status;
}

Status S3FileSystem::batch_delete_impl(const std::vector<Path>& remote_files) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    constexpr size_t max_delete_batch = 1000;
    auto path_iter = remote_files.begin();

    do {
        std::vector<std::string> objects;
        auto path_begin = path_iter;
        for (; path_iter != remote_files.end() && (path_iter - path_begin < max_delete_batch);
             ++path_iter) {
            auto key = DORIS_TRY(get_key(*path_iter));
            objects.emplace_back(std::move(key));
        }
        if (objects.empty()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(client->delete_objects(
                                      {
                                              .bucket = _bucket,
                                      },
                                      std::move(objects))
                                .status);
    } while (path_iter != remote_files.end());

    return Status::OK();
}

Status S3FileSystem::exists_impl(const Path& path, bool* res) const {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto key = DORIS_TRY(get_key(path));

    auto resp = client->head_object({.bucket = _bucket, .key = key});

    if (resp.status.ok()) {
        *res = true;
    } else if (resp.status.is<ErrorCode::NOT_FOUND>()) {
        *res = false;
    } else {
        return s3fs_error(std::move(resp.status),
                          fmt::format("failed to check exists {}", full_s3_path(key)));
    }
    return Status::OK();
}

Status S3FileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    auto key = DORIS_TRY(get_key(file));
    *file_size = DORIS_TRY(_client->object_file_size(_bucket, key));
    return Status::OK();
}

Status S3FileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                               bool* exists) {
    // For object storage, this path is always not exist.
    // So we ignore this property and set exists to true.
    *exists = true;
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto prefix = DORIS_TRY(get_key(dir));
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    auto resp = client->list_objects(
            {
                    .bucket = _bucket,
                    .prefix = prefix,
            },
            files);

    return resp.status;
}

Status S3FileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    return Status::NotSupported("S3FileSystem::rename_impl");
}

Status S3FileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    auto key = DORIS_TRY(get_key(remote_file));
    auto start = std::chrono::steady_clock::now();
    FileWriterPtr obj_writer;
    RETURN_IF_ERROR(create_file_impl(key, &obj_writer, nullptr));
    FileReaderSPtr local_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(local_file, &local_reader));
    size_t local_buffer_size = config::s3_file_system_local_upload_buffer_size;
    std::unique_ptr<char[]> write_buffer = std::make_unique<char[]>(local_buffer_size);
    size_t cur_read = 0;
    while (cur_read < local_reader->size()) {
        size_t bytes_read = 0;
        RETURN_IF_ERROR(local_reader->read_at(
                cur_read, Slice {write_buffer.get(), local_buffer_size}, &bytes_read));
        RETURN_IF_ERROR(obj_writer->append({write_buffer.get(), bytes_read}));
        cur_read += bytes_read;
    }
    RETURN_IF_ERROR(obj_writer->close());
    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);

    auto size = local_reader->size();
    LOG(INFO) << "Upload " << local_file.native() << " to " << full_s3_path(key)
              << ", duration=" << duration.count() << ", bytes=" << size;

    return Status::OK();
}

Status S3FileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                       const std::vector<Path>& remote_files) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    if (local_files.size() != remote_files.size()) {
        return Status::InvalidArgument("local_files.size({}) != remote_files.size({})",
                                       local_files.size(), remote_files.size());
    }

    std::vector<FileWriterPtr> obj_writers(local_files.size());

    auto upload_task = [this](Path local_file, Path remote_file, FileWriterPtr* obj_writer) {
        auto key = DORIS_TRY(get_key(remote_file));
        LOG(INFO) << "Start to upload " << local_file.native() << " to " << full_s3_path(key);
        RETURN_IF_ERROR(create_file_impl(key, obj_writer, nullptr));
        FileReaderSPtr local_reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(local_file, &local_reader));
        size_t local_buffer_size = config::s3_file_system_local_upload_buffer_size;
        std::unique_ptr<char[]> write_buffer = std::make_unique<char[]>(local_buffer_size);
        size_t cur_read = 0;
        while (cur_read < local_reader->size()) {
            size_t bytes_read = 0;
            RETURN_IF_ERROR(local_reader->read_at(
                    cur_read, Slice {write_buffer.get(), local_buffer_size}, &bytes_read));
            RETURN_IF_ERROR((*obj_writer)->append({write_buffer.get(), bytes_read}));
            cur_read += bytes_read;
        }
        RETURN_IF_ERROR((*obj_writer)->close());
        return Status::OK();
    };

    std::vector<std::future<Status>> futures;
    for (int i = 0; i < local_files.size(); ++i) {
        std::shared_ptr<std::packaged_task<Status(Path local_file, Path remote_file,
                                                  FileWriterPtr * obj_writer)>>
                task = std::make_shared<std::packaged_task<Status(Path local_file, Path remote_file,
                                                                  FileWriterPtr * obj_writer)>>(
                        upload_task);
        futures.emplace_back(task->get_future());
        default_executor()->Submit(
                [t = std::move(task), local = local_files[i], remote = remote_files[i],
                 obj_writer = &obj_writers[i]]() mutable { (*t)(local, remote, obj_writer); });
    }
    Status s = Status::OK();
    for (auto&& f : futures) {
        auto cur_s = f.get();
        if (!cur_s.ok()) {
            s = std::move(cur_s);
        }
    }
    return s;
}

Status S3FileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto key = DORIS_TRY(get_key(remote_file));
    int64_t size;
    RETURN_IF_ERROR(file_size(remote_file, &size));
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
    size_t bytes_read = 0;
    auto resp = client->get_object(
            {
                    .bucket = _bucket,
                    .key = key,
            },
            buf.get(), 0, size, &bytes_read);
    if (!resp.status.ok()) {
        return resp.status;
    }
    Aws::OFStream local_file_s;
    local_file_s.open(local_file, std::ios::out | std::ios::binary);
    if (local_file_s.good()) {
        local_file_s << StringViewStream(buf.get(), size).rdbuf();
    } else {
        return Status::IOError("failed to download {}: failed to write file: {}",
                               remote_file.native(), local_file.native());
    }

    return Status::OK();
}

} // namespace doris::io
