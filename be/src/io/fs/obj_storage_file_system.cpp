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

#include "io/fs/obj_storage_file_system.h"

#include <fmt/format.h>

#include <cstddef>

#include "common/compiler_util.h" // IWYU pragma: keep
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <fstream> // IWYU pragma: keep
#include <future>
#include <memory>
#include <variant>

#include "common/check.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/obj-client/obj_storage_client.h"
#include "cpp/obj-client/s3_common.h"
#include "cpp/sync_point.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/obj_storage_file_reader.h"
#include "io/fs/obj_storage_file_writer.h"
#include "io/fs/remote_file_system.h"
#include "io/obj_storage/obj_client_factory.h"
#include "io/obj_storage/obj_storage_uri.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {
constexpr std::string_view OSS_PRIVATE_ENDPOINT_SUFFIX = "-internal.aliyuncs.com";
constexpr int LEN_OF_OSS_PRIVATE_SUFFIX = 9; // length of "-internal"

#ifndef CHECK_S3_CLIENT
#define CHECK_S3_CLIENT(client)                                 \
    if (!client) {                                              \
        return Status::InvalidArgument("init s3 client error"); \
    }
#endif

} // namespace

std::string ObjStorageFileSystem::display_path(std::string_view key) const {
    return _client->display_path(_bucket, key);
}

Result<std::shared_ptr<ObjStorageFileSystem>> ObjStorageFileSystem::create(ObjStorageConfig config,
                                                                           std::string id) {
    std::shared_ptr<ObjStorageFileSystem> fs(
            new ObjStorageFileSystem(std::move(config), std::move(id)));
    RETURN_IF_ERROR_RESULT(fs->init());
    return fs;
}

ObjStorageFileSystem::ObjStorageFileSystem(ObjStorageConfig config, std::string id)
        : RemoteFileSystem(config.prefix, std::move(id), FileSystemType::OBJECT_STORAGE),
          _bucket(std::move(config.bucket)),
          _prefix(std::move(config.prefix)),
          _client(std::make_shared<ObjClientHolder>(std::move(config.client))) {
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

Status ObjStorageFileSystem::init() {
    return _client->init();
}

Result<std::string> ObjStorageFileSystem::_get_key(const Path& full_path) const {
    // FIXME(plat1ko): Check bucket in full path and support relative path
    auto uri = ObjStorageUri::parse_s3(full_path.native());
    if (!uri.has_value()) {
        return ResultError(std::move(uri).error());
    }
    return std::move(uri.value().key);
}

ObjStorageFileSystem::~ObjStorageFileSystem() = default;

Status ObjStorageFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                              const FileWriterOptions* opts) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto key = DORIS_TRY(_get_key(file));
    *writer = std::make_unique<ObjStorageFileWriter>(_client, _bucket, std::move(key), opts);
    return Status::OK();
}

Status ObjStorageFileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                                const FileReaderOptions& opts) {
    TEST_SYNC_POINT_CALLBACK("S3FileSystem::open_file_internal", &file, &opts);
    auto key = DORIS_TRY(_get_key(file));
    *reader =
            DORIS_TRY(ObjStorageFileReader::create(_client, _bucket, key, opts.file_size, nullptr));
    return Status::OK();
}

Status ObjStorageFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    return Status::OK();
}

Status ObjStorageFileSystem::delete_file_impl(const Path& file) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    auto key = DORIS_TRY(_get_key(file));

    auto resp = client->delete_object({.bucket = _bucket, .key = key});

    if (resp.status.code == ErrorCode::OK || resp.status.code == ErrorCode::NOT_FOUND) {
        return Status::OK();
    }
    return std::move(Status(resp.status.code, std::move(resp.status.msg))
                             .append(fmt::format("failed to delete file {}", display_path(key))));
}

Status ObjStorageFileSystem::delete_directory_impl(const Path& dir) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    auto prefix = DORIS_TRY(_get_key(dir));
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    ObjStoragePath delete_path {
            .path = display_path(prefix),
            .bucket = _bucket,
            .prefix = prefix,
    };
    auto resp = delete_objects_recursively(std::move(client), delete_path);
    return {resp.status.code, std::move(resp.status.msg)};
}

Status ObjStorageFileSystem::batch_delete_impl(const std::vector<Path>& remote_files) {
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
            auto key = DORIS_TRY(_get_key(*path_iter));
            objects.emplace_back(std::move(key));
        }
        if (objects.empty()) {
            return Status::OK();
        }
        // clang-format off
        if (auto resp = client->delete_objects( {.bucket = _bucket,}, std::move(objects)); resp.status.code != ErrorCode::OK) {
            return {resp.status.code, std::move(resp.status.msg)};
        }
        // clang-format on
    } while (path_iter != remote_files.end());

    return Status::OK();
}

Status ObjStorageFileSystem::exists_impl(const Path& path, bool* res) const {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto key = DORIS_TRY(_get_key(path));

    VLOG_DEBUG << "key:" << key << " path:" << path;

    auto resp = client->head_object({.bucket = _bucket, .key = key});

    if (resp.resp.status.code == ErrorCode::OK) {
        *res = true;
    } else if (resp.resp.status.code == ErrorCode::NOT_FOUND) {
        *res = false;
    } else {
        return std::move(
                Status(resp.resp.status.code, std::move(resp.resp.status.msg))
                        .append(fmt::format(" failed to check exists {}", display_path(key))));
    }
    return Status::OK();
}

Status ObjStorageFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    auto key = DORIS_TRY(_get_key(file));
    *file_size = DORIS_TRY(_client->object_file_size(_bucket, key));
    return Status::OK();
}

Status ObjStorageFileSystem::list_impl(const Path& dir, bool only_file,
                                       std::vector<FileInfo>* files, bool* exists) {
    // For object storage, this path is always not exist.
    // So we ignore this property and set exists to true.
    *exists = true;
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto prefix = DORIS_TRY(_get_key(dir));
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    std::vector<ObjectMeta> objects;
    auto resp = client->list_objects({.bucket = _bucket, .prefix = prefix}, &objects);
    if (!resp.ok()) {
        files->clear();
        return {resp.status.code, std::move(resp.status.msg)};
    }
    files->reserve(files->size() + objects.size());
    for (auto& obj : objects) {
        obj.key.erase(0, prefix.size());
        bool is_dir = obj.key.empty() || obj.key.back() == '/';
        files->emplace_back(FileInfo {
                .file_name = std::move(obj.key), .file_size = obj.size, .is_file = !is_dir});
    }

    return Status::OK();
}

Status ObjStorageFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    return Status::NotSupported("S3FileSystem::rename_impl");
}

Status ObjStorageFileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    auto key = DORIS_TRY(_get_key(remote_file));
    auto start = std::chrono::steady_clock::now();
    FileWriterPtr obj_writer;
    RETURN_IF_ERROR(create_file_impl(key, &obj_writer, nullptr));
    FileReaderSPtr local_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(local_file, &local_reader));
    size_t local_buffer_size = config::s3_file_system_local_upload_buffer_size;
    std::unique_ptr<char[]> write_buffer =
            std::make_unique_for_overwrite<char[]>(local_buffer_size);
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
    LOG(INFO) << "Upload " << local_file.native() << " to " << display_path(key)
              << ", duration=" << duration.count() << ", bytes=" << size;

    return Status::OK();
}

Status ObjStorageFileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                               const std::vector<Path>& remote_files) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    if (local_files.size() != remote_files.size()) {
        return Status::InvalidArgument("local_files.size({}) != remote_files.size({})",
                                       local_files.size(), remote_files.size());
    }

    std::vector<FileWriterPtr> obj_writers(local_files.size());

    auto upload_task = [&, this](size_t idx) {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->s3_file_buffer_tracker());
        const auto& local_file = local_files[idx];
        const auto& remote_file = remote_files[idx];
        auto& obj_writer = obj_writers[idx];
        auto key = DORIS_TRY(_get_key(remote_file));
        LOG(INFO) << "Start to upload " << local_file.native() << " to " << display_path(key);
        RETURN_IF_ERROR(create_file_impl(key, &obj_writer, nullptr));
        FileReaderSPtr local_reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(local_file, &local_reader));
        size_t local_buffer_size = config::s3_file_system_local_upload_buffer_size;
        std::unique_ptr<char[]> write_buffer =
                std::make_unique_for_overwrite<char[]>(local_buffer_size);
        size_t cur_read = 0;
        while (cur_read < local_reader->size()) {
            size_t bytes_read = 0;
            RETURN_IF_ERROR(local_reader->read_at(
                    cur_read, Slice {write_buffer.get(), local_buffer_size}, &bytes_read));
            RETURN_IF_ERROR((*obj_writer).append({write_buffer.get(), bytes_read}));
            cur_read += bytes_read;
        }
        RETURN_IF_ERROR((*obj_writer).close());
        return Status::OK();
    };

    Status s = Status::OK();
    std::vector<std::future<Status>> futures;
    for (int i = 0; i < local_files.size(); ++i) {
        auto task = std::make_shared<std::packaged_task<Status(size_t idx)>>(upload_task);
        futures.emplace_back(task->get_future());
        auto st = ExecEnv::GetInstance()->s3_file_system_thread_pool()->submit_func(
                [t = std::move(task), idx = i]() mutable { (*t)(idx); });
        // We shouldn't return immediately since the previous submitted tasks might still be running in the thread pool
        if (!st.ok()) {
            s = st;
            break;
        }
    }
    for (auto&& f : futures) {
        auto cur_s = f.get();
        if (!cur_s.ok()) {
            s = std::move(cur_s);
        }
    }
    return s;
}

Status ObjStorageFileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto key = DORIS_TRY(_get_key(remote_file));
    int64_t size;
    RETURN_IF_ERROR(file_size(remote_file, &size));
    std::unique_ptr<char[]> buf = std::make_unique_for_overwrite<char[]>(size);
    size_t bytes_read = 0;
    // clang-format off
    auto resp = client->get_object( {.bucket = _bucket, .key = key,},
            buf.get(), 0, size, &bytes_read);
    // clang-format on
    if (resp.status.code != ErrorCode::OK) {
        return {resp.status.code, std::move(resp.status.msg)};
    }
    std::ofstream local_file_s;
    local_file_s.open(local_file, std::ios::out | std::ios::binary);
    if (local_file_s.good()) {
        local_file_s << StringViewStream(buf.get(), size).rdbuf();
    } else {
        return localfs_error(errno, fmt::format("failed to write file {}", local_file.native()));
    }

    return Status::OK();
}

// oss has public endpoint and private endpoint, is_public_endpoint determines
// whether to return a public endpoint.
std::string ObjStorageFileSystem::generate_presigned_url(const Path& path, int64_t expiration_secs,
                                                         bool is_public_endpoint) const {
    // An empty prefix must not add a separator absent from the uploaded object key.
    std::string key =
            _prefix.empty() ? path.native() : fmt::format("{}/{}", _prefix, path.native());
    std::shared_ptr<ObjStorageClient> client;
    // Read the conf and the client from one snapshot so a concurrent reset cannot pair the
    // endpoint of one generation with the client of another.
    auto state = _client->state();
    if (is_public_endpoint && state->config.endpoint().ends_with(OSS_PRIVATE_ENDPOINT_SUFFIX)) {
        // Only S3-protocol backends have an OSS private endpoint; the public one differs from it
        // only by the "-internal" part of the host.
        auto public_config = state->config;
        auto* s3 = std::get_if<S3ClientConfig>(&public_config.backend);
        DORIS_CHECK(s3 != nullptr);
        s3->endpoint.erase(s3->endpoint.size() - OSS_PRIVATE_ENDPOINT_SUFFIX.size(),
                           LEN_OF_OSS_PRIVATE_SUFFIX);
        auto client_result = ObjClientFactory::instance().get_or_create(public_config);
        if (!client_result) {
            LOG(WARNING) << "failed to create S3 client for presigned URL: "
                         << client_result.error();
            return {};
        }
        client = std::move(client_result).value();
    } else {
        client = state->client;
    }
    return client->generate_presigned_url({.bucket = _bucket, .key = key, .prefix = ""},
                                          expiration_secs);
}

} // namespace doris::io
