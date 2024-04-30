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

#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Error.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>
#include <fmt/format.h>
#include <stddef.h>

#include <algorithm>

#include "common/compiler_util.h" // IWYU pragma: keep
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <fstream> // IWYU pragma: keep
#include <memory>
#include <sstream>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_writer.h"
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

S3ClientHolder::S3ClientHolder(S3ClientConf conf) : _conf(std::move(conf)) {}

S3ClientHolder::~S3ClientHolder() = default;

Status S3ClientHolder::init() {
    auto client = S3ClientFactory::instance().create(_conf);
    if (!client) {
        return Status::InternalError("failed to init s3 client with conf {}", _conf.to_string());
    }

    _client = std::move(client);
    return Status::OK();
}

Status S3ClientHolder::reset(const S3ClientConf& conf) {
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
        _client = std::move(client);
        _conf = std::move(reset_conf);
    }

    return Status::OK();
}

Result<int64_t> S3ClientHolder::object_file_size(const std::string& bucket,
                                                 const std::string& key) const {
    auto client = get();
    if (!client) {
        return ResultError(Status::InternalError("init s3 client error"));
    }

    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(bucket).WithKey(key);

    SCOPED_BVAR_LATENCY(s3_bvar::s3_head_latency);
    auto outcome = client->HeadObject(request);

    if (!outcome.IsSuccess()) {
        return ResultError(s3fs_error(outcome.GetError(), fmt::format("failed to head s3 file {}",
                                                                      full_s3_path(bucket, key))));
    }

    return outcome.GetResult().GetContentLength();
}

std::string S3ClientHolder::full_s3_path(std::string_view bucket, std::string_view key) const {
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
          _client(std::make_shared<S3ClientHolder>(std::move(s3_conf.client_conf))) {
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
    *writer = std::make_unique<S3FileWriter>(std::move(client), _bucket, std::move(key), opts);
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

    Aws::S3::Model::DeleteObjectRequest request;
    auto key = DORIS_TRY(get_key(file));
    request.WithBucket(_bucket).WithKey(key);

    SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_latency);
    auto outcome = client->DeleteObject(request);
    if (outcome.IsSuccess() ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return Status::OK();
    }
    return s3fs_error(outcome.GetError(),
                      fmt::format("failed to delete file {}", full_s3_path(key)));
}

Status S3FileSystem::delete_directory_impl(const Path& dir) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::ListObjectsV2Request request;
    auto prefix = DORIS_TRY(get_key(dir));
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }
    request.WithBucket(_bucket).WithPrefix(prefix);

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(_bucket);
    bool is_trucated = false;
    do {
        Aws::S3::Model::ListObjectsV2Outcome outcome;
        {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
            outcome = client->ListObjectsV2(request);
        }
        if (!outcome.IsSuccess()) {
            return s3fs_error(
                    outcome.GetError(),
                    fmt::format("failed to list objects when delete dir {}", full_s3_path(prefix)));
        }
        const auto& result = outcome.GetResult();
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(result.GetContents().size());
        for (const auto& obj : result.GetContents()) {
            objects.emplace_back().SetKey(obj.GetKey());
        }
        if (!objects.empty()) {
            Aws::S3::Model::Delete del;
            del.WithObjects(std::move(objects)).SetQuiet(true);
            delete_request.SetDelete(std::move(del));
            SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_latency);
            auto delete_outcome = client->DeleteObjects(delete_request);
            if (!delete_outcome.IsSuccess()) {
                return s3fs_error(delete_outcome.GetError(),
                                  fmt::format("failed to delete dir {}", full_s3_path(prefix)));
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                return Status::InternalError("failed to delete object {}: {}",
                                             full_s3_path(e.GetKey()), e.GetMessage());
            }
        }
        is_trucated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_trucated);
    return Status::OK();
}

Status S3FileSystem::batch_delete_impl(const std::vector<Path>& remote_files) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    constexpr size_t max_delete_batch = 1000;
    auto path_iter = remote_files.begin();

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(_bucket);
    do {
        Aws::S3::Model::Delete del;
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        auto path_begin = path_iter;
        for (; path_iter != remote_files.end() && (path_iter - path_begin < max_delete_batch);
             ++path_iter) {
            auto key = DORIS_TRY(get_key(*path_iter));
            objects.emplace_back().SetKey(std::move(key));
        }
        if (objects.empty()) {
            return Status::OK();
        }
        del.WithObjects(std::move(objects)).SetQuiet(true);
        delete_request.SetDelete(std::move(del));
        SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_latency);
        auto delete_outcome = client->DeleteObjects(delete_request);
        if (UNLIKELY(!delete_outcome.IsSuccess())) {
            return s3fs_error(
                    delete_outcome.GetError(),
                    fmt::format("failed to delete objects {}",
                                full_s3_path(
                                        delete_request.GetDelete().GetObjects().front().GetKey())));
        }
        if (UNLIKELY(!delete_outcome.GetResult().GetErrors().empty())) {
            const auto& e = delete_outcome.GetResult().GetErrors().front();
            return Status::InternalError("failed to delete object {}: {}", full_s3_path(e.GetKey()),
                                         e.GetMessage());
        }
    } while (path_iter != remote_files.end());

    return Status::OK();
}

Status S3FileSystem::exists_impl(const Path& path, bool* res) const {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto key = DORIS_TRY(get_key(path));

    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(_bucket).WithKey(key);

    SCOPED_BVAR_LATENCY(s3_bvar::s3_head_latency);
    auto outcome = client->HeadObject(request);
    if (outcome.IsSuccess()) {
        *res = true;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        *res = false;
    } else {
        return s3fs_error(outcome.GetError(),
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

    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(_bucket).WithPrefix(prefix);
    bool is_trucated = false;
    do {
        Aws::S3::Model::ListObjectsV2Outcome outcome;
        {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
            outcome = client->ListObjectsV2(request);
        }
        if (!outcome.IsSuccess()) {
            return s3fs_error(outcome.GetError(),
                              fmt::format("failed to list {}", full_s3_path(prefix)));
        }
        for (const auto& obj : outcome.GetResult().GetContents()) {
            std::string key = obj.GetKey();
            bool is_dir = (key.back() == '/');
            if (only_file && is_dir) {
                continue;
            }
            FileInfo file_info;
            file_info.file_name = obj.GetKey().substr(prefix.size());
            file_info.file_size = obj.GetSize();
            file_info.is_file = !is_dir;
            files->push_back(std::move(file_info));
        }
        is_trucated = outcome.GetResult().GetIsTruncated();
        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (is_trucated);
    return Status::OK();
}

Status S3FileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    return Status::NotSupported("S3FileSystem::rename_impl");
}

Status S3FileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);

    Aws::Transfer::TransferManagerConfiguration transfer_config(default_executor().get());
    transfer_config.s3Client = client;
    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    auto start = std::chrono::steady_clock::now();

    auto key = DORIS_TRY(get_key(remote_file));
    auto handle = transfer_manager->UploadFile(local_file.native(), _bucket, key, "text/plain",
                                               Aws::Map<Aws::String, Aws::String>());
    handle->WaitUntilFinished();

    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);

    if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
        return s3fs_error(
                handle->GetLastError(),
                fmt::format("failed to upload {} to {}", local_file.native(), full_s3_path(key)));
    }

    auto size = handle->GetBytesTransferred();
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

    Aws::Transfer::TransferManagerConfiguration transfer_config(default_executor().get());
    transfer_config.s3Client = client;
    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    std::vector<std::shared_ptr<Aws::Transfer::TransferHandle>> handles;
    for (int i = 0; i < local_files.size(); ++i) {
        auto key = DORIS_TRY(get_key(remote_files[i]));
        LOG(INFO) << "Start to upload " << local_files[i].native() << " to " << full_s3_path(key);
        auto handle =
                transfer_manager->UploadFile(local_files[i].native(), _bucket, key, "text/plain",
                                             Aws::Map<Aws::String, Aws::String>());
        handles.push_back(std::move(handle));
    }
    for (auto& handle : handles) {
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO(cyx): Maybe we can cancel remaining handles.
            return s3fs_error(handle->GetLastError(), fmt::format("failed to upload to {}",
                                                                  full_s3_path(handle->GetKey())));
        }
    }
    return Status::OK();
}

Status S3FileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    auto client = _client->get();
    CHECK_S3_CLIENT(client);
    auto key = DORIS_TRY(get_key(remote_file));
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(_bucket).WithKey(key);
    Aws::S3::Model::GetObjectOutcome response;
    {
        SCOPED_BVAR_LATENCY(s3_bvar::s3_get_latency);
        response = client->GetObject(request);
    }
    if (response.IsSuccess()) {
        Aws::OFStream local_file_s;
        local_file_s.open(local_file, std::ios::out | std::ios::binary);
        if (local_file_s.good()) {
            local_file_s << response.GetResult().GetBody().rdbuf();
        }
        if (!local_file_s.good()) {
            return Status::IOError("failed to download {}: failed to write file: {}",
                                   remote_file.native(), local_file.native());
        }
    } else {
        return localfs_error(errno, fmt::format("failed to write file {}", local_file.native()));
    }
    return Status::OK();
}

} // namespace doris::io
