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
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_writer.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {
namespace io {

#ifndef CHECK_S3_CLIENT
#define CHECK_S3_CLIENT(client)                         \
    if (!client) {                                      \
        return Status::IOError("init s3 client error"); \
    }
#endif

#ifndef CHECK_S3_PATH
#define CHECK_S3_PATH(uri, path) \
    S3URI uri(path.string());    \
    RETURN_IF_ERROR(uri.parse());
#endif

#ifndef GET_KEY
#define GET_KEY(key, path) \
    std::string key;       \
    RETURN_IF_ERROR(get_key(path, &key));
#endif

Status S3FileSystem::create(S3Conf s3_conf, std::string id, std::shared_ptr<S3FileSystem>* fs) {
    (*fs).reset(new S3FileSystem(std::move(s3_conf), std::move(id)));
    return (*fs)->connect();
}

S3FileSystem::S3FileSystem(S3Conf&& s3_conf, std::string&& id)
        : RemoteFileSystem(s3_conf.prefix, std::move(id), FileSystemType::S3),
          _s3_conf(std::move(s3_conf)) {
    // FIXME(plat1ko): Normalize prefix
    // remove the first and last '/'
    if (!_s3_conf.prefix.empty()) {
        if (_s3_conf.prefix[0] == '/') {
            _s3_conf.prefix = _s3_conf.prefix.substr(1);
        }
        if (_s3_conf.prefix.back() == '/') {
            _s3_conf.prefix.pop_back();
        }
    }
    _executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
            _id.c_str(), config::s3_transfer_executor_pool_size);
}

S3FileSystem::~S3FileSystem() = default;

Status S3FileSystem::connect_impl() {
    std::lock_guard lock(_client_mu);
    _client = S3ClientFactory::instance().create(_s3_conf);
    if (!_client) {
        return Status::IOError("failed to init s3 client with {}", _s3_conf.to_string());
    }
    return Status::OK();
}

Status S3FileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                      const FileWriterOptions* opts) {
    GET_KEY(key, file);
    *writer = std::make_unique<S3FileWriter>(
            key, std::static_pointer_cast<S3FileSystem>(shared_from_this()), opts);
    return Status::OK();
}

Status S3FileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                        const FileReaderOptions& opts) {
    int64_t fsize = opts.file_size;
    if (fsize < 0) {
        RETURN_IF_ERROR(file_size_impl(file, &fsize));
    }
    GET_KEY(key, file);
    *reader = std::make_shared<S3FileReader>(
            fsize, std::move(key), std::static_pointer_cast<S3FileSystem>(shared_from_this()));
    return Status::OK();
}

Status S3FileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    return Status::OK();
}

Status S3FileSystem::delete_file_impl(const Path& file) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::DeleteObjectRequest request;
    GET_KEY(key, file);
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = client->DeleteObject(request);
    s3_bvar::s3_delete_total << 1;
    if (outcome.IsSuccess() ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return Status::OK();
    }
    return Status::IOError("failed to delete file {}: {}", file.native(), error_msg(key, outcome));
}

Status S3FileSystem::delete_directory_impl(const Path& dir) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::ListObjectsV2Request request;
    GET_KEY(prefix, dir);
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }
    request.WithBucket(_s3_conf.bucket).WithPrefix(prefix);

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(_s3_conf.bucket);
    bool is_trucated = false;
    do {
        auto outcome = client->ListObjectsV2(request);
        s3_bvar::s3_list_total << 1;
        if (!outcome.IsSuccess()) {
            return Status::IOError("failed to list objects when delete dir {}: {}", dir.native(),
                                   error_msg(prefix, outcome));
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
            auto delete_outcome = client->DeleteObjects(delete_request);
            s3_bvar::s3_delete_total << 1;
            if (!delete_outcome.IsSuccess()) {
                return Status::IOError("failed to delete dir {}: {}", dir.native(),
                                       error_msg(prefix, delete_outcome));
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                return Status::IOError("fail to delete object: {}",
                                       error_msg(e.GetKey(), e.GetMessage()));
            }
        }
        is_trucated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_trucated);
    return Status::OK();
}

Status S3FileSystem::batch_delete_impl(const std::vector<Path>& remote_files) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    constexpr size_t max_delete_batch = 1000;
    auto path_iter = remote_files.begin();

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(_s3_conf.bucket);
    do {
        Aws::S3::Model::Delete del;
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        auto path_begin = path_iter;
        for (; path_iter != remote_files.end() && (path_iter - path_begin < max_delete_batch);
             ++path_iter) {
            GET_KEY(key, *path_iter);
            objects.emplace_back().SetKey(key);
        }
        if (objects.empty()) {
            return Status::OK();
        }
        del.WithObjects(std::move(objects)).SetQuiet(true);
        delete_request.SetDelete(std::move(del));
        auto delete_outcome = client->DeleteObjects(delete_request);
        s3_bvar::s3_delete_total << 1;
        if (UNLIKELY(!delete_outcome.IsSuccess())) {
            return Status::IOError(
                    "failed to delete objects: {}",
                    error_msg(delete_request.GetDelete().GetObjects().front().GetKey(),
                              delete_outcome));
        }
        if (UNLIKELY(!delete_outcome.GetResult().GetErrors().empty())) {
            const auto& e = delete_outcome.GetResult().GetErrors().front();
            return Status::IOError("failed to delete objects: {}",
                                   error_msg(e.GetKey(), delete_outcome));
        }
    } while (path_iter != remote_files.end());

    return Status::OK();
}

Status S3FileSystem::exists_impl(const Path& path, bool* res) const {
    auto client = get_client();
    CHECK_S3_CLIENT(client);
    GET_KEY(key, path);

    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = client->HeadObject(request);
    s3_bvar::s3_head_total << 1;
    if (outcome.IsSuccess()) {
        *res = true;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        *res = false;
    } else {
        return Status::IOError("failed to check exists {}: {}", path.native(),
                               error_msg(key, outcome));
    }
    return Status::OK();
}

Status S3FileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::HeadObjectRequest request;
    GET_KEY(key, file);
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = client->HeadObject(request);
    s3_bvar::s3_head_total << 1;
    if (outcome.IsSuccess()) {
        *file_size = outcome.GetResult().GetContentLength();
    } else {
        return Status::IOError("failed to get file size {}, {}", file.native(),
                               error_msg(key, outcome));
    }
    return Status::OK();
}

Status S3FileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                               bool* exists) {
    // For object storage, this path is always not exist.
    // So we ignore this property and set exists to true.
    *exists = true;
    auto client = get_client();
    CHECK_S3_CLIENT(client);
    GET_KEY(prefix, dir);
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(_s3_conf.bucket).WithPrefix(prefix);
    bool is_trucated = false;
    do {
        auto outcome = client->ListObjectsV2(request);
        s3_bvar::s3_list_total << 1;
        if (!outcome.IsSuccess()) {
            return Status::IOError("failed to list {}: {}", dir.native(),
                                   error_msg(prefix, outcome));
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
    RETURN_IF_ERROR(copy(orig_name, new_name));
    return delete_file_impl(orig_name);
}

Status S3FileSystem::rename_dir_impl(const Path& orig_name, const Path& new_name) {
    RETURN_IF_ERROR(copy_dir(orig_name, new_name));
    return delete_directory_impl(orig_name);
}

Status S3FileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::Transfer::TransferManagerConfiguration transfer_config(_executor.get());
    transfer_config.s3Client = client;
    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    auto start = std::chrono::steady_clock::now();

    GET_KEY(key, remote_file);
    auto handle = transfer_manager->UploadFile(local_file.native(), _s3_conf.bucket, key,
                                               "text/plain", Aws::Map<Aws::String, Aws::String>());
    handle->WaitUntilFinished();

    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);

    if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
        return Status::IOError("failed to upload {}: {}", remote_file.native(),
                               error_msg(key, handle->GetLastError().GetMessage()));
    }

    auto file_size = std::filesystem::file_size(local_file);
    LOG(INFO) << "Upload " << local_file.native() << " to s3, endpoint=" << _s3_conf.endpoint
              << ", bucket=" << _s3_conf.bucket << ", key=" << key
              << ", duration=" << duration.count() << ", capacity=" << file_size
              << ", tp=" << (file_size) / duration.count();

    return Status::OK();
}

Status S3FileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                       const std::vector<Path>& remote_files) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    if (local_files.size() != remote_files.size()) {
        return Status::InvalidArgument("local_files.size({}) != remote_files.size({})",
                                       local_files.size(), remote_files.size());
    }

    Aws::Transfer::TransferManagerConfiguration transfer_config(_executor.get());
    transfer_config.s3Client = client;
    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    std::vector<std::shared_ptr<Aws::Transfer::TransferHandle>> handles;
    for (int i = 0; i < local_files.size(); ++i) {
        GET_KEY(key, remote_files[i]);
        LOG(INFO) << "Start to upload " << local_files[i].native()
                  << " to s3, endpoint=" << _s3_conf.endpoint << ", bucket=" << _s3_conf.bucket
                  << ", key=" << key;
        auto handle =
                transfer_manager->UploadFile(local_files[i].native(), _s3_conf.bucket, key,
                                             "text/plain", Aws::Map<Aws::String, Aws::String>());
        handles.push_back(std::move(handle));
    }
    for (auto& handle : handles) {
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO(cyx): Maybe we can cancel remaining handles.
            return Status::IOError(
                    "failed to upload: {}",
                    error_msg(handle->GetKey(), handle->GetLastError().GetMessage()));
        }
    }
    return Status::OK();
}

Status S3FileSystem::direct_upload_impl(const Path& remote_file, const std::string& content) {
    CHECK_S3_CLIENT(_client);
    Aws::S3::Model::PutObjectRequest request;
    GET_KEY(key, remote_file);
    request.WithBucket(_s3_conf.bucket).WithKey(key);
    const std::shared_ptr<Aws::IOStream> input_data =
            Aws::MakeShared<Aws::StringStream>("upload_directly");
    *input_data << content.c_str();
    if (input_data->good()) {
        request.SetBody(input_data);
    }
    if (!input_data->good()) {
        return Status::IOError("failed to direct upload {}: failed to read from string",
                               remote_file.native());
    }
    Aws::S3::Model::PutObjectOutcome response = _client->PutObject(request);
    s3_bvar::s3_put_total << 1;
    if (response.IsSuccess()) {
        return Status::OK();
    } else {
        return Status::IOError("failed to direct upload {}: {}", remote_file.native(),
                               error_msg(key, response));
    }
}

Status S3FileSystem::upload_with_checksum_impl(const Path& local_file, const Path& remote_file,
                                               const std::string& checksum) {
    return upload_impl(local_file, remote_file.string() + "." + checksum);
}

Status S3FileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);
    GET_KEY(key, remote_file);
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(_s3_conf.bucket).WithKey(key);
    Aws::S3::Model::GetObjectOutcome response = _client->GetObject(request);
    s3_bvar::s3_get_total << 1;
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
        return Status::IOError("failed to download {}: {}", remote_file.native(),
                               error_msg(key, response));
    }
    return Status::OK();
}

Status S3FileSystem::direct_download_impl(const Path& remote, std::string* content) {
    CHECK_S3_CLIENT(_client);
    Aws::S3::Model::GetObjectRequest request;
    GET_KEY(key, remote);
    request.WithBucket(_s3_conf.bucket).WithKey(key);
    Aws::S3::Model::GetObjectOutcome response = _client->GetObject(request);
    s3_bvar::s3_get_total << 1;
    if (response.IsSuccess()) {
        std::stringstream ss;
        ss << response.GetResult().GetBody().rdbuf();
        *content = ss.str();
    } else {
        return Status::IOError("failed to direct download {}: {}", remote.native(),
                               error_msg(key, response));
    }
    return Status::OK();
}

Status S3FileSystem::copy(const Path& src, const Path& dst) {
    CHECK_S3_CLIENT(_client);
    Aws::S3::Model::CopyObjectRequest request;
    GET_KEY(src_key, src);
    GET_KEY(dst_key, dst);
    request.WithCopySource(_s3_conf.bucket + "/" + src_key)
            .WithKey(dst_key)
            .WithBucket(_s3_conf.bucket);
    Aws::S3::Model::CopyObjectOutcome response = _client->CopyObject(request);
    s3_bvar::s3_copy_object_total << 1;
    if (response.IsSuccess()) {
        return Status::OK();
    } else {
        return Status::IOError("failed to copy from {} to {}: {}", src.native(), dst.native(),
                               error_msg(src_key, response));
    }
}

Status S3FileSystem::copy_dir(const Path& src, const Path& dst) {
    std::vector<FileInfo> files;
    bool exists = false;
    RETURN_IF_ERROR(list_impl(src, true, &files, &exists));
    if (!exists) {
        return Status::IOError("path not found: {}", src.native());
    }
    if (files.empty()) {
        LOG(WARNING) << "Nothing need to copy: " << src << " -> " << dst;
        return Status::OK();
    }
    for (auto& file : files) {
        RETURN_IF_ERROR(copy(src / file.file_name, dst / file.file_name));
    }
    return Status::OK();
}

Status S3FileSystem::get_key(const Path& path, std::string* key) const {
    CHECK_S3_PATH(uri, path);
    *key = uri.get_key();
    return Status::OK();
}

template <typename AwsOutcome>
std::string S3FileSystem::error_msg(const std::string& key, const AwsOutcome& outcome) const {
    return fmt::format("(endpoint: {}, bucket: {}, key:{}, {}), {}, error code {}",
                       _s3_conf.endpoint, _s3_conf.bucket, key,
                       outcome.GetError().GetExceptionName(), outcome.GetError().GetMessage(),
                       outcome.GetError().GetResponseCode());
}

std::string S3FileSystem::error_msg(const std::string& key, const std::string& err) const {
    return fmt::format("(endpoint: {}, bucket: {}, key:{}), {}", _s3_conf.endpoint, _s3_conf.bucket,
                       key, err);
}

} // namespace io
} // namespace doris
