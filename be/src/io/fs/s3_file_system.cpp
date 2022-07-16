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

#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/transfer/TransferManager.h>

#include <filesystem>
#include <fstream>
#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "gutil/strings/stringpiece.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_reader.h"

namespace doris {
namespace io {

S3FileSystem::S3FileSystem(const std::map<std::string, std::string>& properties, std::string bucket,
                           std::string prefix, ResourceId resource_id)
        : RemoteFileSystem(Path(properties.at(S3_ENDPOINT)) / bucket / prefix,
                           std::move(resource_id), FileSystemType::S3),
          _properties(properties),
          _bucket(std::move(bucket)),
          _prefix(std::move(prefix)) {
    _endpoint = properties.at(S3_ENDPOINT);
    _executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
            resource_id.c_str(), config::s3_transfer_executor_pool_size);
}

S3FileSystem::~S3FileSystem() = default;

Status S3FileSystem::connect() {
    std::lock_guard lock(_client_mu);
    _client = ClientFactory::instance().create(_properties);
    return Status::OK();
}

Status S3FileSystem::upload(const Path& local_path, const Path& dest_path) {
    auto client = get_client();
    DCHECK(client);

    Aws::Transfer::TransferManagerConfiguration transfer_config(_executor.get());
    transfer_config.s3Client = client;
    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    auto start = std::chrono::steady_clock::now();

    auto key = get_key(dest_path);
    auto handle = transfer_manager->UploadFile(local_path.native(), _bucket, key, "text/plain",
                                               Aws::Map<Aws::String, Aws::String>());
    handle->WaitUntilFinished();

    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);

    if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
        return Status::IOError(fmt::format("failed to upload(endpoint={}, bucket={}, key={}): {}",
                                           _endpoint, _bucket, key,
                                           handle->GetLastError().GetMessage()));
    }

    auto file_size = std::filesystem::file_size(local_path);
    LOG(INFO) << "Upload " << local_path.native() << " to s3, endpoint=" << _endpoint
              << ", bucket=" << _bucket << ", key=" << key << ", duration=" << duration.count()
              << ", capacity=" << file_size << ", tp=" << (file_size) / duration.count();

    return Status::OK();
}

Status S3FileSystem::batch_upload(const std::vector<Path>& local_paths,
                                  const std::vector<Path>& dest_paths) {
    auto client = get_client();
    DCHECK(client);

    if (local_paths.size() != dest_paths.size()) {
        return Status::InvalidArgument("local_paths.size() != dest_paths.size()");
    }

    Aws::Transfer::TransferManagerConfiguration transfer_config(_executor.get());
    transfer_config.s3Client = client;
    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    std::vector<std::shared_ptr<Aws::Transfer::TransferHandle>> handles;
    for (int i = 0; i < local_paths.size(); ++i) {
        auto key = get_key(dest_paths[i]);
        LOG(INFO) << "Start to upload " << local_paths[i].native()
                  << " to s3, endpoint=" << _endpoint << ", bucket=" << _bucket << ", key=" << key;
        auto handle =
                transfer_manager->UploadFile(local_paths[i].native(), _bucket, key, "text/plain",
                                             Aws::Map<Aws::String, Aws::String>());
        handles.push_back(std::move(handle));
    }
    for (auto& handle : handles) {
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO(cyx): Maybe we can cancel remaining handles.
            return Status::IOError(handle->GetLastError().GetMessage());
        }
    }
    return Status::OK();
}

Status S3FileSystem::create_file(const Path& path, FileWriterPtr* writer) {
    return Status::NotSupported("not support");
}

Status S3FileSystem::open_file(const Path& path, FileReaderSPtr* reader) {
    size_t fsize = 0;
    RETURN_IF_ERROR(file_size(path, &fsize));
    auto key = get_key(path);
    auto fs_path = Path(_endpoint) / _bucket / key;
    *reader = std::make_unique<S3FileReader>(std::move(fs_path), fsize, std::move(key), _bucket,
                                             this);
    return Status::OK();
}

Status S3FileSystem::delete_file(const Path& path) {
    auto client = get_client();
    DCHECK(client);

    Aws::S3::Model::DeleteObjectRequest request;
    auto key = get_key(path);
    request.WithBucket(_bucket).WithKey(key);

    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess()) {
        return Status::IOError(
                fmt::format("failed to delete object(endpoint={}, bucket={}, key={}): {}",
                            _endpoint, _bucket, key, outcome.GetError().GetMessage()));
    }
    return Status::OK();
}

Status S3FileSystem::create_directory(const Path& path) {
    return Status::OK();
}

Status S3FileSystem::delete_directory(const Path& path) {
    return Status::NotSupported("not support");
}

Status S3FileSystem::link_file(const Path& src, const Path& dest) {
    return Status::NotSupported("not support");
}

Status S3FileSystem::exists(const Path& path, bool* res) const {
    auto client = get_client();
    DCHECK(client);

    Aws::S3::Model::HeadObjectRequest request;
    auto key = get_key(path);
    request.WithBucket(_bucket).WithKey(key);

    auto outcome = _client->HeadObject(request);
    if (outcome.IsSuccess()) {
        *res = true;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        *res = false;
    } else {
        return Status::IOError(
                fmt::format("failed to get object head(endpoint={}, bucket={}, key={}): {}",
                            _endpoint, _bucket, key, outcome.GetError().GetMessage()));
    }
    return Status::OK();
}

Status S3FileSystem::file_size(const Path& path, size_t* file_size) const {
    auto client = get_client();
    DCHECK(client);

    Aws::S3::Model::HeadObjectRequest request;
    auto key = get_key(path);
    request.WithBucket(_bucket).WithKey(key);

    auto outcome = _client->HeadObject(request);
    if (outcome.IsSuccess()) {
        *file_size = outcome.GetResult().GetContentLength();
    } else {
        return Status::IOError(
                fmt::format("failed to get object size(endpoint={}, bucket={}, key={}): {}",
                            _endpoint, _bucket, key, outcome.GetError().GetMessage()));
    }
    return Status::OK();
}

Status S3FileSystem::list(const Path& path, std::vector<Path>* files) {
    return Status::NotSupported("not support");
}

std::string S3FileSystem::get_key(const Path& path) const {
    StringPiece str(path.native());
    if (str.starts_with(_root_path.native())) {
        return fmt::format("{}/{}", _prefix, str.data() + _root_path.native().size());
    }
    // We consider it as a relative path.
    return fmt::format("{}/{}", _prefix, path.native());
}

} // namespace io
} // namespace doris
