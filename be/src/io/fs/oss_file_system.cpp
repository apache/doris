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

#include "io/fs/oss_file_system.h"

#include <alibabacloud/oss/OssClient.h>
#include <fmt/format.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"

namespace doris::io {
namespace {

#ifndef CHECK_OSS_CLIENT
#define CHECK_OSS_CLIENT(client)                                 \
    if (!client) {                                               \
        return Status::InvalidArgument("init oss client error"); \
    }
#endif

Result<std::string> get_oss_key(const Path& full_path) {
    // Extract key from OSS path: oss://bucket/key
    std::string path_str = full_path.native();

    // Remove oss:// prefix if present
    const std::string oss_prefix = "oss://";
    if (path_str.find(oss_prefix) == 0) {
        path_str = path_str.substr(oss_prefix.length());

        // Find first '/' to separate bucket and key
        size_t pos = path_str.find('/');
        if (pos == std::string::npos) {
            return ""; // No key, just bucket
        }
        return path_str.substr(pos + 1);
    }

    return path_str;
}

} // namespace

// OSSClientHolder implementation

OSSClientHolder::OSSClientHolder(OSSClientConf conf) : _conf(std::move(conf)) {}

OSSClientHolder::~OSSClientHolder() = default;

Status OSSClientHolder::init() {
    _client = OSSClientFactory::instance().create(_conf);
    if (!_client) {
        return Status::InvalidArgument("failed to init oss client with conf {}", _conf.to_string());
    }

    return Status::OK();
}

Status OSSClientHolder::reset(const OSSClientConf& conf) {
    OSSClientConf reset_conf;
    {
        std::shared_lock lock(_mtx);
        if (conf.get_hash() == _conf.get_hash()) {
            return Status::OK(); // Same conf
        }

        reset_conf = _conf;
        reset_conf.ak = conf.ak;
        reset_conf.sk = conf.sk;
        reset_conf.token = conf.token;
        reset_conf.bucket = conf.bucket;
        reset_conf.connect_timeout_ms = conf.connect_timeout_ms;
        reset_conf.max_connections = conf.max_connections;
        reset_conf.request_timeout_ms = conf.request_timeout_ms;
        reset_conf.cred_provider_type = conf.cred_provider_type;
    }

    auto client = OSSClientFactory::instance().create(reset_conf);
    if (!client) {
        return Status::InvalidArgument("failed to init oss client with conf {}", conf.to_string());
    }

    LOG(WARNING) << "reset oss client with new conf: " << conf.to_string();

    {
        std::lock_guard lock(_mtx);
        _client = std::move(client);
        _conf = std::move(reset_conf);
    }

    return Status::OK();
}

Result<int64_t> OSSClientHolder::object_file_size(const std::string& bucket,
                                                   const std::string& key) const {
    auto client = get();
    if (!client) {
        return ResultError(Status::InvalidArgument("init oss client error"));
    }

    try {
        auto outcome = client->HeadObject(bucket, key);
        if (!outcome.isSuccess()) {
            return ResultError(Status::IOError("failed to head oss file {}: {} - {}",
                                               full_oss_path(bucket, key), outcome.error().Code(),
                                               outcome.error().Message()));
        }

        return outcome.result().ContentLength();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return ResultError(Status::IOError("failed to head oss file {}: {} - {}",
                                           full_oss_path(bucket, key), e.GetErrorCode(),
                                           e.GetErrorMessage()));
    }
}

std::string OSSClientHolder::full_oss_path(std::string_view bucket, std::string_view key) const {
    return fmt::format("{}/{}/{}", _conf.endpoint, bucket, key);
}

// OSSFileSystem implementation

std::string OSSFileSystem::full_oss_path(std::string_view key) const {
    return _client->full_oss_path(_bucket, key);
}

Result<std::shared_ptr<OSSFileSystem>> OSSFileSystem::create(OSSConf oss_conf, std::string id) {
    std::shared_ptr<OSSFileSystem> fs(new OSSFileSystem(std::move(oss_conf), std::move(id)));
    RETURN_IF_ERROR_RESULT(fs->init());
    return fs;
}

OSSFileSystem::OSSFileSystem(OSSConf oss_conf, std::string id)
        : RemoteFileSystem(oss_conf.prefix, std::move(id), FileSystemType::OSS),
          _bucket(std::move(oss_conf.bucket)),
          _prefix(std::move(oss_conf.prefix)),
          _client(std::make_shared<OSSClientHolder>(std::move(oss_conf.client_conf))) {
    // Normalize prefix: remove leading and trailing '/'
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

Status OSSFileSystem::init() {
    return _client->init();
}

OSSFileSystem::~OSSFileSystem() = default;

Status OSSFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                       const FileWriterOptions* opts) {
    // TODO: Implement OSSFileWriter
    return Status::NotSupported("OSSFileWriter not implemented yet");
}

Status OSSFileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                         const FileReaderOptions& opts) {
    // TODO: Implement OSSFileReader
    return Status::NotSupported("OSSFileReader not implemented yet");
}

Status OSSFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    // OSS doesn't have real directories, they're just prefixes
    return Status::OK();
}

Status OSSFileSystem::delete_file_impl(const Path& file) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto key = DORIS_TRY(get_oss_key(file));

    try {
        AlibabaCloud::OSS::DeleteObjectRequest request(_bucket, key);
        auto outcome = client->DeleteObject(request);

        // OSS DeleteObject returns success even if object doesn't exist
        if (!outcome.isSuccess()) {
            std::string error_code = outcome.error().Code();
            if (error_code != "NoSuchKey") {
                return Status::IOError("failed to delete file {}: {} - {}", full_oss_path(key),
                                       error_code, outcome.error().Message());
            }
        }

        return Status::OK();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return Status::IOError("failed to delete file {}: {} - {}", full_oss_path(key),
                               e.GetErrorCode(), e.GetErrorMessage());
    }
}

Status OSSFileSystem::delete_directory_impl(const Path& dir) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto prefix = DORIS_TRY(get_oss_key(dir));
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    try {
        // List and delete all objects with prefix
        bool is_truncated = true;
        std::string marker;

        while (is_truncated) {
            AlibabaCloud::OSS::ListObjectsRequest list_request(_bucket);
            list_request.setPrefix(prefix);
            list_request.setMaxKeys(1000);

            if (!marker.empty()) {
                list_request.setMarker(marker);
            }

            auto list_outcome = client->ListObjects(list_request);
            if (!list_outcome.isSuccess()) {
                return Status::IOError("failed to list objects for delete directory {}: {} - {}",
                                       full_oss_path(prefix), list_outcome.error().Code(),
                                       list_outcome.error().Message());
            }

            const auto& result = list_outcome.result();
            const auto& objects = result.ObjectSummarys();

            // Delete objects in batch (max 1000 per request)
            if (!objects.empty()) {
                AlibabaCloud::OSS::DeletedKeyList keys;
                for (const auto& obj : objects) {
                    keys.push_back(obj.Key());
                }

                AlibabaCloud::OSS::DeleteObjectsRequest delete_request(_bucket, keys);
                auto delete_outcome = client->DeleteObjects(delete_request);

                if (!delete_outcome.isSuccess()) {
                    return Status::IOError("failed to batch delete objects: {} - {}",
                                           delete_outcome.error().Code(),
                                           delete_outcome.error().Message());
                }
            }

            is_truncated = result.IsTruncated();
            marker = result.NextMarker();
        }

        return Status::OK();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return Status::IOError("failed to delete directory {}: {} - {}", full_oss_path(prefix),
                               e.GetErrorCode(), e.GetErrorMessage());
    }
}

Status OSSFileSystem::batch_delete_impl(const std::vector<Path>& remote_files) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    // OSS DeleteObjects can handle up to 1000 keys at most
    constexpr size_t max_delete_batch = 1000;
    auto path_iter = remote_files.begin();

    try {
        do {
            AlibabaCloud::OSS::DeletedKeyList keys;
            auto path_begin = path_iter;

            for (; path_iter != remote_files.end() && (path_iter - path_begin < max_delete_batch);
                 ++path_iter) {
                auto key = DORIS_TRY(get_oss_key(*path_iter));
                keys.push_back(key);
            }

            if (keys.empty()) {
                break;
            }

            AlibabaCloud::OSS::DeleteObjectsRequest request(_bucket, keys);
            auto outcome = client->DeleteObjects(request);

            if (!outcome.isSuccess()) {
                return Status::IOError("failed to batch delete objects: {} - {}",
                                       outcome.error().Code(), outcome.error().Message());
            }
        } while (path_iter != remote_files.end());

        return Status::OK();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return Status::IOError("failed to batch delete: {} - {}", e.GetErrorCode(),
                               e.GetErrorMessage());
    }
}

Status OSSFileSystem::exists_impl(const Path& path, bool* res) const {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto key = DORIS_TRY(get_oss_key(path));

    try {
        *res = client->DoesObjectExist(_bucket, key);
        return Status::OK();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return Status::IOError("failed to check existence of {}: {} - {}", full_oss_path(key),
                               e.GetErrorCode(), e.GetErrorMessage());
    }
}

Status OSSFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    auto key = DORIS_TRY(get_oss_key(file));
    *file_size = DORIS_TRY(_client->object_file_size(_bucket, key));
    return Status::OK();
}

Status OSSFileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                                bool* exists) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto prefix = DORIS_TRY(get_oss_key(dir));
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    *exists = false;

    try {
        bool is_truncated = true;
        std::string marker;

        while (is_truncated) {
            AlibabaCloud::OSS::ListObjectsRequest request(_bucket);
            request.setPrefix(prefix);
            request.setMaxKeys(1000);

            if (!marker.empty()) {
                request.setMarker(marker);
            }

            auto outcome = client->ListObjects(request);
            if (!outcome.isSuccess()) {
                return Status::IOError("failed to list objects: {} - {}", outcome.error().Code(),
                                       outcome.error().Message());
            }

            const auto& result = outcome.result();
            const auto& objects = result.ObjectSummarys();

            if (!objects.empty()) {
                *exists = true;
            }

            for (const auto& obj : objects) {
                FileInfo file_info;
                file_info.file_name = obj.Key();
                // Remove prefix from file name
                if (file_info.file_name.find(prefix) == 0) {
                    file_info.file_name = file_info.file_name.substr(prefix.length());
                }

                file_info.file_size = obj.Size();
                file_info.is_file = true; // OSS objects are always files

                files->push_back(std::move(file_info));
            }

            is_truncated = result.IsTruncated();
            marker = result.NextMarker();
        }

        return Status::OK();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return Status::IOError("failed to list {}: {} - {}", full_oss_path(prefix),
                               e.GetErrorCode(), e.GetErrorMessage());
    }
}

Status OSSFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    // OSS doesn't support rename directly, need to copy then delete
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto src_key = DORIS_TRY(get_oss_key(orig_name));
    auto dst_key = DORIS_TRY(get_oss_key(new_name));

    try {
        // Copy object
        AlibabaCloud::OSS::CopyObjectRequest copy_request(_bucket, dst_key);
        copy_request.setCopySource(_bucket, src_key);

        auto copy_outcome = client->CopyObject(copy_request);
        if (!copy_outcome.isSuccess()) {
            return Status::IOError("failed to copy object from {} to {}: {} - {}",
                                   full_oss_path(src_key), full_oss_path(dst_key),
                                   copy_outcome.error().Code(), copy_outcome.error().Message());
        }

        // Delete source object
        AlibabaCloud::OSS::DeleteObjectRequest delete_request(_bucket, src_key);
        auto delete_outcome = client->DeleteObject(delete_request);

        if (!delete_outcome.isSuccess()) {
            LOG(WARNING) << "Failed to delete source object after copy: " << src_key << " - "
                         << delete_outcome.error().Code() << ": "
                         << delete_outcome.error().Message();
            // Don't fail the rename if delete fails, copy succeeded
        }

        return Status::OK();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return Status::IOError("failed to rename from {} to {}: {} - {}", full_oss_path(src_key),
                               full_oss_path(dst_key), e.GetErrorCode(), e.GetErrorMessage());
    }
}

Status OSSFileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto key = DORIS_TRY(get_oss_key(remote_file));

    try {
        AlibabaCloud::OSS::PutObjectRequest request(_bucket, key, local_file.native());

        auto outcome = client->PutObject(request);
        if (!outcome.isSuccess()) {
            return Status::IOError("failed to upload file to {}: {} - {}", full_oss_path(key),
                                   outcome.error().Code(), outcome.error().Message());
        }

        return Status::OK();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return Status::IOError("failed to upload file to {}: {} - {}", full_oss_path(key),
                               e.GetErrorCode(), e.GetErrorMessage());
    }
}

Status OSSFileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                        const std::vector<Path>& remote_files) {
    if (local_files.size() != remote_files.size()) {
        return Status::InvalidArgument("local_files and remote_files size mismatch");
    }

    for (size_t i = 0; i < local_files.size(); ++i) {
        RETURN_IF_ERROR(upload_impl(local_files[i], remote_files[i]));
    }

    return Status::OK();
}

Status OSSFileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto key = DORIS_TRY(get_oss_key(remote_file));

    try {
        AlibabaCloud::OSS::GetObjectRequest request(_bucket, key);

        auto outcome = client->GetObject(request);
        if (!outcome.isSuccess()) {
            return Status::IOError("failed to download file from {}: {} - {}", full_oss_path(key),
                                   outcome.error().Code(), outcome.error().Message());
        }

        // Write to local file
        std::ofstream out(local_file.native(), std::ios::binary);
        if (!out) {
            return Status::IOError("failed to open local file for writing: {}", local_file.native());
        }

        auto& content_stream = outcome.result().Content();
        out << content_stream->rdbuf();

        if (!out.good()) {
            return Status::IOError("failed to write to local file: {}", local_file.native());
        }

        return Status::OK();
    } catch (const AlibabaCloud::OSS::OssException& e) {
        return Status::IOError("failed to download file from {}: {} - {}", full_oss_path(key),
                               e.GetErrorCode(), e.GetErrorMessage());
    }
}

} // namespace doris::io
