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
#include "io/fs/oss_file_reader.h"
#include "io/fs/oss_file_writer.h"
#include "io/fs/remote_file_system.h"

namespace doris::io {
namespace {

constexpr int64_t MULTIPART_COPY_THRESHOLD = 1073741824; // 1GB
constexpr int64_t MULTIPART_COPY_PART_SIZE = 104857600;  // 100MB per part

#ifndef CHECK_OSS_CLIENT
#define CHECK_OSS_CLIENT(client)                                 \
    if (!client) {                                               \
        return Status::InvalidArgument("init oss client error"); \
    }
#endif

Result<std::string> get_oss_key(const Path& full_path) {
    std::string path_str = full_path.native();

    const std::string oss_prefix = "oss://";
    if (path_str.find(oss_prefix) == 0) {
        path_str = path_str.substr(oss_prefix.length());

        size_t pos = path_str.find('/');
        if (pos == std::string::npos) {
            return ""; // No key, just bucket
        }
        return path_str.substr(pos + 1);
    }

    return path_str;
}

} // namespace

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
            return Status::OK();
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

    auto outcome = client->HeadObject(bucket, key);
    if (!outcome.isSuccess()) {
        return ResultError(Status::IOError("failed to head oss file {}: {} - {}",
                                           full_oss_path(bucket, key), outcome.error().Code(),
                                           outcome.error().Message()));
    }

    return outcome.result().ContentLength();
}

std::string OSSClientHolder::full_oss_path(std::string_view bucket, std::string_view key) const {
    return fmt::format("{}/{}/{}", _conf.endpoint, bucket, key);
}

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
    auto key = DORIS_TRY(get_oss_key(file));
    *writer = std::make_unique<OSSFileWriter>(_client, _bucket, key, opts);
    return Status::OK();
}

Status OSSFileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                         const FileReaderOptions& opts) {
    auto key = DORIS_TRY(get_oss_key(file));
    int64_t fsize = opts.file_size;

    auto oss_reader = DORIS_TRY(OSSFileReader::create(_client, _bucket, key, fsize));
    *reader = oss_reader;
    return Status::OK();
}

Status OSSFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    return Status::OK();
}

Status OSSFileSystem::delete_file_impl(const Path& file) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto key = DORIS_TRY(get_oss_key(file));

    auto outcome = client->DeleteObject(_bucket, key);

    if (!outcome.isSuccess()) {
        std::string error_code = outcome.error().Code();
        if (error_code != "NoSuchKey") {
            return Status::IOError("failed to delete file {}: {} - {}", full_oss_path(key),
                                   error_code, outcome.error().Message());
        }
    }

    return Status::OK();
}

Status OSSFileSystem::delete_directory_impl(const Path& dir) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto prefix = DORIS_TRY(get_oss_key(dir));
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    // Abort in-progress multipart uploads
    {
        bool is_truncated = true;
        std::string key_marker;
        std::string upload_id_marker;
        int aborted_count = 0;

        while (is_truncated) {
            AlibabaCloud::OSS::ListMultipartUploadsRequest list_uploads_request(_bucket);
            list_uploads_request.setPrefix(prefix);
            list_uploads_request.setMaxUploads(1000);

            if (!key_marker.empty()) {
                list_uploads_request.setKeyMarker(key_marker);
            }
            if (!upload_id_marker.empty()) {
                list_uploads_request.setUploadIdMarker(upload_id_marker);
            }

            auto list_uploads_outcome = client->ListMultipartUploads(list_uploads_request);
            if (!list_uploads_outcome.isSuccess()) {
                LOG(WARNING) << "Failed to list multipart uploads for prefix " << prefix << ": "
                             << list_uploads_outcome.error().Code() << " - "
                             << list_uploads_outcome.error().Message()
                             << ". Continuing with object deletion.";
                break; // Don't fail deletion if listing uploads fails
            }

            const auto& uploads_result = list_uploads_outcome.result();
            const auto& uploads = uploads_result.MultipartUploadList();

            for (const auto& upload : uploads) {
                AlibabaCloud::OSS::AbortMultipartUploadRequest abort_request(
                        _bucket, upload.Key(), upload.UploadId());
                auto abort_outcome = client->AbortMultipartUpload(abort_request);

                if (!abort_outcome.isSuccess()) {
                    LOG(WARNING) << "Failed to abort multipart upload: key=" << upload.Key()
                                 << " upload_id=" << upload.UploadId() << " error="
                                 << abort_outcome.error().Code() << " - "
                                 << abort_outcome.error().Message();
                    // Don't fail directory deletion if abort fails
                } else {
                    aborted_count++;
                    VLOG(1) << "Aborted multipart upload: key=" << upload.Key()
                            << " upload_id=" << upload.UploadId();
                }
            }

            is_truncated = uploads_result.IsTruncated();
            key_marker = uploads_result.NextKeyMarker();
            upload_id_marker = uploads_result.NextUploadIdMarker();
        }

        if (aborted_count > 0) {
            LOG(INFO) << "Aborted " << aborted_count
                      << " in-progress multipart uploads under prefix: " << prefix;
        }
    }

    // Delete all objects with prefix
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

        if (!objects.empty()) {
            AlibabaCloud::OSS::DeletedKeyList keys;
            for (const auto& obj : objects) {
                keys.push_back(obj.Key());
            }

            AlibabaCloud::OSS::DeleteObjectsRequest delete_request(_bucket);
            delete_request.setKeyList(keys);
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
}

Status OSSFileSystem::batch_delete_impl(const std::vector<Path>& remote_files) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    constexpr size_t max_delete_batch = 1000;
    auto path_iter = remote_files.begin();

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

        AlibabaCloud::OSS::DeleteObjectsRequest request(_bucket);
        request.setKeyList(keys);
        auto outcome = client->DeleteObjects(request);

        if (!outcome.isSuccess()) {
            return Status::IOError("failed to batch delete objects: {} - {}",
                                   outcome.error().Code(), outcome.error().Message());
        }
    } while (path_iter != remote_files.end());

    return Status::OK();
}

Status OSSFileSystem::exists_impl(const Path& path, bool* res) const {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto key = DORIS_TRY(get_oss_key(path));

    *res = client->DoesObjectExist(_bucket, key);
    return Status::OK();
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

    bool is_truncated = true;
    std::string marker;

    while (is_truncated) {
        AlibabaCloud::OSS::ListObjectsRequest request(_bucket);
        request.setPrefix(prefix);
        request.setMaxKeys(1000);

        // Use delimiter for non-recursive listing to prevent memory exhaustion
        if (only_file) {
            request.setDelimiter("/");
        }

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
            file_info.is_file = true;

            files->push_back(std::move(file_info));
        }

        is_truncated = result.IsTruncated();
        marker = result.NextMarker();
    }

    return Status::OK();
}

Status OSSFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto src_key = DORIS_TRY(get_oss_key(orig_name));
    auto dst_key = DORIS_TRY(get_oss_key(new_name));

    // Get source file size to determine copy strategy
    int64_t file_size = DORIS_TRY(_client->object_file_size(_bucket, src_key));

    if (file_size < MULTIPART_COPY_THRESHOLD) {
        // Simple copy for files < 1GB
        AlibabaCloud::OSS::CopyObjectRequest copy_request(_bucket, dst_key);
        copy_request.setCopySource(_bucket, src_key);

        auto copy_outcome = client->CopyObject(copy_request);
        if (!copy_outcome.isSuccess()) {
            return Status::IOError("failed to copy object from {} to {}: {} - {}",
                                   full_oss_path(src_key), full_oss_path(dst_key),
                                   copy_outcome.error().Code(),
                                   copy_outcome.error().Message());
        }
    } else {
        // Multipart copy for large files to avoid timeouts
        auto init_outcome = client->InitiateMultipartUpload(
                AlibabaCloud::OSS::InitiateMultipartUploadRequest(_bucket, dst_key));
        if (!init_outcome.isSuccess()) {
            return Status::IOError("failed to initiate multipart upload: {} - {}",
                                   init_outcome.error().Code(),
                                   init_outcome.error().Message());
        }

        std::string upload_id = init_outcome.result().UploadId();
        AlibabaCloud::OSS::PartList part_etags;

        // Calculate number of parts
        int64_t part_count = (file_size + MULTIPART_COPY_PART_SIZE - 1) / MULTIPART_COPY_PART_SIZE;

        // Copy parts
        for (int64_t i = 0; i < part_count; ++i) {
            int64_t start_offset = i * MULTIPART_COPY_PART_SIZE;
            int64_t end_offset = std::min(start_offset + MULTIPART_COPY_PART_SIZE - 1, file_size - 1);

            AlibabaCloud::OSS::UploadPartCopyRequest part_request(_bucket, dst_key, _bucket, src_key);
            part_request.setUploadId(upload_id);
            part_request.setPartNumber(i + 1);
            part_request.setCopySourceRange(start_offset, end_offset);

            auto part_outcome = client->UploadPartCopy(part_request);
            if (!part_outcome.isSuccess()) {
                // Abort multipart upload on failure
                client->AbortMultipartUpload(
                        AlibabaCloud::OSS::AbortMultipartUploadRequest(_bucket, dst_key,
                                                                        upload_id));
                return Status::IOError("failed to copy part {}: {} - {}", i + 1,
                                       part_outcome.error().Code(),
                                       part_outcome.error().Message());
            }

            part_etags.push_back(AlibabaCloud::OSS::Part(i + 1, part_outcome.result().ETag()));
        }

        // Complete multipart upload
        AlibabaCloud::OSS::CompleteMultipartUploadRequest complete_request(_bucket, dst_key,
                                                                            part_etags, upload_id);
        auto complete_outcome = client->CompleteMultipartUpload(complete_request);
        if (!complete_outcome.isSuccess()) {
            client->AbortMultipartUpload(
                    AlibabaCloud::OSS::AbortMultipartUploadRequest(_bucket, dst_key, upload_id));
            return Status::IOError("failed to complete multipart upload: {} - {}",
                                   complete_outcome.error().Code(),
                                   complete_outcome.error().Message());
        }
    }

    // Delete source object
    auto delete_outcome = client->DeleteObject(_bucket, src_key);
    if (!delete_outcome.isSuccess()) {
        LOG(WARNING) << "Failed to delete source object after copy: " << src_key << " - "
                     << delete_outcome.error().Code() << ": "
                     << delete_outcome.error().Message();
        // Don't fail rename if delete fails, copy succeeded
    }

    return Status::OK();
}

Status OSSFileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    auto client = _client->get();
    CHECK_OSS_CLIENT(client);

    auto key = DORIS_TRY(get_oss_key(remote_file));

    auto outcome = client->PutObject(_bucket, key, local_file.native());
    if (!outcome.isSuccess()) {
        return Status::IOError("failed to upload file to {}: {} - {}", full_oss_path(key),
                               outcome.error().Code(), outcome.error().Message());
    }

    return Status::OK();
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

    AlibabaCloud::OSS::GetObjectRequest request(_bucket, key);

    auto outcome = client->GetObject(request);
    if (!outcome.isSuccess()) {
        return Status::IOError("failed to download file from {}: {} - {}", full_oss_path(key),
                               outcome.error().Code(), outcome.error().Message());
    }

    // Use temp file + rename pattern to ensure atomicity and proper cleanup on failure
    std::string temp_file_path = local_file.native() + ".tmp." + std::to_string(getpid());

    // RAII wrapper to ensure temp file cleanup on failure
    struct TempFileGuard {
        std::string path;
        bool success = false;

        ~TempFileGuard() {
            if (!success && !path.empty()) {
                // Clean up temp file on failure
                std::error_code ec;
                std::filesystem::remove(path, ec);
                if (ec) {
                    LOG(WARNING) << "Failed to remove temp file " << path << ": " << ec.message();
                }
            }
        }
    } temp_guard{temp_file_path};

    // Write to temp file
    std::ofstream out(temp_file_path, std::ios::binary);
    if (!out) {
        return Status::IOError("failed to open temp file for writing: {}", temp_file_path);
    }

    auto& content_stream = outcome.result().Content();
    out << content_stream->rdbuf();

    if (!out.good()) {
        return Status::IOError("failed to write to temp file: {}", temp_file_path);
    }

    out.close();
    if (!out) {
        return Status::IOError("failed to close temp file: {}", temp_file_path);
    }

    // Atomically rename temp file to target file
    std::error_code ec;
    std::filesystem::rename(temp_file_path, local_file.native(), ec);
    if (ec) {
        return Status::IOError("failed to rename temp file {} to {}: {}", temp_file_path,
                               local_file.native(), ec.message());
    }

    temp_guard.success = true; // Prevent cleanup of successfully renamed file
    return Status::OK();
}

} // namespace doris::io
