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

#include "env/env_remote.h"

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "util/s3_storage_backend.h"
#include "util/s3_util.h"

namespace doris {

using std::string;
using strings::Substitute;

class RemoteRandomAccessFile : public RandomAccessFile {
public:
    RemoteRandomAccessFile(std::string filename, std::shared_ptr<StorageBackend> storage_backend)
            : _filename(std::move(filename)), _storage_backend(storage_backend) {}
    ~RemoteRandomAccessFile() {}

    Status read_at(uint64_t offset, const Slice* result) const override {
        return readv_at(offset, result, 1);
    }

    Status readv_at(uint64_t offset, const Slice* result, size_t res_cnt) const override {
        return Status::NotSupported("No support", 1, "");
    }
    Status read_all(std::string* content) const override {
        return _storage_backend->direct_download(_filename, content);
    }
    Status size(uint64_t* size) const override { return Status::NotSupported("No support", 1, ""); }

    const std::string& file_name() const override { return _filename; }

private:
    const std::string _filename;
    std::shared_ptr<StorageBackend> _storage_backend;
};

class RemoteWritableFile : public WritableFile {
public:
    RemoteWritableFile(std::string filename, std::shared_ptr<StorageBackend> storage_backend,
                       uint64_t filesize)
            : _filename(std::move(filename)),
              _storage_backend(storage_backend),
              _filesize(filesize) {}

    ~RemoteWritableFile() override {
        WARN_IF_ERROR(close(), "Failed to close file, file=" + _filename);
    }

    Status append(const Slice& data) override { return appendv(&data, 1); }

    Status appendv(const Slice* data, size_t data_cnt) override {
        size_t bytes_written = 0;
        std::string content;
        for (size_t i = 0; i < data_cnt; i++) {
            content += data[i].to_string();
            bytes_written += data[i].size;
        }
        Status status = _storage_backend->direct_upload(_filename, content);
        RETURN_NOT_OK_STATUS_WITH_WARN(
                status, strings::Substitute("direct_upload failed: $0, err=$1", _filename,
                                            status.to_string()));
        _filesize += bytes_written;
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override {
        return Status::NotSupported("No support", 1, "");
    }

    Status close() override { return Status::OK(); }

    Status flush(FlushMode mode) override { return Status::OK(); }

    Status sync() override { return Status::OK(); }

    uint64_t size() const override { return _filesize; }
    const string& filename() const override { return _filename; }

private:
    std::string _filename;
    std::shared_ptr<StorageBackend> _storage_backend;
    uint64_t _filesize = 0;
};

class RemoteRandomRWFile : public RandomRWFile {
public:
    RemoteRandomRWFile(const FilePathDesc& path_desc) : _path_desc(path_desc) {}

    ~RemoteRandomRWFile() { WARN_IF_ERROR(close(), "Failed to close " + _path_desc.filepath); }
    virtual Status read_at(uint64_t offset, const Slice& result) const {
        return Status::NotSupported("No support", 1, "");
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
        return Status::NotSupported("No support", 1, "");
    }

    Status write_at(uint64_t offset, const Slice& data) {
        return Status::NotSupported("No support", 1, "");
    }

    Status writev_at(uint64_t offset, const Slice* data, size_t data_cnt) {
        return Status::NotSupported("No support", 1, "");
    }

    Status flush(FlushMode mode, uint64_t offset, size_t length) {
        return Status::NotSupported("No support", 1, "");
    }

    Status sync() { return Status::NotSupported("No support", 1, ""); }

    Status close() { return Status::NotSupported("No support", 1, ""); }

    Status size(uint64_t* size) const { return Status::NotSupported("No support", 1, ""); }

    const string& filename() const { return _path_desc.filepath; }

private:
    const FilePathDesc _path_desc;
};

Status RemoteEnv::init_conf() {
    std::map<std::string, std::string> storage_prop;
    if (doris::config::default_remote_storage_s3_ak.empty() ||
        doris::config::default_remote_storage_s3_sk.empty() ||
        doris::config::default_remote_storage_s3_endpoint.empty() ||
        doris::config::default_remote_storage_s3_region.empty()) {
        return Status::OK();
    }
    storage_prop[S3_AK] = doris::config::default_remote_storage_s3_ak;
    storage_prop[S3_SK] = doris::config::default_remote_storage_s3_sk;
    storage_prop[S3_ENDPOINT] = doris::config::default_remote_storage_s3_endpoint;
    storage_prop[S3_REGION] = doris::config::default_remote_storage_s3_region;
    storage_prop[S3_MAX_CONN_SIZE] =
            std::to_string(doris::config::default_remote_storage_s3_max_conn);
    storage_prop[S3_REQUEST_TIMEOUT_MS] =
            std::to_string(doris::config::default_remote_storage_s3_request_timeout_ms);
    storage_prop[S3_CONN_TIMEOUT_MS] =
            std::to_string(doris::config::default_remote_storage_s3_conn_timeout_ms);

    if (ClientFactory::is_s3_conf_valid(storage_prop)) {
        _storage_backend.reset(new S3StorageBackend(storage_prop));
    }
    return Status::OK();
}

Status RemoteEnv::new_sequential_file(const std::string& fname,
                                      std::unique_ptr<SequentialFile>* result) {
    return Status::IOError(strings::Substitute("Unable to new_sequential_file $0", fname), 0, "");
}

// get a RandomAccessFile pointer without file cache
Status RemoteEnv::new_random_access_file(const std::string& fname,
                                         std::unique_ptr<RandomAccessFile>* result) {
    return new_random_access_file(RandomAccessFileOptions(), fname, result);
}

Status RemoteEnv::new_random_access_file(const RandomAccessFileOptions& opts,
                                         const std::string& fname,
                                         std::unique_ptr<RandomAccessFile>* result) {
    result->reset(new RemoteRandomAccessFile(fname, _storage_backend));
    return Status::OK();
}

Status RemoteEnv::new_writable_file(const std::string& fname,
                                    std::unique_ptr<WritableFile>* result) {
    return new_writable_file(WritableFileOptions(), fname, result);
}

Status RemoteEnv::new_writable_file(const WritableFileOptions& opts, const std::string& fname,
                                    std::unique_ptr<WritableFile>* result) {
    uint64_t file_size = 0;
    if (opts.mode == MUST_EXIST) {
        RETURN_IF_ERROR(get_file_size(fname, &file_size));
    }
    result->reset(new RemoteWritableFile(fname, _storage_backend, file_size));
    return Status::OK();
}

Status RemoteEnv::new_random_rw_file(const std::string& fname,
                                     std::unique_ptr<RandomRWFile>* result) {
    return new_random_rw_file(RandomRWFileOptions(), fname, result);
}

Status RemoteEnv::new_random_rw_file(const RandomRWFileOptions& opts, const std::string& fname,
                                     std::unique_ptr<RandomRWFile>* result) {
    return Status::IOError(strings::Substitute("Unable to new_random_rw_file $0", fname), 0, "");
}

Status RemoteEnv::path_exists(const std::string& fname, bool is_dir) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    Status status = Status::OK();
    if (is_dir) {
        status = storage_backend->exist_dir(fname);
    } else {
        status = storage_backend->exist(fname);
    }
    RETURN_NOT_OK_STATUS_WITH_WARN(status, strings::Substitute("path_exists failed: $0, err=$1",
                                                               fname, status.to_string()));
    return Status::OK();
}

Status RemoteEnv::get_children(const std::string& dir, std::vector<std::string>* result) {
    return Status::IOError(strings::Substitute("Unable to get_children $0", dir), 0, "");
}

Status RemoteEnv::iterate_dir(const std::string& dir, const std::function<bool(const char*)>& cb) {
    return Status::IOError(strings::Substitute("Unable to iterate_dir $0", dir), 0, "");
}

Status RemoteEnv::delete_file(const std::string& fname) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    Status status = storage_backend->rm(fname);
    RETURN_NOT_OK_STATUS_WITH_WARN(status, strings::Substitute("delete_file failed: $0, err=$1",
                                                               fname, status.to_string()));
    return Status::OK();
}

Status RemoteEnv::create_dir(const std::string& name) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    return storage_backend->mkdir(name);
}

Status RemoteEnv::create_dir_if_missing(const string& dirname, bool* created) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    if (storage_backend->exist_dir(dirname)) {
        *created = true;
        return Status::OK();
    }
    return storage_backend->mkdir(dirname);
}

Status RemoteEnv::create_dirs(const string& dirname) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    return storage_backend->mkdirs(dirname);
}

// Delete the specified directory.
Status RemoteEnv::delete_dir(const std::string& dirname) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    Status status = storage_backend->rmdir(dirname);
    RETURN_NOT_OK_STATUS_WITH_WARN(status, strings::Substitute("delete_dir failed: $0, err=$1",
                                                               dirname, status.to_string()));
    return Status::OK();
}

Status RemoteEnv::sync_dir(const string& dirname) {
    return Status::IOError(strings::Substitute("Unable to sync_dir $0", dirname), 0, "");
}

Status RemoteEnv::is_directory(const std::string& path, bool* is_dir) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    Status status = storage_backend->exist(path);
    if (status.ok()) {
        *is_dir = false;
        return Status::OK();
    }
    if (!status.is_not_found()) {
        return status;
    }

    status = storage_backend->exist_dir(path);
    if (status.ok()) {
        *is_dir = true;
        return Status::OK();
    }
    if (!status.is_not_found()) {
        return status;
    }

    *is_dir = false;
    return Status::OK();
}

Status RemoteEnv::canonicalize(const std::string& path, std::string* result) {
    *result = path;
    return Status::OK();
}

Status RemoteEnv::get_file_size(const std::string& fname, uint64_t* size) {
    return Status::OK();
    // return EnvBos::get_file_size(fname, size);
}

Status RemoteEnv::get_file_modified_time(const std::string& fname, uint64_t* file_mtime) {
    return Status::IOError(strings::Substitute("Unable to get_file_modified_time $0", fname), 0,
                           "");
}

Status RemoteEnv::copy_path(const std::string& src, const std::string& target) {
    return Status::IOError(strings::Substitute("Unable to copy_path $0 to $1", src, target), 0, "");
}

Status RemoteEnv::rename_file(const std::string& src, const std::string& target) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    Status status = storage_backend->rename(src, target);
    RETURN_NOT_OK_STATUS_WITH_WARN(
            status, strings::Substitute("rename_file failed: from $0 to $1, err=$2", src, target,
                                        status.to_string()));
    return Status::OK();
}

Status RemoteEnv::rename_dir(const std::string& src, const std::string& target) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    Status status = storage_backend->rename_dir(src, target);
    RETURN_NOT_OK_STATUS_WITH_WARN(
            status, strings::Substitute("rename_dir failed: from $0 to $1, err=$2", src, target,
                                        status.to_string()));
    return Status::OK();
}

Status RemoteEnv::link_file(const std::string& old_path, const std::string& new_path) {
    std::shared_ptr<StorageBackend> storage_backend = get_storage_backend();
    Status status = storage_backend->copy(old_path, new_path);
    RETURN_NOT_OK_STATUS_WITH_WARN(
            status, strings::Substitute("link_file failed: from $0 to $1, err=$2", old_path,
                                        new_path, status.to_string()));
    return Status::OK();
}

Status RemoteEnv::get_space_info(const std::string& path, int64_t* capacity, int64_t* available) {
    *capacity = -1;
    *available = -1;
    return Status::OK();
}

std::shared_ptr<StorageBackend> RemoteEnv::get_storage_backend() {
    return _storage_backend;
}

} // end namespace doris
