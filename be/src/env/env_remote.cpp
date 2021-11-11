//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#include "common/logging.h"
#include "common/status.h"
#include "env/env.h"
#include "env/env_remote.h"
#include "util/s3_storage_backend.h"
#include "util/s3_util.h"

#include "gutil/strings/substitute.h"

namespace doris {

using std::string;
using strings::Substitute;

std::map<std::string, std::string> RemoteEnv::_storage_prop;

class RemoteRandomAccessFile : public RandomAccessFile {
public:
    RemoteRandomAccessFile(std::string filename, std::map<std::string, std::string> storage_prop)
            : _filename(std::move(filename)), _storage_backend(new S3StorageBackend(storage_prop)) {}
    ~RemoteRandomAccessFile() {
    }

    Status read_at(uint64_t offset, const Slice* result) const override {
        return readv_at(offset, result, 1);
    }

    Status readv_at(uint64_t offset, const Slice* result, size_t res_cnt) const override {
        return Status::IOError("No support", 1, "");
    }
    Status size(uint64_t* size) const override {
        return Status::IOError("No support", 1, "");
    }

    const std::string& file_name() const override { return _filename; }

private:
    const std::string _filename;
    std::unique_ptr<StorageBackend> _storage_backend;
};

class RemoteWritableFile : public WritableFile {
public:
    RemoteWritableFile(std::string filename, std::map<std::string, std::string> storage_prop, uint64_t filesize)
            : _filename(std::move(filename)),
              _storage_backend(new S3StorageBackend(storage_prop)),
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
        RETURN_NOT_OK_STATUS_WITH_WARN(status, strings::Substitute(
                "direct_upload failed: $0, err=$1", _filename, status.to_string()));
        _filesize += bytes_written;
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override {
        return Status::IOError("No support", 1, "");
    }

    Status close() override {
        return Status::OK();
    }

    Status flush(FlushMode mode) override {
        return Status::OK();
    }

    Status sync() override {
        return Status::OK();
    }

    uint64_t size() const override { return _filesize; }
    const string& filename() const override { return _filename; }

private:
    std::string _filename;
    std::unique_ptr<StorageBackend> _storage_backend;
    uint64_t _filesize = 0;
};

class RemoteRandomRWFile : public RandomRWFile {
public:
    RemoteRandomRWFile(const FilePathDesc& path_desc) : _path_desc(path_desc) {}

    ~RemoteRandomRWFile() { WARN_IF_ERROR(close(), "Failed to close " + _path_desc.filepath); }

    virtual Status read_at(uint64_t offset, const Slice& result) const {
        return Status::IOError("No support", 1, "");
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
        return Status::IOError("No support", 1, "");
    }

    Status write_at(uint64_t offset, const Slice& data) {
        return Status::IOError("No support", 1, "");
    }

    Status writev_at(uint64_t offset, const Slice* data, size_t data_cnt) {
        return Status::IOError("No support", 1, "");
    }

    Status flush(FlushMode mode, uint64_t offset, size_t length) {
        return Status::IOError("No support", 1, "");
    }

    Status sync() {
        return Status::IOError("No support", 1, "");
    }

    Status close() {
        return Status::IOError("No support", 1, "");
    }

    Status size(uint64_t* size) const {
        return Status::IOError("No support", 1, "");
    }

    const string& filename() const { return _path_desc.filepath; }

private:
    const FilePathDesc _path_desc;
    const bool _sync_on_close = false;
    bool _closed = false;
};

Status RemoteEnv::new_sequential_file(const std::string& fname,
                                      std::unique_ptr<SequentialFile>* result) {
    return Status::IOError(strings::Substitute("Unable to new_sequential_file $0", fname), 0, "");
}

// get a RandomAccessFile pointer without file cache
Status RemoteEnv::new_random_access_file(const std::string& fname,
                                         std::unique_ptr<RandomAccessFile>* result) {
    return new_random_access_file(RandomAccessFileOptions(), fname, result);
}

Status RemoteEnv::new_random_access_file(const RandomAccessFileOptions& opts, const std::string& fname,
                                         std::unique_ptr<RandomAccessFile>* result) {
    result->reset(new RemoteRandomAccessFile(fname, _storage_prop));
    return Status::OK();
}

Status RemoteEnv::new_writable_file(const std::string& fname, std::unique_ptr<WritableFile>* result) {
    return new_writable_file(WritableFileOptions(), fname, result);
}

Status RemoteEnv::new_writable_file(const WritableFileOptions& opts, const std::string& fname,
                                    std::unique_ptr<WritableFile>* result) {
    uint64_t file_size = 0;
    if (opts.mode == MUST_EXIST) {
        RETURN_IF_ERROR(get_file_size(fname, &file_size));
    }
    result->reset(new RemoteWritableFile(fname, _storage_prop, file_size));
    return Status::OK();
}

Status RemoteEnv::new_random_rw_file(const std::string& fname, std::unique_ptr<RandomRWFile>* result) {
    return new_random_rw_file(RandomRWFileOptions(), fname, result);
}

Status RemoteEnv::new_random_rw_file(const RandomRWFileOptions& opts, const std::string& fname,
                                     std::unique_ptr<RandomRWFile>* result) {
    return Status::IOError(strings::Substitute("Unable to new_random_rw_file $0", fname), 0, "");
}

Status RemoteEnv::path_exists(const std::string& fname, bool is_dir) {
    std::unique_ptr<StorageBackend> storage_backend(new S3StorageBackend(_storage_prop));
    Status = storage_backend->exist(fname);
    RETURN_NOT_OK_STATUS_WITH_WARN(status, strings::Substitute(
            "path_exists failed: $0, err=$1", fname, status.to_string()));
    return Status::OK();
}

Status RemoteEnv::get_children(const std::string& dir, std::vector<std::string>* result) {
    return Status::IOError(strings::Substitute("Unable to get_children $0", dir), 0, "");
}

Status RemoteEnv::iterate_dir(const std::string& dir,
                              const std::function<bool(const char*)>& cb) {
    return Status::IOError(strings::Substitute("Unable to iterate_dir $0", dir), 0, "");
}

Status RemoteEnv::delete_file(const std::string& fname) {
    std::unique_ptr<StorageBackend> storage_backend(new S3StorageBackend(_storage_prop));
    Status status = storage_backend->rm(fname);
    RETURN_NOT_OK_STATUS_WITH_WARN(status, strings::Substitute(
            "delete_file failed: $0, err=$1", fname, status.to_string()));
    return Status::OK();
}

Status RemoteEnv::create_dir(const std::string& name) {
    return Status::OK();
}

Status RemoteEnv::create_dir_if_missing(const string& dirname, bool* created) {
    *created = true;
    return Status::OK();
}

Status RemoteEnv::create_dirs(const string& dirname) {
    return Status::OK();
}

// Delete the specified directory.
Status RemoteEnv::delete_dir(const std::string& dirname) {
    // TODO
    return Status::OK();
}

Status RemoteEnv::sync_dir(const string& dirname) {
    return Status::IOError(strings::Substitute("Unable to sync_dir $0", dirname), 0, "");
}

Status RemoteEnv::is_directory(const std::string& path, bool* is_dir) {
    // TODO
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
    return Status::IOError(strings::Substitute("Unable to get_file_modified_time $0", fname), 0, "");
}

Status RemoteEnv::copy_path(const std::string& src, const std::string& target) {
    return Status::IOError(strings::Substitute("Unable to copy_path $0 to $1", src, target), 0, "");
}

Status RemoteEnv::rename_file(const std::string& src, const std::string& target) {
    std::unique_ptr<StorageBackend> storage_backend(new S3StorageBackend(_storage_prop));
    Status status = storage_backend->rename(src, target);
    RETURN_NOT_OK_STATUS_WITH_WARN(status, strings::Substitute(
            "rename_file failed: from $0 to $1, err=$2", src, target, status.to_string()));
    return Status::OK();
}

Status RemoteEnv::rename_dir(const std::string& src, const std::string& target) {
    // TODO
    return Status::OK();
}

Status RemoteEnv::link_file(const std::string& old_path, const std::string& new_path) {
    std::unique_ptr<StorageBackend> storage_backend(new S3StorageBackend(_storage_prop));
    Status status = storage_backend->copy(old_path, new_path);
    RETURN_NOT_OK_STATUS_WITH_WARN(status, strings::Substitute(
            "link_file failed: from $0 to $1, err=$2", old_path, new_path, status.to_string()));
    return Status::OK();
}

Status RemoteEnv::get_space_info(const std::string& path, int64_t* capacity, int64_t* available) {
    *capacity = -1;
    *available = -1;
    return Status::OK();
}

std::unique_ptr<StorageBackend> RemoteEnv::get_storage_backend() {
    std::unique_ptr<StorageBackend> storage_backend(new S3StorageBackend(_storage_prop));
    return storage_backend;
}

} // end namespace doris
