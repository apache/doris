//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <memory>

#include "common/logging.h"
#include "env/env.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "gutil/port.h"
#include "gutil/strings/substitute.h"
#include "util/errno.h"
#include "util/file_cache.h"
#include "util/slice.h"

namespace doris {

using std::string;
using strings::Substitute;

// Close file descriptor when object goes out of scope.
class ScopedFdCloser {
public:
    explicit ScopedFdCloser(int fd) : fd_(fd) {}

    ~ScopedFdCloser() {
        int err;
        RETRY_ON_EINTR(err, ::close(fd_));
        if (PREDICT_FALSE(err != 0)) {
            LOG(WARNING) << "Failed to close fd " << fd_;
        }
    }

private:
    const int fd_;
};

static Status io_error(const std::string& context, int err_number) {
    switch (err_number) {
    case EACCES:
    case ELOOP:
    case ENAMETOOLONG:
    case ENOENT:
    case ENOTDIR:
        return Status::NotFound(context, err_number, errno_to_string(err_number));
    case EEXIST:
        return Status::AlreadyExist(context, err_number, errno_to_string(err_number));
    case EOPNOTSUPP:
    case EXDEV: // No cross FS links allowed
        return Status::NotSupported(context, err_number, errno_to_string(err_number));
    case EIO:
        LOG(ERROR) << "I/O error, context=" << context;
    }
    return Status::IOError(context, err_number, errno_to_string(err_number));
}

static Status do_sync(int fd, const string& filename) {
    if (fdatasync(fd) < 0) {
        return io_error(filename, errno);
    }
    return Status::OK();
}

static Status do_open(const string& filename, Env::OpenMode mode, int* fd) {
    int flags = O_RDWR;
    switch (mode) {
    case Env::CREATE_OR_OPEN_WITH_TRUNCATE:
        flags |= O_CREAT | O_TRUNC;
        break;
    case Env::CREATE_OR_OPEN:
        flags |= O_CREAT;
        break;
    case Env::MUST_CREATE:
        flags |= O_CREAT | O_EXCL;
        break;
    case Env::MUST_EXIST:
        break;
    default:
        return Status::NotSupported(strings::Substitute("Unknown create mode $0", mode));
    }
    int f;
    RETRY_ON_EINTR(f, open(filename.c_str(), flags, 0666));
    if (f < 0) {
        return io_error(filename, errno);
    }
    *fd = f;
    return Status::OK();
}

static Status do_readv_at(int fd, const std::string& filename, uint64_t offset, const Slice* res,
                          size_t res_cnt) {
    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested
    size_t bytes_req = 0;
    struct iovec iov[res_cnt];
    for (size_t i = 0; i < res_cnt; i++) {
        const Slice& result = res[i];
        bytes_req += result.size;
        iov[i] = {result.data, result.size};
    }

    uint64_t cur_offset = offset;
    size_t completed_iov = 0;
    size_t rem = bytes_req;
    while (rem > 0) {
        // Never request more than IOV_MAX in one request
        size_t iov_count = std::min(res_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        ssize_t r;
        RETRY_ON_EINTR(r, preadv(fd, iov + completed_iov, iov_count, cur_offset));
        if (PREDICT_FALSE(r < 0)) {
            // An error: return a non-ok status.
            return io_error(filename, errno);
        }

        if (PREDICT_FALSE(r == 0)) {
            return Status::EndOfFile(strings::Substitute("EOF trying to read $0 bytes at offset $1",
                                                         bytes_req, offset));
        }

        if (PREDICT_TRUE(r == rem)) {
            // All requested bytes were read. This is almost always the case.
            return Status::OK();
        }
        DCHECK_LE(r, rem);
        // Adjust iovec vector based on bytes read for the next request
        ssize_t bytes_rem = r;
        for (size_t i = completed_iov; i < res_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // The full length of this iovec was read
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // Partially read this result.
                // Adjust the iov_len and iov_base to request only the missing data.
                iov[i].iov_base = static_cast<uint8_t*>(iov[i].iov_base) + bytes_rem;
                iov[i].iov_len -= bytes_rem;
                break; // Don't need to adjust remaining iovec's
            }
        }
        cur_offset += r;
        rem -= r;
    }
    DCHECK_EQ(0, rem);
    return Status::OK();
}

static Status do_writev_at(int fd, const string& filename, uint64_t offset, const Slice* data,
                           size_t data_cnt, size_t* bytes_written) {
    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested.
    size_t bytes_req = 0;
    struct iovec iov[data_cnt];
    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        bytes_req += result.size;
        iov[i] = {result.data, result.size};
    }

    uint64_t cur_offset = offset;
    size_t completed_iov = 0;
    size_t rem = bytes_req;
    while (rem > 0) {
        // Never request more than IOV_MAX in one request.
        size_t iov_count = std::min(data_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        ssize_t w;
        RETRY_ON_EINTR(w, pwritev(fd, iov + completed_iov, iov_count, cur_offset));
        if (PREDICT_FALSE(w < 0)) {
            // An error: return a non-ok status.
            return io_error(filename, errno);
        }

        if (PREDICT_TRUE(w == rem)) {
            // All requested bytes were read. This is almost always the case.
            rem = 0;
            break;
        }
        // Adjust iovec vector based on bytes read for the next request.
        ssize_t bytes_rem = w;
        for (size_t i = completed_iov; i < data_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // The full length of this iovec was written.
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // Partially wrote this result.
                // Adjust the iov_len and iov_base to write only the missing data.
                iov[i].iov_base = static_cast<uint8_t*>(iov[i].iov_base) + bytes_rem;
                iov[i].iov_len -= bytes_rem;
                break; // Don't need to adjust remaining iovec's.
            }
        }
        cur_offset += w;
        rem -= w;
    }
    DCHECK_EQ(0, rem);
    *bytes_written = bytes_req;
    return Status::OK();
}

class PosixSequentialFile : public SequentialFile {
public:
    PosixSequentialFile(string fname, FILE* f) : _filename(std::move(fname)), _file(f) {}

    ~PosixSequentialFile() override {
        int err;
        RETRY_ON_EINTR(err, fclose(_file));
        if (PREDICT_FALSE(err != 0)) {
            LOG(WARNING) << "Failed to close " << _filename
                         << ", msg=" << errno_to_string(ferror(_file));
        }
    }

    Status read(Slice* result) override {
        size_t r;
        STREAM_RETRY_ON_EINTR(r, _file, fread_unlocked(result->data, 1, result->size, _file));
        if (r < result->size) {
            if (feof(_file)) {
                // We leave status as ok if we hit the end of the file.
                // We need to adjust the slice size.
                result->truncate(r);
            } else {
                // A partial read with an error: return a non-ok status.
                return io_error(_filename, ferror(_file));
            }
        }
        return Status::OK();
    }

    Status skip(uint64_t n) override {
        if (fseek(_file, n, SEEK_CUR)) {
            return io_error(_filename, errno);
        }
        return Status::OK();
    }

    const string& filename() const override { return _filename; }

private:
    const std::string _filename;
    FILE* const _file;
};

class PosixRandomAccessFile : public RandomAccessFile {
public:
    PosixRandomAccessFile(std::string filename, int fd) : _filename(std::move(filename)), _fd(fd) {}
    ~PosixRandomAccessFile() override {
        int res;
        RETRY_ON_EINTR(res, close(_fd));
        if (res != 0) {
            LOG(WARNING) << "close file failed, name=" << _filename
                         << ", msg=" << errno_to_string(errno);
        }
    }

    Status read_at(uint64_t offset, const Slice& result) const override {
        return do_readv_at(_fd, _filename, offset, &result, 1);
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override {
        return do_readv_at(_fd, _filename, offset, res, res_cnt);
    }
    Status size(uint64_t* size) const override {
        struct stat st;
        auto res = fstat(_fd, &st);
        if (res != 0) {
            return io_error(_filename, errno);
        }
        *size = st.st_size;
        return Status::OK();
    }

    const std::string& file_name() const override { return _filename; }

private:
    std::string _filename;
    int _fd;
};

class PosixWritableFile : public WritableFile {
public:
    PosixWritableFile(std::string filename, int fd, uint64_t filesize, bool sync_on_close)
            : _filename(std::move(filename)),
              _fd(fd),
              _sync_on_close(sync_on_close),
              _filesize(filesize) {}

    ~PosixWritableFile() override {
        WARN_IF_ERROR(close(), "Failed to close file, file=" + _filename);
    }

    Status append(const Slice& data) override { return appendv(&data, 1); }

    Status appendv(const Slice* data, size_t cnt) override {
        size_t bytes_written = 0;
        RETURN_IF_ERROR(do_writev_at(_fd, _filename, _filesize, data, cnt, &bytes_written));
        _filesize += bytes_written;
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override {
        uint64_t offset = std::max(_filesize, _pre_allocated_size);
        int ret;
        RETRY_ON_EINTR(ret, fallocate(_fd, 0, offset, size));
        if (ret != 0) {
            if (errno == EOPNOTSUPP) {
                LOG(WARNING) << "The filesystem does not support fallocate().";
            } else if (errno == ENOSYS) {
                LOG(WARNING) << "The kernel does not implement fallocate().";
            } else {
                return io_error(_filename, errno);
            }
        }
        _pre_allocated_size = offset + size;
        return Status::OK();
    }

    Status close() override {
        if (_closed) {
            return Status::OK();
        }
        Status s;

        // If we've allocated more space than we used, truncate to the
        // actual size of the file and perform Sync().
        if (_filesize < _pre_allocated_size) {
            int ret;
            RETRY_ON_EINTR(ret, ftruncate(_fd, _filesize));
            if (ret != 0) {
                s = io_error(_filename, errno);
                _pending_sync = true;
            }
        }

        if (_sync_on_close) {
            Status sync_status = sync();
            if (!sync_status.ok()) {
                LOG(ERROR) << "Unable to Sync " << _filename << ": " << sync_status.to_string();
                if (s.ok()) {
                    s = sync_status;
                }
            }
        }

        int ret;
        RETRY_ON_EINTR(ret, ::close(_fd));
        if (ret < 0) {
            if (s.ok()) {
                s = io_error(_filename, errno);
            }
        }

        _closed = true;
        return s;
    }

    Status flush(FlushMode mode) override {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (mode == FLUSH_SYNC) {
            flags |= SYNC_FILE_RANGE_WAIT_BEFORE;
            flags |= SYNC_FILE_RANGE_WAIT_AFTER;
        }
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            return io_error(_filename, errno);
        }
#else
        if (mode == FLUSH_SYNC && fsync(_fd) < 0) {
            return io_error(_filename, errno);
        }
#endif
        return Status::OK();
    }

    Status sync() override {
        if (_pending_sync) {
            _pending_sync = false;
            RETURN_IF_ERROR(do_sync(_fd, _filename));
        }
        return Status::OK();
    }

    uint64_t size() const override { return _filesize; }
    const string& filename() const override { return _filename; }

private:
    std::string _filename;
    int _fd;
    const bool _sync_on_close = false;
    bool _pending_sync = false;
    bool _closed = false;
    uint64_t _filesize = 0;
    uint64_t _pre_allocated_size = 0;
};

class PosixRandomRWFile : public RandomRWFile {
public:
    PosixRandomRWFile(string fname, int fd, bool sync_on_close)
            : _filename(std::move(fname)), _fd(fd), _sync_on_close(sync_on_close), _closed(false) {}

    ~PosixRandomRWFile() { WARN_IF_ERROR(close(), "Failed to close " + _filename); }

    virtual Status read_at(uint64_t offset, const Slice& result) const override {
        return do_readv_at(_fd, _filename, offset, &result, 1);
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override {
        return do_readv_at(_fd, _filename, offset, res, res_cnt);
    }

    Status write_at(uint64_t offset, const Slice& data) override {
        return writev_at(offset, &data, 1);
    }

    Status writev_at(uint64_t offset, const Slice* data, size_t data_cnt) override {
        size_t bytes_written = 0;
        return do_writev_at(_fd, _filename, offset, data, data_cnt, &bytes_written);
    }

    Status flush(FlushMode mode, uint64_t offset, size_t length) override {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (mode == FLUSH_SYNC) {
            flags |= SYNC_FILE_RANGE_WAIT_AFTER;
        }
        if (sync_file_range(_fd, offset, length, flags) < 0) {
            return io_error(_filename, errno);
        }
#else
        if (mode == FLUSH_SYNC && fsync(_fd) < 0) {
            return io_error(_filename, errno);
        }
#endif
        return Status::OK();
    }

    Status sync() override { return do_sync(_fd, _filename); }

    Status close() override {
        if (_closed) {
            return Status::OK();
        }
        Status s;
        if (_sync_on_close) {
            s = sync();
            if (!s.ok()) {
                LOG(ERROR) << "Unable to Sync " << _filename << ": " << s.to_string();
            }
        }

        int ret;
        RETRY_ON_EINTR(ret, ::close(_fd));
        if (ret < 0) {
            if (s.ok()) {
                s = io_error(_filename, errno);
            }
        }

        _closed = true;
        return s;
    }

    Status size(uint64_t* size) const override {
        struct stat st;
        if (fstat(_fd, &st) == -1) {
            return io_error(_filename, errno);
        }
        *size = st.st_size;
        return Status::OK();
    }

    const string& filename() const override { return _filename; }

private:
    const std::string _filename;
    const int _fd;
    const bool _sync_on_close = false;
    bool _closed = false;
};

class PosixEnv : public Env {
public:
    ~PosixEnv() override {}

    Status new_sequential_file(const string& fname,
                               std::unique_ptr<SequentialFile>* result) override {
        FILE* f;
        POINTER_RETRY_ON_EINTR(f, fopen(fname.c_str(), "r"));
        if (f == nullptr) {
            return io_error(fname, errno);
        }
        result->reset(new PosixSequentialFile(fname, f));
        return Status::OK();
    }

    // get a RandomAccessFile pointer without file cache
    Status new_random_access_file(const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result) override {
        return new_random_access_file(RandomAccessFileOptions(), fname, result);
    }

    Status new_random_access_file(const RandomAccessFileOptions& opts, const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result) override {
        int fd;
        RETRY_ON_EINTR(fd, open(fname.c_str(), O_RDONLY));
        if (fd < 0) {
            return io_error(fname, errno);
        }
        result->reset(new PosixRandomAccessFile(fname, fd));
        return Status::OK();
    }

    Status new_writable_file(const string& fname, std::unique_ptr<WritableFile>* result) override {
        return new_writable_file(WritableFileOptions(), fname, result);
    }

    Status new_writable_file(const WritableFileOptions& opts, const string& fname,
                             std::unique_ptr<WritableFile>* result) override {
        int fd;
        RETURN_IF_ERROR(do_open(fname, opts.mode, &fd));

        uint64_t file_size = 0;
        if (opts.mode == MUST_EXIST) {
            RETURN_IF_ERROR(get_file_size(fname, &file_size));
        }
        result->reset(new PosixWritableFile(fname, fd, file_size, opts.sync_on_close));
        return Status::OK();
    }

    Status new_random_rw_file(const string& fname, std::unique_ptr<RandomRWFile>* result) override {
        return new_random_rw_file(RandomRWFileOptions(), fname, result);
    }

    Status new_random_rw_file(const RandomRWFileOptions& opts, const string& fname,
                              std::unique_ptr<RandomRWFile>* result) override {
        int fd;
        RETURN_IF_ERROR(do_open(fname, opts.mode, &fd));
        result->reset(new PosixRandomRWFile(fname, fd, opts.sync_on_close));
        return Status::OK();
    }

    Status path_exists(const std::string& fname) override {
        if (access(fname.c_str(), F_OK) != 0) {
            return io_error(fname, errno);
        }
        return Status::OK();
    }

    Status get_children(const std::string& dir, std::vector<std::string>* result) override {
        result->clear();
        DIR* d = opendir(dir.c_str());
        if (d == nullptr) {
            return io_error(dir, errno);
        }
        struct dirent* entry;
        while ((entry = readdir(d)) != nullptr) {
            result->push_back(entry->d_name);
        }
        closedir(d);
        return Status::OK();
    }

    Status iterate_dir(const std::string& dir,
                       const std::function<bool(const char*)>& cb) override {
        DIR* d = opendir(dir.c_str());
        if (d == nullptr) {
            return io_error(dir, errno);
        }
        struct dirent* entry;
        while ((entry = readdir(d)) != nullptr) {
            // callback returning false means to terminate iteration
            if (!cb(entry->d_name)) {
                break;
            }
        }
        closedir(d);
        return Status::OK();
    }

    Status delete_file(const std::string& fname) override {
        if (unlink(fname.c_str()) != 0) {
            return io_error(fname, errno);
        }
        return Status::OK();
    }

    Status create_dir(const std::string& name) override {
        if (mkdir(name.c_str(), 0755) != 0) {
            return io_error(name, errno);
        }
        return Status::OK();
    }

    Status create_dir_if_missing(const string& dirname, bool* created = nullptr) override {
        Status s = create_dir(dirname);
        if (created != nullptr) {
            *created = s.ok();
        }

        // Check that dirname is actually a directory.
        if (s.is_already_exist()) {
            bool is_dir = false;
            RETURN_IF_ERROR(is_directory(dirname, &is_dir));
            if (is_dir) {
                return Status::OK();
            } else {
                return s.clone_and_append("path already exists but not a dir");
            }
        }
        return s;
    }

    // Delete the specified directory.
    Status delete_dir(const std::string& dirname) override {
        if (rmdir(dirname.c_str()) != 0) {
            return io_error(dirname, errno);
        }
        return Status::OK();
    }

    Status sync_dir(const string& dirname) override {
        int dir_fd;
        RETRY_ON_EINTR(dir_fd, open(dirname.c_str(), O_DIRECTORY | O_RDONLY));
        if (dir_fd < 0) {
            return io_error(dirname, errno);
        }
        ScopedFdCloser fd_closer(dir_fd);
        if (fsync(dir_fd) != 0) {
            return io_error(dirname, errno);
        }
        return Status::OK();
    }

    Status is_directory(const std::string& path, bool* is_dir) override {
        struct stat path_stat;
        if (stat(path.c_str(), &path_stat) != 0) {
            return io_error(path, errno);
        } else {
            *is_dir = S_ISDIR(path_stat.st_mode);
        }

        return Status::OK();
    }

    Status canonicalize(const std::string& path, std::string* result) override {
        // NOTE: we must use free() to release the buffer retruned by realpath(),
        // because the buffer is allocated by malloc(), see `man 3 realpath`.
        std::unique_ptr<char[], FreeDeleter> r(realpath(path.c_str(), nullptr));
        if (r == nullptr) {
            return io_error(strings::Substitute("Unable to canonicalize $0", path), errno);
        }
        *result = std::string(r.get());
        return Status::OK();
    }

    Status get_file_size(const string& fname, uint64_t* size) override {
        struct stat sbuf;
        if (stat(fname.c_str(), &sbuf) != 0) {
            return io_error(fname, errno);
        } else {
            *size = sbuf.st_size;
        }
        return Status::OK();
    }

    Status get_file_modified_time(const std::string& fname, uint64_t* file_mtime) override {
        struct stat s;
        if (stat(fname.c_str(), &s) != 0) {
            return io_error(fname, errno);
        }
        *file_mtime = static_cast<uint64_t>(s.st_mtime);
        return Status::OK();
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        if (rename(src.c_str(), target.c_str()) != 0) {
            return io_error(src, errno);
        }
        return Status::OK();
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        if (link(old_path.c_str(), new_path.c_str()) != 0) {
            return io_error(old_path, errno);
        }
        return Status::OK();
    }
};

// Default Posix Env
Env* Env::Default() {
    static PosixEnv default_env;
    return &default_env;
}

} // end namespace doris
