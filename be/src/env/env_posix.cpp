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

#include "env/env_posix.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>

#include "env/env.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "gutil/port.h"
#include "gutil/strings/substitute.h"
#include "util/errno.h"
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
        return Status::NotFound("{} (error {}) {}", context, err_number,
                                errno_to_string(err_number));
    case EEXIST:
        return Status::AlreadyExist("{} (error {}) {}", context, err_number,
                                    errno_to_string(err_number));
    case EOPNOTSUPP:
    case EXDEV: // No cross FS links allowed
        return Status::NotSupported("{} (error {}) {}", context, err_number,
                                    errno_to_string(err_number));
    case EIO:
        LOG(ERROR) << "I/O error, context=" << context;
    }
    return Status::IOError("{} (error {}) {}", context, err_number, errno_to_string(err_number));
}

static Status do_sync(int fd, const string& filename) {
#ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) < 0) {
        return io_error(filename, errno);
    }
#else
    if (fdatasync(fd) < 0) {
        return io_error(filename, errno);
    }
#endif
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
        return Status::NotSupported("Unknown create mode {}", mode);
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
            return Status::EndOfFile("EOF trying to read {} bytes at offset {}", bytes_req, offset);
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

    Status read_at(uint64_t offset, const Slice* result) const override {
        return readv_at(offset, result, 1);
    }

    Status readv_at(uint64_t offset, const Slice* result, size_t res_cnt) const override {
        return do_readv_at(_fd, _filename, offset, result, res_cnt);
    }

    Status read_all(std::string* content) const override {
        std::fstream fs(_filename.c_str(), std::fstream::in);
        if (!fs.is_open()) {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    Status::IOError("failed to open cluster id file {}", _filename),
                    "open file failed");
        }
        std::string data;
        fs >> data;
        fs.close();
        if ((fs.rdstate() & std::fstream::eofbit) != 0) {
            *content = data;
        } else {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    Status::Corruption(
                            "read_all from file {} is corrupt. [eofbit={} failbit={} badbit={}]",
                            _filename, fs.rdstate() & std::fstream::eofbit,
                            fs.rdstate() & std::fstream::failbit,
                            fs.rdstate() & std::fstream::badbit),
                    "read_all is error");
        }
        return Status::OK();
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
#ifdef __APPLE__
        return io_error(_filename, ENOSYS);
#else
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
#endif
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

    Status read_at(uint64_t offset, const Slice& result) const override {
        return readv_at(offset, &result, 1);
    }

    Status readv_at(uint64_t offset, const Slice* result, size_t res_cnt) const override {
        return do_readv_at(_fd, _filename, offset, result, res_cnt);
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

// get a RandomAccessFile pointer without file cache
Status PosixEnv::new_random_access_file(const std::string& fname,
                                        std::unique_ptr<RandomAccessFile>* result) {
    return new_random_access_file(RandomAccessFileOptions(), fname, result);
}

Status PosixEnv::new_random_access_file(const RandomAccessFileOptions& opts,
                                        const std::string& fname,
                                        std::unique_ptr<RandomAccessFile>* result) {
    int fd;
    RETRY_ON_EINTR(fd, open(fname.c_str(), O_RDONLY));
    if (fd < 0) {
        return io_error(fname, errno);
    }
    result->reset(new PosixRandomAccessFile(fname, fd));
    return Status::OK();
}

Status PosixEnv::new_writable_file(const string& fname, std::unique_ptr<WritableFile>* result) {
    return new_writable_file(WritableFileOptions(), fname, result);
}

Status PosixEnv::new_writable_file(const WritableFileOptions& opts, const string& fname,
                                   std::unique_ptr<WritableFile>* result) {
    int fd;
    RETURN_IF_ERROR(do_open(fname, opts.mode, &fd));

    uint64_t file_size = 0;
    if (opts.mode == MUST_EXIST) {
        RETURN_IF_ERROR(get_file_size(fname, &file_size));
    }
    result->reset(new PosixWritableFile(fname, fd, file_size, opts.sync_on_close));
    return Status::OK();
}

Status PosixEnv::new_random_rw_file(const string& fname, std::unique_ptr<RandomRWFile>* result) {
    return new_random_rw_file(RandomRWFileOptions(), fname, result);
}

Status PosixEnv::new_random_rw_file(const RandomRWFileOptions& opts, const string& fname,
                                    std::unique_ptr<RandomRWFile>* result) {
    int fd;
    RETURN_IF_ERROR(do_open(fname, opts.mode, &fd));
    result->reset(new PosixRandomRWFile(fname, fd, opts.sync_on_close));
    return Status::OK();
}

Status PosixEnv::path_exists(const std::string& fname, bool is_dir) {
    if (access(fname.c_str(), F_OK) != 0) {
        return io_error(fname, errno);
    }
    return Status::OK();
}

Status PosixEnv::get_children(const std::string& dir, std::vector<std::string>* result) {
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

Status PosixEnv::iterate_dir(const std::string& dir, const std::function<bool(const char*)>& cb) {
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

Status PosixEnv::delete_file(const std::string& fname) {
    if (unlink(fname.c_str()) != 0) {
        return io_error(fname, errno);
    }
    return Status::OK();
}

Status PosixEnv::create_dir(const std::string& name) {
    if (mkdir(name.c_str(), 0755) != 0) {
        return io_error(name, errno);
    }
    return Status::OK();
}

Status PosixEnv::create_dir_if_missing(const string& dirname, bool* created) {
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
            return std::move(s.append("path already exists but not a dir"));
        }
    }
    return s;
}

Status PosixEnv::create_dirs(const string& dirname) {
    if (dirname.empty()) {
        return Status::InvalidArgument("Unknown primitive type({})", dirname);
    }

    std::filesystem::path p(dirname);

    std::string partial_path;
    for (std::filesystem::path::iterator it = p.begin(); it != p.end(); ++it) {
        partial_path = partial_path + it->string() + "/";
        bool is_dir = false;

        Status s = is_directory(partial_path, &is_dir);

        if (s.ok()) {
            if (is_dir) {
                // It's a normal directory.
                continue;
            }

            // Maybe a file or a symlink. Let's try to follow the symlink.
            std::string real_partial_path;
            RETURN_IF_ERROR(canonicalize(partial_path, &real_partial_path));

            RETURN_IF_ERROR(is_directory(real_partial_path, &is_dir));
            if (is_dir) {
                // It's a symlink to a directory.
                continue;
            } else {
                return Status::IOError("{} exists but is not a directory", partial_path);
            }
        }

        RETURN_IF_ERROR(create_dir_if_missing(partial_path));
    }

    return Status::OK();
}

// Delete the specified directory.
Status PosixEnv::delete_dir(const std::string& dirname) {
    std::filesystem::path boost_path(dirname);
    std::error_code ec;
    std::filesystem::remove_all(boost_path, ec);
    if (ec) {
        std::stringstream ss;
        ss << "remove all(" << dirname << ") failed, because: " << ec;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status PosixEnv::sync_dir(const string& dirname) {
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

Status PosixEnv::is_directory(const std::string& path, bool* is_dir) {
    struct stat path_stat;
    if (stat(path.c_str(), &path_stat) != 0) {
        return io_error(path, errno);
    } else {
        *is_dir = S_ISDIR(path_stat.st_mode);
    }

    return Status::OK();
}

Status PosixEnv::canonicalize(const std::string& path, std::string* result) {
    // NOTE: we must use free() to release the buffer retruned by realpath(),
    // because the buffer is allocated by malloc(), see `man 3 realpath`.
    std::unique_ptr<char[], FreeDeleter> r(realpath(path.c_str(), nullptr));
    if (r == nullptr) {
        return io_error(strings::Substitute("Unable to canonicalize $0", path), errno);
    }
    *result = std::string(r.get());
    return Status::OK();
}

Status PosixEnv::get_file_size(const string& fname, uint64_t* size) {
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
        return io_error(fname, errno);
    } else {
        *size = sbuf.st_size;
    }
    return Status::OK();
}

Status PosixEnv::get_file_modified_time(const std::string& fname, uint64_t* file_mtime) {
    struct stat s;
    if (stat(fname.c_str(), &s) != 0) {
        return io_error(fname, errno);
    }
    *file_mtime = static_cast<uint64_t>(s.st_mtime);
    return Status::OK();
}

Status PosixEnv::copy_path(const std::string& src, const std::string& target) {
    try {
        std::filesystem::copy(src, target, std::filesystem::copy_options::recursive);
    } catch (const std::filesystem::filesystem_error& e) {
        std::stringstream ss;
        ss << "failed to copy_path: from " << src << " to " << target << ". err: " << e.what();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status PosixEnv::rename_file(const std::string& src, const std::string& target) {
    if (rename(src.c_str(), target.c_str()) != 0) {
        return io_error(src, errno);
    }
    return Status::OK();
}

Status PosixEnv::rename_dir(const std::string& src, const std::string& target) {
    return rename_file(src, target);
}

Status PosixEnv::link_file(const std::string& old_path, const std::string& new_path) {
    if (link(old_path.c_str(), new_path.c_str()) != 0) {
        return io_error(old_path, errno);
    }
    return Status::OK();
}

Status PosixEnv::get_space_info(const std::string& path, int64_t* capacity, int64_t* available) {
    try {
        std::filesystem::path path_name(path);
        std::filesystem::space_info path_info = std::filesystem::space(path_name);
        *capacity = path_info.capacity;
        *available = path_info.available;
    } catch (std::filesystem::filesystem_error& e) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError("get path {} available capacity failed, error={}", path, e.what()),
                "std::filesystem::space failed");
    }
    return Status::OK();
}

} // end namespace doris
