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

#include "olap/file_helper.h"

#include <stdio.h>
#include <sys/stat.h>

#include <string>
#include <vector>

#include <errno.h>

#include "common/config.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/utils.h"
#include "olap/storage_engine.h"
#include "util/debug_util.h"

using std::string;

namespace doris {

Cache* FileHandler::_s_fd_cache = nullptr;

FileHandler::FileHandler() :
        _fd(-1),
        _wr_length(0),
        _file_name(""),
        _is_using_cache(false),
        _cache_handle(NULL) {
    static std::once_flag once_flag;
    #ifdef BE_TEST
        std::call_once(once_flag, [] {
            _s_fd_cache = new_lru_cache(config::file_descriptor_cache_capacity);
        });
    #else
        // storage engine may not be opened when doris try to read and write 
        // temp file under the storage root path. So we need to check it.
        if (StorageEngine::instance() != nullptr && 
                    StorageEngine::instance()->file_cache() != nullptr) {
            std::call_once(once_flag, [] {
                _s_fd_cache = StorageEngine::instance()->file_cache().get();
            });
        }
    #endif
}

FileHandler::~FileHandler() {
    this->close();
}

OLAPStatus FileHandler::open(const string& file_name, int flag) {
    if (_fd != -1 && _file_name == file_name) {
        return OLAP_SUCCESS;
    }

    if (OLAP_SUCCESS != this->close()) {
        return OLAP_ERR_IO_ERROR;
    }

    _fd = ::open(file_name.c_str(), flag);

    if (_fd < 0) {
        char errmsg[64];
        LOG(WARNING) << "failed to open file. [err=" << strerror_r(errno, errmsg, 64)
                     << ", file_name='" << file_name << "' flag=" << flag << "]";
        if (errno == EEXIST) {
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        return OLAP_ERR_IO_ERROR;
    }

    VLOG(3) << "success to open file. file_name=" << file_name
            << ", mode=" << flag << " fd=" << _fd;
    _is_using_cache = false;
    _file_name = file_name;
    return OLAP_SUCCESS;
}

OLAPStatus FileHandler::open_with_cache(const string& file_name, int flag) {
    if (_s_fd_cache == nullptr) {
        return open(file_name, flag);
    }

    if (_fd != -1 && _file_name == file_name) {
        return OLAP_SUCCESS;
    }

    if (OLAP_SUCCESS != this->close()) {
        return OLAP_ERR_IO_ERROR;
    }

    CacheKey key(file_name.c_str(), file_name.size());
    _cache_handle = _s_fd_cache->lookup(key);
    if (NULL != _cache_handle) {
        FileDescriptor* file_desc =
            reinterpret_cast<FileDescriptor*>(_s_fd_cache->value(_cache_handle));
        _fd = file_desc->fd;
        VLOG(3) << "success to open file with cache. file_name=" << file_name
                << ", mode=" << flag << " fd=" << _fd;
    } else {
        _fd = ::open(file_name.c_str(), flag);
        if (_fd < 0) {
            char errmsg[64];
            LOG(WARNING) << "failed to open file. [err=" << strerror_r(errno, errmsg, 64)
                         << " file_name='" << file_name << "' flag=" << flag << "]";
            if (errno == EEXIST) {
                return OLAP_ERR_FILE_ALREADY_EXIST;
            }
            return OLAP_ERR_IO_ERROR;
        }
        FileDescriptor* file_desc = new FileDescriptor(_fd);
        _cache_handle = _s_fd_cache->insert(
                            key, file_desc, 1,
                            &_delete_cache_file_descriptor);
        VLOG(3) << "success to open file with cache. "
                << "file_name=" << file_name 
                << ", mode=" << flag << ", fd=" << _fd;
    }
    _is_using_cache = true;
    _file_name = file_name;
    return OLAP_SUCCESS;
}

OLAPStatus FileHandler::open_with_mode(const string& file_name, int flag, int mode) {
    if (_fd != -1 && _file_name == file_name) {
        return OLAP_SUCCESS;
    }

    if (OLAP_SUCCESS != this->close()) {
        return OLAP_ERR_IO_ERROR;
    }

    _fd = ::open(file_name.c_str(), flag, mode);

    if (_fd < 0) {
        char err_buf[64];
        LOG(WARNING) << "failed to open file. [err=" << strerror_r(errno, err_buf, 64)
                     << " file_name='" << file_name
                     << "' flag=" << flag
                     << " mode=" << mode << "]";
        if (errno == EEXIST) {
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        return OLAP_ERR_IO_ERROR;
    }

    VLOG(3) << "success to open file. file_name=" << file_name
            << ", mode=" << mode << ", fd=" << _fd;
    _file_name = file_name;
    return OLAP_SUCCESS;
}

OLAPStatus FileHandler::_release() {
    _s_fd_cache->release(_cache_handle);
    _cache_handle = NULL;
    _is_using_cache = false;
    return OLAP_SUCCESS;
}

OLAPStatus FileHandler::close() {
    if (_fd < 0) {
        return OLAP_SUCCESS;
    }

    if (_is_using_cache && _s_fd_cache != nullptr) {
        _release();
    } else {
        // try to sync page cache if have written some bytes
        if (_wr_length > 0) {
            posix_fadvise(_fd, 0, 0, POSIX_FADV_DONTNEED);
            // Clean dirty pages and wait for io queue empty and return
            sync_file_range(_fd, 0, 0, SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
            _wr_length = 0;
        }

        // 在一些极端情况下(fd可用,但fsync失败)可能造成句柄泄漏
        if (::close(_fd) < 0) {
            char errmsg[64];
            LOG(WARNING) << "failed to close file. [err= " << strerror_r(errno, errmsg, 64)
                         << " file_name='" << _file_name << "' fd=" << _fd << "]";
            return OLAP_ERR_IO_ERROR;
        }
    }

    VLOG(3) << "finished to close file. "
            << "file_name=" << _file_name << ", fd=" << _fd;
    _fd = -1;
    _file_name = "";
    _wr_length = 0;
    return OLAP_SUCCESS;
}

OLAPStatus FileHandler::pread(void* buf, size_t size, size_t offset) {
    char* ptr = reinterpret_cast<char*>(buf);

    while (size > 0) {
        ssize_t rd_size = ::pread(_fd, ptr, size, offset);

        if (rd_size < 0) {
            char errmsg[64];
            LOG(WARNING) << "failed to pread from file. [err= " << strerror_r(errno, errmsg, 64)
                         << " file_name='" << _file_name << "' fd=" << _fd << " size=" << size
                         << " offset=" << offset << "]";
            return OLAP_ERR_IO_ERROR;
        } else if (0 == rd_size) {
            char errmsg[64];
            LOG(WARNING) << "read unenough from file. [err= " << strerror_r(errno, errmsg, 64)
                         << " file_name='" << _file_name << "' fd=" << _fd << " size=" << size
                         << " offset=" << offset << "]";
            return OLAP_ERR_READ_UNENOUGH;
        }

        size -= rd_size;
        offset += rd_size;
        ptr += rd_size;
    }

    return OLAP_SUCCESS;
}

OLAPStatus FileHandler::write(const void* buf, size_t buf_size) {

    size_t org_buf_size = buf_size;
    const char* ptr = reinterpret_cast<const char*>(buf);
    while (buf_size > 0) {
        ssize_t wr_size = ::write(_fd, ptr, buf_size);

        if (wr_size < 0) {
            char errmsg[64];
            LOG(WARNING) << "failed to write to file. [err= " << strerror_r(errno, errmsg, 64)
                         << " file_name='" << _file_name << "' fd=" << _fd
                         << " size=" << buf_size << "]";
            return OLAP_ERR_IO_ERROR;
        }  else if (0 == wr_size) {
            char errmsg[64];
            LOG(WARNING) << "write unenough to file. [err=" << strerror_r(errno, errmsg, 64) 
                         << " file_name='" << _file_name << "' fd=" << _fd
                         << " size=" << buf_size << "]";
            return OLAP_ERR_IO_ERROR;
        }

        buf_size -= wr_size;
        ptr += wr_size;
    }
    _wr_length += org_buf_size;
    // try to sync page cache if cache size is bigger than threshold
    if (_wr_length >= _cache_threshold) {
        posix_fadvise(_fd, 0, 0, POSIX_FADV_DONTNEED);
        // Clean dirty pages and wait for io queue empty and return
        sync_file_range(_fd, 0, 0, SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
        _wr_length = 0;
    }
    return OLAP_SUCCESS;
}

OLAPStatus FileHandler::pwrite(const void* buf, size_t buf_size, size_t offset) {
    const char* ptr = reinterpret_cast<const char*>(buf);

    size_t org_buf_size = buf_size;
    while (buf_size > 0) {
        ssize_t wr_size = ::pwrite(_fd, ptr, buf_size, offset);

        if (wr_size < 0) {
            char errmsg[64];
            LOG(WARNING) << "failed to pwrite to file. [err= " << strerror_r(errno, errmsg, 64)
                         << " file_name='" << _file_name << "' fd=" << _fd << " size=" << buf_size
                         << " offset=" << offset << "]";
            return OLAP_ERR_IO_ERROR;
        } else if (0 == wr_size) {
            char errmsg[64];
            LOG(WARNING) << "pwrite unenough to file. [err= " << strerror_r(errno, errmsg, 64)
                         << " file_name='" << _file_name << "' fd=" << _fd << " size=" << buf_size
                         << " offset=" << offset << "]";
            return OLAP_ERR_IO_ERROR;
        }

        buf_size -= wr_size;
        ptr += wr_size;
        offset += wr_size;
    }
    _wr_length += org_buf_size;

    return OLAP_SUCCESS;
}

off_t FileHandler::length() const {
    struct stat stat_data;

    if (fstat(_fd, &stat_data) < 0) {
        OLAP_LOG_WARNING("fstat error. [fd=%d]", _fd);
        return -1;
    }

    return stat_data.st_size;
}

FileHandlerWithBuf::FileHandlerWithBuf() :
    _fp(NULL),
    _file_name("") {
}

FileHandlerWithBuf::~FileHandlerWithBuf() {
    this->close();
}

OLAPStatus FileHandlerWithBuf::open(const string& file_name, const char* mode) {
    if (_fp != NULL && _file_name == file_name) {
        return OLAP_SUCCESS;
    }

    if (OLAP_SUCCESS != this->close()) {
        return OLAP_ERR_IO_ERROR;
    }

    _fp = ::fopen(file_name.c_str(), mode);

    if (NULL == _fp) {
        char errmsg[64];
        LOG(WARNING) << "failed to open file. [err= " << strerror_r(errno, errmsg, 64)
                     << " file_name='" << file_name << "' flag='" << mode << "']";
        if (errno == EEXIST) {
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        return OLAP_ERR_IO_ERROR;
    }

    VLOG(3) << "success to open file. "
            << "file_name=" << file_name << ", mode=" << mode;
    _file_name = file_name;
    return OLAP_SUCCESS;
}

OLAPStatus FileHandlerWithBuf::open_with_mode(const string& file_name, const char* mode) {
    return this->open(file_name, mode);
}

OLAPStatus FileHandlerWithBuf::close() {
    if (NULL == _fp) {
        return OLAP_SUCCESS;
    }

    // 在一些极端情况下(fd可用,但fsync失败)可能造成句柄泄漏
    if (::fclose(_fp) != 0) {
        char errmsg[64];
        LOG(WARNING) << "failed to close file. [err= " << strerror_r(errno, errmsg, 64)
                     << " file_name='" << _file_name << "']";
        return OLAP_ERR_IO_ERROR;
    }

    _fp = NULL;
    _file_name = "";
    return OLAP_SUCCESS;
}

OLAPStatus FileHandlerWithBuf::read(void* buf, size_t size) {
    if (OLAP_UNLIKELY(NULL == _fp)) {
        OLAP_LOG_WARNING("Fail to write, fp is NULL!");
        return OLAP_ERR_NOT_INITED;
    }

    size_t rd_size = ::fread(buf, 1, size, _fp);

    if (rd_size == size) {
        return OLAP_SUCCESS;
    } else if (::feof(_fp)) {
        char errmsg[64];
        LOG(WARNING) << "read unenough from file. [err=" << strerror_r(errno, errmsg, 64)
                     << " file_name='" << _file_name << "' size=" << size
                     << " rd_size=" << rd_size << "]";
        return OLAP_ERR_READ_UNENOUGH;
    } else {
        char errmsg[64];
        LOG(WARNING) << "failed to read from file. [err=" << strerror_r(errno, errmsg, 64)
                     << " file_name='" << _file_name << "' size=" << size << "]";
        return OLAP_ERR_IO_ERROR;
    }
}

OLAPStatus FileHandlerWithBuf::pread(void* buf, size_t size, size_t offset) {
    if (OLAP_UNLIKELY(NULL == _fp)) {
        OLAP_LOG_WARNING("Fail to write, fp is NULL!");
        return OLAP_ERR_NOT_INITED;
    }

    if (0 != ::fseek(_fp, offset, SEEK_SET)) {
        char errmsg[64];
        LOG(WARNING) << "failed to seek file. [err= " << strerror_r(errno, errmsg, 64)
                     << " file_name='" << _file_name << "' size=" << size
                     << " offset=" << offset << "]";
        return OLAP_ERR_IO_ERROR;
    }

    return this->read(buf, size);
}

OLAPStatus FileHandlerWithBuf::write(const void* buf, size_t buf_size) {
    if (OLAP_UNLIKELY(NULL == _fp)) {
        OLAP_LOG_WARNING("Fail to write, fp is NULL!");
        return OLAP_ERR_NOT_INITED;
    }

    size_t wr_size = ::fwrite(buf, 1, buf_size, _fp);

    if (wr_size != buf_size) {
        char errmsg[64];
        LOG(WARNING) << "failed to write to file. [err= " << strerror_r(errno, errmsg, 64)
                     << " file_name='" << _file_name << "' size=" << buf_size << "]";
        return OLAP_ERR_IO_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus FileHandlerWithBuf::pwrite(const void* buf, size_t buf_size, size_t offset) {
    if (OLAP_UNLIKELY(NULL == _fp)) {
        OLAP_LOG_WARNING("Fail to write, fp is NULL!");
        return OLAP_ERR_NOT_INITED;
    }

    if (0 != ::fseek(_fp, offset, SEEK_SET)) {
        char errmsg[64];
        LOG(WARNING) << "failed to seek file. [err= " << strerror_r(errno, errmsg, 64)
                     << " file_name='" << _file_name << "' size=" << buf_size
                     << " offset=" << offset << "]";
        return OLAP_ERR_IO_ERROR;
    }

    return this->write(buf, buf_size);
}

off_t FileHandlerWithBuf::length() const {
    struct stat stat_data;

    if (stat(_file_name.c_str(), &stat_data) < 0) {
        LOG(WARNING) << "fstat error. [file_name='" << _file_name << "']";
        return -1;
    }

    return stat_data.st_size;
}

}  // namespace doris
