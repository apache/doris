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
#include "io/hdfs_file_reader.h"

#include <sys/stat.h>
#include <unistd.h>

#include "service/backend_options.h"

namespace doris {

HdfsFileReader::HdfsFileReader(const THdfsParams& hdfs_params, const std::string& path,
                               int64_t start_offset)
        : _hdfs_params(hdfs_params),
          _path(path),
          _current_offset(start_offset),
          _file_size(-1),
          _hdfs_fs(nullptr),
          _hdfs_file(nullptr) {
    _namenode = _hdfs_params.fs_name;
}

HdfsFileReader::HdfsFileReader(const std::map<std::string, std::string>& properties,
                               const std::string& path, int64_t start_offset)
        : _path(path),
          _current_offset(start_offset),
          _file_size(-1),
          _hdfs_fs(nullptr),
          _hdfs_file(nullptr) {
    _parse_properties(properties);
}

HdfsFileReader::~HdfsFileReader() {
    close();
}

void HdfsFileReader::_parse_properties(const std::map<std::string, std::string>& prop) {
    _hdfs_params = parse_properties(prop);
    auto iter = prop.find(FS_KEY);
    if (iter != prop.end()) {
        _namenode = iter->second;
    }
}

Status HdfsFileReader::open() {
    if (_opened) {
        return Status::IOError("Can't reopen the same reader");
    }
    if (_namenode.empty()) {
        LOG(WARNING) << "hdfs properties is incorrect.";
        return Status::InternalError("hdfs properties is incorrect");
    }
    // if the format of _path is hdfs://ip:port/path, replace it to /path.
    // path like hdfs://ip:port/path can't be used by libhdfs3.
    if (_path.find(_namenode) != _path.npos) {
        _path = _path.substr(_namenode.size());
    }

    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(_hdfs_params, &_fs_handle));
    _hdfs_fs = _fs_handle->hdfs_fs;
    _hdfs_file = hdfsOpenFile(_hdfs_fs, _path.c_str(), O_RDONLY, 0, 0, 0);
    if (_hdfs_file == nullptr) {
        if (_fs_handle->from_cache) {
            // hdfsFS may be disconnected if not used for a long time
            _fs_handle->set_invalid();
            _fs_handle->dec_ref();
            // retry
            RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(_hdfs_params, &_fs_handle));
            _hdfs_fs = _fs_handle->hdfs_fs;
            _hdfs_file = hdfsOpenFile(_hdfs_fs, _path.c_str(), O_RDONLY, 0, 0, 0);
            if (_hdfs_fs == nullptr) {
                return Status::InternalError(
                        "open file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                        BackendOptions::get_localhost(), _namenode, _path, hdfsGetLastError());
            }
        } else {
            return Status::InternalError("open file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                                         BackendOptions::get_localhost(), _namenode, _path,
                                         hdfsGetLastError());
        }
    }
    VLOG_NOTICE << "open file, namenode:" << _namenode << ", path:" << _path;
    _opened = true;
    return seek(_current_offset);
}

void HdfsFileReader::close() {
    if (!_closed) {
        if (_hdfs_file != nullptr && _hdfs_fs != nullptr) {
            VLOG_NOTICE << "close hdfs file: " << _namenode << _path;
            //If the hdfs file was valid, the memory associated with it will
            // be freed at the end of this call, even if there was an I/O error
            hdfsCloseFile(_hdfs_fs, _hdfs_file);
        }

        if (_fs_handle != nullptr) {
            if (_fs_handle->from_cache) {
                _fs_handle->dec_ref();
            } else {
                delete _fs_handle;
            }
        }
    }
    _fs_handle = nullptr;
    _closed = true;
}

bool HdfsFileReader::closed() {
    return _closed;
}

// Read all bytes
Status HdfsFileReader::read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) {
    int64_t file_size = size() - _current_offset;
    if (file_size <= 0) {
        buf->reset();
        *length = 0;
        return Status::OK();
    }
    bool eof = false;
    buf->reset(new uint8_t[file_size]);
    read(buf->get(), file_size, length, &eof);
    return Status::OK();
}

Status HdfsFileReader::read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) {
    readat(_current_offset, buf_len, bytes_read, buf);
    if (*bytes_read == 0) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status HdfsFileReader::readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    if (position != _current_offset) {
        seek(position);
    }

    int64_t has_read = 0;
    char* cast_out = reinterpret_cast<char*>(out);
    while (has_read < nbytes) {
        int64_t loop_read = hdfsRead(_hdfs_fs, _hdfs_file, cast_out + has_read, nbytes - has_read);
        if (loop_read < 0) {
            return Status::InternalError(
                    "Read hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                    BackendOptions::get_localhost(), _namenode, _path, hdfsGetLastError());
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    *bytes_read = has_read;
    _current_offset += has_read; // save offset with file
    return Status::OK();
}

int64_t HdfsFileReader::size() {
    if (_file_size == -1) {
        if (_hdfs_fs != nullptr) {
            hdfsFileInfo* file_info = hdfsGetPathInfo(_hdfs_fs, _path.c_str());
            _file_size = file_info->mSize;
            hdfsFreeFileInfo(file_info, 1);
        }
    }
    return _file_size;
}

Status HdfsFileReader::seek(int64_t position) {
    int res = hdfsSeek(_hdfs_fs, _hdfs_file, position);
    if (res != 0) {
        return Status::InternalError("Seek to offset failed. (BE: {}) offset={}, err: {}",
                                     BackendOptions::get_localhost(), position, hdfsGetLastError());
    }
    _current_offset = position;
    return Status::OK();
}

Status HdfsFileReader::tell(int64_t* position) {
    *position = _current_offset;
    return Status::OK();
}

int HdfsFsCache::MAX_CACHE_HANDLE = 64;

Status HdfsFsCache::_create_fs(THdfsParams& hdfs_params, hdfsFS* fs) {
    HDFSCommonBuilder builder = createHDFSBuilder(hdfs_params);
    if (builder.is_need_kinit()) {
        RETURN_IF_ERROR(builder.run_kinit());
    }
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
        return Status::InternalError("connect to hdfs failed. error: {}", hdfsGetLastError());
    }
    *fs = hdfs_fs;
    return Status::OK();
}

void HdfsFsCache::_clean_invalid() {
    std::vector<uint64> removed_handle;
    for (auto& item : _cache) {
        if (item.second->invalid() && item.second->ref_cnt() == 0) {
            removed_handle.emplace_back(item.first);
        }
    }
    for (auto& handle : removed_handle) {
        _cache.erase(handle);
    }
}

void HdfsFsCache::_clean_oldest() {
    uint64_t oldest_time = ULONG_MAX;
    uint64 oldest = 0;
    for (auto& item : _cache) {
        if (item.second->ref_cnt() == 0 && item.second->last_access_time() < oldest_time) {
            oldest_time = item.second->last_access_time();
            oldest = item.first;
        }
    }
    _cache.erase(oldest);
}

Status HdfsFsCache::get_connection(THdfsParams& hdfs_params, HdfsFsHandle** fs_handle) {
    uint64 hash_code = _hdfs_hash_code(hdfs_params);
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _cache.find(hash_code);
        if (it != _cache.end()) {
            HdfsFsHandle* handle = it->second.get();
            if (handle->invalid()) {
                hdfsFS hdfs_fs = nullptr;
                RETURN_IF_ERROR(_create_fs(hdfs_params, &hdfs_fs));
                *fs_handle = new HdfsFsHandle(hdfs_fs, false);
            } else {
                handle->inc_ref();
                *fs_handle = handle;
            }
        } else {
            hdfsFS hdfs_fs = nullptr;
            RETURN_IF_ERROR(_create_fs(hdfs_params, &hdfs_fs));
            if (_cache.size() >= MAX_CACHE_HANDLE) {
                _clean_invalid();
                _clean_oldest();
            }
            if (_cache.size() < MAX_CACHE_HANDLE) {
                std::unique_ptr<HdfsFsHandle> handle =
                        std::make_unique<HdfsFsHandle>(hdfs_fs, true);
                handle->inc_ref();
                *fs_handle = handle.get();
                _cache[hash_code] = std::move(handle);
            } else {
                *fs_handle = new HdfsFsHandle(hdfs_fs, false);
            }
        }
    }
    return Status::OK();
}

uint64 HdfsFsCache::_hdfs_hash_code(THdfsParams& hdfs_params) {
    uint64 hash_code = 0;
    if (hdfs_params.__isset.fs_name) {
        hash_code += Fingerprint(hdfs_params.fs_name);
    }
    if (hdfs_params.__isset.user) {
        hash_code += Fingerprint(hdfs_params.user);
    }
    if (hdfs_params.__isset.hdfs_kerberos_principal) {
        hash_code += Fingerprint(hdfs_params.hdfs_kerberos_principal);
    }
    if (hdfs_params.__isset.hdfs_kerberos_keytab) {
        hash_code += Fingerprint(hdfs_params.hdfs_kerberos_keytab);
    }
    if (hdfs_params.__isset.hdfs_conf) {
        std::map<std::string, std::string> conf_map;
        for (auto& conf : hdfs_params.hdfs_conf) {
            conf_map[conf.key] = conf.value;
        }
        for (auto& conf : conf_map) {
            hash_code += Fingerprint(conf.first);
            hash_code += Fingerprint(conf.second);
        }
    }
    return hash_code;
}
} // namespace doris
