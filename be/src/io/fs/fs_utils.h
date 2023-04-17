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

#pragma once

#include "common/status.h"
#include "io/fs/file_system.h"

namespace doris {
namespace io {

struct FilePathDesc {
    FilePathDesc(const std::string& path) { filepath = path; }
    FilePathDesc() = default;
    TStorageMedium::type storage_medium = TStorageMedium::HDD;
    std::string filepath;
    std::string remote_path;
    std::string storage_name;
    io::FileSystem* file_system;

    std::string debug_string() const {
        std::stringstream ss;
        ss << "storage_medium: " << to_string(storage_medium) << ", local_path: " << filepath;
        if (!remote_path.empty()) {
            ss << ", storage_name: " << storage_name << ", remote_path: " << remote_path;
        }
        return ss.str();
    }
    // REMOTE_CACHE is the local cache path for remote path, if a data_dir is REMOTE_CACHE,
    // it means the tablet in it will be set as a remote path.
    static bool is_remote(TStorageMedium::type checked_storage_medium) {
        return checked_storage_medium == TStorageMedium::S3 ||
               checked_storage_medium == TStorageMedium::REMOTE_CACHE;
    }
    bool is_remote() const { return is_remote(storage_medium); }
};

class FilePathDescStream {
public:
    FilePathDescStream& operator<<(const FilePathDesc& val) {
        _filepath_stream << val.filepath;
        _storage_medium = val.storage_medium;
        _storage_name = val.storage_name;
        if (FilePathDesc::is_remote(_storage_medium)) {
            _remote_path_stream << val.remote_path;
        }
        return *this;
    }
    FilePathDescStream& operator<<(const std::string& val) {
        _filepath_stream << val;
        if (FilePathDesc::is_remote(_storage_medium)) {
            _remote_path_stream << val;
        }
        return *this;
    }
    FilePathDescStream& operator<<(uint64_t val) {
        _filepath_stream << val;
        if (FilePathDesc::is_remote(_storage_medium)) {
            _remote_path_stream << val;
        }
        return *this;
    }
    FilePathDescStream& operator<<(int64_t val) {
        _filepath_stream << val;
        if (FilePathDesc::is_remote(_storage_medium)) {
            _remote_path_stream << val;
        }
        return *this;
    }
    FilePathDescStream& operator<<(uint32_t val) {
        _filepath_stream << val;
        if (FilePathDesc::is_remote(_storage_medium)) {
            _remote_path_stream << val;
        }
        return *this;
    }
    FilePathDescStream& operator<<(int32_t val) {
        _filepath_stream << val;
        if (FilePathDesc::is_remote(_storage_medium)) {
            _remote_path_stream << val;
        }
        return *this;
    }
    FilePathDesc path_desc() {
        FilePathDesc path_desc(_filepath_stream.str());
        path_desc.storage_medium = _storage_medium;
        if (FilePathDesc::is_remote(_storage_medium)) {
            path_desc.remote_path = _remote_path_stream.str();
        }
        path_desc.storage_name = _storage_name;
        return path_desc;
    }

private:
    TStorageMedium::type _storage_medium = TStorageMedium::HDD;
    std::stringstream _filepath_stream;
    std::stringstream _remote_path_stream;
    std::string _storage_name;
};

// read all data from file to string
Status read_file_to_string(FileSystemSPtr fs, const Path& file, std::string* content);

} // namespace io
} // namespace doris
