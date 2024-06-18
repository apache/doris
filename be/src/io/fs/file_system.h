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

#include <butil/macros.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"

namespace doris::io {

#ifndef FILESYSTEM_M
#define FILESYSTEM_M(stmt)                                  \
    do {                                                    \
        Status _s;                                          \
        if (bthread_self() == 0) {                          \
            _s = (stmt);                                    \
        } else {                                            \
            auto task = [&] { _s = (stmt); };               \
            AsyncIO::run_task(task, _type);                 \
        }                                                   \
        if (!_s) {                                          \
            _s = Status::Error<false>(_s.code(), _s.msg()); \
        }                                                   \
        return _s;                                          \
    } while (0);
#endif

enum class FileSystemType : uint8_t {
    LOCAL,
    S3,
    HDFS,
    BROKER,
};

inline std::ostream& operator<<(std::ostream& ostr, FileSystemType type) {
    switch (type) {
    case FileSystemType::LOCAL:
        ostr << "LOCAL";
        return ostr;
    case FileSystemType::S3:
        ostr << "S3";
        return ostr;
    case FileSystemType::HDFS:
        ostr << "HDFS";
        return ostr;
    case FileSystemType::BROKER:
        ostr << "BROKER";
        return ostr;
    default:
        ostr << "UNKNOWN";
        return ostr;
    }
}

struct FileInfo {
    // only file name, no path
    std::string file_name;
    int64_t file_size;
    bool is_file;
};

// `FileSystem` providing an interface to access FS metadata. Each `RemoteFileSystem` instance
// (except temporary Remote FS) corresponds to a `StorageResource` configured in FE.
class FileSystem {
public:
    // Sometimes `FileSystem` instance maybe a temporary instance which has no corresponding
    // Resource, then use `TMP_FS_ID` as FS id.
    inline static const std::string TMP_FS_ID;

    // The following are public interface.
    // And derived classes should implement all xxx_impl methods.
    Status create_file(const Path& file, FileWriterPtr* writer,
                       const FileWriterOptions* opts = nullptr);
    Status open_file(const Path& file, FileReaderSPtr* reader,
                     const FileReaderOptions* opts = nullptr);
    Status create_directory(const Path& dir, bool failed_if_exists = false);
    Status delete_file(const Path& file);
    Status delete_directory(const Path& dir);
    Status batch_delete(const std::vector<Path>& files);
    Status exists(const Path& path, bool* res) const;
    Status file_size(const Path& file, int64_t* file_size) const;
    Status list(const Path& dir, bool only_file, std::vector<FileInfo>* files, bool* exists);
    Status rename(const Path& orig_name, const Path& new_name);

    const std::string& id() const { return _id; }
    // file system type
    FileSystemType type() const { return _type; }

    virtual ~FileSystem() = default;

    // Each derived class should implement create method to create fs.
    FileSystem(const FileSystem&) = delete;
    const FileSystem& operator=(const FileSystem&) = delete;

protected:
    /// create file and return a FileWriter
    virtual Status create_file_impl(const Path& file, FileWriterPtr* writer,
                                    const FileWriterOptions* opts) = 0;

    /// open file and return a FileReader
    virtual Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                                  const FileReaderOptions* opts) = 0;

    /// create directory recursively
    virtual Status create_directory_impl(const Path& dir, bool failed_if_exists = false) = 0;

    /// delete file.
    /// return OK if file does not exist
    /// return ERR if not a regular file
    virtual Status delete_file_impl(const Path& file) = 0;

    /// delete all files in "files"
    virtual Status batch_delete_impl(const std::vector<Path>& files) = 0;

    /// remove all under directory recursively
    /// return OK if dir does not exist
    /// return ERR if not a dir
    virtual Status delete_directory_impl(const Path& dir) = 0;

    /// check if path exist
    /// return OK and res = 1 means exist, res = 0 means does not exist
    /// return ERR otherwise
    virtual Status exists_impl(const Path& path, bool* res) const = 0;

    /// return OK and get size of given file, save in "file_size".
    /// return ERR otherwise
    virtual Status file_size_impl(const Path& file, int64_t* file_size) const = 0;

    /// return OK and list all objects in "dir", save in "files"
    /// return ERR otherwise
    /// will not traverse dir recursively.
    /// if "only_file" is true, will only return regular files, otherwise, return files and subdirs.
    /// the existence of dir will be saved in "exists"
    /// if "dir" does not exist, it will return Status::OK, but "exists" will to false
    virtual Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                             bool* exists) = 0;

    /// rename file from orig_name to new_name
    virtual Status rename_impl(const Path& orig_name, const Path& new_name) = 0;

    // FIMXE(plat1ko): The implementation and semantics of this function are not completely
    // consistent, which is confused.
    virtual Path absolute_path(const Path& path) const = 0;

    FileSystem(std::string id, FileSystemType type) : _id(std::move(id)), _type(type) {}

    // FS uid, corresponds to the `StorageResource` id generated by FE or MetaService.
    // Although resource id in FE is int64 now, we use string type for flexible extensibility.
    std::string _id;
    FileSystemType _type;
};

using FileSystemSPtr = std::shared_ptr<FileSystem>;

} // namespace doris::io
