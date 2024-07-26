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

#include "io/fs/hdfs_file_system.h"

#include <errno.h>
#include <fcntl.h>
#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "gutil/integral_types.h"
#include "io/fs/err_utils.h"
#include "io/fs/hdfs_file_reader.h"
#include "io/fs/hdfs_file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/hdfs_builder.h"
#include "io/hdfs_util.h"
#include "util/obj_lru_cache.h"
#include "util/slice.h"

namespace doris::io {

#ifndef CHECK_HDFS_HANDLE
#define CHECK_HDFS_HANDLE(handle)                         \
    if (!handle) {                                        \
        return Status::IOError("init Hdfs handle error"); \
    }
#endif

Result<std::shared_ptr<HdfsFileSystem>> HdfsFileSystem::create(
        const std::map<std::string, std::string>& properties, std::string fs_name, std::string id,
        RuntimeProfile* profile, std::string root_path) {
    return HdfsFileSystem::create(parse_properties(properties), std::move(fs_name), std::move(id),
                                  profile, std::move(root_path));
}

Result<std::shared_ptr<HdfsFileSystem>> HdfsFileSystem::create(const THdfsParams& hdfs_params,
                                                               std::string fs_name, std::string id,
                                                               RuntimeProfile* profile,
                                                               std::string root_path) {
#ifdef USE_HADOOP_HDFS
    if (!config::enable_java_support) {
        return ResultError(Status::InternalError(
                "hdfs file system is not enabled, you can change be config enable_java_support to "
                "true."));
    }
#endif
    std::shared_ptr<HdfsFileSystem> fs(new HdfsFileSystem(
            hdfs_params, std::move(fs_name), std::move(id), profile, std::move(root_path)));
    RETURN_IF_ERROR_RESULT(fs->init());
    return fs;
}

HdfsFileSystem::HdfsFileSystem(const THdfsParams& hdfs_params, std::string fs_name, std::string id,
                               RuntimeProfile* profile, std::string root_path)
        : RemoteFileSystem(std::move(root_path), std::move(id), FileSystemType::HDFS),
          _hdfs_params(hdfs_params),
          _fs_name(std::move(fs_name)),
          _profile(profile) {
    if (_fs_name.empty()) {
        _fs_name = hdfs_params.fs_name;
    }
}

HdfsFileSystem::~HdfsFileSystem() = default;

Status HdfsFileSystem::init() {
    RETURN_IF_ERROR(
            HdfsHandlerCache::instance()->get_connection(_hdfs_params, _fs_name, &_fs_handle));
    if (!_fs_handle) {
        return Status::InternalError("failed to init Hdfs handle with, please check hdfs params.");
    }
    return Status::OK();
}

Status HdfsFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                        const FileWriterOptions* opts) {
    auto res = io::HdfsFileWriter::create(file, _fs_handle, _fs_name, opts);
    if (res.has_value()) {
        *writer = std::move(res).value();
        return Status::OK();
    } else {
        return std::move(res).error();
    }
}

Status HdfsFileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                          const FileReaderOptions& opts) {
    CHECK_HDFS_HANDLE(_fs_handle);
    *reader =
            DORIS_TRY(HdfsFileReader::create(file, _fs_handle->hdfs_fs, _fs_name, opts, _profile));
    return Status::OK();
}

Status HdfsFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(dir, _fs_name);
    int res = hdfsCreateDirectory(_fs_handle->hdfs_fs, real_path.string().c_str());
    if (res == -1) {
        return Status::IOError("failed to create directory {}: {}", dir.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::delete_file_impl(const Path& file) {
    return delete_internal(file, 0);
}

Status HdfsFileSystem::delete_directory_impl(const Path& dir) {
    return delete_internal(dir, 1);
}

Status HdfsFileSystem::batch_delete_impl(const std::vector<Path>& files) {
    for (auto& file : files) {
        RETURN_IF_ERROR(delete_file_impl(file));
    }
    return Status::OK();
}

Status HdfsFileSystem::delete_internal(const Path& path, int is_recursive) {
    bool exists = true;
    RETURN_IF_ERROR(exists_impl(path, &exists));
    if (!exists) {
        return Status::OK();
    }
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    int res = hdfsDelete(_fs_handle->hdfs_fs, real_path.string().c_str(), is_recursive);
    if (res == -1) {
        return Status::IOError("failed to delete directory {}: {}", path.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::exists_impl(const Path& path, bool* res) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    int is_exists = hdfsExists(_fs_handle->hdfs_fs, real_path.string().c_str());
#ifdef USE_HADOOP_HDFS
    // when calling hdfsExists() and return non-zero code,
    // if errno is ENOENT, which means the file does not exist.
    // if errno is not ENOENT, which means it encounter other error, should return.
    // NOTE: not for libhdfs3 since it only runs on MaxOS, don't have to support it.
    //
    // See details:
    //  https://github.com/apache/hadoop/blob/5cda162a804fb0cfc2a5ac0058ab407662c5fb00/
    //  hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/hdfs.c#L1923-L1924
    if (is_exists != 0 && errno != ENOENT) {
        char* root_cause = hdfsGetLastExceptionRootCause();
        return Status::IOError("failed to check path existence {}: {}", path.native(),
                               (root_cause ? root_cause : "unknown"));
    }
#endif
    *res = (is_exists == 0);
    return Status::OK();
}

Status HdfsFileSystem::file_size_impl(const Path& path, int64_t* file_size) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    hdfsFileInfo* file_info = hdfsGetPathInfo(_fs_handle->hdfs_fs, real_path.string().c_str());
    if (file_info == nullptr) {
        return Status::IOError("failed to get file size of {}: {}", path.native(), hdfs_error());
    }
    *file_size = file_info->mSize;
    hdfsFreeFileInfo(file_info, 1);
    return Status::OK();
}

Status HdfsFileSystem::list_impl(const Path& path, bool only_file, std::vector<FileInfo>* files,
                                 bool* exists) {
    RETURN_IF_ERROR(exists_impl(path, exists));
    if (!(*exists)) {
        return Status::OK();
    }

    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    int numEntries = 0;
    hdfsFileInfo* hdfs_file_info =
            hdfsListDirectory(_fs_handle->hdfs_fs, real_path.c_str(), &numEntries);
    if (hdfs_file_info == nullptr) {
        return Status::IOError("failed to list files/directors {}: {}", path.native(),
                               hdfs_error());
    }
    for (int idx = 0; idx < numEntries; ++idx) {
        auto& file = hdfs_file_info[idx];
        if (only_file && file.mKind == kObjectKindDirectory) {
            continue;
        }
        auto& file_info = files->emplace_back();
        std::string_view fname(file.mName);
        fname.remove_prefix(fname.rfind('/') + 1);
        file_info.file_name = fname;
        file_info.file_size = file.mSize;
        file_info.is_file = (file.mKind != kObjectKindDirectory);
    }
    hdfsFreeFileInfo(hdfs_file_info, numEntries);
    return Status::OK();
}

Status HdfsFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    Path normal_orig_name = convert_path(orig_name, _fs_name);
    Path normal_new_name = convert_path(new_name, _fs_name);
    int ret = hdfsRename(_fs_handle->hdfs_fs, normal_orig_name.c_str(), normal_new_name.c_str());
    if (ret == 0) {
        LOG(INFO) << "finished to rename file. orig: " << normal_orig_name
                  << ", new: " << normal_new_name;
        return Status::OK();
    } else {
        return Status::IOError("fail to rename from {} to {}: {}", normal_orig_name.native(),
                               normal_new_name.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    // 1. open local file for read
    FileSystemSPtr local_fs = global_local_filesystem();
    FileReaderSPtr local_reader = nullptr;
    RETURN_IF_ERROR(local_fs->open_file(local_file, &local_reader));
    int64_t file_len = local_reader->size();
    if (file_len == -1) {
        return Status::IOError("failed to get size of file: {}", local_file.string());
    }

    // 2. open remote file for write
    FileWriterPtr hdfs_writer = nullptr;
    RETURN_IF_ERROR(create_file_impl(remote_file, &hdfs_writer, nullptr));

    constexpr size_t buf_sz = 1024 * 1024;
    char read_buf[buf_sz];
    size_t left_len = file_len;
    size_t read_offset = 0;
    size_t bytes_read = 0;
    while (left_len > 0) {
        size_t read_len = left_len > buf_sz ? buf_sz : left_len;
        RETURN_IF_ERROR(local_reader->read_at(read_offset, {read_buf, read_len}, &bytes_read));
        RETURN_IF_ERROR(hdfs_writer->append({read_buf, read_len}));

        read_offset += read_len;
        left_len -= read_len;
    }

    return hdfs_writer->close();
}

Status HdfsFileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                         const std::vector<Path>& remote_files) {
    DCHECK(local_files.size() == remote_files.size());
    for (int i = 0; i < local_files.size(); ++i) {
        RETURN_IF_ERROR(upload_impl(local_files[i], remote_files[i]));
    }
    return Status::OK();
}

Status HdfsFileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    // 1. open remote file for read
    FileReaderSPtr hdfs_reader = nullptr;
    RETURN_IF_ERROR(open_file_internal(remote_file, &hdfs_reader, FileReaderOptions::DEFAULT));

    // 2. remove the existing local file if exist
    if (std::filesystem::remove(local_file)) {
        LOG(INFO) << "remove the previously exist local file: " << local_file;
    }

    // 3. open local file for write
    FileSystemSPtr local_fs = global_local_filesystem();
    FileWriterPtr local_writer = nullptr;
    RETURN_IF_ERROR(local_fs->create_file(local_file, &local_writer));

    // 4. read remote and write to local
    LOG(INFO) << "read remote file: " << remote_file << " to local: " << local_file;
    constexpr size_t buf_sz = 1024 * 1024;
    std::unique_ptr<char[]> read_buf(new char[buf_sz]);
    size_t cur_offset = 0;
    while (true) {
        size_t read_len = 0;
        Slice file_slice(read_buf.get(), buf_sz);
        RETURN_IF_ERROR(hdfs_reader->read_at(cur_offset, file_slice, &read_len));
        cur_offset += read_len;
        if (read_len == 0) {
            break;
        }

        RETURN_IF_ERROR(local_writer->append({read_buf.get(), read_len}));
    }
    return local_writer->close();
}

} // namespace doris::io
