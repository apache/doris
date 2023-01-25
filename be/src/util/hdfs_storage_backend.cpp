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

#include "util/hdfs_storage_backend.h"

#include "io/hdfs_file_reader.h"
#include "io/hdfs_writer.h"
#include "olap/file_helper.h"
#include "util/hdfs_util.h"

namespace doris {

#ifndef CHECK_HDFS_CLIENT
#define CHECK_HDFS_CLIENT(client)                                \
    if (!client) {                                               \
        return Status::InternalError("init hdfs client error."); \
    }
#endif

static const std::string hdfs_file_prefix = "hdfs://";

HDFSStorageBackend::HDFSStorageBackend(const std::map<std::string, std::string>& prop)
        : _properties(prop), _builder(createHDFSBuilder(_properties)) {
    _hdfs_fs = HDFSHandle::instance().create_hdfs_fs(_builder);
    DCHECK(_hdfs_fs) << "init hdfs client error.";
}

HDFSStorageBackend::~HDFSStorageBackend() {
    close();
}

Status HDFSStorageBackend::close() {
    if (_hdfs_fs != nullptr) {
        hdfsDisconnect(_hdfs_fs);
        _hdfs_fs = nullptr;
    }
    return Status::OK();
}

// if the format of path is hdfs://ip:port/path, replace it to /path.
// path like hdfs://ip:port/path can't be used by libhdfs3.
std::string HDFSStorageBackend::parse_path(const std::string& path) {
    if (path.find(hdfs_file_prefix) != std::string::npos) {
        std::string temp = path.substr(hdfs_file_prefix.size());
        std::size_t pos = temp.find_first_of('/');
        return temp.substr(pos);
    } else {
        return path;
    }
}

Status HDFSStorageBackend::upload(const std::string& local, const std::string& remote) {
    FileHandler file_handler;
    Status ost = file_handler.open(local, O_RDONLY);
    if (!ost.ok()) {
        return Status::InternalError("failed to open file: " + local);
    }

    size_t file_len = file_handler.length();
    if (file_len == -1) {
        return Status::InternalError("failed to get length of file: " + local);
    }

    std::unique_ptr<HDFSWriter> hdfs_writer(new HDFSWriter(_properties, remote));
    RETURN_IF_ERROR(hdfs_writer->open());
    constexpr size_t buf_sz = 1024 * 1024;
    char read_buf[buf_sz];
    size_t left_len = file_len;
    size_t read_offset = 0;
    while (left_len > 0) {
        size_t read_len = left_len > buf_sz ? buf_sz : left_len;
        ost = file_handler.pread(read_buf, read_len, read_offset);
        if (!ost.ok()) {
            return Status::InternalError("failed to read file: " + local);
        }

        size_t write_len = 0;
        RETURN_IF_ERROR(hdfs_writer->write(reinterpret_cast<const uint8_t*>(read_buf), read_len,
                                           &write_len));
        DCHECK_EQ(write_len, read_len);

        read_offset += read_len;
        left_len -= read_len;
    }

    LOG(INFO) << "finished to write file: " << local << ", length: " << file_len;
    return Status::OK();
}

Status HDFSStorageBackend::direct_upload(const std::string& remote, const std::string& content) {
    std::unique_ptr<HDFSWriter> hdfs_writer(new HDFSWriter(_properties, remote));
    RETURN_IF_ERROR(hdfs_writer->open());
    size_t write_len = 0;
    RETURN_IF_ERROR(hdfs_writer->write(reinterpret_cast<const uint8_t*>(content.c_str()),
                                       content.size(), &write_len));
    DCHECK_EQ(write_len, content.size());
    return Status::OK();
}

Status HDFSStorageBackend::list(const std::string& remote_path, bool contain_md5, bool recursion,
                                std::map<std::string, FileStat>* files) {
    CHECK_HDFS_CLIENT(_hdfs_fs);
    std::string normal_str = parse_path(remote_path);
    int exists = hdfsExists(_hdfs_fs, normal_str.c_str());
    if (exists != 0) {
        LOG(INFO) << "path does not exist: " << normal_str << ", err: " << strerror(errno);
        return Status::OK();
    }

    int file_num = 0;
    hdfsFileInfo* files_info = hdfsListDirectory(_hdfs_fs, normal_str.c_str(), &file_num);
    if (files_info == nullptr) {
        std::stringstream ss;
        ss << "failed to list files from remote path: " << normal_str
           << ", err: " << strerror(errno);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    LOG(INFO) << "finished to list files from remote path:" << normal_str
              << ", file num:" << file_num;
    for (int i = 0; i < file_num; ++i) {
        auto file_info = files_info[i];
        if (file_info.mKind == kObjectKindDirectory) {
            continue;
        }
        const std::string& file_name_with_path(file_info.mName);
        // get filename
        std::filesystem::path file_path(file_name_with_path);
        std::string file_name = file_path.filename();
        size_t pos = file_name.find_last_of('.');
        if (pos == std::string::npos || pos == file_name.size() - 1) {
            // Not found checksum separator, ignore this file
            continue;
        }

        FileStat stat = {std::string(file_name, 0, pos), std::string(file_name, pos + 1),
                         file_info.mSize};
        files->emplace(std::string(file_name, 0, pos), stat);
        VLOG(2) << "split remote file: " << std::string(file_name, 0, pos)
                << ", checksum: " << std::string(file_name, pos + 1);
    }

    hdfsFreeFileInfo(files_info, file_num);
    LOG(INFO) << "finished to split files. valid file num: " << files->size();
    return Status::OK();
}

Status HDFSStorageBackend::download(const std::string& remote, const std::string& local) {
    // 1. open remote file for read
    std::unique_ptr<HdfsFileReader> hdfs_reader(new HdfsFileReader(_properties, remote, 0));
    RETURN_IF_ERROR(hdfs_reader->open());

    // 2. remove the existing local file if exist
    if (std::filesystem::remove(local)) {
        LOG(INFO) << "remove the previously exist local file: " << local;
    }

    // 3. open local file for write
    FileHandler file_handler;
    Status ost =
            file_handler.open_with_mode(local, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if (!ost.ok()) {
        return Status::InternalError("failed to open file: " + local);
    }

    // 4. read remote and write to local
    LOG(INFO) << "read remote file: " << remote << " to local: " << local;
    constexpr size_t buf_sz = 1024 * 1024;
    char read_buf[buf_sz];
    size_t write_offset = 0;
    bool eof = false;
    while (!eof) {
        int64_t read_len = 0;
        RETURN_IF_ERROR(
                hdfs_reader->read(reinterpret_cast<uint8_t*>(read_buf), buf_sz, &read_len, &eof));
        if (eof) {
            continue;
        }

        if (read_len > 0) {
            ost = file_handler.pwrite(read_buf, read_len, write_offset);
            if (!ost.ok()) {
                return Status::InternalError("failed to write file: " + local);
            }

            write_offset += read_len;
        }
    }

    return Status::OK();
}

Status HDFSStorageBackend::direct_download(const std::string& remote, std::string* content) {
    std::unique_ptr<HdfsFileReader> hdfs_reader(new HdfsFileReader(_properties, remote, 0));
    RETURN_IF_ERROR(hdfs_reader->open());
    constexpr size_t buf_sz = 1024 * 1024;
    char read_buf[buf_sz];
    size_t write_offset = 0;
    bool eof = false;
    while (!eof) {
        int64_t read_len = 0;
        RETURN_IF_ERROR(
                hdfs_reader->read(reinterpret_cast<uint8_t*>(read_buf), buf_sz, &read_len, &eof));

        if (eof) {
            continue;
        }

        if (read_len > 0) {
            content->insert(write_offset, read_buf, read_len);
            write_offset += read_len;
        }
    }
    return Status::OK();
}

Status HDFSStorageBackend::upload_with_checksum(const std::string& local, const std::string& remote,
                                                const std::string& checksum) {
    std::string temp = remote + ".part";
    std::string final = remote + "." + checksum;
    RETURN_IF_ERROR(upload(local, temp));
    return rename(temp, final);
}

Status HDFSStorageBackend::rename(const std::string& orig_name, const std::string& new_name) {
    CHECK_HDFS_CLIENT(_hdfs_fs);
    std::string normal_orig_name = parse_path(orig_name);
    std::string normal_new_name = parse_path(new_name);
    int ret = hdfsRename(_hdfs_fs, normal_orig_name.c_str(), normal_new_name.c_str());
    if (ret == 0) {
        LOG(INFO) << "finished to rename file. orig: " << normal_orig_name
                  << ", new: " << normal_new_name;
        return Status::OK();
    } else {
        std::stringstream ss;
        ss << "Fail to rename file: " << normal_orig_name << " to: " << normal_new_name;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
}

Status HDFSStorageBackend::rename_dir(const std::string& orig_name, const std::string& new_name) {
    return rename(orig_name, new_name);
}

Status HDFSStorageBackend::copy(const std::string& src, const std::string& dst) {
    return Status::NotSupported("copy not implemented!");
}

Status HDFSStorageBackend::copy_dir(const std::string& src, const std::string& dst) {
    return copy(src, dst);
}

Status HDFSStorageBackend::mkdir(const std::string& path) {
    return Status::NotSupported("mkdir not implemented!");
}

Status HDFSStorageBackend::mkdirs(const std::string& path) {
    return Status::NotSupported("mkdirs not implemented!");
}

Status HDFSStorageBackend::exist(const std::string& path) {
    CHECK_HDFS_CLIENT(_hdfs_fs);
    int exists = hdfsExists(_hdfs_fs, path.c_str());
    if (exists == 0) {
        return Status::OK();
    } else {
        return Status::NotFound(path + " not exists!");
    }
}

Status HDFSStorageBackend::exist_dir(const std::string& path) {
    return exist(path);
}

Status HDFSStorageBackend::rm(const std::string& remote) {
    CHECK_HDFS_CLIENT(_hdfs_fs);
    int ret = hdfsDelete(_hdfs_fs, remote.c_str(), 1);
    if (ret == 0) {
        return Status::OK();
    } else {
        std::stringstream ss;
        ss << "failed to delete from remote path: " << remote;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
}

Status HDFSStorageBackend::rmdir(const std::string& remote) {
    return rm(remote);
}

} // end namespace doris