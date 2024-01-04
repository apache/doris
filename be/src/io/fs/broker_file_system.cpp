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

#include "io/fs/broker_file_system.h"

#include <fmt/format.h>
#include <gen_cpp/PaloBrokerService_types.h>
#include <gen_cpp/TPaloBrokerService.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <thrift/Thrift.h>
#include <thrift/transport/TTransportException.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <ostream>
#include <thread>
#include <utility>

#include "common/config.h"
#include "io/fs/broker_file_reader.h"
#include "io/fs/broker_file_writer.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/broker_mgr.h"
#include "runtime/exec_env.h"
#include "util/slice.h"

namespace doris::io {

#ifdef BE_TEST
inline BrokerServiceClientCache* client_cache() {
    static BrokerServiceClientCache s_client_cache;
    return &s_client_cache;
}

inline const std::string& client_id(const TNetworkAddress& addr) {
    static std::string s_client_id = "doris_unit_test";
    return s_client_id;
}
#else
inline BrokerServiceClientCache* client_cache() {
    return ExecEnv::GetInstance()->broker_client_cache();
}

inline const std::string& client_id(const TNetworkAddress& addr) {
    return ExecEnv::GetInstance()->broker_mgr()->get_client_id(addr);
}
#endif

#ifndef CHECK_BROKER_CLIENT
#define CHECK_BROKER_CLIENT(client)                         \
    if (!client || !client->is_alive()) {                   \
        return Status::IOError("connect to broker failed"); \
    }
#endif

Status BrokerFileSystem::create(const TNetworkAddress& broker_addr,
                                const std::map<std::string, std::string>& broker_prop,
                                std::shared_ptr<BrokerFileSystem>* fs) {
    (*fs).reset(new BrokerFileSystem(broker_addr, broker_prop));
    return (*fs)->connect();
}

BrokerFileSystem::BrokerFileSystem(const TNetworkAddress& broker_addr,
                                   const std::map<std::string, std::string>& broker_prop)
        : RemoteFileSystem("", "", FileSystemType::BROKER),
          _broker_addr(broker_addr),
          _broker_prop(broker_prop) {}

Status BrokerFileSystem::connect_impl() {
    Status status = Status::OK();
    _connection = std::make_unique<BrokerServiceConnection>(client_cache(), _broker_addr,
                                                            config::thrift_rpc_timeout_ms, &status);
    return status;
}

Status BrokerFileSystem::create_file_impl(const Path& path, FileWriterPtr* writer,
                                          const FileWriterOptions* opts) {
    *writer = std::make_unique<BrokerFileWriter>(ExecEnv::GetInstance(), _broker_addr, _broker_prop,
                                                 path, 0 /* offset */, getSPtr());
    return Status::OK();
}

Status BrokerFileSystem::open_file_internal(const FileDescription& fd, const Path& abs_path,
                                            FileReaderSPtr* reader) {
    int64_t fsize = fd.file_size;
    if (fsize <= 0) {
        RETURN_IF_ERROR(file_size_impl(abs_path, &fsize));
    }

    CHECK_BROKER_CLIENT(_connection);
    TBrokerOpenReaderRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_path(abs_path);
    request.__set_startOffset(0);
    request.__set_clientId(client_id(_broker_addr));
    request.__set_properties(_broker_prop);

    std::unique_ptr<TBrokerOpenReaderResponse> response(new TBrokerOpenReaderResponse());
    try {
        Status status;
        try {
            (*_connection)->openReader(*response, request);
        } catch (apache::thrift::transport::TTransportException&) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            RETURN_IF_ERROR((*_connection).reopen());
            (*_connection)->openReader(*response, request);
        }
    } catch (apache::thrift::TException& e) {
        return Status::RpcError("failed to open file {}: {}", abs_path.native(),
                                error_msg(e.what()));
    }

    if (response->opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        return Status::IOError("failed to open file {}: {}", abs_path.native(),
                               error_msg(response->opStatus.message));
    }
    *reader = std::make_shared<BrokerFileReader>(
            _broker_addr, abs_path, fsize, response->fd,
            std::static_pointer_cast<BrokerFileSystem>(shared_from_this()));
    return Status::OK();
}

Status BrokerFileSystem::create_directory_impl(const Path& /*path*/, bool /*failed_if_exists*/) {
    return Status::NotSupported("create directory not implemented!");
}

Status BrokerFileSystem::delete_file_impl(const Path& file) {
    CHECK_BROKER_CLIENT(_connection);
    try {
        // rm file from remote path
        TBrokerDeletePathRequest del_req;
        TBrokerOperationStatus del_rep;
        del_req.__set_version(TBrokerVersion::VERSION_ONE);
        del_req.__set_path(file);
        del_req.__set_properties(_broker_prop);

        try {
            (*_connection)->deletePath(del_rep, del_req);
        } catch (apache::thrift::transport::TTransportException&) {
            RETURN_IF_ERROR((*_connection).reopen());
            (*_connection)->deletePath(del_rep, del_req);
        }

        if (del_rep.statusCode == TBrokerOperationStatusCode::OK) {
            return Status::OK();
        } else {
            return Status::IOError("failed to delete file {}: {}", file.native(),
                                   error_msg(del_rep.message));
        }
    } catch (apache::thrift::TException& e) {
        return Status::RpcError("failed to delete file {}: {}", file.native(), error_msg(e.what()));
    }
}

// Delete all files under path.
Status BrokerFileSystem::delete_directory_impl(const Path& dir) {
    return delete_file_impl(dir);
}

Status BrokerFileSystem::batch_delete_impl(const std::vector<Path>& files) {
    for (auto& file : files) {
        RETURN_IF_ERROR(delete_file_impl(file));
    }
    return Status::OK();
}

Status BrokerFileSystem::exists_impl(const Path& path, bool* res) const {
    CHECK_BROKER_CLIENT(_connection);
    *res = false;
    try {
        TBrokerCheckPathExistRequest check_req;
        TBrokerCheckPathExistResponse check_rep;
        check_req.__set_version(TBrokerVersion::VERSION_ONE);
        check_req.__set_path(path);
        check_req.__set_properties(_broker_prop);

        try {
            (*_connection)->checkPathExist(check_rep, check_req);
        } catch (apache::thrift::transport::TTransportException&) {
            RETURN_IF_ERROR((*_connection).reopen());
            (*_connection)->checkPathExist(check_rep, check_req);
        }

        if (check_rep.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            return Status::IOError("failed to check exist of path {}: {}", path.native(),
                                   error_msg(check_rep.opStatus.message));
        } else if (!check_rep.isPathExist) {
            *res = false;
            return Status::OK();
        } else {
            *res = true;
            return Status::OK();
        }
    } catch (apache::thrift::TException& e) {
        return Status::RpcError("failed to check exist of path {}: {}", path.native(),
                                error_msg(e.what()));
    }
}

Status BrokerFileSystem::file_size_impl(const Path& path, int64_t* file_size) const {
    CHECK_BROKER_CLIENT(_connection);
    try {
        TBrokerFileSizeRequest req;
        req.__set_version(TBrokerVersion::VERSION_ONE);
        req.__set_path(path);
        req.__set_properties(_broker_prop);

        TBrokerFileSizeResponse resp;
        try {
            (*_connection)->fileSize(resp, req);
        } catch (apache::thrift::transport::TTransportException&) {
            RETURN_IF_ERROR((*_connection).reopen());
            (*_connection)->fileSize(resp, req);
        }

        if (resp.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            return Status::IOError("failed to get file size of path {}: {}", path.native(),
                                   error_msg(resp.opStatus.message));
        }
        if (resp.fileSize < 0) {
            return Status::IOError("failed to get file size of path {}: size is negtive: {}",
                                   path.native(), resp.fileSize);
        }
        *file_size = resp.fileSize;
        return Status::OK();
    } catch (apache::thrift::TException& e) {
        return Status::RpcError("failed to get file size of path {}: {}", path.native(),
                                error_msg(e.what()));
    }
}

Status BrokerFileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                                   bool* exists) {
    RETURN_IF_ERROR(exists_impl(dir, exists));
    if (!(*exists)) {
        return Status::OK();
    }
    CHECK_BROKER_CLIENT(_connection);
    Status status = Status::OK();
    try {
        // get existing files from remote path
        TBrokerListResponse list_rep;
        TBrokerListPathRequest list_req;
        list_req.__set_version(TBrokerVersion::VERSION_ONE);
        list_req.__set_path(dir / "*");
        list_req.__set_isRecursive(false);
        list_req.__set_properties(_broker_prop);
        list_req.__set_fileNameOnly(true); // we only need file name, not abs path

        try {
            (*_connection)->listPath(list_rep, list_req);
        } catch (apache::thrift::transport::TTransportException&) {
            RETURN_IF_ERROR((*_connection).reopen());
            (*_connection)->listPath(list_rep, list_req);
        }

        if (list_rep.opStatus.statusCode == TBrokerOperationStatusCode::FILE_NOT_FOUND) {
            LOG(INFO) << "path does not exist: " << dir;
            *exists = false;
            return Status::OK();
        } else if (list_rep.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            return Status::IOError("failed to list dir {}: {}", dir.native(),
                                   error_msg(list_rep.opStatus.message));
        }
        LOG(INFO) << "finished to list files from remote path. file num: " << list_rep.files.size();
        *exists = true;

        // split file name and checksum
        for (const auto& file : list_rep.files) {
            if (only_file && file.isDir) {
                // this is not a file
                continue;
            }
            FileInfo file_info;
            file_info.file_name = file.path;
            file_info.file_size = file.size;
            file_info.is_file = !file.isDir;
            files->emplace_back(std::move(file_info));
        }

        LOG(INFO) << "finished to split files. valid file num: " << files->size();
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "failed to list files in remote path: " << dir << ", msg: " << e.what();
        return Status::RpcError("failed to list dir {}: {}", dir.native(), error_msg(e.what()));
    }
    return status;
}

Status BrokerFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    CHECK_BROKER_CLIENT(_connection);
    try {
        TBrokerOperationStatus op_status;
        TBrokerRenamePathRequest rename_req;
        rename_req.__set_version(TBrokerVersion::VERSION_ONE);
        rename_req.__set_srcPath(orig_name);
        rename_req.__set_destPath(new_name);
        rename_req.__set_properties(_broker_prop);

        try {
            (*_connection)->renamePath(op_status, rename_req);
        } catch (apache::thrift::transport::TTransportException&) {
            RETURN_IF_ERROR((*_connection).reopen());
            (*_connection)->renamePath(op_status, rename_req);
        }

        if (op_status.statusCode != TBrokerOperationStatusCode::OK) {
            return Status::IOError("failed to rename from {} to {}: {}", orig_name.native(),
                                   new_name.native(), error_msg(op_status.message));
        }
    } catch (apache::thrift::TException& e) {
        return Status::RpcError("failed to rename from {} to {}: {}", orig_name.native(),
                                new_name.native(), error_msg(e.what()));
    }

    LOG(INFO) << "finished to rename file. orig: " << orig_name << ", new: " << new_name;
    return Status::OK();
}

Status BrokerFileSystem::rename_dir_impl(const Path& orig_name, const Path& new_name) {
    return rename_impl(orig_name, new_name);
}

Status BrokerFileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    // 1. open local file for read
    FileSystemSPtr local_fs = global_local_filesystem();
    FileReaderSPtr local_reader = nullptr;
    RETURN_IF_ERROR(local_fs->open_file(local_file, &local_reader));

    int64_t file_len = local_reader->size();
    if (file_len == -1) {
        return Status::IOError("failed to get length of file: {}: {}", local_file.native(),
                               error_msg(""));
    }

    // NOTICE: broker writer must be closed before calling rename
    FileWriterPtr broker_writer = nullptr;
    RETURN_IF_ERROR(create_file_impl(remote_file, &broker_writer, nullptr));

    constexpr size_t buf_sz = 1024 * 1024;
    char read_buf[buf_sz];
    size_t left_len = file_len;
    size_t read_offset = 0;
    size_t bytes_read = 0;
    while (left_len > 0) {
        size_t read_len = left_len > buf_sz ? buf_sz : left_len;
        RETURN_IF_ERROR(local_reader->read_at(read_offset, {read_buf, read_len}, &bytes_read));
        // write through broker
        RETURN_IF_ERROR(broker_writer->append({read_buf, read_len}));

        read_offset += read_len;
        left_len -= read_len;
    }

    // close manually, because we need to check its close status
    RETURN_IF_ERROR(broker_writer->close());
    LOG(INFO) << "finished to write file via broker. file: " << local_file
              << ", length: " << file_len;
    return Status::OK();
}

Status BrokerFileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                           const std::vector<Path>& remote_files) {
    DCHECK(local_files.size() == remote_files.size());
    for (int i = 0; i < local_files.size(); ++i) {
        RETURN_IF_ERROR(upload_impl(local_files[i], remote_files[i]));
    }
    return Status::OK();
}

Status BrokerFileSystem::direct_upload_impl(const Path& remote_file, const std::string& content) {
    FileWriterPtr broker_writer = nullptr;
    RETURN_IF_ERROR(create_file_impl(remote_file, &broker_writer, nullptr));
    RETURN_IF_ERROR(broker_writer->append({content}));
    return broker_writer->close();
}

Status BrokerFileSystem::upload_with_checksum_impl(const Path& local_file, const Path& remote_file,
                                                   const std::string& checksum) {
    std::string temp = remote_file.string() + ".part";
    std::string final_file = remote_file.string() + "." + checksum;
    RETURN_IF_ERROR(upload_impl(local_file, temp));
    return rename_impl(temp, final_file);
}

Status BrokerFileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    // 1. open remote file for read
    FileReaderSPtr broker_reader = nullptr;
    FileDescription fd;
    fd.path = remote_file.native();
    RETURN_IF_ERROR(open_file_internal(fd, remote_file, &broker_reader));

    // 2. remove the existing local file if exist
    if (std::filesystem::remove(local_file)) {
        VLOG(2) << "remove the previously exist local file: " << local_file;
    }

    // 3. open local file for write
    FileSystemSPtr local_fs = global_local_filesystem();
    FileWriterPtr local_writer = nullptr;
    RETURN_IF_ERROR(local_fs->create_file(local_file, &local_writer));

    // 4. read remote and write to local
    VLOG(2) << "read remote file: " << remote_file << " to local: " << local_file;
    constexpr size_t buf_sz = 1024 * 1024;
    std::unique_ptr<uint8_t[]> read_buf(new uint8_t[buf_sz]);
    size_t write_offset = 0;
    size_t cur_offset = 0;
    while (true) {
        size_t read_len = 0;
        Slice file_slice(read_buf.get(), buf_sz);
        RETURN_IF_ERROR(broker_reader->read_at(cur_offset, file_slice, &read_len));
        cur_offset += read_len;
        if (read_len == 0) {
            break;
        }

        RETURN_IF_ERROR(local_writer->write_at(write_offset, {read_buf.get(), read_len}));
        write_offset += read_len;
    } // file_handler should be closed before calculating checksum

    return Status::OK();
}

Status BrokerFileSystem::direct_download_impl(const Path& remote_file, std::string* content) {
    // 1. open remote file for read
    FileReaderSPtr broker_reader = nullptr;
    FileDescription fd;
    fd.path = remote_file.native();
    RETURN_IF_ERROR(open_file_internal(fd, remote_file, &broker_reader));

    constexpr size_t buf_sz = 1024 * 1024;
    std::unique_ptr<char[]> read_buf(new char[buf_sz]);
    size_t write_offset = 0;
    size_t cur_offset = 0;
    while (true) {
        size_t read_len = 0;
        Slice file_slice(read_buf.get(), buf_sz);
        RETURN_IF_ERROR(broker_reader->read_at(cur_offset, file_slice, &read_len));
        cur_offset += read_len;
        if (read_len == 0) {
            break;
        }

        content->insert(write_offset, read_buf.get(), read_len);
        write_offset += read_len;
    }
    return Status::OK();
}

Status BrokerFileSystem::read_file(const TBrokerFD& fd, size_t offset, size_t bytes_req,
                                   std::string* data) const {
    if (data == nullptr) {
        return Status::InvalidArgument("data should be not null");
    }
    CHECK_BROKER_CLIENT(_connection);
    TBrokerPReadRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(fd);
    request.__set_offset(offset);
    request.__set_length(bytes_req);

    TBrokerReadResponse response;
    try {
        VLOG_RPC << "send pread request to broker:" << _broker_addr << " position:" << offset
                 << ", read bytes length:" << bytes_req;
        try {
            (*_connection)->pread(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            RETURN_IF_ERROR((*_connection).reopen());
            LOG(INFO) << "retry reading from broker: " << _broker_addr << ". reason: " << e.what();
            (*_connection)->pread(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "read broker file failed, broker:" << _broker_addr << " failed:" << e.what();
        return Status::RpcError(ss.str());
    }

    if (response.opStatus.statusCode == TBrokerOperationStatusCode::END_OF_FILE) {
        // read the end of broker's file
        return Status::OK();
    }
    if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Open broker reader failed, broker:" << _broker_addr
           << " failed:" << response.opStatus.message;
        return Status::InternalError(ss.str());
    }
    *data = std::move(response.data);
    return Status::OK();
}

Status BrokerFileSystem::close_file(const TBrokerFD& fd) const {
    CHECK_BROKER_CLIENT(_connection);
    TBrokerCloseReaderRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(fd);

    TBrokerOperationStatus response;
    try {
        try {
            (*_connection)->closeReader(response, request);
        } catch (apache::thrift::transport::TTransportException&) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            RETURN_IF_ERROR((*_connection).reopen());
            (*_connection)->closeReader(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "close broker file failed, broker:" << _broker_addr << " failed:" << e.what();
        return Status::RpcError(ss.str());
    }

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "close broker file failed, broker:" << _broker_addr << " failed:" << response.message;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

std::string BrokerFileSystem::error_msg(const std::string& err) const {
    return fmt::format("({}:{}), {}", _broker_addr.hostname, _broker_addr.port, err);
}

} // namespace doris::io
