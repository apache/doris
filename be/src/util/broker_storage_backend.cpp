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

#include "util/broker_storage_backend.h"

#include "env/env.h"
#include "exec/broker_reader.h"
#include "exec/broker_writer.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "olap/file_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"

namespace doris {

#ifdef BE_TEST
inline BrokerServiceClientCache* client_cache(ExecEnv* env) {
    static BrokerServiceClientCache s_client_cache;
    return &s_client_cache;
}
#else
inline BrokerServiceClientCache* client_cache(ExecEnv* env) {
    return env->broker_client_cache();
}
#endif

BrokerStorageBackend::BrokerStorageBackend(ExecEnv* env, const TNetworkAddress& broker_addr,
                                           const std::map<std::string, std::string>& broker_prop)
        : _env(env), _broker_addr(broker_addr), _broker_prop(broker_prop) {}

Status BrokerStorageBackend::download(const std::string& remote, const std::string& local) {
    // 1. open remote file for read
    std::vector<TNetworkAddress> broker_addrs;
    broker_addrs.push_back(_broker_addr);
    std::unique_ptr<BrokerReader> broker_reader(
            new BrokerReader(_env, broker_addrs, _broker_prop, remote, 0 /* offset */));
    RETURN_IF_ERROR(broker_reader->open());

    // 2. remove the existing local file if exist
    if (std::filesystem::remove(local)) {
        VLOG(2) << "remove the previously exist local file: " << local;
    }

    // 3. open local file for write
    FileHandler file_handler;
    Status ost =
            file_handler.open_with_mode(local, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if (!ost.ok()) {
        return Status::InternalError("failed to open file: " + local);
    }

    // 4. read remote and write to local
    VLOG(2) << "read remote file: " << remote << " to local: " << local;
    constexpr size_t buf_sz = 1024 * 1024;
    char read_buf[buf_sz];
    size_t write_offset = 0;
    bool eof = false;
    while (!eof) {
        int64_t read_len = 0;
        RETURN_IF_ERROR(
                broker_reader->read(reinterpret_cast<uint8_t*>(read_buf), buf_sz, &read_len, &eof));

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

    } // file_handler should be closed before calculating checksum

    return Status::OK();
}

Status BrokerStorageBackend::direct_download(const std::string& remote, std::string* content) {
    return Status::IOError("broker direct_download not support ");
}

Status BrokerStorageBackend::upload(const std::string& local, const std::string& remote) {
    // read file and write to broker
    FileHandler file_handler;
    Status ost = file_handler.open(local, O_RDONLY);
    if (!ost.ok()) {
        return Status::InternalError("failed to open file: " + local);
    }

    size_t file_len = file_handler.length();
    if (file_len == -1) {
        return Status::InternalError("failed to get length of file: " + local);
    }

    // NOTICE: broker writer must be closed before calling rename
    std::vector<TNetworkAddress> broker_addrs;
    broker_addrs.push_back(_broker_addr);
    std::unique_ptr<BrokerWriter> broker_writer(
            new BrokerWriter(_env, broker_addrs, _broker_prop, remote, 0 /* offset */));
    RETURN_IF_ERROR(broker_writer->open());

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
        // write through broker
        size_t write_len = 0;
        RETURN_IF_ERROR(broker_writer->write(reinterpret_cast<const uint8_t*>(read_buf), read_len,
                                             &write_len));
        DCHECK_EQ(write_len, read_len);

        read_offset += read_len;
        left_len -= read_len;
    }

    // close manually, because we need to check its close status
    RETURN_IF_ERROR(broker_writer->close());

    LOG(INFO) << "finished to write file via broker. file: " << local << ", length: " << file_len;
    return Status::OK();
}

Status BrokerStorageBackend::rename(const std::string& orig_name, const std::string& new_name) {
    Status status = Status::OK();
    BrokerServiceConnection client(client_cache(_env), _broker_addr, config::thrift_rpc_timeout_ms,
                                   &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to get broker client. "
           << "broker addr: " << _broker_addr << ". msg: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    try {
        TBrokerOperationStatus op_status;
        TBrokerRenamePathRequest rename_req;
        rename_req.__set_version(TBrokerVersion::VERSION_ONE);
        rename_req.__set_srcPath(orig_name);
        rename_req.__set_destPath(new_name);
        rename_req.__set_properties(_broker_prop);

        try {
            client->renamePath(op_status, rename_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->renamePath(op_status, rename_req);
        }

        if (op_status.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "Fail to rename file: " << orig_name << " to: " << new_name
               << " msg:" << op_status.message;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Fail to rename file: " << orig_name << " to: " << new_name << " msg:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    LOG(INFO) << "finished to rename file. orig: " << orig_name << ", new: " << new_name;

    return status;
}

Status BrokerStorageBackend::rename_dir(const std::string& orig_name, const std::string& new_name) {
    return rename(orig_name, new_name);
}

Status BrokerStorageBackend::list(const std::string& remote_path, bool contain_md5, bool recursion,
                                  std::map<std::string, FileStat>* files) {
    Status status = Status::OK();
    BrokerServiceConnection client(client_cache(_env), _broker_addr, config::thrift_rpc_timeout_ms,
                                   &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to get broker client. "
           << "broker addr: " << _broker_addr << ". msg: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    try {
        // get existing files from remote path
        TBrokerListResponse list_rep;
        TBrokerListPathRequest list_req;
        list_req.__set_version(TBrokerVersion::VERSION_ONE);
        list_req.__set_path(remote_path + "/*");
        list_req.__set_isRecursive(false);
        list_req.__set_properties(_broker_prop);
        list_req.__set_fileNameOnly(true); // we only need file name, not abs path

        try {
            client->listPath(list_rep, list_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->listPath(list_rep, list_req);
        }

        if (list_rep.opStatus.statusCode == TBrokerOperationStatusCode::FILE_NOT_FOUND) {
            LOG(INFO) << "path does not exist: " << remote_path;
            return Status::OK();
        } else if (list_rep.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "failed to list files from remote path: " << remote_path
               << ", msg: " << list_rep.opStatus.message;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        LOG(INFO) << "finished to list files from remote path. file num: " << list_rep.files.size();

        // split file name and checksum
        for (const auto& file : list_rep.files) {
            if (file.isDir) {
                // this is not a file
                continue;
            }

            const std::string& file_name = file.path;
            size_t pos = file_name.find_last_of(".");
            if (pos == std::string::npos || pos == file_name.size() - 1) {
                // Not found checksum separator, ignore this file
                continue;
            }

            FileStat stat = {std::string(file_name, 0, pos), std::string(file_name, pos + 1),
                             file.size};
            files->emplace(std::string(file_name, 0, pos), stat);
            VLOG(2) << "split remote file: " << std::string(file_name, 0, pos)
                    << ", checksum: " << std::string(file_name, pos + 1);
        }

        LOG(INFO) << "finished to split files. valid file num: " << files->size();

    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "failed to list files in remote path: " << remote_path << ", msg: " << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    return status;
}
Status BrokerStorageBackend::direct_upload(const std::string& remote, const std::string& content) {
    std::vector<TNetworkAddress> broker_addrs;
    broker_addrs.push_back(_broker_addr);
    std::unique_ptr<BrokerWriter> broker_writer(
            new BrokerWriter(_env, broker_addrs, _broker_prop, remote, 0 /* offset */));
    RETURN_IF_ERROR(broker_writer->open());
    size_t write_len = 0;
    RETURN_IF_ERROR(broker_writer->write(reinterpret_cast<const uint8_t*>(content.c_str()),
                                         content.size(), &write_len));
    DCHECK_EQ(write_len, content.size());
    RETURN_IF_ERROR(broker_writer->close());
    return Status::OK();
}

Status BrokerStorageBackend::rm(const std::string& remote) {
    Status status = Status::OK();
    BrokerServiceConnection client(client_cache(_env), _broker_addr, config::thrift_rpc_timeout_ms,
                                   &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to get broker client. "
           << "broker addr: " << _broker_addr << ". msg: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    try {
        // rm file from remote path
        TBrokerDeletePathRequest del_req;
        TBrokerOperationStatus del_rep;
        del_req.__set_version(TBrokerVersion::VERSION_ONE);
        del_req.__set_path(remote);
        del_req.__set_properties(_broker_prop);

        try {
            client->deletePath(del_rep, del_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->deletePath(del_rep, del_req);
        }

        if (del_rep.statusCode == TBrokerOperationStatusCode::OK) {
            return Status::OK();
        } else {
            std::stringstream ss;
            ss << "failed to delete from remote path: " << remote << ", msg: " << del_rep.message;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "failed to delete file in remote path: " << remote << ", msg: " << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }
}

Status BrokerStorageBackend::rmdir(const std::string& remote) {
    return rm(remote);
}

Status BrokerStorageBackend::copy(const std::string& src, const std::string& dst) {
    return Status::NotSupported("copy not implemented!");
}

Status BrokerStorageBackend::copy_dir(const std::string& src, const std::string& dst) {
    return copy(src, dst);
}

Status BrokerStorageBackend::mkdir(const std::string& path) {
    return Status::NotSupported("mkdir not implemented!");
}

Status BrokerStorageBackend::mkdirs(const std::string& path) {
    return Status::NotSupported("mkdirs not implemented!");
}

Status BrokerStorageBackend::exist(const std::string& path) {
    Status status = Status::OK();
    BrokerServiceConnection client(client_cache(_env), _broker_addr, config::thrift_rpc_timeout_ms,
                                   &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to get broker client. "
           << "broker addr: " << _broker_addr << ". msg: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    try {
        TBrokerCheckPathExistRequest check_req;
        TBrokerCheckPathExistResponse check_rep;
        check_req.__set_version(TBrokerVersion::VERSION_ONE);
        check_req.__set_path(path);
        check_req.__set_properties(_broker_prop);

        try {
            client->checkPathExist(check_rep, check_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->checkPathExist(check_rep, check_req);
        }

        if (check_rep.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "failed to check exist: " << path << ", msg: " << check_rep.opStatus.message;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        } else if (!check_rep.isPathExist) {
            return Status::NotFound(path + " not exists!");
        } else {
            return Status::OK();
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "failed to check exist: " << path << ", msg: " << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }
}

Status BrokerStorageBackend::exist_dir(const std::string& path) {
    return exist(path);
}

Status BrokerStorageBackend::upload_with_checksum(const std::string& local,
                                                  const std::string& remote,
                                                  const std::string& checksum) {
    std::string temp = remote + ".part";
    std::string final = remote + "." + checksum;
    RETURN_IF_ERROR(upload(local, remote + ".part"));
    return rename(temp, final);
}

} // end namespace doris
