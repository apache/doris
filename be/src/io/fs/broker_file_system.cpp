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

#include <gen_cpp/PaloBrokerService_types.h>
#include <gen_cpp/TPaloBrokerService.h>

#include "io/fs/broker_file_reader.h"
#include "runtime/broker_mgr.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"
#include "util/storage_backend.h"

namespace doris {
namespace io {

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
#define CHECK_BROKER_CLIENT(client)                               \
    if (!client) {                                                \
        return Status::InternalError("init Broker client error"); \
    }
#endif

BrokerFileSystem::BrokerFileSystem(const TNetworkAddress& broker_addr,
                                   const std::map<std::string, std::string>& broker_prop,
                                   size_t file_size)
        : RemoteFileSystem("", "", FileSystemType::BROKER),
          _broker_addr(broker_addr),
          _broker_prop(broker_prop),
          _file_size(file_size) {}

Status BrokerFileSystem::connect() {
    Status status = Status::OK();
    _client.reset(new BrokerServiceConnection(client_cache(), _broker_addr,
                                              config::thrift_rpc_timeout_ms, &status));
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to get broker client. "
           << "broker addr: " << _broker_addr << ". msg: " << status;
        status = Status::InternalError(ss.str());
    }
    return status;
}

Status BrokerFileSystem::open_file(const Path& path, FileReaderSPtr* reader) {
    CHECK_BROKER_CLIENT(_client);
    TBrokerOpenReaderRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_path(path);
    request.__set_startOffset(0);
    request.__set_clientId(client_id(_broker_addr));
    request.__set_properties(_broker_prop);

    TBrokerOpenReaderResponse* response = new TBrokerOpenReaderResponse();
    Defer del_reponse {[&] { delete response; }};
    try {
        Status status;
        try {
            (*_client)->openReader(*response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            RETURN_IF_ERROR((*_client).reopen());
            (*_client)->openReader(*response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Open broker reader failed, broker:" << _broker_addr << " failed: " << e.what();
        return Status::RpcError(ss.str());
    }

    if (response->opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Open broker reader failed, broker: " << _broker_addr
           << " failed: " << response->opStatus.message;
        return Status::InternalError(ss.str());
    }
    // TODO(cmy): The file size is no longer got from openReader() method.
    // But leave the code here for compatibility.
    // This will be removed later.
    TBrokerFD fd;
    if (response->__isset.size) {
        _file_size = response->size;
    }
    fd = response->fd;
    *reader = std::make_shared<BrokerFileReader>(_broker_addr, path, _file_size, fd, this);
    return Status::OK();
}

Status BrokerFileSystem::delete_file(const Path& path) {
    CHECK_BROKER_CLIENT(_client);
    try {
        // rm file from remote path
        TBrokerDeletePathRequest del_req;
        TBrokerOperationStatus del_rep;
        del_req.__set_version(TBrokerVersion::VERSION_ONE);
        del_req.__set_path(path);
        del_req.__set_properties(_broker_prop);

        try {
            (*_client)->deletePath(del_rep, del_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR((*_client).reopen());
            (*_client)->deletePath(del_rep, del_req);
        }

        if (del_rep.statusCode == TBrokerOperationStatusCode::OK) {
            return Status::OK();
        } else {
            std::stringstream ss;
            ss << "failed to delete from remote path: " << path << ", msg: " << del_rep.message;
            return Status::InternalError(ss.str());
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "failed to delete file in remote path: " << path << ", msg: " << e.what();
        return Status::RpcError(ss.str());
    }
}

Status BrokerFileSystem::create_directory(const Path& /*path*/) {
    return Status::NotSupported("create directory not implemented!");
}

// Delete all files under path.
Status BrokerFileSystem::delete_directory(const Path& path) {
    return delete_file(path);
}

Status BrokerFileSystem::exists(const Path& path, bool* res) const {
    CHECK_BROKER_CLIENT(_client);
    *res = false;
    try {
        TBrokerCheckPathExistRequest check_req;
        TBrokerCheckPathExistResponse check_rep;
        check_req.__set_version(TBrokerVersion::VERSION_ONE);
        check_req.__set_path(path);
        check_req.__set_properties(_broker_prop);

        try {
            (*_client)->checkPathExist(check_rep, check_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR((*_client).reopen());
            (*_client)->checkPathExist(check_rep, check_req);
        }

        if (check_rep.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "failed to check exist: " << path << ", msg: " << check_rep.opStatus.message;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        } else if (!check_rep.isPathExist) {
            return Status::NotFound("{} not exists!", path.string());
        } else {
            *res = true;
            return Status::OK();
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "failed to check exist: " << path << ", msg: " << e.what();
        return Status::RpcError(ss.str());
    }
}

Status BrokerFileSystem::file_size(const Path& path, size_t* file_size) const {
    *file_size = _file_size;
    return Status::OK();
}

Status BrokerFileSystem::list(const Path& path, std::vector<Path>* files) {
    CHECK_BROKER_CLIENT(_client);
    Status status = Status::OK();
    try {
        // get existing files from remote path
        TBrokerListResponse list_rep;
        TBrokerListPathRequest list_req;
        list_req.__set_version(TBrokerVersion::VERSION_ONE);
        list_req.__set_path(path / "*");
        list_req.__set_isRecursive(false);
        list_req.__set_properties(_broker_prop);
        list_req.__set_fileNameOnly(true); // we only need file name, not abs path

        try {
            (*_client)->listPath(list_rep, list_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR((*_client).reopen());
            (*_client)->listPath(list_rep, list_req);
        }

        if (list_rep.opStatus.statusCode == TBrokerOperationStatusCode::FILE_NOT_FOUND) {
            LOG(INFO) << "path does not exist: " << path;
            return Status::OK();
        } else if (list_rep.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "failed to list files from remote path: " << path
               << ", msg: " << list_rep.opStatus.message;
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
            size_t pos = file_name.find_last_of('.');
            if (pos == std::string::npos || pos == file_name.size() - 1) {
                // Not found checksum separator, ignore this file
                continue;
            }

            FileStat stat = {std::string(file_name, 0, pos), std::string(file_name, pos + 1),
                             file.size};
            files->emplace_back(std::string(file_name, 0, pos));
            VLOG(2) << "split remote file: " << std::string(file_name, 0, pos)
                    << ", checksum: " << std::string(file_name, pos + 1);
        }

        LOG(INFO) << "finished to split files. valid file num: " << files->size();

    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "failed to list files in remote path: " << path << ", msg: " << e.what();
        return Status::RpcError(ss.str());
    }
    return status;
}

Status BrokerFileSystem::get_client(std::shared_ptr<BrokerServiceConnection>* client) const {
    CHECK_BROKER_CLIENT(_client);
    *client = _client;
    return Status::OK();
}

} // namespace io
} // namespace doris
