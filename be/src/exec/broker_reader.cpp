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

#include "exec/broker_reader.h"

#include <sstream>

#include "common/logging.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/thrift_util.h"

namespace doris {

// Broker

BrokerReader::BrokerReader(ExecEnv* env, const std::vector<TNetworkAddress>& broker_addresses,
                           const std::map<std::string, std::string>& properties,
                           const std::string& path, int64_t start_offset, int64_t file_size)
        : _env(env),
          _addresses(broker_addresses),
          _properties(properties),
          _path(path),
          _cur_offset(start_offset),
          _is_fd_valid(false),
          _file_size(file_size),
          _addr_idx(0) {}

BrokerReader::~BrokerReader() {
    close();
}

#ifdef BE_TEST
inline BrokerServiceClientCache* client_cache(ExecEnv* env) {
    static BrokerServiceClientCache s_client_cache;
    return &s_client_cache;
}

inline const std::string& client_id(ExecEnv* env, const TNetworkAddress& addr) {
    static std::string s_client_id = "doris_unit_test";
    return s_client_id;
}
#else
inline BrokerServiceClientCache* client_cache(ExecEnv* env) {
    return env->broker_client_cache();
}

inline const std::string& client_id(ExecEnv* env, const TNetworkAddress& addr) {
    return env->broker_mgr()->get_client_id(addr);
}
#endif

Status BrokerReader::open() {
    TBrokerOpenReaderRequest request;

    const TNetworkAddress& broker_addr = _addresses[_addr_idx];
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_path(_path);
    request.__set_startOffset(_cur_offset);
    request.__set_clientId(client_id(_env, broker_addr));
    request.__set_properties(_properties);

    TBrokerOpenReaderResponse response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(_env), broker_addr,
                                       config::thrift_rpc_timeout_ms, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker client failed. broker=" << broker_addr
                         << ", status=" << status.get_error_msg();
            return status;
        }

        try {
            client->openReader(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            RETURN_IF_ERROR(client.reopen());
            client->openReader(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Open broker reader failed, broker:" << broker_addr << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Open broker reader failed, broker:" << broker_addr
           << " failed:" << response.opStatus.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    // TODO(cmy): The file size is no longer got from openReader() method.
    // But leave the code here for compatibility.
    // This will be removed later.
    if (response.__isset.size) {
        _file_size = response.size;
    }

    _fd = response.fd;
    _is_fd_valid = true;
    return Status::OK();
}

//not support
Status BrokerReader::read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) {
    return Status::NotSupported("broker reader doesn't support read_one_message interface");
}

Status BrokerReader::read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) {
    DCHECK_NE(buf_len, 0);
    RETURN_IF_ERROR(readat(_cur_offset, buf_len, bytes_read, buf));
    if (*bytes_read == 0) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status BrokerReader::readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    const TNetworkAddress& broker_addr = _addresses[_addr_idx];
    TBrokerPReadRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);
    request.__set_offset(position);
    request.__set_length(nbytes);

    TBrokerReadResponse response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(_env), broker_addr,
                                       config::thrift_rpc_timeout_ms, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker client failed. broker=" << broker_addr
                         << ", status=" << status.get_error_msg();
            return status;
        }

        VLOG_RPC << "send pread request to broker:" << broker_addr << " position:" << position
                 << ", read bytes length:" << nbytes;

        try {
            client->pread(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            RETURN_IF_ERROR(client.reopen());
            LOG(INFO) << "retry reading from broker: " << broker_addr << ". reason: " << e.what();
            client->pread(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Read from broker failed, broker:" << broker_addr << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    if (response.opStatus.statusCode == TBrokerOperationStatusCode::END_OF_FILE) {
        // read the end of broker's file
        *bytes_read = 0;
        return Status::OK();
    } else if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Read from broker failed, broker:" << broker_addr
           << " failed:" << response.opStatus.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    *bytes_read = response.data.size();
    memcpy(out, response.data.data(), *bytes_read);
    _cur_offset = position + *bytes_read;
    return Status::OK();
}

int64_t BrokerReader::size() {
    return _file_size;
}

Status BrokerReader::seek(int64_t position) {
    _cur_offset = position;
    return Status::OK();
}

Status BrokerReader::tell(int64_t* position) {
    *position = _cur_offset;
    return Status::OK();
}

bool BrokerReader::closed() {
    return !_is_fd_valid; //return true iff closed
}

void BrokerReader::close() {
    if (!_is_fd_valid) {
        return;
    }
    TBrokerCloseReaderRequest request;

    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);

    const TNetworkAddress& broker_addr = _addresses[_addr_idx];
    TBrokerOperationStatus response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(_env), broker_addr,
                                       config::thrift_rpc_timeout_ms, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker client failed. broker=" << broker_addr
                         << ", status=" << status.get_error_msg();
            return;
        }

        try {
            client->closeReader(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            status = client.reopen();
            if (!status.ok()) {
                LOG(WARNING) << "Close broker reader failed. broker=" << broker_addr
                             << ", status=" << status.get_error_msg();
                return;
            }
            client->closeReader(response, request);
        }
    } catch (apache::thrift::TException& e) {
        LOG(WARNING) << "Close broker reader failed, broker:" << broker_addr
                     << " failed:" << e.what();
        return;
    }

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        LOG(WARNING) << "Open broker reader failed, broker:" << broker_addr
                     << " failed:" << response.message;
        return;
    }
    _is_fd_valid = false;
}

} // namespace doris
