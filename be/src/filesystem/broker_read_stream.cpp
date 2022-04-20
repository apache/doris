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

#include "filesystem/broker_read_stream.h"

#include "gen_cpp/TPaloBrokerService.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"

namespace doris {

#ifdef BE_TEST
BrokerServiceClientCache* client_cache(ExecEnv* env) {
    static BrokerServiceClientCache s_client_cache;
    return &s_client_cache;
}
#else
BrokerServiceClientCache* client_cache(ExecEnv* env) {
    return env->broker_client_cache();
}
#endif

BrokerReadStream::BrokerReadStream(ExecEnv* env, const std::vector<TNetworkAddress>& addrs,
                                   TBrokerFD fd, size_t file_size)
        : _env(env), _addrs(addrs), _fd(fd), _file_size(file_size) {}

BrokerReadStream::~BrokerReadStream() {
    close();
}

Status BrokerReadStream::read(char* to, size_t req_n, size_t* read_n) {
    RETURN_IF_ERROR(read_at(_offset, to, req_n, read_n));
    _offset += *read_n;
    return Status::OK();
}

Status BrokerReadStream::read_at(size_t position, char* to, size_t req_n, size_t* read_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _file_size) {
        return Status::IOError("Position exceeds range");
    }
    req_n = std::min(req_n, _file_size - position);
    if (req_n == 0) {
        *read_n = 0;
        return Status::OK();
    }

    const TNetworkAddress& addr = _addrs[_addr_idx];
    TBrokerPReadRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);
    request.__set_offset(position);
    request.__set_length(req_n);

    TBrokerReadResponse response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(_env), addr, config::thrift_rpc_timeout_ms,
                                       &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker client failed. broker=" << addr
                         << ", status=" << status.get_error_msg();
            return status;
        }

        VLOG_RPC << "send pread request to broker:" << addr << " position:" << position
                 << ", read bytes length:" << req_n;

        try {
            client->pread(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            RETURN_IF_ERROR(client.reopen());
            LOG(INFO) << "retry reading from broker: " << addr << ". reason: " << e.what();
            client->pread(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Read from broker failed, broker:" << addr << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    if (response.opStatus.statusCode == TBrokerOperationStatusCode::END_OF_FILE) {
        // read the end of broker's file
        *read_n = 0;
        return Status::OK();
    } else if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Read from broker failed, broker:" << addr << " failed:" << response.opStatus.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    *read_n = response.data.size();
    memcpy(to, response.data.data(), *read_n);
    return Status::OK();
}

Status BrokerReadStream::seek(size_t position) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _file_size) {
        return Status::IOError("Position exceeds range");
    }
    _offset = position;
    return Status::OK();
}

Status BrokerReadStream::tell(size_t* position) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *position = _offset;
    return Status::OK();
}

Status BrokerReadStream::available(size_t* n_bytes) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *n_bytes = _file_size - _offset;
    return Status::OK();
}

Status BrokerReadStream::close() {
    if (closed()) {
        return Status::OK();
    }
    TBrokerCloseReaderRequest request;

    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);

    const TNetworkAddress& addr = _addrs[_addr_idx];
    TBrokerOperationStatus response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(_env), addr, config::thrift_rpc_timeout_ms,
                                       &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker client failed. broker=" << addr
                         << ", status=" << status.get_error_msg();
            return status;
        }

        try {
            client->closeReader(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            status = client.reopen();
            if (!status.ok()) {
                LOG(WARNING) << "Close broker reader failed. broker=" << addr
                             << ", status=" << status.get_error_msg();
                return status;
            }
            client->closeReader(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Close broker reader failed, broker:" << addr << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Open broker reader failed, broker:" << addr << " failed:" << response.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    _closed = true;
    return Status::OK();
}

} // namespace doris
