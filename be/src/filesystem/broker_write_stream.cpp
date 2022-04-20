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

#include "filesystem/broker_write_stream.h"

#include "gen_cpp/TPaloBrokerService.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/thrift_util.h"

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

BrokerWriteStream::BrokerWriteStream(ExecEnv* env, const std::vector<TNetworkAddress>& addrs,
                                     TBrokerFD fd)
        : _env(env), _addrs(addrs), _fd(fd) {}

BrokerWriteStream::~BrokerWriteStream() {
    close();
}

Status BrokerWriteStream::write(const char* from, size_t put_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (put_n == 0) {
        return Status::OK();
    }

    const TNetworkAddress& addr = _addrs[_addr_idx];
    TBrokerPWriteRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);
    request.__set_offset(_offset);
    request.__set_data(std::string(from, put_n));

    VLOG_ROW << "debug: send broker pwrite request: "
             << apache::thrift::ThriftDebugString(request).c_str();

    TBrokerOperationStatus response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(_env), addr, config::thrift_rpc_timeout_ms,
                                       &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker write client failed. "
                         << "broker=" << addr << ", status=" << status.get_error_msg();
            return status;
        }

        // we do not re-try simply, because broker server may already write data
        try {
            client->pwrite(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());

            std::stringstream ss;
            ss << "Fail to write to broker, broker:" << addr << " failed:" << e.what();
            LOG(WARNING) << ss.str();
            return Status::ThriftRpcError(ss.str());
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Fail to write to broker, broker:" << addr << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    VLOG_ROW << "debug: send broker pwrite response: "
             << apache::thrift::ThriftDebugString(response).c_str();

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Fail to write to broker, broker:" << addr << " msg:" << response.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    _offset += put_n;

    return Status::OK();
}

Status BrokerWriteStream::sync() {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    return Status::OK();
}

Status BrokerWriteStream::close() {
    if (closed()) {
        return Status::OK();
    }
    TBrokerCloseWriterRequest request;

    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);

    VLOG_ROW << "debug: send broker close writer request: "
             << apache::thrift::ThriftDebugString(request).c_str();

    const TNetworkAddress& addr = _addrs[_addr_idx];
    TBrokerOperationStatus response;
    try {
        Status status;
        // use 20 second because close may take longer in remote storage, sometimes.
        // TODO(cmy): optimize this if necessary.
        BrokerServiceConnection client(client_cache(_env), addr, 20000, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker write client failed. broker=" << addr
                         << ", status=" << status.get_error_msg();
            return status;
        }

        try {
            client->closeWriter(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Close broker writer failed. broker=" << addr
                         << ", status=" << status.get_error_msg();
            status = client.reopen();
            if (!status.ok()) {
                LOG(WARNING) << "Reopen broker writer failed. broker=" << addr
                             << ", status=" << status.get_error_msg();
                return status;
            }
            client->closeWriter(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Close broker writer failed, broker:" << addr << " msg:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    VLOG_ROW << "debug: send broker close writer response: "
             << apache::thrift::ThriftDebugString(response).c_str();

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Close broker writer failed, broker:" << addr << " msg:" << response.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    _closed = true;
    return Status::OK();
}

} // namespace doris
