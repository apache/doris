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

#include "exec/broker_writer.h"

#include <sstream>

#include "common/logging.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/thrift_util.h"

namespace doris {

BrokerWriter::BrokerWriter(ExecEnv* env, const std::vector<TNetworkAddress>& broker_addresses,
                           const std::map<std::string, std::string>& properties,
                           const std::string& path, int64_t start_offset)
        : _env(env),
          _addresses(broker_addresses),
          _properties(properties),
          _path(path),
          _cur_offset(start_offset),
          _is_closed(false),
          _addr_idx(0) {}

BrokerWriter::~BrokerWriter() {
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

Status BrokerWriter::open() {
    TBrokerOpenWriterRequest request;

    const TNetworkAddress& broker_addr = _addresses[_addr_idx];
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_path(_path);
    request.__set_openMode(TBrokerOpenMode::APPEND);
    request.__set_clientId(client_id(_env, broker_addr));
    request.__set_properties(_properties);

    VLOG_ROW << "debug: send broker open writer request: "
             << apache::thrift::ThriftDebugString(request).c_str();

    TBrokerOpenWriterResponse response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(_env), broker_addr,
                                       config::thrift_rpc_timeout_ms, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker writer client failed. "
                         << "broker=" << broker_addr << ", status=" << status.get_error_msg();
            return status;
        }

        try {
            client->openWriter(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->openWriter(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Open broker writer failed, broker:" << broker_addr << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    VLOG_ROW << "debug: send broker open writer response: "
             << apache::thrift::ThriftDebugString(response).c_str();

    if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Open broker writer failed, broker:" << broker_addr
           << " failed:" << response.opStatus.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    _fd = response.fd;
    return Status::OK();
}

Status BrokerWriter::write(const uint8_t* buf, size_t buf_len, size_t* written_len) {
    if (buf_len == 0) {
        *written_len = 0;
        return Status::OK();
    }

    const TNetworkAddress& broker_addr = _addresses[_addr_idx];
    TBrokerPWriteRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);
    request.__set_offset(_cur_offset);
    request.__set_data(std::string(reinterpret_cast<const char*>(buf), buf_len));

    VLOG_ROW << "debug: send broker pwrite request: "
             << apache::thrift::ThriftDebugString(request).c_str();

    TBrokerOperationStatus response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(_env), broker_addr,
                                       config::thrift_rpc_timeout_ms, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker write client failed. "
                         << "broker=" << broker_addr << ", status=" << status.get_error_msg();
            return status;
        }

        // we do not re-try simply, because broker server may already write data
        try {
            client->pwrite(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());

            std::stringstream ss;
            ss << "Fail to write to broker, broker:" << broker_addr << " failed:" << e.what();
            LOG(WARNING) << ss.str();
            return Status::ThriftRpcError(ss.str());
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Fail to write to broker, broker:" << broker_addr << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }

    VLOG_ROW << "debug: send broker pwrite response: "
             << apache::thrift::ThriftDebugString(response).c_str();

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Fail to write to broker, broker:" << broker_addr << " msg:" << response.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    *written_len = buf_len;
    _cur_offset += buf_len;

    return Status::OK();
}

Status BrokerWriter::close() {
    if (_is_closed) {
        return Status::OK();
    }
    TBrokerCloseWriterRequest request;

    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);

    VLOG_ROW << "debug: send broker close writer request: "
             << apache::thrift::ThriftDebugString(request).c_str();

    const TNetworkAddress& broker_addr = _addresses[_addr_idx];
    TBrokerOperationStatus response;
    try {
        Status status;
        // use 20 second because close may take longer in remote storage, sometimes.
        // TODO(cmy): optimize this if necessary.
        BrokerServiceConnection client(client_cache(_env), broker_addr, 20000, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker write client failed. broker=" << broker_addr
                         << ", status=" << status.get_error_msg();
            return status;
        }

        try {
            client->closeWriter(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Close broker writer failed. broker=" << broker_addr
                         << ", status=" << status.get_error_msg();
            status = client.reopen();
            if (!status.ok()) {
                LOG(WARNING) << "Reopen broker writer failed. broker=" << broker_addr
                             << ", status=" << status.get_error_msg();
                return status;
            }
            client->closeWriter(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Close broker writer failed, broker:" << broker_addr << " msg:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    VLOG_ROW << "debug: send broker close writer response: "
             << apache::thrift::ThriftDebugString(response).c_str();

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Close broker writer failed, broker:" << broker_addr << " msg:" << response.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    _is_closed = true;
    return Status::OK();
}

} // end namespace doris
