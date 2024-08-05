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

#include "io/fs/broker_file_writer.h"

#include <gen_cpp/PaloBrokerService_types.h>
#include <gen_cpp/TPaloBrokerService.h>
#include <gen_cpp/Types_types.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TTransportException.h>

#include <sstream>

#include "common/config.h"
#include "common/logging.h"
#include "io/fs/file_writer.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"

namespace doris::io {

BrokerFileWriter::BrokerFileWriter(ExecEnv* env, const TNetworkAddress& broker_address, Path path,
                                   TBrokerFD fd)
        : _env(env), _address(broker_address), _path(std::move(path)), _fd(fd) {}

BrokerFileWriter::~BrokerFileWriter() = default;

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

Status BrokerFileWriter::close(bool non_block) {
    if (_state == State::CLOSED) {
        return Status::InternalError("BrokerFileWriter already closed, file path {}",
                                     _path.native());
    }
    if (_state == State::ASYNC_CLOSING) {
        if (non_block) {
            return Status::InternalError("Don't submit async close multi times");
        }
        // Actucally the first time call to close(true) would return the value of _finalize, if it returned one
        // error status then the code would never call the second close(true)
        _state = State::CLOSED;
        return Status::OK();
    }
    if (non_block) {
        _state = State::ASYNC_CLOSING;
    } else {
        _state = State::CLOSED;
    }
    return _close_impl();
}

Status BrokerFileWriter::_close_impl() {
    TBrokerCloseWriterRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);
    VLOG_ROW << "debug: send broker close writer request: "
             << apache::thrift::ThriftDebugString(request).c_str();

    TBrokerOperationStatus response;
    try {
        Status status;
        // use 20 second because close may take longer in remote storage, sometimes.
        // TODO(cmy): optimize this if necessary.
        BrokerServiceConnection client(client_cache(_env), _address, 20000, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker write client failed. broker=" << _address
                         << ", status=" << status;
            return status;
        }

        try {
            client->closeWriter(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Close broker writer failed. broker:" << _address
                         << " msg:" << e.what();
            status = client.reopen();
            if (!status.ok()) {
                LOG(WARNING) << "Reopen broker writer failed. broker=" << _address
                             << ", status=" << status;
                return status;
            }
            client->closeWriter(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Close broker writer failed, broker:" << _address << " msg:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    VLOG_ROW << "debug: send broker close writer response: "
             << apache::thrift::ThriftDebugString(response).c_str();

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Close broker writer failed, broker:" << _address << " msg:" << response.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status BrokerFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_state != State::OPENED) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }

    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        size_t left_bytes = result.size;
        const char* p = result.data;
        while (left_bytes > 0) {
            size_t written_bytes = 0;
            RETURN_IF_ERROR(_write((const uint8_t*)p, left_bytes, &written_bytes));
            left_bytes -= written_bytes;
            p += written_bytes;
        }
    }
    return Status::OK();
}

Result<FileWriterPtr> BrokerFileWriter::create(ExecEnv* env, const TNetworkAddress& broker_address,
                                               const std::map<std::string, std::string>& properties,
                                               Path path) {
    TBrokerOpenWriterRequest request;

    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_path(path);
    request.__set_openMode(TBrokerOpenMode::APPEND);
    request.__set_clientId(client_id(env, broker_address));
    request.__set_properties(properties);

    VLOG_ROW << "debug: send broker open writer request: "
             << apache::thrift::ThriftDebugString(request).c_str();

    TBrokerOpenWriterResponse response;
    try {
        Status status;
        BrokerServiceConnection client(client_cache(env), broker_address,
                                       config::thrift_rpc_timeout_ms, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker writer client failed. "
                         << "broker=" << broker_address << ", status=" << status;
            return ResultError(std::move(status));
        }

        try {
            client->openWriter(response, request);
        } catch (apache::thrift::transport::TTransportException&) {
            RETURN_IF_ERROR_RESULT(client.reopen());
            client->openWriter(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Open broker writer failed, broker:" << broker_address << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return ResultError(Status::RpcError(ss.str()));
    }

    VLOG_ROW << "debug: send broker open writer response: "
             << apache::thrift::ThriftDebugString(response).c_str();

    if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Open broker writer failed, broker:" << broker_address
           << " failed:" << response.opStatus.message;
        LOG(WARNING) << ss.str();
        return ResultError(Status::InternalError(ss.str()));
    }

    return std::make_unique<BrokerFileWriter>(env, broker_address, std::move(path), response.fd);
}

Status BrokerFileWriter::_write(const uint8_t* buf, size_t buf_len, size_t* written_bytes) {
    if (buf_len == 0) {
        *written_bytes = 0;
        return Status::OK();
    }

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
        BrokerServiceConnection client(client_cache(_env), _address, config::thrift_rpc_timeout_ms,
                                       &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker write client failed. "
                         << "broker=" << _address << ", status=" << status;
            return status;
        }

        try {
            client->pwrite(response, request);
        } catch (apache::thrift::transport::TTransportException&) {
            RETURN_IF_ERROR(client.reopen());
            // broker server will check write offset, so it is safe to re-try
            client->pwrite(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Fail to write to broker, broker:" << _address << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status::RpcError(ss.str());
    }

    VLOG_ROW << "debug: send broker pwrite response: "
             << apache::thrift::ThriftDebugString(response).c_str();

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Fail to write to broker, broker:" << _address << " msg:" << response.message;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    *written_bytes = buf_len;
    _cur_offset += buf_len;

    return Status::OK();
}

} // namespace doris::io
