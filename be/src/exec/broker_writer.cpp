// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#include "runtime/runtime_state.h"
#include "util/thrift_util.h"

namespace palo {

BrokerWriter::BrokerWriter(
        RuntimeState* state,
        const std::vector<TNetworkAddress>& broker_addresses,
        const std::map<std::string, std::string>& properties,
        const std::string& path,
        int64_t start_offset) :
            _state(state),
            _addresses(broker_addresses),
            _properties(properties),
            _path(path),
            _cur_offset(start_offset),
            _is_closed(false),
            _addr_idx(0) {
}

BrokerWriter::~BrokerWriter() {
    close();
}

#ifdef BE_TEST
inline BrokerServiceClientCache* client_cache(RuntimeState* state) {
    static BrokerServiceClientCache s_client_cache;
    return &s_client_cache;
}

inline const std::string& client_id(RuntimeState* state, const TNetworkAddress& addr) {
    static std::string s_client_id = "palo_unit_test";
    return s_client_id;
}
#else
inline BrokerServiceClientCache* client_cache(RuntimeState* state) {
    return state->exec_env()->broker_client_cache();
}

inline const std::string& client_id(RuntimeState* state, const TNetworkAddress& addr) {
    return state->exec_env()->broker_mgr()->get_client_id(addr);
}
#endif

Status BrokerWriter::open() {
    TBrokerOpenWriterRequest request;

    const TNetworkAddress& broker_addr = _addresses[_addr_idx];
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_path(_path);
    request.__set_openMode(TBrokerOpenMode::APPEND);
    request.__set_clientId(client_id(_state, broker_addr));
    request.__set_properties(_properties);

    VLOG_ROW << "debug: send broker open writer request: "
            << apache::thrift::ThriftDebugString(request).c_str();

    TBrokerOpenWriterResponse response;
    try {
        Status status;
        // 500ms is enough
        BrokerServiceConnection client(client_cache(_state), broker_addr, 500, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker writer client failed. "
                << "broker=" << broker_addr
                << ", status=" << status.get_error_msg();
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
        return Status(TStatusCode::THRIFT_RPC_ERROR, ss.str(), false);
    }

    VLOG_ROW << "debug: send broker open writer response: "
            << apache::thrift::ThriftDebugString(response).c_str();

    if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Open broker writer failed, broker:" << broker_addr
            << " failed:" << response.opStatus.message;
        LOG(WARNING) << ss.str();
        return Status(ss.str());
    }

    _fd = response.fd;
    return Status::OK;
}

Status BrokerWriter::write(const uint8_t* buf, size_t buf_len, size_t* written_len) {
    if (buf_len == 0) {
        *written_len = 0;
        return Status::OK;
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
        // we make timeout to be 5s, to avoid error in Network jitter scenarios.
        BrokerServiceConnection client(client_cache(_state), broker_addr, 5000, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker write client failed. "
                    << "broker=" << broker_addr
                    << ", status=" << status.get_error_msg();
            return status;
        }

        // we do not re-try simplely, because broker server may already write data
        try {
            client->pwrite(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());

            std::stringstream ss;
            ss << "Fail to write to broker, broker:" << broker_addr << " failed:" << e.what();
            LOG(WARNING) << ss.str();
            return Status(TStatusCode::THRIFT_RPC_ERROR, ss.str());
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Fail to write to broker, broker:" << broker_addr << " failed:" << e.what();
        LOG(WARNING) << ss.str();
        return Status(TStatusCode::THRIFT_RPC_ERROR, ss.str());
    }

    VLOG_ROW << "debug: send broker pwrite response: "
            << apache::thrift::ThriftDebugString(response).c_str();

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Fail to write to broker, broker:" << broker_addr
            << " msg:" << response.message;
        LOG(WARNING) << ss.str();
        return Status(ss.str());
    }

    *written_len = buf_len;
    _cur_offset += buf_len;

    return Status::OK;
}

void BrokerWriter::close() {
    if (_is_closed) {
        return;
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
        // 500ms is enough
        BrokerServiceConnection client(client_cache(_state), broker_addr, 500, &status);
        if (!status.ok()) {
            LOG(WARNING) << "Create broker write client failed. broker=" << broker_addr
                << ", status=" << status.get_error_msg();
            return;
        }

        try {
            client->closeWriter(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            status = client.reopen();
            if (!status.ok()) {
                LOG(WARNING) << "Close broker writer failed. broker=" << broker_addr
                    << ", status=" << status.get_error_msg();
                return;
            }
            client->closeWriter(response, request);
        }
    } catch (apache::thrift::TException& e) {
        LOG(WARNING) << "Close broker writer failed, broker:" << broker_addr
            << " msg:" << e.what();
        return;
    }

    VLOG_ROW << "debug: send broker close writer response: "
            << apache::thrift::ThriftDebugString(response).c_str();

    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        LOG(WARNING) << "Close broker writer failed, broker:" << broker_addr
            << " msg:" << response.message;
        return;
    }

    _is_closed = true;
}

} // end namespace palo
