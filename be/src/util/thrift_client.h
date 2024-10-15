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

#pragma once

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include <memory>
#include <sstream>
#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "util/thrift_server.h"

namespace apache {
namespace thrift {
namespace transport {
class TTransport;
} // namespace transport
} // namespace thrift
} // namespace apache

namespace doris {

#define THRIFT_MOVE_VALUES(thrift, member, value) \
    thrift.__isset.member = true;                 \
    thrift.member = std::move(value);

// Super class for templatized thrift clients.
class ThriftClientImpl {
public:
    virtual ~ThriftClientImpl() { close(); }
    const std::string& ipaddress() { return _ipaddress; }
    int port() const { return _port; }

    // Open the connection to the remote server. May be called
    // repeatedly, is idempotent unless there is a failure to connect.
    Status open();

    // Retry the Open num_retries time waiting wait_ms milliseconds between retries.
    Status open_with_retry(int num_retries, int wait_ms);

    // close the connection with the remote server. May be called
    // repeatedly.
    void close();

    // Set the connect timeout
    void set_conn_timeout(int ms) { _socket->setConnTimeout(ms); }

    // Set the receive timeout
    void set_recv_timeout(int ms) { _socket->setRecvTimeout(ms); }

    // Set the send timeout
    void set_send_timeout(int ms) { _socket->setSendTimeout(ms); }

protected:
    ThriftClientImpl(const std::string& ipaddress, int port)
            : _ipaddress(ipaddress),
              _port(port),
              _socket(new apache::thrift::transport::TSocket(ipaddress, port)) {}

    std::string _ipaddress;
    int _port;

    // All shared pointers, because Thrift requires them to be
    std::shared_ptr<apache::thrift::transport::TSocket> _socket;
    std::shared_ptr<apache::thrift::transport::TTransport> _transport;
    std::shared_ptr<apache::thrift::protocol::TBinaryProtocol> _protocol;
};

// Utility client to a Thrift server. The parameter type is the
// Thrift interface type that the server implements.
template <class InterfaceType>
class ThriftClient : public ThriftClientImpl {
public:
    ThriftClient(const std::string& ipaddress, int port);

    ThriftClient(const std::string& ipaddress, int port, ThriftServer::ServerType server_type);

    // Returns the object used to actually make RPCs against the remote server
    InterfaceType* iface() { return _iface.get(); }

private:
    std::shared_ptr<InterfaceType> _iface;
};

template <class InterfaceType>
ThriftClient<InterfaceType>::ThriftClient(const std::string& ipaddress, int port)
        : ThriftClientImpl(ipaddress, port) {
    _transport.reset(new apache::thrift::transport::TBufferedTransport(_socket));
    _protocol.reset(new apache::thrift::protocol::TBinaryProtocol(_transport));
    _iface.reset(new InterfaceType(_protocol));
}

template <class InterfaceType>
ThriftClient<InterfaceType>::ThriftClient(const std::string& ipaddress, int port,
                                          ThriftServer::ServerType server_type)
        : ThriftClientImpl(ipaddress, port) {
    switch (server_type) {
    case ThriftServer::NON_BLOCKING:
        _transport.reset(new apache::thrift::transport::TFramedTransport(_socket));
        break;
    case ThriftServer::THREADED:
    case ThriftServer::THREAD_POOL:
        _transport.reset(new apache::thrift::transport::TBufferedTransport(_socket));
        break;
    default:
        std::stringstream error_msg;
        error_msg << "Unsupported server type: " << server_type;
        LOG(ERROR) << error_msg.str();
        DCHECK(false);
        break;
    }

    _protocol.reset(new apache::thrift::protocol::TBinaryProtocol(_transport));
    _iface.reset(new InterfaceType(_protocol));
}

} // namespace doris
