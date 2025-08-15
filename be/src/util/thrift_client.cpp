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

#include "util/thrift_client.h"

#include <thrift/transport/TTransport.h>
#include <thrift/transport/TTransportException.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <string>
#include <thread>

#include "common/config.h"
#include "gutil/strings/substitute.h"

namespace doris {

ThriftClientImpl::ThriftClientImpl(const std::string& ipaddress, int port)
        : _ipaddress(ipaddress), _port(port) {
    if (config::enable_tls) {
        apache::thrift::transport::initializeOpenSSL();
        auto ssl_factory = std::make_unique<apache::thrift::transport::TSSLSocketFactory>(
                apache::thrift::transport::SSLProtocol::TLSv1_2);
        ssl_factory->ciphers("ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
        ssl_factory->loadCertificate(config::tls_certificate_path.c_str());
        ssl_factory->loadPrivateKey(config::tls_private_key_path.c_str());
        ssl_factory->loadTrustedCertificates(config::tls_ca_certificate_path.c_str());
        if (config::tls_verify_mode == "verify_fail_if_no_peer_cert") {
            ssl_factory->authenticate(true);
        } else if (config::tls_verify_mode == "verify_peer") {
            ssl_factory->authenticate(true);
        } else if (config::tls_verify_mode == "verify_none") {
            // nothing
        } else {
            throw Status::RuntimeError(
                    "unknown verify_mode: {}, only support: verify_fail_if_no_peer_cert, "
                    "verify_peer, verify_none",
                    config::tls_verify_mode);
        }
        ssl_factory->authenticate(true);
        ssl_factory->server(false);
        _socket = ssl_factory->createSocket(ipaddress, port);
    } else {
        _socket = std::make_shared<apache::thrift::transport::TSocket>(ipaddress, port);
    }
}

Status ThriftClientImpl::open() {
    try {
        if (!_transport->isOpen()) {
            _transport->open();
        }
    } catch (const apache::thrift::transport::TTransportException& e) {
        try {
            _transport->close();
        } catch (const apache::thrift::transport::TTransportException& e) {
            VLOG_CRITICAL << "Error closing socket to: " << ipaddress() << ":" << port()
                          << ", ignoring (" << e.what() << ")";
        }
        // In certain cases in which the remote host is overloaded, this failure can
        // happen quite frequently. Let's print this error message without the stack
        // trace as there aren't many callers of this function.
        const std::string& err_msg = strings::Substitute("Couldn't open transport for $0:$1 ($2)",
                                                         ipaddress(), port(), e.what());
        VLOG_CRITICAL << err_msg;
        return Status::RpcError(err_msg);
    }
    return Status::OK();
}

Status ThriftClientImpl::open_with_retry(int num_tries, int wait_ms) {
    DCHECK_GE(wait_ms, 0);
    Status status;
    int try_count = 0L;

    while (num_tries <= 0 || try_count < num_tries) {
        ++try_count;
        status = open();

        if (status.ok()) {
            return status;
        }

        LOG(INFO) << "Unable to connect to " << _ipaddress << ":" << _port;

        if (num_tries < 0) {
            LOG(INFO) << "(Attempt " << try_count << ", will retry indefinitely)";
        } else {
            LOG(INFO) << "(Attempt " << try_count << " of " << num_tries << ")";
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
    }

    return status;
}

void ThriftClientImpl::close() {
    try {
        if (_transport != nullptr && _transport->isOpen()) {
            _transport->close();
        }
    } catch (const apache::thrift::transport::TTransportException& e) {
        LOG(INFO) << "Error closing connection to: " << ipaddress() << ":" << port()
                  << ", ignoring (" << e.what() << ")";
        // Forcibly close the socket (since the transport may have failed to get that far
        // during close())
        try {
            if (_socket != nullptr) {
                _socket->close();
            }
        } catch (const apache::thrift::transport::TTransportException& e) {
            LOG(INFO) << "Error closing socket to: " << ipaddress() << ":" << port()
                      << ", ignoring (" << e.what() << ")";
        }
    }
}

} // namespace doris
