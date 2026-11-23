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

#include "util/private_rpc_tls.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/ssl_options.h>
#include <thrift/transport/TSSLServerSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TTransportException.h>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <system_error>
#include <utility>

#include "common/config.h"
#include "util/string_util.h"
#include "util/thrift_server.h"

namespace doris::rpc_tls {
namespace {

Status validate_configuration() {
    // Optional certificates cannot authenticate peers carrying private file-key RPC payloads.
    if (config::tls_verify_mode != "verify_fail_if_no_peer_cert" ||
        config::tls_certificate_path.empty() || config::tls_private_key_path.empty() ||
        config::tls_ca_certificate_path.empty()) {
        return Status::InvalidArgument(
                "Private RPC TLS requires a certificate, private key, CA and mutual auth");
    }
    if (config::tls_cert_refresh_interval_seconds != 0 ||
        !config::tls_peer_cert_required_san_dns.empty() ||
        !config::tls_private_key_password.empty()) {
        return Status::NotSupported(
                "Private RPC TLS requires static PEM identities and CA-scoped peer trust");
    }
    // BRPC accepts either paths or inline PEM. Only operator-owned local files may reach that API,
    // so malformed key input cannot be treated as an inline certificate and echoed by its parser.
    for (const auto* path : {&config::tls_certificate_path, &config::tls_private_key_path,
                             &config::tls_ca_certificate_path}) {
        std::error_code error;
        if (!std::filesystem::is_regular_file(*path, error)) {
            return Status::InvalidArgument(
                    "Private RPC TLS requires readable local identity files");
        }
    }
    return Status::OK();
}

class HandshakeSocket final : public apache::thrift::transport::TSSLSocket {
public:
    HandshakeSocket(std::shared_ptr<apache::thrift::transport::SSLContext> context,
                    const std::string& host, int port, int timeout)
            : TSSLSocket(std::move(context), host, port),
              _connect_host(host),
              _handshake_timeout(timeout) {}

    HandshakeSocket(std::shared_ptr<apache::thrift::transport::SSLContext> context,
                    THRIFT_SOCKET socket, std::shared_ptr<THRIFT_SOCKET> interrupt, int timeout)
            : TSSLSocket(std::move(context), socket, std::move(interrupt)),
              _handshake_timeout(timeout) {}

    void open() override {
        const int connect_timeout = connTimeout_;
        const std::string peer_hostname = getHost();
        setConnTimeout(bounded_timeout(connect_timeout));
        try {
            // Cache connections use a resolved IP, while SNI and certificate checks must retain
            // the registered hostname. Restore that identity before starting the lazy handshake.
            setHost(_connect_host);
            TSSLSocket::open();
            setHost(peer_hostname);
            setConnTimeout(connect_timeout);
            authenticate_peer();
        } catch (...) {
            setHost(peer_hostname);
            setConnTimeout(connect_timeout);
            TSSLSocket::close();
            throw;
        }
    }

    bool peek() override {
        // Pool probes after close() must report a closed transport without starting another handshake.
        if (!TSocket::isOpen()) {
            return false;
        }
        authenticate_peer();
        return TSSLSocket::peek();
    }

    uint32_t read(uint8_t* buffer, uint32_t size) override {
        authenticate_peer();
        return TSSLSocket::read(buffer, size);
    }

    void write(const uint8_t* buffer, uint32_t size) override {
        authenticate_peer();
        TSSLSocket::write(buffer, size);
    }

    uint32_t write_partial(const uint8_t* buffer, uint32_t size) override {
        authenticate_peer();
        return TSSLSocket::write_partial(buffer, size);
    }

    void flush() override {
        // Thrift closes input and output transports separately, so the second flush must stay harmless.
        if (!TSocket::isOpen()) {
            TSSLSocket::flush();
            return;
        }
        authenticate_peer();
        TSSLSocket::flush();
    }

private:
    int bounded_timeout(int timeout) const {
        return timeout > 0 ? std::min(timeout, _handshake_timeout) : _handshake_timeout;
    }

    void authenticate_peer() {
        if (checkHandshake()) {
            return;
        }
        // Unlimited RPC reads must not let a TCP-only peer occupy a worker during TLS authentication.
        // Restore the RPC timeouts before reading the first application byte after authentication.
        const int read_timeout = recvTimeout_;
        const int write_timeout = sendTimeout_;
        setRecvTimeout(bounded_timeout(read_timeout));
        setSendTimeout(bounded_timeout(write_timeout));
        try {
            initializeHandshake();
        } catch (...) {
            TSSLSocket::close();
            setRecvTimeout(read_timeout);
            setSendTimeout(write_timeout);
            throw;
        }
        setRecvTimeout(read_timeout);
        setSendTimeout(write_timeout);
    }

    const std::string _connect_host;
    const int _handshake_timeout;
};

class HandshakeSocketFactory final : public apache::thrift::transport::TSSLSocketFactory {
public:
    explicit HandshakeSocketFactory(int timeout)
            : TSSLSocketFactory(apache::thrift::transport::TLSv1_2), _handshake_timeout(timeout) {}

    using TSSLSocketFactory::createSocket;

    std::shared_ptr<apache::thrift::transport::TSSLSocket> createSocket(const std::string& host,
                                                                        int port) override {
        auto socket = std::make_shared<HandshakeSocket>(ctx_, host, port, _handshake_timeout);
        socket->server(false);
        socket->access(std::make_shared<apache::thrift::transport::DefaultClientAccessManager>());
        return socket;
    }

    std::shared_ptr<apache::thrift::transport::TSSLSocket> createSocket(THRIFT_SOCKET fd) override {
        return createSocket(fd, nullptr);
    }

    std::shared_ptr<apache::thrift::transport::TSSLSocket> createSocket(
            THRIFT_SOCKET fd, std::shared_ptr<THRIFT_SOCKET> interrupt) override {
        auto socket = std::make_shared<HandshakeSocket>(ctx_, fd, std::move(interrupt),
                                                        _handshake_timeout);
        socket->server(true);
        return socket;
    }

private:
    const int _handshake_timeout;
};

std::shared_ptr<apache::thrift::transport::TSSLSocketFactory> socket_factory(bool server) {
    // Snapshot the mutable setting so validation and millisecond conversion use the same value.
    // Zero leaves authentication unbounded, and large seconds can overflow socket milliseconds.
    const int timeout_seconds = config::thrift_connect_timeout_seconds;
    if (timeout_seconds <= 0 || timeout_seconds > std::numeric_limits<int>::max() / 1000) {
        throw apache::thrift::transport::TTransportException(
                apache::thrift::transport::TTransportException::BAD_ARGS,
                "Private RPC TLS requires a positive bounded Thrift connect timeout");
    }
    auto factory = std::make_shared<HandshakeSocketFactory>(timeout_seconds * 1000);
    factory->server(server);
    factory->authenticate(true);
    // Never let an encrypted PEM trigger an interactive password prompt on a server thread.
    factory->overrideDefaultPasswordCallback();
    factory->loadCertificate(config::tls_certificate_path.c_str());
    factory->loadPrivateKey(config::tls_private_key_path.c_str());
    factory->loadTrustedCertificates(config::tls_ca_certificate_path.c_str());
    return factory;
}

class RetryingSslServerSocket final : public apache::thrift::transport::TSSLServerSocket {
public:
    using TSSLServerSocket::TSSLServerSocket;

protected:
    std::shared_ptr<apache::thrift::transport::TTransport> acceptImpl() override {
        // Stack-collection signals must not stop a TLS listener any more than a plaintext listener.
        while (true) {
            try {
                return TSSLServerSocket::acceptImpl();
            } catch (const apache::thrift::transport::TTransportException& error) {
                if (!is_unknown_eintr(error)) {
                    throw;
                }
            }
        }
    }
};

struct ClientSocketLifetime {
    // Destruction order keeps the factory (and its OpenSSL initialization) alive through the socket.
    std::shared_ptr<apache::thrift::transport::TSSLSocketFactory> factory;
    std::shared_ptr<apache::thrift::transport::TSSLSocket> socket;
};

} // namespace

bool enabled(std::string_view protocol) {
    if (!config::enable_tls) {
        return false;
    }
    const auto excluded = split(to_lower(config::tls_excluded_protocols), ",");
    return std::ranges::none_of(excluded,
                                [&](const auto& value) { return trim(value) == protocol; });
}

bool enabled_for_java_thrift() {
    // Only the built-in provider can project this PEM policy into an isolated Java plugin.
#ifdef DORIS_OSS_PRIVATE_RPC_PROVIDERS
    return enabled("thrift");
#else
    return false;
#endif
}

Status configure_channel(brpc::ChannelOptions* options) {
    if (!enabled("brpc")) {
        return Status::OK();
    }
    RETURN_IF_ERROR(validate_configuration());
    auto* ssl = options->mutable_ssl_options();
    ssl->protocols = "TLSv1.2";
    ssl->verify.verify_depth = 8;
    ssl->verify.ca_file_path = config::tls_ca_certificate_path;
    ssl->client_cert.certificate = config::tls_certificate_path;
    ssl->client_cert.private_key = config::tls_private_key_path;
    return Status::OK();
}

Status configure_server(brpc::ServerOptions* options) {
    RETURN_IF_ERROR(validate_configuration());
    auto* ssl = options->mutable_ssl_options();
    ssl->default_cert.certificate = config::tls_certificate_path;
    ssl->default_cert.private_key = config::tls_private_key_path;
    ssl->verify.verify_depth = 8;
    ssl->verify.ca_file_path = config::tls_ca_certificate_path;
    // TLS 1.0/1.1 have no AES-GCM suites; the pinned BRPC server has no protocol-floor option.
    ssl->ciphers = "ECDHE+AESGCM:DHE+AESGCM";
    ssl->alpns = "h2,baidu_std,http";
    options->force_ssl = true;
    return Status::OK();
}

Status configure_thrift_server(ThriftServer* server, const std::string& address, int port) {
    if (!enabled("thrift")) {
        return Status::OK();
    }
    RETURN_IF_ERROR(validate_configuration());
    try {
        server->set_server_socket(
                std::make_shared<RetryingSslServerSocket>(address, port, socket_factory(true)));
        return Status::OK();
    } catch (const std::exception&) {
        return Status::InternalError("Private RPC TLS initialization failed");
    }
}

std::shared_ptr<apache::thrift::transport::TSocket> create_client_socket(const std::string& host,
                                                                         int port) {
    using apache::thrift::transport::TSocket;
    using apache::thrift::transport::TTransportException;
    auto status = validate_configuration();
    if (!status.ok()) {
        throw TTransportException(TTransportException::INTERNAL_ERROR, status.to_string());
    }
    try {
        auto lifetime = std::make_shared<ClientSocketLifetime>();
        lifetime->factory = socket_factory(false);
        lifetime->socket = lifetime->factory->createSocket(host, port);
        return {lifetime, lifetime->socket.get()};
    } catch (const std::exception&) {
        // OpenSSL/parser diagnostics can contain input paths or PEM material. Preserve only a classification.
        throw TTransportException(TTransportException::INTERNAL_ERROR,
                                  "Private RPC TLS initialization failed");
    }
}

void set_peer_hostname(apache::thrift::transport::TSocket* socket, const std::string& hostname) {
    // Plain transports and independently supplied TLS implementations retain their existing policy.
    if (auto* tls = dynamic_cast<HandshakeSocket*>(socket)) {
        tls->setHost(hostname);
    }
}

} // namespace doris::rpc_tls
