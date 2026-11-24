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

package org.apache.doris.tls;

import org.apache.doris.common.Config;
import org.apache.doris.tls.server.TlsProtocolSet;
import org.apache.doris.tls.server.TlsProtocolSet.Protocol;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

/** Authenticated TLS for the private RPC providers; public protocol providers remain independent. */
public final class PrivateRpcTls {
    private PrivateRpcTls() {
    }

    public static boolean enabled(Protocol protocol) {
        return Config.enable_tls && TlsProtocolSet.isProtocolIncluded(protocol);
    }

    public static SslContext grpcClientContext() {
        validateConfiguration();
        try {
            return GrpcSslContexts.configure(SslContextBuilder.forClient(), SslProvider.JDK)
                    .trustManager(new File(Config.tls_ca_certificate_path))
                    .keyManager(new File(Config.tls_certificate_path), new File(Config.tls_private_key_path),
                            Config.tls_private_key_password.isEmpty() ? null : Config.tls_private_key_password)
                    .build();
        } catch (Exception failure) {
            // Provider exceptions can include PEM input, passwords or local key paths.
            throw new IllegalStateException("Private RPC TLS initialization failed");
        }
    }

    public static SSLServerSocket thriftServerSocket(InetSocketAddress address, int backlog) {
        validateConfiguration();
        thriftHandshakeTimeout(0);
        try {
            JdkSslContext context = (JdkSslContext) SslContextBuilder.forServer(
                    new File(Config.tls_certificate_path), new File(Config.tls_private_key_path),
                    Config.tls_private_key_password.isEmpty() ? null : Config.tls_private_key_password)
                    .sslProvider(SslProvider.JDK).trustManager(new File(Config.tls_ca_certificate_path))
                    .clientAuth(ClientAuth.REQUIRE).build();
            SSLServerSocket socket = (SSLServerSocket) context.context().getServerSocketFactory().createServerSocket();
            boolean bound = false;
            try {
                socket.setNeedClientAuth(true);
                socket.setEnabledProtocols(new String[] {"TLSv1.2", "TLSv1.3"});
                socket.bind(address, backlog);
                bound = true;
                return socket;
            } finally {
                if (!bound) {
                    socket.close();
                }
            }
        } catch (Exception failure) {
            throw new IllegalStateException("Private RPC TLS listener initialization failed");
        }
    }

    public static SSLSocket openThriftSocket(String host, int port, int connectTimeout, int readTimeout)
            throws TTransportException {
        return openThriftSocket(host, port, connectTimeout, readTimeout, host);
    }

    public static SSLSocket openThriftSocket(String host, int port, int connectTimeout, int readTimeout,
            String peerHost) throws TTransportException {
        Socket tcp = null;
        SSLSocket tls = null;
        boolean connected = false;
        try {
            validateConfiguration();
            JdkSslContext context = (JdkSslContext) SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                    .trustManager(new File(Config.tls_ca_certificate_path))
                    .keyManager(new File(Config.tls_certificate_path), new File(Config.tls_private_key_path),
                            Config.tls_private_key_password.isEmpty() ? null : Config.tls_private_key_password).build();
            tcp = new Socket();
            tcp.connect(new InetSocketAddress(host, port), thriftHandshakeTimeout(connectTimeout));
            tcp.setTcpNoDelay(true);
            // Connect to the DNS cache's current IP but authenticate the registered node name on every open.
            tls = (SSLSocket) context.context().getSocketFactory().createSocket(tcp, peerHost, port, true);
            // RPC reads can be unlimited; authentication must still expire for a stalled TCP peer.
            tls.setSoTimeout(thriftHandshakeTimeout(readTimeout));
            tls.setEnabledProtocols(new String[] {"TLSv1.2", "TLSv1.3"});
            SSLParameters parameters = tls.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            tls.setSSLParameters(parameters);
            tls.startHandshake();
            tls.setSoTimeout(readTimeout);
            connected = true;
            return tls;
        } catch (SocketTimeoutException failure) {
            throw new TTransportException(TTransportException.TIMED_OUT, "Private RPC TLS connection timed out");
        } catch (Exception failure) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Private RPC TLS connection failed");
        } finally {
            if (!connected) {
                closeFailedSocket(tls == null ? tcp : tls);
            }
        }
    }

    static int thriftHandshakeTimeout(int requestedTimeout) {
        // Snapshot the mutable setting so an update cannot turn a validated timeout into unlimited IO.
        int configuredTimeout = Config.thrift_rpc_connect_timeout_ms;
        if (configuredTimeout <= 0) {
            throw new IllegalStateException("Private RPC TLS requires a positive Thrift connect timeout");
        }
        return requestedTimeout > 0 ? Math.min(requestedTimeout, configuredTimeout) : configuredTimeout;
    }

    private static void closeFailedSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // This connection has already failed and is never returned to the pool.
            }
        }
    }

    private static void validateConfiguration() {
        // Optional peer certificates or verification bypasses cannot protect file-key RPC payloads.
        if (!"verify_fail_if_no_peer_cert".equals(Config.tls_verify_mode)
                || Config.tls_certificate_path.isEmpty() || Config.tls_private_key_path.isEmpty()
                || Config.tls_ca_certificate_path.isEmpty()) {
            throw new IllegalStateException("Private RPC TLS requires a certificate, private key, CA and mutual auth");
        }
        // This provider uses static SSL contexts; operators must restart nodes for certificate changes.
        if (Config.tls_cert_refresh_interval_seconds != 0 || !Config.tls_peer_cert_required_san_dns.isEmpty()) {
            throw new IllegalStateException("Private RPC TLS requires static certificates and CA-scoped peer trust");
        }
    }
}
