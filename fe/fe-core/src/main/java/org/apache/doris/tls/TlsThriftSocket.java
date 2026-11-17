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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import javax.net.ssl.SSLSocket;

/** Keeps the pool's TSocket contract while every open, including retries, performs authenticated TLS. */
public final class TlsThriftSocket extends TSocket {
    private static final Logger LOG = LogManager.getLogger(TlsThriftSocket.class);
    private final String host;
    private final int port;
    private int connectTimeout;
    private int readTimeout;
    private SSLSocket tlsSocket;

    public TlsThriftSocket(String host, int port, int timeout) throws TTransportException {
        super(host, port, timeout);
        // TSocket.open() constructs plain TCP after close(). Keep its IO/accounting, and own only TLS sockets.
        super.close();
        this.host = host;
        this.port = port;
        connectTimeout = timeout;
        readTimeout = timeout;
    }

    @Override
    public boolean isOpen() {
        return tlsSocket != null && tlsSocket.isConnected() && !tlsSocket.isClosed();
    }

    @Override
    public void open() throws TTransportException {
        if (isOpen()) {
            throw new TTransportException(TTransportException.ALREADY_OPEN, "Private RPC TLS socket is already open");
        }
        tlsSocket = PrivateRpcTls.openThriftSocket(host, port, connectTimeout, readTimeout);
        try {
            inputStream_ = new BufferedInputStream(tlsSocket.getInputStream());
            outputStream_ = new BufferedOutputStream(tlsSocket.getOutputStream());
        } catch (IOException failure) {
            close();
            throw new TTransportException(TTransportException.NOT_OPEN, "Private RPC TLS stream initialization failed");
        }
    }

    @Override
    public void close() {
        SSLSocket previous = tlsSocket;
        tlsSocket = null;
        super.close();
        if (previous != null) {
            try {
                previous.close();
            } catch (IOException failure) {
                LOG.warn("Failed to close private RPC TLS socket");
            }
        }
    }

    @Override
    public void setConnectTimeout(int timeout) {
        // GenericPool's stale-connection diagnostic reads the superclass connect timeout as well.
        super.setConnectTimeout(timeout);
        connectTimeout = timeout;
    }

    @Override
    public void setSocketTimeout(int timeout) {
        readTimeout = timeout;
        // A locally closed stale socket is expected before GenericPool.reopen() replaces it.
        if (tlsSocket != null && !tlsSocket.isClosed()) {
            try {
                tlsSocket.setSoTimeout(timeout);
            } catch (SocketException failure) {
                close();
                throw new IllegalStateException("Private RPC TLS timeout update failed");
            }
        }
    }

    @Override
    public Socket getSocket() {
        return tlsSocket;
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        return tlsSocket == null ? null : tlsSocket.getRemoteSocketAddress();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        return tlsSocket == null ? null : tlsSocket.getLocalSocketAddress();
    }
}
