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

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import javax.net.ssl.SSLSocket;

/** Bounds authentication on a request worker without changing authenticated RPC read timeouts. */
public final class TlsAcceptedThriftSocket extends TSocket {
    private final SSLSocket socket;
    private final int handshakeTimeout;
    private boolean authenticated;

    public TlsAcceptedThriftSocket(SSLSocket socket) throws TTransportException {
        super(socket);
        this.socket = socket;
        try {
            handshakeTimeout = PrivateRpcTls.thriftHandshakeTimeout(0);
        } catch (RuntimeException failure) {
            close();
            throw new TTransportException(TTransportException.NOT_OPEN, "Private RPC TLS timeout configuration failed");
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws TTransportException {
        authenticatePeer();
        return super.read(buffer, offset, length);
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws TTransportException {
        authenticatePeer();
        super.write(buffer, offset, length);
    }

    private void authenticatePeer() throws TTransportException {
        if (authenticated) {
            return;
        }
        try {
            int readTimeout = socket.getSoTimeout();
            // Zero is a valid RPC idle timeout, but cannot leave an unauthenticated peer waiting forever.
            socket.setSoTimeout(readTimeout > 0 ? Math.min(readTimeout, handshakeTimeout) : handshakeTimeout);
            socket.startHandshake();
            socket.setSoTimeout(readTimeout);
            authenticated = true;
        } catch (SocketTimeoutException failure) {
            close();
            throw new TTransportException(TTransportException.TIMED_OUT, "Private RPC TLS authentication timed out");
        } catch (IOException failure) {
            close();
            throw new TTransportException(TTransportException.NOT_OPEN, "Private RPC TLS authentication failed");
        }
    }
}
