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

package org.apache.doris.filesystem.broker;

import org.apache.doris.thrift.TBrokerCloseWriterRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPWriteRequest;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * {@link OutputStream} backed by a Broker {@code pwrite} RPC.
 *
 * <p>Holds an open Thrift client (borrowed from the pool on construction) for
 * the lifetime of this stream; the client is returned to the pool on {@link #close()}.
 */
class BrokerOutputStream extends OutputStream {

    private static final Logger LOG = LogManager.getLogger(BrokerOutputStream.class);

    private final TNetworkAddress endpoint;
    private final BrokerClientPool clientPool;
    private final TPaloBrokerService.Client client;
    private final TBrokerFD fd;

    private long writeOffset;
    private boolean closed;

    BrokerOutputStream(TNetworkAddress endpoint, BrokerClientPool clientPool,
            TPaloBrokerService.Client client, TBrokerFD fd) {
        this.endpoint = endpoint;
        this.clientPool = clientPool;
        this.client = client;
        this.fd = fd;
        this.writeOffset = 0;
        this.closed = false;
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is already closed");
        }
        if (len == 0) {
            return;
        }
        byte[] data = new byte[len];
        System.arraycopy(bytes, off, data, 0, len);
        try {
            TBrokerPWriteRequest req = new TBrokerPWriteRequest(
                    TBrokerVersion.VERSION_ONE, fd, writeOffset, ByteBuffer.wrap(data));
            TBrokerOperationStatus opst = client.pwrite(req);
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new IOException("Broker pwrite failed at offset " + writeOffset + ": " + opst.getMessage());
            }
            writeOffset += len;
        } catch (TException e) {
            clientPool.invalidate(endpoint, client);
            closed = true;
            throw new IOException("Broker pwrite RPC failed at offset " + writeOffset + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            TBrokerCloseWriterRequest req = new TBrokerCloseWriterRequest(TBrokerVersion.VERSION_ONE, fd);
            TBrokerOperationStatus opst = client.closeWriter(req);
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                // Broker-side state after a failed close is unknown; the client may have buffered
                // bytes that were never flushed. Drop the client from the pool and surface the
                // failure so callers do not believe the write was durable.
                LOG.warn("Failed to close broker writer for fd {}: {}", fd, opst.getMessage());
                clientPool.invalidate(endpoint, client);
                throw new IOException("Failed to close broker writer for fd " + fd + ": " + opst.getMessage());
            }
            clientPool.returnGood(endpoint, client);
        } catch (TException e) {
            clientPool.invalidate(endpoint, client);
            throw new IOException("Broker closeWriter RPC failed: " + e.getMessage(), e);
        }
    }
}
