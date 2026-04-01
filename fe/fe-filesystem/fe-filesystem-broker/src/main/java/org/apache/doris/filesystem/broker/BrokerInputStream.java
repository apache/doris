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

import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPReadRequest;
import org.apache.doris.thrift.TBrokerReadResponse;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;

/**
 * {@link DorisInputStream} backed by a Broker {@code pread} RPC.
 *
 * <p>Holds an open Thrift client (borrowed from the pool on construction) for
 * the lifetime of this stream; the client is returned to the pool on {@link #close()}.
 */
class BrokerInputStream extends DorisInputStream {

    private static final Logger LOG = LogManager.getLogger(BrokerInputStream.class);
    private static final int READ_BUFFER = 1 << 20; // 1 MB

    private final TNetworkAddress endpoint;
    private final BrokerClientPool clientPool;
    private final TPaloBrokerService.Client client;
    private final TBrokerFD fd;

    private long position;
    /** Internal byte buffer for buffered reads. */
    private byte[] buffer = new byte[0];
    private int bufOffset;
    private int bufLen;
    private boolean eof;
    /** True when the Thrift client has already been invalidated (e.g. on RPC failure). */
    private boolean clientInvalidated = false;

    BrokerInputStream(TNetworkAddress endpoint, BrokerClientPool clientPool,
            TPaloBrokerService.Client client, TBrokerFD fd) {
        this.endpoint = endpoint;
        this.clientPool = clientPool;
        this.client = client;
        this.fd = fd;
        this.position = 0;
        this.eof = false;
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new IOException("Seek position must be >= 0, got: " + pos);
        }
        // Flush the internal buffer; next read will pread from the new position.
        position = pos;
        bufOffset = 0;
        bufLen = 0;
        eof = false;
    }

    @Override
    public int read() throws IOException {
        if (eof) {
            return -1;
        }
        if (bufLen == 0) {
            fillBuffer();
            if (eof) {
                return -1;
            }
        }
        int b = buffer[bufOffset] & 0xFF;
        bufOffset++;
        bufLen--;
        position++;
        return b;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if (eof) {
            return -1;
        }
        if (bufLen == 0) {
            fillBuffer();
            if (eof) {
                return -1;
            }
        }
        int toCopy = Math.min(len, bufLen);
        System.arraycopy(buffer, bufOffset, bytes, off, toCopy);
        bufOffset += toCopy;
        bufLen -= toCopy;
        position += toCopy;
        return toCopy;
    }

    private void fillBuffer() throws IOException {
        try {
            TBrokerPReadRequest req = new TBrokerPReadRequest(
                    TBrokerVersion.VERSION_ONE, fd, position, READ_BUFFER);
            TBrokerReadResponse rep = client.pread(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() == TBrokerOperationStatusCode.END_OF_FILE) {
                eof = true;
                return;
            }
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new IOException("Broker pread failed at offset " + position + ": " + opst.getMessage());
            }
            buffer = rep.getData();
            bufOffset = 0;
            bufLen = buffer.length;
            if (bufLen == 0) {
                eof = true;
            }
        } catch (TException e) {
            clientInvalidated = true;
            clientPool.invalidate(endpoint, client);
            throw new IOException("Broker pread RPC failed at offset " + position + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (clientInvalidated) {
            // Client already invalidated by a prior RPC failure; broker FD will time out on the broker side.
            return;
        }
        try {
            TBrokerCloseReaderRequest req = new TBrokerCloseReaderRequest(TBrokerVersion.VERSION_ONE, fd);
            TBrokerOperationStatus opst = client.closeReader(req);
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                LOG.warn("Failed to close broker reader for fd {}: {}", fd, opst.getMessage());
            }
            clientPool.returnGood(endpoint, client);
        } catch (TException e) {
            clientPool.invalidate(endpoint, client);
            throw new IOException("Broker closeReader RPC failed: " + e.getMessage(), e);
        }
    }
}
