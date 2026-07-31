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

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.Location;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerOpenReaderRequest;
import org.apache.doris.thrift.TBrokerOpenReaderResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.TException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@link DorisInputFile} backed by the Doris Broker {@code openReader} Thrift RPC.
 *
 * <p>A new Thrift client and broker file descriptor are opened on each {@link #newStream()} call.
 * The length is lazily obtained from the broker file listing if not provided at construction.
 */
class BrokerInputFile implements DorisInputFile {

    private final BrokerSpiFileSystem fs;
    private final Location location;
    /** Known file length, or -1 if not yet determined. */
    private long knownLength;
    private final TNetworkAddress endpoint;
    private final String clientId;
    private final Map<String, String> brokerParams;
    private final BrokerClientPool clientPool;

    BrokerInputFile(BrokerSpiFileSystem fs, Location location, long knownLength, TNetworkAddress endpoint,
            String clientId, Map<String, String> brokerParams, BrokerClientPool clientPool) {
        this.fs = fs;
        this.location = location;
        this.knownLength = knownLength;
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.brokerParams = brokerParams;
        this.clientPool = clientPool;
    }

    @Override
    public Location location() {
        return location;
    }

    @Override
    public long length() throws IOException {
        if (knownLength >= 0) {
            return knownLength;
        }
        List<TBrokerFileStatus> statuses = fs.listPath(location.uri(), false);
        if (statuses.isEmpty()) {
            throw new FileNotFoundException("File does not exist: " + location);
        }
        TBrokerFileStatus status = statuses.get(0);
        if (status.isIsDir()) {
            throw new IOException("Not a file: " + location);
        }
        knownLength = status.getSize();
        return knownLength;
    }

    @Override
    public DorisInputStream newStream() throws IOException {
        TPaloBrokerService.Client client = clientPool.borrow(endpoint);
        boolean returnToPool = true;
        try {
            TBrokerOpenReaderRequest req = new TBrokerOpenReaderRequest(
                    TBrokerVersion.VERSION_ONE, location.uri(), 0L, clientId, brokerParams);
            TBrokerOpenReaderResponse rep = client.openReader(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                // Application-level failure: the Thrift client itself is healthy, so it
                // remains eligible to be returned to the pool by the finally block.
                throw new IOException("Failed to open broker reader for [" + location + "]: " + opst.getMessage());
            }
            TBrokerFD fd = new TBrokerFD(rep.getFd().getHigh(), rep.getFd().getLow());
            returnToPool = false; // BrokerInputStream takes ownership of the client
            return new BrokerInputStream(endpoint, clientPool, client, fd);
        } catch (TException e) {
            // Transport-level failure: the client is broken and must not be returned to the pool.
            returnToPool = false;
            clientPool.invalidate(endpoint, client);
            throw new IOException("Broker openReader RPC failed for [" + location + "]: " + e.getMessage(), e);
        } finally {
            if (returnToPool) {
                clientPool.returnGood(endpoint, client);
            }
        }
    }
}
