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

import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.Location;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerOpenMode;
import org.apache.doris.thrift.TBrokerOpenWriterRequest;
import org.apache.doris.thrift.TBrokerOpenWriterResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.TException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * {@link DorisOutputFile} backed by the Doris Broker {@code openWriter} Thrift RPC.
 */
class BrokerOutputFile implements DorisOutputFile {

    private final BrokerSpiFileSystem fs;
    private final Location location;
    private final TNetworkAddress endpoint;
    private final String clientId;
    private final Map<String, String> brokerParams;
    private final BrokerClientPool clientPool;

    BrokerOutputFile(BrokerSpiFileSystem fs, Location location, TNetworkAddress endpoint,
            String clientId, Map<String, String> brokerParams, BrokerClientPool clientPool) {
        this.fs = fs;
        this.location = location;
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.brokerParams = brokerParams;
        this.clientPool = clientPool;
    }

    @Override
    public Location location() {
        return location;
    }

    /**
     * Creates the file. Fails with {@link IOException} if the file already exists.
     *
     * <p>The existence check and the subsequent open are not atomic: between the
     * {@code exists} probe and {@code openWriter} another writer may create the file.
     * Callers requiring strict no-clobber semantics must serialize externally.
     */
    @Override
    public OutputStream create() throws IOException {
        if (fs.exists(location)) {
            throw new IOException("File already exists: " + location);
        }
        return openWriter(TBrokerOpenMode.APPEND);
    }

    /**
     * Creates the file or truncates it if it already exists.
     *
     * <p>The broker Thrift IDL only exposes {@code APPEND} open mode (no truncating
     * {@code WRITE} mode), so truncation is emulated by deleting the existing path
     * before opening the writer. The delete and open are not atomic; concurrent
     * writers to the same path may interleave.
     */
    @Override
    public OutputStream createOrOverwrite() throws IOException {
        // delete() is a no-op when the path does not exist (broker FILE_NOT_FOUND is swallowed).
        fs.delete(location, false);
        return openWriter(TBrokerOpenMode.APPEND);
    }

    private OutputStream openWriter(TBrokerOpenMode mode) throws IOException {
        TPaloBrokerService.Client client = clientPool.borrow(endpoint);
        boolean returnToPool = true;
        try {
            TBrokerOpenWriterRequest req = new TBrokerOpenWriterRequest(
                    TBrokerVersion.VERSION_ONE, location.uri(), mode, clientId, brokerParams);
            TBrokerOpenWriterResponse rep = client.openWriter(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                // Application-level failure: the Thrift client itself is healthy, so it
                // remains eligible to be returned to the pool by the finally block.
                throw new IOException("Failed to open broker writer for [" + location + "]: " + opst.getMessage());
            }
            TBrokerFD fd = new TBrokerFD(rep.getFd().getHigh(), rep.getFd().getLow());
            returnToPool = false; // BrokerOutputStream takes ownership of the client
            return new BrokerOutputStream(endpoint, clientPool, client, fd);
        } catch (TException e) {
            // Transport-level failure: the client is broken and must not be returned to the pool.
            returnToPool = false;
            clientPool.invalidate(endpoint, client);
            throw new IOException("Broker openWriter RPC failed for [" + location + "]: " + e.getMessage(), e);
        } finally {
            if (returnToPool) {
                clientPool.returnGood(endpoint, client);
            }
        }
    }
}
