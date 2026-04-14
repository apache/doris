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

    private final Location location;
    private final TNetworkAddress endpoint;
    private final String clientId;
    private final Map<String, String> brokerParams;
    private final BrokerClientPool clientPool;

    BrokerOutputFile(Location location, TNetworkAddress endpoint,
            String clientId, Map<String, String> brokerParams, BrokerClientPool clientPool) {
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

    @Override
    public OutputStream create() throws IOException {
        return openWriter(TBrokerOpenMode.APPEND);
    }

    @Override
    public OutputStream createOrOverwrite() throws IOException {
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
                throw new IOException("Failed to open broker writer for [" + location + "]: " + opst.getMessage());
            }
            TBrokerFD fd = new TBrokerFD(rep.getFd().getHigh(), rep.getFd().getLow());
            returnToPool = false; // BrokerOutputStream takes ownership of the client
            return new BrokerOutputStream(endpoint, clientPool, client, fd);
        } catch (TException e) {
            throw new IOException("Broker openWriter RPC failed for [" + location + "]: " + e.getMessage(), e);
        } finally {
            if (returnToPool) {
                clientPool.returnGood(endpoint, client);
            }
        }
    }
}
