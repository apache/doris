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

import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Commons-pool2 factory that creates and validates TPaloBrokerService Thrift clients.
 */
class BrokerClientFactory extends BaseKeyedPooledObjectFactory<TNetworkAddress, TPaloBrokerService.Client> {

    private static final Logger LOG = LogManager.getLogger(BrokerClientFactory.class);

    private static final int SOCKET_TIMEOUT_MS = 300_000;

    @Override
    public TPaloBrokerService.Client create(TNetworkAddress address) throws Exception {
        TSocket socket = new TSocket(address.getHostname(), address.getPort(), SOCKET_TIMEOUT_MS);
        TTransport transport = socket;
        try {
            transport.open();
        } catch (TTransportException e) {
            throw new Exception("Failed to connect to broker at " + address + ": " + e.getMessage(), e);
        }
        return new TPaloBrokerService.Client(new TBinaryProtocol(transport));
    }

    @Override
    public PooledObject<TPaloBrokerService.Client> wrap(TPaloBrokerService.Client client) {
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(TNetworkAddress address, PooledObject<TPaloBrokerService.Client> obj) {
        try {
            TTransport transport = obj.getObject().getInputProtocol().getTransport();
            if (transport.isOpen()) {
                transport.close();
            }
        } catch (Exception e) {
            LOG.warn("Failed to close broker client transport for {}: {}", address, e.getMessage());
        }
    }

    @Override
    public boolean validateObject(TNetworkAddress address, PooledObject<TPaloBrokerService.Client> obj) {
        TTransport transport = obj.getObject().getInputProtocol().getTransport();
        return transport.isOpen();
    }
}
