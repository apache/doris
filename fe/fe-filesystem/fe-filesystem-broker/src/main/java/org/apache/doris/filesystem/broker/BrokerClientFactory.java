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

import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPingBrokerRequest;
import org.apache.doris.thrift.TBrokerVersion;
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
 *
 * <p>Transport stack mirrors fe-core's {@code ClientPool.brokerPool} which constructs
 * a {@link org.apache.doris.common.GenericPool} with {@code isNonBlockingIO=false},
 * yielding a raw {@link TSocket} (no {@code TFramedTransport}). See
 * {@code fe/fe-core/src/main/java/org/apache/doris/common/ClientPool.java:80-81} and
 * {@code fe/fe-core/src/main/java/org/apache/doris/common/GenericPool.java:212}.
 */
class BrokerClientFactory extends BaseKeyedPooledObjectFactory<TNetworkAddress, TPaloBrokerService.Client> {

    private static final Logger LOG = LogManager.getLogger(BrokerClientFactory.class);

    private static final int SOCKET_TIMEOUT_MS = 300_000;
    /** Short timeout used only for the validation ping so a half-broken socket fails fast. */
    private static final int VALIDATE_PING_TIMEOUT_MS = 5_000;
    /** Stable client id sent with the validation ping; broker uses it for its own logging. */
    private static final String VALIDATOR_CLIENT_ID = "fe-broker-pool-validator";

    @Override
    public TPaloBrokerService.Client create(TNetworkAddress address) throws Exception {
        // The base class signature ({@link BaseKeyedPooledObjectFactory#create}) requires
        // {@code throws Exception}, so we keep that here and log the underlying cause
        // verbosely — commons-pool2 wraps factory failures in NoSuchElementException, which
        // by itself loses the connect-failure context.
        TSocket socket = new TSocket(address.getHostname(), address.getPort(), SOCKET_TIMEOUT_MS);
        TTransport transport = socket;
        try {
            transport.open();
        } catch (TTransportException e) {
            LOG.warn("Failed to open broker transport for {}: {}", address, e.getMessage(), e);
            throw new TTransportException(e.getType(),
                    "Failed to connect to broker at " + address + ": " + e.getMessage(), e);
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
        TPaloBrokerService.Client client = obj.getObject();
        TTransport transport = client.getInputProtocol().getTransport();
        boolean isOpen = transport.isOpen();
        if (!isOpen) {
            return false;
        }
        return pingBroker(client, transport);
    }

    /**
     * Issues a lightweight {@code ping} RPC to detect half-broken sockets that still report
     * {@code isOpen()=true}. Uses a short socket timeout so a broken peer fails fast; restores
     * the regular RPC timeout on success.
     */
    boolean pingBroker(TPaloBrokerService.Client client, TTransport transport) {
        boolean adjustTimeout = transport instanceof TSocket;
        if (adjustTimeout) {
            ((TSocket) transport).setTimeout(VALIDATE_PING_TIMEOUT_MS);
        }
        try {
            TBrokerPingBrokerRequest req = new TBrokerPingBrokerRequest(
                    TBrokerVersion.VERSION_ONE, VALIDATOR_CLIENT_ID);
            TBrokerOperationStatus status = client.ping(req);
            return status.getStatusCode() == TBrokerOperationStatusCode.OK;
        } catch (Exception e) {
            LOG.debug("Broker client ping validation failed: {}", e.getMessage());
            return false;
        } finally {
            if (adjustTimeout) {
                try {
                    ((TSocket) transport).setTimeout(SOCKET_TIMEOUT_MS);
                } catch (Exception ignored) {
                    // socket may already be broken; pool will destroy this client
                }
            }
        }
    }
}
