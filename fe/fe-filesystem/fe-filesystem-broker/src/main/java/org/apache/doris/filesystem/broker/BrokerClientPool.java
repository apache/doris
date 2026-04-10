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

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

/**
 * Thread-safe pool of {@link TPaloBrokerService.Client} Thrift clients keyed by
 * {@link TNetworkAddress}.
 *
 * <p>Manages connection lifecycle independently of fe-core's {@code ClientPool}.
 * Each {@link BrokerSpiFileSystem} instance holds its own pool scoped to one broker endpoint.
 */
class BrokerClientPool implements Closeable {

    private static final Logger LOG = LogManager.getLogger(BrokerClientPool.class);

    private final GenericKeyedObjectPool<TNetworkAddress, TPaloBrokerService.Client> pool;

    @SuppressWarnings("unchecked")
    BrokerClientPool() {
        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setMaxIdlePerKey(16);
        config.setMinIdlePerKey(0);
        config.setMaxTotalPerKey(-1);
        config.setMaxTotal(-1);
        config.setMaxWaitMillis(500);
        config.setTestOnBorrow(true);
        this.pool = new GenericKeyedObjectPool<>(new BrokerClientFactory(), config);
    }

    TPaloBrokerService.Client borrow(TNetworkAddress address) throws IOException {
        try {
            return pool.borrowObject(address);
        } catch (Exception e) {
            throw new IOException("Failed to borrow broker client for " + address + ": " + e.getMessage(), e);
        }
    }

    void returnGood(TNetworkAddress address, TPaloBrokerService.Client client) {
        try {
            pool.returnObject(address, client);
        } catch (Exception e) {
            LOG.warn("Failed to return broker client for {}: {}", address, e.getMessage());
        }
    }

    void invalidate(TNetworkAddress address, TPaloBrokerService.Client client) {
        try {
            pool.invalidateObject(address, client);
        } catch (Exception e) {
            LOG.warn("Failed to invalidate broker client for {}: {}", address, e.getMessage());
        }
    }

    @Override
    public void close() {
        pool.close();
    }
}
