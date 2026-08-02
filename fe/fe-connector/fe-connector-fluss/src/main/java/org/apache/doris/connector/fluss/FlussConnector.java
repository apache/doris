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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Fluss connector: one instance per catalog, owning that catalog's single fluss {@link Connection}.
 *
 * <p>The connection is the expensive, thread-safe, long-lived object (it carries the RPC client,
 * metadata updater and their threads); {@code Admin} handles taken from it are cheap and memoized by
 * the connection itself. So the connection is created once, lazily, and closed when the catalog goes
 * away, while each statement gets its own {@link ConnectorMetadata} over a fresh
 * {@link ConnectionBackedFlussAdminOps} view of it.
 */
public class FlussConnector implements Connector {

    private final Map<String, String> properties;
    private final String catalogName;

    private volatile Connection connection;

    public FlussConnector(Map<String, String> properties, ConnectorContext context) {
        FlussConnectorProperties.validate(properties);
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        this.catalogName = context.getCatalogName();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new FlussConnectorMetadata(adminOps());
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            adminOps().listDatabases();
            return ConnectorTestResult.success();
        } catch (Exception e) {
            return ConnectorTestResult.failure("Fluss connectivity test failed: " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        Connection toClose = connection;
        connection = null;
        if (toClose == null) {
            return;
        }
        try {
            toClose.close();
        } catch (Exception e) {
            throw new IOException("Failed to close the fluss connection of catalog '" + catalogName + "'", e);
        }
    }

    private FlussAdminOps adminOps() {
        return new ConnectionBackedFlussAdminOps(getOrCreateConnection(), catalogName,
                FlussConnectorProperties.bootstrapServers(properties));
    }

    private Connection getOrCreateConnection() {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    connection = createConnection();
                }
            }
        }
        return connection;
    }

    private Connection createConnection() {
        Configuration config = new Configuration();
        FlussConnectorProperties.toFlussClientConfig(properties).forEach(config::setString);

        // TCCL pin, and this is the locus that matters: creating the connection is what spawns the
        // fluss client's own threads (netty IO, metadata updater), and a thread inherits the context
        // classloader of whoever created it. Started under the engine's loader they would resolve
        // fluss classes against fe-core instead of this child-first plugin, where fluss does not exist
        // at all. Pinning here fixes every thread the connection owns for its whole life, which is why
        // the per-call admin path needs no pin of its own.
        ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        try {
            return ConnectionFactory.createConnection(config);
        } catch (RuntimeException e) {
            throw new DorisConnectorException("Failed to connect fluss catalog '" + catalogName + "' ("
                    + FlussConnectorProperties.BOOTSTRAP_SERVERS + "="
                    + FlussConnectorProperties.bootstrapServers(properties) + ")", e);
        } finally {
            Thread.currentThread().setContextClassLoader(callerLoader);
        }
    }
}
