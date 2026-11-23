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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.ConnectorValidationContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Lance connector implementation. Created once per catalog lifecycle.
 *
 * <p>Building it binds the properties and nothing else: no {@code org.lance.*} class is touched and no
 * storage is reached until a statement needs the namespace (see {@link LanceCatalogClient}), because an FE
 * replaying the edit log builds every catalog.
 */
public final class LanceConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(LanceConnector.class);

    /**
     * Statement-level snapshots are what pins every step of a scan to one dataset version. Declared on every
     * Lance catalog, which also keeps the table class the same as the one a 4.x image replays into.
     */
    private static final Set<ConnectorCapability> CAPABILITIES =
            Collections.unmodifiableSet(EnumSet.of(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT));

    private final LanceCatalogProperties props;
    private final ConnectorContext context;
    private final LanceCatalogClient client;

    public LanceConnector(Map<String, String> properties, ConnectorContext context) {
        this.props = LanceCatalogProperties.of(properties);
        this.context = context;
        this.client = new LanceCatalogClient(props, () -> context.getStorageContext().getStorageProperties(),
                () -> LanceConf.arrowMemoryLimitBytes(context));
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new LanceConnectorMetadata(client);
    }

    /** Built per call because it holds no state; everything worth keeping lives on the catalog client. */
    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new LanceScanPlanProvider(client, props.getCatalogType(),
                () -> context.getStorageContext().getBackendStorageProperties());
    }

    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return CAPABILITIES;
    }

    /**
     * Runs on CREATE CATALOG only, so an OSS-HDFS warehouse is refused where the user wrote it. The first use of
     * the namespace refuses it again: ALTER CATALOG does not run this check.
     */
    @Override
    public void preCreateValidation(ConnectorValidationContext validationContext) {
        if (!props.isRest()) {
            LanceStorageOptions.rejectOssHdfs(context.getStorageContext().getStorageProperties());
        }
    }

    /** {@code test_connection}: the namespace answers a listing under the catalog's parent namespace. */
    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            client.testConnection();
            return ConnectorTestResult.success("Connected to the Lance " + props.getCatalogType() + " catalog");
        } catch (DorisConnectorException e) {
            // Already sanitized: no credential or dataset URI reaches the log or the user.
            LOG.warn("Lance {} catalog connectivity test failed: {}", props.getCatalogType(), e.getMessage());
            return ConnectorTestResult.failure("Lance " + props.getCatalogType()
                    + " catalog connectivity test failed: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
