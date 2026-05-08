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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.hms.ThriftHmsClient;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Hive connector implementation. Manages the lifecycle of
 * an {@link HmsClient} for HMS operations and provides scan planning.
 */
public class HiveConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(HiveConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile HmsClient hmsClient;

    public HiveConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new HiveConnectorMetadata(getOrCreateClient(), properties);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new HiveScanPlanProvider(getOrCreateClient(), properties);
    }

    private HmsClient getOrCreateClient() {
        if (hmsClient == null) {
            synchronized (this) {
                if (hmsClient == null) {
                    hmsClient = createClient();
                }
            }
        }
        return hmsClient;
    }

    private HmsClient createClient() {
        String metastoreUri = properties.get(HiveConnectorProperties.HIVE_METASTORE_URIS);
        if (metastoreUri == null || metastoreUri.isEmpty()) {
            // Also check the "uri" short form
            metastoreUri = properties.get("uri");
        }
        if (metastoreUri == null || metastoreUri.isEmpty()) {
            throw new DorisConnectorException(
                    "HMS URI ('" + HiveConnectorProperties.HIVE_METASTORE_URIS + "') is required");
        }

        int poolSize = HiveConnectorProperties.getInt(
                properties, HiveConnectorProperties.HMS_CLIENT_POOL_SIZE,
                HiveConnectorProperties.DEFAULT_HMS_CLIENT_POOL_SIZE);

        HmsClientConfig config = new HmsClientConfig(properties, poolSize);
        LOG.info("Creating Hive connector client for catalog='{}', uri={}, type={}, poolSize={}",
                context.getCatalogName(), config.getMetastoreUri(),
                config.getMetastoreType(), poolSize);

        ThriftHmsClient.AuthAction authAction = context::executeAuthenticated;
        return new ThriftHmsClient(config, authAction);
    }

    @Override
    public void close() throws IOException {
        HmsClient c = hmsClient;
        if (c != null) {
            c.close();
            hmsClient = null;
        }
    }
}
