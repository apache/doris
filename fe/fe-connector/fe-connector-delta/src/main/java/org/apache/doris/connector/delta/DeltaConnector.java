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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Native Delta connector with pluggable catalog adapters. */
public final class DeltaConnector implements Connector {

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private final DeltaCatalogAdapter catalogAdapter;
    private final DeltaKernelWriter writer;
    private final DeltaScanPlanProvider scanPlanProvider;
    private final DeltaWritePlanProvider writePlanProvider;

    public DeltaConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = immutableCopy(properties);
        this.context = Objects.requireNonNull(context, "context");

        Configuration hadoopConfiguration = buildHadoopConfiguration(this.properties);
        String catalogType = DeltaConnectorProperties.catalogType(this.properties);
        boolean writeEnabled = Boolean.parseBoolean(this.properties.getOrDefault(
                DeltaConnectorProperties.WRITE_ENABLED, "false"));
        DeltaKernelWriter writer = null;
        if (DeltaConnectorProperties.CATALOG_TYPE_PATH.equals(catalogType)) {
            Engine engine = DefaultEngine.create(hadoopConfiguration);
            DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                    engine);
            writer = writeEnabled ? new DeltaKernelWriter(engine) : null;
            this.catalogAdapter = new DeltaPathCatalogAdapter(
                    this.properties.get(DeltaConnectorProperties.DATABASE),
                    this.properties.get(DeltaConnectorProperties.TABLE),
                    this.properties.get(DeltaConnectorProperties.TABLE_PATH), loader, writer);
        } else {
            UnityDeltaClient unityClient = UnityDeltaClient.create(this.properties);
            this.catalogAdapter = new UnityDeltaCatalogAdapter(
                    this.properties.get(DeltaConnectorProperties.UNITY_CATALOG),
                    unityClient, hadoopConfiguration, this.properties);
        }
        this.writer = writer;
        this.scanPlanProvider = new DeltaScanPlanProvider(catalogAdapter, this.properties);
        this.writePlanProvider = writeEnabled
                ? new DeltaWritePlanProvider(catalogAdapter, context) : null;
    }

    @Override
    public DeltaConnectorMetadata getMetadata(ConnectorSession session) {
        return new DeltaConnectorMetadata(catalogAdapter, properties, writer);
    }

    @Override
    public DeltaScanPlanProvider getScanPlanProvider() {
        return scanPlanProvider;
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        return writePlanProvider;
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            return ConnectorTestResult.success(catalogAdapter.testConnection());
        } catch (RuntimeException e) {
            return ConnectorTestResult.failure(e.getMessage());
        }
    }

    @Override
    public boolean defaultTestConnection() {
        return true;
    }

    @Override
    public Set<ConnectorCapability> getCapabilities() {
        EnumSet<ConnectorCapability> capabilities = EnumSet.of(
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT);
        return Collections.unmodifiableSet(capabilities);
    }

    /** Returns the adapter for the scan planner and future catalog integrations. */
    public DeltaCatalogAdapter getCatalogAdapter() {
        return catalogAdapter;
    }

    /** Returns immutable catalog properties for connector-owned planners. */
    public Map<String, String> getProperties() {
        return properties;
    }

    /** Returns the FE context associated with this connector. */
    public ConnectorContext getContext() {
        return context;
    }

    static Configuration buildHadoopConfiguration(Map<String, String> properties) {
        Configuration configuration = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (isKernelConfigurationProperty(entry.getKey())) {
                configuration.set(entry.getKey(), entry.getValue());
            }
        }
        DeltaStorageProperties.configureHadoop(configuration, properties);
        return configuration;
    }

    private static boolean isKernelConfigurationProperty(String key) {
        return key.startsWith("hadoop.") || key.startsWith("fs.")
                || key.startsWith("dfs.") || key.startsWith("hive.")
                || key.startsWith("s3.") || key.startsWith("s3a.")
                || key.startsWith("azure.") || key.startsWith("adls.")
                || key.startsWith("gcs.") || key.startsWith("google.")
                || key.startsWith("cos.") || key.startsWith("oss.")
                || key.startsWith("obs.");
    }

    private static Map<String, String> immutableCopy(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");
        return Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    @Override
    public void close() throws IOException {
        // Delta Kernel's DefaultEngine has no close contract.  The connector owns no other
        // closeable resources; the method is retained for Connector lifecycle symmetry.
    }
}
