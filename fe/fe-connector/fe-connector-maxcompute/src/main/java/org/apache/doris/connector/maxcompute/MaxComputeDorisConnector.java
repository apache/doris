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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.spi.ConnectorContext;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AccountFormat;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Main Connector implementation for MaxCompute (ODPS).
 * Manages the Odps client lifecycle and provides metadata access.
 *
 * <p>Note: the shared ODPS {@link EnvironmentSettings} (from odps-sdk-table-api)
 * is built here and consumed by both {@link MaxComputeScanPlanProvider} and
 * {@link MaxComputeWritePlanProvider}; SplitOptions remains scan-specific and
 * stays in the scan plan provider.
 */
public class MaxComputeDorisConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(
            MaxComputeDorisConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;

    private Odps odps;
    private String endpoint;
    private String defaultProject;
    private boolean enableNamespaceSchema;
    private String quota;
    private McStructureHelper structureHelper;
    private MaxComputeScanPlanProvider scanPlanProvider;
    private MaxComputeWritePlanProvider writePlanProvider;
    private EnvironmentSettings settings;

    private volatile boolean initialized;

    public MaxComputeDorisConnector(Map<String, String> properties,
            ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    doInit();
                    initialized = true;
                }
            }
        }
    }

    private void doInit() {
        endpoint = MCConnectorEndpoint.resolveEndpoint(properties);

        defaultProject = properties.get(MCConnectorProperties.PROJECT);
        quota = properties.getOrDefault(
                MCConnectorProperties.QUOTA,
                MCConnectorProperties.DEFAULT_QUOTA);

        odps = MCConnectorClientFactory.createClient(properties);
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(endpoint);

        String accountFormatProp = properties.getOrDefault(
                MCConnectorProperties.ACCOUNT_FORMAT,
                MCConnectorProperties.DEFAULT_ACCOUNT_FORMAT);
        AccountFormat accountFormat;
        if (accountFormatProp.equals(
                MCConnectorProperties.ACCOUNT_FORMAT_ID)) {
            accountFormat = AccountFormat.ID;
        } else {
            accountFormat = AccountFormat.DISPLAYNAME;
        }
        odps.setAccountFormat(accountFormat);

        enableNamespaceSchema = Boolean.parseBoolean(
                properties.getOrDefault(
                        MCConnectorProperties.ENABLE_NAMESPACE_SCHEMA,
                        MCConnectorProperties
                                .DEFAULT_ENABLE_NAMESPACE_SCHEMA));
        structureHelper = McStructureHelper.getHelper(
                enableNamespaceSchema, defaultProject);
        settings = buildSettings();
        scanPlanProvider = new MaxComputeScanPlanProvider(this);
        writePlanProvider = new MaxComputeWritePlanProvider(this);
    }

    /**
     * Builds the shared ODPS {@link EnvironmentSettings} (credentials, endpoint,
     * quota, REST timeouts). Mirrors the legacy {@code MaxComputeExternalCatalog}
     * which holds a single {@code settings} used by both the scan path
     * ({@code MaxComputeScanNode}) and the write path ({@code MCTransaction});
     * the connector likewise shares one instance across
     * {@link MaxComputeScanPlanProvider} and {@link MaxComputeWritePlanProvider}.
     */
    private EnvironmentSettings buildSettings() {
        int connectTimeout = Integer.parseInt(properties.getOrDefault(
                MCConnectorProperties.CONNECT_TIMEOUT,
                MCConnectorProperties.DEFAULT_CONNECT_TIMEOUT));
        int readTimeout = Integer.parseInt(properties.getOrDefault(
                MCConnectorProperties.READ_TIMEOUT,
                MCConnectorProperties.DEFAULT_READ_TIMEOUT));
        int retryTimes = Integer.parseInt(properties.getOrDefault(
                MCConnectorProperties.RETRY_COUNT,
                MCConnectorProperties.DEFAULT_RETRY_COUNT));

        // Apply the same timeouts to the raw ODPS client: metadata / project / schema / DDL and the
        // CREATE-time connectivity test (testConnection) go through odps.getRestClient(), not the
        // Storage API. Mirrors legacy MaxComputeExternalCatalog.initLocalObjectsImpl; the RestOptions
        // below cover only the Storage API EnvironmentSettings used by the scan/write paths.
        odps.getRestClient().setConnectTimeout(connectTimeout);
        odps.getRestClient().setReadTimeout(readTimeout);
        odps.getRestClient().setRetryTimes(retryTimes);

        RestOptions restOptions = RestOptions.newBuilder()
                .withConnectTimeout(connectTimeout)
                .withReadTimeout(readTimeout)
                .withRetryTimes(retryTimes)
                .build();

        Credentials credentials = Credentials.newBuilder()
                .withAccount(odps.getAccount())
                .withAppAccount(odps.getAppAccount())
                .build();

        return EnvironmentSettings.newBuilder()
                .withCredentials(credentials)
                .withServiceEndpoint(odps.getEndpoint())
                .withQuotaName(quota)
                .withRestOptions(restOptions)
                .build();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        ensureInitialized();
        return new MaxComputeConnectorMetadata(
                odps, structureHelper, defaultProject, endpoint, quota, properties);
    }

    /**
     * MaxCompute writes use multiple parallel writers, and dynamic-partition writes must be
     * hash-distributed and locally sorted by the partition columns: the ODPS Storage API streams
     * partition writers and closes the previous one when a new partition value appears, so
     * un-grouped rows trigger "writer has been closed". These two capabilities drive the planner
     * sink distribution ({@code PhysicalConnectorTableSink.getRequirePhysicalProperties}), mirroring
     * the legacy {@code PhysicalMaxComputeTableSink}.
     */
    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return EnumSet.of(ConnectorCapability.SUPPORTS_PARALLEL_WRITE,
                ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT,
                // MaxCompute's columnar Storage API / JNI writer maps data positionally against the
                // full table schema, so the sink must project rows to full-schema order (see
                // BindSink.bindConnectorTableSink); not declared by name-mapped connectors like JDBC.
                ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        ensureInitialized();
        return scanPlanProvider;
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        ensureInitialized();
        return writePlanProvider;
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            ensureInitialized();
            validateMaxComputeConnection();
            return ConnectorTestResult.success(
                    "MaxCompute project '" + defaultProject + "' is accessible");
        } catch (Exception e) {
            return ConnectorTestResult.failure(e.getMessage());
        }
    }

    /**
     * Validates FE&rarr;ODPS connectivity for CREATE CATALOG (test_connection=true), mirroring
     * legacy {@code MaxComputeExternalCatalog.validateMaxComputeConnection}. When namespace schema
     * is enabled the project is three-tier, so the schema list must be reachable; otherwise the
     * project itself must exist and be accessible.
     */
    protected void validateMaxComputeConnection() {
        if (enableNamespaceSchema) {
            validateMaxComputeProjectAndNamespaceSchema();
        } else {
            validateMaxComputeProject();
        }
    }

    private void validateMaxComputeProject() {
        boolean projectExists;
        try {
            projectExists = maxComputeProjectExists(defaultProject);
        } catch (Exception e) {
            throw new RuntimeException("Failed to validate MaxCompute project '" + defaultProject
                    + "'. Check " + MCConnectorProperties.PROJECT + ", " + MCConnectorProperties.ENDPOINT
                    + " and credentials. Cause: " + e.getMessage(), e);
        }
        if (!projectExists) {
            throw new RuntimeException("Failed to validate MaxCompute project '" + defaultProject
                    + "'. Check " + MCConnectorProperties.PROJECT + ", " + MCConnectorProperties.ENDPOINT
                    + " and credentials. Cause: project does not exist or is not accessible");
        }
    }

    private void validateMaxComputeProjectAndNamespaceSchema() {
        try {
            validateMaxComputeNamespaceSchemaAccess(defaultProject);
        } catch (Exception e) {
            throw new RuntimeException("Failed to validate MaxCompute project '" + defaultProject
                    + "' with namespace schema. Check " + MCConnectorProperties.PROJECT + ", "
                    + MCConnectorProperties.ENDPOINT
                    + ", credentials, and whether the schema list is accessible for the namespace "
                    + "schema configuration. Cause: " + e.getMessage(), e);
        }
    }

    protected boolean maxComputeProjectExists(String projectName) throws OdpsException {
        return odps.projects().exists(projectName);
    }

    protected void validateMaxComputeNamespaceSchemaAccess(String projectName) throws OdpsException {
        odps.schemas().iterator(projectName).hasNext();
    }

    public Odps getClient() {
        ensureInitialized();
        return odps;
    }

    public String getEndpoint() {
        ensureInitialized();
        return endpoint;
    }

    public String getDefaultProject() {
        ensureInitialized();
        return defaultProject;
    }

    public String getQuota() {
        ensureInitialized();
        return quota;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public McStructureHelper getStructureHelper() {
        ensureInitialized();
        return structureHelper;
    }

    /**
     * Returns the shared ODPS {@link EnvironmentSettings} used by both scan and
     * write planning (see {@link #buildSettings()}).
     */
    public EnvironmentSettings getSettings() {
        ensureInitialized();
        return settings;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing MaxCompute connector for project: {}",
                defaultProject);
    }
}
