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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AccountFormat;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

    private final MCCatalogProperties props;
    private final ConnectorContext context;

    // Connector-owned partition-listing cache, shared by the (per-call) metadata's three partition-listing
    // methods. One per connector — the metadata is rebuilt per query, so the cache must live on the long-lived
    // connector to survive across queries. Its loader captures this connector and reads structureHelper/odps
    // lazily at query time (always post-init, since getMetadata calls ensureInitialized before use).
    private final MaxComputePartitionCache partitionCache;

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
        this.props = MCCatalogProperties.of(properties);
        this.context = context;
        // The cache reads the framework's own meta.cache.* keys off the raw map, so it keeps taking one.
        this.partitionCache = new MaxComputePartitionCache(props.getRaw(),
                (db, t) -> structureHelper.getPartitions(odps, db, t));
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
        endpoint = props.getResolvedEndpoint();

        defaultProject = props.getProject();
        quota = props.getQuota();

        odps = MCConnectorClientFactory.createClient(props);
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(endpoint);

        odps.setAccountFormat(props.getAccountFormat() == MCCatalogProperties.AccountFormat.ID
                ? AccountFormat.ID : AccountFormat.DISPLAYNAME);

        enableNamespaceSchema = props.isEnableNamespaceSchema();
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
        int connectTimeout = props.getConnectTimeout();
        int readTimeout = props.getReadTimeout();
        int retryTimes = props.getRetryCount();

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
                odps, structureHelper, defaultProject, endpoint, quota, props.getRaw(), partitionCache);
    }

    /**
     * REFRESH TABLE hook: drops this table's connector-owned partition listing. fe-core routes
     * {@code REFRESH TABLE} to {@code connector.invalidateTable} for a plugin-driven catalog. Mirrors
     * {@code HiveConnector.invalidateTable}.
     */
    @Override
    public void invalidateTable(String dbName, String tableName) {
        partitionCache.invalidateTable(dbName, tableName);
    }

    /**
     * REFRESH DATABASE hook: drops the connector-owned partition listings for every table in one database.
     * Mirrors {@code HiveConnector.invalidateDb}.
     */
    @Override
    public void invalidateDb(String dbName) {
        partitionCache.invalidateDb(dbName);
    }

    /** REFRESH CATALOG hook: drops the whole connector-owned partition cache. Mirrors {@code HiveConnector}. */
    @Override
    public void invalidateAll() {
        partitionCache.invalidateAll();
    }

    /**
     * Invalidates a table's partition cache on a partition add/drop/alter. The cache is keyed by {@code (db,
     * table)} and cannot target a single partition name, so this degrades to a whole-table flush (correctness
     * -safe: the cache re-lists on the next miss). Mirrors {@code HiveConnector.invalidatePartition}.
     */
    @Override
    public void invalidatePartition(String dbName, String tableName, List<String> partitionNames) {
        partitionCache.invalidateTable(dbName, tableName);
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
                    + "'. Check " + MCCatalogProperties.PROJECT + ", " + MCCatalogProperties.ENDPOINT
                    + " and credentials. Cause: " + e.getMessage(), e);
        }
        if (!projectExists) {
            throw new RuntimeException("Failed to validate MaxCompute project '" + defaultProject
                    + "'. Check " + MCCatalogProperties.PROJECT + ", " + MCCatalogProperties.ENDPOINT
                    + " and credentials. Cause: project does not exist or is not accessible");
        }
    }

    private void validateMaxComputeProjectAndNamespaceSchema() {
        try {
            validateMaxComputeNamespaceSchemaAccess(defaultProject);
        } catch (Exception e) {
            throw new RuntimeException("Failed to validate MaxCompute project '" + defaultProject
                    + "' with namespace schema. Check " + MCCatalogProperties.PROJECT + ", "
                    + MCCatalogProperties.ENDPOINT
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

    public MCCatalogProperties getProps() {
        return props;
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
