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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.ConnectorValidationContext;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TJdbcTable;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * JDBC connector implementation. Manages the lifecycle of
 * {@link JdbcConnectorClient} (HikariCP data source, JDBC driver classloader).
 */
public class JdbcDorisConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(JdbcDorisConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile JdbcConnectorClient client;
    private volatile JdbcScanPlanProvider scanPlanProvider;
    private volatile boolean closed;

    static final String JDBC_PROPERTIES_PREFIX = "jdbc.";

    public JdbcDorisConnector(Map<String, String> properties, ConnectorContext context) {
        Map<String, String> normalized = new HashMap<>();
        // Strip "jdbc." prefix from property keys for backward compatibility.
        // Users may write "jdbc.jdbc_url" or "jdbc.user" in CREATE CATALOG;
        // internally we use the short form ("jdbc_url", "user").
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(JDBC_PROPERTIES_PREFIX)) {
                String shortKey = key.substring(JDBC_PROPERTIES_PREFIX.length());
                // Only strip if the short key is not already present
                if (!properties.containsKey(shortKey)) {
                    normalized.put(shortKey, entry.getValue());
                    continue;
                }
            }
            normalized.put(key, entry.getValue());
        }
        String rawUrl = normalized.get(JdbcConnectorProperties.JDBC_URL);
        if (rawUrl != null && !rawUrl.isEmpty()) {
            JdbcDbType dbType = JdbcDbType.parseFromUrl(rawUrl);
            normalized.put(JdbcConnectorProperties.JDBC_URL,
                    JdbcUrlNormalizer.normalize(rawUrl, dbType, context.getEnvironment()));
        }
        this.properties = Collections.unmodifiableMap(normalized);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new JdbcConnectorMetadata(getOrCreateClient(), properties);
    }

    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return EnumSet.of(
                ConnectorCapability.SUPPORTS_INSERT,
                ConnectorCapability.SUPPORTS_PASSTHROUGH_QUERY
        );
    }

    @Override
    public boolean defaultTestConnection() {
        return true;
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        if (scanPlanProvider == null) {
            synchronized (this) {
                if (closed) {
                    throw new DorisConnectorException("JdbcDorisConnector has been closed");
                }
                if (scanPlanProvider == null) {
                    // Use client's effective dbType instead of static URL parsing,
                    // so OceanBase Oracle mode is detected correctly
                    JdbcDbType dbType = getOrCreateClient().getDbType();
                    scanPlanProvider = new JdbcScanPlanProvider(
                            dbType, properties, context.getCatalogId());
                }
            }
        }
        return scanPlanProvider;
    }

    @Override
    public void preCreateValidation(ConnectorValidationContext context) throws Exception {
        // 1. Validate/resolve JDBC driver — format, whitelist, secure_path, file existence.
        String driverUrl = properties.get(JdbcConnectorProperties.DRIVER_URL);
        if (driverUrl != null && !driverUrl.isEmpty()) {
            context.validateAndResolveDriverPath(driverUrl);

            // 2. Compute and verify checksum.
            String computedChecksum = context.computeDriverChecksum(driverUrl);
            String providedChecksum = context.getProperty(JdbcConnectorProperties.DRIVER_CHECKSUM);
            if (providedChecksum != null && !providedChecksum.isEmpty()) {
                if (!providedChecksum.equals(computedChecksum)) {
                    throw new DorisConnectorException(
                            "The provided checksum (" + providedChecksum
                                    + ") does not match the computed checksum (" + computedChecksum
                                    + ") for the driver_url.");
                }
            } else {
                context.storeProperty(JdbcConnectorProperties.DRIVER_CHECKSUM, computedChecksum);
            }
        }

        // 3. Test BE→JDBC connectivity via BRPC (only when test_connection is enabled).
        // The connector builds the serialized payload; the engine sends it after validation.
        boolean testConnection = Boolean.parseBoolean(
                properties.getOrDefault("test_connection", "true"));
        if (testConnection) {
            TTableDescriptor testThrift = buildTestTableDescriptor(context);
            TOdbcTableType tableType = parseOdbcType();
            byte[] serialized = new TSerializer().serialize(testThrift);
            context.requestBeConnectivityTest(serialized, tableType.getValue(), getTestQuery());
        }
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            JdbcConnectorClient c = getOrCreateClient();
            List<String> dbs = c.getDatabaseNameList();
            LOG.info("JDBC connection test succeeded, found {} databases", dbs.size());
            return ConnectorTestResult.success(
                    "Connected successfully, found " + dbs.size() + " databases");
        } catch (Exception e) {
            LOG.warn("JDBC connection test failed", e);
            return ConnectorTestResult.failure("JDBC connection failed: " + e.getMessage());
        }
    }

    private JdbcConnectorClient getOrCreateClient() {
        if (closed) {
            throw new DorisConnectorException("JdbcDorisConnector has been closed");
        }
        if (client == null) {
            synchronized (this) {
                if (closed) {
                    throw new DorisConnectorException("JdbcDorisConnector has been closed");
                }
                if (client == null) {
                    client = createClient();
                }
            }
        }
        return client;
    }

    private JdbcConnectorClient createClient() {
        String jdbcUrl = properties.get(JdbcConnectorProperties.JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new DorisConnectorException("JDBC URL ('" + JdbcConnectorProperties.JDBC_URL + "') is required");
        }
        JdbcDbType dbType = JdbcDbType.parseFromUrl(jdbcUrl);
        String user = properties.getOrDefault(JdbcConnectorProperties.USER, "");
        String password = properties.getOrDefault(JdbcConnectorProperties.PASSWORD, "");
        String driverUrl = resolveDriverUrl(properties.get(JdbcConnectorProperties.DRIVER_URL));
        String driverClass = properties.get(JdbcConnectorProperties.DRIVER_CLASS);
        int poolMinSize = JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MIN_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MIN_SIZE);
        int poolMaxSize = JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_SIZE);
        int poolMaxWaitTime = JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_WAIT_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_WAIT_TIME);
        int poolMaxLifeTime = JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_LIFE_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_LIFE_TIME);
        boolean onlySpecifiedDatabase = Boolean.parseBoolean(
                properties.getOrDefault(JdbcConnectorProperties.ONLY_SPECIFIED_DATABASE, "false"));
        boolean enableMappingVarbinary = Boolean.parseBoolean(
                properties.getOrDefault(JdbcConnectorProperties.ENABLE_MAPPING_VARBINARY, "false"));
        boolean enableMappingTimestampTz = Boolean.parseBoolean(
                properties.getOrDefault(JdbcConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));

        LOG.info("Creating JDBC connector client for dbType={}, url={}", dbType, jdbcUrl);
        return JdbcConnectorClient.create(
                dbType, context.getCatalogName(), jdbcUrl, user, password,
                driverUrl, driverClass,
                poolMinSize, poolMaxSize, poolMaxWaitTime, poolMaxLifeTime,
                onlySpecifiedDatabase, properties,
                enableMappingVarbinary, enableMappingTimestampTz,
                context::sanitizeJdbcUrl);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            closed = true;
            JdbcConnectorClient c = client;
            client = null;
            scanPlanProvider = null;
            if (c != null) {
                c.close();
            }
        }
    }

    /**
     * Resolves driver URL using the environment from ConnectorContext.
     * If the URL is a plain filename (e.g., "mysql-connector-j-8.4.0.jar"),
     * resolves it using the jdbc_drivers_dir from the environment.
     */
    private String resolveDriverUrl(String driverUrl) {
        if (driverUrl == null || driverUrl.isEmpty()) {
            return driverUrl;
        }
        if (driverUrl.startsWith("file://") || driverUrl.startsWith("http://")
                || driverUrl.startsWith("https://") || driverUrl.startsWith("/")) {
            return driverUrl;
        }
        // Plain filename — resolve using jdbc_drivers_dir from environment
        Map<String, String> env = context.getEnvironment();
        String driversDir = env.get("jdbc_drivers_dir");
        String dorisHome = env.get("doris_home");
        if (driversDir != null && !driversDir.isEmpty()) {
            String newPath = driversDir + "/" + driverUrl;
            if (new File(newPath).exists()) {
                return "file://" + newPath;
            }
            // Backward compatibility: check the old default directory
            // (DORIS_HOME/jdbc_drivers) when the user hasn't customized jdbc_drivers_dir
            if (dorisHome != null) {
                String defaultNewDir = dorisHome + "/plugins/jdbc_drivers";
                if (driversDir.equals(defaultNewDir)) {
                    String oldPath = dorisHome + "/jdbc_drivers/" + driverUrl;
                    if (new File(oldPath).exists()) {
                        LOG.info("Resolved driver_url '{}' from old default directory: {}",
                                driverUrl, oldPath);
                        return "file://" + oldPath;
                    }
                }
            }
            String resolved = "file://" + newPath;
            LOG.info("Resolved driver_url '{}' to '{}' using jdbc_drivers_dir", driverUrl, resolved);
            return resolved;
        }
        return "file://" + driverUrl;
    }

    private TTableDescriptor buildTestTableDescriptor(ConnectorValidationContext context) {
        TJdbcTable tJdbcTable = new TJdbcTable();
        tJdbcTable.setCatalogId(context.getCatalogId());
        tJdbcTable.setJdbcUrl(properties.getOrDefault(JdbcConnectorProperties.JDBC_URL, ""));
        tJdbcTable.setJdbcUser(properties.getOrDefault(JdbcConnectorProperties.USER, ""));
        tJdbcTable.setJdbcPassword(properties.getOrDefault(JdbcConnectorProperties.PASSWORD, ""));
        tJdbcTable.setJdbcTableName("test_jdbc_connection");
        tJdbcTable.setJdbcDriverClass(
                properties.getOrDefault(JdbcConnectorProperties.DRIVER_CLASS, ""));
        tJdbcTable.setJdbcDriverUrl(
                properties.getOrDefault(JdbcConnectorProperties.DRIVER_URL, ""));
        tJdbcTable.setJdbcResourceName("");
        // Use the checksum that was computed/verified during driver validation.
        String checksum = context.getProperty(JdbcConnectorProperties.DRIVER_CHECKSUM);
        tJdbcTable.setJdbcDriverChecksum(checksum != null ? checksum : "");
        tJdbcTable.setConnectionPoolMinSize(JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MIN_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MIN_SIZE));
        tJdbcTable.setConnectionPoolMaxSize(JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_SIZE));
        tJdbcTable.setConnectionPoolMaxWaitTime(JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_WAIT_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_WAIT_TIME));
        tJdbcTable.setConnectionPoolMaxLifeTime(JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_LIFE_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_LIFE_TIME));
        tJdbcTable.setConnectionPoolKeepAlive(Boolean.parseBoolean(
                properties.getOrDefault(JdbcConnectorProperties.CONNECTION_POOL_KEEP_ALIVE,
                        String.valueOf(JdbcConnectorProperties.DEFAULT_POOL_KEEP_ALIVE))));
        TTableDescriptor tTableDescriptor = new TTableDescriptor(
                0, TTableType.JDBC_TABLE, 0, 0, "test_jdbc_connection", "");
        tTableDescriptor.setJdbcTable(tJdbcTable);
        return tTableDescriptor;
    }

    private TOdbcTableType parseOdbcType() {
        String jdbcUrl = properties.getOrDefault(JdbcConnectorProperties.JDBC_URL, "");
        JdbcDbType dbType = JdbcDbType.parseFromUrl(jdbcUrl);
        try {
            return TOdbcTableType.valueOf(dbType.name());
        } catch (Exception e) {
            return TOdbcTableType.MYSQL;
        }
    }

    private String getTestQuery() {
        String jdbcUrl = properties.getOrDefault(JdbcConnectorProperties.JDBC_URL, "");
        JdbcDbType dbType = JdbcDbType.parseFromUrl(jdbcUrl);
        switch (dbType) {
            case ORACLE:
            case OCEANBASE_ORACLE:
                return "SELECT 1 FROM dual";
            case DB2:
                return "SELECT 1 FROM SYSIBM.SYSDUMMY1";
            case SAP_HANA:
                return "SELECT 1 FROM DUMMY";
            default:
                return "SELECT 1";
        }
    }
}
