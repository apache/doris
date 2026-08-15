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

import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.ConnectorValidationContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.thrift.TJdbcTable;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TSerializer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * JDBC connector implementation. Manages the lifecycle of
 * {@link JdbcConnectorClient} (HikariCP data source, JDBC driver classloader).
 */
public class JdbcDorisConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(JdbcDorisConnector.class);

    private final JdbcCatalogProperties props;
    private final ConnectorContext context;
    private volatile JdbcConnectorClient client;
    private volatile JdbcScanPlanProvider scanPlanProvider;
    private volatile boolean closed;

    public JdbcDorisConnector(Map<String, String> properties, ConnectorContext context) {
        // The URL normalization needs a per-FE setting, which is why the holder takes it as a function
        // rather than reading deployment config itself -- see JdbcCatalogProperties.of.
        this.props = JdbcCatalogProperties.of(properties,
                url -> JdbcUrlNormalizer.normalize(url, JdbcDbType.parseFromUrl(url),
                        JdbcConf.forceSqlServerEncryptFalse(context)));
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new JdbcConnectorMetadata(getOrCreateClient(), props);
    }

    @Override
    public Set<ConnectorCapability> getCapabilities() {
        // SUPPORTS_METADATA_PRELOAD: preserves the legacy engine-name "jdbc" gate of
        // PluginDrivenExternalTable.supportsExternalMetadataPreload (F11) now that it is capability-driven, so
        // jdbc tables keep async metadata pre-load.
        // Passthrough SQL is NOT declared here: JdbcConnectorMetadata implements
        // ConnectorPassthroughSqlOps, and implementing that interface IS the declaration.
        return EnumSet.of(
                ConnectorCapability.SUPPORTS_METADATA_PRELOAD
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
                            dbType, props, context.getCatalogId());
                }
            }
        }
        return scanPlanProvider;
    }

    // A scheme-less driver_url must be a plain jar file name: letters, digits, dot, underscore, hyphen.
    // This intentionally forbids any path separator, so it can never escape jdbc_drivers_dir.
    private static final Pattern SAFE_DRIVER_FILE_NAME = Pattern.compile("^[A-Za-z0-9._-]+\\.jar$");

    /**
     * Mandatory, non-configurable driver_url security rule. It is invoked from
     * {@link JdbcConnectorProvider#validateProperties} (and from {@link #preCreateValidation}),
     * i.e. from the engine's {@code checkProperties()} hook, which runs only on the user-facing
     * CREATE / ALTER CATALOG paths (both guarded by {@code !isReplay}). Therefore the rule never
     * runs during metadata/edit-log replay nor at query time, so existing catalogs are unaffected
     * and FE startup / follower replay can never be blocked by it.
     *
     * <p>The rule cannot be turned off:
     * <ul>
     *   <li>any {@code ..} path-traversal segment is rejected, for {@code file://} and {@code http(s)} alike;</li>
     *   <li>a scheme-less driver_url must be a bare jar file name matching {@code [A-Za-z0-9._-]+.jar}
     *       (no directories, no special characters), which is then resolved under {@code jdbc_drivers_dir}.</li>
     * </ul>
     * Whether a remote/absolute URL is allowed at all remains governed by the fe.conf-only
     * {@code jdbc_driver_secure_path} / {@code jdbc_driver_url_white_list} configs; this rule only
     * forbids traversal and enforces the bare-name charset.
     *
     * <p>Throws {@link IllegalArgumentException} so the engine wraps it into a {@code DdlException}
     * (and, on ALTER, triggers the property rollback).
     */
    public static void checkDriverUrlSecurityRule(String driverUrl) {
        if (driverUrl == null || driverUrl.isEmpty()) {
            return;
        }
        // Check traversal on the decoded path so percent-encoded segments (e.g. %2e%2e) — which the
        // driver-loading consumers decode — cannot slip a ".." past this rule.
        String pathToCheck = driverUrl;
        if (driverUrl.contains("://")) {
            try {
                String decoded = new URI(driverUrl).getPath();
                if (decoded != null) {
                    pathToCheck = decoded;
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid driver_url: " + driverUrl);
            }
        }
        String probe = pathToCheck.replace('\\', '/');
        for (String segment : probe.split("/")) {
            if ("..".equals(segment)) {
                throw new IllegalArgumentException(
                        "Invalid driver_url: path traversal ('..') is not allowed: " + driverUrl);
            }
        }
        if (!driverUrl.contains("://")) {
            if (!SAFE_DRIVER_FILE_NAME.matcher(driverUrl).matches()) {
                throw new IllegalArgumentException(
                        "Invalid driver_url: a driver file name must match [A-Za-z0-9._-]+.jar (got: "
                                + driverUrl + ")");
            }
        }
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        // Returning a non-null provider routes jdbc writes through the unified plan-provider sink
        // path (PhysicalPlanTranslator.visitPhysicalConnectorTableSink). The provider builds the
        // TJdbcTableSink itself (P6.3-T02 / OQ-1); there is no config-bag path anymore.
        return new JdbcWritePlanProvider(getOrCreateClient(), props);
    }

    @Override
    public void preCreateValidation(ConnectorValidationContext context) throws Exception {
        // 1. Validate/resolve JDBC driver — format, whitelist, secure_path, file existence.
        String driverUrl = props.getDriverUrl();
        if (driverUrl != null && !driverUrl.isEmpty()) {
            // Mandatory, non-configurable security rule, enforced on catalog creation only.
            checkDriverUrlSecurityRule(driverUrl);
            context.validateAndResolveDriverPath(driverUrl);

            // 2. Compute and verify checksum.
            String computedChecksum = context.computeDriverChecksum(driverUrl);
            String providedChecksum = context.getProperty(JdbcCatalogProperties.DRIVER_CHECKSUM);
            if (providedChecksum != null && !providedChecksum.isEmpty()) {
                if (!providedChecksum.equals(computedChecksum)) {
                    throw new DorisConnectorException(
                            "The provided checksum (" + providedChecksum
                                    + ") does not match the computed checksum (" + computedChecksum
                                    + ") for the driver_url.");
                }
            } else {
                context.storeProperty(JdbcCatalogProperties.DRIVER_CHECKSUM, computedChecksum);
            }
        }

        // 3. Test BE→JDBC connectivity via BRPC (only when test_connection is enabled).
        // The connector builds the serialized payload; the engine sends it after validation.
        boolean testConnection = Boolean.parseBoolean(
                props.getRaw().getOrDefault(JdbcCatalogProperties.TEST_CONNECTION, "true"));
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
        String jdbcUrl = props.getJdbcUrl();
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new DorisConnectorException("JDBC URL ('" + JdbcCatalogProperties.JDBC_URL + "') is required");
        }
        JdbcDbType dbType = JdbcDbType.parseFromUrl(jdbcUrl);
        String user = props.getUser();
        String password = props.getPassword();
        String driverUrl = resolveDriverUrl(props.getDriverUrl());
        String driverClass = props.getDriverClass();
        int poolMinSize = props.getConnectionPoolMinSize();
        int poolMaxSize = props.getConnectionPoolMaxSize();
        int poolMaxWaitTime = props.getConnectionPoolMaxWaitTime();
        int poolMaxLifeTime = props.getConnectionPoolMaxLifeTime();
        boolean onlySpecifiedDatabase = props.isOnlySpecifiedDatabase();
        boolean enableMappingVarbinary = props.isEnableMappingVarbinary();
        boolean enableMappingTimestampTz = props.isEnableMappingTimestampTz();

        LOG.info("Creating JDBC connector client for dbType={}, url={}", dbType, jdbcUrl);
        return JdbcConnectorClient.create(
                dbType, context.getCatalogName(), jdbcUrl, user, password,
                driverUrl, driverClass,
                poolMinSize, poolMaxSize, poolMaxWaitTime, poolMaxLifeTime,
                onlySpecifiedDatabase, props.getRaw(),
                enableMappingVarbinary, enableMappingTimestampTz,
                context::sanitizeOutboundUrl);
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
     * Resolves driver URL against the configured drivers directory.
     * If the URL is a plain filename (e.g., "mysql-connector-j-8.4.0.jar"),
     * resolves it under {@code drivers_dir} from this plugin's jdbc.conf, or fe.conf's
     * {@code jdbc_drivers_dir}.
     */
    private String resolveDriverUrl(String driverUrl) {
        if (driverUrl == null || driverUrl.isEmpty()) {
            return driverUrl;
        }
        if (driverUrl.startsWith("file://") || driverUrl.startsWith("http://")
                || driverUrl.startsWith("https://") || driverUrl.startsWith("/")) {
            return driverUrl;
        }
        // Plain filename — resolve under the configured drivers directory. doris_home is engine-wide
        // rather than this connector's setting, so it keeps coming from the engine environment.
        String driversDir = JdbcConf.driversDir(context);
        String dorisHome = JdbcConf.dorisHome(context);
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
        tJdbcTable.setJdbcUrl(props.getJdbcUrl());
        tJdbcTable.setJdbcUser(props.getUser());
        tJdbcTable.setJdbcPassword(props.getPassword());
        tJdbcTable.setJdbcTableName("test_jdbc_connection");
        tJdbcTable.setJdbcDriverClass(
                props.getDriverClass());
        tJdbcTable.setJdbcDriverUrl(
                props.getDriverUrl());
        tJdbcTable.setJdbcResourceName("");
        // Use the checksum that was computed/verified during driver validation.
        String checksum = context.getProperty(JdbcCatalogProperties.DRIVER_CHECKSUM);
        tJdbcTable.setJdbcDriverChecksum(checksum != null ? checksum : "");
        tJdbcTable.setConnectionPoolMinSize(props.getConnectionPoolMinSize());
        tJdbcTable.setConnectionPoolMaxSize(props.getConnectionPoolMaxSize());
        tJdbcTable.setConnectionPoolMaxWaitTime(props.getConnectionPoolMaxWaitTime());
        tJdbcTable.setConnectionPoolMaxLifeTime(props.getConnectionPoolMaxLifeTime());
        tJdbcTable.setConnectionPoolKeepAlive(props.isConnectionPoolKeepAlive());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(
                0, TTableType.JDBC_TABLE, 0, 0, "test_jdbc_connection", "");
        tTableDescriptor.setJdbcTable(tJdbcTable);
        return tTableDescriptor;
    }

    private TOdbcTableType parseOdbcType() {
        String jdbcUrl = props.getJdbcUrl();
        JdbcDbType dbType = JdbcDbType.parseFromUrl(jdbcUrl);
        try {
            return TOdbcTableType.valueOf(dbType.name());
        } catch (Exception e) {
            return TOdbcTableType.MYSQL;
        }
    }

    private String getTestQuery() {
        String jdbcUrl = props.getJdbcUrl();
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
