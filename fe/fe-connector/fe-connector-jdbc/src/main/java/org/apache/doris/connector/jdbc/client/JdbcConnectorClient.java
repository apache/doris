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

package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.jdbc.JdbcDbType;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Base JDBC metadata client. Mirrors the structure of fe-core's {@code JdbcClient}
 * but returns {@link ConnectorType} instead of Doris {@code Type}.
 *
 * <p>Subclasses override {@link #jdbcTypeToConnectorType} and optionally
 * metadata-discovery methods for database-specific behaviour.</p>
 */
public abstract class JdbcConnectorClient implements Closeable {

    static {
        // HikariCP reads this once at class-load time to decide whether ConcurrentBag
        // should use WeakReferences for connection tracking. Set it early and globally.
        System.setProperty("com.zaxxer.hikari.useWeakReferences", "true");
    }

    private static final Logger LOG = LogManager.getLogger(JdbcConnectorClient.class);
    protected static final int JDBC_DATETIME_SCALE = 6;
    protected static final int MAX_DECIMAL128_PRECISION = 38;

    private static final Map<URL, RefCountedClassLoader> CLASS_LOADER_MAP = new ConcurrentHashMap<>();

    /**
     * Pairs a ClassLoader with a reference count so the map entry can be removed
     * when the last client using that driver URL is closed.
     */
    static final class RefCountedClassLoader {
        final ClassLoader loader;
        final AtomicInteger refCount = new AtomicInteger(1);

        RefCountedClassLoader(ClassLoader loader) {
            this.loader = loader;
        }
    }

    protected final String catalogName;
    protected final JdbcDbType dbType;
    protected final String jdbcUrl;
    protected final boolean onlySpecifiedDatabase;
    protected final Map<String, Boolean> includeDatabaseMap;
    protected final Map<String, Boolean> excludeDatabaseMap;
    protected final boolean enableMappingVarbinary;
    protected final boolean enableMappingTimestampTz;
    protected ClassLoader classLoader;
    private URL classLoaderUrl;
    protected HikariDataSource dataSource;

    /**
     * Factory method to create the correct client subclass for the given DB type.
     *
     * @param urlSanitizer engine-level JDBC URL sanitizer (e.g., SSRF protection).
     *                     Applied before passing the URL to HikariCP. Pass
     *                     {@link UnaryOperator#identity()} if no sanitization is needed.
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public static JdbcConnectorClient create(
            JdbcDbType dbType, String catalogName,
            String jdbcUrl, String user, String password,
            String driverUrl, String driverClass,
            int poolMinSize, int poolMaxSize,
            int poolMaxWaitTime, int poolMaxLifeTime,
            boolean onlySpecifiedDatabase,
            Map<String, String> allProperties,
            boolean enableMappingVarbinary,
            boolean enableMappingTimestampTz,
            UnaryOperator<String> urlSanitizer) {
        Map<String, Boolean> includeMap = parseDatabaseMap(
                allProperties.get("include_database_list"));
        Map<String, Boolean> excludeMap = parseDatabaseMap(
                allProperties.get("exclude_database_list"));

        JdbcConnectorClient client;
        switch (dbType) {
            case MYSQL:
                client = new JdbcMySQLConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case POSTGRESQL:
                client = new JdbcPostgreSQLConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case ORACLE:
                client = new JdbcOracleConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case CLICKHOUSE:
                client = new JdbcClickHouseConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case SQLSERVER:
                client = new JdbcSQLServerConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case SAP_HANA:
                client = new JdbcSapHanaConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case TRINO:
            case PRESTO:
                client = new JdbcTrinoConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case OCEANBASE:
                client = new JdbcOceanBaseConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case DB2:
                client = new JdbcDB2ConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            case GBASE:
                client = new JdbcGbaseConnectorClient(
                        catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                        includeMap, excludeMap, enableMappingVarbinary,
                        enableMappingTimestampTz);
                break;
            default:
                throw new DorisConnectorException("Unsupported JDBC DB type: " + dbType);
        }
        client.initializeClassLoader(driverUrl);
        String sanitizedUrl = urlSanitizer.apply(jdbcUrl);
        client.initializeDataSource(sanitizedUrl, user, password, driverClass,
                poolMinSize, poolMaxSize, poolMaxWaitTime, poolMaxLifeTime);
        client.postInitialize();
        return client;
    }

    protected JdbcConnectorClient(
            String catalogName, JdbcDbType dbType, String jdbcUrl,
            boolean onlySpecifiedDatabase,
            Map<String, Boolean> includeDatabaseMap,
            Map<String, Boolean> excludeDatabaseMap,
            boolean enableMappingVarbinary,
            boolean enableMappingTimestampTz) {
        this.catalogName = catalogName;
        this.dbType = dbType;
        this.jdbcUrl = jdbcUrl;
        this.onlySpecifiedDatabase = onlySpecifiedDatabase;
        this.includeDatabaseMap = includeDatabaseMap != null ? includeDatabaseMap : Collections.emptyMap();
        this.excludeDatabaseMap = excludeDatabaseMap != null ? excludeDatabaseMap : Collections.emptyMap();
        this.enableMappingVarbinary = enableMappingVarbinary;
        this.enableMappingTimestampTz = enableMappingTimestampTz;
    }

    // -- lifecycle --

    /**
     * Hook called after class loader and data source are fully initialized.
     * Subclasses can override to perform additional setup that requires a live connection.
     */
    protected void postInitialize() {
    }

    private void initializeDataSource(String url, String user, String password,
            String driverClass, int poolMin, int poolMax, int maxWait, int maxLife) {
        ClassLoader old = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(this.classLoader);
            dataSource = new HikariDataSource();
            dataSource.setDriverClassName(driverClass);
            dataSource.setJdbcUrl(url);
            dataSource.setUsername(user);
            dataSource.setPassword(password);
            dataSource.setMinimumIdle(poolMin);
            dataSource.setMaximumPoolSize(poolMax);
            dataSource.setConnectionTimeout(maxWait);
            dataSource.setMaxLifetime(maxLife);
            dataSource.setIdleTimeout(maxLife / 2L);
            dataSource.setConnectionTestQuery(getTestQuery());
            LOG.info("JdbcConnectorClient set PoolMin={}, PoolMax={}, MaxWait={}, MaxLife={}",
                    poolMin, poolMax, maxWait, maxLife);
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to initialize JDBC data source: " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(old);
        }
    }

    private synchronized void initializeClassLoader(String driverUrl) {
        if (driverUrl == null || driverUrl.isEmpty()) {
            this.classLoader = getClass().getClassLoader();
            return;
        }
        try {
            URL[] urls = {new URL(resolveDriverUrl(driverUrl))};
            this.classLoaderUrl = urls[0];
            RefCountedClassLoader entry = CLASS_LOADER_MAP.compute(urls[0], (key, existing) -> {
                if (existing != null) {
                    existing.refCount.incrementAndGet();
                    return existing;
                }
                ClassLoader parent = getClass().getClassLoader();
                return new RefCountedClassLoader(URLClassLoader.newInstance(urls, parent));
            });
            this.classLoader = entry.loader;
        } catch (MalformedURLException e) {
            throw new DorisConnectorException(
                    "Failed to load JDBC driver from path: " + driverUrl, e);
        }
    }

    private static String resolveDriverUrl(String driverUrl) {
        if (driverUrl.startsWith("file://") || driverUrl.startsWith("http://")
                || driverUrl.startsWith("https://")) {
            return driverUrl;
        }
        return "file://" + driverUrl;
    }

    @Override
    public void close() {
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
        releaseClassLoader();
    }

    private void releaseClassLoader() {
        if (classLoaderUrl == null) {
            return;
        }
        CLASS_LOADER_MAP.computeIfPresent(classLoaderUrl, (key, entry) -> {
            int remaining = entry.refCount.decrementAndGet();
            return remaining <= 0 ? null : entry;
        });
        classLoaderUrl = null;
    }

    // Visible for testing
    static int classLoaderCacheSize() {
        return CLASS_LOADER_MAP.size();
    }

    public JdbcDbType getDbType() {
        return dbType;
    }

    // -- connection helpers --

    public Connection getConnection() {
        ClassLoader old = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(this.classLoader);
            return dataSource.getConnection();
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Catalog '" + catalogName + "' cannot connect to JDBC: "
                    + getAllExceptionMessages(e), e);
        } finally {
            Thread.currentThread().setContextClassLoader(old);
        }
    }

    protected void closeResources(Object... resources) {
        for (Object r : resources) {
            if (r != null) {
                try {
                    if (r instanceof ResultSet) {
                        ((ResultSet) r).close();
                    } else if (r instanceof Statement) {
                        ((Statement) r).close();
                    } else if (r instanceof Connection) {
                        ((Connection) r).close();
                    }
                } catch (SQLException e) {
                    LOG.warn("Failed to close resource: {}", e.getMessage(), e);
                }
            }
        }
    }

    protected String getTestQuery() {
        return "select 1";
    }

    // -- metadata discovery (overridable) --

    public List<String> getDatabaseNameList() {
        Connection conn = null;
        ResultSet rs = null;
        List<String> names = new ArrayList<>();
        try {
            conn = getConnection();
            if (onlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                String current = conn.getSchema();
                names.add(current);
            } else {
                rs = conn.getMetaData().getSchemas(conn.getCatalog(), null);
                while (rs.next()) {
                    names.add(rs.getString("TABLE_SCHEM"));
                }
            }
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to get database name list from JDBC", e);
        } finally {
            closeResources(rs, conn);
        }
        return filterDatabaseNames(names);
    }

    public List<String> getTablesNameList(String remoteDbName) {
        List<String> names = new ArrayList<>();
        String[] tableTypes = getTableTypes();
        processTable(remoteDbName, null, tableTypes, rs -> {
            try {
                while (rs.next()) {
                    names.add(rs.getString("TABLE_NAME"));
                }
            } catch (SQLException e) {
                throw new DorisConnectorException(
                        "Failed to list tables for remote database: " + remoteDbName, e);
            }
        });
        return names;
    }

    public boolean isTableExist(String remoteDbName, String remoteTableName) {
        boolean[] exists = {false};
        String[] tableTypes = getTableTypes();
        processTable(remoteDbName, remoteTableName, tableTypes, rs -> {
            try {
                if (rs.next()) {
                    exists[0] = true;
                }
            } catch (SQLException e) {
                throw new DorisConnectorException(
                        "Failed to check table existence: " + remoteDbName + "." + remoteTableName, e);
            }
        });
        return exists[0];
    }

    public List<JdbcFieldInfo> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
        Connection conn = null;
        ResultSet rs = null;
        List<JdbcFieldInfo> schema = new ArrayList<>();
        try {
            conn = getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String cat = getCatalogName(conn);
            rs = getRemoteColumns(databaseMetaData, cat, remoteDbName, remoteTableName);
            while (rs.next()) {
                schema.add(new JdbcFieldInfo(rs));
            }
        } catch (SQLException e) {
            throw new DorisConnectorException(
                    "Failed to get JDBC columns info for " + remoteDbName + "." + remoteTableName
                    + ": " + getRootCauseMessage(e), e);
        } finally {
            closeResources(rs, conn);
        }
        return schema;
    }

    /**
     * Get primary keys of one table.
     */
    public List<String> getPrimaryKeys(String remoteDbName, String remoteTableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<String> primaryKeys = new ArrayList<>();
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String cat = getCatalogName(conn);
            rs = databaseMetaData.getPrimaryKeys(cat, remoteDbName, remoteTableName);
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            throw new DorisConnectorException(
                    "Failed to get primary keys for " + remoteDbName + "." + remoteTableName
                    + ": " + getRootCauseMessage(e), e);
        } finally {
            closeResources(rs, conn);
        }
        return primaryKeys;
    }

    /**
     * Get table comment. Base implementation returns empty string.
     * Subclasses (e.g. MySQL) override to query INFORMATION_SCHEMA or DatabaseMetaData.
     */
    public String getTableComment(String remoteDbName, String remoteTableName) {
        return "";
    }

    /**
     * Returns the estimated row count for a table from database system catalogs.
     * Returns -1 if unavailable.
     */
    public long getRowCount(String dbName, String tableName) {
        return -1;
    }

    /**
     * Execute a DML statement (INSERT/UPDATE/DELETE) via JDBC.
     */
    public void executeStmt(String origStmt) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            int effectedRows = stmt.executeUpdate(origStmt);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished executing DML stmt: {}, effected rows: {}", origStmt, effectedRows);
            }
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to execute stmt: " + e.getMessage(), e);
        } finally {
            closeResources(stmt, conn);
        }
    }

    /**
     * Get column metadata from a query by preparing it and reading ResultSetMetaData.
     */
    public List<JdbcFieldInfo> getColumnsFromQuery(String query) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        List<JdbcFieldInfo> columns = new ArrayList<>();
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(query);
            ResultSetMetaData metaData = pstmt.getMetaData();
            if (metaData == null) {
                throw new DorisConnectorException(
                        "Query not supported: failed to get ResultSetMetaData from query: " + query);
            }
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                columns.add(new JdbcFieldInfo(metaData, i));
            }
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to get columns from query: " + query, e);
        } finally {
            closeResources(pstmt, conn);
        }
        return columns;
    }

    /**
     * Get the JDBC driver version.
     */
    public String getJdbcDriverVersion() {
        Connection conn = null;
        try {
            conn = getConnection();
            return conn.getMetaData().getDriverVersion();
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to get JDBC driver version", e);
        } finally {
            closeResources(conn);
        }
    }

    // -- abstract type mapping --

    public abstract ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo);

    // -- overridable hooks --

    protected String getCatalogName(Connection conn) throws SQLException {
        return conn.getCatalog();
    }

    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW"};
    }

    protected void processTable(String remoteDbName, String remoteTableName,
            String[] tableTypes, Consumer<ResultSet> consumer) {
        Connection conn = null;
        ResultSet standardRs = null;
        Statement stmt = null;
        ResultSet customRs = null;
        try {
            conn = getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String cat = getCatalogName(conn);
            String escapedTableName = escapeSearchPattern(databaseMetaData, remoteTableName);
            standardRs = databaseMetaData.getTables(cat, remoteDbName, escapedTableName, tableTypes);
            consumer.accept(standardRs);
            String additionalQuery = getAdditionalTablesQuery(remoteDbName, remoteTableName, tableTypes);
            if (additionalQuery != null && !additionalQuery.trim().isEmpty()) {
                stmt = conn.createStatement();
                customRs = stmt.executeQuery(additionalQuery);
                consumer.accept(customRs);
            }
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to process table", e);
        } finally {
            closeResources(customRs, stmt, standardRs, conn);
        }
    }

    protected ResultSet getRemoteColumns(DatabaseMetaData meta, String catalog,
            String remoteDbName, String remoteTableName) throws SQLException {
        String escapedTableName = escapeSearchPattern(meta, remoteTableName);
        return meta.getColumns(catalog, remoteDbName, escapedTableName, null);
    }

    /**
     * Escapes the driver-specific search string escape character in a name
     * before passing it to DatabaseMetaData methods like getTables() or
     * getColumns(), which interpret the name as a pattern.
     *
     * Only the escape character itself is escaped (e.g. Oracle's '/' becomes '//').
     * Wildcards '%' and '_' are intentionally NOT escaped because:
     * 1. They rarely appear in real table/column names;
     * 2. Some older drivers (e.g. ojdbc6) do not support escape sequences
     *    in getTables(), so escaping '_' would break lookup for names like
     *    TEST_ALL_TYPES.
     */
    protected static String escapeSearchPattern(DatabaseMetaData meta, String name) {
        if (name == null) {
            return null;
        }
        try {
            String escape = meta.getSearchStringEscape();
            if (escape == null || escape.isEmpty()) {
                return name;
            }
            StringBuilder sb = new StringBuilder(name.length() + 4);
            for (int i = 0; i < name.length(); i++) {
                char ch = name.charAt(i);
                if (escape.indexOf(ch) >= 0) {
                    sb.append(escape);
                }
                sb.append(ch);
            }
            return sb.toString();
        } catch (SQLException e) {
            return name;
        }
    }

    protected String getAdditionalTablesQuery(String remoteDbName, String remoteTableName, String[] tableTypes) {
        return null;
    }

    protected Set<String> getFilterInternalDatabases() {
        Set<String> set = new HashSet<>();
        set.add("information_schema");
        set.add("performance_schema");
        set.add("mysql");
        return set;
    }

    protected List<String> filterDatabaseNames(List<String> names) {
        Set<String> internal = getFilterInternalDatabases();
        List<String> result = new ArrayList<>();
        for (String name : names) {
            if (onlySpecifiedDatabase) {
                if (!excludeDatabaseMap.isEmpty() && excludeDatabaseMap.containsKey(name)) {
                    continue;
                }
                if (!includeDatabaseMap.isEmpty() && !includeDatabaseMap.containsKey(name)) {
                    continue;
                }
            }
            if (internal.contains(name.toLowerCase())) {
                continue;
            }
            result.add(name);
        }
        return result;
    }

    // -- type mapping helpers --

    protected ConnectorType createDecimalOrString(int precision, int scale) {
        if (precision > 0 && precision <= MAX_DECIMAL128_PRECISION) {
            return ConnectorType.of("DECIMALV3", precision, scale);
        }
        return ConnectorType.of("STRING");
    }

    // -- utility --

    private static Map<String, Boolean> parseDatabaseMap(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Boolean> map = new java.util.HashMap<>();
        for (String db : commaSeparated.split(",")) {
            String trimmed = db.trim();
            if (!trimmed.isEmpty()) {
                map.put(trimmed, true);
            }
        }
        return map;
    }

    private static String getAllExceptionMessages(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        while (throwable != null) {
            String msg = throwable.getMessage();
            if (msg != null && !msg.isEmpty()) {
                if (sb.length() > 0) {
                    sb.append(" | Caused by: ");
                }
                sb.append(msg);
            }
            throwable = throwable.getCause();
        }
        return sb.toString();
    }

    private static String getRootCauseMessage(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
