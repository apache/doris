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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.jdbc.JdbcIdentifierMapping;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

@Getter
public abstract class JdbcClient {
    private static final Logger LOG = LogManager.getLogger(JdbcClient.class);
    private static final int HTTP_TIMEOUT_MS = 10000;
    protected static final int JDBC_DATETIME_SCALE = 6;

    private String catalogName;
    protected String dbType;
    protected String jdbcUser;
    protected ClassLoader classLoader = null;
    protected HikariDataSource dataSource = null;
    protected boolean isOnlySpecifiedDatabase;
    protected boolean isLowerCaseMetaNames;
    protected String metaNamesMapping;
    protected Map<String, Boolean> includeDatabaseMap;
    protected Map<String, Boolean> excludeDatabaseMap;
    protected JdbcIdentifierMapping jdbcLowerCaseMetaMatching;

    public static JdbcClient createJdbcClient(JdbcClientConfig jdbcClientConfig) {
        String dbType = parseDbType(jdbcClientConfig.getJdbcUrl());
        switch (dbType) {
            case JdbcResource.MYSQL:
                return new JdbcMySQLClient(jdbcClientConfig);
            case JdbcResource.OCEANBASE:
                JdbcOceanBaseClient jdbcOceanBaseClient = new JdbcOceanBaseClient(jdbcClientConfig);
                return jdbcOceanBaseClient.createClient(jdbcClientConfig);
            case JdbcResource.POSTGRESQL:
                return new JdbcPostgreSQLClient(jdbcClientConfig);
            case JdbcResource.ORACLE:
                return new JdbcOracleClient(jdbcClientConfig);
            case JdbcResource.SQLSERVER:
                return new JdbcSQLServerClient(jdbcClientConfig);
            case JdbcResource.CLICKHOUSE:
                return new JdbcClickHouseClient(jdbcClientConfig);
            case JdbcResource.SAP_HANA:
                return new JdbcSapHanaClient(jdbcClientConfig);
            case JdbcResource.TRINO:
            case JdbcResource.PRESTO:
                return new JdbcTrinoClient(jdbcClientConfig);
            case JdbcResource.DB2:
                return new JdbcDB2Client(jdbcClientConfig);
            default:
                throw new IllegalArgumentException("Unsupported DB type: " + dbType);
        }
    }

    protected JdbcClient(JdbcClientConfig jdbcClientConfig) {
        this.catalogName = jdbcClientConfig.getCatalog();
        this.jdbcUser = jdbcClientConfig.getUser();
        this.isOnlySpecifiedDatabase = Boolean.parseBoolean(jdbcClientConfig.getOnlySpecifiedDatabase());
        this.isLowerCaseMetaNames = Boolean.parseBoolean(jdbcClientConfig.getIsLowerCaseMetaNames());
        this.metaNamesMapping = jdbcClientConfig.getMetaNamesMapping();
        this.includeDatabaseMap =
                Optional.ofNullable(jdbcClientConfig.getIncludeDatabaseMap()).orElse(Collections.emptyMap());
        this.excludeDatabaseMap =
                Optional.ofNullable(jdbcClientConfig.getExcludeDatabaseMap()).orElse(Collections.emptyMap());
        String jdbcUrl = jdbcClientConfig.getJdbcUrl();
        this.dbType = parseDbType(jdbcUrl);
        initializeClassLoader(jdbcClientConfig);
        initializeDataSource(jdbcClientConfig);
        this.jdbcLowerCaseMetaMatching = new JdbcIdentifierMapping(isLowerCaseMetaNames, metaNamesMapping, this);
    }

    // Initialize DataSource
    private void initializeDataSource(JdbcClientConfig config) {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(this.classLoader);
            dataSource = new HikariDataSource();
            dataSource.setDriverClassName(config.getDriverClass());
            dataSource.setJdbcUrl(SecurityChecker.getInstance().getSafeJdbcUrl(config.getJdbcUrl()));
            dataSource.setUsername(config.getUser());
            dataSource.setPassword(config.getPassword());
            dataSource.setMinimumIdle(config.getConnectionPoolMinSize()); // default 1
            dataSource.setMaximumPoolSize(config.getConnectionPoolMaxSize()); // default 10
            // set connection timeout to 5s.
            // The default is 30s, which is too long.
            // Because when querying information_schema db, BE will call thrift rpc(default timeout is 30s)
            // to FE to get schema info, and may create connection here, if we set it too long and the url is invalid,
            // it may cause the thrift rpc timeout.
            dataSource.setConnectionTimeout(config.getConnectionPoolMaxWaitTime()); // default 5000
            dataSource.setMaxLifetime(config.getConnectionPoolMaxLifeTime()); // default 30 min
            dataSource.setIdleTimeout(config.getConnectionPoolMaxLifeTime() / 2L); // default 15 min
            LOG.info("JdbcClient set"
                    + " ConnectionPoolMinSize = " + config.getConnectionPoolMinSize()
                    + ", ConnectionPoolMaxSize = " + config.getConnectionPoolMaxSize()
                    + ", ConnectionPoolMaxWaitTime = " + config.getConnectionPoolMaxWaitTime()
                    + ", ConnectionPoolMaxLifeTime = " + config.getConnectionPoolMaxLifeTime());
        } catch (Exception e) {
            throw new JdbcClientException(e.getMessage());
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    private void initializeClassLoader(JdbcClientConfig config) {
        try {
            URL[] urls = {new URL(JdbcResource.getFullDriverUrl(config.getDriverUrl()))};
            ClassLoader parent = getClass().getClassLoader();
            this.classLoader = URLClassLoader.newInstance(urls, parent);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Error loading JDBC driver.", e);
        }
    }

    public static String parseDbType(String jdbcUrl) {
        try {
            return JdbcResource.parseDbType(jdbcUrl);
        } catch (DdlException e) {
            throw new JdbcClientException("Failed to parse db type from jdbcUrl: " + jdbcUrl, e);
        }
    }

    public void closeClient() {
        dataSource.close();
    }

    public Connection getConnection() throws JdbcClientException {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        Connection conn;
        try {
            Thread.currentThread().setContextClassLoader(this.classLoader);
            conn = dataSource.getConnection();
        } catch (Exception e) {
            String errorMessage = String.format("Can not connect to jdbc due to error: %s, Catalog name: %s",
                    e.getMessage(), this.getCatalogName());
            throw new JdbcClientException(errorMessage, e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
        return conn;
    }

    public void close(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    throw new JdbcClientException("Can not close : ", e);
                }
            }
        }
    }

    /**
     * Execute stmt direct via jdbc
     *
     * @param origStmt, the raw stmt string
     */
    public void executeStmt(String origStmt) {
        Connection conn = getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            int effectedRows = stmt.executeUpdate(origStmt);
            if (LOG.isDebugEnabled()) {
                LOG.debug("finished to execute dml stmt: {}, effected rows: {}", origStmt, effectedRows);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to execute stmt. error: " + e.getMessage(), e);
        } finally {
            close(stmt, conn);
        }
    }

    /**
     * Execute query via jdbc
     *
     * @param query, the query string
     * @return List<Column>
     */
    public List<Column> getColumnsFromQuery(String query) {
        Connection conn = getConnection();
        List<Column> columns = Lists.newArrayList();
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSetMetaData metaData = pstmt.getMetaData();
            if (metaData == null) {
                throw new JdbcClientException("Query not supported: Failed to get ResultSetMetaData from query: %s",
                        query);
            } else {
                List<JdbcFieldSchema> schemas = getSchemaFromResultSetMetaData(metaData);
                for (JdbcFieldSchema schema : schemas) {
                    columns.add(new Column(schema.getColumnName(), jdbcTypeToDoris(schema), true, null, true, null,
                            true, -1));
                }
            }
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to get columns from query: %s", e, query);
        } finally {
            close(conn);
        }
        return columns;
    }


    /**
     * Get schema from ResultSetMetaData
     *
     * @param metaData, the ResultSetMetaData
     * @return List<JdbcFieldSchema>
     */
    public List<JdbcFieldSchema> getSchemaFromResultSetMetaData(ResultSetMetaData metaData) throws SQLException {
        List<JdbcFieldSchema> schemas = Lists.newArrayList();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            schemas.add(new JdbcFieldSchema(metaData, i));
        }
        return schemas;
    }

    // This part used to process meta-information of database, table and column.

    /**
     * get all database name through JDBC
     *
     * @return list of database names
     */
    public List<String> getDatabaseNameList() {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<String> remoteDatabaseNames = Lists.newArrayList();
        try {
            if (isOnlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                String currentDatabase = conn.getSchema();
                remoteDatabaseNames.add(currentDatabase);
            } else {
                rs = conn.getMetaData().getSchemas(conn.getCatalog(), null);
                while (rs.next()) {
                    remoteDatabaseNames.add(rs.getString("TABLE_SCHEM"));
                }
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get database name list from jdbc", e);
        } finally {
            close(rs, conn);
        }
        return filterDatabaseNames(remoteDatabaseNames);
    }

    /**
     * get all tables of one database
     */
    public List<String> getTablesNameList(String localDbName) {
        List<String> remoteTablesNames = Lists.newArrayList();
        String[] tableTypes = getTableTypes();
        String remoteDbName = getRemoteDatabaseName(localDbName);
        processTable(remoteDbName, null, tableTypes, (rs) -> {
            try {
                while (rs.next()) {
                    remoteTablesNames.add(rs.getString("TABLE_NAME"));
                }
            } catch (SQLException e) {
                throw new JdbcClientException("failed to get all tables for remote database: `%s`", e, remoteDbName);
            }
        });
        return filterTableNames(remoteDbName, remoteTablesNames);
    }

    public boolean isTableExist(String localDbName, String localTableName) {
        final boolean[] isExist = {false};
        String[] tableTypes = getTableTypes();
        String remoteDbName = getRemoteDatabaseName(localDbName);
        String remoteTableName = getRemoteTableName(localDbName, localTableName);
        processTable(remoteDbName, remoteTableName, tableTypes, (rs) -> {
            try {
                if (rs.next()) {
                    isExist[0] = true;
                }
            } catch (SQLException e) {
                throw new JdbcClientException("failed to judge if table exist for table %s in db %s",
                        e, remoteTableName, remoteDbName);
            }
        });
        return isExist[0];
    }

    /**
     * get all columns of one table
     */
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String localDbName, String localTableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = Lists.newArrayList();
        String remoteDbName = getRemoteDatabaseName(localDbName);
        String remoteTableName = getRemoteTableName(localDbName, localTableName);
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            rs = getRemoteColumns(databaseMetaData, catalogName, remoteDbName, remoteTableName);
            while (rs.next()) {
                tableSchema.add(new JdbcFieldSchema(rs));
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get jdbc columns info for remote table `%s.%s`: %s",
                    remoteDbName, remoteTableName, Util.getRootCauseMessage(e));
        } finally {
            close(rs, conn);
        }
        return tableSchema;
    }

    public List<Column> getColumnsFromJdbc(String localDbName, String localTableName) {
        List<JdbcFieldSchema> jdbcTableSchema = getJdbcColumnsInfo(localDbName, localTableName);
        List<Column> dorisTableSchema = Lists.newArrayListWithCapacity(jdbcTableSchema.size());
        for (JdbcFieldSchema field : jdbcTableSchema) {
            dorisTableSchema.add(new Column(field.getColumnName(),
                    jdbcTypeToDoris(field), true, null,
                    field.isAllowNull(), field.getRemarks(),
                    true, -1));
        }
        String remoteDbName = getRemoteDatabaseName(localDbName);
        String remoteTableName = getRemoteTableName(localDbName, localTableName);
        return filterColumnName(remoteDbName, remoteTableName, dorisTableSchema);
    }

    public String getRemoteDatabaseName(String localDbname) {
        return jdbcLowerCaseMetaMatching.getRemoteDatabaseName(localDbname);
    }

    public String getRemoteTableName(String localDbName, String localTableName) {
        return jdbcLowerCaseMetaMatching.getRemoteTableName(localDbName, localTableName);
    }

    public Map<String, String> getRemoteColumnNames(String localDbName, String localTableName) {
        return jdbcLowerCaseMetaMatching.getRemoteColumnNames(localDbName, localTableName);
    }

    // protected methods,for subclass to override
    protected String getCatalogName(Connection conn) throws SQLException {
        return conn.getCatalog();
    }

    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW"};
    }

    protected void processTable(String remoteDbName, String remoteTableName, String[] tableTypes,
            Consumer<ResultSet> resultSetConsumer) {
        Connection conn = getConnection();
        ResultSet rs = null;
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            rs = databaseMetaData.getTables(catalogName, remoteDbName, remoteTableName, tableTypes);
            resultSetConsumer.accept(rs);
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to process table", e);
        } finally {
            close(rs, conn);
        }
    }

    protected String modifyTableNameIfNecessary(String remoteTableName) {
        return remoteTableName;
    }

    protected boolean isTableModified(String modifiedTableName, String actualTableName) {
        return false;
    }

    protected ResultSet getRemoteColumns(DatabaseMetaData databaseMetaData, String catalogName, String remoteDbName,
            String remoteTableName) throws SQLException {
        return databaseMetaData.getColumns(catalogName, remoteDbName, remoteTableName, null);
    }

    protected List<String> filterDatabaseNames(List<String> remoteDbNames) {
        Set<String> filterInternalDatabases = getFilterInternalDatabases();
        List<String> filteredDatabaseNames = Lists.newArrayList();
        for (String databaseName : remoteDbNames) {
            if (isOnlySpecifiedDatabase) {
                if (!excludeDatabaseMap.isEmpty() && excludeDatabaseMap.containsKey(databaseName)) {
                    continue;
                }
                if (!includeDatabaseMap.isEmpty() && !includeDatabaseMap.containsKey(databaseName)) {
                    continue;
                }
            }
            if (filterInternalDatabases.contains(databaseName.toLowerCase())) {
                continue;
            }
            filteredDatabaseNames.add(databaseName);
        }
        return jdbcLowerCaseMetaMatching.setDatabaseNameMapping(filteredDatabaseNames);
    }

    protected Set<String> getFilterInternalDatabases() {
        return ImmutableSet.<String>builder()
                .add("information_schema")
                .add("performance_schema")
                .add("mysql")
                .build();
    }

    protected List<String> filterTableNames(String remoteDbName, List<String> remoteTableNames) {
        return jdbcLowerCaseMetaMatching.setTableNameMapping(remoteDbName, remoteTableNames);
    }

    protected List<Column> filterColumnName(String remoteDbName, String remoteTableName, List<Column> remoteColumns) {
        return jdbcLowerCaseMetaMatching.setColumnNameMapping(remoteDbName, remoteTableName, remoteColumns);
    }

    protected abstract Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema);

    protected Type createDecimalOrStringType(int precision, int scale) {
        if (precision <= ScalarType.MAX_DECIMAL128_PRECISION && precision > 0) {
            return ScalarType.createDecimalV3Type(precision, scale);
        }
        return ScalarType.createStringType();
    }

    public void testConnection() {
        String testQuery = getTestQuery();
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(testQuery)) {
            if (!rs.next()) {
                throw new JdbcClientException(
                        "Failed to test connection in FE: query executed but returned no results.");
            }
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to test connection in FE: " + e.getMessage(), e);
        }
    }

    public String getTestQuery() {
        return "select 1";
    }
}
