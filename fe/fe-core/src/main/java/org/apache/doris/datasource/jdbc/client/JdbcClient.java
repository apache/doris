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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.Util;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Getter
public abstract class JdbcClient {
    private static final Logger LOG = LogManager.getLogger(JdbcClient.class);
    private static final int HTTP_TIMEOUT_MS = 10000;
    protected static final int JDBC_DATETIME_SCALE = 6;

    private String catalog;
    protected String dbType;
    protected String jdbcUser;
    protected URLClassLoader classLoader = null;
    protected DruidDataSource dataSource = null;
    protected boolean isOnlySpecifiedDatabase;
    protected boolean isLowerCaseTableNames;
    protected String oceanbaseMode = "";

    protected Map<String, Boolean> includeDatabaseMap;
    protected Map<String, Boolean> excludeDatabaseMap;
    // only used when isLowerCaseTableNames = true.
    protected final ConcurrentHashMap<String, String> lowerDBToRealDB = new ConcurrentHashMap<>();
    // only used when isLowerCaseTableNames = true.
    protected final ConcurrentHashMap<String, String> lowerTableToRealTable = new ConcurrentHashMap<>();

    private final AtomicBoolean dbNamesLoaded = new AtomicBoolean(false);
    private final AtomicBoolean tableNamesLoaded = new AtomicBoolean(false);

    public static JdbcClient createJdbcClient(JdbcClientConfig jdbcClientConfig) {
        String dbType = parseDbType(jdbcClientConfig.getJdbcUrl());
        switch (dbType) {
            case JdbcResource.MYSQL:
                return new JdbcMySQLClient(jdbcClientConfig);
            case JdbcResource.OCEANBASE:
                return new JdbcOceanBaseClient(jdbcClientConfig);
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
            default:
                throw new IllegalArgumentException("Unsupported DB type: " + dbType);
        }
    }

    protected JdbcClient(JdbcClientConfig jdbcClientConfig) {
        this.catalog = jdbcClientConfig.getCatalog();
        this.jdbcUser = jdbcClientConfig.getUser();
        this.isOnlySpecifiedDatabase = Boolean.parseBoolean(jdbcClientConfig.getOnlySpecifiedDatabase());
        this.isLowerCaseTableNames = Boolean.parseBoolean(jdbcClientConfig.getIsLowerCaseTableNames());
        this.includeDatabaseMap =
                Optional.ofNullable(jdbcClientConfig.getIncludeDatabaseMap()).orElse(Collections.emptyMap());
        this.excludeDatabaseMap =
                Optional.ofNullable(jdbcClientConfig.getExcludeDatabaseMap()).orElse(Collections.emptyMap());
        String jdbcUrl = jdbcClientConfig.getJdbcUrl();
        this.dbType = parseDbType(jdbcUrl);
        initializeDataSource(jdbcClientConfig.getPassword(), jdbcUrl, jdbcClientConfig.getDriverUrl(),
                jdbcClientConfig.getDriverClass());
    }

    // Initialize DruidDataSource
    private void initializeDataSource(String password, String jdbcUrl, String driverUrl, String driverClass) {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            // TODO(ftw): The problem here is that the jar package is handled by FE
            //  and URLClassLoader may load the jar package directly into memory
            URL[] urls = {new URL(JdbcResource.getFullDriverUrl(driverUrl))};
            // set parent ClassLoader to null, we can achieve class loading isolation.
            ClassLoader parent = getClass().getClassLoader();
            ClassLoader classLoader = URLClassLoader.newInstance(urls, parent);
            LOG.debug("parent ClassLoader: {}, old ClassLoader: {}, class Loader: {}.",
                    parent, oldClassLoader, classLoader);
            Thread.currentThread().setContextClassLoader(classLoader);
            dataSource = new DruidDataSource();
            dataSource.setDriverClassLoader(classLoader);
            dataSource.setDriverClassName(driverClass);
            dataSource.setUrl(jdbcUrl);
            dataSource.setUsername(jdbcUser);
            dataSource.setPassword(password);
            dataSource.setMinIdle(1);
            dataSource.setInitialSize(1);
            dataSource.setMaxActive(100);
            dataSource.setTimeBetweenEvictionRunsMillis(600000);
            dataSource.setMinEvictableIdleTimeMillis(300000);
            // set connection timeout to 5s.
            // The default is 30s, which is too long.
            // Because when querying information_schema db, BE will call thrift rpc(default timeout is 30s)
            // to FE to get schema info, and may create connection here, if we set it too long and the url is invalid,
            // it may cause the thrift rpc timeout.
            dataSource.setMaxWait(5000);
        } catch (MalformedURLException e) {
            throw new JdbcClientException("MalformedURLException to load class about " + driverUrl, e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    private static String parseDbType(String jdbcUrl) {
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
        Connection conn;
        try {
            conn = dataSource.getConnection();
        } catch (Exception e) {
            String errorMessage = String.format("Can not connect to jdbc due to error: %s, Catalog name: %s", e,
                    this.getCatalog());
            throw new JdbcClientException(errorMessage, e);
        }
        return conn;
    }

    public void close(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception  e) {
                    throw new JdbcClientException("Can not close : ", e);
                }
            }
        }
    }

    // This part used to process meta-information of database, table and column.
    /**
     * get all database name through JDBC
     * @return list of database names
     */
    public List<String> getDatabaseNameList() {
        Connection conn = getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        if (isOnlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
            return getSpecifiedDatabase(conn);
        }
        List<String> databaseNames = Lists.newArrayList();
        try {
            stmt = conn.createStatement();
            String sql = getDatabaseQuery();
            rs = stmt.executeQuery(sql);
            List<String> tempDatabaseNames = Lists.newArrayList();
            while (rs.next()) {
                String databaseName = rs.getString(1);
                if (isLowerCaseTableNames) {
                    lowerDBToRealDB.put(databaseName.toLowerCase(), databaseName);
                    databaseName = databaseName.toLowerCase();
                }
                tempDatabaseNames.add(databaseName);
            }
            if (isOnlySpecifiedDatabase) {
                for (String db : tempDatabaseNames) {
                    // Exclude database map take effect with higher priority over include database map
                    if (!excludeDatabaseMap.isEmpty() && excludeDatabaseMap.containsKey(db)) {
                        continue;
                    }
                    if (!includeDatabaseMap.isEmpty() && !includeDatabaseMap.containsKey(db)) {
                        continue;
                    }
                    databaseNames.add(db);
                }
            } else {
                databaseNames = tempDatabaseNames;
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get database name list from jdbc", e);
        } finally {
            close(rs, stmt, conn);
        }
        return databaseNames;
    }

    /**
     * get all tables of one database
     */
    public List<String> getTablesNameList(String dbName) {
        String currentDbName = dbName;
        List<String> tablesName = Lists.newArrayList();
        String[] tableTypes = getTableTypes();
        if (isLowerCaseTableNames) {
            currentDbName = getRealDatabaseName(dbName);
        }
        String finalDbName = currentDbName;
        processTable(finalDbName, null, tableTypes, (rs) -> {
            try {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (isLowerCaseTableNames) {
                        lowerTableToRealTable.put(tableName.toLowerCase(), tableName);
                        tableName = tableName.toLowerCase();
                    }
                    tablesName.add(tableName);
                }
            } catch (SQLException e) {
                throw new JdbcClientException("failed to get all tables for db %s", e, finalDbName);
            }
        });
        return tablesName;
    }

    public boolean isTableExist(String dbName, String tableName) {
        String currentDbName = dbName;
        String currentTableName = tableName;
        final boolean[] isExist = {false};
        if (isLowerCaseTableNames) {
            currentDbName = getRealDatabaseName(dbName);
            currentTableName = getRealTableName(dbName, tableName);
        }
        String[] tableTypes = getTableTypes();
        String finalTableName = currentTableName;
        String finalDbName = currentDbName;
        processTable(finalDbName, finalTableName, tableTypes, (rs) -> {
            try {
                if (rs.next()) {
                    isExist[0] = true;
                }
            } catch (SQLException e) {
                throw new JdbcClientException("failed to judge if table exist for table %s in db %s",
                        e, finalTableName, finalDbName);
            }
        });
        return isExist[0];
    }

    /**
     * get all columns of one table
     */
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String dbName, String tableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = Lists.newArrayList();
        // if isLowerCaseTableNames == true, tableName is lower case
        // but databaseMetaData.getColumns() is case sensitive
        String currentDbName = dbName;
        String currentTableName = tableName;
        if (isLowerCaseTableNames) {
            currentDbName = getRealDatabaseName(dbName);
            currentTableName = getRealTableName(dbName, tableName);
        }
        String finalDbName = currentDbName;
        String finalTableName = currentTableName;
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            rs = getColumns(databaseMetaData, catalogName, finalDbName, finalTableName);
            while (rs.next()) {
                JdbcFieldSchema field = new JdbcFieldSchema();
                field.setColumnName(rs.getString("COLUMN_NAME"));
                field.setDataType(rs.getInt("DATA_TYPE"));
                field.setDataTypeName(rs.getString("TYPE_NAME"));
                /*
                   We used this method to retrieve the key column of the JDBC table, but since we only tested mysql,
                   we kept the default key behavior in the parent class and only overwrite it in the mysql subclass
                */
                field.setKey(true);
                field.setColumnSize(rs.getInt("COLUMN_SIZE"));
                field.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
                field.setNumPrecRadix(rs.getInt("NUM_PREC_RADIX"));
                /*
                   Whether it is allowed to be NULL
                   0 (columnNoNulls)
                   1 (columnNullable)
                   2 (columnNullableUnknown)
                 */
                field.setAllowNull(rs.getInt("NULLABLE") != 0);
                field.setRemarks(rs.getString("REMARKS"));
                field.setCharOctetLength(rs.getInt("CHAR_OCTET_LENGTH"));
                tableSchema.add(field);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get table name list from jdbc for table %s:%s", e, finalTableName,
                    Util.getRootCauseMessage(e));
        } finally {
            close(rs, conn);
        }
        return tableSchema;
    }

    public List<Column> getColumnsFromJdbc(String dbName, String tableName) {
        List<JdbcFieldSchema> jdbcTableSchema = getJdbcColumnsInfo(dbName, tableName);
        List<Column> dorisTableSchema = Lists.newArrayListWithCapacity(jdbcTableSchema.size());
        for (JdbcFieldSchema field : jdbcTableSchema) {
            dorisTableSchema.add(new Column(field.getColumnName(),
                    jdbcTypeToDoris(field), field.isKey, null,
                    field.isAllowNull(), field.getRemarks(),
                    true, -1));
        }
        return dorisTableSchema;
    }

    public String getRealDatabaseName(String dbname) {
        if (!isLowerCaseTableNames) {
            return dbname;
        }

        if (lowerDBToRealDB.isEmpty() || !lowerDBToRealDB.containsKey(dbname)) {
            loadDatabaseNamesIfNeeded();
        }

        return lowerDBToRealDB.get(dbname);
    }

    public String getRealTableName(String dbName, String tableName) {
        if (!isLowerCaseTableNames) {
            return tableName;
        }

        if (lowerTableToRealTable.isEmpty() || !lowerTableToRealTable.containsKey(tableName)) {
            loadTableNamesIfNeeded(dbName);
        }

        return lowerTableToRealTable.get(tableName);
    }

    private void loadDatabaseNamesIfNeeded() {
        if (dbNamesLoaded.compareAndSet(false, true)) {
            getDatabaseNameList();
        }
    }

    private void loadTableNamesIfNeeded(String dbName) {
        if (tableNamesLoaded.compareAndSet(false, true)) {
            getTablesNameList(dbName);
        }
    }

    // protected methods,for subclass to override
    protected String getCatalogName(Connection conn) throws SQLException {
        return null;
    }

    protected abstract String getDatabaseQuery();

    protected List<String> getSpecifiedDatabase(Connection conn) {
        List<String> databaseNames = Lists.newArrayList();
        try {
            databaseNames.add(conn.getSchema());
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get specified database name from jdbc", e);
        } finally {
            close(conn);
        }
        return databaseNames;
    }

    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW"};
    }

    protected void processTable(String dbName, String tableName, String[] tableTypes,
                                Consumer<ResultSet> resultSetConsumer) {
        Connection conn = getConnection();
        ResultSet rs = null;
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            rs = databaseMetaData.getTables(catalogName, dbName, tableName, tableTypes);
            resultSetConsumer.accept(rs);
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to process table", e);
        } finally {
            close(rs, conn);
        }
    }

    protected String modifyTableNameIfNecessary(String tableName) {
        return tableName;
    }

    protected boolean isTableModified(String modifiedTableName, String actualTableName) {
        return false;
    }

    protected ResultSet getColumns(DatabaseMetaData databaseMetaData, String catalogName, String schemaName,
                                   String tableName) throws SQLException {
        return databaseMetaData.getColumns(catalogName, schemaName, tableName, null);
    }

    @Data
    protected static class JdbcFieldSchema {
        protected String columnName;
        // The SQL type of the corresponding java.sql.types (Type ID)
        protected int dataType;
        // The SQL type of the corresponding java.sql.types (Type Name)
        protected String dataTypeName;
        protected boolean isKey;
        // For CHAR/DATA, columnSize means the maximum number of chars.
        // For NUMERIC/DECIMAL, columnSize means precision.
        protected int columnSize;
        protected int decimalDigits;
        // Base number (usually 10 or 2)
        protected int numPrecRadix;
        // column description
        protected String remarks;
        // This length is the maximum number of bytes for CHAR type
        // for utf8 encoding, if columnSize=10, then charOctetLength=30
        // because for utf8 encoding, a Chinese character takes up 3 bytes
        protected int charOctetLength;
        protected boolean isAllowNull;
        protected boolean isAutoincrement;
        protected String defaultValue;
    }

    protected abstract Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema);

    protected Type createDecimalOrStringType(int precision, int scale) {
        if (precision <= ScalarType.MAX_DECIMAL128_PRECISION) {
            return ScalarType.createDecimalV3Type(precision, scale);
        }
        return ScalarType.createStringType();
    }
}
