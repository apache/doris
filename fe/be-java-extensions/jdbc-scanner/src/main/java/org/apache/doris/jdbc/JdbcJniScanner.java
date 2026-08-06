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

package org.apache.doris.jdbc;

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JdbcJniScanner reads data from JDBC sources via the unified JniScanner framework.
 * It extends JniScanner to integrate with the JniConnector/JniReader system on the C++ side,
 * following the same pattern as PaimonJniScanner, HudiJniScanner, etc.
 *
 * <p>This class uses the {@link JdbcTypeHandler} strategy pattern for database-specific
 * type handling. The appropriate handler is selected via {@link JdbcTypeHandlerFactory}
 * based on the "table_type" parameter.
 *
 * <p>Parameters (passed via constructor params map):
 * <ul>
 *   <li>jdbc_url - JDBC connection URL</li>
 *   <li>jdbc_user - database user</li>
 *   <li>jdbc_password - database password</li>
 *   <li>jdbc_driver_class - JDBC driver class name</li>
 *   <li>jdbc_driver_url - path to driver JAR</li>
 *   <li>query_sql - the SELECT SQL to execute</li>
 *   <li>catalog_id - catalog ID for connection pool keying</li>
 *   <li>table_type - database type (MYSQL, ORACLE, POSTGRESQL, etc.)</li>
 *   <li>connection_pool_min_size - min connection pool size</li>
 *   <li>connection_pool_max_size - max connection pool size</li>
 *   <li>connection_pool_max_wait_time - max wait time (ms)</li>
 *   <li>connection_pool_max_life_time - max lifetime (ms)</li>
 *   <li>connection_pool_keep_alive - "true"/"false"</li>
 *   <li>required_fields - comma-separated output field names</li>
 *   <li>columns_types - #-separated column type strings</li>
 * </ul>
 */
public class JdbcJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(JdbcJniScanner.class);

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String jdbcDriverClass;
    private final String jdbcDriverUrl;
    private final String querySql;
    private final long catalogId;
    private final int connectionPoolMinSize;
    private final int connectionPoolMaxSize;
    private final int connectionPoolMaxWaitTime;
    private final int connectionPoolMaxLifeTime;
    private final boolean connectionPoolKeepAlive;

    // Database-specific type handling strategy
    private final JdbcTypeHandler typeHandler;
    // Per-column output converters, initialized once per scan
    private ColumnValueConverter[] outputConverters;

    private HikariDataSource hikariDataSource = null;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private ResultSet resultSet = null;
    private ResultSetMetaData resultSetMetaData = null;
    private ClassLoader classLoader = null;

    // Read state
    private boolean resultSetOpened = false;
    private List<Object[]> block = null;
    // Per-column replace strings for special type handling (bitmap, hll)
    private String[] replaceStringList;
    // Mapping from field index (in types[]/fields[]) to JDBC ResultSet column index (1-based).
    // The slot descriptor order may differ from the query SQL column order.
    private int[] columnIndexMapping;

    // Statistics
    private long readRows = 0;
    private long readTime = 0;

    public JdbcJniScanner(int batchSize, Map<String, String> params) {
        this.jdbcUrl = params.getOrDefault("jdbc_url", "");
        this.jdbcUser = params.getOrDefault("jdbc_user", "");
        this.jdbcPassword = params.getOrDefault("jdbc_password", "");
        this.jdbcDriverClass = params.getOrDefault("jdbc_driver_class", "");
        this.jdbcDriverUrl = params.getOrDefault("jdbc_driver_url", "");
        this.querySql = params.getOrDefault("query_sql", "");
        this.catalogId = Long.parseLong(params.getOrDefault("catalog_id", "0"));
        this.connectionPoolMinSize = Integer.parseInt(
                params.getOrDefault("connection_pool_min_size", "1"));
        this.connectionPoolMaxSize = Integer.parseInt(
                params.getOrDefault("connection_pool_max_size", "10"));
        this.connectionPoolMaxWaitTime = Integer.parseInt(
                params.getOrDefault("connection_pool_max_wait_time", "5000"));
        this.connectionPoolMaxLifeTime = Integer.parseInt(
                params.getOrDefault("connection_pool_max_life_time", "1800000"));
        this.connectionPoolKeepAlive = "true".equalsIgnoreCase(
                params.getOrDefault("connection_pool_keep_alive", "false"));

        // Select database-specific type handler
        String tableType = params.getOrDefault("table_type", "");
        this.typeHandler = JdbcTypeHandlerFactory.create(tableType);

        String requiredFields = params.getOrDefault("required_fields", "");
        String columnsTypes = params.getOrDefault("columns_types", "");
        String[] fieldArr = requiredFields.isEmpty() ? new String[0] : requiredFields.split(",");
        ColumnType[] typeArr;
        if (columnsTypes.isEmpty()) {
            typeArr = new ColumnType[0];
        } else {
            String[] typeStrs = columnsTypes.split("#");
            typeArr = new ColumnType[typeStrs.length];
            for (int i = 0; i < typeStrs.length; i++) {
                typeArr[i] = ColumnType.parseType(fieldArr[i], typeStrs[i]);
            }
        }
        initTableInfo(typeArr, fieldArr, batchSize);

        // Parse replace_string for special type handling (bitmap, hll, etc.)
        String replaceString = params.getOrDefault("replace_string", "");
        if (!replaceString.isEmpty()) {
            replaceStringList = replaceString.split(",");
        } else {
            replaceStringList = new String[fieldArr.length];
            for (int i = 0; i < fieldArr.length; i++) {
                replaceStringList[i] = "not_replace";
            }
        }
    }

    @Override
    public void open() throws IOException {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            // HikariCP's setDriverClassName() uses the thread context classloader
            // to load the driver class.
            initializeClassLoaderAndDataSource();

            conn = hikariDataSource.getConnection();

            // Use type handler to create the statement with database-specific settings
            stmt = typeHandler.initializeStatement(conn, querySql, batchSize);

            LOG.info("JdbcJniScanner: Executing query: " + querySql);
            resultSet = stmt.executeQuery();
            resultSetMetaData = resultSet.getMetaData();
            resultSetOpened = true;

            // Build column name -> JDBC ResultSet index mapping.
            // The slot descriptors (fields[]) may be in a different order than the
            // query SQL columns. We must map by name to avoid reading data with
            // the wrong type handler.
            columnIndexMapping = new int[fields.length];
            int rsColumnCount = resultSetMetaData.getColumnCount();
            Map<String, Integer> rsColumnMap = new HashMap<>(rsColumnCount);
            for (int i = 1; i <= rsColumnCount; i++) {
                String colName = resultSetMetaData.getColumnLabel(i).toLowerCase();
                rsColumnMap.put(colName, i);
            }
            for (int i = 0; i < fields.length; i++) {
                String fieldName = fields[i].toLowerCase();
                Integer rsIdx = rsColumnMap.get(fieldName);
                if (rsIdx != null) {
                    columnIndexMapping[i] = rsIdx;
                } else {
                    // Fallback to positional mapping if name not found
                    columnIndexMapping[i] = i + 1;
                    LOG.warn("Column '" + fields[i] + "' not found in ResultSet by name, "
                            + "falling back to positional index " + (i + 1));
                }
            }

            block = new ArrayList<>(types.length);

            // Initialize per-column output converters once
            outputConverters = new ColumnValueConverter[types.length];
            for (int i = 0; i < types.length; i++) {
                String replaceStr = (replaceStringList != null && i < replaceStringList.length)
                        ? replaceStringList[i] : "not_replace";
                outputConverters[i] = typeHandler.getOutputConverter(types[i], replaceStr);
            }
        } catch (Exception e) {
            LOG.warn("JdbcJniScanner " + jdbcUrl + " open failed: " + e.getMessage(), e);
            throw new IOException("JdbcJniScanner open failed: " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    @Override
    protected int getNext() throws IOException {
        try {
            if (!resultSetOpened || resultSet == null) {
                return 0;
            }

            long startRead = System.nanoTime();

            // Initialize block arrays for this batch
            block.clear();
            for (int i = 0; i < types.length; i++) {
                String replaceStr = (replaceStringList != null && i < replaceStringList.length)
                        ? replaceStringList[i] : "not_replace";
                if ("bitmap".equals(replaceStr) || "hll".equals(replaceStr)
                        || "quantile_state".equals(replaceStr)) {
                    // bitmap/hll/quantile_state columns: use byte[][] for raw binary data
                    block.add(new byte[batchSize][]);
                } else if (outputConverters[i] != null) {
                    // When a converter exists, the raw value from getColumnValue() may have a
                    // different type than the final column type. For example, MySQL returns ARRAY
                    // columns as JSON Strings, but newObjectContainerArray() creates ArrayList[].
                    // Use Object[] to avoid ArrayStoreException; the converter will produce
                    // the correctly typed array before data is written to VectorTable.
                    block.add(new Object[batchSize]);
                } else {
                    block.add(vectorTable.getColumn(i).newObjectContainerArray(batchSize));
                }
            }

            int curRows = 0;
            while (curRows < batchSize) {
                if (!resultSet.next()) {
                    break;
                }
                for (int col = 0; col < types.length; col++) {
                    int columnIndex = columnIndexMapping[col];
                    String replaceStr = (replaceStringList != null && col < replaceStringList.length)
                            ? replaceStringList[col] : "not_replace";
                    if ("bitmap".equals(replaceStr) || "hll".equals(replaceStr)
                            || "quantile_state".equals(replaceStr)) {
                        // bitmap/hll: read raw bytes directly
                        byte[] data = resultSet.getBytes(columnIndex);
                        block.get(col)[curRows] = resultSet.wasNull() ? null : data;
                    } else {
                        // Use type handler for database-specific value extraction
                        Object value = typeHandler.getColumnValue(
                                resultSet, columnIndex, types[col], resultSetMetaData);
                        block.get(col)[curRows] = value;
                    }
                }
                curRows++;
            }

            if (curRows > 0) {
                for (int col = 0; col < types.length; col++) {
                    Object[] columnData = block.get(col);
                    if (curRows < batchSize) {
                        // Trim to actual size
                        Class<?> componentType = columnData.getClass().getComponentType();
                        Object[] trimmed = (Object[]) Array.newInstance(componentType, curRows);
                        System.arraycopy(columnData, 0, trimmed, 0, curRows);
                        columnData = trimmed;
                    }
                    // Apply column-level output converter if present
                    if (outputConverters[col] != null) {
                        columnData = outputConverters[col].convert(columnData);
                    }
                    vectorTable.appendData(col, columnData, null, true);
                }
            }

            readTime += System.nanoTime() - startRead;
            readRows += curRows;
            return curRows;
        } catch (Exception e) {
            LOG.warn("JdbcJniScanner getNext failed: " + e.getMessage(), e);
            throw new IOException("JdbcJniScanner getNext failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // Use type handler for database-specific connection abort
            if (conn != null && resultSet != null) {
                typeHandler.abortReadConnection(conn, resultSet);
            }
        } catch (Exception e) {
            LOG.warn("JdbcJniScanner abort connection error: " + e.getMessage(), e);
        }
        try {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (stmt != null && !stmt.isClosed()) {
                stmt.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (Exception e) {
            LOG.warn("JdbcJniScanner close error: " + e.getMessage(), e);
        } finally {
            resultSet = null;
            stmt = null;
            conn = null;
            if (connectionPoolMinSize == 0 && hikariDataSource != null) {
                hikariDataSource.close();
                JdbcDataSource.getDataSource().getSourcesMap().remove(createCacheKey());
                hikariDataSource = null;
            }
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> stats = new HashMap<>();
        stats.put("counter:ReadRows", String.valueOf(readRows));
        stats.put("timer:ReadTime", String.valueOf(readTime));
        return stats;
    }

    private void initializeClassLoaderAndDataSource() throws Exception {
        java.net.URL[] urls = {new java.net.URL(jdbcDriverUrl)};
        ClassLoader parent = getClass().getClassLoader();
        this.classLoader = java.net.URLClassLoader.newInstance(urls, parent);
        // Must set thread context classloader BEFORE creating HikariDataSource,
        // because HikariCP's setDriverClassName() loads the driver class from
        // the thread context classloader.
        Thread.currentThread().setContextClassLoader(classLoader);

        String cacheKey = createCacheKey();
        hikariDataSource = JdbcDataSource.getDataSource().getSource(cacheKey);
        if (hikariDataSource == null) {
            synchronized (JdbcJniScanner.class) {
                hikariDataSource = JdbcDataSource.getDataSource().getSource(cacheKey);
                if (hikariDataSource == null) {
                    HikariDataSource ds = new HikariDataSource();
                    ds.setDriverClassName(jdbcDriverClass);
                    ds.setJdbcUrl(SecurityChecker.getInstance().getSafeJdbcUrl(jdbcUrl));
                    ds.setUsername(jdbcUser);
                    ds.setPassword(jdbcPassword);
                    ds.setMinimumIdle(connectionPoolMinSize);
                    ds.setMaximumPoolSize(connectionPoolMaxSize);
                    ds.setConnectionTimeout(connectionPoolMaxWaitTime);
                    ds.setMaxLifetime(connectionPoolMaxLifeTime);
                    ds.setIdleTimeout(connectionPoolMaxLifeTime / 2L);
                    // Use type handler for database-specific validation query
                    typeHandler.setValidationQuery(ds);
                    if (connectionPoolKeepAlive) {
                        ds.setKeepaliveTime(connectionPoolMaxLifeTime / 5L);
                    }
                    hikariDataSource = ds;
                    JdbcDataSource.getDataSource().putSource(cacheKey, hikariDataSource);
                    LOG.info("JdbcJniScanner: Created connection pool for " + jdbcUrl);
                }
            }
        }
    }

    private String createCacheKey() {
        return JdbcDataSource.createCacheKey(catalogId, jdbcUrl, jdbcUser, jdbcPassword,
                jdbcDriverUrl, jdbcDriverClass, connectionPoolMinSize, connectionPoolMaxSize,
                connectionPoolMaxLifeTime, connectionPoolMaxWaitTime, connectionPoolKeepAlive);
    }
}
