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
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorColumn;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JdbcJniScanner reads data from JDBC sources via the unified JniScanner framework.
 * It extends JniScanner to integrate with the JniConnector/JniReader system on the C++ side,
 * following the same pattern as PaimonJniScanner, HudiJniScanner, etc.
 *
 * <p>This is an independent class that does NOT depend on BaseJdbcExecutor. It manages
 * its own connection pool, statement, and result set lifecycle.
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

    private HikariDataSource hikariDataSource = null;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private ResultSet resultSet = null;
    private ResultSetMetaData resultSetMetaData = null;
    private ClassLoader classLoader = null;

    // Read state
    private boolean resultSetOpened = false;
    private List<Object[]> block = null;

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
    }

    @Override
    public void open() throws IOException {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            initializeClassLoaderAndDataSource();
            Thread.currentThread().setContextClassLoader(classLoader);

            conn = hikariDataSource.getConnection();
            conn.setAutoCommit(false);

            stmt = conn.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(batchSize);

            LOG.info("JdbcJniScanner: Executing query: " + querySql);
            resultSet = stmt.executeQuery();
            resultSetMetaData = resultSet.getMetaData();
            resultSetOpened = true;

            block = new ArrayList<>(types.length);
        } catch (Exception e) {
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
                block.add(vectorTable.getColumn(i).newObjectContainerArray(batchSize));
            }

            int curRows = 0;
            while (curRows < batchSize) {
                if (!resultSet.next()) {
                    break;
                }
                for (int col = 0; col < types.length; col++) {
                    int columnIndex = col + 1; // JDBC columns are 1-indexed
                    block.get(col)[curRows] = getColumnValue(columnIndex, types[col]);
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
                    vectorTable.appendData(col, columnData, null, true);
                }
            }

            readTime += System.nanoTime() - startRead;
            readRows += curRows;
            return curRows;
        } catch (SQLException e) {
            throw new IOException("JdbcJniScanner getNext failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
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
        } catch (SQLException e) {
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

    // =====================================================================
    // Private helpers — reading column values from ResultSet
    // Adapted from BaseJdbcExecutor.getColumnValue() logic
    // =====================================================================

    private Object getColumnValue(int columnIndex, ColumnType type) throws SQLException {
        Object value = null;
        switch (type.getType()) {
            case BOOLEAN:
                value = resultSet.getBoolean(columnIndex);
                break;
            case TINYINT:
                value = resultSet.getByte(columnIndex);
                break;
            case SMALLINT:
                value = resultSet.getShort(columnIndex);
                break;
            case INT:
                value = resultSet.getInt(columnIndex);
                break;
            case BIGINT:
                value = resultSet.getLong(columnIndex);
                break;
            case LARGEINT:
                value = resultSet.getObject(columnIndex);
                if (value instanceof BigDecimal) {
                    value = ((BigDecimal) value).toBigInteger();
                } else if (value != null && !(value instanceof BigInteger)) {
                    value = new BigInteger(value.toString());
                }
                break;
            case FLOAT:
                value = resultSet.getFloat(columnIndex);
                break;
            case DOUBLE:
                value = resultSet.getDouble(columnIndex);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                value = resultSet.getBigDecimal(columnIndex);
                break;
            case DATEV2:
                Date sqlDate = resultSet.getDate(columnIndex);
                if (sqlDate != null) {
                    value = sqlDate.toLocalDate();
                }
                break;
            case DATETIMEV2:
                Timestamp ts = resultSet.getTimestamp(columnIndex);
                if (ts != null) {
                    value = ts.toLocalDateTime();
                }
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
                value = resultSet.getString(columnIndex);
                break;
            default:
                value = resultSet.getString(columnIndex);
                break;
        }
        if (resultSet.wasNull()) {
            return null;
        }
        return value;
    }

    private void initializeClassLoaderAndDataSource() throws Exception {
        java.net.URL[] urls = {new java.net.URL(jdbcDriverUrl)};
        ClassLoader parent = getClass().getClassLoader();
        this.classLoader = java.net.URLClassLoader.newInstance(urls, parent);

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
                    ds.setConnectionTestQuery("SELECT 1");
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
        return catalogId + "#" + jdbcUrl + "#" + jdbcUser + "#" + jdbcDriverClass;
    }
}
