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
import org.apache.doris.common.jni.JniWriter;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorColumn;
import org.apache.doris.common.jni.vec.VectorTable;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * JdbcJniWriter writes C++ Block data to JDBC targets via PreparedStatement batch inserts.
 * It extends JniWriter to integrate with the unified VJniFormatTransformer framework,
 * following the same pattern as MaxComputeJniWriter.
 *
 * <p>Lifecycle (managed by C++ VJniFormatTransformer):
 * <pre>
 *   open() -> write() [repeated] -> close()
 * </pre>
 *
 * <p>Transaction control is exposed via getStatistics() responses and
 * additional JNI method calls from C++ side.
 *
 * <p>Parameters (passed via constructor params map):
 * <ul>
 *   <li>jdbc_url - JDBC connection URL</li>
 *   <li>jdbc_user - database user</li>
 *   <li>jdbc_password - database password</li>
 *   <li>jdbc_driver_class - JDBC driver class name</li>
 *   <li>jdbc_driver_url - path to driver JAR</li>
 *   <li>jdbc_driver_checksum - MD5 checksum of driver JAR</li>
 *   <li>insert_sql - INSERT SQL with ? placeholders</li>
 *   <li>use_transaction - "true"/"false"</li>
 *   <li>catalog_id - catalog ID for connection pool keying</li>
 *   <li>connection_pool_min_size - min pool size</li>
 *   <li>connection_pool_max_size - max pool size</li>
 *   <li>connection_pool_max_wait_time - max wait time (ms)</li>
 *   <li>connection_pool_max_life_time - max lifetime (ms)</li>
 *   <li>connection_pool_keep_alive - "true"/"false"</li>
 * </ul>
 */
public class JdbcJniWriter extends JniWriter {
    private static final Logger LOG = Logger.getLogger(JdbcJniWriter.class);

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String jdbcDriverClass;
    private final String jdbcDriverUrl;
    private final String jdbcDriverChecksum;
    private final String insertSql;
    private final boolean useTransaction;
    private final long catalogId;
    private final int connectionPoolMinSize;
    private final int connectionPoolMaxSize;
    private final int connectionPoolMaxWaitTime;
    private final int connectionPoolMaxLifeTime;
    private final boolean connectionPoolKeepAlive;

    private HikariDataSource hikariDataSource = null;
    private Connection conn = null;
    private PreparedStatement preparedStatement = null;
    private ClassLoader classLoader = null;

    // Statistics
    private long writtenRows = 0;
    private long insertTime = 0;

    public JdbcJniWriter(int batchSize, Map<String, String> params) {
        super(batchSize, params);
        this.jdbcUrl = params.getOrDefault("jdbc_url", "");
        this.jdbcUser = params.getOrDefault("jdbc_user", "");
        this.jdbcPassword = params.getOrDefault("jdbc_password", "");
        this.jdbcDriverClass = params.getOrDefault("jdbc_driver_class", "");
        this.jdbcDriverUrl = params.getOrDefault("jdbc_driver_url", "");
        this.jdbcDriverChecksum = params.getOrDefault("jdbc_driver_checksum", "");
        this.insertSql = params.getOrDefault("insert_sql", "");
        this.useTransaction = "true".equalsIgnoreCase(params.getOrDefault("use_transaction", "false"));
        this.catalogId = Long.parseLong(params.getOrDefault("catalog_id", "0"));
        this.connectionPoolMinSize = Integer.parseInt(params.getOrDefault("connection_pool_min_size", "1"));
        this.connectionPoolMaxSize = Integer.parseInt(params.getOrDefault("connection_pool_max_size", "10"));
        this.connectionPoolMaxWaitTime = Integer.parseInt(
                params.getOrDefault("connection_pool_max_wait_time", "5000"));
        this.connectionPoolMaxLifeTime = Integer.parseInt(
                params.getOrDefault("connection_pool_max_life_time", "1800000"));
        this.connectionPoolKeepAlive = "true".equalsIgnoreCase(
                params.getOrDefault("connection_pool_keep_alive", "false"));
    }

    @Override
    public void open() throws IOException {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            initializeClassLoaderAndDataSource();

            conn = hikariDataSource.getConnection();

            if (useTransaction) {
                conn.setAutoCommit(false);
            }

            LOG.debug("JdbcJniWriter: Preparing insert statement: " + insertSql);
            preparedStatement = conn.prepareStatement(insertSql);
        } catch (Exception e) {
            throw new IOException("JdbcJniWriter open failed: " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    @Override
    protected void writeInternal(VectorTable inputTable) throws IOException {
        try {
            long startInsert = System.nanoTime();
            int numRows = inputTable.getNumRows();
            VectorColumn[] columns = inputTable.getColumns();

            for (int i = 0; i < numRows; ++i) {
                for (int j = 0; j < columns.length; ++j) {
                    insertColumn(i, j, columns[j]);
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();

            insertTime += System.nanoTime() - startInsert;
            writtenRows += numRows;
        } catch (SQLException e) {
            throw new IOException("JdbcJniWriter write failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // Commit transaction before closing if useTransaction is enabled.
            // autoCommit was set to false in open(), so without explicit commit()
            // the JDBC driver will roll back all writes on connection close.
            if (useTransaction && conn != null && !conn.isClosed()) {
                conn.commit();
            }
            if (preparedStatement != null && !preparedStatement.isClosed()) {
                preparedStatement.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            // If commit or close fails, attempt rollback before rethrowing
            try {
                if (useTransaction && conn != null && !conn.isClosed()) {
                    conn.rollback();
                }
            } catch (SQLException rollbackEx) {
                LOG.warn("JdbcJniWriter rollback on close failure also failed: "
                        + rollbackEx.getMessage(), rollbackEx);
            }
            throw new IOException("JdbcJniWriter close failed: " + e.getMessage(), e);
        } finally {
            preparedStatement = null;
            conn = null;
            if (connectionPoolMinSize == 0 && hikariDataSource != null) {
                hikariDataSource.close();
                JdbcDataSource.getDataSource().getSourcesMap()
                        .remove(createCacheKey());
                hikariDataSource = null;
            }
        }
    }

    // === Transaction control methods (called by C++ via JNI) ===

    public void openTrans() throws IOException {
        try {
            if (conn != null) {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new IOException("JdbcJniWriter openTrans failed: " + e.getMessage(), e);
        }
    }

    public void commitTrans() throws IOException {
        try {
            if (conn != null) {
                conn.commit();
            }
        } catch (SQLException e) {
            throw new IOException("JdbcJniWriter commitTrans failed: " + e.getMessage(), e);
        }
    }

    public void rollbackTrans() throws IOException {
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException e) {
            throw new IOException("JdbcJniWriter rollbackTrans failed: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> stats = new HashMap<>();
        stats.put("counter:WrittenRows", String.valueOf(writtenRows));
        stats.put("timer:InsertTime", String.valueOf(insertTime));
        stats.put("timer:WriteTime", String.valueOf(writeTime));
        stats.put("timer:ReadTableTime", String.valueOf(readTableTime));
        return stats;
    }

    // =====================================================================
    // Private helpers — adapted from BaseJdbcExecutor.insert/insertColumn
    // =====================================================================

    private void insertColumn(int rowIdx, int colIdx, VectorColumn column) throws SQLException {
        int parameterIndex = colIdx + 1;
        ColumnType.Type dorisType = column.getColumnPrimitiveType();
        if (column.isNullAt(rowIdx)) {
            insertNullColumn(parameterIndex, dorisType);
            return;
        }
        switch (dorisType) {
            case BOOLEAN:
                preparedStatement.setBoolean(parameterIndex, column.getBoolean(rowIdx));
                break;
            case TINYINT:
                preparedStatement.setByte(parameterIndex, column.getByte(rowIdx));
                break;
            case SMALLINT:
                preparedStatement.setShort(parameterIndex, column.getShort(rowIdx));
                break;
            case INT:
                preparedStatement.setInt(parameterIndex, column.getInt(rowIdx));
                break;
            case BIGINT:
                preparedStatement.setLong(parameterIndex, column.getLong(rowIdx));
                break;
            case LARGEINT:
                preparedStatement.setObject(parameterIndex, column.getBigInteger(rowIdx));
                break;
            case FLOAT:
                preparedStatement.setFloat(parameterIndex, column.getFloat(rowIdx));
                break;
            case DOUBLE:
                preparedStatement.setDouble(parameterIndex, column.getDouble(rowIdx));
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                preparedStatement.setBigDecimal(parameterIndex, column.getDecimal(rowIdx));
                break;
            case DATEV2:
                preparedStatement.setDate(parameterIndex, Date.valueOf(column.getDate(rowIdx)));
                break;
            case DATETIMEV2:
                preparedStatement.setTimestamp(
                        parameterIndex, Timestamp.valueOf(column.getDateTime(rowIdx)));
                break;
            case TIMESTAMPTZ:
                preparedStatement.setObject(
                        parameterIndex, Timestamp.valueOf(column.getTimeStampTz(rowIdx)));
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
                preparedStatement.setString(parameterIndex, column.getStringWithOffset(rowIdx));
                break;
            case BINARY:
            case VARBINARY:
                preparedStatement.setBytes(parameterIndex, column.getBytesVarbinary(rowIdx));
                break;
            default:
                throw new RuntimeException("Unknown type value: " + dorisType);
        }
    }

    private void insertNullColumn(int parameterIndex, ColumnType.Type dorisType)
            throws SQLException {
        switch (dorisType) {
            case BOOLEAN:
                preparedStatement.setNull(parameterIndex, Types.BOOLEAN);
                break;
            case TINYINT:
                preparedStatement.setNull(parameterIndex, Types.TINYINT);
                break;
            case SMALLINT:
                preparedStatement.setNull(parameterIndex, Types.SMALLINT);
                break;
            case INT:
                preparedStatement.setNull(parameterIndex, Types.INTEGER);
                break;
            case BIGINT:
                preparedStatement.setNull(parameterIndex, Types.BIGINT);
                break;
            case LARGEINT:
                preparedStatement.setNull(parameterIndex, Types.JAVA_OBJECT);
                break;
            case FLOAT:
                preparedStatement.setNull(parameterIndex, Types.FLOAT);
                break;
            case DOUBLE:
                preparedStatement.setNull(parameterIndex, Types.DOUBLE);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                preparedStatement.setNull(parameterIndex, Types.DECIMAL);
                break;
            case DATEV2:
                preparedStatement.setNull(parameterIndex, Types.DATE);
                break;
            case DATETIMEV2:
                preparedStatement.setNull(parameterIndex, Types.TIMESTAMP);
                break;
            case TIMESTAMPTZ:
                preparedStatement.setNull(parameterIndex, Types.TIMESTAMP_WITH_TIMEZONE);
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
                preparedStatement.setNull(parameterIndex, Types.VARCHAR);
                break;
            case BINARY:
            case VARBINARY:
                preparedStatement.setNull(parameterIndex, Types.VARBINARY);
                break;
            default:
                throw new RuntimeException("Unknown type value: " + dorisType);
        }
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
            synchronized (JdbcJniWriter.class) {
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
                    // Do not set connectionTestQuery — HikariCP defaults to JDBC4 isValid()
                    // which works across all databases (Oracle, MySQL, PostgreSQL, etc.)
                    if (connectionPoolKeepAlive) {
                        ds.setKeepaliveTime(connectionPoolMaxLifeTime / 5L);
                    }
                    hikariDataSource = ds;
                    JdbcDataSource.getDataSource().putSource(cacheKey, hikariDataSource);
                    LOG.info("JdbcJniWriter: Created connection pool for " + jdbcUrl);
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
