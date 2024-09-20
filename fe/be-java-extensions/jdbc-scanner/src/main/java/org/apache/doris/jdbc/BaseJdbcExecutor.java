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
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorColumn;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.thrift.TJdbcExecutorCtorParams;
import org.apache.doris.thrift.TJdbcOperation;

import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.semver4j.Semver;

import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class BaseJdbcExecutor implements JdbcExecutor {
    private static final Logger LOG = Logger.getLogger(BaseJdbcExecutor.class);
    private static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();
    private HikariDataSource hikariDataSource = null;
    private final byte[] hikariDataSourceLock = new byte[0];
    private JdbcDataSourceConfig config;
    private Connection conn = null;
    protected PreparedStatement preparedStatement = null;
    protected Statement stmt = null;
    protected ResultSet resultSet = null;
    protected ResultSetMetaData resultSetMetaData = null;
    protected List<Object[]> block = null;
    protected VectorTable outputTable = null;
    protected int batchSizeNum = 0;
    protected int curBlockRows = 0;
    protected String jdbcDriverVersion;

    public BaseJdbcExecutor(byte[] thriftParams) throws Exception {
        TJdbcExecutorCtorParams request = new TJdbcExecutorCtorParams();
        TDeserializer deserializer = new TDeserializer(PROTOCOL_FACTORY);
        try {
            deserializer.deserialize(request, thriftParams);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
        this.config = new JdbcDataSourceConfig()
                .setCatalogId(request.catalog_id)
                .setJdbcUser(request.jdbc_user)
                .setJdbcPassword(request.jdbc_password)
                .setJdbcUrl(request.jdbc_url)
                .setJdbcDriverUrl(request.driver_path)
                .setJdbcDriverClass(request.jdbc_driver_class)
                .setBatchSize(request.batch_size)
                .setOp(request.op)
                .setTableType(request.table_type)
                .setConnectionPoolMinSize(request.connection_pool_min_size)
                .setConnectionPoolMaxSize(request.connection_pool_max_size)
                .setConnectionPoolMaxWaitTime(request.connection_pool_max_wait_time)
                .setConnectionPoolMaxLifeTime(request.connection_pool_max_life_time)
                .setConnectionPoolKeepAlive(request.connection_pool_keep_alive);
        JdbcDataSource.getDataSource().setCleanupInterval(request.connection_pool_cache_clear_time);
        System.setProperty("com.zaxxer.hikari.useWeakReferences", "true");
        init(config, request.statement);
        this.jdbcDriverVersion = getJdbcDriverVersion();
    }

    public void close() throws Exception {
        try {
            if (stmt != null && !stmt.isClosed()) {
                try {
                    stmt.cancel();
                } catch (SQLException e) {
                    LOG.warn("Cannot cancelling statement: ", e);
                }
            }

            if (conn != null && resultSet != null) {
                abortReadConnection(conn, resultSet);
            }
            closeResources(resultSet, stmt, conn);
        } finally {
            if (config.getConnectionPoolMinSize() == 0 && hikariDataSource != null) {
                hikariDataSource.close();
                JdbcDataSource.getDataSource().getSourcesMap().remove(config.createCacheKey());
                hikariDataSource = null;
            }
        }
    }

    private void closeResources(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    LOG.warn("Cannot close resource: ", e);
                }
            }
        }
    }

    protected void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException {
    }

    public void cleanDataSource() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
            JdbcDataSource.getDataSource().getSourcesMap().remove(config.createCacheKey());
            hikariDataSource = null;
        }
    }

    public void testConnection() throws JdbcExecutorException {
        try {
            resultSet = ((PreparedStatement) stmt).executeQuery();
            if (!resultSet.next()) {
                throw new JdbcExecutorException(
                        "Failed to test connection in BE: query executed but returned no results.");
            }
        } catch (SQLException e) {
            throw new JdbcExecutorException("Failed to test connection in BE: ", e);
        }
    }

    public int read() throws JdbcExecutorException {
        try {
            resultSet = ((PreparedStatement) stmt).executeQuery();
            resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            block = new ArrayList<>(columnCount);
            return columnCount;
        } catch (SQLException e) {
            throw new JdbcExecutorException("JDBC executor sql has error: ", e);
        }
    }

    public long getBlockAddress(int batchSize, Map<String, String> outputParams) throws JdbcExecutorException {
        try {
            if (outputTable != null) {
                outputTable.close();
            }

            outputTable = VectorTable.createWritableTable(outputParams, 0);

            String isNullableString = outputParams.get("is_nullable");
            String replaceString = outputParams.get("replace_string");

            if (isNullableString == null || replaceString == null) {
                throw new IllegalArgumentException(
                        "Output parameters 'is_nullable' and 'replace_string' are required.");
            }

            String[] nullableList = isNullableString.split(",");
            String[] replaceStringList = replaceString.split(",");
            curBlockRows = 0;
            int columnCount = resultSetMetaData.getColumnCount();

            initializeBlock(columnCount, replaceStringList, batchSize, outputTable);

            do {
                for (int i = 0; i < columnCount; ++i) {
                    ColumnType type = outputTable.getColumnType(i);
                    block.get(i)[curBlockRows] = getColumnValue(i, type, replaceStringList);
                }
                curBlockRows++;
            } while (curBlockRows < batchSize && resultSet.next());

            for (int i = 0; i < columnCount; ++i) {
                ColumnType type = outputTable.getColumnType(i);
                Object[] columnData = block.get(i);
                Class<?> componentType = columnData.getClass().getComponentType();
                Object[] newColumn = (Object[]) Array.newInstance(componentType, curBlockRows);
                System.arraycopy(columnData, 0, newColumn, 0, curBlockRows);
                boolean isNullable = Boolean.parseBoolean(nullableList[i]);
                outputTable.appendData(i, newColumn, getOutputConverter(type, replaceStringList[i]), isNullable);
            }
        } catch (Exception e) {
            LOG.warn("jdbc get block address exception: ", e);
            throw new JdbcExecutorException("jdbc get block address: ", e);
        } finally {
            block.clear();
        }
        return outputTable.getMetaAddress();
    }

    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            block.add(outputTable.getColumn(i).newObjectContainerArray(batchSizeNum));
        }
    }

    public int write(Map<String, String> params) throws JdbcExecutorException {
        VectorTable batchTable = VectorTable.createReadableTable(params);
        // Can't release or close batchTable, it's released by c++
        try {
            insert(batchTable);
        } catch (SQLException e) {
            throw new JdbcExecutorException("JDBC executor sql has error: ", e);
        }
        return batchTable.getNumRows();
    }

    public void openTrans() throws JdbcExecutorException {
        try {
            if (conn != null) {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new JdbcExecutorException("JDBC executor open transaction has error: ", e);
        }
    }

    public void commitTrans() throws JdbcExecutorException {
        try {
            if (conn != null) {
                conn.commit();
            }
        } catch (SQLException e) {
            throw new JdbcExecutorException("JDBC executor commit transaction has error: ", e);
        }
    }

    public void rollbackTrans() throws JdbcExecutorException {
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException e) {
            throw new JdbcExecutorException("JDBC executor rollback transaction has error: ", e);
        }
    }

    public int getCurBlockRows() {
        return curBlockRows;
    }

    public boolean hasNext() throws JdbcExecutorException {
        try {
            if (resultSet == null) {
                return false;
            }
            return resultSet.next();
        } catch (SQLException e) {
            throw new JdbcExecutorException("resultSet to get next error: ", e);
        }
    }

    private void init(JdbcDataSourceConfig config, String sql) throws JdbcExecutorException {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        String hikariDataSourceKey = config.createCacheKey();
        try {
            ClassLoader parent = getClass().getClassLoader();
            ClassLoader classLoader = UdfUtils.getClassLoader(config.getJdbcDriverUrl(), parent);
            Thread.currentThread().setContextClassLoader(classLoader);
            hikariDataSource = JdbcDataSource.getDataSource().getSource(hikariDataSourceKey);
            if (hikariDataSource == null) {
                synchronized (hikariDataSourceLock) {
                    hikariDataSource = JdbcDataSource.getDataSource().getSource(hikariDataSourceKey);
                    if (hikariDataSource == null) {
                        long start = System.currentTimeMillis();
                        HikariDataSource ds = new HikariDataSource();
                        ds.setDriverClassName(config.getJdbcDriverClass());
                        ds.setJdbcUrl(SecurityChecker.getInstance().getSafeJdbcUrl(config.getJdbcUrl()));
                        ds.setUsername(config.getJdbcUser());
                        ds.setPassword(config.getJdbcPassword());
                        ds.setMinimumIdle(config.getConnectionPoolMinSize()); // default 1
                        ds.setMaximumPoolSize(config.getConnectionPoolMaxSize()); // default 10
                        ds.setConnectionTimeout(config.getConnectionPoolMaxWaitTime()); // default 5000
                        ds.setMaxLifetime(config.getConnectionPoolMaxLifeTime()); // default 30 min
                        ds.setIdleTimeout(config.getConnectionPoolMaxLifeTime() / 2L); // default 15 min
                        setValidationQuery(ds);
                        if (config.isConnectionPoolKeepAlive()) {
                            ds.setKeepaliveTime(config.getConnectionPoolMaxLifeTime() / 5L); // default 6 min
                        }
                        hikariDataSource = ds;
                        JdbcDataSource.getDataSource().putSource(hikariDataSourceKey, hikariDataSource);
                        LOG.info("JdbcClient set"
                                + " ConnectionPoolMinSize = " + config.getConnectionPoolMinSize()
                                + ", ConnectionPoolMaxSize = " + config.getConnectionPoolMaxSize()
                                + ", ConnectionPoolMaxWaitTime = " + config.getConnectionPoolMaxWaitTime()
                                + ", ConnectionPoolMaxLifeTime = " + config.getConnectionPoolMaxLifeTime()
                                + ", ConnectionPoolKeepAlive = " + config.isConnectionPoolKeepAlive());
                        LOG.info("init datasource [" + (config.getJdbcUrl() + config.getJdbcUser()) + "] cost: " + (
                                System.currentTimeMillis() - start) + " ms");
                    }
                }
            }

            long start = System.currentTimeMillis();
            conn = hikariDataSource.getConnection();
            LOG.info("get connection [" + (config.getJdbcUrl() + config.getJdbcUser()) + "] cost: " + (
                    System.currentTimeMillis() - start)
                    + " ms");

            initializeStatement(conn, config, sql);

        } catch (MalformedURLException e) {
            throw new JdbcExecutorException("MalformedURLException to load class about "
                    + config.getJdbcDriverUrl(), e);
        } catch (SQLException e) {
            throw new JdbcExecutorException("Initialize datasource failed: ", e);
        } catch (FileNotFoundException e) {
            throw new JdbcExecutorException("FileNotFoundException failed: ", e);
        } catch (Exception e) {
            throw new JdbcExecutorException("Initialize datasource failed: ", e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    protected void setValidationQuery(HikariDataSource ds) {
        ds.setConnectionTestQuery("SELECT 1");
    }

    protected void initializeStatement(Connection conn, JdbcDataSourceConfig config, String sql) throws SQLException {
        if (config.getOp() == TJdbcOperation.READ) {
            conn.setAutoCommit(false);
            Preconditions.checkArgument(sql != null, "SQL statement cannot be null for READ operation.");
            stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(config.getBatchSize()); // set fetch size to batch size
            batchSizeNum = config.getBatchSize();
        } else {
            Preconditions.checkArgument(sql != null, "SQL statement cannot be null for WRITE operation.");
            LOG.info("Insert SQL: " + sql);
            preparedStatement = conn.prepareStatement(sql);
        }
    }

    protected abstract Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList)
            throws SQLException;

    /*
    | Type                                        | Java Array Type            |
    |---------------------------------------------|----------------------------|
    | BOOLEAN                                     | Boolean[]                  |
    | TINYINT                                     | Byte[]                     |
    | SMALLINT                                    | Short[]                    |
    | INT                                         | Integer[]                  |
    | BIGINT                                      | Long[]                     |
    | LARGEINT                                    | BigInteger[]               |
    | FLOAT                                       | Float[]                    |
    | DOUBLE                                      | Double[]                   |
    | DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128 | BigDecimal[]               |
    | DATE, DATEV2                                | LocalDate[]                |
    | DATETIME, DATETIMEV2                        | LocalDateTime[]            |
    | CHAR, VARCHAR, STRING                       | String[]                   |
    | ARRAY                                       | List<Object>[]             |
    | MAP                                         | Map<Object, Object>[]      |
    | STRUCT                                      | Map<String, Object>[]      |
    */

    protected abstract ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString);

    protected ColumnValueConverter createConverter(
            Function<Object, ?> converterFunction, Class<?> type) {
        return (Object[] columnData) -> {
            Object[] result = (Object[]) Array.newInstance(type, columnData.length);
            for (int i = 0; i < columnData.length; i++) {
                result[i] = columnData[i] != null ? converterFunction.apply(columnData[i]) : null;
            }
            return result;
        };
    }

    private int insert(VectorTable data) throws SQLException {
        for (int i = 0; i < data.getNumRows(); ++i) {
            for (int j = 0; j < data.getColumns().length; ++j) {
                insertColumn(i, j, data.getColumns()[j]);
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.clearBatch();
        return data.getNumRows();
    }

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
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
                preparedStatement.setString(parameterIndex, column.getStringWithOffset(rowIdx));
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
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
                preparedStatement.setNull(parameterIndex, Types.VARCHAR);
                break;
            default:
                throw new RuntimeException("Unknown type value: " + dorisType);
        }
    }

    private String getJdbcDriverVersion() {
        try {
            if (conn != null) {
                DatabaseMetaData metaData = conn.getMetaData();
                return metaData.getDriverVersion();
            } else {
                return null;
            }
        } catch (SQLException e) {
            LOG.warn("Failed to retrieve JDBC Driver version", e);
            return null;
        }
    }

    protected boolean isJdbcVersionGreaterThanOrEqualTo(String version) {
        Semver currentVersion = Semver.coerce(jdbcDriverVersion);
        Semver targetVersion = Semver.coerce(version);
        if (currentVersion != null && targetVersion != null) {
            return currentVersion.isGreaterThanOrEqualTo(targetVersion);
        } else {
            return false;
        }
    }

    protected String trimSpaces(String str) {
        int end = str.length() - 1;
        while (end >= 0 && str.charAt(end) == ' ') {
            end--;
        }
        return str.substring(0, end + 1);
    }

    protected String timeToString(java.sql.Time time) {
        if (time == null) {
            return null;
        } else {
            long milliseconds = time.getTime() % 1000L;
            if (milliseconds > 0) {
                return String.format("%s.%03d", time, milliseconds);
            } else {
                return time.toString();
            }
        }
    }

    protected String defaultByteArrayToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex.toUpperCase());
        }
        return hexString.toString();
    }
}
