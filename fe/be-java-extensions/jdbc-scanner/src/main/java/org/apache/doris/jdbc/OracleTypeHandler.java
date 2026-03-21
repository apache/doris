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

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Oracle-specific type handler.
 * Key specializations:
 * - Validation query: "SELECT 1 FROM dual"
 * - CHAR trimming
 * - LARGEINT: BigDecimal → BigInteger
 * - STRING: Clob and byte[] handling
 * - Supports both old (ojdbc6, < 12.2.0 JDBC 4.0) and new (>= 12.2.0 JDBC 4.1) drivers.
 *   Old drivers don't support rs.getObject(int, Class), so we fall back to typed getters.
 */
public class OracleTypeHandler extends DefaultTypeHandler {
    private static final Logger LOG = Logger.getLogger(OracleTypeHandler.class);

    // Whether the JDBC driver supports JDBC 4.1 getObject(int, Class) method.
    // Determined at runtime from the driver version. Oracle ojdbc6 (< 12.2.0) does not.
    private boolean jdbc41Supported = true;

    // Flag to track if we've detected the driver version yet
    private volatile boolean versionDetected = false;

    /**
     * Detect Oracle JDBC driver version from the connection metadata.
     * ojdbc6 (version < 12.2.0) does not support JDBC 4.1 getObject(int, Class).
     */
    private void detectDriverVersion(Connection conn) {
        if (versionDetected) {
            return;
        }
        try {
            DatabaseMetaData meta = conn.getMetaData();
            String driverVersion = meta.getDriverVersion();
            if (driverVersion != null) {
                jdbc41Supported = isVersionGreaterThanOrEqual(driverVersion, "12.2.0");
                LOG.info("Oracle JDBC driver version: " + driverVersion
                        + ", JDBC 4.1 supported: " + jdbc41Supported);
            }
        } catch (SQLException e) {
            LOG.warn("Failed to detect Oracle driver version, assuming JDBC 4.1: " + e.getMessage());
        }
        versionDetected = true;
    }

    private static boolean isVersionGreaterThanOrEqual(String version, String target) {
        try {
            String[] vParts = version.split("[^0-9]+");
            String[] tParts = target.split("[^0-9]+");
            for (int i = 0; i < Math.max(vParts.length, tParts.length); i++) {
                int v = i < vParts.length && !vParts[i].isEmpty() ? Integer.parseInt(vParts[i]) : 0;
                int t = i < tParts.length && !tParts[i].isEmpty() ? Integer.parseInt(tParts[i]) : 0;
                if (v > t) {
                    return true;
                }
                if (v < t) {
                    return false;
                }
            }
            return true; // equal
        } catch (NumberFormatException e) {
            return true; // assume new version if parsing fails
        }
    }

    @Override
    public PreparedStatement initializeStatement(Connection conn, String sql,
                                                 int fetchSize) throws SQLException {
        // Detect driver version when creating the statement (first time we have access to connection)
        detectDriverVersion(conn);
        PreparedStatement stmt = conn.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
        if (jdbc41Supported) {
            return newGetColumnValue(rs, columnIndex, type);
        } else {
            return oldGetColumnValue(rs, columnIndex, type);
        }
    }

    /**
     * JDBC 4.1+ path: uses rs.getObject(int, Class) for typed retrieval.
     */
    private Object newGetColumnValue(ResultSet rs, int columnIndex, ColumnType type)
            throws SQLException {
        switch (type.getType()) {
            case BOOLEAN:
                return rs.getObject(columnIndex, Boolean.class);
            case TINYINT:
                return rs.getObject(columnIndex, Byte.class);
            case SMALLINT:
                return rs.getObject(columnIndex, Short.class);
            case INT:
                return rs.getObject(columnIndex, Integer.class);
            case BIGINT:
                return rs.getObject(columnIndex, Long.class);
            case LARGEINT:
                return rs.getObject(columnIndex, BigDecimal.class);
            case FLOAT:
                return rs.getObject(columnIndex, Float.class);
            case DOUBLE:
                return rs.getObject(columnIndex, Double.class);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return rs.getObject(columnIndex, BigDecimal.class);
            case DATE:
            case DATEV2:
                return rs.getObject(columnIndex, LocalDate.class);
            case DATETIME:
            case DATETIMEV2:
                return rs.getObject(columnIndex, LocalDateTime.class);
            case CHAR:
            case VARCHAR:
            case STRING:
                return rs.getObject(columnIndex);
            case VARBINARY:
                return rs.getBytes(columnIndex);
            case TIMESTAMPTZ: {
                Timestamp ts = rs.getObject(columnIndex, Timestamp.class);
                return ts == null ? null : LocalDateTime.ofInstant(ts.toInstant(), java.time.ZoneOffset.UTC);
            }
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    /**
     * JDBC 4.0 fallback path for old Oracle drivers (ojdbc6, < 12.2.0).
     * Uses typed getter methods (getByte, getShort, etc.) instead of getObject(int, Class).
     */
    private Object oldGetColumnValue(ResultSet rs, int columnIndex, ColumnType type)
            throws SQLException {
        switch (type.getType()) {
            case TINYINT: {
                byte val = rs.getByte(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case SMALLINT: {
                short val = rs.getShort(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case INT: {
                int val = rs.getInt(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case BIGINT: {
                long val = rs.getLong(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case FLOAT: {
                float val = rs.getFloat(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case DOUBLE: {
                double val = rs.getDouble(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case LARGEINT:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                BigDecimal val = rs.getBigDecimal(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case DATE:
            case DATEV2: {
                java.sql.Date val = rs.getDate(columnIndex);
                return val == null ? null : val.toLocalDate();
            }
            case DATETIME:
            case DATETIMEV2: {
                Timestamp val = rs.getTimestamp(columnIndex);
                return val == null ? null : val.toLocalDateTime();
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                Object val = rs.getObject(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case VARBINARY: {
                byte[] val = rs.getBytes(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case TIMESTAMPTZ: {
                Timestamp val = rs.getTimestamp(columnIndex);
                return val == null ? null : LocalDateTime.ofInstant(val.toInstant(), java.time.ZoneOffset.UTC);
            }
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case CHAR:
                return createConverter(input -> trimSpaces(input.toString()), String.class);
            case LARGEINT:
                return createConverter(
                        input -> ((BigDecimal) input).toBigInteger(), BigInteger.class);
            case STRING:
                return createConverter(input -> {
                    if (input instanceof Clob) {
                        try {
                            return ((Clob) input).getSubString(1, (int) ((Clob) input).length());
                        } catch (SQLException e) {
                            LOG.error("Failed to get string from clob", e);
                            return null;
                        }
                    } else if (input instanceof byte[]) {
                        return convertByteArrayToString((byte[]) input);
                    }
                    return input.toString();
                }, String.class);
            default:
                return null;
        }
    }

    /**
     * Oracle RAW type returns byte[]. Try to decode as UTF-8 first;
     * if it's not valid UTF-8, fall back to hex string with "0x" prefix.
     * This matches the behavior of the old OracleJdbcExecutor.
     */
    private static String convertByteArrayToString(byte[] bytes) {
        CharsetDecoder utf8Decoder = StandardCharsets.UTF_8.newDecoder();
        try {
            utf8Decoder.decode(ByteBuffer.wrap(bytes));
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (CharacterCodingException e) {
            return defaultByteArrayToHexString(bytes);
        }
    }

    @Override
    public void setValidationQuery(HikariDataSource ds) {
        ds.setConnectionTestQuery("SELECT 1 FROM dual");
    }
}
