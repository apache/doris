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

import com.google.common.util.concurrent.MoreExecutors;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * MySQL-specific type handler.
 * Key specializations:
 * - Streaming result set via Integer.MIN_VALUE fetchSize
 * - TIME type uses getString() for >24h values
 * - TINYINT/LARGEINT need type conversion
 * - byte[] → hex string conversion
 * - Connection abort for incomplete result sets
 */
public class MySQLTypeHandler extends DefaultTypeHandler {

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
        switch (type.getType()) {
            case BOOLEAN:
                return rs.getObject(columnIndex, Boolean.class);
            case TINYINT:
            case SMALLINT:
            case LARGEINT:
                return rs.getObject(columnIndex);
            case INT:
                return rs.getObject(columnIndex, Integer.class);
            case BIGINT:
                return rs.getObject(columnIndex, Long.class);
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
            case ARRAY:
                return rs.getObject(columnIndex, String.class);
            case STRING: {
                int jdbcType = metadata.getColumnType(columnIndex);
                // MySQL TIME type needs getString() to handle >24h values.
                // Also needed when MySQL driver connects to MariaDB.
                if (jdbcType == Types.TIME) {
                    return rs.getString(columnIndex);
                }
                return rs.getObject(columnIndex);
            }
            case BYTE:
            case VARBINARY: {
                byte[] data = rs.getBytes(columnIndex);
                return rs.wasNull() ? null : data;
            }
            case TIMESTAMPTZ:
                return rs.getObject(columnIndex, LocalDateTime.class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case TINYINT:
                return createConverter(input -> {
                    if (input instanceof Integer) {
                        return ((Integer) input).byteValue();
                    }
                    return input;
                }, Byte.class);
            case SMALLINT:
                return createConverter(input -> {
                    if (input instanceof Integer) {
                        return ((Integer) input).shortValue();
                    }
                    return input;
                }, Short.class);
            case LARGEINT:
                return createConverter(input -> {
                    if (input instanceof String) {
                        return new BigInteger((String) input);
                    }
                    return input;
                }, BigInteger.class);
            case STRING:
                if ("bitmap".equals(replaceString) || "hll".equals(replaceString)) {
                    return null;
                }
                return createConverter(input -> {
                    if (input instanceof byte[]) {
                        return defaultByteArrayToHexString((byte[]) input);
                    } else if (input instanceof java.sql.Time) {
                        return timeToString((java.sql.Time) input);
                    }
                    return input.toString();
                }, String.class);
            case ARRAY:
                return createConverter(
                        input -> convertArray(input, columnType.getChildTypes().get(0)),
                        List.class);
            default:
                return null;
        }
    }

    @Override
    public PreparedStatement initializeStatement(Connection conn, String sql,
                                                 int fetchSize) throws SQLException {
        conn.setAutoCommit(false);
        PreparedStatement stmt = conn.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        // MySQL: Integer.MIN_VALUE signals streaming results to avoid OOM
        stmt.setFetchSize(Integer.MIN_VALUE);
        return stmt;
    }

    @Override
    public void abortReadConnection(Connection conn, ResultSet rs) throws SQLException {
        if (rs != null && !rs.isAfterLast()) {
            // Abort connection to avoid draining all remaining results
            conn.abort(MoreExecutors.directExecutor());
        }
    }

    @Override
    public void setSystemProperties() {
        super.setSystemProperties();
        System.setProperty("com.mysql.cj.disableAbandonedConnectionCleanup", "true");
    }

    // MySQL-specific array conversion (parse JSON array strings)
    private Object convertArray(Object input, ColumnType childType) {
        if (input == null) {
            return null;
        }
        // MySQL returns arrays as JSON strings; parse them
        return input.toString();
    }
}
