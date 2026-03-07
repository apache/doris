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
 */
public class OracleTypeHandler extends DefaultTypeHandler {
    private static final Logger LOG = Logger.getLogger(OracleTypeHandler.class);

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
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
            // Valid UTF-8, return as string
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (CharacterCodingException e) {
            // Not valid UTF-8, return as hex string
            return defaultByteArrayToHexString(bytes);
        }
    }

    @Override
    public void setValidationQuery(HikariDataSource ds) {
        ds.setConnectionTestQuery("SELECT 1 FROM dual");
    }
}
