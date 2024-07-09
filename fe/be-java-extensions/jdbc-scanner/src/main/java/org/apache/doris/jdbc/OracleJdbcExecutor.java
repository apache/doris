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
import org.apache.doris.common.jni.vec.ColumnType.Type;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorTable;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class OracleJdbcExecutor extends BaseJdbcExecutor {
    private static final Logger LOG = Logger.getLogger(OracleJdbcExecutor.class);
    private final CharsetDecoder utf8Decoder = StandardCharsets.UTF_8.newDecoder();

    public OracleJdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    @Override
    protected void setValidationQuery(HikariDataSource ds) {
        ds.setConnectionTestQuery("SELECT 1 FROM dual");
    }

    @Override
    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (outputTable.getColumnType(i).getType() == Type.LARGEINT) {
                block.add(new BigDecimal[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == Type.STRING) {
                block.add(new Object[batchSizeNum]);
            } else {
                block.add(outputTable.getColumn(i).newObjectContainerArray(batchSizeNum));
            }
        }
    }

    @Override
    protected Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList) throws SQLException {
        try {
            switch (type.getType()) {
                case TINYINT:
                    return resultSet.getObject(columnIndex + 1, Byte.class);
                case SMALLINT:
                    return resultSet.getObject(columnIndex + 1, Short.class);
                case INT:
                    return resultSet.getObject(columnIndex + 1, Integer.class);
                case BIGINT:
                    return resultSet.getObject(columnIndex + 1, Long.class);
                case FLOAT:
                    return resultSet.getObject(columnIndex + 1, Float.class);
                case DOUBLE:
                    return resultSet.getObject(columnIndex + 1, Double.class);
                case LARGEINT:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return resultSet.getObject(columnIndex + 1, BigDecimal.class);
                case DATE:
                case DATEV2:
                    return resultSet.getObject(columnIndex + 1, LocalDate.class);
                case DATETIME:
                case DATETIMEV2:
                    return resultSet.getObject(columnIndex + 1, LocalDateTime.class);
                case CHAR:
                case VARCHAR:
                case STRING:
                    return resultSet.getObject(columnIndex + 1);
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + type.getType());
            }
        } catch (AbstractMethodError e) {
            LOG.warn("Detected an outdated ojdbc driver. Please use ojdbc8 or above.", e);
            throw new SQLException("Detected an outdated ojdbc driver. Please use ojdbc8 or above.");
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case CHAR:
                return createConverter(
                        input -> trimSpaces(input.toString()), String.class);
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
                    } else {
                        return input.toString();
                    }
                }, String.class);
            default:
                return null;
        }
    }

    private String convertByteArrayToString(byte[] bytes) {
        if (isValidUtf8(bytes)) {
            return new String(bytes, StandardCharsets.UTF_8);
        } else {
            // Convert byte[] to hexadecimal string with "0x" prefix
            return "0x" + bytesToHex(bytes);
        }
    }

    private boolean isValidUtf8(byte[] bytes) {
        utf8Decoder.reset();
        try {
            utf8Decoder.decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
}
