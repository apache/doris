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

import com.google.common.util.concurrent.MoreExecutors;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class SQLServerJdbcExecutor extends BaseJdbcExecutor {
    public SQLServerJdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    @Override
    protected boolean abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException {
        if (!resultSet.isAfterLast()) {
            // Abort connection before closing. Without this, the SQLServer driver
            // attempts to drain the connection by reading all the results.
            connection.abort(MoreExecutors.directExecutor());
            return true;
        }
        return false;
    }

    @Override
    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (outputTable.getColumnType(i).getType() == Type.DATE
                    || outputTable.getColumnType(i).getType() == Type.DATEV2) {
                block.add(new Date[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == Type.DATETIME
                    || outputTable.getColumnType(i).getType() == Type.DATETIMEV2) {
                block.add(new Timestamp[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == Type.STRING) {
                block.add(new Object[batchSizeNum]);
            } else {
                block.add(outputTable.getColumn(i).newObjectContainerArray(batchSizeNum));
            }
        }
    }

    @Override
    protected Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList) throws SQLException {
        switch (type.getType()) {
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return resultSet.getBigDecimal(columnIndex + 1);
            case BOOLEAN:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case DATEV2:
            case DATETIME:
            case DATETIMEV2:
            case CHAR:
            case VARCHAR:
            case STRING:
                return resultSet.getObject(columnIndex + 1);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case DATE:
            case DATEV2:
                return createConverter(
                        input -> ((Date) input).toLocalDate(), LocalDate.class);
            case DATETIME:
            case DATETIMEV2:
                return createConverter(
                        input -> ((Timestamp) input).toLocalDateTime(), LocalDateTime.class);
            case STRING:
                return createConverter(input -> {
                    if (input instanceof java.sql.Time) {
                        return timeToString((java.sql.Time) input);
                    } else if (input instanceof byte[]) {
                        return sqlserverByteArrayToHexString((byte[]) input);
                    } else {
                        return input.toString();
                    }
                }, String.class);
            default:
                return null;
        }
    }

    private String sqlserverByteArrayToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder("0x");
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
