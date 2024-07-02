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
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class SapHanaJdbcExecutor extends BaseJdbcExecutor {
    private static final Logger LOG = Logger.getLogger(SapHanaJdbcExecutor.class);

    public SapHanaJdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    @Override
    protected void setValidationQuery(HikariDataSource ds) {
        ds.setConnectionTestQuery("SELECT 1 FROM DUMMY");
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
            case BOOLEAN:
                return resultSet.getObject(columnIndex + 1, Boolean.class);
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
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return resultSet.getObject(columnIndex + 1, BigDecimal.class);
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
                    if (input instanceof Clob) {
                        try {
                            return ((Clob) input).getSubString(1, (int) ((Clob) input).length());
                        } catch (SQLException e) {
                            LOG.error("Failed to get string from clob", e);
                            return null;
                        }
                    } else if (input instanceof byte[]) {
                        return defaultByteArrayToHexString((byte[]) input);
                    } else {
                        return input.toString();
                    }
                }, String.class);
            default:
                return null;
        }
    }

}
