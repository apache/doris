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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class DB2JdbcExecutor extends BaseJdbcExecutor {
    public DB2JdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    @Override
    protected void setValidationQuery(HikariDataSource ds) {
        ds.setConnectionTestQuery("select 1 from sysibm.sysdummy1");
    }

    @Override
    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (outputTable.getColumnType(i).getType() == Type.SMALLINT) {
                block.add(new Integer[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == Type.DATE
                    || outputTable.getColumnType(i).getType() == Type.DATEV2) {
                block.add(new Date[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == Type.DATETIME
                    || outputTable.getColumnType(i).getType() == Type.DATETIMEV2) {
                block.add(new Timestamp[batchSizeNum]);
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
                return resultSet.getObject(columnIndex + 1, BigDecimal.class);
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case DATEV2:
            case DATETIME:
            case DATETIMEV2:
                return resultSet.getObject(columnIndex + 1);
            case CHAR:
            case VARCHAR:
            case STRING:
                return resultSet.getObject(columnIndex + 1, String.class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case SMALLINT:
                return createConverter(
                        input -> ((Integer) input).shortValue(), Short.class);
            case DATE:
            case DATEV2:
                return createConverter(
                        input -> ((Date) input).toLocalDate(), LocalDate.class);
            case DATETIME:
            case DATETIMEV2:
                return createConverter(
                        input -> ((Timestamp) input).toLocalDateTime(), LocalDateTime.class);
            case CHAR:
                return createConverter(
                        input -> trimSpaces(input.toString()), String.class);
            default:
                return null;
        }
    }
}
