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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;

public class GbaseJdbcExecutor extends BaseJdbcExecutor {

    public GbaseJdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    @Override
    protected Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList) throws SQLException {
        switch (type.getType()) {
            case TINYINT:
                byte tinyIntVal = resultSet.getByte(columnIndex + 1);
                return resultSet.wasNull() ? null : tinyIntVal;
            case SMALLINT:
                short smallIntVal = resultSet.getShort(columnIndex + 1);
                return resultSet.wasNull() ? null : smallIntVal;
            case INT:
                int intVal = resultSet.getInt(columnIndex + 1);
                return resultSet.wasNull() ? null : intVal;
            case BIGINT:
                long bigIntVal = resultSet.getLong(columnIndex + 1);
                return resultSet.wasNull() ? null : bigIntVal;
            case FLOAT:
                float floatVal = resultSet.getFloat(columnIndex + 1);
                return resultSet.wasNull() ? null : floatVal;
            case DOUBLE:
                double doubleVal = resultSet.getDouble(columnIndex + 1);
                return resultSet.wasNull() ? null : doubleVal;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                BigDecimal decimalVal = resultSet.getBigDecimal(columnIndex + 1);
                return resultSet.wasNull() ? null : decimalVal;
            case DATE:
            case DATEV2:
                Date dateVal = resultSet.getDate(columnIndex + 1);
                return resultSet.wasNull() ? null : dateVal.toLocalDate();
            case DATETIME:
            case DATETIMEV2:
                Timestamp timestampVal = resultSet.getTimestamp(columnIndex + 1);
                return resultSet.wasNull() ? null : timestampVal.toLocalDateTime();
            case CHAR:
            case VARCHAR:
            case STRING:
                String stringVal = (String) resultSet.getObject(columnIndex + 1);
                return resultSet.wasNull() ? null : stringVal;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        if (Objects.requireNonNull(columnType.getType()) == Type.CHAR) {
            return createConverter(
                    input -> trimSpaces(input.toString()), String.class);
        }
        return null;
    }
}
