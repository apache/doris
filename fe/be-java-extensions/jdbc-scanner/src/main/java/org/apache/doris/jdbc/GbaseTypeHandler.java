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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Gbase-specific type handler.
 * Key specializations:
 * - CHAR trimming
 * - Explicit null checks via wasNull() for primitive types
 */
public class GbaseTypeHandler extends DefaultTypeHandler {

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
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
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                BigDecimal val = rs.getBigDecimal(columnIndex);
                return rs.wasNull() ? null : val;
            }
            case DATE:
            case DATEV2: {
                Date val = rs.getDate(columnIndex);
                return rs.wasNull() ? null : val.toLocalDate();
            }
            case DATETIME:
            case DATETIMEV2: {
                Timestamp val = rs.getTimestamp(columnIndex);
                return rs.wasNull() ? null : val.toLocalDateTime();
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                String val = (String) rs.getObject(columnIndex);
                return rs.wasNull() ? null : val;
            }
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        if (columnType.getType() == ColumnType.Type.CHAR) {
            return createConverter(input -> trimSpaces(input.toString()), String.class);
        }
        return null;
    }
}
