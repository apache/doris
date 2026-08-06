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
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Trino/Presto-specific type handler.
 * Key specializations:
 * - DATETIME: uses Timestamp.class then converts in output converter
 * - ARRAY: getArray() → Object[] → List
 */
public class TrinoTypeHandler extends DefaultTypeHandler {

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
                return rs.getObject(columnIndex, Timestamp.class);
            case CHAR:
            case VARCHAR:
            case STRING:
                return rs.getObject(columnIndex, String.class);
            case ARRAY: {
                Array array = rs.getArray(columnIndex);
                if (array == null) {
                    return null;
                }
                Object[] dataArray = (Object[]) array.getArray();
                if (dataArray.length == 0) {
                    return Collections.emptyList();
                }
                return Arrays.asList(dataArray);
            }
            case VARBINARY:
                return rs.getObject(columnIndex, byte[].class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case DATETIME:
            case DATETIMEV2:
                return createConverter(
                        input -> ((Timestamp) input).toLocalDateTime(), LocalDateTime.class);
            case ARRAY:
                return createConverter(
                        input -> convertArray((List<?>) input, columnType.getChildTypes().get(0)),
                        List.class);
            default:
                return null;
        }
    }

    private Object convertArray(List<?> input, ColumnType childType) {
        return input;
    }
}
