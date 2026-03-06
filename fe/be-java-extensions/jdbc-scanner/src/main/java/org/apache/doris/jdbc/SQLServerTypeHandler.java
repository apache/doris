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

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * SQLServer-specific type handler.
 * Key specializations:
 * - Connection abort for incomplete result sets (avoids driver drain)
 * - DATE/DATETIME: explicit type conversion in output converter
 * - STRING: Time and byte[] handling
 */
public class SQLServerTypeHandler extends DefaultTypeHandler {

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
        switch (type.getType()) {
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return rs.getBigDecimal(columnIndex);
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
                return rs.getObject(columnIndex);
            case VARBINARY:
                return rs.getObject(columnIndex, byte[].class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
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
                        return defaultByteArrayToHexString((byte[]) input);
                    }
                    return input.toString();
                }, String.class);
            default:
                return null;
        }
    }

    @Override
    public void abortReadConnection(Connection conn, ResultSet rs) throws SQLException {
        if (rs != null && !rs.isAfterLast()) {
            // SQLServer driver attempts to drain results on close.
            // Abort connection to prevent this behavior.
            conn.abort(MoreExecutors.directExecutor());
        }
    }
}
