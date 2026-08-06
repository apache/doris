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

import com.zaxxer.hikari.HikariDataSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * DB2-specific type handler.
 * Key specializations:
 * - Validation query: "select 1 from sysibm.sysdummy1"
 * - Uses getObject(Class) for DECIMAL types
 */
public class DB2TypeHandler extends DefaultTypeHandler {

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
        switch (type.getType()) {
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return rs.getObject(columnIndex, BigDecimal.class);
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return rs.getObject(columnIndex);
            case DATE:
            case DATEV2: {
                Date sqlDate = rs.getDate(columnIndex);
                return rs.wasNull() ? null : sqlDate.toLocalDate();
            }
            case DATETIME:
            case DATETIMEV2: {
                Timestamp ts = rs.getTimestamp(columnIndex);
                return rs.wasNull() ? null : ts.toLocalDateTime();
            }
            case CHAR:
            case VARCHAR:
            case STRING:
                return rs.getObject(columnIndex, String.class);
            case VARBINARY:
                return rs.getObject(columnIndex, byte[].class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public void setValidationQuery(HikariDataSource ds) {
        ds.setConnectionTestQuery("select 1 from sysibm.sysdummy1");
    }
}
