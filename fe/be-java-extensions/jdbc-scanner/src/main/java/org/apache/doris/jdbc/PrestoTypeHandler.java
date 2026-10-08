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

import org.apache.doris.jni.spi.vec.ColumnType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** PrestoDB needs a VARCHAR parameter with an explicit server-side zoned timestamp cast. */
public class PrestoTypeHandler extends TrinoTypeHandler {
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
            ResultSetMetaData metadata) throws SQLException {
        if (type.getType() == ColumnType.Type.TIMESTAMPTZ) {
            // PrestoDB lacks typed getObject; getTimestamp preserves the zone encoded in the remote value.
            Timestamp value = rs.getTimestamp(columnIndex);
            return value == null ? null : checkedUtcTimestamp(value.toInstant());
        }
        return super.getColumnValue(rs, columnIndex, type, metadata);
    }

    @Override
    public void setTimestampTz(PreparedStatement statement, int parameterIndex, LocalDateTime value)
            throws SQLException {
        // The official PrestoDB driver has no TIMESTAMP_WITH_TIMEZONE setObject implementation.
        statement.setString(parameterIndex, value.format(FORMAT) + " UTC");
    }

    @Override
    public void setTimestampTzNull(PreparedStatement statement, int parameterIndex) throws SQLException {
        statement.setNull(parameterIndex, Types.VARCHAR);
    }
}
