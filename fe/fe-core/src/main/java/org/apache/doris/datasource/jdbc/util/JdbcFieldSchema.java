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

package org.apache.doris.datasource.jdbc.util;

import lombok.Data;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

@Data
public class JdbcFieldSchema {
    protected String columnName;
    // The SQL type of the corresponding java.sql.types (Type ID)
    protected int dataType;
    // The SQL type of the corresponding java.sql.types (Type Name)
    protected Optional<String> dataTypeName;
    // For CHAR/DATA, columnSize means the maximum number of chars.
    // For NUMERIC/DECIMAL, columnSize means precision.
    protected Optional<Integer> columnSize;
    protected Optional<Integer> decimalDigits;
    // Base number (usually 10 or 2)
    protected int numPrecRadix;
    // column description
    protected String remarks;
    // This length is the maximum number of bytes for CHAR type
    // for utf8 encoding, if columnSize=10, then charOctetLength=30
    // because for utf8 encoding, a Chinese character takes up 3 bytes
    protected int charOctetLength;
    protected boolean isAllowNull;


    public JdbcFieldSchema(JdbcFieldSchema other) {
        this.columnName = other.columnName;
        this.dataType = other.dataType;
        this.dataTypeName = other.dataTypeName;
        this.columnSize = other.columnSize;
        this.decimalDigits = other.decimalDigits;
        this.numPrecRadix = other.numPrecRadix;
        this.remarks = other.remarks;
        this.charOctetLength = other.charOctetLength;
        this.isAllowNull = other.isAllowNull;
    }

    public JdbcFieldSchema(ResultSet rs) throws SQLException {
        this.columnName = rs.getString("COLUMN_NAME");
        this.dataType = getInteger(rs, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null"));
        this.dataTypeName = Optional.ofNullable(rs.getString("TYPE_NAME"));
        this.columnSize = getInteger(rs, "COLUMN_SIZE");
        this.decimalDigits =  getInteger(rs, "DECIMAL_DIGITS");
        this.numPrecRadix = rs.getInt("NUM_PREC_RADIX");
        this.isAllowNull = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
        this.remarks = rs.getString("REMARKS");
        this.charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
    }

    public JdbcFieldSchema(ResultSet rs, Map<String, String> dataTypeOverrides) throws SQLException {
        this.columnName = rs.getString("COLUMN_NAME");
        this.dataType = getInteger(rs, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null"));
        this.dataTypeName = Optional.ofNullable(dataTypeOverrides.getOrDefault(columnName, rs.getString("TYPE_NAME")));
        this.columnSize = getInteger(rs, "COLUMN_SIZE");
        this.decimalDigits =  getInteger(rs, "DECIMAL_DIGITS");
        this.numPrecRadix = rs.getInt("NUM_PREC_RADIX");
        this.isAllowNull = rs.getInt("NULLABLE") != 0;
        this.remarks = rs.getString("REMARKS");
        this.charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
    }

    public JdbcFieldSchema(ResultSetMetaData metaData, int columnIndex) throws SQLException {
        this.columnName = metaData.getColumnName(columnIndex);
        this.dataType = metaData.getColumnType(columnIndex);
        this.dataTypeName = Optional.ofNullable(metaData.getColumnTypeName(columnIndex));
        this.columnSize = Optional.of(metaData.getPrecision(columnIndex));
        this.decimalDigits = Optional.of(metaData.getScale(columnIndex));
    }

    protected static Optional<Integer> getInteger(ResultSet resultSet, String columnLabel)
            throws SQLException {
        int value = resultSet.getInt(columnLabel);
        if (resultSet.wasNull()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }
}

