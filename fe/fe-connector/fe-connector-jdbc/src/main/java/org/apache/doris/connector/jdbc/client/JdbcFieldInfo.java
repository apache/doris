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

package org.apache.doris.connector.jdbc.client;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

/**
 * Holds JDBC column metadata. Adapted from fe-core's {@code JdbcFieldSchema}
 * with identical field semantics but no fe-core dependencies.
 */
public class JdbcFieldInfo {

    private String columnName;
    private int dataType;
    private Optional<String> dataTypeName;
    private Optional<Integer> columnSize;
    private Optional<Integer> decimalDigits;
    private Optional<Integer> arrayDimensions;
    private int numPrecRadix;
    private String remarks;
    private int charOctetLength;
    private boolean allowNull;

    public JdbcFieldInfo(JdbcFieldInfo other) {
        this.columnName = other.columnName;
        this.dataType = other.dataType;
        this.dataTypeName = other.dataTypeName;
        this.columnSize = other.columnSize;
        this.decimalDigits = other.decimalDigits;
        this.arrayDimensions = other.arrayDimensions;
        this.numPrecRadix = other.numPrecRadix;
        this.remarks = other.remarks;
        this.charOctetLength = other.charOctetLength;
        this.allowNull = other.allowNull;
    }

    public JdbcFieldInfo(ResultSet rs) throws SQLException {
        this.columnName = rs.getString("COLUMN_NAME");
        this.dataType = getInteger(rs, "DATA_TYPE").orElseThrow(
                () -> new IllegalStateException("DATA_TYPE is null"));
        this.dataTypeName = Optional.ofNullable(rs.getString("TYPE_NAME"));
        this.columnSize = getInteger(rs, "COLUMN_SIZE");
        this.decimalDigits = getInteger(rs, "DECIMAL_DIGITS");
        this.numPrecRadix = rs.getInt("NUM_PREC_RADIX");
        this.allowNull = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
        this.remarks = rs.getString("REMARKS");
        this.charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
    }

    public JdbcFieldInfo(ResultSet rs, int arrayDimensions) throws SQLException {
        this(rs);
        this.arrayDimensions = Optional.of(arrayDimensions);
    }

    public JdbcFieldInfo(ResultSet rs, Map<String, String> dataTypeOverrides) throws SQLException {
        this.columnName = rs.getString("COLUMN_NAME");
        this.dataType = getInteger(rs, "DATA_TYPE").orElseThrow(
                () -> new IllegalStateException("DATA_TYPE is null"));
        this.dataTypeName = Optional.ofNullable(
                dataTypeOverrides.getOrDefault(columnName, rs.getString("TYPE_NAME")));
        this.columnSize = getInteger(rs, "COLUMN_SIZE");
        this.decimalDigits = getInteger(rs, "DECIMAL_DIGITS");
        this.numPrecRadix = rs.getInt("NUM_PREC_RADIX");
        this.allowNull = rs.getInt("NULLABLE") != 0;
        this.remarks = rs.getString("REMARKS");
        this.charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
    }

    public JdbcFieldInfo(ResultSetMetaData metaData, int columnIndex) throws SQLException {
        String columnLabel = metaData.getColumnLabel(columnIndex);
        this.columnName = (columnLabel == null || columnLabel.isEmpty())
                ? metaData.getColumnName(columnIndex) : columnLabel;
        this.dataType = metaData.getColumnType(columnIndex);
        this.dataTypeName = Optional.ofNullable(metaData.getColumnTypeName(columnIndex));
        this.columnSize = Optional.of(metaData.getPrecision(columnIndex));
        this.decimalDigits = Optional.of(metaData.getScale(columnIndex));
        this.arrayDimensions = Optional.of(0);
    }

    /**
     * Minimal constructor for unit testing — sets only the fields used by type mapping.
     */
    public JdbcFieldInfo(String columnName, Optional<String> dataTypeName, int dataType,
            Optional<Integer> columnSize, Optional<Integer> decimalDigits,
            Optional<Integer> arrayDimensions) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.dataTypeName = dataTypeName;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.arrayDimensions = arrayDimensions;
    }

    // -- accessors --

    public String getColumnName() {
        return columnName;
    }

    public int getDataType() {
        return dataType;
    }

    public Optional<String> getDataTypeName() {
        return dataTypeName;
    }

    public void setDataTypeName(Optional<String> dataTypeName) {
        this.dataTypeName = dataTypeName;
    }

    public Optional<Integer> getColumnSize() {
        return columnSize;
    }

    public Optional<Integer> getDecimalDigits() {
        return decimalDigits;
    }

    public Optional<Integer> getArrayDimensions() {
        return arrayDimensions;
    }

    public int getNumPrecRadix() {
        return numPrecRadix;
    }

    public String getRemarks() {
        return remarks;
    }

    public int getCharOctetLength() {
        return charOctetLength;
    }

    public boolean isAllowNull() {
        return allowNull;
    }

    public void setAllowNull(boolean allowNull) {
        this.allowNull = allowNull;
    }

    public int requiredColumnSize() {
        return columnSize.orElseThrow(() -> new IllegalStateException("column size not present"));
    }

    public int requiredDecimalDigits() {
        return decimalDigits.orElseThrow(() -> new IllegalStateException("decimal digits not present"));
    }

    private static Optional<Integer> getInteger(ResultSet resultSet, String columnLabel)
            throws SQLException {
        int value = resultSet.getInt(columnLabel);
        if (resultSet.wasNull()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }
}
