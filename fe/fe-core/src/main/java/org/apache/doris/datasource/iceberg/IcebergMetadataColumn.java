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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Iceberg metadata columns that can be projected in queries.
 *
 * These columns are not stored in data files but are generated during scanning.
 * Doris uses __DORIS_ICEBERG_ROWID_COL__ for row-id and does not expose $row_id.
 */
public enum IcebergMetadataColumn {
    /**
     * File path of the data file containing the row.
     * Type: STRING
     * Used in Position Delete files to identify which file a row belongs to.
     */
    FILE_PATH("$file_path", ScalarType.createStringType()),

    /**
     * Position (row number) of the row within the data file.
     * Type: BIGINT
     * Used in Position Delete files to identify the exact row to delete.
     */
    ROW_POSITION("$row_position", ScalarType.createType(org.apache.doris.catalog.PrimitiveType.BIGINT)),

    /**
     * Partition specification ID.
     * Type: INT
     * Identifies which partition specification is used for this row.
     */
    PARTITION_SPEC_ID("$partition_spec_id", ScalarType.createType(org.apache.doris.catalog.PrimitiveType.INT)),

    /**
     * Partition data as JSON string.
     * Type: STRING
     * Contains the partition values for this row in JSON format.
     */
    PARTITION_DATA("$partition_data", ScalarType.createStringType());

    private final String columnName;
    private final Type columnType;

    IcebergMetadataColumn(String columnName, Type columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public Type getColumnType() {
        return columnType;
    }

    /**
     * Check if a column name is a metadata column.
     */
    public static boolean isMetadataColumn(String columnName) {
        return fromColumnName(columnName) != null;
    }

    /**
     * Get metadata column by name.
     */
    public static IcebergMetadataColumn fromColumnName(String columnName) {
        for (IcebergMetadataColumn column : values()) {
            if (column.columnName.equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    /**
     * Get all metadata column names.
     */
    public static List<String> getAllColumnNames() {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (IcebergMetadataColumn column : values()) {
            builder.add(column.columnName);
        }
        return builder.build();
    }

    /**
     * Check if a column name is $file_path.
     */
    public static boolean isFilePathColumn(String columnName) {
        return FILE_PATH.columnName.equals(columnName);
    }

    /**
     * Check if a column name is $row_position.
     */
    public static boolean isRowPositionColumn(String columnName) {
        return ROW_POSITION.columnName.equals(columnName);
    }

}
