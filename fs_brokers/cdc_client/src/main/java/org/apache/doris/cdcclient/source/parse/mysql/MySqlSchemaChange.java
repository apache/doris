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

package org.apache.doris.cdcclient.source.parse.mysql;

import io.debezium.relational.Column;
import io.debezium.relational.TableId;

/** Parsed MySQL column schema change used by the Doris from-to write path. */
public final class MySqlSchemaChange {
    public enum Type {
        ADD,
        DROP,
        CHANGE,
        MODIFY,
        RENAME
    }

    private final Type type;
    private final TableId tableId;
    private final Column column;
    private final String columnName;
    private final String newColumnName;

    private MySqlSchemaChange(
            Type type, TableId tableId, Column column, String columnName, String newColumnName) {
        this.type = type;
        this.tableId = tableId;
        this.column = column;
        this.columnName = columnName;
        this.newColumnName = newColumnName;
    }

    public static MySqlSchemaChange add(TableId tableId, Column column) {
        return new MySqlSchemaChange(Type.ADD, tableId, column, column.name(), null);
    }

    public static MySqlSchemaChange drop(TableId tableId, String columnName) {
        return new MySqlSchemaChange(Type.DROP, tableId, null, columnName, null);
    }

    public static MySqlSchemaChange change(
            TableId tableId, String columnName, String newColumnName) {
        return new MySqlSchemaChange(Type.CHANGE, tableId, null, columnName, newColumnName);
    }

    public static MySqlSchemaChange modify(TableId tableId, String columnName) {
        return new MySqlSchemaChange(Type.MODIFY, tableId, null, columnName, columnName);
    }

    public static MySqlSchemaChange rename(
            TableId tableId, String columnName, String newColumnName) {
        return new MySqlSchemaChange(Type.RENAME, tableId, null, columnName, newColumnName);
    }

    public Type getType() {
        return type;
    }

    public TableId getTableId() {
        return tableId;
    }

    public Column getColumn() {
        return column;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getNewColumnName() {
        return newColumnName;
    }
}
