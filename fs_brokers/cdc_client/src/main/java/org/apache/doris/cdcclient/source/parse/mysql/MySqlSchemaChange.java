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
        UNSUPPORTED
    }

    private final Type type;
    private final TableId tableId;
    private final Column column;
    private final String columnName;
    private final String sourceOperation;
    private final String reason;

    private MySqlSchemaChange(
            Type type,
            TableId tableId,
            Column column,
            String columnName,
            String sourceOperation,
            String reason) {
        this.type = type;
        this.tableId = tableId;
        this.column = column;
        this.columnName = columnName;
        this.sourceOperation = sourceOperation;
        this.reason = reason;
    }

    public static MySqlSchemaChange add(TableId tableId, Column column) {
        return new MySqlSchemaChange(Type.ADD, tableId, column, column.name(), "ADD", null);
    }

    public static MySqlSchemaChange drop(TableId tableId, String columnName) {
        return new MySqlSchemaChange(Type.DROP, tableId, null, columnName, "DROP", null);
    }

    public static MySqlSchemaChange unsupported(
            TableId tableId, String sourceOperation, String reason) {
        return new MySqlSchemaChange(
                Type.UNSUPPORTED, tableId, null, null, sourceOperation, reason);
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

    public String getSourceOperation() {
        return sourceOperation;
    }

    public String getReason() {
        return reason;
    }
}
