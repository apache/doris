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

package org.apache.doris.cdcclient.utils;

/** A generated Doris schema change with metadata used to verify idempotent retries. */
public class SchemaChangeOperation {
    public enum Type {
        ADD,
        DROP
    }

    private final Type type;
    private final String tableName;
    private final String columnName;
    private final String sql;

    private SchemaChangeOperation(Type type, String tableName, String columnName, String sql) {
        this.type = type;
        this.tableName = tableName;
        this.columnName = columnName;
        this.sql = sql;
    }

    public static SchemaChangeOperation addColumn(String tableName, String columnName, String sql) {
        return new SchemaChangeOperation(Type.ADD, tableName, columnName, sql);
    }

    public static SchemaChangeOperation dropColumn(
            String tableName, String columnName, String sql) {
        return new SchemaChangeOperation(Type.DROP, tableName, columnName, sql);
    }

    public Type getType() {
        return type;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getSql() {
        return sql;
    }
}
