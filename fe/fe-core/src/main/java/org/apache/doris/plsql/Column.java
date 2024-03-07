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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Column.java
// and modified by Doris

package org.apache.doris.plsql;

/**
 * Table column
 */
public class Column {
    private org.apache.doris.plsql.ColumnDefinition definition;
    private Var value;

    public Column(String name, String type, Var value) {
        this.definition = new org.apache.doris.plsql.ColumnDefinition(name,
                org.apache.doris.plsql.ColumnType.parse(type));
        this.value = value;
    }

    /**
     * Set the column value
     */
    public void setValue(Var value) {
        this.value = value;
    }

    /**
     * Get the column name
     */
    public String getName() {
        return definition.columnName();
    }

    /**
     * Get the column type
     */
    public String getType() {
        return definition.columnType().typeString();
    }

    public org.apache.doris.plsql.ColumnDefinition definition() {
        return definition;
    }

    /**
     * Get the column value
     */
    Var getValue() {
        return value;
    }
}

