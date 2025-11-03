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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Row.java
// and modified by Doris

package org.apache.doris.plsql;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Table row (all columns)
 */
public class Row {
    private final org.apache.doris.plsql.ColumnMap colMap
            = new org.apache.doris.plsql.ColumnMap();

    public Row() {
    }

    Row(Row row) {
        for (Column c : row.colMap.columns()) {
            addColumnDefinition(c.getName(), c.getType());
        }
    }

    /**
     * Add a column with specified data type
     */
    public void addColumnDefinition(String name, String type) {
        colMap.add(new Column(name, type, null));
    }

    public void addColumn(String name, String type, Var value) {
        Column column = new Column(name, type, value);
        colMap.add(column);
    }

    /**
     * Get the data type by column name
     */
    public String getType(String name) {
        Column column = colMap.get(name);
        return column != null ? column.getType() : null;
    }

    /**
     * Get value by index
     */
    public Var getValue(int i) {
        return colMap.at(i).getValue();
    }

    /**
     * Get value by column name
     */
    Var getValue(String name) {
        Column column = colMap.get(name);
        return column != null ? column.getValue() : null;
    }

    /**
     * Get columns
     */
    List<Column> getColumns() {
        return colMap.columns();
    }

    /**
     * Get column by index
     */
    public Column getColumn(int i) {
        return colMap.at(i);
    }

    /**
     * Get the number of columns
     */
    int size() {
        return colMap.size();
    }

    public List<org.apache.doris.plsql.ColumnDefinition> columnDefinitions() {
        return getColumns().stream().map(Column::definition).collect(Collectors.toList());
    }
}

