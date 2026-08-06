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

package org.apache.doris.nereids.trees.plans.commands.info;

/**
 * Sort field information for CREATE TABLE ORDER BY clause
 */
public class SortFieldInfo {
    private final String columnName;
    private final boolean ascending;
    private final boolean nullFirst;

    public SortFieldInfo(String columnName) {
        this.columnName = columnName;
        this.ascending = true; // default to ascending
        this.nullFirst = true; // default to nulls first
    }

    public SortFieldInfo(String columnName, boolean ascending, boolean nullFirst) {
        this.columnName = columnName;
        this.ascending = ascending;
        this.nullFirst = nullFirst;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isAscending() {
        return ascending;
    }

    public boolean isNullFirst() {
        return nullFirst;
    }

    /**
     * Convert to SQL format with backticks around column name
     * @return SQL string like: `column_name` ASC NULLS FIRST
     */
    public String toSql() {
        StringBuilder sql = new StringBuilder("`").append(columnName).append("`");
        sql.append(ascending ? " ASC" : " DESC");
        sql.append(" NULLS ").append(nullFirst ? "FIRST" : "LAST");
        return sql.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
