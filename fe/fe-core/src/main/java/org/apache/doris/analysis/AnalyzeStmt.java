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

package org.apache.doris.analysis;

import java.util.List;
import java.util.Map;

/**
 * Collect statistics about a database
 *
 * syntax:
 * ANALYZE [[ db_name.tb_name ] [( column_name [, ...] )], ...] [ PROPERTIES(...) ]
 *
 *     db_name.tb_name: collect table and column statistics from tb_name
 *
 *     column_name: collect column statistics from column_name
 *
 *     properties: properties of statistics jobs
 *
 */
public class AnalyzeStmt extends DdlStmt {
    private final TableName tbl;
    private List<String> columnNames;
    private Map<String, String> properties;

    public AnalyzeStmt(TableName tbl, List<String> columns, Map<String, String> properties) {
        this.tbl = tbl;
        this.columnNames = columns;
        this.properties = properties;
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public TableName getTableName() {
        return this.tbl;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
