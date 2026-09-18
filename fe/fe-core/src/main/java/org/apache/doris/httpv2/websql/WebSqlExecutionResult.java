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

package org.apache.doris.httpv2.websql;

import java.util.List;

/** JSON result of one statement, including rows or update count plus session metadata used by HTTP clients. */
public class WebSqlExecutionResult {
    private final List<WebSqlColumn> columns;
    private final List<List<Object>> rows;
    private final long affectedRows;
    private final long elapsedTimeMs;
    private final String queryId;
    private final List<String> warnings;
    private final String catalog;
    private final String database;
    private final boolean truncated;

    public WebSqlExecutionResult(List<WebSqlColumn> columns, List<List<Object>> rows, long affectedRows,
            long elapsedTimeMs, String queryId, List<String> warnings, String catalog, String database,
            boolean truncated) {
        this.columns = columns;
        this.rows = rows;
        this.affectedRows = affectedRows;
        this.elapsedTimeMs = elapsedTimeMs;
        this.queryId = queryId;
        this.warnings = warnings;
        this.catalog = catalog;
        this.database = database;
        this.truncated = truncated;
    }

    public List<WebSqlColumn> getColumns() {
        return columns;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getElapsedTimeMs() {
        return elapsedTimeMs;
    }

    public String getQueryId() {
        return queryId;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDatabase() {
        return database;
    }

    public boolean isTruncated() {
        return truncated;
    }
}
