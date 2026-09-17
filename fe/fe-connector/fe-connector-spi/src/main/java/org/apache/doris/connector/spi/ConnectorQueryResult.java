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


package org.apache.doris.connector.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The fully materialized rows of a probe query run on the remote source through
 * {@link ConnectorPassthroughSqlOps#executeQuery}.
 *
 * <p>Column names are the driver's result-set labels in result order; every row has one value per column,
 * in the same order, and a value is whatever the driver's {@code getObject} answered (a {@link Boolean} for
 * a PostgreSQL {@code boolean}, a {@link String} for a MySQL {@code SHOW VARIABLES} value, {@code null} for
 * SQL NULL). Callers must not assume every value is a {@code String}.</p>
 *
 * <p>Immutable. The whole result lives in memory, so it is meant for the small answers an engine-side probe
 * reads (a variable, a catalog row), not for data.</p>
 */
public final class ConnectorQueryResult {

    private final List<String> columnNames;
    private final List<List<Object>> rows;

    public ConnectorQueryResult(List<String> columnNames, List<List<Object>> rows) {
        this.columnNames = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(columnNames, "columnNames")));
        List<List<Object>> copied = new ArrayList<>(Objects.requireNonNull(rows, "rows").size());
        for (List<Object> row : rows) {
            copied.add(Collections.unmodifiableList(new ArrayList<>(row)));
        }
        this.rows = Collections.unmodifiableList(copied);
    }

    /** The result's column labels, in result order. */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /** The rows, each holding one value per column in {@link #getColumnNames()} order. */
    public List<List<Object>> getRows() {
        return rows;
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    @Override
    public String toString() {
        return "ConnectorQueryResult{columns=" + columnNames + ", rows=" + rows.size() + "}";
    }
}
