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

package org.apache.doris.connector.api;

import java.util.Objects;

/**
 * The neutral definition of a connector view: its stored SQL text and the SQL dialect that text is
 * written in. Returned by {@code ConnectorTableOps.getViewDefinition} so fe-core can parse and analyze
 * an external view (e.g. iceberg) without knowing the connector's native view types. Trino-aligned
 * ({@code ConnectorViewDefinition} carries the SQL + dialect as first-class fields).
 */
public final class ConnectorViewDefinition {

    private final String sql;
    private final String dialect;

    public ConnectorViewDefinition(String sql, String dialect) {
        this.sql = Objects.requireNonNull(sql, "sql");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    /** The stored view SQL text. */
    public String getSql() {
        return sql;
    }

    /** The SQL dialect the {@link #getSql() text} is written in (e.g. {@code spark}, {@code trino}). */
    public String getDialect() {
        return dialect;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorViewDefinition)) {
            return false;
        }
        ConnectorViewDefinition that = (ConnectorViewDefinition) o;
        return sql.equals(that.sql)
                && dialect.equals(that.dialect);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, dialect);
    }

    @Override
    public String toString() {
        return "ConnectorViewDefinition{dialect='" + dialect + "'}";
    }
}
