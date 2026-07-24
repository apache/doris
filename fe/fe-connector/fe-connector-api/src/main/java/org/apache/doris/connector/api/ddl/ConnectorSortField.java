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

package org.apache.doris.connector.api.ddl;

import java.util.Objects;

/**
 * One field of a {@code CREATE TABLE ... ORDER BY (...)} write-order clause, carried neutrally
 * across the SPI in {@link ConnectorCreateTableRequest#getSortOrder()}.
 *
 * <p>Mirrors the fe-core {@code SortFieldInfo} (column name + ASC/DESC + NULLS FIRST/LAST) without
 * the connector taking a compile-time dependency on fe-core. Connectors that do not support a
 * write order (e.g. paimon) simply ignore the list.</p>
 */
public final class ConnectorSortField {

    private final String columnName;
    private final boolean ascending;
    private final boolean nullFirst;

    public ConnectorSortField(String columnName, boolean ascending, boolean nullFirst) {
        this.columnName = Objects.requireNonNull(columnName, "columnName");
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

    @Override
    public String toString() {
        return columnName + (ascending ? " ASC" : " DESC")
                + (nullFirst ? " NULLS FIRST" : " NULLS LAST");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorSortField)) {
            return false;
        }
        ConnectorSortField that = (ConnectorSortField) o;
        return ascending == that.ascending
                && nullFirst == that.nullFirst
                && columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, ascending, nullFirst);
    }
}
