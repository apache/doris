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
 * The position of a column in an {@code ALTER TABLE ADD/MODIFY COLUMN} clause, carried neutrally
 * across the SPI by {@link org.apache.doris.connector.api.ConnectorTableOps#addColumn} /
 * {@link org.apache.doris.connector.api.ConnectorTableOps#modifyColumn}.
 *
 * <p>Faithful, lossless neutralization of the fe-catalog {@code ColumnPosition}, which is exactly
 * {@code FIRST | AFTER <column>} (there is no {@code BEFORE} variant). The connector taking a
 * compile-time dependency on fe-catalog/nereids types is forbidden by the iron law, so the SPI
 * passes this DTO instead.</p>
 *
 * <p>A {@code null} position means "no position clause" (append at the end / keep current position),
 * matching the legacy {@code if (position != null)} guard.</p>
 */
public final class ConnectorColumnPosition {

    /** Place the column first. Mirrors fe-catalog {@code ColumnPosition.FIRST}. */
    public static final ConnectorColumnPosition FIRST = new ConnectorColumnPosition(true, null);

    private final boolean first;
    // The column name to place this column after; non-null iff !first.
    private final String afterColumn;

    private ConnectorColumnPosition(boolean first, String afterColumn) {
        this.first = first;
        this.afterColumn = afterColumn;
    }

    /** Place the column after the named column. Mirrors fe-catalog {@code new ColumnPosition(col)}. */
    public static ConnectorColumnPosition after(String afterColumn) {
        return new ConnectorColumnPosition(false, Objects.requireNonNull(afterColumn, "afterColumn"));
    }

    public boolean isFirst() {
        return first;
    }

    /** The column to place this column after; only meaningful when {@link #isFirst()} is false. */
    public String getAfterColumn() {
        return afterColumn;
    }

    @Override
    public String toString() {
        return first ? "FIRST" : "AFTER " + afterColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorColumnPosition)) {
            return false;
        }
        ConnectorColumnPosition that = (ConnectorColumnPosition) o;
        return first == that.first && Objects.equals(afterColumn, that.afterColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, afterColumn);
    }
}
