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

package org.apache.doris.connector.api.write;

/**
 * One column of a connector's declared write-side sort, in an engine-neutral form.
 *
 * <p>A write-capable connector returns a list of these from
 * {@link ConnectorWritePlanProvider#getWriteSortColumns} when its target requires the BE writer to
 * sort rows before writing (e.g. an iceberg table with a {@code WRITE ORDERED BY} sort order). The
 * engine resolves {@link #getColumnIndex()} against the bound sink output (the same full-schema
 * indexing the sink uses for write distribution) and builds the Thrift {@code TSortInfo}, which the
 * connector stamps onto its opaque sink in {@code planWrite}.</p>
 *
 * <p>The three fields map 1:1 onto the legacy iceberg sink's {@code orderingExprs} /
 * {@code isAscOrder} / {@code isNullsFirst} triple. The connector cannot build the {@code TSortInfo}
 * itself because the bound output expressions live only in the engine (translation time).</p>
 */
public final class ConnectorWriteSortColumn {

    private final int columnIndex;
    private final boolean asc;
    private final boolean nullsFirst;

    public ConnectorWriteSortColumn(int columnIndex, boolean asc, boolean nullsFirst) {
        this.columnIndex = columnIndex;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
    }

    /** Position of the sort column in the sink's full-schema output. */
    public int getColumnIndex() {
        return columnIndex;
    }

    /** Whether the column sorts ascending. */
    public boolean isAsc() {
        return asc;
    }

    /** Whether nulls sort first. */
    public boolean isNullsFirst() {
        return nullsFirst;
    }
}
